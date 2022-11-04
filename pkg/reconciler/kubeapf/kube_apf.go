package kubeapf

import (
	"context"
	"sync"
	"time"

	"github.com/kcp-dev/logicalcluster/v2"
	kubeinformers "k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"

	flowcontrol "k8s.io/api/flowcontrol/v1beta2"
	genericapirequest "k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/apiserver/pkg/server/mux"
	utilflowcontrol "k8s.io/apiserver/pkg/util/flowcontrol"
	fq "k8s.io/apiserver/pkg/util/flowcontrol/fairqueuing"
	fcrequest "k8s.io/apiserver/pkg/util/flowcontrol/request"
)

// KubeApfDelegator implements k8s APF controller interface
// it is cluster-aware and manages the life cycles of
// cluster-specific APF controller instances and delegates
// requests from the handler chain to them
type KubeApfDelegator struct {
	// scopingInformerFactory is not cluster scoped but can be made cluster scoped
	scopingSharedInformerFactory *scopingSharedInformerFactory
	// kubeCluster ClusterInterface can be used to get cluster scoped clientset
	kubeCluster kubernetes.ClusterInterface

	// for now assume these are globl configurations for all logical clusters to inherit
	serverConcurrencyLimit int
	requestWaitLimit       time.Duration

	lock sync.RWMutex
	// delegates are the references to cluster specific apf controllers
	delegates map[logicalcluster.Name]utilflowcontrol.Interface
	// delegateStopChs are cluster specific stopChs that can be used
	// to stop single delegate when its corresponding ClusterWorkspace
	// is removed
	delegateStopChs map[logicalcluster.Name]chan struct{}

	pathRecorderMux *mux.PathRecorderMux

	// stopCh lets delegator receive stop signal from outside
	stopCh <-chan struct{}

	utilflowcontrol.KcpWatchTracker
}

// Make sure utilflowcontrol.Interface is implemented
// var _ utilflowcontrol.Interface = &KubeApfDelegator{}

// NewKubeApfDelegator
func NewKubeApfDelegator(
	informerFactory kubeinformers.SharedInformerFactory,
	kubeCluster kubernetes.ClusterInterface,
	serverConcurrencyLimit int,
	requestWaitLimit time.Duration,
) *KubeApfDelegator {
	return &KubeApfDelegator{
		scopingSharedInformerFactory: newScopingSharedInformerFactory(informerFactory), // not cluster scoped
		kubeCluster:                  kubeCluster,                                      // can be made cluster scoped
		serverConcurrencyLimit:       serverConcurrencyLimit,
		requestWaitLimit:             requestWaitLimit,
		delegates:                    map[logicalcluster.Name]utilflowcontrol.Interface{},
		delegateStopChs:              map[logicalcluster.Name]chan struct{}{},
		KcpWatchTracker:              utilflowcontrol.NewKcpWatchTracker(),
	}
}

// TODO: monitor ClusterWorkspace Deletes and remove corresponding delegate

// Handle implements flowcontrol.Interface
func (k *KubeApfDelegator) Handle(ctx context.Context,
	requestDigest utilflowcontrol.RequestDigest,
	noteFn func(fs *flowcontrol.FlowSchema, pl *flowcontrol.PriorityLevelConfiguration, flowDistinguisher string),
	workEstimator func() fcrequest.WorkEstimate,
	queueNoteFn fq.QueueNoteFn,
	execFn func(),
) {
	cluster, _ := genericapirequest.ValidClusterFrom(ctx)
	klog.V(3).InfoS("KubeApfFilter Handle request for cluster ", "clusterName", cluster.Name)

	delegate, _ := k.getOrCreateDelegate(cluster.Name)
	delegate.Handle(ctx, requestDigest, noteFn, workEstimator, queueNoteFn, execFn)
}

// Install implements flowcontrol.Interface
func (k *KubeApfDelegator) Install(c *mux.PathRecorderMux) {
	k.pathRecorderMux = c // store the reference for Install later // FIXME
}

// MaintainObservations see comments for the Run function
func (k *KubeApfDelegator) MaintainObservations(stopCh <-chan struct{}) {
	k.lock.Lock()
	if k.stopCh == nil {
		k.stopCh = stopCh
	}
	k.lock.Unlock()
	// Block waiting only so that it behaves similarly to cfgCtlr
	<-stopCh
}

// Run doesn't actually call Run functions of delegates directly
// It stores the stopCh for later use when the cluster specific delegates are created
func (k *KubeApfDelegator) Run(stopCh <-chan struct{}) error {
	k.lock.Lock()
	if k.stopCh == nil {
		k.stopCh = stopCh
	}
	k.lock.Unlock()
	// Block waiting only so that it behaves similarly to cfgCtlr
	<-stopCh
	return nil
}

// getOrCreateDelegate creates a utilflowcontrol.Interface (apf filter) for clusterName.
func (k *KubeApfDelegator) getOrCreateDelegate(clusterName logicalcluster.Name) (utilflowcontrol.Interface, error) {
	k.lock.RLock()
	delegate := k.delegates[clusterName]
	k.lock.RUnlock()

	if delegate != nil {
		return delegate, nil
	}

	k.lock.Lock()
	defer k.lock.Unlock()

	delegate = k.delegates[clusterName]
	if delegate != nil {
		return delegate, nil
	}

	delegateStopCh := make(chan struct{})
	go func() {
		select {
		case <-k.stopCh:
			close(delegateStopCh)
		case <-delegateStopCh:
		}
	}()

	// New delegate uses cluster scoped informer factory and flowcontrol clients
	scopedInformerFactory := k.scopingSharedInformerFactory.ForCluster(clusterName)
	flowcontrolClient := k.kubeCluster.Cluster(clusterName).FlowcontrolV1beta2()
	delegate = utilflowcontrol.New(
		scopedInformerFactory,
		flowcontrolClient,
		k.serverConcurrencyLimit,
		k.requestWaitLimit,
	)
	scopedInformerFactory.Start(delegateStopCh)
	k.delegates[clusterName] = delegate
	k.delegateStopChs[clusterName] = delegateStopCh
	// TODO: can Unlock here?

	// Run cluster specific apf controller
	go delegate.MaintainObservations(delegateStopCh) // FIXME: Metric observations need to work per-cluster --> beware of metrics explosion
	go delegate.Run(delegateStopCh)

	// TODO: need to install per-cluster debug endpoint
	delegate.Install(k.pathRecorderMux) // FIXME: this is nil

	klog.V(3).InfoS("Started new apf controller for cluster", "clusterName", clusterName)
	return delegate, nil
}
