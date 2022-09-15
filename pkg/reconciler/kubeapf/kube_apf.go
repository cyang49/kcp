package kubeapf

import (
	"context"
	"net/http"
	"sync"
	"time"

	"github.com/kcp-dev/logicalcluster/v2"
	kubeinformers "k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"

	flowcontrol "k8s.io/api/flowcontrol/v1beta2"
	"k8s.io/apiserver/pkg/endpoints/request"
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

	// delegates are the references to cluster scoped apf controllers
	delegates map[logicalcluster.Name]utilflowcontrol.Interface

	lock sync.RWMutex

	pathRecorderMux *mux.PathRecorderMux

	stopCh <-chan struct{}
}

// Make sure utilflowcontrol.Interface is implemented
var _ utilflowcontrol.Interface = &KubeApfDelegator{}

var defaultCluster logicalcluster.Name = logicalcluster.Wildcard

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
	}
}

// Handle implements flowcontrol.Interface
func (k *KubeApfDelegator) Handle(ctx context.Context,
	requestDigest utilflowcontrol.RequestDigest,
	noteFn func(fs *flowcontrol.FlowSchema, pl *flowcontrol.PriorityLevelConfiguration, flowDistinguisher string),
	workEstimator func() fcrequest.WorkEstimate,
	queueNoteFn fq.QueueNoteFn,
	execFn func(),
) {
	// TODO: missing error handling
	cluster, _ := genericapirequest.ValidClusterFrom(ctx)
	klog.V(3).InfoS("KubeApfFilter Handle request for cluster ", "clusterName", cluster.Name)

	delegate, _ := k.getOrCreateDelegate(cluster.Name)

	delegate.Handle(ctx, requestDigest, noteFn, workEstimator, queueNoteFn, execFn)
}

// GetInterestedWatchCount implements flowcontrol.Interface
func (k *KubeApfDelegator) GetInterestedWatchCount(requestInfo *request.RequestInfo) int {
	// FIXME: Figure out the right way to implement WatchTracker
	return k.delegates[defaultCluster].GetInterestedWatchCount(requestInfo)
}

// RegisterWatch implements flowcontrol.Interface
func (k *KubeApfDelegator) RegisterWatch(r *http.Request) utilflowcontrol.ForgetWatchFunc {
	// FIXME: Figure out the right way to implement WatchTracker
	return k.delegates[defaultCluster].RegisterWatch(r)
}

// Install implements flowcontrol.Interface
func (k *KubeApfDelegator) Install(c *mux.PathRecorderMux) {
	// k.pathRecorderMux = c // store the reference for Install later
	// Do nothing for now
}

// MaintainObservations implements flowcontrol.Interface
func (k *KubeApfDelegator) MaintainObservations(stopCh <-chan struct{}) {
	// TODO: make sure MaintainObservations implementation works with clusters
	k.stopCh = stopCh
}

// Run implements flowcontrol.Interface
// The delegator doesn't actually call apf controller Run here
// It stores the stopCh for later use when the cluster scoped
// apf controllers are created
func (k *KubeApfDelegator) Run(stopCh <-chan struct{}) error {
	k.stopCh = stopCh
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

	// New delegate uses cluster scoped informer factory and flowcontrol clients
	scopedInformerFactory := k.scopingSharedInformerFactory.ForCluster(clusterName)
	flowcontrolClient := k.kubeCluster.Cluster(clusterName).FlowcontrolV1beta2()
	delegate = utilflowcontrol.New(
		clusterName.String()+"-controller",
		scopedInformerFactory,
		flowcontrolClient,
		k.serverConcurrencyLimit,
		k.requestWaitLimit,
	)

	// TODO: call scopedInformerFactory.Start?
	scopedInformerFactory.Start(k.stopCh)

	k.delegates[clusterName] = delegate
	// Start cluster scoped apf controller
	go delegate.MaintainObservations(k.stopCh)
	go delegate.Run(k.stopCh)

	// delegate.Install(k.pathRecorderMux)

	klog.V(3).InfoS("Started new apf controller for cluster", "clusterName", clusterName)
	return delegate, nil
}
