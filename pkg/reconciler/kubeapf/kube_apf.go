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
	flowcontrolclient "k8s.io/client-go/kubernetes/typed/flowcontrol/v1beta2"
)

// KubeApfDelegator implements k8s APF controller interface
// it is cluster-aware and manages the life cycles of
// cluster-specific APF controller instances and delegates
// requests from the handler chain to them
type KubeApfDelegator struct {
	// scopingInformerFactory is not cluster scoped but can be made cluster scoped
	scopingSharedInformerFactory *scopingSharedInformerFactory
	// scopingGenericSharedInformerFactory *kubequota.ScopingGenericSharedInformerFactory
	kubeCluster kubernetes.ClusterInterface

	// flowcontrolClient should be cluster scoped
	flowcontrolClient flowcontrolclient.FlowcontrolV1beta2Interface

	// for now assume these are globl configurations for all logical clusters to inherit
	serverConcurrencyLimit int
	requestWaitLimit       time.Duration

	// delegates stores the references to cluster scoped apf filters
	delegates map[logicalcluster.Name]utilflowcontrol.Interface

	lock sync.RWMutex
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
	newFilter := &KubeApfDelegator{
		scopingSharedInformerFactory: newScopingSharedInformerFactory(informerFactory), // not cluster scoped
		kubeCluster:                  kubeCluster,                                      // can be made cluster scoped
		serverConcurrencyLimit:       serverConcurrencyLimit,
		requestWaitLimit:             requestWaitLimit,
		delegates:                    map[logicalcluster.Name]utilflowcontrol.Interface{},
	}

	// start APF on the default Cluster
	flowcontrolClient := kubeCluster.Cluster(defaultCluster).FlowcontrolV1beta2()
	scopedSharedInformerFactory := newFilter.scopingSharedInformerFactory.ForCluster(defaultCluster)
	newFilter.delegates[defaultCluster] = utilflowcontrol.New(scopedSharedInformerFactory, flowcontrolClient, serverConcurrencyLimit, requestWaitLimit)

	return newFilter
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

	// k.delegates[defaultCluster].Handle(ctx, requestDigest, noteFn, workEstimator, queueNoteFn, execFn)
	delegate.Handle(ctx, requestDigest, noteFn, workEstimator, queueNoteFn, execFn)
}

// GetInterestedWatchCount implements flowcontrol.Interface
func (k *KubeApfDelegator) GetInterestedWatchCount(requestInfo *request.RequestInfo) int {
	return k.delegates[defaultCluster].GetInterestedWatchCount(requestInfo)
}

// RegisterWatch implements flowcontrol.Interface
func (k *KubeApfDelegator) RegisterWatch(r *http.Request) utilflowcontrol.ForgetWatchFunc {
	return k.delegates[defaultCluster].RegisterWatch(r)
}

// Install implements flowcontrol.Interface
func (k *KubeApfDelegator) Install(c *mux.PathRecorderMux) {
	k.delegates[defaultCluster].Install(c)
}

// MaintainObservations implements flowcontrol.Interface
func (k *KubeApfDelegator) MaintainObservations(stopCh <-chan struct{}) {
	k.delegates[defaultCluster].MaintainObservations(stopCh)
}

// Run implements flowcontrol.Interface
// it starts kube apf controller
func (k *KubeApfDelegator) Run(stopCh <-chan struct{}) error {
	// TODO: start ClusterWorkspaceDeletionMonitor

	// Run kube apf controller. Cluster specific apf controller will be created
	// on demand
	return k.delegates[defaultCluster].Run(stopCh)
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

	// // Set up a context that is cancelable and that is bounded by k.serverDone
	// ctx, cancel := context.WithCancel(context.Background())
	// go func() {
	// 	// Wait for either the context or the server to be done. If it's the server, cancel the context.
	// 	select {
	// 	case <-ctx.Done():
	// 		case <-k.serverDone:
	// 			cancel()
	// 	}
	// }()

	// TODO: new delegate should use cluster scoped informer factory and flowcontrol clients
	scopedInformerFactory := k.scopingSharedInformerFactory.ForCluster(defaultCluster)
	flowcontrolClient := k.kubeCluster.Cluster(clusterName).FlowcontrolV1beta2()
	delegate = utilflowcontrol.New(
		scopedInformerFactory,
		flowcontrolClient,
		k.serverConcurrencyLimit,
		k.requestWaitLimit,
	)

	k.delegates[clusterName] = delegate
	// TODO: implement stop channel mechanism
	stopCh := make(chan struct{})
	// Start cluster scoped apf controller
	go delegate.Run(stopCh)

	klog.V(3).InfoS("Starting new apf controller for cluster", "clusterName", clusterName)
	return delegate, nil
}
