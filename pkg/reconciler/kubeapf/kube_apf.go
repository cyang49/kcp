package kubeapf

import (
	"context"
	"fmt"
	"sync"
	"time"

	tenancyv1alpha1 "github.com/kcp-dev/kcp/pkg/apis/tenancy/v1alpha1"
	"github.com/kcp-dev/kcp/pkg/client"
	tenancyinformers "github.com/kcp-dev/kcp/pkg/client/informers/externalversions/tenancy/v1alpha1"
	"github.com/kcp-dev/kcp/pkg/logging"
	"github.com/kcp-dev/logicalcluster/v2"
	flowcontrol "k8s.io/api/flowcontrol/v1beta2"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	genericapirequest "k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/apiserver/pkg/server/mux"
	utilflowcontrol "k8s.io/apiserver/pkg/util/flowcontrol"
	fq "k8s.io/apiserver/pkg/util/flowcontrol/fairqueuing"
	fcrequest "k8s.io/apiserver/pkg/util/flowcontrol/request"
	kubeinformers "k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"
)

const KubeApfDelegatorName = "kcp-kube-apf-delegator"

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

	cwQueue workqueue.RateLimitingInterface

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

	getClusterWorkspace func(key string) (*tenancyv1alpha1.ClusterWorkspace, error)
}

// Make sure utilflowcontrol.Interface is implemented
// var _ utilflowcontrol.Interface = &KubeApfDelegator{}

// NewKubeApfDelegator
func NewKubeApfDelegator(
	informerFactory kubeinformers.SharedInformerFactory,
	kubeCluster kubernetes.ClusterInterface,
	clusterWorkspacesInformer tenancyinformers.ClusterWorkspaceInformer,
	serverConcurrencyLimit int,
	requestWaitLimit time.Duration,
) *KubeApfDelegator {
	k := &KubeApfDelegator{
		scopingSharedInformerFactory: newScopingSharedInformerFactory(informerFactory), // not cluster scoped
		kubeCluster:                  kubeCluster,                                      // can be made cluster scoped
		serverConcurrencyLimit:       serverConcurrencyLimit,
		requestWaitLimit:             requestWaitLimit,
		cwQueue:                      workqueue.NewRateLimitingQueue(workqueue.DefaultControllerRateLimiter()),
		delegates:                    map[logicalcluster.Name]utilflowcontrol.Interface{},
		delegateStopChs:              map[logicalcluster.Name]chan struct{}{},
		KcpWatchTracker:              utilflowcontrol.NewKcpWatchTracker(),
		getClusterWorkspace: func(key string) (*tenancyv1alpha1.ClusterWorkspace, error) {
			return clusterWorkspacesInformer.Lister().Get(key)
		},
	}
	clusterWorkspacesInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		DeleteFunc: k.enqueueClusterWorkspace,
	})
	return k
}

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

// MaintainObservations doesn't actually call MaintainObservations functions of delegates directly
// It stores the stopCh for later use
func (k *KubeApfDelegator) MaintainObservations(stopCh <-chan struct{}) {
	k.lock.Lock()
	if k.stopCh == nil {
		k.stopCh = stopCh
	}
	k.lock.Unlock()
	// Block waiting only so that it behaves similarly to cfgCtlr
	<-stopCh
}

// Run starts a goroutine to watch ClusterWorkspace deletions
func (k *KubeApfDelegator) Run(stopCh <-chan struct{}) error {
	k.lock.Lock()
	if k.stopCh == nil {
		k.stopCh = stopCh
	}
	k.lock.Unlock()

	go wait.Until(k.runClusterWorkspaceWorker, time.Second, stopCh)

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

func (k *KubeApfDelegator) enqueueClusterWorkspace(obj interface{}) {
	key, err := cache.DeletionHandlingMetaNamespaceKeyFunc(obj)
	if err != nil {
		runtime.HandleError(err)
		return
	}
	logger := logging.WithQueueKey(logging.WithReconciler(klog.Background(), KubeApfDelegatorName), key)
	logger.V(2).Info("queueing ClusterWorkspace")
	k.cwQueue.Add(key)
}

func (k *KubeApfDelegator) runClusterWorkspaceWorker() {
	for k.processNext(k.cwQueue, k.processClusterWorkspace) {
	}
}

func (c *KubeApfDelegator) processNext(
	queue workqueue.RateLimitingInterface,
	processFunc func(key string) error,
) bool {
	// Wait until there is a new item in the working queue
	k, quit := queue.Get()
	if quit {
		return false
	}
	key := k.(string)

	// No matter what, tell the queue we're done with this key, to unblock
	// other workers.
	defer queue.Done(key)

	if err := processFunc(key); err != nil {
		runtime.HandleError(fmt.Errorf("%q controller failed to sync %q, err: %w", KubeApfDelegatorName, key, err))
		queue.AddRateLimited(key)
		return true
	}
	queue.Forget(key)
	return true
}

func (k *KubeApfDelegator) processClusterWorkspace(key string) error {
	// e.g. root:org<separator>ws
	parent, name := client.SplitClusterAwareKey(key)

	// turn it into root:org:ws
	clusterName := parent.Join(name)
	_, err := k.getClusterWorkspace(key)
	if err != nil {
		if kerrors.IsNotFound(err) {
			k.stopAndRemoveDelegate(clusterName)
			return nil
		}
		return err
	}
	return nil
}

func (k *KubeApfDelegator) stopAndRemoveDelegate(cluster logicalcluster.Name) {
	k.lock.Lock()
	defer k.lock.Unlock()

	if stopCh, ok := k.delegateStopChs[cluster]; ok {
		close(stopCh)
		delete(k.delegateStopChs, cluster)
	}

	delete(k.delegates, cluster)
}
