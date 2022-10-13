package soct

import (
	"context"
	"fmt"
	"time"

	apisv1alpha1 "github.com/kcp-dev/kcp/pkg/apis/apis/v1alpha1"
	tenancyv1alpha1 "github.com/kcp-dev/kcp/pkg/apis/tenancy/v1alpha1"
	tenancyinformers "github.com/kcp-dev/kcp/pkg/client/informers/externalversions/tenancy/v1alpha1"
	"github.com/kcp-dev/kcp/pkg/logging"
	"github.com/kcp-dev/logicalcluster/v2"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apiextensionsinformers "k8s.io/apiextensions-apiserver/pkg/client/informers/externalversions/apiextensions/v1"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	flowcontrolrequest "k8s.io/apiserver/pkg/util/flowcontrol/request"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clusters"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"
)

const SOCTControllerName = "kcp-storage-object-count-tracker-controller"

// SOCTController monitors ClusterWorkspaces, APIBindings and CRDs and creates or deletes
// cluster-scoped storage object count observer goroutines and trackers accordingly.
// It contains a registry for Getter funcs of all resource types that will be used by
// observer goroutines to udpate the tracker object counts. It also acts as a multiplexer
// for API request-driven queries to retrieve the latest resource object count numbers.
type SOCTController struct {
	cwQueue        workqueue.RateLimitingInterface
	crdQueue       workqueue.RateLimitingInterface
	getterRegistry flowcontrolrequest.StorageObjectCountGetterRegistry
	tracker        flowcontrolrequest.KcpStorageObjectCountTracker

	getClusterWorkspace func(key string) (*tenancyv1alpha1.ClusterWorkspace, error)
	getCRD              func(key string) (*apiextensionsv1.CustomResourceDefinition, error)
	// listCRDs            func() ([]*apiextensionsv1.CustomResourceDefinition, error)
}

// NewSOCTController
func NewSOCTController(
	clusterWorkspacesInformer tenancyinformers.ClusterWorkspaceInformer,
	crdInformer apiextensionsinformers.CustomResourceDefinitionInformer,
	getterRegistry flowcontrolrequest.StorageObjectCountGetterRegistry,
	tracker flowcontrolrequest.KcpStorageObjectCountTracker,
) *SOCTController {
	c := &SOCTController{
		cwQueue:        workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), SOCTControllerName),
		crdQueue:       workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), SOCTControllerName),
		getterRegistry: getterRegistry,
		tracker:        tracker,
		getClusterWorkspace: func(key string) (*tenancyv1alpha1.ClusterWorkspace, error) {
			return clusterWorkspacesInformer.Lister().Get(key)
		},
		getCRD: func(key string) (*apiextensionsv1.CustomResourceDefinition, error) {
			return crdInformer.Lister().Get(key)
		},
		// listCRDs: func() ([]*apiextensionsv1.CustomResourceDefinition, error) {
		// 	return crdInformer.Lister().List(labels.Everything())
		// },
	}

	clusterWorkspacesInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    c.enqueueClusterWorkspace,
		DeleteFunc: c.enqueueClusterWorkspace,
	})

	crdInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    c.enqueueCrd,
		DeleteFunc: c.enqueueCrd,
	})
	return c
}

func (c *SOCTController) enqueueClusterWorkspace(obj interface{}) {
	key, err := cache.DeletionHandlingMetaNamespaceKeyFunc(obj)
	if err != nil {
		runtime.HandleError(err)
		return
	}
	logger := logging.WithQueueKey(logging.WithReconciler(klog.Background(), SOCTControllerName), key)
	logger.V(2).Info("queueing ClusterWorkspace")
	c.cwQueue.Add(key)
}

func (c *SOCTController) enqueueCrd(obj interface{}) {
	key, err := cache.DeletionHandlingMetaNamespaceKeyFunc(obj)
	if err != nil {
		runtime.HandleError(err)
		return
	}
	logger := logging.WithQueueKey(logging.WithReconciler(klog.Background(), SOCTControllerName), key)
	logger.V(2).Info("queueing CRD")
	c.crdQueue.Add(key)
}

// Run starts the SOCT controller. It needs to start updating
// counters in trackers for APF to work
func (c *SOCTController) Run(ctx context.Context, stop <-chan struct{}) {
	defer runtime.HandleCrash()
	defer c.cwQueue.ShutDown()
	defer c.crdQueue.ShutDown()

	klog.Infof("Starting %s controller", SOCTControllerName)
	defer klog.Infof("Shutting down %s controller", SOCTControllerName)

	go wait.Until(func() { c.startClusterWorkspaceWorker(ctx) }, time.Second, stop)
	go wait.Until(func() { c.startCrdWorker(ctx) }, time.Second, stop)

	<-stop
}

func (c *SOCTController) startClusterWorkspaceWorker(ctx context.Context) {
	for c.processNext(ctx, c.cwQueue, c.processClusterWorkspace) {
	}
}

func (c *SOCTController) startCrdWorker(ctx context.Context) {
	for c.processNext(ctx, c.crdQueue, c.processCrd) {
	}
}

func (c *SOCTController) processNext(
	ctx context.Context,
	queue workqueue.RateLimitingInterface,
	processFunc func(ctx context.Context, key string) error,
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

	if err := processFunc(ctx, key); err != nil {
		runtime.HandleError(fmt.Errorf("%q controller failed to sync %q, err: %w", SOCTControllerName, key, err))
		queue.AddRateLimited(key)
		return true
	}
	queue.Forget(key)
	return true
}

func getBoundCluster(crd *apiextensionsv1.CustomResourceDefinition) string {
	cluster, ok := crd.GetAnnotations()[apisv1alpha1.AnnotationSchemaClusterKey]
	if !ok {
		return ""
	}
	return cluster
}

func getResource(crd *apiextensionsv1.CustomResourceDefinition) string {
	return crd.Spec.Names.Plural
}

func (c *SOCTController) processCrd(ctx context.Context, key string) error {
	logger := klog.FromContext(ctx)

	crd, err := c.getCRD(key)
	cluster := getBoundCluster(crd)
	resource := getResource(crd)

	logger = logging.WithObject(logger, crd)
	if err != nil {
		if kerrors.IsNotFound(err) {
			logger.V(2).Info("CRD not found - stopping observer for it (if needed)")
			c.tracker.StopObserving(cluster, resource)
			return nil
		}
		return err
	}
	logger.V(2).Info("New CRD - starting observer for it (if needed)")
	c.tracker.StartObserving(cluster, resource,
		func() int64 {
			return c.getterRegistry.GetObjectCount(logicalcluster.New(cluster), resource)
		},
	)
	return nil
}

func (c *SOCTController) processClusterWorkspace(ctx context.Context, key string) error {
	logger := klog.FromContext(ctx)
	// e.g. root:org<separator>ws
	parent, name := clusters.SplitClusterAwareKey(key)

	// turn it into root:org:ws
	clusterName := parent.Join(name)
	clusterNameStr := clusterName.String()
	ws, err := c.getClusterWorkspace(key)
	logger = logger.WithValues("logicalCluster", clusterNameStr)
	logger.V(2).Info("processClusterWorkspace called")
	if err != nil {
		if kerrors.IsNotFound(err) {
			logger.V(2).Info("ClusterWorkspace not found - deleting tracker")
			c.tracker.DeleteTracker(clusterNameStr)
			return nil
		}
		return err
	}
	logger = logging.WithObject(logger, ws)
	c.tracker.CreateTracker(clusterNameStr)
	logger.V(2).Info("Cluster tracker started")
	// TODO: start observer goroutines
	//       for all api-resources in the logical cluster

	return nil
}
