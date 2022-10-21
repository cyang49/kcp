package soct

import (
	"context"
	"fmt"
	"time"

	tenancyv1alpha1 "github.com/kcp-dev/kcp/pkg/apis/tenancy/v1alpha1"
	tenancyinformers "github.com/kcp-dev/kcp/pkg/client/informers/externalversions/tenancy/v1alpha1"
	"github.com/kcp-dev/kcp/pkg/informer"
	"github.com/kcp-dev/kcp/pkg/logging"
	"github.com/kcp-dev/logicalcluster/v2"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	flowcontrolrequest "k8s.io/apiserver/pkg/util/flowcontrol/request"
	kubernetesclient "k8s.io/client-go/kubernetes"
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
	dynamicDiscoverySharedInformerFactory *informer.DynamicDiscoverySharedInformerFactory
	cwQueue                               workqueue.RateLimitingInterface
	getterRegistry                        flowcontrolrequest.StorageObjectCountGetterRegistry
	tracker                               flowcontrolrequest.KcpStorageObjectCountTracker

	getClusterWorkspace func(key string) (*tenancyv1alpha1.ClusterWorkspace, error)
}

// NewSOCTController
func NewSOCTController(
	kubeClusterClient *kubernetesclient.Cluster,
	clusterWorkspacesInformer tenancyinformers.ClusterWorkspaceInformer,
	dynamicDiscoverySharedInformerFactory *informer.DynamicDiscoverySharedInformerFactory,
	getterRegistry flowcontrolrequest.StorageObjectCountGetterRegistry,
	tracker flowcontrolrequest.KcpStorageObjectCountTracker,
) *SOCTController {
	c := &SOCTController{
		dynamicDiscoverySharedInformerFactory: dynamicDiscoverySharedInformerFactory,
		cwQueue:                               workqueue.NewRateLimitingQueue(workqueue.DefaultControllerRateLimiter()),
		getterRegistry:                        getterRegistry,
		tracker:                               tracker,
		getClusterWorkspace: func(key string) (*tenancyv1alpha1.ClusterWorkspace, error) {
			return clusterWorkspacesInformer.Lister().Get(key)
		},
	}

	clusterWorkspacesInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    c.enqueueClusterWorkspace,
		DeleteFunc: c.enqueueClusterWorkspace,
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

// Run starts the SOCT controller. It needs to start updating
// counters in trackers for APF to work
func (c *SOCTController) Run(ctx context.Context) {
	defer runtime.HandleCrash()
	defer c.cwQueue.ShutDown()

	logger := logging.WithReconciler(klog.FromContext(ctx), SOCTControllerName)
	ctx = klog.NewContext(ctx, logger)
	logger.Info("Starting controller")
	defer logger.Info("Shutting down controller")

	// Start trackers of default clusters
	defaultClusters := []logicalcluster.Name{
		logicalcluster.New("root"),
		logicalcluster.New("system:system-crds"),
		logicalcluster.New("system:shard"),
		logicalcluster.New("system:admin"),
		logicalcluster.New("system:bound-crds"),
		logicalcluster.New("root:compute"),
	}
	for _, cluster := range defaultClusters {
		logger.Info("Starting storage object count tracker for logical cluster", "clusterName", cluster)
		c.startClusterTracker(ctx, cluster)
		defer c.stopClusterTracker(ctx, cluster)
	}

	go wait.UntilWithContext(ctx, c.runClusterWorkspaceWorker, time.Second)

	<-ctx.Done()
}

func (c *SOCTController) runClusterWorkspaceWorker(ctx context.Context) {
	for c.processNext(ctx, c.cwQueue, c.processClusterWorkspace) {
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

	logger := logging.WithQueueKey(klog.FromContext(ctx), key)
	ctx = klog.NewContext(ctx, logger)
	logger.V(1).Info("SOCT-processing-key")

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

func (c *SOCTController) processClusterWorkspace(ctx context.Context, key string) error {
	logger := klog.FromContext(ctx)
	// e.g. root:org<separator>ws
	parent, name := clusters.SplitClusterAwareKey(key)

	// turn it into root:org:ws
	clusterName := parent.Join(name)
	ws, err := c.getClusterWorkspace(key)
	logger = logger.WithValues("logicalCluster", clusterName.String())
	logger.V(2).Info("processClusterWorkspace called")
	if err != nil {
		if kerrors.IsNotFound(err) {
			logger.V(2).Info("ClusterWorkspace not found - deleting tracker")
			c.stopClusterTracker(ctx, clusterName)
			return nil
		}
		return err
	}
	logger = logging.WithObject(logger, ws)
	c.startClusterTracker(ctx, clusterName)
	logger.V(2).Info("Cluster tracker started")

	return nil
}

func (c *SOCTController) startClusterTracker(ctx context.Context, clusterName logicalcluster.Name) {
	clusterNameStr := clusterName.String()

	// Pass the global context in for the cluster specific tracker created to catch global ctx.Done
	// so that the pruning and the observer goroutines can be gracefully terminated if global context
	// is cancelled
	c.tracker.CreateTracker(ctx, clusterNameStr)

	// Start a goroutine to subscribe to changes in API
	apisChanged := c.dynamicDiscoverySharedInformerFactory.Subscribe("soct-" + clusterNameStr)
	go func() {
		var discoveryCancel func()

		for {
			select {
			case <-ctx.Done():
				if discoveryCancel != nil {
					discoveryCancel()
				}

				return
			case <-apisChanged:
				if discoveryCancel != nil {
					discoveryCancel()
				}
				// logger.V(4).Info("got API change notification")
				ctx, discoveryCancel = context.WithCancel(ctx) // TODO: fix the usage of contexts
				c.updateObservers(ctx, clusterName)
			}
		}
	}()
}

func (c *SOCTController) stopClusterTracker(ctx context.Context, clusterName logicalcluster.Name) {
	clusterNameStr := clusterName.String()
	c.dynamicDiscoverySharedInformerFactory.Unsubscribe("soct-" + clusterNameStr)
	// FIXME: should also stop discovery threads
	c.tracker.DeleteTracker(clusterNameStr)
}

func (c *SOCTController) updateObservers(ctx context.Context, cluster logicalcluster.Name) {
	// Start observer goroutines for all api resources in the logical cluster
	listers, notSynced := c.dynamicDiscoverySharedInformerFactory.Listers()

	// TODO: should pass context into start and stop observer functions

	// StartObserving might be called multiple times for the same resource
	// subsequent calls will be ignored
	for gvr := range listers {
		resourceName := gvr.GroupResource().String()
		c.tracker.StartObserving(cluster.String(), resourceName,
			func() int64 { return c.getterRegistry.GetObjectCount(cluster, resourceName) },
		)
	}
	for _, gvr := range notSynced {
		resourceName := gvr.GroupResource().String()
		c.tracker.StartObserving(cluster.String(), resourceName,
			func() int64 { return c.getterRegistry.GetObjectCount(cluster, resourceName) },
		)
	}
}
