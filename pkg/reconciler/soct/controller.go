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
func (c *SOCTController) Run(ctx context.Context, stop <-chan struct{}) {
	defer runtime.HandleCrash()
	defer c.cwQueue.ShutDown()

	klog.Infof("Starting %s controller", SOCTControllerName)
	defer klog.Infof("Shutting down %s controller", SOCTControllerName)

	go wait.Until(func() { c.runClusterWorkspaceWorker(ctx) }, time.Second, stop)

	<-stop
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
	// c.updateObservers(ctx, clusterName)

	// TODO: start a goroutine to subscribe to changes in API
	apisChanged := c.dynamicDiscoverySharedInformerFactory.Subscribe("soct-" + clusterName.String())
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

				logger.V(4).Info("got API change notification")

				ctx, discoveryCancel = context.WithCancel(ctx)
				c.updateObservers(ctx, clusterName)
			}
		}
	}()
	return nil
}

func (c *SOCTController) updateObservers(ctx context.Context, cluster logicalcluster.Name) {
	// Start observer goroutines for all api resources in the logical cluster
	listers, notSynced := c.dynamicDiscoverySharedInformerFactory.Listers()
	// StartObserving might be called multiple times for the same resource
	// subsequent calls will be ignored
	for gvr := range listers {
		c.tracker.StartObserving(cluster.String(), gvr.String(),
			func() int64 { return c.getterRegistry.GetObjectCount(cluster, gvr.String()) },
		)
	}
	for _, gvr := range notSynced {
		c.tracker.StartObserving(cluster.String(), gvr.String(),
			func() int64 { return c.getterRegistry.GetObjectCount(cluster, gvr.String()) },
		)
	}
}
