package util

import (
	"time"

	"github.com/kcp-dev/logicalcluster/v2"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/tools/cache"
)

// DelegatingInformer embeds a cache.SharedIndexInformer, delegating adding event handlers to
// registerEventHandlerForCluster.
type DelegatingInformer struct {
	ClusterName logicalcluster.Name
	Resource    schema.GroupResource
	cache.SharedIndexInformer
	DelegatingEventHandler *DelegatingEventHandler
}

// AddEventHandler registers with the delegating event handler.
func (d *DelegatingInformer) AddEventHandler(handler cache.ResourceEventHandler) {
	d.DelegatingEventHandler.registerEventHandler(d.Resource, d.SharedIndexInformer, d.ClusterName, handler)
}

// AddEventHandlerWithResyncPeriod registers with the delegating event handler with the given resync period.
func (d *DelegatingInformer) AddEventHandlerWithResyncPeriod(handler cache.ResourceEventHandler, resyncPeriod time.Duration) {
	d.DelegatingEventHandler.registerEventHandlerWithResyncPeriod(d.Resource, d.SharedIndexInformer, d.ClusterName, handler, resyncPeriod)
}
