package kubeapf

import (
	reflect "reflect"

	"github.com/kcp-dev/kcp/pkg/reconciler/util"
	"github.com/kcp-dev/logicalcluster/v2"
	runtime "k8s.io/apimachinery/pkg/runtime"
	schema "k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/informers"
	admissionregistration "k8s.io/client-go/informers/admissionregistration"
	apiserverinternal "k8s.io/client-go/informers/apiserverinternal"
	apps "k8s.io/client-go/informers/apps"
	autoscaling "k8s.io/client-go/informers/autoscaling"
	batch "k8s.io/client-go/informers/batch"
	certificates "k8s.io/client-go/informers/certificates"
	coordination "k8s.io/client-go/informers/coordination"
	core "k8s.io/client-go/informers/core"
	discovery "k8s.io/client-go/informers/discovery"
	events "k8s.io/client-go/informers/events"
	extensions "k8s.io/client-go/informers/extensions"
	flowcontrol "k8s.io/client-go/informers/flowcontrol"
	"k8s.io/client-go/informers/internalinterfaces"
	networking "k8s.io/client-go/informers/networking"
	node "k8s.io/client-go/informers/node"
	policy "k8s.io/client-go/informers/policy"
	rbac "k8s.io/client-go/informers/rbac"
	scheduling "k8s.io/client-go/informers/scheduling"
	storage "k8s.io/client-go/informers/storage"
	cache "k8s.io/client-go/tools/cache"
)

// scopingSharedInformerFactory is used to create scopedSharedInformerFactory that
// targets a specific logical cluster
type scopingSharedInformerFactory struct {
	factory                informers.SharedInformerFactory
	delegatingEventHandler *util.DelegatingEventHandler
}

func newScopingSharedInformerFactory(factory informers.SharedInformerFactory) *scopingSharedInformerFactory {
	return &scopingSharedInformerFactory{
		factory:                factory,
		delegatingEventHandler: util.NewDelegatingEventHandler(),
	}
}

// ForCluster returns a scopedSharedInformerFactory that works on a specific logical cluster
func (f *scopingSharedInformerFactory) ForCluster(clusterName logicalcluster.Name) *scopedSharedInformerFactory {
	return &scopedSharedInformerFactory{
		delegate:               f.factory,
		clusterName:            clusterName,
		delegatingEventHandler: f.delegatingEventHandler,
	}
}

// scopedSharedInformerFactory can create informers (currently limited to Flowcontrol)
// that are for a single cluster
type scopedSharedInformerFactory struct {
	delegate               informers.SharedInformerFactory
	clusterName            logicalcluster.Name
	delegatingEventHandler *util.DelegatingEventHandler
}

var _ informers.SharedInformerFactory = &scopedSharedInformerFactory{}

// ExtraClusterScopedIndexers implements informers.SharedInformerFactory
func (*scopedSharedInformerFactory) ExtraClusterScopedIndexers() cache.Indexers {
	panic("unimplemented")
}

// ExtraNamespaceScopedIndexers implements informers.SharedInformerFactory
func (*scopedSharedInformerFactory) ExtraNamespaceScopedIndexers() cache.Indexers {
	panic("unimplemented")
}

// InformerFor implements informers.SharedInformerFactory
func (*scopedSharedInformerFactory) InformerFor(obj runtime.Object, newFunc internalinterfaces.NewInformerFunc) cache.SharedIndexInformer {
	panic("unimplemented")
}

// KeyFunction implements informers.SharedInformerFactory
func (*scopedSharedInformerFactory) KeyFunction() cache.KeyFunc {
	panic("unimplemented")
}

// Start implements informers.SharedInformerFactory
func (f *scopedSharedInformerFactory) Start(stopCh <-chan struct{}) {
	f.delegate.Start(stopCh)
}

// Admissionregistration implements informers.SharedInformerFactory
func (*scopedSharedInformerFactory) Admissionregistration() admissionregistration.Interface {
	panic("unimplemented")
}

// Apps implements informers.SharedInformerFactory
func (*scopedSharedInformerFactory) Apps() apps.Interface {
	panic("unimplemented")
}

// Autoscaling implements informers.SharedInformerFactory
func (*scopedSharedInformerFactory) Autoscaling() autoscaling.Interface {
	panic("unimplemented")
}

// Batch implements informers.SharedInformerFactory
func (*scopedSharedInformerFactory) Batch() batch.Interface {
	panic("unimplemented")
}

// Certificates implements informers.SharedInformerFactory
func (*scopedSharedInformerFactory) Certificates() certificates.Interface {
	panic("unimplemented")
}

// Coordination implements informers.SharedInformerFactory
func (*scopedSharedInformerFactory) Coordination() coordination.Interface {
	panic("unimplemented")
}

// Core implements informers.SharedInformerFactory
func (*scopedSharedInformerFactory) Core() core.Interface {
	panic("unimplemented")
}

// Discovery implements informers.SharedInformerFactory
func (*scopedSharedInformerFactory) Discovery() discovery.Interface {
	panic("unimplemented")
}

// Events implements informers.SharedInformerFactory
func (*scopedSharedInformerFactory) Events() events.Interface {
	panic("unimplemented")
}

// Extensions implements informers.SharedInformerFactory
func (*scopedSharedInformerFactory) Extensions() extensions.Interface {
	panic("unimplemented")
}

// Flowcontrol returns an implementation of flowcontrol.Interface
// that is targeting a single cluster
func (f *scopedSharedInformerFactory) Flowcontrol() flowcontrol.Interface {
	return &scopedFlowcontrol{
		clusterName:            f.clusterName,
		delegate:               f.delegate.Flowcontrol(),
		delegatingEventHandler: f.delegatingEventHandler,
	}
}

// ForResource implements informers.SharedInformerFactory
func (*scopedSharedInformerFactory) ForResource(resource schema.GroupVersionResource) (informers.GenericInformer, error) {
	panic("unimplemented")
}

// Internal implements informers.SharedInformerFactory
func (*scopedSharedInformerFactory) Internal() apiserverinternal.Interface {
	panic("unimplemented")
}

// Networking implements informers.SharedInformerFactory
func (*scopedSharedInformerFactory) Networking() networking.Interface {
	panic("unimplemented")
}

// Node implements informers.SharedInformerFactory
func (*scopedSharedInformerFactory) Node() node.Interface {
	panic("unimplemented")
}

// Policy implements informers.SharedInformerFactory
func (*scopedSharedInformerFactory) Policy() policy.Interface {
	panic("unimplemented")
}

// Rbac implements informers.SharedInformerFactory
func (*scopedSharedInformerFactory) Rbac() rbac.Interface {
	panic("unimplemented")
}

// Scheduling implements informers.SharedInformerFactory
func (*scopedSharedInformerFactory) Scheduling() scheduling.Interface {
	panic("unimplemented")
}

// Storage implements informers.SharedInformerFactory
func (*scopedSharedInformerFactory) Storage() storage.Interface {
	panic("unimplemented")
}

// WaitForCacheSync implements informers.SharedInformerFactory
func (*scopedSharedInformerFactory) WaitForCacheSync(stopCh <-chan struct{}) map[reflect.Type]bool {
	panic("unimplemented")
}
