package kubeapf

import (
	"github.com/kcp-dev/kcp/pkg/client"
	"github.com/kcp-dev/kcp/pkg/indexers"
	"github.com/kcp-dev/kcp/pkg/reconciler/util"
	"github.com/kcp-dev/logicalcluster/v2"
	apiflowcontrolv1beta2 "k8s.io/api/flowcontrol/v1beta2"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/labels"
	flowcontrol "k8s.io/client-go/informers/flowcontrol"
	v1alpha1 "k8s.io/client-go/informers/flowcontrol/v1alpha1"
	v1beta1 "k8s.io/client-go/informers/flowcontrol/v1beta1"
	v1beta2 "k8s.io/client-go/informers/flowcontrol/v1beta2"
	flowcontrollisterv1beta2 "k8s.io/client-go/listers/flowcontrol/v1beta2"
	cache "k8s.io/client-go/tools/cache"
)

type scopedFlowcontrol struct {
	clusterName            logicalcluster.Name
	delegate               flowcontrol.Interface
	delegatingEventHandler *util.DelegatingEventHandler
}

var _ flowcontrol.Interface = &scopedFlowcontrol{}

// V1alpha1 implements flowcontrol.Interface
func (*scopedFlowcontrol) V1alpha1() v1alpha1.Interface {
	panic("unimplemented")
}

// V1beta1 implements flowcontrol.Interface
func (*scopedFlowcontrol) V1beta1() v1beta1.Interface {
	panic("unimplemented")
}

// V1beta2 implements flowcontrol.Interface
func (f *scopedFlowcontrol) V1beta2() v1beta2.Interface {
	return &scopedFlowcontrolV1Beta2{
		clusterName:            f.clusterName,
		delegate:               f.delegate.V1beta2(),
		delegatingEventHandler: f.delegatingEventHandler,
	}
}

type scopedFlowcontrolV1Beta2 struct {
	clusterName            logicalcluster.Name
	delegate               v1beta2.Interface
	delegatingEventHandler *util.DelegatingEventHandler
}

var _ v1beta2.Interface = &scopedFlowcontrolV1Beta2{}

// FlowSchemas implements v1beta2.Interface
func (f *scopedFlowcontrolV1Beta2) FlowSchemas() v1beta2.FlowSchemaInformer {
	return &scopedFlowSchemaInformer{
		clusterName:            f.clusterName,
		delegate:               f.delegate.FlowSchemas(),
		delegatingEventHandler: f.delegatingEventHandler,
	}
}

// PriorityLevelConfigurations implements v1beta2.Interface
func (f *scopedFlowcontrolV1Beta2) PriorityLevelConfigurations() v1beta2.PriorityLevelConfigurationInformer {
	return &scopedPriorityLevelConfigurationInformer{
		clusterName:            f.clusterName,
		delegate:               f.delegate.PriorityLevelConfigurations(),
		delegatingEventHandler: f.delegatingEventHandler,
	}
}

type scopedFlowSchemaInformer struct {
	clusterName            logicalcluster.Name
	delegate               v1beta2.FlowSchemaInformer
	delegatingEventHandler *util.DelegatingEventHandler
}
type scopedPriorityLevelConfigurationInformer struct {
	clusterName            logicalcluster.Name
	delegate               v1beta2.PriorityLevelConfigurationInformer
	delegatingEventHandler *util.DelegatingEventHandler
}

var _ v1beta2.FlowSchemaInformer = &scopedFlowSchemaInformer{}
var _ v1beta2.PriorityLevelConfigurationInformer = &scopedPriorityLevelConfigurationInformer{}

// Informer implements v1beta2.FlowSchemaInformer
func (s *scopedFlowSchemaInformer) Informer() cache.SharedIndexInformer {
	return &util.DelegatingInformer{
		ClusterName:            s.clusterName,
		Resource:               apiflowcontrolv1beta2.Resource("flowschemas"),
		SharedIndexInformer:    s.delegate.Informer(), // this informer is checked through sharedInformerFactory logic, i.e. single instance will be shared
		DelegatingEventHandler: s.delegatingEventHandler,
	}
}

// Lister implements v1beta2.FlowSchemaInformer
func (s *scopedFlowSchemaInformer) Lister() flowcontrollisterv1beta2.FlowSchemaLister {
	return &SingleClusterFlowSchemaLister{
		indexer:     s.delegate.Informer().GetIndexer(),
		clusterName: s.clusterName,
	}
}

// Informer implements v1beta2.PriorityLevelConfigurationInformer
func (s *scopedPriorityLevelConfigurationInformer) Informer() cache.SharedIndexInformer {
	return &util.DelegatingInformer{
		ClusterName:            s.clusterName,
		Resource:               apiflowcontrolv1beta2.Resource("prioritylevelconfigurations"),
		SharedIndexInformer:    s.delegate.Informer(), // this informer is checked through sharedInformerFactory logic, i.e. single instance will be shared
		DelegatingEventHandler: s.delegatingEventHandler,
	}
}

// Lister implements v1beta2.PriorityLevelConfigurationInformer
func (s *scopedPriorityLevelConfigurationInformer) Lister() flowcontrollisterv1beta2.PriorityLevelConfigurationLister {
	return &SingleClusterPriorityLevelConfigurationLister{
		indexer:     s.delegate.Informer().GetIndexer(),
		clusterName: s.clusterName,
	}
}

type SingleClusterFlowSchemaLister struct {
	indexer     cache.Indexer
	clusterName logicalcluster.Name
}

var _ flowcontrollisterv1beta2.FlowSchemaLister = &SingleClusterFlowSchemaLister{}

// Get implements v1beta2.FlowSchemaLister
func (s *SingleClusterFlowSchemaLister) Get(name string) (*apiflowcontrolv1beta2.FlowSchema, error) {
	key := client.ToClusterAwareKey(s.clusterName, name) // FIXME: not sure if this is the right way
	obj, exists, err := s.indexer.GetByKey(key)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, errors.NewNotFound(apiflowcontrolv1beta2.Resource("flowschema"), name)
	}
	return obj.(*apiflowcontrolv1beta2.FlowSchema), nil
}

// List implements v1beta2.FlowSchemaLister
func (s *SingleClusterFlowSchemaLister) List(selector labels.Selector) (ret []*apiflowcontrolv1beta2.FlowSchema, err error) {
	if err := util.ListByIndex(s.indexer, indexers.ByLogicalCluster, s.clusterName.String(), selector, func(obj interface{}) {
		ret = append(ret, obj.(*apiflowcontrolv1beta2.FlowSchema))
	}); err != nil {
		return nil, err
	}

	return ret, nil
}

type SingleClusterPriorityLevelConfigurationLister struct {
	indexer     cache.Indexer
	clusterName logicalcluster.Name
}

var _ flowcontrollisterv1beta2.PriorityLevelConfigurationLister = &SingleClusterPriorityLevelConfigurationLister{}

// Get implements v1beta2.PriorityLevelConfigurationLister
func (s *SingleClusterPriorityLevelConfigurationLister) Get(name string) (*apiflowcontrolv1beta2.PriorityLevelConfiguration, error) {
	key := client.ToClusterAwareKey(s.clusterName, name) // FIXME: not sure if this is the right way
	obj, exists, err := s.indexer.GetByKey(key)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, errors.NewNotFound(apiflowcontrolv1beta2.Resource("prioritylevelconfiguration"), name)
	}
	return obj.(*apiflowcontrolv1beta2.PriorityLevelConfiguration), nil
}

// List implements v1beta2.PriorityLevelConfigurationLister
func (s *SingleClusterPriorityLevelConfigurationLister) List(selector labels.Selector) (ret []*apiflowcontrolv1beta2.PriorityLevelConfiguration, err error) {
	if err := util.ListByIndex(s.indexer, indexers.ByLogicalCluster, s.clusterName.String(), selector, func(obj interface{}) {
		ret = append(ret, obj.(*apiflowcontrolv1beta2.PriorityLevelConfiguration))
	}); err != nil {
		return nil, err
	}

	return ret, nil
}
