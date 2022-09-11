package common

import (
	"github.com/kcp-dev/kcp/pkg/indexers"
	"github.com/kcp-dev/logicalcluster/v2"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clusters"
)

// scopedGenericInformer wraps an informers.GenericInformer and produces instances of cache.GenericLister that are
// scoped to a single logical cluster.
type scopedGenericInformer struct {
	delegate               informers.GenericInformer
	clusterName            logicalcluster.Name
	resource               schema.GroupResource
	delegatingEventHandler *DelegatingEventHandler
}

// Informer invokes Informer() on the underlying informers.GenericInformer.
func (s *scopedGenericInformer) Informer() cache.SharedIndexInformer {
	return &DelegatingInformer{
		ClusterName:            s.clusterName,
		SharedIndexInformer:    s.delegate.Informer(),
		DelegatingEventHandler: s.delegatingEventHandler,
	}
}

// Lister returns an implementation of cache.GenericLister that is scoped to a single logical cluster.
func (s *scopedGenericInformer) Lister() cache.GenericLister {
	return &scopedGenericLister{
		indexer:     s.delegate.Informer().GetIndexer(),
		clusterName: s.clusterName,
		resource:    s.resource,
	}
}

// scopedGenericLister wraps a cache.Indexer to implement a cache.GenericLister that is scoped to a single logical
// cluster.
type scopedGenericLister struct {
	indexer     cache.Indexer
	clusterName logicalcluster.Name
	resource    schema.GroupResource
}

// List returns all instances from the cache.Indexer scoped to a single logical cluster and matching selector.
func (s *scopedGenericLister) List(selector labels.Selector) (ret []runtime.Object, err error) {
	err = ListByIndex(s.indexer, indexers.ByLogicalCluster, s.clusterName.String(), selector, func(obj interface{}) {
		ret = append(ret, obj.(runtime.Object))
	})
	return ret, err
}

// ByNamespace returns an implementation of cache.GenericNamespaceLister that is scoped to a single logical cluster.
func (s *scopedGenericLister) ByNamespace(namespace string) cache.GenericNamespaceLister {
	return &scopedGenericNamespaceLister{
		indexer:     s.indexer,
		clusterName: s.clusterName,
		namespace:   namespace,
		resource:    s.resource,
	}
}

// Get returns the runtime.Object from the cache.Indexer identified by name, from the appropriate logical cluster.
func (s *scopedGenericLister) Get(name string) (runtime.Object, error) {
	key := clusters.ToClusterAwareKey(s.clusterName, name)
	obj, exists, err := s.indexer.GetByKey(key)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, errors.NewNotFound(s.resource, name)
	}
	return obj.(runtime.Object), nil
}

func ListByIndex(indexer cache.Indexer, indexName, indexValue string, selector labels.Selector, appendFunc func(obj interface{})) error {
	selectAll := selector == nil || selector.Empty()

	list, err := indexer.ByIndex(indexName, indexValue)
	if err != nil {
		return err
	}

	for i := range list {
		if selectAll {
			appendFunc(list[i])
			continue
		}

		metadata, err := meta.Accessor(list[i])
		if err != nil {
			return err
		}
		if selector.Matches(labels.Set(metadata.GetLabels())) {
			appendFunc(list[i])
		}
	}

	return nil
}

// scopedGenericNamespaceLister wraps a cache.Indexer to implement a cache.GenericNamespaceLister that is scoped to a
// single logical cluster.
type scopedGenericNamespaceLister struct {
	indexer     cache.Indexer
	clusterName logicalcluster.Name
	namespace   string
	resource    schema.GroupResource
}

// List lists all instances from the cache.Indexer scoped to a single logical cluster and namespace, and matching
// selector.
func (s *scopedGenericNamespaceLister) List(selector labels.Selector) (ret []runtime.Object, err error) {
	// To support e.g. quota for cluster-scoped resources, we've hacked the k8s quota to use namespace="" when
	// checking quota for cluster-scoped resources. But because all the upstream quota code is written to only
	// support namespace-scoped resources, we have to hack the "namespace lister" to support returning all items
	// when its namespace is "".
	var indexName, indexValue string
	if s.namespace == "" {
		indexName = indexers.ByLogicalCluster
		indexValue = s.clusterName.String()
	} else {
		indexName = indexers.ByLogicalClusterAndNamespace
		indexValue = clusters.ToClusterAwareKey(s.clusterName, s.namespace)
	}
	err = ListByIndex(s.indexer, indexName, indexValue, selector, func(obj interface{}) {
		ret = append(ret, obj.(runtime.Object))
	})
	return ret, err
}

// Get returns the runtime.Object from the cache.Indexer identified by name, from the appropriate logical cluster and
// namespace.
func (s *scopedGenericNamespaceLister) Get(name string) (runtime.Object, error) {
	obj, exists, err := s.indexer.GetByKey(s.namespace + "/" + name)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, errors.NewNotFound(s.resource, name)
	}
	return obj.(runtime.Object), nil
}
