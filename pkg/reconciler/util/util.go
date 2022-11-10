package util

import (
	"github.com/kcp-dev/kcp/pkg/client"
	"github.com/kcp-dev/logicalcluster/v2"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/tools/cache"
)

func ClusterNameForObj(obj interface{}) logicalcluster.Name {
	key, err := cache.DeletionHandlingMetaNamespaceKeyFunc(obj)
	if err != nil {
		utilruntime.HandleError(err)
		return logicalcluster.Name{}
	}

	_, clusterAndName, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		utilruntime.HandleError(err)
		return logicalcluster.Name{}
	}

	cluster, _ := client.SplitClusterAwareKey(clusterAndName)
	return cluster
}
