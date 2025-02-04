/*
Copyright 2022 The KCP Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package rootcompute

import (
	"context"
	"embed"

	kcpapiextensionsclientset "github.com/kcp-dev/apiextensions-apiserver/pkg/client/clientset/versioned"
	kcpdynamic "github.com/kcp-dev/client-go/dynamic"
	"github.com/kcp-dev/logicalcluster/v2"

	"k8s.io/apimachinery/pkg/util/sets"

	confighelpers "github.com/kcp-dev/kcp/config/helpers"
	kube124 "github.com/kcp-dev/kcp/config/rootcompute/kube-1.24"
	tenancyv1alpha1 "github.com/kcp-dev/kcp/pkg/apis/tenancy/v1alpha1"
)

//go:embed *.yaml
var fs embed.FS

// RootComuteWorkspace is the workspace to host common kubernetes APIs
var RootComputeWorkspace = logicalcluster.New("root:compute")

// Bootstrap creates resources in this package by continuously retrying the list.
// This is blocking, i.e. it only returns (with error) when the context is closed or with nil when
// the bootstrapping is successfully completed.
func Bootstrap(ctx context.Context, apiExtensionClusterClient kcpapiextensionsclientset.ClusterInterface, dynamicClusterClient kcpdynamic.ClusterInterface, batteriesIncluded sets.String) error {
	rootDiscoveryClient := apiExtensionClusterClient.Cluster(tenancyv1alpha1.RootCluster).Discovery()
	rootDynamicClient := dynamicClusterClient.Cluster(tenancyv1alpha1.RootCluster)
	if err := confighelpers.Bootstrap(ctx, rootDiscoveryClient, rootDynamicClient, batteriesIncluded, fs); err != nil {
		return err
	}

	computeDiscoveryClient := apiExtensionClusterClient.Cluster(RootComputeWorkspace).Discovery()
	computeDynamicClient := dynamicClusterClient.Cluster(RootComputeWorkspace)

	return kube124.Bootstrap(ctx, computeDiscoveryClient, computeDynamicClient, batteriesIncluded)
}
