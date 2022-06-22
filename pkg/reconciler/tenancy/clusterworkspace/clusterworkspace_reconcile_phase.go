/*
Copyright 2021 The KCP Authors.

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

package clusterworkspace

import (
	"context"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	tenancyv1alpha1 "github.com/kcp-dev/kcp/pkg/apis/tenancy/v1alpha1"
)

type phaseReconciler struct {
	getShardWithQuorum func(ctx context.Context, name string, options metav1.GetOptions) (*tenancyv1alpha1.ClusterWorkspaceShard, error)
}

func (r *phaseReconciler) reconcile(ctx context.Context, workspace *tenancyv1alpha1.ClusterWorkspace) (reconcileStatus, error) {
	switch workspace.Status.Phase {
	case "":
		workspace.Status.Phase = tenancyv1alpha1.ClusterWorkspacePhaseScheduling
	case tenancyv1alpha1.ClusterWorkspacePhaseScheduling:
		// TODO(sttts): in the future this step is done by a workspace shard itself. I.e. moving to initializing is a step
		//              of acceptance of the workspace on that shard.
		if workspace.Status.Location.Current != "" && workspace.Status.BaseURL != "" {
			// do final quorum read to avoid race when the workspace shard is being deleted
			_, err := r.getShardWithQuorum(ctx, workspace.Status.Location.Current, metav1.GetOptions{})
			if err != nil {
				// reschedule
				workspace.Status.Location.Current = ""
				workspace.Status.BaseURL = ""
				return reconcileStatusContinue, nil // nolint:nilerr
			}

			workspace.Status.Phase = tenancyv1alpha1.ClusterWorkspacePhaseInitializing
		}
	case tenancyv1alpha1.ClusterWorkspacePhaseInitializing:
		if len(workspace.Status.Initializers) == 0 {
			workspace.Status.Phase = tenancyv1alpha1.ClusterWorkspacePhaseReady
		}
	}

	return reconcileStatusContinue, nil
}