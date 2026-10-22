/*
 * Copyright (c) NVIDIA CORPORATION.  All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package controllers

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/go-logr/logr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/yaml"

	upgrade_v1alpha1 "github.com/NVIDIA/k8s-operator-libs/api/upgrade/v1alpha1"
	"github.com/NVIDIA/k8s-operator-libs/pkg/upgrade"

	gpuv1 "github.com/NVIDIA/gpu-operator/api/nvidia/v1"
)

// Keep real driver discovery and metrics, but capture the state instead of
// starting upgrades. This exercises the selector against actual operand labels.
type discoveryUpgradeStateManager struct {
	upgrade.ClusterUpgradeStateManager
	appliedState *upgrade.ClusterUpgradeState
}

func (m *discoveryUpgradeStateManager) ApplyState(_ context.Context, state *upgrade.ClusterUpgradeState, _ *upgrade_v1alpha1.DriverUpgradePolicySpec) error {
	m.appliedState = state
	return nil
}

func TestUpgradeReconcileClusterPolicyDriverDiscovery(t *testing.T) {
	previousController := clusterPolicyCtrl
	t.Cleanup(func() { clusterPolicyCtrl = previousController })

	tests := map[string]struct {
		dtk         bool
		precompiled bool
	}{
		"standard driver": {},
		"driver toolkit":  {dtk: true},
		"precompiled":     {precompiled: true},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			// An operator restart leaves existing driver pods in the cluster
			// before the first ClusterPolicy reconcile initializes its globals.
			clusterPolicyCtrl = ClusterPolicyController{}
			data, err := os.ReadFile(filepath.Join(cfg.root, driverAssetsPath, "0500_daemonset.yaml"))
			require.NoError(t, err)
			ds := &appsv1.DaemonSet{}
			require.NoError(t, yaml.Unmarshal(data, ds))
			if tc.dtk {
				n := ClusterPolicyController{logger: logr.Discard()}
				n.ocpDriverToolkit.enabled = true
				n.ocpDriverToolkit.currentRhcosVersion = "416.94.202410211619-0"
				require.NoError(t, transformOpenShiftDriverToolkitContainer(ds, &gpuv1.ClusterPolicySpec{}, n, "nvidia-driver-ctr"))
			}
			if tc.precompiled {
				n := ClusterPolicyController{
					currentKernelVersion: "5.14.0-427.el9.x86_64",
					kernelVersionMap:     map[string]string{"5.14.0-427.el9.x86_64": "rhcos4.16"},
				}
				require.NoError(t, transformPrecompiledDriverDaemonset(ds, n))
			}
			ds.Namespace = testOperatorNamespace
			ds.UID = types.UID(ds.Name)
			ds.Status.DesiredNumberScheduled = 1
			node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "gpu-node"}}
			pod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name: "driver-pod", Namespace: ds.Namespace,
					Labels:          ds.Spec.Template.Labels,
					OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(ds, appsv1.SchemeGroupVersion.WithKind("DaemonSet"))},
				},
				Spec: corev1.PodSpec{NodeName: node.Name},
			}
			// A different component must not become part of the driver state.
			otherPod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name: "device-plugin", Namespace: ds.Namespace,
					Labels: map[string]string{AppComponentLabelKey: "nvidia-device-plugin"},
				},
				Spec: corev1.PodSpec{NodeName: node.Name},
			}
			cp := &gpuv1.ClusterPolicy{ObjectMeta: metav1.ObjectMeta{Name: "cluster-policy"}}
			cp.Spec.Driver.UpgradePolicy = &upgrade_v1alpha1.DriverUpgradePolicySpec{AutoUpgrade: true}
			r, _ := newTestUpgradeReconciler(t, cp, node, pod, otherPod)
			require.NoError(t, appsv1.AddToScheme(r.Scheme))
			require.NoError(t, r.Create(context.Background(), ds))
			stateManager := newDiscoveryUpgradeStateManager(r.Client)
			r.StateManager = stateManager

			result, err := r.Reconcile(context.Background(), upgradeSingletonRequest())
			require.NoError(t, err)
			assert.Equal(t, plannedRequeueInterval, result.RequeueAfter)
			require.NotNil(t, stateManager.appliedState)
			require.Equal(t, 1, stateManager.GetTotalManagedNodes(stateManager.appliedState), "the first reconcile must discover the existing driver")
			nodeState := stateManager.appliedState.NodeStates[upgrade.UpgradeStateUnknown][0]
			assert.Equal(t, node.Name, nodeState.Node.Name)
			assert.Equal(t, pod.Name, nodeState.DriverPod.Name)
			assert.Equal(t, ds.Name, nodeState.DriverDaemonSet.Name)
		})
	}
}

func newDiscoveryUpgradeStateManager(c client.Client) *discoveryUpgradeStateManager {
	return &discoveryUpgradeStateManager{
		ClusterUpgradeStateManager: &upgrade.ClusterUpgradeStateManagerImpl{
			CommonUpgradeManagerImpl: &upgrade.CommonUpgradeManagerImpl{
				Log: logr.Discard(), K8sClient: c,
				NodeUpgradeStateProvider: upgrade.NewNodeUpgradeStateProvider(c, logr.Discard(), record.NewFakeRecorder(10)),
			},
		},
	}
}

func TestUpgradeReconcileConcurrentClusterPolicyInitialization(t *testing.T) {
	previousController := clusterPolicyCtrl
	t.Cleanup(func() { clusterPolicyCtrl = previousController })
	precompiled := true
	clusterPolicyCtrl = ClusterPolicyController{
		openshift: "4.16",
		singleton: &gpuv1.ClusterPolicy{Spec: gpuv1.ClusterPolicySpec{
			Driver: gpuv1.DriverSpec{UsePrecompiled: &precompiled},
		}},
	}
	cp := &gpuv1.ClusterPolicy{ObjectMeta: metav1.ObjectMeta{Name: "cluster-policy"}}
	cp.Spec.Driver.UpgradePolicy = &upgrade_v1alpha1.DriverUpgradePolicySpec{AutoUpgrade: true}
	r, _ := newTestUpgradeReconciler(t, cp)

	// Exercise the real ClusterPolicy initialization write concurrently with
	// upgrade reconciliation. Run with -race to detect shared-state reads.
	stop, done := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(done)
		for {
			select {
			case <-stop:
				return
			default:
				if err := clusterPolicyCtrl.initOCPParams(); err != nil {
					t.Error(err)
					return
				}
			}
		}
	}()
	defer func() { close(stop); <-done }()
	for range 100 {
		_, err := r.Reconcile(context.Background(), upgradeSingletonRequest())
		require.NoError(t, err)
	}
}
