/**
# Copyright (c) NVIDIA CORPORATION.  All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
**/

package mustgather

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	kubernetesfake "k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	restfake "k8s.io/client-go/rest/fake"
	clienttesting "k8s.io/client-go/testing"
	"sigs.k8s.io/yaml"
)

const namespace = "gpu-operator"

func testPod(name string, labels map[string]string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace, Labels: labels},
		Spec: corev1.PodSpec{NodeName: "worker", Containers: []corev1.Container{
			{Name: "main", Image: "nvidia/test:latest"},
		}},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}
}

func testCollector(t *testing.T, objects ...runtime.Object) *collector {
	t.Helper()
	client := kubernetesfake.NewClientset(objects...)
	client.PrependReactor("get", "pods", func(action clienttesting.Action) (bool, runtime.Object, error) {
		if action.GetSubresource() != "log" {
			return false, nil, nil
		}
		opts := action.(clienttesting.GenericAction).GetValue().(*corev1.PodLogOptions)
		if opts.Previous {
			return true, nil, apierrors.NewBadRequest("previous terminated container not found")
		}
		return true, &runtime.Unknown{Raw: []byte("2026-11-16T10:30:00Z " + opts.Container + " log\n")}, nil
	})
	tableData := `{"apiVersion":"meta.k8s.io/v1","kind":"Table","columnDefinitions":[{"name":"Name","type":"string","format":"name"},{"name":"Status","type":"string"},{"name":"Node","type":"string","priority":1}],"rows":[{"cells":["worker","Running","node-wide"],"object":{"apiVersion":"v1","kind":"Node","metadata":{"name":"worker","namespace":"gpu-operator"}}}]}`
	return &collector{
		Options: Options{Directory: t.TempDir(), Version: "test-version", Stdout: io.Discard, Stderr: &bytes.Buffer{}},
		client:  client,
		mapper:  meta.NewDefaultRESTMapper(nil),
		rest: &restfake.RESTClient{
			NegotiatedSerializer: scheme.Codecs.WithoutConversion(),
			Client: restfake.CreateHTTPClient(func(req *http.Request) (*http.Response, error) {
				require.Equal(t, "application/json;as=Table;v=v1;g=meta.k8s.io", req.Header.Get("Accept"))
				return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": {"application/json"}}, Body: io.NopCloser(strings.NewReader(tableData))}, nil
			}),
		},
		exec: func(_ context.Context, _ *corev1.Pod, command []string, stdout, _ io.Writer) error {
			if command[0] == "cat" {
				_, err := stdout.Write([]byte{0x1f, 0x8b, 0x00, 0xff})
				return err
			}
			_, err := fmt.Fprintln(stdout, "driver diagnostics")
			return err
		},
	}
}

type resourceFixture struct {
	group, version, resource, kind, filename string
	namespaced                               bool
}

var optionalResources = []resourceFixture{
	{"config.openshift.io", "v1", "clusterversions", "ClusterVersion", "openshift_version.yaml", false},
	{"hco.kubevirt.io", "v1beta1", "hyperconvergeds", "HyperConverged", "hyperconverged.yaml", true},
	{"kubevirt.io", "v1", "kubevirts", "KubeVirt", "kubevirt.yaml", true},
	{"nvidia.com", "v1", "clusterpolicies", "ClusterPolicy", "cluster_policy.yaml", false},
	{"nvidia.com", "v1alpha1", "nvidiadrivers", "NVIDIADriver", "nvidiadrivers.yaml", false},
	{"nvidia.com", "v1alpha1", "gpuclusters", "GPUCluster", "gpu_cluster.yaml", false},
	{"resource.k8s.io", "v1beta1", "deviceclasses", "DeviceClass", "deviceclasses.yaml", false},
	{"resource.k8s.io", "v1beta1", "resourceslices", "ResourceSlice", "resourceslices-gpu.nvidia.com.yaml", false},
	{"machine.openshift.io", "v1beta1", "machines", "Machine", "all_machines.list", true},
}

func addOptionalResources(g *collector) *dynamicfake.FakeDynamicClient {
	mapper := meta.NewDefaultRESTMapper(nil)
	listKinds := map[schema.GroupVersionResource]string{}
	var objects []runtime.Object
	for _, fixture := range optionalResources {
		gv := schema.GroupVersion{Group: fixture.group, Version: fixture.version}
		scope := meta.RESTScopeRoot
		if fixture.namespaced {
			scope = meta.RESTScopeNamespace
		}
		mapper.AddSpecific(gv.WithKind(fixture.kind), gv.WithResource(fixture.resource), gv.WithResource(strings.ToLower(fixture.kind)), scope)
		listKinds[gv.WithResource(fixture.resource)] = fixture.kind + "List"
		obj := &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": gv.String(), "kind": fixture.kind,
			"metadata": map[string]any{"name": "version"},
			"spec":     map[string]any{"driver": "gpu.nvidia.com"},
		}}
		if fixture.namespaced {
			obj.SetNamespace("virtualization")
		}
		objects = append(objects, obj)
	}
	client := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), listKinds, objects...)
	g.mapper = mapper
	g.dynamic = client
	return client
}

func readArtifact(t *testing.T, g *collector, name string) string {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(g.Directory, name))
	require.NoError(t, err)
	return string(data)
}

func TestCollectScriptArtifacts(t *testing.T) {
	operator := testPod("operator", map[string]string{"app": "gpu-operator"})
	driver := testPod("driver", map[string]string{
		"openshift.driver-toolkit": "true", "app.kubernetes.io/component": "nvidia-driver", "app": "nvidia-vgpu-manager-daemonset",
	})
	plugin := testPod("plugin", map[string]string{"app": "nvidia-device-plugin"})
	plugin.Spec.InitContainers = []corev1.Container{{Name: "init"}}
	plugin.Spec.Containers = append(plugin.Spec.Containers, corev1.Container{Name: "sidecar", Image: "sidecar:latest"})
	plugin.Spec.EphemeralContainers = []corev1.EphemeralContainer{{EphemeralContainerCommon: corev1.EphemeralContainerCommon{Name: "debug"}}}
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "worker", Labels: map[string]string{
		"feature.node.kubernetes.io/pci-10de.present": "true", "feature.node.kubernetes.io/pci-0300_10de.present": "true", "nvidia.com/gpu.present": "true",
	}}}
	ds := &appsv1.DaemonSet{ObjectMeta: metav1.ObjectMeta{Name: "driver", Namespace: namespace, Labels: driver.Labels}, Spec: appsv1.DaemonSetSpec{
		Selector: &metav1.LabelSelector{MatchLabels: driver.Labels}, Template: corev1.PodTemplateSpec{Spec: driver.Spec},
	}}
	g := testCollector(t, operator, driver, plugin, node, ds, testPod("unrelated", map[string]string{"app": "other"}))
	dynamicClient := addOptionalResources(g)
	var executed [][]string
	exec := g.exec
	g.exec = func(ctx context.Context, pod *corev1.Pod, cmd []string, stdout, stderr io.Writer) error {
		executed = append(executed, cmd)
		return exec(ctx, pod, cmd, stdout, stderr)
	}
	require.NoError(t, g.run(t.Context()))
	expected := []string{
		"must-gather.log", "must-gather.stderr.log", "version",
		"gpu_nodes.labels", "gpu_nodes.status", "gpu_nodes.descr",
		"gpu_operator_pod.status", "gpu_operator_pod.yaml", "gpu_operator_pod.log", "gpu_operator_pod.previous.log",
		"gpu_operand_pods.status", "gpu_operand_pods.yaml", "gpu_operand_pod_images.txt",
		"gpu_operand_pod_driver.log", "gpu_operand_pod_driver.previous.log", "gpu_operand_pod_driver.descr",
		"gpu_operand_pod_plugin.log", "gpu_operand_pod_plugin.previous.log", "gpu_operand_pod_plugin.descr",
		"gpu_operand_ds.status", "gpu_operand_ds.yaml", "gpu_operand_ds_driver.descr",
		"nvidia-bug-report_worker.log.gz", "gpu_device_usage_worker.log",
		"resourceslices-compute-domain.nvidia.com.yaml",
	}
	for _, fixture := range optionalResources {
		expected = append(expected, fixture.filename)
	}
	entries, err := os.ReadDir(g.Directory)
	require.NoError(t, err)
	var names []string
	for _, entry := range entries {
		names = append(names, entry.Name())
	}
	require.ElementsMatch(t, expected, names)
	require.Equal(t, "NVIDIA GPU Operator\ntest-version\n", readArtifact(t, g, "version"))
	require.Equal(t, 1, strings.Count(readArtifact(t, g, "gpu_nodes.labels"), "worker\n"))
	require.Contains(t, readArtifact(t, g, "gpu_nodes.descr"), "Allocated resources:")
	require.Contains(t, readArtifact(t, g, "gpu_operand_pod_driver.descr"), "Events:")
	require.Contains(t, readArtifact(t, g, "gpu_operand_ds_driver.descr"), "Desired Number of Nodes Scheduled:")
	require.Contains(t, readArtifact(t, g, "gpu_operator_pod.log"), "main log")
	require.Empty(t, readArtifact(t, g, "gpu_operator_pod.previous.log"))
	for _, container := range []string{"init", "main", "sidecar", "debug"} {
		require.Contains(t, readArtifact(t, g, "gpu_operand_pod_plugin.log"), "[pod/plugin/"+container+"] ")
	}
	require.Contains(t, readArtifact(t, g, "gpu_operand_pod_images.txt"), "sidecar:latest")
	require.Equal(t, string([]byte{0x1f, 0x8b, 0x00, 0xff}), readArtifact(t, g, "nvidia-bug-report_worker.log.gz"))
	require.Len(t, executed, 7, "driver selectors must be deduplicated")
	require.Equal(t, []string{"cat", "/tmp/nvidia-bug-report.log.gz"}, executed[1])
	for _, command := range []string{"nvidia-smi", "lsof", "ps -p", "fuser", "ls -la"} {
		require.Contains(t, fmt.Sprint(executed), command)
	}
	for _, filename := range []string{"gpu_operator_pod.yaml", "gpu_operand_pods.yaml", "gpu_operand_ds.yaml", "cluster_policy.yaml", "gpu_cluster.yaml", "deviceclasses.yaml"} {
		var obj map[string]any
		require.NoError(t, yaml.Unmarshal([]byte(readArtifact(t, g, filename)), &obj))
		require.NotEmpty(t, obj["apiVersion"])
		require.NotEmpty(t, obj["kind"])
	}
	var selectors []string
	for _, action := range dynamicClient.Actions() {
		if action.GetVerb() == "list" && action.GetResource().Resource == "resourceslices" {
			selectors = append(selectors, action.(clienttesting.ListAction).GetListRestrictions().Fields.String())
		}
	}
	require.ElementsMatch(t, []string{"spec.driver=gpu.nvidia.com", "spec.driver=compute-domain.nvidia.com"}, selectors)
}

func TestMissingOptionalAPIsAndGPUNodes(t *testing.T) {
	g := testCollector(t, testPod("operator", map[string]string{"app": "gpu-operator"}))
	require.NoError(t, g.run(t.Context()))
	require.Empty(t, readArtifact(t, g, "cluster_policy.missing"))
	require.FileExists(t, filepath.Join(g.Directory, "gpu_operator_pod.log"))
	require.FileExists(t, filepath.Join(g.Directory, "gpu_operand_ds.yaml"))
	require.NoFileExists(t, filepath.Join(g.Directory, "openshift_version.yaml"))
	require.NoFileExists(t, filepath.Join(g.Directory, "gpu_nodes.labels"))
	require.Contains(t, readArtifact(t, g, "must-gather.stderr.log"), "previous terminated container not found")
}

func TestOperatorDiscovery(t *testing.T) {
	for name, tc := range map[string]struct {
		pods      []runtime.Object
		namespace string
		wantError string
	}{
		"missing":   {wantError: "could not find the GPU Operator pod"},
		"replicas":  {pods: []runtime.Object{testPod("operator-1", map[string]string{"app": "gpu-operator"}), testPod("operator-2", map[string]string{"app": "gpu-operator"})}},
		"ambiguous": {pods: []runtime.Object{testPod("operator", map[string]string{"app": "gpu-operator"}), &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "operator", Namespace: "another", Labels: map[string]string{"app": "gpu-operator"}}}}, wantError: "specify --namespace"},
		"explicit":  {pods: []runtime.Object{testPod("operator", map[string]string{"app": "gpu-operator"}), &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "operator", Namespace: "another", Labels: map[string]string{"app": "gpu-operator"}}}}, namespace: namespace},
	} {
		t.Run(name, func(t *testing.T) {
			g := testCollector(t, tc.pods...)
			g.Namespace = tc.namespace
			err := g.run(t.Context())
			if tc.wantError != "" {
				require.ErrorContains(t, err, tc.wantError)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestCollectionFailuresAreBestEffort(t *testing.T) {
	g := testCollector(t, testPod("operator", map[string]string{"app": "gpu-operator"}), testPod("driver", map[string]string{"app.kubernetes.io/component": "nvidia-driver"}))
	client := addOptionalResources(g)
	client.PrependReactor("list", "deviceclasses", func(clienttesting.Action) (bool, runtime.Object, error) {
		return true, nil, apierrors.NewForbidden(schema.GroupResource{Group: "resource.k8s.io", Resource: "deviceclasses"}, "", errors.New("denied"))
	})
	g.exec = func(context.Context, *corev1.Pod, []string, io.Writer, io.Writer) error {
		return errors.New("exec denied")
	}
	require.NoError(t, g.run(t.Context()))
	require.FileExists(t, filepath.Join(g.Directory, "gpu_operand_ds.yaml"))
	require.NoFileExists(t, filepath.Join(g.Directory, "nvidia-bug-report_worker.log.gz"))
	require.Contains(t, readArtifact(t, g, "gpu_device_usage_worker.log"), "exec denied")
	require.Contains(t, readArtifact(t, g, "must-gather.stderr.log"), "deviceclasses")
}

func TestArtifactErrors(t *testing.T) {
	g := testCollector(t, testPod("operator", map[string]string{"app": "gpu-operator"}))
	require.NoError(t, os.Mkdir(filepath.Join(g.Directory, "gpu_operator_pod.yaml"), 0o750))
	require.Error(t, g.run(t.Context()))
	require.FileExists(t, filepath.Join(g.Directory, "gpu_operand_ds.yaml"))
	g.Directory = ""
	require.ErrorContains(t, g.run(t.Context()), "artifact directory")
}

func TestTableColumns(t *testing.T) {
	for _, wide := range []bool{false, true} {
		t.Run(fmt.Sprint(wide), func(t *testing.T) {
			g := testCollector(t)
			root, err := os.OpenRoot(g.Directory)
			require.NoError(t, err)
			defer root.Close()
			g.root = root
			g.table(t.Context(), "status", "/api/v1/nodes", "nvidia.com/gpu.present=true", true, wide)
			status := readArtifact(t, g, "status")
			require.Contains(t, status, "NAMESPACE")
			require.Contains(t, status, namespace)
			require.Contains(t, status, "Running")
			require.Equal(t, wide, strings.Contains(status, "node-wide"))
		})
	}
}

func TestContextTransportCancelsResponseBody(t *testing.T) {
	started := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.(http.Flusher).Flush()
		close(started)
		<-r.Context().Done()
	}))
	defer server.Close()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	client := &http.Client{Transport: &contextTransport{ctx: ctx, base: http.DefaultTransport}, Timeout: 5 * time.Second}
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, server.URL, nil)
	require.NoError(t, err)
	resp, err := client.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	<-started
	cancel()
	_, err = io.ReadAll(resp.Body)
	require.ErrorIs(t, err, context.Canceled)
}

func TestRunWithCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	err := Run(ctx, &rest.Config{Host: "https://127.0.0.1:1"}, Options{Directory: t.TempDir(), Stdout: io.Discard, Stderr: io.Discard})
	require.Error(t, err)
}

func TestYAMLOmitsManagedFields(t *testing.T) {
	g := testCollector(t)
	root, err := os.OpenRoot(g.Directory)
	require.NoError(t, err)
	defer root.Close()
	g.root = root
	pod := testPod("operator", nil)
	pod.ManagedFields = []metav1.ManagedFieldsEntry{{Manager: "test"}}
	pod.SetGroupVersionKind(corev1.SchemeGroupVersion.WithKind("Pod"))
	g.yaml("pod.yaml", pod)
	require.NotContains(t, readArtifact(t, g, "pod.yaml"), "managedFields")
	var obj corev1.Pod
	data, err := yaml.YAMLToJSON([]byte(readArtifact(t, g, "pod.yaml")))
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(data, &obj))
	require.Equal(t, pod.Name, obj.Name)
}

func TestRunAgainstAPIServer(t *testing.T) {
	operator := testPod("operator", map[string]string{"app": "gpu-operator"})
	operator.SetGroupVersionKind(corev1.SchemeGroupVersion.WithKind("Pod"))
	pods := &corev1.PodList{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "PodList"}, Items: []corev1.Pod{*operator}}
	responses := map[string]any{
		"/api": metav1.APIVersions{Versions: []string{"v1"}},
		"/apis": metav1.APIGroupList{Groups: []metav1.APIGroup{{
			Name: "nvidia.com", Versions: []metav1.GroupVersionForDiscovery{{GroupVersion: "nvidia.com/v1", Version: "v1"}},
			PreferredVersion: metav1.GroupVersionForDiscovery{GroupVersion: "nvidia.com/v1", Version: "v1"},
		}}},
		"/api/v1": metav1.APIResourceList{GroupVersion: "v1", APIResources: []metav1.APIResource{
			{Name: "pods", Kind: "Pod", Namespaced: true}, {Name: "nodes", Kind: "Node"},
		}},
		"/apis/nvidia.com/v1": metav1.APIResourceList{GroupVersion: "nvidia.com/v1", APIResources: []metav1.APIResource{
			{Name: "clusterpolicies", SingularName: "clusterpolicy", Kind: "ClusterPolicy"},
		}},
		"/apis/nvidia.com/v1/clusterpolicies": map[string]any{
			"apiVersion": "nvidia.com/v1", "kind": "ClusterPolicyList",
			"items": []any{map[string]any{"apiVersion": "nvidia.com/v1", "kind": "ClusterPolicy", "metadata": map[string]any{"name": "cluster-policy"}}},
		},
		"/api/v1/pods":                         pods,
		"/api/v1/namespaces/gpu-operator/pods": pods,
		"/api/v1/nodes":                        &corev1.NodeList{Items: []corev1.Node{}},

		"/apis/apps/v1/namespaces/gpu-operator/daemonsets": &appsv1.DaemonSetList{Items: []appsv1.DaemonSet{}},
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if strings.HasSuffix(req.URL.Path, "/log") {
			require.Equal(t, "true", req.URL.Query().Get("timestamps"))
			fmt.Fprintln(w, "2026-11-16T10:30:00Z operator logs")
			return
		}
		w.Header().Set("Content-Type", "application/json")
		if strings.Contains(req.Header.Get("Accept"), "as=Table") {
			require.NoError(t, json.NewEncoder(w).Encode(&metav1.Table{TypeMeta: metav1.TypeMeta{APIVersion: "meta.k8s.io/v1", Kind: "Table"}}))
			return
		}
		obj, ok := responses[req.URL.Path]
		if !ok {
			t.Errorf("unexpected API request: %s", req.URL)
			http.NotFound(w, req)
			return
		}
		require.NoError(t, json.NewEncoder(w).Encode(obj))
	}))
	defer server.Close()
	directory := t.TempDir()
	err := Run(t.Context(), &rest.Config{Host: server.URL}, Options{
		Directory: directory, Version: "test", Stdout: io.Discard, Stderr: io.Discard,
	})
	require.NoError(t, err)
	data, err := os.ReadFile(filepath.Join(directory, "cluster_policy.yaml"))
	require.NoError(t, err)
	require.Contains(t, string(data), "kind: ClusterPolicy")
	require.Contains(t, string(data), "name: cluster-policy")
	require.NoFileExists(t, filepath.Join(directory, "cluster_policy.missing"))
}
