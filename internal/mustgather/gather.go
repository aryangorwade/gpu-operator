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

// Package mustgather collects the diagnostics produced by hack/must-gather.sh
// without requiring a shell or Kubernetes CLI in the collector image.
package mustgather

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"slices"
	"strings"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/cli-runtime/pkg/printers"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/discovery/cached/memory"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/restmapper"
	"k8s.io/kubectl/pkg/describe"
)

// Options controls where diagnostics are written and which installation is collected.
type Options struct {
	Directory string
	Namespace string
	Version   string
	Stdout    io.Writer
	Stderr    io.Writer
}

type collector struct {
	Options
	client    kubernetes.Interface
	dynamic   dynamic.Interface
	mapper    meta.RESTMapper
	rest      rest.Interface
	root      *os.Root
	exec      func(context.Context, *corev1.Pod, []string, io.Writer, io.Writer) error
	fileError error
}

// Run gathers diagnostics using in-cluster credentials or a caller-supplied kubeconfig.
// Missing optional APIs and individual collection failures are logged, not fatal:
// a partial bundle is often most useful when the cluster is unhealthy.
func Run(ctx context.Context, config *rest.Config, opts Options) error {
	config = rest.CopyConfig(config)
	// kubectl's describers predate context-aware APIs. Bind their requests, too,
	// to the collection deadline rather than leaving them running after cancellation.
	config.Wrap(func(rt http.RoundTripper) http.RoundTripper { return &contextTransport{ctx: ctx, base: rt} })
	client, err := kubernetes.NewForConfig(config)
	if err != nil {
		return err
	}
	dynamicClient, err := dynamic.NewForConfig(config)
	if err != nil {
		return err
	}
	discoveryClient, err := discovery.NewDiscoveryClientForConfig(config)
	if err != nil {
		return err
	}
	g := &collector{
		Options: opts, client: client, dynamic: dynamicClient, rest: client.CoreV1().RESTClient(),
		mapper: restmapper.NewDeferredDiscoveryRESTMapper(memory.NewMemCacheClient(discoveryClient)),
	}
	g.exec = podExecutor(config, g.rest)
	return g.run(ctx)
}

type contextTransport struct {
	ctx  context.Context
	base http.RoundTripper
}

func (t *contextTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	ctx, cancel := context.WithCancel(req.Context())
	//nolint:contextcheck // Merge collection cancellation with the request's own context.
	stop := context.AfterFunc(t.ctx, cancel)
	// Keep cancellation active until the response body is consumed, not just
	// until headers arrive (notably for streaming container logs).
	resp, err := t.base.RoundTrip(req.Clone(ctx))
	if err != nil {
		stop()
		cancel()
		return nil, err
	}
	resp.Body = &cancelBody{ReadCloser: resp.Body, cancel: cancel, stop: stop}
	return resp, nil
}

type cancelBody struct {
	io.ReadCloser
	cancel context.CancelFunc
	stop   func() bool
}

func (b *cancelBody) Close() error {
	defer b.cancel()
	b.stop()
	return b.ReadCloser.Close()
}

func (g *collector) run(ctx context.Context) error {
	if g.Directory == "" {
		return errors.New("artifact directory must not be empty")
	}
	if err := os.MkdirAll(g.Directory, 0o750); err != nil {
		return fmt.Errorf("create artifact directory: %w", err)
	}
	root, err := os.OpenRoot(g.Directory)
	if err != nil {
		return err
	}
	g.root = root
	defer root.Close()
	stdout, err := root.Create("must-gather.log")
	if err != nil {
		return err
	}
	defer stdout.Close()
	stderr, err := root.Create("must-gather.stderr.log")
	if err != nil {
		return err
	}
	defer stderr.Close()
	if g.Stdout == nil {
		g.Stdout = os.Stdout
	}
	if g.Stderr == nil {
		g.Stderr = os.Stderr
	}
	g.Stdout = io.MultiWriter(g.Stdout, stdout)
	g.Stderr = io.MultiWriter(g.Stderr, stderr)
	fmt.Fprintf(g.Stdout, "Using ARTIFACT_DIR=%s\n", g.Directory)
	g.write("version", func(w io.Writer) error {
		_, err := fmt.Fprintf(w, "NVIDIA GPU Operator\n%s\n", g.Version)
		return err
	})

	openshift := g.collectResource(ctx, "config.openshift.io", "clusterversions", "version", "openshift_version.yaml", "", false)
	g.collectResource(ctx, "hco.kubevirt.io", "hyperconvergeds", "", "hyperconverged.yaml", "", false)

	operators, err := g.client.CoreV1().Pods(g.Namespace).List(ctx, metav1.ListOptions{LabelSelector: "app=gpu-operator"})
	if err != nil {
		return fmt.Errorf("find GPU Operator pods: %w", err)
	}
	if len(operators.Items) == 0 {
		return errors.New("could not find the GPU Operator pod")
	}
	g.Namespace = operators.Items[0].Namespace
	for _, pod := range operators.Items {
		if pod.Namespace != g.Namespace {
			return errors.New("found GPU Operators in multiple namespaces; specify --namespace")
		}
	}
	fmt.Fprintf(g.Stdout, "Using %q as operator namespace\n", g.Namespace)

	g.collectResource(ctx, "kubevirt.io", "kubevirts", "", "kubevirt.yaml", "", false)
	if !g.collectResource(ctx, "nvidia.com", "clusterpolicies", "", "cluster_policy.yaml", "", true) {
		g.write("cluster_policy.missing", func(io.Writer) error { return nil })
	}
	g.collectResource(ctx, "nvidia.com", "nvidiadrivers", "", "nvidiadrivers.yaml", "", false)
	g.collectResource(ctx, "nvidia.com", "gpuclusters", "", "gpu_cluster.yaml", "", true)
	g.collectResource(ctx, "resource.k8s.io", "deviceclasses", "", "deviceclasses.yaml", "", false)
	for _, driver := range []string{"gpu.nvidia.com", "compute-domain.nvidia.com"} {
		g.collectResource(ctx, "resource.k8s.io", "resourceslices", "", "resourceslices-"+driver+".yaml", "spec.driver="+driver, false)
	}
	if openshift {
		gvr, err := g.mapper.ResourceFor(schema.GroupVersionResource{Group: "machine.openshift.io", Resource: "machines"})
		if err != nil {
			g.report("find machines API", err)
		} else {
			g.table(ctx, "all_machines.list", "/apis/"+gvr.Group+"/"+gvr.Version+"/machines", "", true, false)
		}
	}
	g.collectNodes(ctx)
	pods := g.collectPods(ctx, operators)
	g.collectDaemonSets(ctx)
	for i := range pods {
		if isDriverPod(&pods[i]) {
			g.collectDriver(ctx, &pods[i])
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	fmt.Fprintf(g.Stdout, "All done! Logs saved into %s.\n", g.Directory)
	return g.fileError
}

func (g *collector) report(action string, err error) {
	fmt.Fprintf(g.Stderr, "%s: %v\n", action, err)
}

func (g *collector) write(name string, collect func(io.Writer) error) {
	fmt.Fprintf(g.Stdout, "Collecting %s\n", name)
	f, err := g.root.Create(name)
	if err != nil {
		g.fileError = errors.Join(g.fileError, err)
		g.report(name, err)
		return
	}
	if err := collect(f); err != nil {
		g.report(name, err)
	}
	if err := f.Close(); err != nil {
		g.fileError = errors.Join(g.fileError, err)
		g.report(name, err)
	}
}

func (g *collector) yaml(name string, obj runtime.Object) {
	g.write(name, func(w io.Writer) error {
		p := printers.OmitManagedFieldsPrinter{Delegate: &printers.YAMLPrinter{}}
		return p.PrintObj(obj, w)
	})
}

func (g *collector) collectResource(ctx context.Context, group, resource, name, filename, selector string, single bool) bool {
	gvr, err := g.mapper.ResourceFor(schema.GroupVersionResource{Group: group, Resource: resource})
	if err != nil {
		if !meta.IsNoMatchError(err) {
			g.report(resource, err)
		}
		return false
	}
	if name != "" {
		obj, err := g.dynamic.Resource(gvr).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			if !apierrors.IsNotFound(err) {
				g.report(resource, err)
			}
			return false
		}
		g.yaml(filename, obj)
		return true
	}
	list, err := g.dynamic.Resource(gvr).List(ctx, metav1.ListOptions{FieldSelector: selector})
	if err != nil {
		if !apierrors.IsNotFound(err) {
			g.report(resource, err)
		}
		return false
	}
	if len(list.Items) == 0 {
		return false
	}
	if single && len(list.Items) == 1 {
		g.yaml(filename, &list.Items[0])
	} else {
		list.SetGroupVersionKind(schema.GroupVersionKind{Version: "v1", Kind: "List"})
		g.yaml(filename, list)
	}
	return true
}

// Request server-side tables, as kubectl does, so CRD print columns and wide
// status columns stay consistent with the cluster's Kubernetes version.
func (g *collector) table(ctx context.Context, filename, path, selector string, namespace, wide bool) {
	g.write(filename, func(w io.Writer) error {
		data, err := g.rest.Get().AbsPath(path).
			SetHeader("Accept", "application/json;as=Table;v=v1;g=meta.k8s.io").
			Param("labelSelector", selector).DoRaw(ctx)
		if err != nil {
			return err
		}
		var table metav1.Table
		if err := json.Unmarshal(data, &table); err != nil {
			return err
		}
		if table.Kind != "Table" {
			return errors.New("API server did not return a Table")
		}
		if len(table.Rows) == 0 {
			return nil
		}
		for i := range table.Rows {
			if len(table.Rows[i].Object.Raw) == 0 {
				continue
			}
			var metadata metav1.PartialObjectMetadata
			if err := json.Unmarshal(table.Rows[i].Object.Raw, &metadata); err != nil {
				return err
			}
			table.Rows[i].Object.Object = &metadata
		}
		return printers.NewTablePrinter(printers.PrintOptions{Wide: wide, WithNamespace: namespace}).PrintObj(&table, w)
	})
}

func (g *collector) collectNodes(ctx context.Context) {
	nodes := map[string]corev1.Node{}
	for _, label := range []string{
		"feature.node.kubernetes.io/pci-10de.present",
		"feature.node.kubernetes.io/pci-0302_10de.present",
		"feature.node.kubernetes.io/pci-0300_10de.present",
	} {
		list, err := g.client.CoreV1().Nodes().List(ctx, metav1.ListOptions{LabelSelector: label})
		if err != nil {
			g.report("list PCI nodes", err)
			continue
		}
		for _, node := range list.Items {
			nodes[node.Name] = node
		}
	}
	if len(nodes) > 0 {
		g.write("gpu_nodes.labels", func(w io.Writer) error {
			names := make([]string, 0, len(nodes))
			for name := range nodes {
				names = append(names, name)
			}
			slices.Sort(names)
			for _, name := range names {
				fmt.Fprintln(w, name)
				labels := make([]string, 0, len(nodes[name].Labels))
				for key, value := range nodes[name].Labels {
					labels = append(labels, key+"="+value)
				}
				slices.Sort(labels)
				for _, label := range labels {
					fmt.Fprintf(w, "- %s\n", label)
				}
				fmt.Fprintln(w)
			}
			return nil
		})
	}
	g.table(ctx, "gpu_nodes.status", "/api/v1/nodes", "nvidia.com/gpu.present=true", false, true)
	g.write("gpu_nodes.descr", func(w io.Writer) error {
		list, err := g.client.CoreV1().Nodes().List(ctx, metav1.ListOptions{LabelSelector: "nvidia.com/gpu.present=true"})
		if err != nil {
			return err
		}
		d := &describe.NodeDescriber{Interface: g.client}
		for _, node := range list.Items {
			g.description(w, d, "", node.Name)
		}
		return nil
	})
}

func (g *collector) description(w io.Writer, d describe.ResourceDescriber, namespace, name string) {
	text, err := d.Describe(namespace, name, describe.DescriberSettings{ShowEvents: true, ChunkSize: 500})
	if err != nil {
		g.report("describe "+name, err)
		return
	}
	fmt.Fprintln(w, text)
}

func gpuLabels(labels map[string]string) bool {
	for key, value := range labels {
		if strings.Contains(key, "nvidia") || strings.Contains(key, "gpu") || strings.Contains(value, "nvidia") || strings.Contains(value, "gpu") {
			return true
		}
	}
	return false
}

func podObject(pods *corev1.PodList) runtime.Object {
	if len(pods.Items) == 1 {
		pod := pods.Items[0].DeepCopy()
		pod.SetGroupVersionKind(corev1.SchemeGroupVersion.WithKind("Pod"))
		return pod
	}
	return pods
}

func (g *collector) collectPods(ctx context.Context, operators *corev1.PodList) []corev1.Pod {
	path := "/api/v1/namespaces/" + g.Namespace + "/pods"
	g.table(ctx, "gpu_operator_pod.status", path, "app=gpu-operator", false, true)
	operators.SetGroupVersionKind(corev1.SchemeGroupVersion.WithKind("PodList"))
	g.yaml("gpu_operator_pod.yaml", podObject(operators))
	for _, previous := range []bool{false, true} {
		filename := "gpu_operator_pod.log"
		if previous {
			filename = "gpu_operator_pod.previous.log"
		}
		g.write(filename, func(w io.Writer) error {
			for i := range operators.Items {
				g.logs(ctx, w, &operators.Items[i], previous, false)
			}
			return nil
		})
	}
	g.table(ctx, "gpu_operand_pods.status", path, "", false, true)
	pods, err := g.client.CoreV1().Pods(g.Namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		g.report("list operand pods", err)
		return nil
	}
	pods.SetGroupVersionKind(corev1.SchemeGroupVersion.WithKind("PodList"))
	g.yaml("gpu_operand_pods.yaml", pods)
	g.write("gpu_operand_pod_images.txt", func(w io.Writer) error {
		for _, pod := range pods.Items {
			fmt.Fprintf(w, "\n%s:\t", pod.Name)
			for _, container := range pod.Spec.Containers {
				fmt.Fprintf(w, "%s ", container.Image)
			}
		}
		return nil
	})
	for i := range pods.Items {
		pod := &pods.Items[i]
		if !gpuLabels(pod.Labels) || pod.Labels["app"] == "gpu-operator" {
			continue
		}
		prefix := "gpu_operand_pod_" + pod.Name
		g.write(prefix+".log", func(w io.Writer) error { g.logs(ctx, w, pod, false, true); return nil })
		g.write(prefix+".previous.log", func(w io.Writer) error { g.logs(ctx, w, pod, true, true); return nil })
		g.write(prefix+".descr", func(w io.Writer) error {
			g.description(w, &describe.PodDescriber{Interface: g.client}, g.Namespace, pod.Name)
			return nil
		})
	}
	return pods.Items
}

func (g *collector) collectDaemonSets(ctx context.Context) {
	g.table(ctx, "gpu_operand_ds.status", "/apis/apps/v1/namespaces/"+g.Namespace+"/daemonsets", "", false, false)
	list, err := g.client.AppsV1().DaemonSets(g.Namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		g.report("list daemonsets", err)
		return
	}
	list.SetGroupVersionKind(schema.GroupVersionKind{Group: "apps", Version: "v1", Kind: "DaemonSetList"})
	g.yaml("gpu_operand_ds.yaml", list)
	for _, ds := range list.Items {
		if !gpuLabels(ds.Labels) {
			continue
		}
		g.write("gpu_operand_ds_"+ds.Name+".descr", func(w io.Writer) error {
			g.description(w, &describe.DaemonSetDescriber{Interface: g.client}, g.Namespace, ds.Name)
			return nil
		})
	}
}
