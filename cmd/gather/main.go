/*
Copyright (c), NVIDIA CORPORATION.  All rights reserved.

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

// Command gather is the Go version of hack/must-gather.sh. The operator image
// has it at /usr/bin/gather, which is the path that `oc adm must-gather` starts.
package main

import (
	"bufio"
	"cmp"
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/cli-runtime/pkg/printers"
	"k8s.io/client-go/discovery/cached/memory"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/restmapper"
	"k8s.io/client-go/tools/remotecommand"
	"k8s.io/kubectl/pkg/describe"
	ctrlconfig "sigs.k8s.io/controller-runtime/pkg/client/config"

	"github.com/NVIDIA/gpu-operator/internal/info"
)

const devices = "/run/nvidia/driver/dev/nvidia*"

var (
	wide = printers.PrintOptions{Wide: true}
	// The script selects NVIDIA objects with `grep` on the labels. These expressions do the same.
	nvidia  = regexp.MustCompile(`nvidia|gpu`)
	pciNode = regexp.MustCompile(`feature\.node\.kubernetes\.io/pci-(030[02]_)?10de\.present`)
)

type opts = metav1.ListOptions

// gatherer writes files into dir. A step that fails does not stop the subsequent steps.
type gatherer struct {
	dir     string
	errLog  io.Writer
	cfg     *rest.Config
	cs      *kubernetes.Clientset
	objects dynamic.Interface // returns objects
	tables  dynamic.Interface // returns the tables that `kubectl get` shows
	mapper  *restmapper.DeferredDiscoveryRESTMapper
}

func main() {
	flag.Parse() // controller-runtime adds the --kubeconfig flag
	dir := filepath.Clean(cmp.Or(os.Getenv("ARTIFACT_DIR"), "/tmp/nvidia-gpu-operator_"+time.Now().Format("20060102_1504")))
	plugin := os.Args[0] == "/usr/bin/gather" // `oc adm must-gather` copies /must-gather from the pod
	if plugin {
		dir = "/must-gather"
	}
	cfg, err := ctrlconfig.GetConfig()
	if err != nil {
		log.Fatal(err)
	}
	tableCfg := dynamic.ConfigFor(cfg)
	tableCfg.AcceptContentTypes = "application/json;as=Table;v=v1;g=meta.k8s.io"
	tables, err := rest.UnversionedRESTClientFor(tableCfg)
	if err == nil {
		err = os.MkdirAll(dir, 0o755)
	}
	if err != nil {
		log.Fatal(err)
	}
	g := &gatherer{dir: dir, errLog: os.Stderr, cfg: cfg, cs: kubernetes.NewForConfigOrDie(cfg),
		objects: dynamic.NewForConfigOrDie(cfg), tables: dynamic.New(tables)}
	g.mapper = restmapper.NewDeferredDiscoveryRESTMapper(memory.NewMemCacheClient(g.cs.Discovery()))
	g.errLog = g.create("must-gather.stderr.log")
	log.SetFlags(0)
	log.SetOutput(io.MultiWriter(os.Stdout, g.create("must-gather.log")))
	ctx := context.Background()

	if plugin {
		f := g.create("version")
		fmt.Fprintf(f, "NVIDIA GPU Operator\n%s\n", info.GetVersionString())
		f.Close()
	}
	ocp := len(g.getYAML(ctx, "openshift_version.yaml", "clusterversions.config.openshift.io", "", opts{}, true)) > 0
	g.getYAML(ctx, "hyperconverged.yaml", "hyperconvergeds.hco.kubevirt.io", "", opts{}, false)

	operator := opts{LabelSelector: "app=gpu-operator"}
	operators, err := g.cs.CoreV1().Pods("").List(ctx, operator)
	g.warn(err)
	if len(operators.Items) == 0 {
		log.Fatal("FATAL: could not find the GPU Operator Pod")
	}
	ns := operators.Items[0].Namespace

	g.getYAML(ctx, "kubevirt.yaml", "kubevirts.kubevirt.io", "", opts{}, false)
	if len(g.getYAML(ctx, "cluster_policy.yaml", "clusterpolicies.nvidia.com", "", opts{}, true)) == 0 {
		g.create("cluster_policy.missing").Close()
	}
	g.getYAML(ctx, "nvidiadrivers.yaml", "nvidiadrivers.nvidia.com", "", opts{}, false)
	g.getYAML(ctx, "gpu_cluster.yaml", "gpuclusters.nvidia.com", "", opts{}, true)
	g.getYAML(ctx, "deviceclasses.yaml", "deviceclasses.resource.k8s.io", "", opts{}, false)
	for _, driver := range []string{"gpu.nvidia.com", "compute-domain.nvidia.com"} {
		g.getYAML(ctx, "resourceslices-"+driver+".yaml", "resourceslices.resource.k8s.io", "", opts{FieldSelector: "spec.driver=" + driver}, false)
	}

	if ocp {
		g.getTable(ctx, "all_machines.list", "machines.machine.openshift.io", "", opts{}, printers.PrintOptions{WithNamespace: true})
	}
	var nodeLabels strings.Builder
	for _, node := range g.list(ctx, g.objects, "nodes", "", opts{}).Items {
		if l := labels.Set(node.GetLabels()).String(); pciNode.MatchString(l) {
			fmt.Fprintf(&nodeLabels, "%s\n- %s\n\n", node.GetName(), strings.ReplaceAll(l, ",", "\n- "))
		}
	}
	if nodeLabels.Len() == 0 {
		log.Print("FATAL: could not find nodes with NVIDIA PCI labels")
		return
	}
	f := g.create("gpu_nodes.labels")
	fmt.Fprint(f, nodeLabels.String())
	f.Close()
	gpuNodes := opts{LabelSelector: "nvidia.com/gpu.present=true"}
	g.getTable(ctx, "gpu_nodes.status", "nodes", "", gpuNodes, wide)
	g.describe(ctx, "gpu_nodes.descr", "nodes", "", gpuNodes)

	g.getTable(ctx, "gpu_operator_pod.status", "pods", ns, operator, wide)
	g.getYAML(ctx, "gpu_operator_pod.yaml", "pods", ns, operator, false)
	g.getYAML(ctx, "gpu_operator_namespace_leases.yaml", "leases.coordination.k8s.io", ns, opts{}, false)
	for _, pod := range operators.Items {
		g.logs(ctx, "gpu_operator_pod_"+pod.Name+".log", &pod, false, false)
		g.logs(ctx, "gpu_operator_pod_"+pod.Name+".previous.log", &pod, true, false)
	}

	g.getTable(ctx, "gpu_operand_pods.status", "pods", ns, opts{}, wide)
	g.getYAML(ctx, "gpu_operand_pods.yaml", "pods", ns, opts{}, false)
	pods, err := g.cs.CoreV1().Pods(ns).List(ctx, opts{})
	g.warn(err)
	images := g.create("gpu_operand_pod_images.txt")
	for _, pod := range pods.Items {
		fmt.Fprintf(images, "\n%s:\t", pod.Name)
		for _, c := range pod.Spec.Containers {
			fmt.Fprint(images, c.Image, " ")
		}
		if !nvidia.MatchString(labels.Set(pod.Labels).String()) || pod.Labels["app"] == "gpu-operator" {
			continue
		}
		g.logs(ctx, "gpu_operand_pod_"+pod.Name+".log", &pod, false, true)
		g.logs(ctx, "gpu_operand_pod_"+pod.Name+".previous.log", &pod, true, true)
		g.describe(ctx, "gpu_operand_pod_"+pod.Name+".descr", "pods", ns, opts{FieldSelector: "metadata.name=" + pod.Name})
	}
	images.Close()

	g.getTable(ctx, "gpu_operand_ds.status", "daemonsets.apps", ns, opts{}, printers.PrintOptions{})
	for _, ds := range g.getYAML(ctx, "gpu_operand_ds.yaml", "daemonsets.apps", ns, opts{}, false) {
		if nvidia.MatchString(labels.Set(ds.GetLabels()).String()) {
			g.describe(ctx, "gpu_operand_ds_"+ds.GetName()+".descr", "daemonsets.apps", ns, opts{FieldSelector: "metadata.name=" + ds.GetName()})
		}
	}

	for _, pod := range pods.Items {
		l, node := pod.Labels, pod.Spec.NodeName
		if _, dtk := l["openshift.driver-toolkit"]; !dtk && l["app.kubernetes.io/component"] != "nvidia-driver" && l["app"] != "nvidia-vgpu-manager-daemonset" {
			continue
		}
		report := g.create("nvidia-bug-report_" + node + ".log.gz")
		g.exec(ctx, &pod, report, g.errLog, "bash", "-c", "cd /tmp && nvidia-bug-report.sh >&2 && cat nvidia-bug-report.log.gz")
		report.Close()

		usage := g.create("gpu_device_usage_" + node + ".log")
		for _, cmd := range [][]string{
			{"nvidia-smi"},
			onHost("lsof " + devices),
			onHost("for pid in $(lsof " + devices + " 2>/dev/null | awk 'NR>1 {print $2}' | sort -u); do ps -p $pid -o pid,ppid,user,stat,start,etime,pcpu,pmem,vsz,rss,args; done"),
			onHost("fuser -v " + devices),
			onHost("ls -la " + devices),
		} {
			fmt.Fprintf(usage, "\n# %s output from %s\n", cmd[len(cmd)-1], node)
			g.exec(ctx, &pod, usage, usage, cmd...)
		}
		usage.Close()
	}
	fmt.Println("All done! Logs saved into", dir)
}

// onHost returns the command that runs a shell command in the host namespaces from a driver pod.
func onHost(shell string) []string {
	return []string{"nsenter", "--target", "1", "--mount", "--pid", "--", "bash", "-c", shell}
}

// defaultContainer returns the container that kubectl selects if the user gives none.
func defaultContainer(pod *corev1.Pod) string {
	return cmp.Or(pod.Annotations["kubectl.kubernetes.io/default-container"], pod.Spec.Containers[0].Name)
}

// warn writes the error of a step that failed to must-gather.stderr.log.
func (g *gatherer) warn(err error) {
	if err != nil {
		fmt.Fprintln(g.errLog, err)
	}
}

// create makes a file in the artifact directory. If it fails, the nil file rejects each write, and the run continues.
func (g *gatherer) create(name string) *os.File {
	log.Print("Writing ", name)
	f, err := os.Create(filepath.Join(g.dir, name))
	g.warn(err)
	return f
}

// list mirrors `kubectl get <resource>`. It returns no items if the cluster does not have the resource.
func (g *gatherer) list(ctx context.Context, client dynamic.Interface, resource, ns string, o opts) *unstructured.UnstructuredList {
	gvr, err := g.mapper.ResourceFor(schema.ParseGroupResource(resource).WithVersion(""))
	if err == nil {
		var l *unstructured.UnstructuredList
		if l, err = client.Resource(gvr).Namespace(ns).List(ctx, o); err == nil {
			return l
		}
	}
	g.warn(err)
	return &unstructured.UnstructuredList{}
}

// getYAML mirrors `kubectl get <resource> -o yaml > file`. It returns the objects, and writes no file if there are none.
// If single is set, it writes one object without the list, as `kubectl get <resource>/<name>` does.
func (g *gatherer) getYAML(ctx context.Context, file, resource, ns string, o opts, single bool) []unstructured.Unstructured {
	l := g.list(ctx, g.objects, resource, ns, o)
	if len(l.Items) == 0 {
		log.Print(resource, " not found in the cluster")
		return nil
	}
	l.Object = map[string]any{"apiVersion": "v1", "kind": "List", "metadata": map[string]any{"resourceVersion": ""}}
	var obj runtime.Object = l
	if single && len(l.Items) == 1 {
		obj = &l.Items[0]
	}
	f := g.create(file)
	defer f.Close()
	g.warn((&printers.OmitManagedFieldsPrinter{Delegate: &printers.YAMLPrinter{}}).PrintObj(obj, f))
	return l.Items
}

// getTable mirrors `kubectl get <resource> > file`. The API server makes the table.
func (g *gatherer) getTable(ctx context.Context, file, resource, ns string, o opts, po printers.PrintOptions) {
	t := &metav1.Table{}
	g.warn(runtime.DefaultUnstructuredConverter.FromUnstructured(g.list(ctx, g.tables, resource, ns, o).Object, t))
	for i := range t.Rows { // the printer reads the namespace of a row from the decoded object
		t.Rows[i].Object.Object, _ = runtime.Decode(unstructured.UnstructuredJSONScheme, t.Rows[i].Object.Raw)
	}
	f := g.create(file)
	defer f.Close()
	g.warn(printers.NewTablePrinter(po).PrintObj(t, f))
}

// describe mirrors `kubectl describe <resource> > file`.
func (g *gatherer) describe(ctx context.Context, file, resource, ns string, o opts) {
	var out []string
	objs := g.list(ctx, g.objects, resource, ns, o).Items
	for _, obj := range objs {
		d, _ := describe.DescriberFor(obj.GroupVersionKind().GroupKind(), g.cfg)
		s, err := d.Describe(ns, obj.GetName(), describe.DescriberSettings{ShowEvents: len(objs) == 1}) // as kubectl does
		g.warn(err)
		out = append(out, s)
	}
	f := g.create(file)
	defer f.Close()
	fmt.Fprint(f, strings.Join(out, "\n\n"))
}

// logs mirrors `kubectl logs <pod> --timestamps > file`. If all is set, it adds `--all-containers --prefix`.
func (g *gatherer) logs(ctx context.Context, file string, pod *corev1.Pod, previous, all bool) {
	f := g.create(file)
	defer f.Close()
	containers, format := []corev1.Container{{Name: defaultContainer(pod)}}, "%[3]s\n" // the line only
	if all {
		containers, format = slices.Concat(pod.Spec.InitContainers, pod.Spec.Containers), "[pod/%s/%s] %s\n"
	}
	for _, c := range containers {
		stream, err := g.cs.CoreV1().Pods(pod.Namespace).GetLogs(pod.Name, &corev1.PodLogOptions{Container: c.Name, Timestamps: true, Previous: previous}).Stream(ctx)
		if err != nil {
			g.warn(err)
			continue
		}
		lines := bufio.NewScanner(stream)
		lines.Buffer(nil, 1<<20) // a log line can be longer than the default limit of 64 KiB
		for lines.Scan() {
			fmt.Fprintf(f, format, pod.Name, c.Name, lines.Text())
		}
		g.warn(lines.Err())
		stream.Close()
	}
}

// exec mirrors `kubectl exec <pod> -- <cmd>`. It writes a failure to stderr.
func (g *gatherer) exec(ctx context.Context, pod *corev1.Pod, stdout, stderr io.Writer, cmd ...string) {
	req := g.cs.CoreV1().RESTClient().Post().Resource("pods").Namespace(pod.Namespace).Name(pod.Name).SubResource("exec").
		VersionedParams(&corev1.PodExecOptions{Container: defaultContainer(pod), Command: cmd, Stdout: true, Stderr: true}, scheme.ParameterCodec)
	executor, err := remotecommand.NewSPDYExecutor(g.cfg, "POST", req.URL())
	if err == nil {
		err = executor.StreamWithContext(ctx, remotecommand.StreamOptions{Stdout: stdout, Stderr: stderr})
	}
	if err != nil {
		fmt.Fprintln(stderr, err)
	}
}
