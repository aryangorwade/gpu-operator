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
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/remotecommand"
	"k8s.io/streaming/pkg/httpstream"
)

func defaultContainer(pod *corev1.Pod) string {
	name := pod.Annotations["kubectl.kubernetes.io/default-container"]
	for _, container := range pod.Spec.Containers {
		if container.Name == name {
			return name
		}
	}
	if len(pod.Spec.Containers) > 0 {
		return pod.Spec.Containers[0].Name
	}
	return ""
}

func (g *collector) logs(ctx context.Context, w io.Writer, pod *corev1.Pod, previous, all bool) {
	containers := []string{defaultContainer(pod)}
	if all {
		containers = nil
		for _, container := range pod.Spec.InitContainers {
			containers = append(containers, container.Name)
		}
		for _, container := range pod.Spec.Containers {
			containers = append(containers, container.Name)
		}
		for _, container := range pod.Spec.EphemeralContainers {
			containers = append(containers, container.Name)
		}
	}
	for _, container := range containers {
		stream, err := g.client.CoreV1().Pods(pod.Namespace).GetLogs(pod.Name, &corev1.PodLogOptions{
			Container: container, Previous: previous, Timestamps: true,
		}).Stream(ctx)
		if err != nil {
			g.report(fmt.Sprintf("logs %s/%s (previous=%t)", pod.Name, container, previous), err)
			continue
		}
		if all {
			err = copyPrefixed(w, stream, fmt.Sprintf("[pod/%s/%s] ", pod.Name, container))
		} else {
			_, err = io.Copy(w, stream)
		}
		stream.Close()
		if err != nil {
			g.report("logs "+pod.Name+"/"+container, err)
		}
	}
}

func copyPrefixed(w io.Writer, r io.Reader, prefix string) error {
	reader := bufio.NewReader(r)
	for {
		line, err := reader.ReadString('\n')
		if len(line) > 0 {
			if _, writeErr := fmt.Fprint(w, prefix, line); writeErr != nil {
				return writeErr
			}
		}
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
	}
}

func podExecutor(config *rest.Config, client rest.Interface) func(context.Context, *corev1.Pod, []string, io.Writer, io.Writer) error {
	return func(ctx context.Context, pod *corev1.Pod, command []string, stdout, stderr io.Writer) error {
		req := client.Post().Namespace(pod.Namespace).Resource("pods").Name(pod.Name).SubResource("exec").
			VersionedParams(&corev1.PodExecOptions{
				Container: defaultContainer(pod), Command: command, Stdout: true, Stderr: true,
			}, scheme.ParameterCodec)
		spdy, err := remotecommand.NewSPDYExecutor(config, http.MethodPost, req.URL())
		if err != nil {
			return err
		}
		websocket, err := remotecommand.NewWebSocketExecutor(config, http.MethodGet, req.URL().String())
		if err != nil {
			return err
		}
		executor, err := remotecommand.NewFallbackExecutor(websocket, spdy, func(err error) bool {
			return httpstream.IsUpgradeFailure(err) || httpstream.IsHTTPSProxyError(err)
		})
		if err != nil {
			return err
		}
		return executor.StreamWithContext(ctx, remotecommand.StreamOptions{Stdout: stdout, Stderr: stderr})
	}
}

func isDriverPod(pod *corev1.Pod) bool {
	_, toolkit := pod.Labels["openshift.driver-toolkit"]
	return toolkit || pod.Labels["app.kubernetes.io/component"] == "nvidia-driver" || pod.Labels["app"] == "nvidia-vgpu-manager-daemonset"
}

func (g *collector) collectDriver(ctx context.Context, pod *corev1.Pod) {
	node := pod.Spec.NodeName
	if node == "" {
		return
	}
	if err := g.exec(ctx, pod, []string{"bash", "-c", "cd /tmp && nvidia-bug-report.sh"}, g.Stderr, g.Stderr); err != nil {
		g.report("nvidia-bug-report on "+node, err)
	} else {
		g.write("nvidia-bug-report_"+node+".log.gz", func(w io.Writer) error {
			// Stream just the report; unlike kubectl cp, this needs no local tar
			// executable and cannot extract arbitrary paths from a remote archive.
			return g.exec(ctx, pod, []string{"cat", "/tmp/nvidia-bug-report.log.gz"}, w, g.Stderr)
		})
	}
	g.write("gpu_device_usage_"+node+".log", func(w io.Writer) error {
		commands := []struct {
			title   string
			command []string
		}{
			{"nvidia-smi", []string{"nvidia-smi"}},
			{"lsof /run/nvidia/driver/dev/nvidia*", hostCommand("lsof /run/nvidia/driver/dev/nvidia* 2>&1 || true")},
			{"Process details for PIDs using GPU devices", hostCommand(processDetails)},
			{"fuser -v /run/nvidia/driver/dev/nvidia*", hostCommand("fuser -v /run/nvidia/driver/dev/nvidia* 2>&1 || true")},
			{"ls -la /run/nvidia/driver/dev/nvidia*", hostCommand("ls -la /run/nvidia/driver/dev/nvidia* 2>&1 || true")},
		}
		for _, cmd := range commands {
			fmt.Fprintf(w, "\n# %s output from %s\n", cmd.title, node)
			if err := g.exec(ctx, pod, cmd.command, w, w); err != nil {
				fmt.Fprintf(w, "Failed to run %s: %v\n", cmd.command[0], err)
				g.report(strings.Join(cmd.command, " ")+" on "+node, err)
			}
		}
		return nil
	})
}

func hostCommand(command string) []string {
	return []string{"nsenter", "--target", "1", "--mount", "--pid", "--", "bash", "-c", command}
}

// These commands execute inside the existing driver pod, never in the
// distroless collector. Keep the script's host-process diagnostics unchanged.
const processDetails = `
pids=$(lsof /run/nvidia/driver/dev/nvidia* 2>/dev/null | awk 'NR>1 {print $2}' | sort -u)
if [ -n "$pids" ]; then
    echo "PIDs using GPU: $pids"
    echo ""
    for pid in $pids; do
        echo "=== Process $pid ==="
        ps -p "$pid" -o pid,ppid,user,stat,start,etime,pcpu,pmem,vsz,rss,args 2>&1 || echo "Process $pid not found"
        echo ""
    done
else
    echo "No processes found using GPU devices"
fi
`
