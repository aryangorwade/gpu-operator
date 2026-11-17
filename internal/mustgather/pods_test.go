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
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

func TestCopyPrefixed(t *testing.T) {
	for name, tc := range map[string]struct{ input, expected string }{
		"empty":   {},
		"lines":   {"first\nsecond\n", "[pod/x/y] first\n[pod/x/y] second\n"},
		"partial": {"first\nsecond", "[pod/x/y] first\n[pod/x/y] second"},
		"long":    {strings.Repeat("x", 128*1024), "[pod/x/y] " + strings.Repeat("x", 128*1024)},
	} {
		t.Run(name, func(t *testing.T) {
			var out bytes.Buffer
			require.NoError(t, copyPrefixed(&out, strings.NewReader(tc.input), "[pod/x/y] "))
			require.Equal(t, tc.expected, out.String())
		})
	}
}

func TestDriverPodSelectors(t *testing.T) {
	for name, tc := range map[string]struct {
		labels map[string]string
		want   bool
	}{
		"toolkit": {map[string]string{"openshift.driver-toolkit": ""}, true},
		"driver":  {map[string]string{"app.kubernetes.io/component": "nvidia-driver"}, true},
		"vgpu":    {map[string]string{"app": "nvidia-vgpu-manager-daemonset"}, true},
		"other":   {map[string]string{"app": "nvidia-device-plugin"}, false},
		"none":    {},
	} {
		t.Run(name, func(t *testing.T) { require.Equal(t, tc.want, isDriverPod(testPod("pod", tc.labels))) })
	}
}

func TestDefaultContainer(t *testing.T) {
	pod := testPod("pod", nil)
	pod.Spec.Containers = append(pod.Spec.Containers, corev1.Container{Name: "driver"})
	require.Equal(t, "main", defaultContainer(pod))
	pod.Annotations = map[string]string{"kubectl.kubernetes.io/default-container": "driver"}
	require.Equal(t, "driver", defaultContainer(pod))
	pod.Annotations["kubectl.kubernetes.io/default-container"] = "missing"
	require.Equal(t, "main", defaultContainer(pod))
}

func TestPodExecutorFallsBackToSPDY(t *testing.T) {
	requests := make(chan *http.Request, 2)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		requests <- req
		http.Error(w, "upgrade unavailable", http.StatusBadRequest)
	}))
	defer server.Close()
	config := &rest.Config{Host: server.URL, Timeout: 5 * time.Second}
	client, err := kubernetes.NewForConfig(config)
	require.NoError(t, err)
	pod := testPod("driver", nil)
	pod.Spec.Containers = append(pod.Spec.Containers, corev1.Container{Name: "driver-container"})
	pod.Annotations = map[string]string{"kubectl.kubernetes.io/default-container": "driver-container"}
	command := []string{"cat", "/tmp/nvidia-bug-report.log.gz"}
	err = podExecutor(config, client.CoreV1().RESTClient())(t.Context(), pod, command, io.Discard, io.Discard)
	require.Error(t, err)
	for _, method := range []string{http.MethodGet, http.MethodPost} {
		select {
		case req := <-requests:
			require.Equal(t, method, req.Method)
			require.Equal(t, "/api/v1/namespaces/gpu-operator/pods/driver/exec", req.URL.Path)
			query, err := url.ParseQuery(req.URL.RawQuery)
			require.NoError(t, err)
			require.Equal(t, command, query["command"])
			require.Equal(t, "driver-container", query.Get("container"))
			require.Equal(t, "true", query.Get("stdout"))
			require.Equal(t, "true", query.Get("stderr"))
		default:
			t.Fatalf("missing %s exec request", method)
		}
	}
}
