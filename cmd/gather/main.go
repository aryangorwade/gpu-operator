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

package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/urfave/cli/v3"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/NVIDIA/gpu-operator/internal/info"
	"github.com/NVIDIA/gpu-operator/internal/mustgather"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	err := newCommand().Run(ctx, os.Args)
	stop()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func defaultArtifactDir(executable string, now time.Time) string {
	if executable == "/usr/bin/gather" {
		return "/must-gather"
	}
	return filepath.Join(os.TempDir(), "nvidia-gpu-operator_"+now.Format("20060102_1504"))
}

func newCommand() *cli.Command {
	return &cli.Command{
		Name:    "gather",
		Usage:   "Collect NVIDIA GPU Operator diagnostics (also an oc adm must-gather plugin)",
		Version: info.GetVersionString(),
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name: "dest-dir", Usage: "Directory for diagnostic artifacts",
				Value: defaultArtifactDir(os.Args[0], time.Now()), Sources: cli.EnvVars("ARTIFACT_DIR"),
			},
			&cli.StringFlag{Name: "kubeconfig", Usage: "Path to kubeconfig (defaults to KUBECONFIG, in-cluster credentials, or ~/.kube/config)"},
			&cli.StringFlag{Name: "namespace", Usage: "GPU Operator namespace (auto-detected when omitted)"},
			&cli.DurationFlag{Name: "timeout", Usage: "Maximum collection duration", Value: 10 * time.Minute},
		},
		Action: func(ctx context.Context, cmd *cli.Command) error {
			loadingRules := clientcmd.NewDefaultClientConfigLoadingRules()
			loadingRules.ExplicitPath = cmd.String("kubeconfig")
			config, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(loadingRules, &clientcmd.ConfigOverrides{}).ClientConfig()
			if err != nil {
				return fmt.Errorf("load Kubernetes configuration: %w", err)
			}
			config.Timeout = 30 * time.Second
			ctx, cancel := context.WithTimeout(ctx, cmd.Duration("timeout"))
			defer cancel()
			return mustgather.Run(ctx, config, mustgather.Options{
				Directory: cmd.String("dest-dir"), Namespace: cmd.String("namespace"),
				Version: info.GetVersionString(), Stdout: cmd.Writer, Stderr: cmd.ErrWriter,
			})
		},
	}
}
