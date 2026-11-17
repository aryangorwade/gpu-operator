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
	"bytes"
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/urfave/cli/v3"
)

func TestHelpWithoutKubernetes(t *testing.T) {
	t.Setenv("KUBECONFIG", filepath.Join(t.TempDir(), "missing"))
	var output bytes.Buffer
	cmd := newCommand()
	cmd.Writer = &output
	require.NoError(t, cmd.Run(t.Context(), []string{"gather", "--help"}))
	require.Contains(t, output.String(), "oc adm must-gather")
	require.Contains(t, output.String(), "--dest-dir")
}

func TestDefaultArtifactDir(t *testing.T) {
	now := time.Date(2026, time.November, 16, 10, 30, 0, 0, time.UTC)
	require.Equal(t, "/must-gather", defaultArtifactDir("/usr/bin/gather", now))
	require.Equal(t, "nvidia-gpu-operator_20261116_1030", filepath.Base(defaultArtifactDir("./gather", now)))
}

func TestArtifactDirOverride(t *testing.T) {
	t.Setenv("ARTIFACT_DIR", "/artifacts/from-env")
	for _, tc := range []struct {
		name string
		args []string
		want string
	}{
		{"environment", []string{"gather"}, "/artifacts/from-env"},
		{"flag", []string{"gather", "--dest-dir=/artifacts/from-flag"}, "/artifacts/from-flag"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cmd := newCommand()
			cmd.Action = func(_ context.Context, cmd *cli.Command) error {
				require.Equal(t, tc.want, cmd.String("dest-dir"))
				return nil
			}
			require.NoError(t, cmd.Run(t.Context(), tc.args))
		})
	}
}

func TestInvalidKubeconfig(t *testing.T) {
	cmd := newCommand()
	err := cmd.Run(t.Context(), []string{"gather", "--kubeconfig", filepath.Join(t.TempDir(), "missing")})
	require.ErrorContains(t, err, "load Kubernetes configuration")
}
