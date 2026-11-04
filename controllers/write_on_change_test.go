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

package controllers

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

type writeCountingClient struct {
	client.Client
	creates, updates, deletes int
}

func (c *writeCountingClient) Create(ctx context.Context, obj client.Object, opts ...client.CreateOption) error {
	c.creates++
	return c.Client.Create(ctx, obj, opts...)
}

func (c *writeCountingClient) Update(ctx context.Context, obj client.Object, opts ...client.UpdateOption) error {
	c.updates++
	return c.Client.Update(ctx, obj, opts...)
}

func (c *writeCountingClient) Delete(ctx context.Context, obj client.Object, opts ...client.DeleteOption) error {
	c.deletes++
	return c.Client.Delete(ctx, obj, opts...)
}

func TestWriteOnlyWhenRenderedRoleChanges(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, rbacv1.AddToScheme(scheme))
	c := &writeCountingClient{Client: fake.NewClientBuilder().WithScheme(scheme).Build()}
	desired := func(verb string) *rbacv1.Role {
		return &rbacv1.Role{ObjectMeta: metav1.ObjectMeta{Name: "role", Namespace: "test"},
			Rules: []rbacv1.PolicyRule{{Verbs: []string{verb}, Resources: []string{"pods"}}}}
	}
	current := &rbacv1.Role{}
	require.NoError(t, syncRenderedObject(t.Context(), c, desired("get"), current))
	require.NoError(t, c.Get(t.Context(), client.ObjectKey{Name: "role", Namespace: "test"}, current))
	current.Generation = 1
	current.CreationTimestamp = metav1.Now()
	require.NoError(t, c.Client.Update(t.Context(), current))
	require.NoError(t, syncRenderedObject(t.Context(), c, desired("get"), current))
	require.Equal(t, 1, c.creates)
	require.Zero(t, c.updates)
	require.NoError(t, syncRenderedObject(t.Context(), c, desired("list"), current))
	require.NoError(t, syncRenderedObject(t.Context(), c, desired("list"), current))
	require.Equal(t, 1, c.updates)
	require.NoError(t, c.Get(t.Context(), client.ObjectKey{Name: "role", Namespace: "test"}, current))
	current.Rules[0].Verbs = []string{"watch"}
	require.NoError(t, c.Client.Update(t.Context(), current))
	require.NoError(t, syncRenderedObject(t.Context(), c, desired("list"), &rbacv1.Role{}))
	require.Equal(t, 2, c.updates, "a changed object must be repaired even if its hash is unchanged")
}

func TestDeleteMissingObjectDoesNotWrite(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, rbacv1.AddToScheme(scheme))
	c := &writeCountingClient{Client: fake.NewClientBuilder().WithScheme(scheme).Build()}
	obj := &rbacv1.Role{ObjectMeta: metav1.ObjectMeta{Name: "role", Namespace: "test"}}
	require.NoError(t, deleteIfExists(t.Context(), c, obj))
	require.Zero(t, c.deletes)
	require.NoError(t, c.Client.Create(t.Context(), obj))
	require.NoError(t, deleteIfExists(t.Context(), c, obj))
	require.Equal(t, 1, c.deletes)
}
