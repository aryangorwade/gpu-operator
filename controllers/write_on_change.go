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

	corev1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/NVIDIA/gpu-operator/internal/utils"
)

// syncRenderedObject uses the cached object to avoid unnecessary API writes.
func syncRenderedObject(ctx context.Context, c client.Client, desired, current client.Object) error {
	annotations := desired.GetAnnotations()
	if annotations == nil {
		annotations = make(map[string]string)
	}
	delete(annotations, NvidiaAnnotationHashKey)
	desired.SetAnnotations(annotations)
	annotations[NvidiaAnnotationHashKey] = utils.GetObjectHash(desired)
	desired.SetAnnotations(annotations)

	err := c.Get(ctx, client.ObjectKeyFromObject(desired), current)
	if apierrors.IsNotFound(err) {
		return c.Create(ctx, desired)
	}
	if err != nil {
		return err
	}
	comparison := current.DeepCopyObject().(client.Object)
	comparison.SetCreationTimestamp(desired.GetCreationTimestamp())
	comparison.SetGeneration(desired.GetGeneration())
	if current.GetAnnotations()[NvidiaAnnotationHashKey] == annotations[NvidiaAnnotationHashKey] &&
		apiequality.Semantic.DeepDerivative(desired, comparison) {
		return nil
	}

	desired.SetResourceVersion(current.GetResourceVersion())
	if service, ok := desired.(*corev1.Service); ok {
		found := current.(*corev1.Service)
		service.Spec.ClusterIP = found.Spec.ClusterIP
		service.Spec.ClusterIPs = found.Spec.ClusterIPs
		service.Spec.IPFamilies = found.Spec.IPFamilies
		service.Spec.IPFamilyPolicy = found.Spec.IPFamilyPolicy
		service.Spec.HealthCheckNodePort = found.Spec.HealthCheckNodePort
	}
	if sa, ok := desired.(*corev1.ServiceAccount); ok {
		found := current.(*corev1.ServiceAccount)
		sa.Secrets = found.Secrets
		if len(sa.ImagePullSecrets) == 0 {
			sa.ImagePullSecrets = found.ImagePullSecrets
		}
	}
	return c.Update(ctx, desired)
}

func deleteIfExists(ctx context.Context, c client.Client, obj client.Object) error {
	current := obj.DeepCopyObject().(client.Object)
	err := c.Get(ctx, client.ObjectKeyFromObject(obj), current)
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return err
	}
	return c.Delete(ctx, current)
}
