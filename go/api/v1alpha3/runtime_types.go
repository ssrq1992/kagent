// Copyright 2026 The kagent Authors
// SPDX-License-Identifier: Apache-2.0

package v1alpha3

import corev1 "k8s.io/api/core/v1"

// RuntimeEnvVar configures one runtime environment variable.
//
// +kubebuilder:validation:XValidation:rule="has(self.value) != has(self.credentialRef)",message="exactly one of value or credentialRef must be specified"
// +kubebuilder:validation:XValidation:rule="!has(self.credentialRef) || self.credentialRef.name.size() > 0",message="credentialRef name must not be empty"
type RuntimeEnvVar struct {
	// +kubebuilder:validation:MinLength=1
	// +required
	Name string `json:"name"`

	// Value is a literal value, including an empty string.
	// +optional
	Value *string `json:"value,omitempty"`

	// CredentialRef references a key in a same-namespace Secret.
	// +optional
	CredentialRef *corev1.SecretKeySelector `json:"credentialRef,omitempty"`
}

// RuntimeAXPolicy selects an AX TaskGroup in the resource's namespace.
type RuntimeAXPolicy struct {
	// TaskGroupRef references an administrator-managed AX TaskGroup.
	// +kubebuilder:validation:XValidation:rule="self.name.size() > 0",message="taskGroupRef name must not be empty"
	// +required
	TaskGroupRef corev1.LocalObjectReference `json:"taskGroupRef"`

	// SnapshotLocationOverride optionally replaces the TaskGroup storage prefix.
	// +kubebuilder:validation:Pattern=`^(gs|s3)://[^[:space:]]+$`
	// +optional
	SnapshotLocationOverride string `json:"snapshotLocationOverride,omitempty"`
}
