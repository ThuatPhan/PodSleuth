// Package domain holds PodSleuth's pure domain types. It must not import
// Kubernetes clients, CLI, storage or rendering packages.
package domain

import (
	"encoding/json"
	"time"
)

// ResourceRef identifies a Kubernetes resource within a cluster.
type ResourceRef struct {
	ClusterID  string `json:"cluster_id"`
	APIVersion string `json:"api_version,omitempty"`
	Kind       string `json:"kind"`
	Namespace  string `json:"namespace,omitempty"`
	Name       string `json:"name"`
	UID        string `json:"uid,omitempty"`
}

// Observation is a normalized snapshot of a resource as seen by a watcher.
type Observation struct {
	Resource        ResourceRef     `json:"resource"`
	ObservedAt      time.Time       `json:"observed_at"`
	ResourceVersion string          `json:"resource_version,omitempty"`
	Payload         json.RawMessage `json:"payload"`
}
