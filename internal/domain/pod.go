package domain

import (
	"encoding/json"
	"fmt"
)

// PodView is the detection-oriented view of a Pod. It is decoded from
// the JSON payload of a watcher observation, keeping the domain
// independent of Kubernetes API types.
type PodView struct {
	Metadata PodMeta       `json:"metadata"`
	Spec     PodSpecView   `json:"spec"`
	Status   PodStatusView `json:"status"`
}

// PodSpecView is the Pod spec needed by detection and analysis.
type PodSpecView struct {
	NodeName       string              `json:"nodeName,omitempty"`
	Containers     []ContainerSpecView `json:"containers,omitempty"`
	InitContainers []ContainerSpecView `json:"initContainers,omitempty"`
}

// ContainerSpecView is one container's spec, limited to fields used by
// analysis.
type ContainerSpecView struct {
	Name           string     `json:"name,omitempty"`
	Image          string     `json:"image,omitempty"`
	LivenessProbe  *ProbeView `json:"livenessProbe,omitempty"`
	ReadinessProbe *ProbeView `json:"readinessProbe,omitempty"`
	StartupProbe   *ProbeView `json:"startupProbe,omitempty"`
}

// ProbeView is a presence-only view of a probe definition; MVP rules
// only need to know whether a probe is configured.
type ProbeView struct{}

// Probe returns the probe of the given kind ("liveness", "readiness"
// or "startup") configured on the container, if any.
func (p *PodView) Probe(container, kind string) *ProbeView {
	for _, c := range p.AllSpecContainers() {
		if c.Name != container {
			continue
		}
		switch kind {
		case "liveness":
			return c.LivenessProbe
		case "readiness":
			return c.ReadinessProbe
		case "startup":
			return c.StartupProbe
		}
	}
	return nil
}

func (p *PodView) AllSpecContainers() []ContainerSpecView {
	out := make([]ContainerSpecView, 0, len(p.Spec.Containers)+len(p.Spec.InitContainers))
	out = append(out, p.Spec.Containers...)
	out = append(out, p.Spec.InitContainers...)
	return out
}

// PodMeta is the Pod metadata needed for detection and identity.
type PodMeta struct {
	Namespace       string     `json:"namespace,omitempty"`
	Name            string     `json:"name,omitempty"`
	UID             string     `json:"uid,omitempty"`
	ResourceVersion string     `json:"resourceVersion,omitempty"`
	OwnerReferences []OwnerRef `json:"ownerReferences,omitempty"`
}

// OwnerRef identifies a workload owner of a Pod.
type OwnerRef struct {
	Kind       string `json:"kind,omitempty"`
	Name       string `json:"name,omitempty"`
	UID        string `json:"uid,omitempty"`
	Controller *bool  `json:"controller,omitempty"`
}

// PodStatusView is the Pod status needed for detection and recovery.
type PodStatusView struct {
	Phase                 string            `json:"phase,omitempty"`
	Reason                string            `json:"reason,omitempty"`
	Message               string            `json:"message,omitempty"`
	Conditions            []PodCondition    `json:"conditions,omitempty"`
	ContainerStatuses     []ContainerStatus `json:"containerStatuses,omitempty"`
	InitContainerStatuses []ContainerStatus `json:"initContainerStatuses,omitempty"`
}

// PodCondition is one Pod status condition.
type PodCondition struct {
	Type    string `json:"type,omitempty"`
	Status  string `json:"status,omitempty"`
	Reason  string `json:"reason,omitempty"`
	Message string `json:"message,omitempty"`
}

// ContainerStatus is one container's observed state.
type ContainerStatus struct {
	Name                 string         `json:"name,omitempty"`
	RestartCount         int32          `json:"restartCount,omitempty"`
	State                ContainerState `json:"state,omitempty"`
	LastTerminationState ContainerState `json:"lastState,omitempty"`
}

// ContainerState is the waiting/running/terminated triple of a container.
type ContainerState struct {
	Waiting    *ContainerWaiting    `json:"waiting,omitempty"`
	Running    *struct{}            `json:"running,omitempty"`
	Terminated *ContainerTerminated `json:"terminated,omitempty"`
}

// ContainerWaiting is a waiting container state.
type ContainerWaiting struct {
	Reason  string `json:"reason,omitempty"`
	Message string `json:"message,omitempty"`
}

// ContainerTerminated is a terminated container state.
type ContainerTerminated struct {
	ExitCode int32  `json:"exitCode"`
	Reason   string `json:"reason,omitempty"`
}

// ParsePodView decodes a watcher payload into a PodView.
func ParsePodView(payload []byte) (*PodView, error) {
	var p PodView
	if err := json.Unmarshal(payload, &p); err != nil {
		return nil, fmt.Errorf("cannot parse pod payload: %w", err)
	}
	return &p, nil
}

// ControllerOwner returns the controller owner reference, if any.
func (p *PodView) ControllerOwner() *OwnerRef {
	for i := range p.Metadata.OwnerReferences {
		if o := &p.Metadata.OwnerReferences[i]; o.Controller != nil && *o.Controller {
			return o
		}
	}
	return nil
}

// IsReady reports whether the Pod Ready condition is true.
func (p *PodView) IsReady() bool {
	for _, c := range p.Status.Conditions {
		if c.Type == "Ready" {
			return c.Status == "True"
		}
	}
	return false
}

// AllContainerStatuses returns regular and init container statuses.
func (p *PodView) AllContainerStatuses() []ContainerStatus {
	out := make([]ContainerStatus, 0, len(p.Status.ContainerStatuses)+len(p.Status.InitContainerStatuses))
	out = append(out, p.Status.ContainerStatuses...)
	out = append(out, p.Status.InitContainerStatuses...)
	return out
}
