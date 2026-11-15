package domain

import "time"

// IncidentStatus is the lifecycle state of an incident.
type IncidentStatus string

// Incident lifecycle states.
const (
	StatusDetected         IncidentStatus = "detected"
	StatusCollecting       IncidentStatus = "collecting"
	StatusAnalyzing        IncidentStatus = "analyzing"
	StatusOpen             IncidentStatus = "open"
	StatusCollectionFailed IncidentStatus = "collection_failed"
	StatusResolved         IncidentStatus = "resolved"
	StatusClosed           IncidentStatus = "closed"
)

// DefaultSeverity is applied to incidents without a more specific rating.
const DefaultSeverity = "warning"

// FailureSignal reports one detected failure condition on a resource.
type FailureSignal struct {
	Type        string            `json:"type"`
	Reason      string            `json:"reason"`
	Fingerprint string            `json:"fingerprint"`
	Resource    ResourceRef       `json:"resource"`
	ObservedAt  time.Time         `json:"observed_at"`
	Attributes  map[string]string `json:"attributes,omitempty"`
}

// Incident is one failure episode for a container or workload context.
type Incident struct {
	ID            string         `json:"id"`
	ClusterID     string         `json:"cluster_id"`
	Fingerprint   string         `json:"fingerprint"`
	FailureType   string         `json:"failure_type"`
	Severity      string         `json:"severity"`
	Status        IncidentStatus `json:"status"`
	Resource      ResourceRef    `json:"resource"`
	WorkloadKind  string         `json:"workload_kind,omitempty"`
	WorkloadName  string         `json:"workload_name,omitempty"`
	WorkloadUID   string         `json:"workload_uid,omitempty"`
	ContainerName string         `json:"container_name,omitempty"`
	FirstSeenAt   time.Time      `json:"first_seen_at"`
	LastSeenAt    time.Time      `json:"last_seen_at"`
	ResolvedAt    *time.Time     `json:"resolved_at,omitempty"`
	CreatedAt     time.Time      `json:"created_at"`
	UpdatedAt     time.Time      `json:"updated_at"`
	Summary       string         `json:"summary,omitempty"`
}

// CanTransition reports whether the incident lifecycle allows moving
// from one status to another.
func CanTransition(from, to IncidentStatus) bool {
	return allowedTransitions[from][to]
}

// allowedTransitions implements the state diagram in the spec. The one
// addition is detected -> resolved: recovery can complete before
// collection starts, and without it recovered incidents would be stuck
// in detected until collection exists.
var allowedTransitions = map[IncidentStatus]map[IncidentStatus]bool{
	StatusDetected: {
		StatusCollecting:       true,
		StatusCollectionFailed: true,
		StatusResolved:         true,
	},
	StatusCollecting: {
		StatusAnalyzing:        true,
		StatusCollectionFailed: true,
	},
	StatusCollectionFailed: {
		StatusCollecting: true,
	},
	StatusAnalyzing: {
		StatusOpen: true,
	},
	StatusOpen: {
		StatusResolved: true,
		StatusClosed:   true,
	},
	StatusResolved: {
		StatusOpen:   true,
		StatusClosed: true,
	},
	StatusClosed: {},
}
