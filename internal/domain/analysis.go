package domain

import "time"

// Hypothesis is one analyzer hypothesis linked to persisted evidence.
// Score is a transparent heuristic ranking score, not a probability.
type Hypothesis struct {
	ID              string   `json:"id"`
	IncidentID      string   `json:"incident_id"`
	RuleID          string   `json:"rule_id"`
	Summary         string   `json:"summary"`
	Score           float64  `json:"score"`
	EvidenceIDs     []string `json:"evidence_ids"`
	MissingEvidence []string `json:"missing_evidence,omitempty"`
	SuggestedChecks []string `json:"suggested_checks,omitempty"`
}

// CollectionRun statuses recorded by collectors' callers.
const (
	RunStatusSuccess = "success"
	RunStatusFailed  = "failed"
)

// TimelineEvent is one ordered event in an incident's history.
type TimelineEvent struct {
	ID               string            `json:"id"`
	IncidentID       string            `json:"incident_id"`
	EventType        string            `json:"event_type"`
	OccurredAt       time.Time         `json:"occurred_at"`
	SourceEvidenceID string            `json:"source_evidence_id,omitempty"`
	Message          string            `json:"message"`
	Attributes       map[string]string `json:"attributes,omitempty"`
}

// CollectionRun records one collector execution for an incident.
type CollectionRun struct {
	ID                   string     `json:"id"`
	IncidentID           string     `json:"incident_id"`
	CollectorName        string     `json:"collector_name"`
	Status               string     `json:"status"`
	StartedAt            time.Time  `json:"started_at"`
	CompletedAt          *time.Time `json:"completed_at,omitempty"`
	ErrorCode            string     `json:"error_code,omitempty"`
	ErrorMessageRedacted string     `json:"error_message_redacted,omitempty"`
}
