package domain

import (
	"encoding/json"
	"time"
)

// Evidence is one collected piece of forensic data for an incident.
type Evidence struct {
	ID               string          `json:"id"`
	IncidentID       string          `json:"incident_id"`
	Kind             string          `json:"kind"`
	Source           string          `json:"source"`
	Resource         ResourceRef     `json:"resource"`
	ObservedAt       time.Time       `json:"observed_at"`
	EventTime        *time.Time      `json:"event_time,omitempty"`
	Content          json.RawMessage `json:"content,omitempty"`
	Text             string          `json:"text,omitempty"`
	ContentHash      string          `json:"content_hash"`
	Redacted         bool            `json:"redacted"`
	CollectionStatus string          `json:"collection_status"`
}
