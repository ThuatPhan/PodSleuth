// Package storage defines the persistence contract for incidents and
// their associated data. Implementations live in subpackages; the
// domain layer never imports them.
package storage

import (
	"context"
	"errors"

	"github.com/ThuatPhan/PodSleuth/internal/domain"
)

// Sentinel errors returned by Store implementations.
var (
	// ErrNotFound is returned when a requested record does not exist.
	ErrNotFound = errors.New("storage: not found")
	// ErrAlreadyExists is returned when a record with the same
	// identity already exists and idempotency requires no duplicate.
	ErrAlreadyExists = errors.New("storage: already exists")
)

// IncidentFilter narrows ListIncidents. Empty fields match everything.
type IncidentFilter struct {
	Status    domain.IncidentStatus
	Namespace string
}

// Store persists incidents and their associated data.
type Store interface {
	// EnsureCluster idempotently records a cluster identity.
	EnsureCluster(ctx context.Context, id, displayName string) error

	// CreateIncident persists a new incident. An incident with the same
	// ID, or an active incident with the same cluster and fingerprint,
	// yields ErrAlreadyExists.
	CreateIncident(ctx context.Context, incident domain.Incident) error
	// GetIncident returns one incident by ID, or ErrNotFound.
	GetIncident(ctx context.Context, id string) (domain.Incident, error)
	// UpdateIncident replaces the mutable fields of an incident.
	UpdateIncident(ctx context.Context, incident domain.Incident) error
	// ListIncidents returns incidents matching the filter, most
	// recently seen first.
	ListIncidents(ctx context.Context, filter IncidentFilter) ([]domain.Incident, error)

	// AddEvidence persists a batch of evidence atomically.
	AddEvidence(ctx context.Context, evidence []domain.Evidence) error
	// ListEvidence returns an incident's evidence, oldest first.
	ListEvidence(ctx context.Context, incidentID string) ([]domain.Evidence, error)

	// SaveAnalysis replaces an incident's persisted hypotheses.
	SaveAnalysis(ctx context.Context, incidentID string, hypotheses []domain.Hypothesis) error
	// GetAnalysis returns an incident's hypotheses, ranked order.
	GetAnalysis(ctx context.Context, incidentID string) ([]domain.Hypothesis, error)

	// AppendTimeline persists timeline events atomically.
	AppendTimeline(ctx context.Context, events []domain.TimelineEvent) error
	// ListTimeline returns an incident's timeline, chronological.
	ListTimeline(ctx context.Context, incidentID string) ([]domain.TimelineEvent, error)

	// CreateCollectionRun persists one collector execution record.
	CreateCollectionRun(ctx context.Context, run domain.CollectionRun) error
	// ListCollectionRuns returns an incident's collection runs.
	ListCollectionRuns(ctx context.Context, incidentID string) ([]domain.CollectionRun, error)

	// Close releases database resources.
	Close() error
}
