package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

	"github.com/ThuatPhan/PodSleuth/internal/domain"
)

// AppendTimeline implements storage.Store. The batch is atomic.
func (s *Store) AppendTimeline(ctx context.Context, events []domain.TimelineEvent) error {
	if len(events) == 0 {
		return nil
	}
	return s.withTx(ctx, func(tx *sql.Tx) error {
		for i := range events {
			ev := events[i]
			if ev.ID == "" || ev.IncidentID == "" || ev.EventType == "" || ev.Message == "" {
				return fmt.Errorf("append timeline[%d]: id, incident id, event type and message are required", i)
			}
			attrs, err := json.Marshal(ev.Attributes)
			if err != nil {
				return fmt.Errorf("append timeline %s: %w", ev.ID, err)
			}
			if _, err := tx.ExecContext(ctx, `INSERT INTO timeline_events (
				id, incident_id, event_type, occurred_at_ms,
				source_evidence_id, message, attributes_json
			) VALUES (?, ?, ?, ?, ?, ?, ?)`,
				ev.ID, ev.IncidentID, ev.EventType, toMillis(ev.OccurredAt),
				nullStringFrom(ev.SourceEvidenceID), ev.Message, nullString(attrs),
			); err != nil {
				return wrapErr("append timeline event "+ev.ID, err)
			}
		}
		return nil
	})
}

// ListTimeline implements storage.Store.
func (s *Store) ListTimeline(ctx context.Context, incidentID string) ([]domain.TimelineEvent, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT
		id, incident_id, event_type, occurred_at_ms,
		source_evidence_id, message, attributes_json
		FROM timeline_events WHERE incident_id = ? ORDER BY occurred_at_ms ASC`, incidentID)
	if err != nil {
		return nil, wrapErr("list timeline", err)
	}
	defer rows.Close()

	var out []domain.TimelineEvent
	for rows.Next() {
		var ev domain.TimelineEvent
		var sourceEvidence, attrs sql.NullString
		var occurredAt int64
		if err := rows.Scan(
			&ev.ID, &ev.IncidentID, &ev.EventType, &occurredAt,
			&sourceEvidence, &ev.Message, &attrs,
		); err != nil {
			return nil, wrapErr("scan timeline event", err)
		}
		ev.OccurredAt = fromMillis(occurredAt)
		ev.SourceEvidenceID = sourceEvidence.String
		if attrs.Valid && attrs.String != "" {
			_ = json.Unmarshal([]byte(attrs.String), &ev.Attributes)
		}
		out = append(out, ev)
	}
	if err := rows.Err(); err != nil {
		return nil, wrapErr("list timeline", err)
	}
	return out, nil
}

// CreateCollectionRun implements storage.Store.
func (s *Store) CreateCollectionRun(ctx context.Context, run domain.CollectionRun) error {
	if run.ID == "" || run.IncidentID == "" || run.CollectorName == "" || run.Status == "" || run.StartedAt.IsZero() {
		return fmt.Errorf("sqlite: collection run %s requires id, incident id, collector name, status and start time", run.ID)
	}
	if _, err := s.db.ExecContext(ctx, `INSERT INTO collection_runs (
		id, incident_id, collector_name, status,
		started_at_ms, completed_at_ms, error_code, error_message_redacted
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		run.ID, run.IncidentID, run.CollectorName, run.Status,
		toMillis(run.StartedAt), nullMillis(run.CompletedAt),
		nullStringFrom(run.ErrorCode), nullStringFrom(run.ErrorMessageRedacted),
	); err != nil {
		return wrapErr("create collection run "+run.ID, err)
	}
	return nil
}

// ListCollectionRuns implements storage.Store.
func (s *Store) ListCollectionRuns(ctx context.Context, incidentID string) ([]domain.CollectionRun, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT
		id, incident_id, collector_name, status,
		started_at_ms, completed_at_ms, error_code, error_message_redacted
		FROM collection_runs WHERE incident_id = ? ORDER BY started_at_ms ASC`, incidentID)
	if err != nil {
		return nil, wrapErr("list collection runs", err)
	}
	defer rows.Close()

	var out []domain.CollectionRun
	for rows.Next() {
		var run domain.CollectionRun
		var errorCode, errorMessage sql.NullString
		var startedAt int64
		var completedAt sql.NullInt64
		if err := rows.Scan(
			&run.ID, &run.IncidentID, &run.CollectorName, &run.Status,
			&startedAt, &completedAt, &errorCode, &errorMessage,
		); err != nil {
			return nil, wrapErr("scan collection run", err)
		}
		run.StartedAt = fromMillis(startedAt)
		run.CompletedAt = timeFromNull(completedAt)
		run.ErrorCode = errorCode.String
		run.ErrorMessageRedacted = errorMessage.String
		out = append(out, run)
	}
	if err := rows.Err(); err != nil {
		return nil, wrapErr("list collection runs", err)
	}
	return out, nil
}
