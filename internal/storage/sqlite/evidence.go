package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

	"github.com/ThuatPhan/PodSleuth/internal/domain"
)

// AddEvidence implements storage.Store. The batch is atomic: one
// invalid item rolls back all of them.
func (s *Store) AddEvidence(ctx context.Context, items []domain.Evidence) error {
	if len(items) == 0 {
		return nil
	}
	return s.withTx(ctx, func(tx *sql.Tx) error {
		for i := range items {
			ev := items[i]
			if ev.ID == "" || ev.IncidentID == "" || ev.Kind == "" || ev.ContentHash == "" {
				return fmt.Errorf("add evidence[%d]: id, incident id, kind and content hash are required", i)
			}
			created := ev.ObservedAt
			if _, err := tx.ExecContext(ctx, `INSERT INTO evidence (
				id, incident_id, kind, source,
				resource_kind, resource_namespace, resource_name, resource_uid,
				observed_at_ms, event_time_ms, content_json, content_text,
				content_hash, redacted, collection_status, collector_version, created_at_ms
			) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
				ev.ID, ev.IncidentID, ev.Kind, ev.Source,
				ev.Resource.Kind, ev.Resource.Namespace, ev.Resource.Name, ev.Resource.UID,
				toMillis(ev.ObservedAt), nullMillis(ev.EventTime),
				nullString(ev.Content), nullStringFrom(ev.Text),
				ev.ContentHash, boolToInt(ev.Redacted), ev.CollectionStatus, "", toMillis(created),
			); err != nil {
				return wrapErr(fmt.Sprintf("add evidence %s", ev.ID), err)
			}
		}
		return nil
	})
}

// ListEvidence implements storage.Store.
func (s *Store) ListEvidence(ctx context.Context, incidentID string) ([]domain.Evidence, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT
		id, incident_id, kind, source,
		resource_kind, resource_namespace, resource_name, resource_uid,
		observed_at_ms, event_time_ms, content_json, content_text,
		content_hash, redacted, collection_status, collector_version, created_at_ms
		FROM evidence WHERE incident_id = ? ORDER BY observed_at_ms ASC`, incidentID)
	if err != nil {
		return nil, wrapErr("list evidence", err)
	}
	defer rows.Close()

	var out []domain.Evidence
	for rows.Next() {
		var ev domain.Evidence
		var resourceKind, resourceNS, resourceName, resourceUID sql.NullString
		var contentJSON, contentText, collectorVersion sql.NullString
		var eventTime sql.NullInt64
		var redacted int
		var observedAt, createdAt int64
		if err := rows.Scan(
			&ev.ID, &ev.IncidentID, &ev.Kind, &ev.Source,
			&resourceKind, &resourceNS, &resourceName, &resourceUID,
			&observedAt, &eventTime, &contentJSON, &contentText,
			&ev.ContentHash, &redacted, &ev.CollectionStatus, &collectorVersion, &createdAt,
		); err != nil {
			return nil, wrapErr("scan evidence", err)
		}
		ev.Resource = domain.ResourceRef{
			Kind:      resourceKind.String,
			Namespace: resourceNS.String,
			Name:      resourceName.String,
			UID:       resourceUID.String,
		}
		ev.ObservedAt = fromMillis(observedAt)
		ev.EventTime = timeFromNull(eventTime)
		if contentJSON.Valid && contentJSON.String != "" {
			ev.Content = []byte(contentJSON.String)
		}
		ev.Text = contentText.String
		ev.Redacted = redacted != 0
		out = append(out, ev)
	}
	if err := rows.Err(); err != nil {
		return nil, wrapErr("list evidence", err)
	}
	return out, nil
}

// SaveAnalysis implements storage.Store. It replaces any previously
// persisted hypotheses for the incident atomically.
func (s *Store) SaveAnalysis(ctx context.Context, incidentID string, hypotheses []domain.Hypothesis) error {
	return s.withTx(ctx, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx,
			`DELETE FROM hypotheses WHERE incident_id = ?`, incidentID); err != nil {
			return wrapErr("clear analysis", err)
		}
		for i := range hypotheses {
			h := hypotheses[i]
			if h.ID == "" || h.IncidentID == "" || h.RuleID == "" || h.Summary == "" {
				return fmt.Errorf("save analysis[%d]: id, incident id, rule id and summary are required", i)
			}
			if h.IncidentID != incidentID {
				return fmt.Errorf("save analysis[%d]: hypothesis incident id %q does not match %q", i, h.IncidentID, incidentID)
			}
			missing, err := json.Marshal(h.MissingEvidence)
			if err != nil {
				return fmt.Errorf("save analysis %s: %w", h.ID, err)
			}
			checks, err := json.Marshal(h.SuggestedChecks)
			if err != nil {
				return fmt.Errorf("save analysis %s: %w", h.ID, err)
			}
			if _, err := tx.ExecContext(ctx, `INSERT INTO hypotheses (
				id, incident_id, rule_id, summary, confidence, rank,
				missing_evidence_json, suggested_checks_json, created_at_ms
			) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
				h.ID, h.IncidentID, h.RuleID, h.Summary, h.Score, i,
				nullString(missing), nullString(checks), nowMillis(),
			); err != nil {
				return wrapErr("save hypothesis "+h.ID, err)
			}
			for _, evidenceID := range h.EvidenceIDs {
				if _, err := tx.ExecContext(ctx,
					`INSERT INTO hypothesis_evidence (hypothesis_id, evidence_id) VALUES (?, ?)`,
					h.ID, evidenceID); err != nil {
					return wrapErr(fmt.Sprintf("link hypothesis %s to evidence %s", h.ID, evidenceID), err)
				}
			}
		}
		return nil
	})
}

// GetAnalysis implements storage.Store.
func (s *Store) GetAnalysis(ctx context.Context, incidentID string) ([]domain.Hypothesis, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT
		id, incident_id, rule_id, summary, confidence, rank,
		missing_evidence_json, suggested_checks_json
		FROM hypotheses WHERE incident_id = ? ORDER BY rank ASC`, incidentID)
	if err != nil {
		return nil, wrapErr("get analysis", err)
	}
	defer rows.Close()

	var out []domain.Hypothesis
	for rows.Next() {
		var h domain.Hypothesis
		var missing, checks sql.NullString
		if err := rows.Scan(
			&h.ID, &h.IncidentID, &h.RuleID, &h.Summary, &h.Score, new(int),
			&missing, &checks,
		); err != nil {
			return nil, wrapErr("scan hypothesis", err)
		}
		h.MissingEvidence = decodeStrings(missing)
		h.SuggestedChecks = decodeStrings(checks)
		out = append(out, h)
	}
	if err := rows.Err(); err != nil {
		return nil, wrapErr("get analysis", err)
	}

	for i := range out {
		ids, err := s.hypothesisEvidenceIDs(ctx, out[i].ID)
		if err != nil {
			return nil, err
		}
		out[i].EvidenceIDs = ids
	}
	return out, nil
}

func (s *Store) hypothesisEvidenceIDs(ctx context.Context, hypothesisID string) ([]string, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT evidence_id FROM hypothesis_evidence WHERE hypothesis_id = ? ORDER BY evidence_id ASC`,
		hypothesisID)
	if err != nil {
		return nil, wrapErr("list hypothesis evidence", err)
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, wrapErr("scan hypothesis evidence", err)
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

func decodeStrings(raw sql.NullString) []string {
	if !raw.Valid || raw.String == "" {
		return nil
	}
	var out []string
	if err := json.Unmarshal([]byte(raw.String), &out); err != nil {
		return nil
	}
	return out
}

func nullString(b []byte) sql.NullString {
	if len(b) == 0 {
		return sql.NullString{}
	}
	return sql.NullString{String: string(b), Valid: true}
}

func nullStringFrom(s string) sql.NullString {
	if s == "" {
		return sql.NullString{}
	}
	return sql.NullString{String: s, Valid: true}
}

func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}
