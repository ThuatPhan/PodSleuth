package sqlite

import (
	"context"
	"database/sql"
	"fmt"
)

// schemaVersion is the latest migration version.
const schemaVersion = 1

// migration1 is the initial schema from the specification. Two
// deviations, both reported for approval:
//   - UNIQUE(cluster_id, fingerprint) is a partial unique index on
//     active incidents instead: the lifecycle requires a new incident
//     ID for a new episode of the same fingerprint, so resolved
//     incidents must be allowed to keep their fingerprint.
//   - timeline_events and collection_runs gain incident/time indexes
//     for their list queries; indexes do not change the data model.
var migration1 = []string{
	`CREATE TABLE clusters (
	  id TEXT PRIMARY KEY,
	  display_name TEXT NOT NULL,
	  created_at_ms INTEGER NOT NULL
	)`,
	`CREATE TABLE incidents (
	  id TEXT PRIMARY KEY,
	  cluster_id TEXT NOT NULL REFERENCES clusters(id),
	  fingerprint TEXT NOT NULL,
	  failure_type TEXT NOT NULL,
	  severity TEXT NOT NULL DEFAULT 'warning',
	  status TEXT NOT NULL,
	  namespace TEXT NOT NULL,
	  pod_name TEXT,
	  pod_uid TEXT,
	  workload_kind TEXT,
	  workload_name TEXT,
	  workload_uid TEXT,
	  container_name TEXT,
	  first_seen_at_ms INTEGER NOT NULL,
	  last_seen_at_ms INTEGER NOT NULL,
	  resolved_at_ms INTEGER,
	  summary TEXT,
	  created_at_ms INTEGER NOT NULL,
	  updated_at_ms INTEGER NOT NULL
	)`,
	`CREATE UNIQUE INDEX idx_incidents_active_fingerprint
	  ON incidents(cluster_id, fingerprint)
	  WHERE status NOT IN ('resolved', 'closed')`,
	`CREATE INDEX idx_incidents_status_time ON incidents(status, last_seen_at_ms DESC)`,
	`CREATE INDEX idx_incidents_namespace_time ON incidents(namespace, first_seen_at_ms DESC)`,
	`CREATE TABLE evidence (
	  id TEXT PRIMARY KEY,
	  incident_id TEXT NOT NULL REFERENCES incidents(id) ON DELETE CASCADE,
	  kind TEXT NOT NULL,
	  source TEXT NOT NULL,
	  resource_kind TEXT,
	  resource_namespace TEXT,
	  resource_name TEXT,
	  resource_uid TEXT,
	  observed_at_ms INTEGER NOT NULL,
	  event_time_ms INTEGER,
	  content_json TEXT,
	  content_text TEXT,
	  content_hash TEXT NOT NULL,
	  redacted INTEGER NOT NULL DEFAULT 0,
	  collection_status TEXT NOT NULL DEFAULT 'success',
	  collector_version TEXT,
	  created_at_ms INTEGER NOT NULL
	)`,
	`CREATE INDEX idx_evidence_incident_time ON evidence(incident_id, observed_at_ms)`,
	`CREATE INDEX idx_evidence_kind_time ON evidence(kind, observed_at_ms)`,
	`CREATE TABLE hypotheses (
	  id TEXT PRIMARY KEY,
	  incident_id TEXT NOT NULL REFERENCES incidents(id) ON DELETE CASCADE,
	  rule_id TEXT NOT NULL,
	  summary TEXT NOT NULL,
	  confidence REAL NOT NULL,
	  rank INTEGER NOT NULL,
	  missing_evidence_json TEXT,
	  suggested_checks_json TEXT,
	  created_at_ms INTEGER NOT NULL
	)`,
	`CREATE TABLE hypothesis_evidence (
	  hypothesis_id TEXT NOT NULL REFERENCES hypotheses(id) ON DELETE CASCADE,
	  evidence_id TEXT NOT NULL REFERENCES evidence(id) ON DELETE CASCADE,
	  PRIMARY KEY(hypothesis_id, evidence_id)
	)`,
	`CREATE TABLE timeline_events (
	  id TEXT PRIMARY KEY,
	  incident_id TEXT NOT NULL REFERENCES incidents(id) ON DELETE CASCADE,
	  event_type TEXT NOT NULL,
	  occurred_at_ms INTEGER NOT NULL,
	  source_evidence_id TEXT REFERENCES evidence(id),
	  message TEXT NOT NULL,
	  attributes_json TEXT
	)`,
	`CREATE INDEX idx_timeline_incident_time ON timeline_events(incident_id, occurred_at_ms)`,
	`CREATE TABLE collection_runs (
	  id TEXT PRIMARY KEY,
	  incident_id TEXT NOT NULL REFERENCES incidents(id) ON DELETE CASCADE,
	  collector_name TEXT NOT NULL,
	  status TEXT NOT NULL,
	  started_at_ms INTEGER NOT NULL,
	  completed_at_ms INTEGER,
	  error_code TEXT,
	  error_message_redacted TEXT
	)`,
	`CREATE INDEX idx_collection_runs_incident_time ON collection_runs(incident_id, started_at_ms)`,
}

// migrations maps versions to their statements, in order.
var migrations = map[int][]string{
	1: migration1,
}

func (s *Store) migrate(ctx context.Context) error {
	if _, err := s.db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (
	  version INTEGER PRIMARY KEY,
	  applied_at_ms INTEGER NOT NULL
	)`); err != nil {
		return fmt.Errorf("sqlite: cannot create schema_migrations: %w", err)
	}

	var current sql.NullInt64
	if err := s.db.QueryRowContext(ctx,
		`SELECT MAX(version) FROM schema_migrations`).Scan(&current); err != nil {
		return fmt.Errorf("sqlite: cannot read schema version: %w", err)
	}

	for v := int(current.Int64) + 1; v <= schemaVersion; v++ {
		stmts, ok := migrations[v]
		if !ok {
			return fmt.Errorf("sqlite: migration %d is missing", v)
		}
		err := s.withTx(ctx, func(tx *sql.Tx) error {
			for _, stmt := range stmts {
				if _, err := tx.ExecContext(ctx, stmt); err != nil {
					return fmt.Errorf("migration %d: %w", v, err)
				}
			}
			if _, err := tx.ExecContext(ctx,
				`INSERT INTO schema_migrations (version, applied_at_ms) VALUES (?, ?)`,
				v, nowMillis()); err != nil {
				return fmt.Errorf("migration %d: %w", v, err)
			}
			return nil
		})
		if err != nil {
			return fmt.Errorf("sqlite: %w", err)
		}
	}
	return nil
}
