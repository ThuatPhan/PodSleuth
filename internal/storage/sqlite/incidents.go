package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/ThuatPhan/PodSleuth/internal/domain"
	"github.com/ThuatPhan/PodSleuth/internal/storage"
)

// EnsureCluster implements storage.Store.
func (s *Store) EnsureCluster(ctx context.Context, id, displayName string) error {
	if id == "" {
		return fmt.Errorf("sqlite: cluster id is required")
	}
	if displayName == "" {
		displayName = id
	}
	if _, err := s.db.ExecContext(ctx,
		`INSERT INTO clusters (id, display_name, created_at_ms) VALUES (?, ?, ?)
		 ON CONFLICT(id) DO NOTHING`,
		id, displayName, nowMillis()); err != nil {
		return wrapErr("ensure cluster", err)
	}
	return nil
}

// CreateIncident implements storage.Store. The cluster row is ensured
// in the same transaction so the foreign key always holds.
func (s *Store) CreateIncident(ctx context.Context, in domain.Incident) error {
	if err := validateIncident(in); err != nil {
		return err
	}
	return s.withTx(ctx, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO clusters (id, display_name, created_at_ms) VALUES (?, ?, ?)
			 ON CONFLICT(id) DO NOTHING`,
			in.ClusterID, in.ClusterID, nowMillis()); err != nil {
			return wrapErr("ensure cluster", err)
		}
		_, err := tx.ExecContext(ctx, `INSERT INTO incidents (
			id, cluster_id, fingerprint, failure_type, severity, status,
			namespace, pod_name, pod_uid, workload_kind, workload_name, workload_uid,
			container_name, first_seen_at_ms, last_seen_at_ms, resolved_at_ms,
			summary, created_at_ms, updated_at_ms
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			in.ID, in.ClusterID, in.Fingerprint, in.FailureType, in.Severity, string(in.Status),
			in.Resource.Namespace, in.Resource.Name, in.Resource.UID,
			in.WorkloadKind, in.WorkloadName, in.WorkloadUID,
			in.ContainerName, toMillis(in.FirstSeenAt), toMillis(in.LastSeenAt),
			nullMillis(in.ResolvedAt), in.Summary,
			toMillis(in.CreatedAt), toMillis(in.UpdatedAt),
		)
		if err != nil {
			return wrapErr("create incident "+in.ID, err)
		}
		return nil
	})
}

// GetIncident implements storage.Store.
func (s *Store) GetIncident(ctx context.Context, id string) (domain.Incident, error) {
	row := s.db.QueryRowContext(ctx, `SELECT
		id, cluster_id, fingerprint, failure_type, severity, status,
		namespace, pod_name, pod_uid, workload_kind, workload_name, workload_uid,
		container_name, first_seen_at_ms, last_seen_at_ms, resolved_at_ms,
		summary, created_at_ms, updated_at_ms
		FROM incidents WHERE id = ?`, id)
	return scanIncident(row)
}

// UpdateIncident implements storage.Store.
func (s *Store) UpdateIncident(ctx context.Context, in domain.Incident) error {
	if err := validateIncident(in); err != nil {
		return err
	}
	res, err := s.db.ExecContext(ctx, `UPDATE incidents SET
		cluster_id = ?, fingerprint = ?, failure_type = ?, severity = ?, status = ?,
		namespace = ?, pod_name = ?, pod_uid = ?,
		workload_kind = ?, workload_name = ?, workload_uid = ?,
		container_name = ?, first_seen_at_ms = ?, last_seen_at_ms = ?, resolved_at_ms = ?,
		summary = ?, created_at_ms = ?, updated_at_ms = ?
		WHERE id = ?`,
		in.ClusterID, in.Fingerprint, in.FailureType, in.Severity, string(in.Status),
		in.Resource.Namespace, in.Resource.Name, in.Resource.UID,
		in.WorkloadKind, in.WorkloadName, in.WorkloadUID,
		in.ContainerName, toMillis(in.FirstSeenAt), toMillis(in.LastSeenAt),
		nullMillis(in.ResolvedAt), in.Summary,
		toMillis(in.CreatedAt), toMillis(in.UpdatedAt),
		in.ID,
	)
	if err != nil {
		return wrapErr("update incident "+in.ID, err)
	}
	if n, err := res.RowsAffected(); err == nil && n == 0 {
		return fmt.Errorf("update incident %s: %w", in.ID, storage.ErrNotFound)
	}
	return nil
}

// ListIncidents implements storage.Store.
func (s *Store) ListIncidents(ctx context.Context, filter storage.IncidentFilter) ([]domain.Incident, error) {
	var where []string
	var args []any
	if filter.Status != "" {
		where = append(where, "status = ?")
		args = append(args, string(filter.Status))
	}
	if filter.Namespace != "" {
		where = append(where, "namespace = ?")
		args = append(args, filter.Namespace)
	}
	query := `SELECT
		id, cluster_id, fingerprint, failure_type, severity, status,
		namespace, pod_name, pod_uid, workload_kind, workload_name, workload_uid,
		container_name, first_seen_at_ms, last_seen_at_ms, resolved_at_ms,
		summary, created_at_ms, updated_at_ms
		FROM incidents`
	if len(where) > 0 {
		query += " WHERE " + strings.Join(where, " AND ")
	}
	query += " ORDER BY last_seen_at_ms DESC"

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, wrapErr("list incidents", err)
	}
	defer rows.Close()

	var out []domain.Incident
	for rows.Next() {
		inc, err := scanIncident(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, inc)
	}
	if err := rows.Err(); err != nil {
		return nil, wrapErr("list incidents", err)
	}
	return out, nil
}

func validateIncident(in domain.Incident) error {
	switch {
	case in.ID == "":
		return fmt.Errorf("sqlite: incident id is required")
	case in.ClusterID == "":
		return fmt.Errorf("sqlite: incident %s cluster id is required", in.ID)
	case in.Fingerprint == "":
		return fmt.Errorf("sqlite: incident %s fingerprint is required", in.ID)
	case in.FailureType == "":
		return fmt.Errorf("sqlite: incident %s failure type is required", in.ID)
	case in.Status == "":
		return fmt.Errorf("sqlite: incident %s status is required", in.ID)
	case in.Resource.Namespace == "":
		return fmt.Errorf("sqlite: incident %s namespace is required", in.ID)
	case in.FirstSeenAt.IsZero() || in.LastSeenAt.IsZero():
		return fmt.Errorf("sqlite: incident %s first/last seen times are required", in.ID)
	case in.CreatedAt.IsZero() || in.UpdatedAt.IsZero():
		return fmt.Errorf("sqlite: incident %s created/updated times are required", in.ID)
	}
	return nil
}

// scanner matches *sql.Row and *sql.Rows.
type scanner interface {
	Scan(dest ...any) error
}

func scanIncident(row scanner) (domain.Incident, error) {
	var in domain.Incident
	var status string
	var firstSeen, lastSeen, createdAt, updatedAt int64
	var resolvedAt sql.NullInt64
	err := row.Scan(
		&in.ID, &in.ClusterID, &in.Fingerprint, &in.FailureType, &in.Severity, &status,
		&in.Resource.Namespace, &in.Resource.Name, &in.Resource.UID,
		&in.WorkloadKind, &in.WorkloadName, &in.WorkloadUID,
		&in.ContainerName, &firstSeen, &lastSeen, &resolvedAt,
		&in.Summary, &createdAt, &updatedAt,
	)
	if err != nil {
		return domain.Incident{}, wrapErr("get incident", err)
	}
	in.Status = domain.IncidentStatus(status)
	in.Resource.ClusterID = in.ClusterID
	in.FirstSeenAt = fromMillis(firstSeen)
	in.LastSeenAt = fromMillis(lastSeen)
	in.ResolvedAt = timeFromNull(resolvedAt)
	in.CreatedAt = fromMillis(createdAt)
	in.UpdatedAt = fromMillis(updatedAt)
	return in, nil
}
