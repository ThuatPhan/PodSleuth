package sqlite

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/ThuatPhan/PodSleuth/internal/domain"
	"github.com/ThuatPhan/PodSleuth/internal/storage"
)

// openTestStore opens a Store on a temporary database file.
func openTestStore(t *testing.T) *Store {
	t.Helper()
	path := filepath.Join(t.TempDir(), "podsleuth.db")
	s, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func baseIncident(id string) domain.Incident {
	now := time.Date(2026, 11, 15, 12, 0, 0, 0, time.UTC)
	return domain.Incident{
		ID:          id,
		ClusterID:   "test-cluster",
		Fingerprint: "fp-" + id,
		FailureType: "CrashLoopBackOff",
		Severity:    domain.DefaultSeverity,
		Status:      domain.StatusDetected,
		Resource: domain.ResourceRef{
			ClusterID:  "test-cluster",
			APIVersion: "v1",
			Kind:       "Pod",
			Namespace:  "app",
			Name:       "web-1",
			UID:        "pod-uid-1",
		},
		WorkloadKind:  "ReplicaSet",
		WorkloadName:  "web-7d9f",
		WorkloadUID:   "rs-uid-1",
		ContainerName: "app",
		FirstSeenAt:   now,
		LastSeenAt:    now,
		CreatedAt:     now,
		UpdatedAt:     now,
		Summary:       "CrashLoopBackOff on pod app/web-1 container app",
	}
}

func TestOpenCreatesSchema(t *testing.T) {
	s := openTestStore(t)

	var version int
	if err := s.db.QueryRow(`SELECT MAX(version) FROM schema_migrations`).Scan(&version); err != nil {
		t.Fatal(err)
	}
	if version != schemaVersion {
		t.Errorf("schema version = %d, want %d", version, schemaVersion)
	}

	// Reopening the same database must not re-apply or fail.
	path := filepath.Join(t.TempDir(), "podsleuth.db")
	s1, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := s1.Close(); err != nil {
		t.Fatal(err)
	}
	s2, err := Open(path)
	if err != nil {
		t.Fatalf("reopen error = %v", err)
	}
	defer s2.Close()
}

func TestOpenFilePermissions(t *testing.T) {
	path := filepath.Join(t.TempDir(), "podsleuth.db")
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if perm := info.Mode().Perm(); perm != 0o600 {
		t.Errorf("file mode = %o, want 600 (user-only)", perm)
	}
}

func TestIncidentRoundTrip(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	in := baseIncident("inc-1")

	if err := s.CreateIncident(ctx, in); err != nil {
		t.Fatalf("CreateIncident() error = %v", err)
	}
	got, err := s.GetIncident(ctx, "inc-1")
	if err != nil {
		t.Fatalf("GetIncident() error = %v", err)
	}
	// The schema stores the pod identity but not the resource
	// APIVersion/Kind; compare persisted fields only.
	in.Resource.APIVersion = ""
	in.Resource.Kind = ""
	if got != in {
		t.Errorf("round trip mismatch:\n got %+v\nwant %+v", got, in)
	}
}

func TestGetIncidentNotFound(t *testing.T) {
	s := openTestStore(t)
	_, err := s.GetIncident(context.Background(), "missing")
	if err == nil || !isNotFound(err) {
		t.Errorf("GetIncident() error = %v, want storage.ErrNotFound", err)
	}
}

func TestUpdateIncident(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	in := baseIncident("inc-1")
	if err := s.CreateIncident(ctx, in); err != nil {
		t.Fatal(err)
	}

	resolved := time.Date(2026, 11, 15, 12, 5, 0, 0, time.UTC)
	in.Status = domain.StatusResolved
	in.ResolvedAt = &resolved
	in.LastSeenAt = resolved
	in.UpdatedAt = resolved
	if err := s.UpdateIncident(ctx, in); err != nil {
		t.Fatalf("UpdateIncident() error = %v", err)
	}

	got, err := s.GetIncident(ctx, "inc-1")
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != domain.StatusResolved {
		t.Errorf("status = %q, want resolved", got.Status)
	}
	if got.ResolvedAt == nil || !got.ResolvedAt.Equal(resolved) {
		t.Errorf("ResolvedAt = %v, want %v", got.ResolvedAt, resolved)
	}
	if !got.UpdatedAt.Equal(resolved) {
		t.Errorf("UpdatedAt = %v, want %v", got.UpdatedAt, resolved)
	}
}

func TestUpdateIncidentNotFound(t *testing.T) {
	s := openTestStore(t)
	err := s.UpdateIncident(context.Background(), baseIncident("missing"))
	if err == nil || !isNotFound(err) {
		t.Errorf("UpdateIncident() error = %v, want storage.ErrNotFound", err)
	}
}

func TestListIncidentsFilters(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	a := baseIncident("inc-a") // namespace app, detected
	if err := s.CreateIncident(ctx, a); err != nil {
		t.Fatal(err)
	}
	b := baseIncident("inc-b")
	b.Resource.Namespace = "other"
	b.LastSeenAt = b.LastSeenAt.Add(time.Minute)
	b.UpdatedAt = b.LastSeenAt
	if err := s.CreateIncident(ctx, b); err != nil {
		t.Fatal(err)
	}
	c := baseIncident("inc-c")
	c.Status = domain.StatusResolved
	c.LastSeenAt = c.LastSeenAt.Add(2 * time.Minute)
	c.UpdatedAt = c.LastSeenAt
	if err := s.CreateIncident(ctx, c); err != nil {
		t.Fatal(err)
	}

	byStatus, err := s.ListIncidents(ctx, storage.IncidentFilter{Status: domain.StatusDetected})
	if err != nil {
		t.Fatal(err)
	}
	if len(byStatus) != 2 || byStatus[0].ID != "inc-b" || byStatus[1].ID != "inc-a" {
		t.Errorf("status filter = %v, want [inc-b inc-a] (last seen desc)", ids(byStatus))
	}

	byNS, err := s.ListIncidents(ctx, storage.IncidentFilter{Namespace: "app"})
	if err != nil {
		t.Fatal(err)
	}
	if len(byNS) != 2 {
		t.Errorf("namespace filter = %v, want [inc-c inc-a]", ids(byNS))
	}

	all, err := s.ListIncidents(ctx, storage.IncidentFilter{})
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 3 {
		t.Errorf("all = %v, want 3 incidents", ids(all))
	}
}

func TestCreateIncidentValidation(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	in := baseIncident("inc-x")
	in.ID = ""
	if err := s.CreateIncident(ctx, in); err == nil {
		t.Error("missing id: error = nil, want error")
	}

	in = baseIncident("inc-x")
	in.Fingerprint = ""
	if err := s.CreateIncident(ctx, in); err == nil {
		t.Error("missing fingerprint: error = nil, want error")
	}

	in = baseIncident("inc-x")
	in.FirstSeenAt = time.Time{}
	if err := s.CreateIncident(ctx, in); err == nil {
		t.Error("missing first seen: error = nil, want error")
	}
}

func TestCreateIncidentDuplicateActiveFingerprint(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	a := baseIncident("inc-a")
	a.Fingerprint = "same-fp"
	if err := s.CreateIncident(ctx, a); err != nil {
		t.Fatal(err)
	}

	b := baseIncident("inc-b")
	b.Fingerprint = "same-fp"
	err := s.CreateIncident(ctx, b)
	if err == nil || !isAlreadyExists(err) {
		t.Fatalf("second active incident error = %v, want storage.ErrAlreadyExists", err)
	}

	// Resolving the first frees the fingerprint for a new episode.
	resolved := a.LastSeenAt.Add(time.Minute)
	a.Status = domain.StatusResolved
	a.ResolvedAt = &resolved
	a.UpdatedAt = resolved
	if err := s.UpdateIncident(ctx, a); err != nil {
		t.Fatal(err)
	}
	if err := s.CreateIncident(ctx, b); err != nil {
		t.Fatalf("new episode after resolution error = %v, want nil", err)
	}
}

func TestCreateIncidentDuplicateID(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	if err := s.CreateIncident(ctx, baseIncident("inc-1")); err != nil {
		t.Fatal(err)
	}
	err := s.CreateIncident(ctx, baseIncident("inc-1"))
	if err == nil || !isAlreadyExists(err) {
		t.Errorf("duplicate id error = %v, want storage.ErrAlreadyExists", err)
	}
}

func isNotFound(err error) bool {
	return errors.Is(err, storage.ErrNotFound)
}

func isAlreadyExists(err error) bool {
	return errors.Is(err, storage.ErrAlreadyExists)
}

func ids(in []domain.Incident) []string {
	out := make([]string, 0, len(in))
	for _, i := range in {
		out = append(out, i.ID)
	}
	return out
}
