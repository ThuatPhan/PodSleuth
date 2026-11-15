package sqlite

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/ThuatPhan/PodSleuth/internal/domain"
	"github.com/ThuatPhan/PodSleuth/internal/storage"
)

func TestPersistenceAcrossReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "podsleuth.db")

	s1, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := s1.CreateIncident(ctx, baseIncident("inc-1")); err != nil {
		t.Fatal(err)
	}
	if err := s1.AddEvidence(ctx, []domain.Evidence{evidenceItem("ev-1", "inc-1")}); err != nil {
		t.Fatal(err)
	}
	if err := s1.AppendTimeline(ctx, []domain.TimelineEvent{{
		ID: "tl-1", IncidentID: "inc-1", EventType: "detected",
		OccurredAt: time.Now(), Message: "detected",
	}}); err != nil {
		t.Fatal(err)
	}
	if err := s1.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	// Reopen: everything must still be there.
	s2, err := Open(path)
	if err != nil {
		t.Fatalf("reopen error = %v", err)
	}
	defer s2.Close()

	got, err := s2.GetIncident(ctx, "inc-1")
	if err != nil {
		t.Fatalf("GetIncident after reopen error = %v", err)
	}
	if got.ID != "inc-1" || got.FailureType != "CrashLoopBackOff" {
		t.Errorf("incident after reopen = %+v", got)
	}
	evs, err := s2.ListEvidence(ctx, "inc-1")
	if err != nil || len(evs) != 1 || evs[0].ID != "ev-1" {
		t.Errorf("evidence after reopen = %v (err %v), want [ev-1]", evidenceIDs(evs), err)
	}
	tl, err := s2.ListTimeline(ctx, "inc-1")
	if err != nil || len(tl) != 1 || tl[0].ID != "tl-1" {
		t.Errorf("timeline after reopen = %v (err %v), want [tl-1]", timelineIDs(tl), err)
	}
}

func TestConcurrentAccess(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	const workers = 8
	var wg sync.WaitGroup
	errCh := make(chan error, workers)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			in := baseIncident("inc-" + string(rune('a'+n)))
			in.Resource.Name = "web-" + string(rune('a'+n))
			if err := s.CreateIncident(ctx, in); err != nil {
				errCh <- err
				return
			}
			ev := evidenceItem("ev-"+string(rune('a'+n)), in.ID)
			if err := s.AddEvidence(ctx, []domain.Evidence{ev}); err != nil {
				errCh <- err
			}
		}(i)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Errorf("concurrent write error = %v", err)
	}

	all, err := s.ListIncidents(ctx, storage.IncidentFilter{})
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != workers {
		t.Errorf("incidents after concurrent writes = %d, want %d", len(all), workers)
	}
}

func TestIncidentForeignKeyCluster(t *testing.T) {
	// CreateIncident ensures the cluster row itself, so this must
	// succeed without a prior EnsureCluster.
	s := openTestStore(t)
	in := baseIncident("inc-1")
	in.ClusterID = "brand-new-cluster"
	if err := s.CreateIncident(context.Background(), in); err != nil {
		t.Fatalf("CreateIncident() error = %v, want nil (cluster auto-ensured)", err)
	}
}

func TestEnsureClusterIdempotent(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	if err := s.EnsureCluster(ctx, "c1", "Display Name"); err != nil {
		t.Fatal(err)
	}
	// Second call with a different display name must not fail; the
	// first name wins.
	if err := s.EnsureCluster(ctx, "c1", "Other Name"); err != nil {
		t.Fatalf("EnsureCluster() second call error = %v", err)
	}
	var name string
	if err := s.db.QueryRow(`SELECT display_name FROM clusters WHERE id = 'c1'`).Scan(&name); err != nil {
		t.Fatal(err)
	}
	if name != "Display Name" {
		t.Errorf("display name = %q, want first value", name)
	}
}

func TestEvidenceValidation(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	if err := s.CreateIncident(ctx, baseIncident("inc-1")); err != nil {
		t.Fatal(err)
	}

	ev := evidenceItem("ev-1", "inc-1")
	ev.ContentHash = ""
	err := s.AddEvidence(ctx, []domain.Evidence{ev})
	if err == nil {
		t.Fatal("missing content hash: error = nil, want error")
	}

	ev = evidenceItem("ev-1", "inc-1")
	ev.ID = ""
	err = s.AddEvidence(ctx, []domain.Evidence{ev})
	if err == nil {
		t.Fatal("missing id: error = nil, want error")
	}
}

func TestTimelineValidation(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	if err := s.CreateIncident(ctx, baseIncident("inc-1")); err != nil {
		t.Fatal(err)
	}
	err := s.AppendTimeline(ctx, []domain.TimelineEvent{{
		ID: "tl-1", IncidentID: "inc-1", EventType: "detected", OccurredAt: time.Now(),
	}})
	if err == nil {
		t.Fatal("missing message: error = nil, want error")
	}
}

func TestAnalysisValidation(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	if err := s.CreateIncident(ctx, baseIncident("inc-1")); err != nil {
		t.Fatal(err)
	}
	err := s.SaveAnalysis(ctx, "inc-1", []domain.Hypothesis{{
		ID: "hyp-1", IncidentID: "inc-2", RuleID: "OOM-001", Summary: "mismatched incident",
	}})
	if err == nil {
		t.Fatal("mismatched incident id: error = nil, want error")
	}
}

func TestEmptyBatchesAreNoOps(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	if err := s.AddEvidence(ctx, nil); err != nil {
		t.Errorf("AddEvidence(nil) error = %v", err)
	}
	if err := s.AppendTimeline(ctx, nil); err != nil {
		t.Errorf("AppendTimeline(nil) error = %v", err)
	}
}

func TestContextCancellation(t *testing.T) {
	s := openTestStore(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := s.CreateIncident(ctx, baseIncident("inc-1"))
	if err == nil {
		t.Fatal("CreateIncident with canceled context: error = nil, want error")
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("error = %v, want context.Canceled", err)
	}
}

func evidenceIDs(in []domain.Evidence) []string {
	out := make([]string, 0, len(in))
	for _, e := range in {
		out = append(out, e.ID)
	}
	return out
}
