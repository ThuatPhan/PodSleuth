package cli

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/ThuatPhan/PodSleuth/internal/domain"
	"github.com/ThuatPhan/PodSleuth/internal/storage"
	sqlitestore "github.com/ThuatPhan/PodSleuth/internal/storage/sqlite"
)

// seedStore opens a temp SQLite store, persists one incident with
// evidence, analysis, timeline and a collection run, and returns Deps
// whose NewStore opens the same database (the CLI's own open acts as a
// reopen from disk).
func seedStore(t *testing.T) (Deps, storage.Store, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "podsleuth.db")
	store, err := sqlitestore.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	deps := Deps{NewStore: func(string) (storage.Store, error) {
		return sqlitestore.Open(path)
	}}
	ctx := context.Background()

	now := time.Date(2026, 11, 15, 12, 0, 0, 0, time.UTC)
	incident := domain.Incident{
		ID:          "inc-1",
		ClusterID:   "test-cluster",
		Fingerprint: "fp-1",
		FailureType: "CrashLoopBackOff",
		Severity:    domain.DefaultSeverity,
		Status:      domain.StatusDetected,
		Resource: domain.ResourceRef{
			ClusterID: "test-cluster", Kind: "Pod", Namespace: "app", Name: "web-1", UID: "pod-uid-1",
		},
		ContainerName: "app",
		FirstSeenAt:   now,
		LastSeenAt:    now,
		CreatedAt:     now,
		UpdatedAt:     now,
		Summary:       "CrashLoopBackOff on pod app/web-1 container app",
	}
	if err := store.CreateIncident(ctx, incident); err != nil {
		t.Fatal(err)
	}

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "web-1", UID: types.UID("pod-uid-1")},
		Status:     corev1.PodStatus{Phase: corev1.PodRunning},
	}
	podJSON, err := json.Marshal(pod)
	if err != nil {
		t.Fatal(err)
	}
	evidenceItems := []domain.Evidence{
		{
			ID: "ev-1", IncidentID: "inc-1", Kind: "pod", Source: "api-server",
			Resource:   domain.ResourceRef{Kind: "Pod", Namespace: "app", Name: "web-1", UID: "pod-uid-1"},
			ObservedAt: now, Content: podJSON, ContentHash: "hash-1", CollectionStatus: "success",
		},
		{
			ID: "ev-2", IncidentID: "inc-1", Kind: "previous-logs", Source: "api-server",
			ObservedAt: now, Text: "panic: runtime error\n", ContentHash: "hash-2", CollectionStatus: "success",
		},
	}
	if err := store.AddEvidence(ctx, evidenceItems); err != nil {
		t.Fatal(err)
	}
	if err := store.SaveAnalysis(ctx, "inc-1", []domain.Hypothesis{{
		ID: "inc-1:CRASH-001", IncidentID: "inc-1", RuleID: "CRASH-001",
		Summary: "Container app is in CrashLoopBackOff after 5 restarts.", Score: 0.7,
		EvidenceIDs:     []string{"ev-1", "ev-2"},
		MissingEvidence: []string{"previous container logs"},
		SuggestedChecks: []string{"kubectl logs web-1 -n app --previous"},
	}}); err != nil {
		t.Fatal(err)
	}
	if err := store.AppendTimeline(ctx, []domain.TimelineEvent{{
		ID: "tl-1", IncidentID: "inc-1", EventType: "detected", OccurredAt: now, Message: "CrashLoopBackOff detected",
	}}); err != nil {
		t.Fatal(err)
	}
	if err := store.CreateCollectionRun(ctx, domain.CollectionRun{
		ID: "run-1", IncidentID: "inc-1", CollectorName: "logs", Status: "success", StartedAt: now,
	}); err != nil {
		t.Fatal(err)
	}
	return deps, store, path
}

// TestReportCommandAfterReopen seeds a database, closes it, reopens
// it, and runs the actual CLI report command against the reopened
// database in both formats.
func TestReportCommandAfterReopen(t *testing.T) {
	deps, _, path := seedStore(t)

	// Close the seeding store; the CLI must open the database itself.
	// seedStore registered a cleanup Close, so reopen a fresh handle
	// first to prove the data survived, then close it again.
	reopened, err := sqlitestore.Open(path)
	if err != nil {
		t.Fatalf("reopen error = %v", err)
	}
	got, err := reopened.GetIncident(context.Background(), "inc-1")
	if err != nil {
		t.Fatalf("GetIncident after reopen error = %v", err)
	}
	if got.ID != "inc-1" {
		t.Errorf("incident after reopen = %+v", got)
	}
	if err := reopened.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	out, err := runCommand(t, deps, "report", "inc-1")
	if err != nil {
		t.Fatalf("report (markdown) error = %v", err)
	}
	for _, want := range []string{
		"# PodSleuth Investigation Report",
		"- ID: inc-1",
		"### 1. CRASH-001 — score 0.70",
		"- Supporting evidence: ev-1, ev-2",
		"- 2026-11-15T12:00:00Z detected: CrashLoopBackOff detected",
		"## Gaps and unknowns",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("markdown report missing %q", want)
		}
	}

	jsonOut, err := runCommand(t, deps, "report", "inc-1", "--output", "json")
	if err != nil {
		t.Fatalf("report (json) error = %v", err)
	}
	var doc struct {
		Incident   domain.Incident     `json:"incident"`
		Hypotheses []domain.Hypothesis `json:"hypotheses"`
		Evidence   []domain.Evidence   `json:"evidence"`
	}
	if err := json.Unmarshal([]byte(jsonOut), &doc); err != nil {
		t.Fatalf("cli json output is not parseable: %v\n%s", err, jsonOut)
	}
	if doc.Incident.ID != "inc-1" || len(doc.Hypotheses) != 1 || len(doc.Evidence) != 2 {
		t.Errorf("parsed document = incident %q, %d hypotheses, %d evidence",
			doc.Incident.ID, len(doc.Hypotheses), len(doc.Evidence))
	}
	if doc.Hypotheses[0].Score != 0.7 {
		t.Errorf("hypothesis score = %v, want 0.7", doc.Hypotheses[0].Score)
	}
}

func TestReportCommandMarkdown(t *testing.T) {
	deps, _, _ := seedStore(t)

	out, err := runCommand(t, deps, "report", "inc-1")
	if err != nil {
		t.Fatalf("report error = %v", err)
	}
	for _, want := range []string{
		"# PodSleuth Investigation Report",
		"- ID: inc-1",
		"### 1. CRASH-001 — score 0.70",
		"- Supporting evidence: ev-1, ev-2",
		"- 2026-11-15T12:00:00Z detected: CrashLoopBackOff detected",
		"### ev-1 — pod (api-server)",
		"## Gaps and unknowns",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("report output missing %q", want)
		}
	}
}

func TestReportCommandJSON(t *testing.T) {
	deps, _, _ := seedStore(t)

	out, err := runCommand(t, deps, "report", "inc-1", "--output", "json")
	if err != nil {
		t.Fatalf("report error = %v", err)
	}
	var doc struct {
		Incident   domain.Incident     `json:"incident"`
		Hypotheses []domain.Hypothesis `json:"hypotheses"`
	}
	if err := json.Unmarshal([]byte(out), &doc); err != nil {
		t.Fatalf("output is not valid JSON: %v\n%s", err, out)
	}
	if doc.Incident.ID != "inc-1" || len(doc.Hypotheses) != 1 || doc.Hypotheses[0].RuleID != "CRASH-001" {
		t.Errorf("document = %+v", doc)
	}
}

func TestReportCommandUnknownIncident(t *testing.T) {
	deps, _, _ := seedStore(t)

	_, err := runCommand(t, deps, "report", "missing")
	if err == nil {
		t.Fatal("report error = nil, want error")
	}
	if exitCodeOf(err) != 2 {
		t.Errorf("exit code = %d, want 2", exitCodeOf(err))
	}
	if !strings.Contains(err.Error(), `incident "missing" not found`) {
		t.Errorf("error = %v, want useful not-found message", err)
	}
}

func TestReportCommandInvalidFormat(t *testing.T) {
	deps, _, _ := seedStore(t)

	_, err := runCommand(t, deps, "report", "inc-1", "--output", "html")
	if err == nil {
		t.Fatal("report error = nil, want error")
	}
	if exitCodeOf(err) != 2 {
		t.Errorf("exit code = %d, want 2", exitCodeOf(err))
	}
	if !strings.Contains(err.Error(), "invalid output format") {
		t.Errorf("error = %v", err)
	}
}

func TestReportCommandArgumentValidation(t *testing.T) {
	deps, _, _ := seedStore(t)

	if _, err := runCommand(t, deps, "report"); err == nil || exitCodeOf(err) != 2 {
		t.Errorf("no args: err = %v, want exit 2", err)
	}
	if _, err := runCommand(t, deps, "report", "a", "b"); err == nil || exitCodeOf(err) != 2 {
		t.Errorf("two args: err = %v, want exit 2", err)
	}
}

func TestReportCommandPartialCollection(t *testing.T) {
	// A fresh store seeded with a truncated evidence item; the CLI
	// opens it through the injected constructor.
	path := filepath.Join(t.TempDir(), "podsleuth.db")
	store, err := sqlitestore.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	now := time.Date(2026, 11, 15, 12, 0, 0, 0, time.UTC)
	incident := domain.Incident{
		ID: "inc-1", ClusterID: "test-cluster", Fingerprint: "fp-1",
		FailureType: "CrashLoopBackOff", Status: domain.StatusDetected,
		Resource:    domain.ResourceRef{ClusterID: "test-cluster", Kind: "Pod", Namespace: "app", Name: "web-1", UID: "pod-uid-1"},
		FirstSeenAt: now, LastSeenAt: now, CreatedAt: now, UpdatedAt: now,
	}
	if err := store.CreateIncident(ctx, incident); err != nil {
		t.Fatal(err)
	}
	if err := store.AddEvidence(ctx, []domain.Evidence{{
		ID: "ev-1", IncidentID: "inc-1", Kind: "previous-logs", Source: "api-server",
		ObservedAt: now, Text: "log", ContentHash: "h", CollectionStatus: "truncated",
	}}); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	deps := Deps{NewStore: func(string) (storage.Store, error) {
		return sqlitestore.Open(path)
	}}
	out, err := runCommand(t, deps, "report", "inc-1")
	if err == nil {
		t.Fatal("report error = nil, want partial-collection error (exit 3)")
	}
	var partial *partialReportError
	if !errors.As(err, &partial) {
		t.Errorf("error = %v (%T), want *partialReportError", err, err)
	}
	if exitCodeOf(err) != 3 {
		t.Errorf("exit code = %d, want 3", exitCodeOf(err))
	}
	// The report itself must still have been printed.
	if !strings.Contains(out, "# PodSleuth Investigation Report") {
		t.Errorf("output = %q, want rendered report", out)
	}
	if !strings.Contains(out, "Evidence ev-1 was collected with status") {
		t.Errorf("output = %q, want gap noting truncated evidence", out)
	}
}

func TestReportCommandStorageOpenFailure(t *testing.T) {
	deps := Deps{NewStore: func(string) (storage.Store, error) {
		return nil, errors.New("disk unavailable")
	}}
	_, err := runCommand(t, deps, "report", "inc-1")
	if err == nil {
		t.Fatal("report error = nil, want error")
	}
	if exitCodeOf(err) != 1 {
		t.Errorf("exit code = %d, want 1 (unexpected internal error)", exitCodeOf(err))
	}
	if !strings.Contains(err.Error(), "open storage") {
		t.Errorf("error = %v, want storage-open context", err)
	}
}
