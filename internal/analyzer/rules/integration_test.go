package rules

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	clientgotesting "k8s.io/client-go/testing"

	"github.com/ThuatPhan/PodSleuth/internal/analyzer"
	"github.com/ThuatPhan/PodSleuth/internal/collector"
	"github.com/ThuatPhan/PodSleuth/internal/collector/events"
	"github.com/ThuatPhan/PodSleuth/internal/domain"
	"github.com/ThuatPhan/PodSleuth/internal/evidence"
	sqlitestore "github.com/ThuatPhan/PodSleuth/internal/storage/sqlite"
)

// TestEventsEvidenceShapeDriftGuard produces an events evidence item
// with the real events collector and verifies the rules package parses
// it. If the collector's content shape changes, this test fails.
func TestEventsEvidenceShapeDriftGuard(t *testing.T) {
	cs := fake.NewSimpleClientset()
	cs.PrependReactor("list", "events", func(action clientgotesting.Action) (bool, runtime.Object, error) {
		return true, &corev1.EventList{Items: []corev1.Event{{
			ObjectMeta:     metav1.ObjectMeta{Namespace: "app", Name: "web-1.1"},
			InvolvedObject: corev1.ObjectReference{Kind: "Pod", Namespace: "app", Name: "web-1", UID: types.UID("pod-uid-1")},
			Type:           "Warning",
			Reason:         "FailedScheduling",
			Message:        "0/3 nodes are available: insufficient memory",
			Count:          4,
			Source:         corev1.EventSource{Component: "default-scheduler"},
		}}}, nil
	})
	c := events.NewEventsCollector(cs, evidence.DefaultOptions())
	result, err := c.Collect(context.Background(), collector.Request{
		Incident: testIncident(),
		Signal:   domain.FailureSignal{Type: "FailedScheduling"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Evidence) != 1 {
		t.Fatalf("collector evidence = %d, want 1", len(result.Evidence))
	}

	// The real collector output must drive SCHED-001.
	input := analyzer.Input{Incident: testIncident(), Evidence: result.Evidence}
	h := evaluate(t, &SchedRule{}, input)
	if h == nil {
		t.Fatal("SCHED-001 did not fire on collector-produced events evidence")
	}
	if !strings.Contains(h.Summary, "0/3 nodes are available") {
		t.Errorf("summary = %q", h.Summary)
	}
}

// TestAnalysisPersistsThroughStore proves the analyzer output round
// trips through the existing storage interface.
func TestAnalysisPersistsThroughStore(t *testing.T) {
	path := filepath.Join(t.TempDir(), "podsleuth.db")
	store, err := sqlitestore.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	ctx := context.Background()

	incident := testIncident()
	if err := store.CreateIncident(ctx, incident); err != nil {
		t.Fatal(err)
	}
	pod := podFixture(corev1.PodStatus{
		ContainerStatuses: []corev1.ContainerStatus{cstatus("worker", 1, running(), terminated(137, "OOMKilled"))},
	}, corev1.PodSpec{})
	ev := podEvItem(t, "ev-1", pod)
	if err := store.AddEvidence(ctx, []domain.Evidence{ev}); err != nil {
		t.Fatal(err)
	}

	engine, err := analyzer.NewEngine(All()...)
	if err != nil {
		t.Fatal(err)
	}
	result, err := engine.Analyze(ctx, analyzer.Input{Incident: incident, Evidence: []domain.Evidence{ev}})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Hypotheses) != 1 || result.Hypotheses[0].RuleID != IDOOM {
		t.Fatalf("hypotheses = %+v, want OOM-001", result.Hypotheses)
	}

	if err := store.SaveAnalysis(ctx, incident.ID, result.Hypotheses); err != nil {
		t.Fatalf("SaveAnalysis() error = %v", err)
	}
	got, err := store.GetAnalysis(ctx, incident.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 {
		t.Fatalf("persisted hypotheses = %d, want 1", len(got))
	}
	h := got[0]
	want := result.Hypotheses[0]
	if h.ID != want.ID || h.RuleID != want.RuleID || h.Summary != want.Summary || h.Score != want.Score {
		t.Errorf("persisted hypothesis = %+v, want %+v", h, want)
	}
	if len(h.EvidenceIDs) != 1 || h.EvidenceIDs[0] != "ev-1" {
		t.Errorf("persisted evidence links = %v, want [ev-1]", h.EvidenceIDs)
	}
}

// TestEngineEndToEndMultipleHypotheses runs the full engine over a pod
// that supports two rules and checks ranking and coverage.
func TestEngineEndToEndMultipleHypotheses(t *testing.T) {
	pod := podFixture(corev1.PodStatus{
		Phase: corev1.PodRunning,
		ContainerStatuses: []corev1.ContainerStatus{
			cstatus("app", 5, waiting("CrashLoopBackOff"), terminated(1, "Error")),
		},
	}, corev1.PodSpec{})
	input := analyzer.Input{
		Incident: testIncident(),
		Evidence: []domain.Evidence{
			podEvItem(t, "ev-pod", pod),
			logsEvItem("ev-logs", "previous-logs", "panic: something broke\n"),
		},
	}
	engine, err := analyzer.NewEngine(All()...)
	if err != nil {
		t.Fatal(err)
	}
	res, err := engine.Analyze(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Hypotheses) != 1 {
		t.Fatalf("hypotheses = %d, want 1 (only CRASH-001 matches)", len(res.Hypotheses))
	}
	if res.Hypotheses[0].RuleID != IDCrash {
		t.Errorf("rule = %q, want CRASH-001", res.Hypotheses[0].RuleID)
	}
	if res.Hypotheses[0].Score != 0.8 {
		t.Errorf("score = %v, want 0.8 (direct status + logs)", res.Hypotheses[0].Score)
	}
	if len(res.Coverage) != 8 {
		t.Errorf("coverage = %d rules, want 8", len(res.Coverage))
	}
	if !strings.Contains(res.Summary, "CRASH-001") {
		t.Errorf("summary = %q, want strongest rule named", res.Summary)
	}
}
