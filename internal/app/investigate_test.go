package app

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	clientgotesting "k8s.io/client-go/testing"

	"github.com/ThuatPhan/PodSleuth/internal/analyzer"
	"github.com/ThuatPhan/PodSleuth/internal/analyzer/rules"
	"github.com/ThuatPhan/PodSleuth/internal/collector"
	"github.com/ThuatPhan/PodSleuth/internal/collector/events"
	"github.com/ThuatPhan/PodSleuth/internal/collector/logs"
	"github.com/ThuatPhan/PodSleuth/internal/collector/pod"
	"github.com/ThuatPhan/PodSleuth/internal/detector"
	"github.com/ThuatPhan/PodSleuth/internal/domain"
	"github.com/ThuatPhan/PodSleuth/internal/evidence"
	"github.com/ThuatPhan/PodSleuth/internal/storage"
	sqlitestore "github.com/ThuatPhan/PodSleuth/internal/storage/sqlite"
)

// env wires an Investigator to a fake clientset and a temp SQLite
// store with deterministic IDs.
type env struct {
	t      *testing.T
	client *fake.Clientset
	store  storage.Store
	inv    *Investigator
	idSeq  int
}

func newEnv(t *testing.T, objects ...runtime.Object) *env {
	t.Helper()
	client := fake.NewSimpleClientset(objects...)
	path := t.TempDir() + "/podsleuth.db"
	store, err := sqlitestore.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })

	e := &env{t: t, client: client, store: store}
	e.inv = NewInvestigator(
		client,
		"test-cluster",
		store,
		detector.NewPodDetector("test-cluster"),
		collector.NewRegistry(
			pod.NewPodCollector(client, evidence.DefaultOptions()),
			logs.NewLogsCollector(client, logs.Options{PreviousLogs: true, CurrentLogs: true}, evidence.DefaultOptions()),
			events.NewEventsCollector(client, evidence.DefaultOptions()),
		),
		mustTestEngine(t),
	)
	return e
}

func filepath_Join(t *testing.T) string {
	return t.TempDir() + "/podsleuth.db"
}

func mustTestEngine(t *testing.T) analyzer.Analyzer {
	t.Helper()
	engine, err := analyzer.NewEngine(rules.All()...)
	if err != nil {
		t.Fatal(err)
	}
	return engine
}

// nextID returns deterministic IDs for timeline events and runs.
func (e *env) nextID() string {
	e.idSeq++
	return "id-" + string(rune('a'+e.idSeq-1))
}

func (e *env) investigate(ctx context.Context, namespace, name string) (Investigation, error) {
	e.inv.now = func() time.Time { return time.Date(2026, 11, 15, 12, 0, 0, 0, time.UTC) }
	e.inv.newID = e.nextID
	return e.inv.Investigate(ctx, namespace, name)
}

func crashPod(name string, containers ...corev1.ContainerStatus) *corev1.Pod {
	controller := true
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:       "app",
			Name:            name,
			UID:             types.UID("uid-" + name),
			ResourceVersion: "1000",
			OwnerReferences: []metav1.OwnerReference{{Kind: "ReplicaSet", Name: name + "-rs", UID: types.UID("rs-uid-" + name), Controller: &controller}},
		},
		Status: corev1.PodStatus{
			Phase:             corev1.PodRunning,
			ContainerStatuses: containers,
		},
	}
}

func crashLoopStatus(container string, restarts int32) corev1.ContainerStatus {
	return corev1.ContainerStatus{
		Name:                 container,
		RestartCount:         restarts,
		State:                corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "CrashLoopBackOff"}},
		LastTerminationState: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 1, Reason: "Error"}},
	}
}

func oomStatus(container string, restarts int32) corev1.ContainerStatus {
	return corev1.ContainerStatus{
		Name:                 container,
		RestartCount:         restarts,
		State:                corev1.ContainerState{Running: &corev1.ContainerStateRunning{}},
		LastTerminationState: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 137, Reason: "OOMKilled"}},
	}
}

func healthyStatus(container string) corev1.ContainerStatus {
	return corev1.ContainerStatus{
		Name:  container,
		State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}},
	}
}

func TestInvestigateCrashLoopPod(t *testing.T) {
	e := newEnv(t, crashPod("web-1", crashLoopStatus("app", 5)))
	seedBackOffEvent(e.client, "web-1", "pod-uid-web-1")

	result, err := e.investigate(context.Background(), "app", "web-1")
	if err != nil {
		t.Fatalf("Investigate() error = %v", err)
	}
	if result.Healthy || len(result.IncidentIDs) != 1 {
		t.Fatalf("result = %+v, want one incident", result)
	}
	incidentID := result.IncidentIDs[0]

	inc, err := e.store.GetIncident(context.Background(), incidentID)
	if err != nil {
		t.Fatal(err)
	}
	if inc.FailureType != "CrashLoopBackOff" || inc.ContainerName != "app" || inc.Status != domain.StatusDetected {
		t.Errorf("incident = %+v", inc)
	}
	if inc.WorkloadKind != "ReplicaSet" || inc.WorkloadUID != "rs-uid-web-1" {
		t.Errorf("workload = %s/%s/%s", inc.WorkloadKind, inc.WorkloadName, inc.WorkloadUID)
	}

	evs, err := e.store.ListEvidence(context.Background(), incidentID)
	if err != nil {
		t.Fatal(err)
	}
	kinds := map[string]bool{}
	for _, ev := range evs {
		if ev.IncidentID != incidentID {
			t.Errorf("evidence %s IncidentID = %q", ev.ID, ev.IncidentID)
		}
		kinds[ev.Kind] = true
	}
	for _, want := range []string{"pod", "previous-logs", "current-logs", "events"} {
		if !kinds[want] {
			t.Errorf("evidence kind %q missing; got %v", want, kinds)
		}
	}

	runs, err := e.store.ListCollectionRuns(context.Background(), incidentID)
	if err != nil {
		t.Fatal(err)
	}
	if len(runs) != 3 {
		t.Fatalf("collection runs = %d, want 3", len(runs))
	}
	for _, r := range runs {
		if r.Status != domain.RunStatusSuccess {
			t.Errorf("run %s status = %q, want success", r.CollectorName, r.Status)
		}
	}

	tl, err := e.store.ListTimeline(context.Background(), incidentID)
	if err != nil {
		t.Fatal(err)
	}
	if len(tl) != 3 {
		t.Fatalf("timeline = %d events, want detected+collected+analyzed", len(tl))
	}
	wantTypes := []string{"detected", "collected", "analyzed"}
	for i, want := range wantTypes {
		if tl[i].EventType != want {
			t.Errorf("timeline[%d] = %q, want %q", i, tl[i].EventType, want)
		}
	}

	analysis, err := e.store.GetAnalysis(context.Background(), incidentID)
	if err != nil {
		t.Fatal(err)
	}
	if len(analysis) != 1 || analysis[0].RuleID != "CRASH-001" {
		t.Fatalf("analysis = %+v, want CRASH-001", analysis)
	}
	if len(analysis[0].EvidenceIDs) == 0 {
		t.Error("hypothesis has no evidence links")
	}
}

// seedBackOffEvent installs a reactor serving one BackOff event so the
// events collector produces evidence.
func seedBackOffEvent(client *fake.Clientset, podName, podUID string) {
	client.PrependReactor("list", "events", func(action clientgotesting.Action) (bool, runtime.Object, error) {
		return true, &corev1.EventList{Items: []corev1.Event{{
			ObjectMeta:     metav1.ObjectMeta{Namespace: "app", Name: podName + ".1"},
			InvolvedObject: corev1.ObjectReference{Kind: "Pod", Namespace: "app", Name: podName, UID: types.UID(podUID)},
			Type:           "Warning",
			Reason:         "BackOff",
			Message:        "back-off restarting failed container",
			Count:          5,
		}}}, nil
	})
}

func TestInvestigateHealthyPod(t *testing.T) {
	e := newEnv(t, crashPod("web-1", healthyStatus("app")))

	result, err := e.investigate(context.Background(), "app", "web-1")
	if err != nil {
		t.Fatalf("Investigate() error = %v", err)
	}
	if !result.Healthy || len(result.IncidentIDs) != 0 {
		t.Fatalf("result = %+v, want healthy with no incidents", result)
	}
	all, err := e.store.ListIncidents(context.Background(), storage.IncidentFilter{})
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 0 {
		t.Errorf("incidents persisted for healthy pod = %d, want 0", len(all))
	}
}

func TestInvestigatePodNotFound(t *testing.T) {
	e := newEnv(t)

	_, err := e.investigate(context.Background(), "app", "missing")
	if err == nil {
		t.Fatal("Investigate() error = nil, want PodNotFoundError")
	}
	var nf *PodNotFoundError
	if !errors.As(err, &nf) {
		t.Fatalf("error = %T (%v), want *PodNotFoundError", err, err)
	}
	if nf.Namespace != "app" || nf.Name != "missing" {
		t.Errorf("error identity = %s/%s", nf.Namespace, nf.Name)
	}
}

func TestInvestigateMultipleContainers(t *testing.T) {
	e := newEnv(t, crashPod("web-1",
		crashLoopStatus("app", 5),
		oomStatus("sidecar", 2),
	))

	result, err := e.investigate(context.Background(), "app", "web-1")
	if err != nil {
		t.Fatal(err)
	}
	if len(result.IncidentIDs) != 2 {
		t.Fatalf("incidents = %d, want 2 (one per failing container)", len(result.IncidentIDs))
	}

	first, err := e.store.GetIncident(context.Background(), result.IncidentIDs[0])
	if err != nil {
		t.Fatal(err)
	}
	if first.ContainerName != "app" || first.FailureType != "CrashLoopBackOff" {
		t.Errorf("primary incident = %+v, want app/CrashLoopBackOff", first)
	}
	second, err := e.store.GetIncident(context.Background(), result.IncidentIDs[1])
	if err != nil {
		t.Fatal(err)
	}
	if second.ContainerName != "sidecar" || second.FailureType != "OOMKilled" {
		t.Errorf("second incident = %+v, want sidecar/OOMKilled", second)
	}

	for _, id := range result.IncidentIDs {
		analysis, err := e.store.GetAnalysis(context.Background(), id)
		if err != nil {
			t.Fatal(err)
		}
		if len(analysis) == 0 {
			t.Errorf("incident %s has no persisted analysis", id)
		}
	}
}

func TestInvestigateMissingPreviousLogs(t *testing.T) {
	e := newEnv(t, crashPod("web-1", crashLoopStatus("app", 5)))
	e.client.PrependReactor("get", "pods/log", func(action clientgotesting.Action) (bool, runtime.Object, error) {
		generic := action.(clientgotesting.GenericAction)
		opts := generic.GetValue().(*corev1.PodLogOptions)
		if opts.Previous {
			return true, nil, apierrors.NewBadRequest(`previous terminated container "app" in pod "web-1" not found`)
		}
		return true, &runtime.Unknown{Raw: []byte("current log line\n")}, nil
	})

	result, err := e.investigate(context.Background(), "app", "web-1")
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Warnings) == 0 || !strings.Contains(strings.Join(result.Warnings, "; "), "has not terminated") {
		t.Errorf("warnings = %v, want missing-previous warning", result.Warnings)
	}

	evs, err := e.store.ListEvidence(context.Background(), result.IncidentIDs[0])
	if err != nil {
		t.Fatal(err)
	}
	var hasCurrent bool
	for _, ev := range evs {
		if ev.Kind == "current-logs" && ev.Text == "current log line\n" {
			hasCurrent = true
		}
	}
	if !hasCurrent {
		t.Errorf("current logs not collected; kinds = %v", evs)
	}
}

func TestInvestigatePartialCollectorFailure(t *testing.T) {
	e := newEnv(t, crashPod("web-1", crashLoopStatus("app", 5)))
	e.client.PrependReactor("list", "events", func(action clientgotesting.Action) (bool, runtime.Object, error) {
		return true, nil, apierrors.NewForbidden(schema.GroupResource{Resource: "events"}, "", errors.New("rbac"))
	})

	result, err := e.investigate(context.Background(), "app", "web-1")
	if err != nil {
		t.Fatalf("Investigate() error = %v, want success with warnings", err)
	}
	joined := strings.Join(result.Warnings, "; ")
	if !strings.Contains(joined, "forbidden") {
		t.Errorf("warnings = %v, want forbidden classification", result.Warnings)
	}

	// Pod and logs evidence must survive the events failure.
	evs, err := e.store.ListEvidence(context.Background(), result.IncidentIDs[0])
	if err != nil {
		t.Fatal(err)
	}
	kinds := map[string]bool{}
	for _, ev := range evs {
		kinds[ev.Kind] = true
	}
	if !kinds["pod"] || !kinds["previous-logs"] {
		t.Errorf("evidence kinds = %v, want pod and logs despite events failure", kinds)
	}

	runs, err := e.store.ListCollectionRuns(context.Background(), result.IncidentIDs[0])
	if err != nil {
		t.Fatal(err)
	}
	var eventsRun *domain.CollectionRun
	for i := range runs {
		if runs[i].CollectorName == "events" {
			eventsRun = &runs[i]
		}
	}
	if eventsRun == nil {
		t.Fatal("events collection run not recorded")
	}
	if eventsRun.Status != domain.RunStatusFailed || eventsRun.ErrorCode != "forbidden" {
		t.Errorf("events run = %+v, want failed/forbidden", eventsRun)
	}
}

func TestInvestigateReusesActiveIncident(t *testing.T) {
	e := newEnv(t, crashPod("web-1", crashLoopStatus("app", 5)))

	first, err := e.investigate(context.Background(), "app", "web-1")
	if err != nil {
		t.Fatal(err)
	}
	second, err := e.investigate(context.Background(), "app", "web-1")
	if err != nil {
		t.Fatal(err)
	}

	if first.IncidentIDs[0] != second.IncidentIDs[0] {
		t.Errorf("incident IDs differ: %q vs %q; retry must reuse the active incident",
			first.IncidentIDs[0], second.IncidentIDs[0])
	}
	all, err := e.store.ListIncidents(context.Background(), storage.IncidentFilter{})
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 1 {
		t.Errorf("incidents after two runs = %d, want 1", len(all))
	}
}

func TestInvestigateAnalysisPersistenceFailure(t *testing.T) {
	e := newEnv(t, crashPod("web-1", crashLoopStatus("app", 5)))
	e.store = &failingAnalysisStore{Store: e.store}
	e.inv.store = e.store

	_, err := e.investigate(context.Background(), "app", "web-1")
	if err == nil {
		t.Fatal("Investigate() error = nil, want analysis persistence error")
	}
	if !strings.Contains(err.Error(), "persist analysis") {
		t.Errorf("error = %v, want analysis persistence context", err)
	}

	// The incident and its evidence must already be persisted.
	all, err := e.store.ListIncidents(context.Background(), storage.IncidentFilter{})
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 1 {
		t.Fatalf("incidents persisted = %d, want 1", len(all))
	}
	evs, err := e.store.ListEvidence(context.Background(), all[0].ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(evs) == 0 {
		t.Error("evidence not persisted despite analysis failure")
	}
}

func TestInvestigateContextCanceled(t *testing.T) {
	e := newEnv(t, crashPod("web-1", crashLoopStatus("app", 5)))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := e.investigate(ctx, "app", "web-1"); !errors.Is(err, context.Canceled) {
		t.Errorf("error = %v, want context.Canceled", err)
	}
}

func TestInvestigateValidation(t *testing.T) {
	e := newEnv(t)
	if _, err := e.investigate(context.Background(), "", "web-1"); err == nil {
		t.Error("empty namespace: error = nil, want error")
	}
	if _, err := e.investigate(context.Background(), "app", ""); err == nil {
		t.Error("empty pod name: error = nil, want error")
	}
}

// failingAnalysisStore fails only SaveAnalysis.
type failingAnalysisStore struct {
	storage.Store
}

func (s *failingAnalysisStore) SaveAnalysis(ctx context.Context, incidentID string, hypotheses []domain.Hypothesis) error {
	return errors.New("disk full")
}
