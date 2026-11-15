package incident

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/ThuatPhan/PodSleuth/internal/detector"
	"github.com/ThuatPhan/PodSleuth/internal/domain"
)

// fakeClock is a manually advanced clock.
type fakeClock struct {
	t time.Time
}

func (c *fakeClock) Now() time.Time          { return c.t }
func (c *fakeClock) Advance(d time.Duration) { c.t = c.t.Add(d) }

// idSeq generates deterministic incident IDs.
type idSeq struct {
	n int
}

func (s *idSeq) NewID() string {
	s.n++
	return fmt.Sprintf("inc-%d", s.n)
}

// env bundles the manager and its deterministic dependencies.
type env struct {
	clock *fakeClock
	ids   *idSeq
	mgr   *Manager
	dedup *detector.MemoryDeduplicator
	det   *detector.PodDetector
}

func newEnv(stabilization time.Duration) *env {
	clock := &fakeClock{t: time.Date(2026, 11, 15, 12, 0, 0, 0, time.UTC)}
	ids := &idSeq{}
	dedup := detector.NewMemoryDeduplicator()
	mgr := NewManager(dedup, Options{Stabilization: stabilization, Clock: clock.Now, NewID: ids.NewID})
	return &env{
		clock: clock,
		ids:   ids,
		mgr:   mgr,
		dedup: dedup,
		det:   detector.NewPodDetector("test-cluster"),
	}
}

// observe builds a Pod observation, runs detection, and feeds the
// pipeline into the manager, mirroring app.Pipeline.
func (e *env) observe(t *testing.T, pod *corev1.Pod) ([]domain.FailureSignal, error) {
	t.Helper()
	payload, err := json.Marshal(pod)
	if err != nil {
		t.Fatal(err)
	}
	obs := domain.Observation{
		Resource: domain.ResourceRef{
			ClusterID:  "test-cluster",
			APIVersion: "v1",
			Kind:       "Pod",
			Namespace:  pod.Namespace,
			Name:       pod.Name,
			UID:        string(pod.UID),
		},
		ObservedAt:      e.clock.Now(),
		ResourceVersion: pod.ResourceVersion,
		Payload:         payload,
	}
	signals, err := e.det.Evaluate(context.Background(), obs)
	if err != nil {
		t.Fatal(err)
	}
	return signals, e.mgr.Process(context.Background(), obs, signals)
}

func rsOwner(uid string) metav1.OwnerReference {
	controller := true
	return metav1.OwnerReference{Kind: "ReplicaSet", Name: "web-7d9f", UID: types.UID(uid), Controller: &controller}
}

func podMeta(ns, name, uid string, owners ...metav1.OwnerReference) metav1.ObjectMeta {
	return metav1.ObjectMeta{Namespace: ns, Name: name, UID: types.UID(uid), ResourceVersion: "1000", OwnerReferences: owners}
}

func crashLoopStatus(container string, restarts int32) corev1.PodStatus {
	return corev1.PodStatus{
		Phase: corev1.PodRunning,
		Conditions: []corev1.PodCondition{
			{Type: corev1.PodReady, Status: corev1.ConditionFalse},
		},
		ContainerStatuses: []corev1.ContainerStatus{{
			Name:                 container,
			RestartCount:         restarts,
			State:                corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "CrashLoopBackOff"}},
			LastTerminationState: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 1, Reason: "Error"}},
		}},
	}
}

func readyStatus() corev1.PodStatus {
	return corev1.PodStatus{
		Phase: corev1.PodRunning,
		Conditions: []corev1.PodCondition{
			{Type: corev1.PodReady, Status: corev1.ConditionTrue},
		},
		ContainerStatuses: []corev1.ContainerStatus{{
			Name:  "app",
			State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}},
		}},
	}
}

func TestManagerCreatesIncident(t *testing.T) {
	e := newEnv(0)
	pod := &corev1.Pod{
		ObjectMeta: podMeta("app", "web-1", "pod-uid-1", rsOwner("rs-uid-1")),
		Status:     crashLoopStatus("app", 5),
	}
	if _, err := e.observe(t, pod); err != nil {
		t.Fatal(err)
	}

	active := e.mgr.ListActive()
	if len(active) != 1 {
		t.Fatalf("ListActive() = %d incidents, want 1", len(active))
	}
	inc := active[0]
	if inc.Status != domain.StatusDetected {
		t.Errorf("Status = %q, want detected", inc.Status)
	}
	if inc.FailureType != detector.TypeCrashLoopBackOff {
		t.Errorf("FailureType = %q, want CrashLoopBackOff", inc.FailureType)
	}
	if inc.ContainerName != "app" {
		t.Errorf("ContainerName = %q, want app", inc.ContainerName)
	}
	if inc.WorkloadKind != "ReplicaSet" || inc.WorkloadUID != "rs-uid-1" {
		t.Errorf("Workload = %s/%s/%s, want ReplicaSet/rs-uid-1", inc.WorkloadKind, inc.WorkloadName, inc.WorkloadUID)
	}
	if inc.ClusterID != "test-cluster" || inc.Resource.UID != "pod-uid-1" {
		t.Errorf("Resource identity = %+v", inc.Resource)
	}
	if inc.Severity != domain.DefaultSeverity {
		t.Errorf("Severity = %q, want %q", inc.Severity, domain.DefaultSeverity)
	}
	if inc.Summary == "" {
		t.Error("Summary is empty")
	}
	if !inc.FirstSeenAt.Equal(inc.LastSeenAt) {
		t.Errorf("FirstSeen %v != LastSeen %v on creation", inc.FirstSeenAt, inc.LastSeenAt)
	}
}

func TestManagerDedupsRepeatedSignals(t *testing.T) {
	e := newEnv(0)
	pod := &corev1.Pod{
		ObjectMeta: podMeta("app", "web-1", "pod-uid-1", rsOwner("rs-uid-1")),
		Status:     crashLoopStatus("app", 5),
	}
	if _, err := e.observe(t, pod); err != nil {
		t.Fatal(err)
	}
	first := e.mgr.ListActive()[0]

	e.clock.Advance(1 * time.Minute)
	pod.ResourceVersion = "1001"
	pod.Status.ContainerStatuses[0].RestartCount = 6
	if _, err := e.observe(t, pod); err != nil {
		t.Fatal(err)
	}

	active := e.mgr.ListActive()
	if len(active) != 1 {
		t.Fatalf("repeated signal created %d incidents, want 1", len(active))
	}
	inc := active[0]
	if inc.ID != first.ID {
		t.Errorf("incident ID changed: %q -> %q", first.ID, inc.ID)
	}
	if !inc.FirstSeenAt.Equal(first.FirstSeenAt) {
		t.Errorf("FirstSeenAt changed: %v -> %v", first.FirstSeenAt, inc.FirstSeenAt)
	}
	if !inc.LastSeenAt.After(first.LastSeenAt) {
		t.Errorf("LastSeenAt = %v, want advanced past %v", inc.LastSeenAt, first.LastSeenAt)
	}
}

func TestManagerRecoversViaReplacementPod(t *testing.T) {
	e := newEnv(2 * time.Minute)
	failing := &corev1.Pod{
		ObjectMeta: podMeta("app", "web-1", "pod-uid-1", rsOwner("rs-uid-1")),
		Status:     crashLoopStatus("app", 5),
	}
	if _, err := e.observe(t, failing); err != nil {
		t.Fatal(err)
	}
	incidentID := e.mgr.ListActive()[0].ID

	// Replacement Pod from the same workload comes up Ready.
	e.clock.Advance(30 * time.Second)
	replacement := &corev1.Pod{
		ObjectMeta: podMeta("app", "web-2", "pod-uid-2", rsOwner("rs-uid-1")),
		Status:     readyStatus(),
	}
	if _, err := e.observe(t, replacement); err != nil {
		t.Fatal(err)
	}

	// Not yet stabilized.
	e.clock.Advance(1 * time.Minute)
	replacement.ResourceVersion = "1001"
	if _, err := e.observe(t, replacement); err != nil {
		t.Fatal(err)
	}
	if got, _ := e.mgr.Get(incidentID); got.Status != domain.StatusDetected {
		t.Fatalf("status after 1.5m stable = %q, want still detected", got.Status)
	}

	// Stabilization reached.
	e.clock.Advance(1 * time.Minute)
	replacement.ResourceVersion = "1002"
	if _, err := e.observe(t, replacement); err != nil {
		t.Fatal(err)
	}
	got, ok := e.mgr.Get(incidentID)
	if !ok {
		t.Fatal("incident disappeared")
	}
	if got.Status != domain.StatusResolved {
		t.Errorf("status = %q, want resolved", got.Status)
	}
	if got.ResolvedAt == nil {
		t.Error("ResolvedAt is nil")
	}
	if len(e.mgr.ListActive()) != 0 {
		t.Errorf("ListActive() = %d, want 0", len(e.mgr.ListActive()))
	}

	// A new failure of the same kind starts a NEW episode.
	e.clock.Advance(1 * time.Minute)
	failing.ResourceVersion = "1003"
	if _, err := e.observe(t, failing); err != nil {
		t.Fatal(err)
	}
	active := e.mgr.ListActive()
	if len(active) != 1 {
		t.Fatalf("ListActive() after new failure = %d, want 1", len(active))
	}
	if active[0].ID == incidentID {
		t.Error("new episode reused the resolved incident ID")
	}
}

func TestManagerRecoveryResetByNewFailure(t *testing.T) {
	e := newEnv(2 * time.Minute)
	failing := &corev1.Pod{
		ObjectMeta: podMeta("app", "web-1", "pod-uid-1", rsOwner("rs-uid-1")),
		Status:     crashLoopStatus("app", 5),
	}
	if _, err := e.observe(t, failing); err != nil {
		t.Fatal(err)
	}
	incidentID := e.mgr.ListActive()[0].ID

	// Healthy period starts.
	e.clock.Advance(30 * time.Second)
	healthy := &corev1.Pod{
		ObjectMeta: podMeta("app", "web-1", "pod-uid-1", rsOwner("rs-uid-1")),
		Status:     readyStatus(),
	}
	if _, err := e.observe(t, healthy); err != nil {
		t.Fatal(err)
	}

	// A new failure resets the stabilization window.
	e.clock.Advance(1 * time.Minute)
	failing.ResourceVersion = "1001"
	if _, err := e.observe(t, failing); err != nil {
		t.Fatal(err)
	}

	// Healthy again; only 1 minute has accumulated since the reset.
	e.clock.Advance(1 * time.Minute)
	healthy.ResourceVersion = "1002"
	if _, err := e.observe(t, healthy); err != nil {
		t.Fatal(err)
	}
	if got, _ := e.mgr.Get(incidentID); got.Status != domain.StatusDetected {
		t.Fatalf("status = %q, want still detected (stabilization reset)", got.Status)
	}

	// Full stabilization after the reset resolves the incident.
	e.clock.Advance(2 * time.Minute)
	healthy.ResourceVersion = "1003"
	if _, err := e.observe(t, healthy); err != nil {
		t.Fatal(err)
	}
	if got, _ := e.mgr.Get(incidentID); got.Status != domain.StatusResolved {
		t.Errorf("status = %q, want resolved", got.Status)
	}
}

func TestManagerDoesNotResolveOnPodDisappearance(t *testing.T) {
	e := newEnv(2 * time.Minute)
	failing := &corev1.Pod{
		ObjectMeta: podMeta("app", "web-1", "pod-uid-1", rsOwner("rs-uid-1")),
		Status:     crashLoopStatus("app", 5),
	}
	if _, err := e.observe(t, failing); err != nil {
		t.Fatal(err)
	}
	incidentID := e.mgr.ListActive()[0].ID

	// The Pod is deleted: no further observations of it ever arrive.
	// Unrelated traffic must not resolve the incident.
	e.clock.Advance(1 * time.Hour)
	unrelated := &corev1.Pod{
		ObjectMeta: podMeta("other", "batch-1", "pod-uid-9"),
		Status:     readyStatus(),
	}
	if _, err := e.observe(t, unrelated); err != nil {
		t.Fatal(err)
	}

	got, ok := e.mgr.Get(incidentID)
	if !ok {
		t.Fatal("incident disappeared")
	}
	if got.Status != domain.StatusDetected {
		t.Errorf("status = %q, want still detected (pod disappearance is not recovery)", got.Status)
	}
}

func TestManagerStandalonePodRecovery(t *testing.T) {
	e := newEnv(2 * time.Minute)
	// Standalone Pod: no controller owner.
	failing := &corev1.Pod{
		ObjectMeta: podMeta("app", "worker-1", "pod-uid-1"),
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			ContainerStatuses: []corev1.ContainerStatus{{
				Name:                 "worker",
				RestartCount:         1,
				State:                corev1.ContainerState{Running: &corev1.ContainerStateRunning{}},
				LastTerminationState: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 137, Reason: "OOMKilled"}},
			}},
		},
	}
	if _, err := e.observe(t, failing); err != nil {
		t.Fatal(err)
	}
	incidentID := e.mgr.ListActive()[0].ID
	if inc, _ := e.mgr.Get(incidentID); inc.WorkloadUID != "" {
		t.Errorf("WorkloadUID = %q, want empty for standalone pod", inc.WorkloadUID)
	}

	// The same Pod becomes Ready and stays Ready.
	e.clock.Advance(30 * time.Second)
	healthy := &corev1.Pod{
		ObjectMeta: podMeta("app", "worker-1", "pod-uid-1"),
		Status:     readyStatus(),
	}
	if _, err := e.observe(t, healthy); err != nil {
		t.Fatal(err)
	}
	e.clock.Advance(2 * time.Minute)
	healthy.ResourceVersion = "1001"
	if _, err := e.observe(t, healthy); err != nil {
		t.Fatal(err)
	}
	if got, _ := e.mgr.Get(incidentID); got.Status != domain.StatusResolved {
		t.Errorf("status = %q, want resolved", got.Status)
	}
}

func TestManagerMultipleContainersIndependent(t *testing.T) {
	e := newEnv(0)
	pod := &corev1.Pod{
		ObjectMeta: podMeta("app", "web-1", "pod-uid-1", rsOwner("rs-uid-1")),
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			ContainerStatuses: []corev1.ContainerStatus{
				{
					Name:                 "app",
					RestartCount:         5,
					State:                corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "CrashLoopBackOff"}},
					LastTerminationState: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 1, Reason: "Error"}},
				},
				{
					Name:                 "sidecar",
					RestartCount:         1,
					State:                corev1.ContainerState{Running: &corev1.ContainerStateRunning{}},
					LastTerminationState: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 137, Reason: "OOMKilled"}},
				},
			},
		},
	}
	if _, err := e.observe(t, pod); err != nil {
		t.Fatal(err)
	}

	active := e.mgr.ListActive()
	if len(active) != 2 {
		t.Fatalf("ListActive() = %d incidents, want 2", len(active))
	}
	containers := map[string]bool{}
	for _, inc := range active {
		containers[inc.ContainerName] = true
	}
	if !containers["app"] || !containers["sidecar"] {
		t.Errorf("incidents per container = %v, want app and sidecar", containers)
	}
}

func TestManagerNotReadyObservationDoesNotResolve(t *testing.T) {
	e := newEnv(2 * time.Minute)
	failing := &corev1.Pod{
		ObjectMeta: podMeta("app", "web-1", "pod-uid-1", rsOwner("rs-uid-1")),
		Status:     crashLoopStatus("app", 5),
	}
	if _, err := e.observe(t, failing); err != nil {
		t.Fatal(err)
	}
	incidentID := e.mgr.ListActive()[0].ID

	// The Pod keeps reporting not-Ready; stabilization never starts.
	for i := 0; i < 5; i++ {
		e.clock.Advance(1 * time.Minute)
		failing.ResourceVersion = fmt.Sprintf("%d", 1001+i)
		if _, err := e.observe(t, failing); err != nil {
			t.Fatal(err)
		}
	}
	if got, _ := e.mgr.Get(incidentID); got.Status != domain.StatusDetected {
		t.Errorf("status = %q, want still detected", got.Status)
	}
}

func TestManagerInvalidPayload(t *testing.T) {
	e := newEnv(0)
	obs := domain.Observation{
		Resource:   domain.ResourceRef{ClusterID: "c", Kind: "Pod", Namespace: "ns", Name: "p"},
		Payload:    []byte("{not json"),
		ObservedAt: e.clock.Now(),
	}
	if err := e.mgr.Process(context.Background(), obs, nil); err == nil {
		t.Fatal("Process() error = nil, want error")
	}
	if len(e.mgr.List()) != 0 {
		t.Error("incidents created from unparsable payload")
	}
}

func TestManagerGetUnknown(t *testing.T) {
	e := newEnv(0)
	if _, ok := e.mgr.Get("missing"); ok {
		t.Error("Get(missing) = ok, want not found")
	}
}
