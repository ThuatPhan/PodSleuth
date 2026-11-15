package rules

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/ThuatPhan/PodSleuth/internal/analyzer"
	"github.com/ThuatPhan/PodSleuth/internal/domain"
)

var obsTime = time.Date(2026, 11, 15, 12, 0, 0, 0, time.UTC)

func testIncident() domain.Incident {
	now := obsTime
	return domain.Incident{
		ID:          "inc-1",
		ClusterID:   "test-cluster",
		Fingerprint: "fp-inc-1",
		FailureType: "CrashLoopBackOff",
		Status:      domain.StatusDetected,
		Resource: domain.ResourceRef{
			ClusterID: "test-cluster",
			Kind:      "Pod",
			Namespace: "app",
			Name:      "web-1",
			UID:       "pod-uid-1",
		},
		FirstSeenAt: now,
		LastSeenAt:  now,
		CreatedAt:   now,
		UpdatedAt:   now,
	}
}

func podEvItem(t *testing.T, id string, pod *corev1.Pod) domain.Evidence {
	t.Helper()
	payload, err := json.Marshal(pod)
	if err != nil {
		t.Fatal(err)
	}
	return domain.Evidence{
		ID:          id,
		IncidentID:  "inc-1",
		Kind:        "pod",
		Source:      "api-server",
		Resource:    domain.ResourceRef{Kind: "Pod", Namespace: "app", Name: "web-1", UID: "pod-uid-1"},
		ObservedAt:  obsTime,
		Content:     payload,
		ContentHash: "hash-" + id,
	}
}

func logsEvItem(id, kind, text string) domain.Evidence {
	return domain.Evidence{
		ID:          id,
		IncidentID:  "inc-1",
		Kind:        kind,
		Source:      "api-server",
		ObservedAt:  obsTime,
		Text:        text,
		ContentHash: "hash-" + id,
	}
}

func eventsEvItem(t *testing.T, id string, views ...eventView) domain.Evidence {
	t.Helper()
	payload, err := json.Marshal(views)
	if err != nil {
		t.Fatal(err)
	}
	return domain.Evidence{
		ID:          id,
		IncidentID:  "inc-1",
		Kind:        "events",
		Source:      "api-server",
		ObservedAt:  obsTime,
		Content:     payload,
		ContentHash: "hash-" + id,
	}
}

func makeEvent(reason, message string) eventView {
	return eventView{
		Type:    "Warning",
		Reason:  reason,
		Message: message,
		Count:   3,
		InvolvedObject: struct {
			Kind      string `json:"kind"`
			Namespace string `json:"namespace"`
			Name      string `json:"name"`
			UID       string `json:"uid"`
		}{Kind: "Pod", Namespace: "app", Name: "web-1", UID: "pod-uid-1"},
	}
}

func podFixture(status corev1.PodStatus, spec corev1.PodSpec) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "app",
			Name:      "web-1",
			UID:       types.UID("pod-uid-1"),
		},
		Spec:   spec,
		Status: status,
	}
}

func cstatus(name string, restarts int32, state, last corev1.ContainerState) corev1.ContainerStatus {
	return corev1.ContainerStatus{
		Name:                 name,
		RestartCount:         restarts,
		State:                state,
		LastTerminationState: last,
	}
}

func waiting(reason string) corev1.ContainerState {
	return corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: reason}}
}

func terminated(exit int32, reason string) corev1.ContainerState {
	return corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: exit, Reason: reason}}
}

func running() corev1.ContainerState {
	return corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}
}

func readyCondition(ready bool) []corev1.PodCondition {
	v := corev1.ConditionFalse
	if ready {
		v = corev1.ConditionTrue
	}
	return []corev1.PodCondition{{Type: corev1.PodReady, Status: v}}
}

// evaluate runs one rule over the given evidence.
func evaluate(t *testing.T, rule analyzer.Rule, input analyzer.Input) *domain.Hypothesis {
	t.Helper()
	h, err := rule.Evaluate(context.Background(), input)
	if err != nil {
		t.Fatalf("Evaluate() error = %v", err)
	}
	return h
}

func TestOOMRule(t *testing.T) {
	rule := &OOMRule{}

	t.Run("fires on last termination OOMKilled", func(t *testing.T) {
		pod := podFixture(corev1.PodStatus{
			Phase:             corev1.PodRunning,
			ContainerStatuses: []corev1.ContainerStatus{cstatus("worker", 1, running(), terminated(137, "OOMKilled"))},
		}, corev1.PodSpec{})
		input := analyzer.Input{Incident: testIncident(), Evidence: []domain.Evidence{podEvItem(t, "ev-pod", pod)}}
		h := evaluate(t, rule, input)
		if h == nil {
			t.Fatal("hypothesis = nil, want fired")
		}
		if h.RuleID != IDOOM || h.Score != analyzer.ScoreDirectStatus {
			t.Errorf("hypothesis = %+v", h)
		}
		if !strings.Contains(h.Summary, "OOM killer") || !strings.Contains(h.Summary, "exit code 137") {
			t.Errorf("summary = %q", h.Summary)
		}
		if !strings.Contains(h.Summary, "unknown without metrics") {
			t.Errorf("summary must not claim exact peak usage: %q", h.Summary)
		}
		if len(h.EvidenceIDs) != 1 || h.EvidenceIDs[0] != "ev-pod" {
			t.Errorf("evidence = %v", h.EvidenceIDs)
		}
		if len(h.MissingEvidence) != 1 || !strings.Contains(h.MissingEvidence[0], "metrics") {
			t.Errorf("missing = %v", h.MissingEvidence)
		}
		for _, c := range h.SuggestedChecks {
			if !strings.HasPrefix(c, "kubectl ") {
				t.Errorf("suggested check not read-only kubectl: %q", c)
			}
		}
	})

	t.Run("boosted by OOM event", func(t *testing.T) {
		pod := podFixture(corev1.PodStatus{
			ContainerStatuses: []corev1.ContainerStatus{cstatus("worker", 1, running(), terminated(137, "OOMKilled"))},
		}, corev1.PodSpec{})
		input := analyzer.Input{Incident: testIncident(), Evidence: []domain.Evidence{
			podEvItem(t, "ev-pod", pod),
			eventsEvItem(t, "ev-events", makeEvent("OOMKilling", "Memory cgroup out of memory")),
		}}
		h := evaluate(t, rule, input)
		if h == nil || h.Score != analyzer.ScoreDirectStatus+analyzer.BoostEvents {
			t.Fatalf("hypothesis = %+v, want boosted score", h)
		}
		if len(h.EvidenceIDs) != 2 {
			t.Errorf("evidence = %v, want pod and events", h.EvidenceIDs)
		}
	})

	t.Run("surfaces currently-running contradiction", func(t *testing.T) {
		pod := podFixture(corev1.PodStatus{
			Phase:             corev1.PodRunning,
			Conditions:        readyCondition(true),
			ContainerStatuses: []corev1.ContainerStatus{cstatus("worker", 1, running(), terminated(137, "OOMKilled"))},
		}, corev1.PodSpec{})
		input := analyzer.Input{Incident: testIncident(), Evidence: []domain.Evidence{podEvItem(t, "ev-pod", pod)}}
		h := evaluate(t, rule, input)
		if h == nil || !strings.Contains(h.Summary, "currently running") {
			t.Errorf("summary = %q, want contradiction surfaced", h.Summary)
		}
	})

	t.Run("does not fire on healthy pod", func(t *testing.T) {
		pod := podFixture(corev1.PodStatus{Phase: corev1.PodRunning, Conditions: readyCondition(true)}, corev1.PodSpec{})
		input := analyzer.Input{Incident: testIncident(), Evidence: []domain.Evidence{podEvItem(t, "ev-pod", pod)}}
		if h := evaluate(t, rule, input); h != nil {
			t.Errorf("hypothesis = %+v, want nil", h)
		}
	})
}

func TestCrashRule(t *testing.T) {
	rule := &CrashRule{}

	t.Run("fires on CrashLoopBackOff", func(t *testing.T) {
		pod := podFixture(corev1.PodStatus{
			ContainerStatuses: []corev1.ContainerStatus{cstatus("app", 5, waiting("CrashLoopBackOff"), terminated(1, "Error"))},
		}, corev1.PodSpec{})
		input := analyzer.Input{Incident: testIncident(), Evidence: []domain.Evidence{podEvItem(t, "ev-pod", pod)}}
		h := evaluate(t, rule, input)
		if h == nil {
			t.Fatal("hypothesis = nil, want fired")
		}
		if !strings.Contains(h.Summary, "CrashLoopBackOff") || !strings.Contains(h.Summary, "not necessarily an application bug") {
			t.Errorf("summary = %q", h.Summary)
		}
		if len(h.MissingEvidence) != 1 || !strings.Contains(h.MissingEvidence[0], "logs") {
			t.Errorf("missing = %v, want missing logs", h.MissingEvidence)
		}
	})

	t.Run("quotes redacted log clue when present", func(t *testing.T) {
		pod := podFixture(corev1.PodStatus{
			ContainerStatuses: []corev1.ContainerStatus{cstatus("app", 5, waiting("CrashLoopBackOff"), terminated(1, "Error"))},
		}, corev1.PodSpec{})
		input := analyzer.Input{Incident: testIncident(), Evidence: []domain.Evidence{
			podEvItem(t, "ev-pod", pod),
			logsEvItem("ev-logs", "previous-logs", "panic: runtime error: index out of range\n"),
		}}
		h := evaluate(t, rule, input)
		if h == nil {
			t.Fatal("hypothesis = nil, want fired")
		}
		if h.Score != analyzer.ScoreDirectStatus+analyzer.BoostLogs {
			t.Errorf("score = %v, want boosted", h.Score)
		}
		if !strings.Contains(h.Summary, `Log clue: "panic: runtime error: index out of range"`) {
			t.Errorf("summary = %q, want quoted clue", h.Summary)
		}
		if len(h.EvidenceIDs) != 2 {
			t.Errorf("evidence = %v, want pod and logs", h.EvidenceIDs)
		}
		if len(h.MissingEvidence) != 0 {
			t.Errorf("missing = %v, want none", h.MissingEvidence)
		}
	})

	t.Run("does not fire on OOMKilled termination", func(t *testing.T) {
		pod := podFixture(corev1.PodStatus{
			ContainerStatuses: []corev1.ContainerStatus{cstatus("app", 1, running(), terminated(137, "OOMKilled"))},
		}, corev1.PodSpec{})
		input := analyzer.Input{Incident: testIncident(), Evidence: []domain.Evidence{podEvItem(t, "ev-pod", pod)}}
		if h := evaluate(t, rule, input); h != nil {
			t.Errorf("hypothesis = %+v, want nil (OOM-001 owns it)", h)
		}
	})

	t.Run("does not fire on clean exit", func(t *testing.T) {
		pod := podFixture(corev1.PodStatus{
			ContainerStatuses: []corev1.ContainerStatus{cstatus("app", 0, terminated(0, "Completed"), corev1.ContainerState{})},
		}, corev1.PodSpec{})
		input := analyzer.Input{Incident: testIncident(), Evidence: []domain.Evidence{podEvItem(t, "ev-pod", pod)}}
		if h := evaluate(t, rule, input); h != nil {
			t.Errorf("hypothesis = %+v, want nil", h)
		}
	})
}

func TestImageRule(t *testing.T) {
	rule := &ImageRule{}

	t.Run("fires on status with image reference", func(t *testing.T) {
		pod := podFixture(corev1.PodStatus{
			ContainerStatuses: []corev1.ContainerStatus{cstatus("app", 0, waiting("ImagePullBackOff"), corev1.ContainerState{})},
		}, corev1.PodSpec{Containers: []corev1.Container{{Name: "app", Image: "registry.example.test/app:v2"}}})
		input := analyzer.Input{Incident: testIncident(), Evidence: []domain.Evidence{podEvItem(t, "ev-pod", pod)}}
		h := evaluate(t, rule, input)
		if h == nil {
			t.Fatal("hypothesis = nil, want fired")
		}
		if h.Score != analyzer.ScoreDirectStatus {
			t.Errorf("score = %v", h.Score)
		}
		if !strings.Contains(h.Summary, "registry.example.test/app:v2") {
			t.Errorf("summary = %q, want image reference", h.Summary)
		}
		if !strings.Contains(h.Summary, "does not prove the image name is invalid") {
			t.Errorf("summary = %q, want hedged claim", h.Summary)
		}
	})

	t.Run("fires on pull event alone", func(t *testing.T) {
		input := analyzer.Input{Incident: testIncident(), Evidence: []domain.Evidence{
			eventsEvItem(t, "ev-events", makeEvent("PullError", "Failed to pull image app:v2: not found")),
		}}
		h := evaluate(t, rule, input)
		if h == nil {
			t.Fatal("hypothesis = nil, want fired")
		}
		if h.Score != analyzer.ScoreEventOnly {
			t.Errorf("score = %v, want event-only", h.Score)
		}
		if len(h.MissingEvidence) != 1 {
			t.Errorf("missing = %v, want missing pod status", h.MissingEvidence)
		}
	})

	t.Run("no fire on unrelated events", func(t *testing.T) {
		input := analyzer.Input{Incident: testIncident(), Evidence: []domain.Evidence{
			eventsEvItem(t, "ev-events", makeEvent("Pulled", "image pulled successfully")),
		}}
		if h := evaluate(t, rule, input); h != nil {
			t.Errorf("hypothesis = %+v, want nil", h)
		}
	})
}

func TestConfigRule(t *testing.T) {
	rule := &ConfigRule{}

	t.Run("fires on status", func(t *testing.T) {
		pod := podFixture(corev1.PodStatus{
			ContainerStatuses: []corev1.ContainerStatus{cstatus("app", 0, waiting("CreateContainerConfigError"), corev1.ContainerState{})},
		}, corev1.PodSpec{})
		input := analyzer.Input{Incident: testIncident(), Evidence: []domain.Evidence{podEvItem(t, "ev-pod", pod)}}
		h := evaluate(t, rule, input)
		if h == nil {
			t.Fatal("hypothesis = nil, want fired")
		}
		if !strings.Contains(h.Summary, "Secret values were not inspected") {
			t.Errorf("summary = %q", h.Summary)
		}
	})

	t.Run("fires on config event alone", func(t *testing.T) {
		input := analyzer.Input{Incident: testIncident(), Evidence: []domain.Evidence{
			eventsEvItem(t, "ev-events", makeEvent("Failed", "CreateContainerConfigError: configmap \"cfg\" not found")),
		}}
		h := evaluate(t, rule, input)
		if h == nil || h.Score != analyzer.ScoreEventOnly {
			t.Fatalf("hypothesis = %+v", h)
		}
	})
}

func TestSchedRule(t *testing.T) {
	rule := &SchedRule{}

	t.Run("fires on condition with message", func(t *testing.T) {
		pod := podFixture(corev1.PodStatus{
			Phase: corev1.PodPending,
			Conditions: []corev1.PodCondition{
				{Type: corev1.PodScheduled, Status: corev1.ConditionFalse, Reason: "Unschedulable", Message: "0/3 nodes are available: insufficient memory"},
			},
		}, corev1.PodSpec{})
		input := analyzer.Input{Incident: testIncident(), Evidence: []domain.Evidence{podEvItem(t, "ev-pod", pod)}}
		h := evaluate(t, rule, input)
		if h == nil {
			t.Fatal("hypothesis = nil, want fired")
		}
		if h.Score != analyzer.ScoreDirectStatus {
			t.Errorf("score = %v", h.Score)
		}
		if !strings.Contains(h.Summary, "0/3 nodes are available") {
			t.Errorf("summary = %q, want event message qualifier", h.Summary)
		}
		if !strings.Contains(h.Summary, "does not necessarily indicate insufficient cluster capacity") {
			t.Errorf("summary = %q, want hedged claim", h.Summary)
		}
	})

	t.Run("fires on event alone", func(t *testing.T) {
		input := analyzer.Input{Incident: testIncident(), Evidence: []domain.Evidence{
			eventsEvItem(t, "ev-events", makeEvent("FailedScheduling", "0/3 nodes are available")),
		}}
		h := evaluate(t, rule, input)
		if h == nil || h.Score != analyzer.ScoreEventOnly {
			t.Fatalf("hypothesis = %+v", h)
		}
	})

	t.Run("no fire when scheduled", func(t *testing.T) {
		pod := podFixture(corev1.PodStatus{
			Conditions: []corev1.PodCondition{{Type: corev1.PodScheduled, Status: corev1.ConditionTrue}},
		}, corev1.PodSpec{})
		input := analyzer.Input{Incident: testIncident(), Evidence: []domain.Evidence{podEvItem(t, "ev-pod", pod)}}
		if h := evaluate(t, rule, input); h != nil {
			t.Errorf("hypothesis = %+v, want nil", h)
		}
	})
}

func TestProbeRule(t *testing.T) {
	rule := &ProbeRule{}

	t.Run("fires with event and probe spec", func(t *testing.T) {
		pod := podFixture(corev1.PodStatus{
			ContainerStatuses: []corev1.ContainerStatus{cstatus("app", 2, running(), terminated(137, "Error"))},
		}, corev1.PodSpec{Containers: []corev1.Container{{
			Name:          "app",
			LivenessProbe: &corev1.Probe{},
		}}})
		input := analyzer.Input{Incident: testIncident(), Evidence: []domain.Evidence{
			podEvItem(t, "ev-pod", pod),
			eventsEvItem(t, "ev-events", makeEvent("Unhealthy", "Liveness probe failed: HTTP probe failed with statuscode 500")),
		}}
		h := evaluate(t, rule, input)
		if h == nil {
			t.Fatal("hypothesis = nil, want fired")
		}
		if h.Score != analyzer.ScoreEventOnly+analyzer.BoostEvents {
			t.Errorf("score = %v, want boosted", h.Score)
		}
		if !strings.Contains(h.Summary, "liveness") {
			t.Errorf("summary = %q, want probe kind", h.Summary)
		}
		if len(h.EvidenceIDs) != 2 {
			t.Errorf("evidence = %v, want events and pod", h.EvidenceIDs)
		}
	})

	t.Run("distinguishes startup probe", func(t *testing.T) {
		input := analyzer.Input{Incident: testIncident(), Evidence: []domain.Evidence{
			eventsEvItem(t, "ev-events", makeEvent("StartupProbeFailed", "Startup probe failed")),
		}}
		h := evaluate(t, rule, input)
		if h == nil || !strings.Contains(h.Summary, "startup") {
			t.Errorf("hypothesis = %+v, want startup kind", h)
		}
	})

	t.Run("does not fire without probe event", func(t *testing.T) {
		pod := podFixture(corev1.PodStatus{}, corev1.PodSpec{Containers: []corev1.Container{{Name: "app", LivenessProbe: &corev1.Probe{}}}})
		input := analyzer.Input{Incident: testIncident(), Evidence: []domain.Evidence{podEvItem(t, "ev-pod", pod)}}
		if h := evaluate(t, rule, input); h != nil {
			t.Errorf("hypothesis = %+v, want nil (event is required)", h)
		}
	})
}

func TestEvictRule(t *testing.T) {
	rule := &EvictRule{}

	t.Run("fires with pressure reason", func(t *testing.T) {
		pod := podFixture(corev1.PodStatus{
			Phase:   corev1.PodFailed,
			Reason:  "Evicted",
			Message: "The node was low on resource: memory.",
		}, corev1.PodSpec{})
		input := analyzer.Input{Incident: testIncident(), Evidence: []domain.Evidence{podEvItem(t, "ev-pod", pod)}}
		h := evaluate(t, rule, input)
		if h == nil {
			t.Fatal("hypothesis = nil, want fired")
		}
		if !strings.Contains(h.Summary, "The node was low on resource: memory.") {
			t.Errorf("summary = %q, want reported pressure reason", h.Summary)
		}
		if !strings.Contains(h.Summary, "not an application failure") {
			t.Errorf("summary = %q, want hedged claim", h.Summary)
		}
		if len(h.MissingEvidence) != 1 || !strings.Contains(h.MissingEvidence[0], "node") {
			t.Errorf("missing = %v", h.MissingEvidence)
		}
	})

	t.Run("no fire on normal pod", func(t *testing.T) {
		pod := podFixture(corev1.PodStatus{Phase: corev1.PodRunning}, corev1.PodSpec{})
		input := analyzer.Input{Incident: testIncident(), Evidence: []domain.Evidence{podEvItem(t, "ev-pod", pod)}}
		if h := evaluate(t, rule, input); h != nil {
			t.Errorf("hypothesis = %+v, want nil", h)
		}
	})
}

func TestPVCRule(t *testing.T) {
	rule := &PVCRule{}

	t.Run("fires on FailedMount event", func(t *testing.T) {
		input := analyzer.Input{Incident: testIncident(), Evidence: []domain.Evidence{
			eventsEvItem(t, "ev-events", makeEvent("FailedMount", "MountVolume failed: volume \"data\" not found")),
		}}
		h := evaluate(t, rule, input)
		if h == nil {
			t.Fatal("hypothesis = nil, want fired")
		}
		if !strings.Contains(h.Summary, `volume "data" not found`) {
			t.Errorf("summary = %q, want exact symptom", h.Summary)
		}
		if len(h.MissingEvidence) != 1 || !strings.Contains(h.MissingEvidence[0], "PVC") {
			t.Errorf("missing = %v", h.MissingEvidence)
		}
	})

	t.Run("no fire on unrelated events", func(t *testing.T) {
		input := analyzer.Input{Incident: testIncident(), Evidence: []domain.Evidence{
			eventsEvItem(t, "ev-events", makeEvent("Pulled", "image pulled")),
		}}
		if h := evaluate(t, rule, input); h != nil {
			t.Errorf("hypothesis = %+v, want nil", h)
		}
	})
}

func TestAllRulesRegistered(t *testing.T) {
	all := All()
	if len(all) != 8 {
		t.Fatalf("rules = %d, want 8", len(all))
	}
	want := map[string]bool{
		IDOOM: true, IDCrash: true, IDImage: true, IDConfig: true,
		IDSched: true, IDProbe: true, IDEvict: true, IDPVC: true,
	}
	for _, r := range all {
		if !want[r.ID()] {
			t.Errorf("unexpected rule %q", r.ID())
		}
		delete(want, r.ID())
	}
	for id := range want {
		t.Errorf("rule %q missing", id)
	}
}

// TestSuggestedChecksAreRendered verifies that every fired rule's
// suggested checks have their placeholders resolved: no format
// directives or unreplaced markers may reach reports.
func TestSuggestedChecksAreRendered(t *testing.T) {
	pod := podFixture(corev1.PodStatus{
		ContainerStatuses: []corev1.ContainerStatus{cstatus("app", 5, waiting("CrashLoopBackOff"), terminated(1, "Error"))},
	}, corev1.PodSpec{})
	input := analyzer.Input{Incident: testIncident(), Evidence: []domain.Evidence{podEvItem(t, "ev-pod", pod)}}

	for _, rule := range All() {
		h := evaluate(t, rule, input)
		if h == nil {
			continue
		}
		for _, c := range h.SuggestedChecks {
			if strings.Contains(c, "%s") || strings.Contains(c, "%!") || strings.Contains(c, "{pod}") || strings.Contains(c, "{ns}") {
				t.Errorf("rule %s produced an unrendered check: %q", rule.ID(), c)
			}
			if !strings.Contains(c, "web-1") && !strings.Contains(c, "-n app") {
				t.Errorf("rule %s check missing incident identity: %q", rule.ID(), c)
			}
		}
	}
}

func TestUnparsableEvidenceDoesNotFire(t *testing.T) {
	bad := domain.Evidence{
		ID: "ev-bad", IncidentID: "inc-1", Kind: "pod",
		Content: []byte("{not json"), ContentHash: "h",
	}
	input := analyzer.Input{Incident: testIncident(), Evidence: []domain.Evidence{bad}}
	for _, rule := range All() {
		h, err := rule.Evaluate(context.Background(), input)
		if err != nil {
			t.Errorf("rule %s: error = %v", rule.ID(), err)
		}
		if h != nil {
			t.Errorf("rule %s fired on unparsable evidence", rule.ID())
		}
	}
}
