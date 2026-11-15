package collector_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	clientgotesting "k8s.io/client-go/testing"

	"github.com/ThuatPhan/PodSleuth/internal/collector"
	"github.com/ThuatPhan/PodSleuth/internal/collector/events"
	"github.com/ThuatPhan/PodSleuth/internal/collector/logs"
	"github.com/ThuatPhan/PodSleuth/internal/collector/pod"
	"github.com/ThuatPhan/PodSleuth/internal/detector"
	"github.com/ThuatPhan/PodSleuth/internal/domain"
	"github.com/ThuatPhan/PodSleuth/internal/evidence"
	"github.com/ThuatPhan/PodSleuth/internal/incident"
)

// TestFlowIncidentToEvidence runs the intended pipeline by hand:
// detection -> incident manager -> registry -> collectors, and verifies
// that every evidence item is associated with the created incident.
func TestFlowIncidentToEvidence(t *testing.T) {
	controller := true
	podObj := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:       "app",
			Name:            "web-1",
			UID:             types.UID("pod-uid-1"),
			ResourceVersion: "1000",
			OwnerReferences: []metav1.OwnerReference{{Kind: "ReplicaSet", Name: "web-7d9f", UID: "rs-uid-1", Controller: &controller}},
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			Conditions: []corev1.PodCondition{
				{Type: corev1.PodReady, Status: corev1.ConditionFalse},
			},
			ContainerStatuses: []corev1.ContainerStatus{{
				Name:                 "app",
				RestartCount:         5,
				State:                corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "CrashLoopBackOff"}},
				LastTerminationState: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 1, Reason: "Error"}},
			}},
		},
	}
	payload, err := json.Marshal(podObj)
	if err != nil {
		t.Fatal(err)
	}
	obs := domain.Observation{
		Resource: domain.ResourceRef{
			ClusterID:  "test-cluster",
			APIVersion: "v1",
			Kind:       "Pod",
			Namespace:  "app",
			Name:       "web-1",
			UID:        "pod-uid-1",
		},
		ObservedAt:      fixedObsTime,
		ResourceVersion: "1000",
		Payload:         payload,
	}

	// Detect and create the incident.
	det := detector.NewPodDetector("test-cluster")
	signals, err := det.Evaluate(context.Background(), obs)
	if err != nil {
		t.Fatal(err)
	}
	if len(signals) != 1 {
		t.Fatalf("signals = %d, want 1", len(signals))
	}
	mgr := incident.NewManager(detector.NewMemoryDeduplicator(), incident.Options{})
	if err := mgr.Process(context.Background(), obs, signals); err != nil {
		t.Fatal(err)
	}
	incidents := mgr.ListActive()
	if len(incidents) != 1 {
		t.Fatalf("incidents = %d, want 1", len(incidents))
	}
	inc := incidents[0]

	// Collect from all matching collectors.
	cs := fake.NewSimpleClientset(podObj)
	cs.PrependReactor("get", "pods/log", func(action clientgotesting.Action) (bool, runtime.Object, error) {
		return true, &runtime.Unknown{Raw: []byte("previous run log line\n")}, nil
	})
	cs.PrependReactor("list", "events", func(action clientgotesting.Action) (bool, runtime.Object, error) {
		return true, &corev1.EventList{Items: []corev1.Event{{
			ObjectMeta:     metav1.ObjectMeta{Namespace: "app", Name: "web-1.1"},
			InvolvedObject: corev1.ObjectReference{Kind: "Pod", Namespace: "app", Name: "web-1", UID: types.UID("pod-uid-1")},
			Type:           "Warning",
			Reason:         "BackOff",
			Message:        "back-off restarting failed container",
			Count:          5,
		}}}, nil
	})

	reg := collector.NewRegistry(
		pod.NewPodCollector(cs, evidence.DefaultOptions()),
		logs.NewLogsCollector(cs, logs.Options{PreviousLogs: true, CurrentLogs: true}, evidence.DefaultOptions()),
		events.NewEventsCollector(cs, evidence.DefaultOptions()),
	)

	req := collector.Request{Incident: inc, Signal: signals[0]}
	var allEvidence []domain.Evidence
	var allWarnings []string
	for _, c := range reg.ForSignal(signals[0]) {
		result, err := c.Collect(context.Background(), req)
		if err != nil {
			t.Fatalf("collector %s: Collect() error = %v", c.Name(), err)
		}
		allEvidence = append(allEvidence, result.Evidence...)
		allWarnings = append(allWarnings, result.Warnings...)
	}

	if len(allWarnings) != 0 {
		t.Errorf("warnings = %v, want none", allWarnings)
	}
	kindsSeen := map[string]bool{}
	for _, ev := range allEvidence {
		if ev.IncidentID != inc.ID {
			t.Errorf("evidence %s IncidentID = %q, want %q", ev.ID, ev.IncidentID, inc.ID)
		}
		if ev.Resource.UID != "pod-uid-1" || ev.Resource.ClusterID != "test-cluster" {
			t.Errorf("evidence %s Resource = %+v", ev.ID, ev.Resource)
		}
		if ev.ContentHash == "" {
			t.Errorf("evidence %s ContentHash is empty", ev.ID)
		}
		kindsSeen[ev.Kind] = true
	}
	for _, want := range []string{"pod", "previous-logs", "current-logs", "events"} {
		if !kindsSeen[want] {
			t.Errorf("evidence kind %q missing; got %v", want, kindsSeen)
		}
	}
	if len(allEvidence) != 4 {
		t.Errorf("evidence count = %d, want 4", len(allEvidence))
	}
	if !strings.Contains(inc.Summary, "CrashLoopBackOff") {
		t.Errorf("summary = %q", inc.Summary)
	}
}

// fixedObsTime is a deterministic observation time.
var fixedObsTime = time.Date(2026, 11, 15, 12, 0, 0, 0, time.UTC)
