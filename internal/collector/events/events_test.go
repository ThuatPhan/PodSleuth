package events

import (
	"context"
	"encoding/json"
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

	"github.com/ThuatPhan/PodSleuth/internal/collector"
	"github.com/ThuatPhan/PodSleuth/internal/domain"
	"github.com/ThuatPhan/PodSleuth/internal/evidence"
)

func incident() domain.Incident {
	return domain.Incident{
		ID:        "inc-1",
		ClusterID: "test-cluster",
		Status:    domain.StatusDetected,
		Resource: domain.ResourceRef{
			ClusterID:  "test-cluster",
			APIVersion: "v1",
			Kind:       "Pod",
			Namespace:  "app",
			Name:       "web-1",
			UID:        "pod-uid-1",
		},
	}
}

func event(name, uid, evType, reason, message string, last time.Time) corev1.Event {
	return corev1.Event{
		ObjectMeta:     metav1.ObjectMeta{Namespace: "app", Name: name},
		InvolvedObject: corev1.ObjectReference{Kind: "Pod", Namespace: "app", Name: name, UID: types.UID(uid)},
		Type:           evType,
		Reason:         reason,
		Message:        message,
		Count:          3,
		FirstTimestamp: metav1.NewTime(last.Add(-time.Minute)),
		LastTimestamp:  metav1.NewTime(last),
		Source:         corev1.EventSource{Component: "kubelet"},
	}
}

// selectorRecorder records the field selectors of events list calls and
// serves canned results per selector.
func selectorRecorder(t *testing.T, cs *fake.Clientset, serve func(fieldSelector string) ([]corev1.Event, error)) *[]string {
	t.Helper()
	var mu selectorLog
	cs.PrependReactor("list", "events", func(action clientgotesting.Action) (bool, runtime.Object, error) {
		list, ok := action.(clientgotesting.ListAction)
		if !ok {
			t.Fatalf("expected ListAction, got %T", action)
		}
		sel := list.GetListRestrictions().Fields.String()
		mu.add(sel)
		items, err := serve(sel)
		if err != nil {
			return true, nil, err
		}
		return true, &corev1.EventList{Items: items}, nil
	})
	return &mu.selectors
}

type selectorLog struct {
	selectors []string
}

func (l *selectorLog) add(s string) { l.selectors = append(l.selectors, s) }

func TestEventsCollectorQueriesByUID(t *testing.T) {
	last := time.Date(2026, 11, 15, 12, 0, 0, 0, time.UTC)
	cs := fake.NewSimpleClientset()
	selectors := selectorRecorder(t, cs, func(sel string) ([]corev1.Event, error) {
		if !strings.Contains(sel, "involvedObject.uid=pod-uid-1") {
			return nil, nil
		}
		return []corev1.Event{
			event("web-1", "pod-uid-1", "Warning", "BackOff", "back-off restarting failed container", last),
			event("web-1", "pod-uid-1", "Normal", "Pulled", "image pulled", last.Add(-time.Minute)),
		}, nil
	})
	c := NewEventsCollector(cs, evidence.DefaultOptions())

	result, err := c.Collect(context.Background(), collector.Request{Incident: incident()})
	if err != nil {
		t.Fatalf("Collect() error = %v", err)
	}
	if len(*selectors) != 1 || !strings.Contains((*selectors)[0], "involvedObject.uid=pod-uid-1") {
		t.Fatalf("selectors = %v, want single UID query", *selectors)
	}
	if len(result.Evidence) != 1 {
		t.Fatalf("evidence count = %d, want 1", len(result.Evidence))
	}
	ev := result.Evidence[0]
	if ev.Kind != Kind || ev.Source != Source || ev.IncidentID != "inc-1" {
		t.Errorf("identity = %q/%q/%q", ev.Kind, ev.Source, ev.IncidentID)
	}
	if ev.EventTime == nil || !ev.EventTime.Equal(last) {
		t.Errorf("EventTime = %v, want %v (latest event)", ev.EventTime, last)
	}

	var views []eventView
	if err := json.Unmarshal(ev.Content, &views); err != nil {
		t.Fatalf("content is not valid JSON: %v", err)
	}
	if len(views) != 2 {
		t.Fatalf("event views = %d, want 2", len(views))
	}
	if views[0].Reason != "BackOff" || views[0].Count != 3 || views[0].ReportingComponent != "kubelet" {
		t.Errorf("view = %+v", views[0])
	}
	if views[0].InvolvedObject.UID != "pod-uid-1" {
		t.Errorf("involved object = %+v", views[0].InvolvedObject)
	}
}

func TestEventsCollectorFallsBackToName(t *testing.T) {
	last := time.Date(2026, 11, 15, 12, 0, 0, 0, time.UTC)
	cs := fake.NewSimpleClientset()
	selectors := selectorRecorder(t, cs, func(sel string) ([]corev1.Event, error) {
		if strings.Contains(sel, "involvedObject.uid=") {
			return nil, nil // UID query returns nothing
		}
		// Name query returns events for two pods with the same name.
		return []corev1.Event{
			event("web-1", "pod-uid-1", "Warning", "BackOff", "back-off", last),
			event("web-1", "pod-uid-OTHER", "Warning", "BackOff", "other pod", last),
		}, nil
	})
	c := NewEventsCollector(cs, evidence.DefaultOptions())

	result, err := c.Collect(context.Background(), collector.Request{Incident: incident()})
	if err != nil {
		t.Fatal(err)
	}
	if len(*selectors) != 2 {
		t.Fatalf("selectors = %v, want UID query then name fallback", *selectors)
	}
	if !strings.Contains((*selectors)[0], "involvedObject.uid=") || !strings.Contains((*selectors)[1], "involvedObject.name=web-1") {
		t.Fatalf("selectors = %v, want UID first then name", *selectors)
	}

	var views []eventView
	if err := json.Unmarshal(result.Evidence[0].Content, &views); err != nil {
		t.Fatal(err)
	}
	if len(views) != 1 || views[0].InvolvedObject.UID != "pod-uid-1" {
		t.Errorf("views = %+v, want only the incident pod's events", views)
	}
}

func TestEventsCollectorRedactsMessages(t *testing.T) {
	last := time.Date(2026, 11, 15, 12, 0, 0, 0, time.UTC)
	cs := fake.NewSimpleClientset()
	selectorRecorder(t, cs, func(sel string) ([]corev1.Event, error) {
		return []corev1.Event{
			event("web-1", "pod-uid-1", "Warning", "FailedMount", "mount failed: password=supersecret", last),
		}, nil
	})
	c := NewEventsCollector(cs, evidence.DefaultOptions())

	result, err := c.Collect(context.Background(), collector.Request{Incident: incident()})
	if err != nil {
		t.Fatal(err)
	}
	if !result.Evidence[0].Redacted {
		t.Error("Redacted = false, want true")
	}
	if strings.Contains(string(result.Evidence[0].Content), "supersecret") {
		t.Error("content contains unredacted secret")
	}
}

func TestEventsCollectorNoEvents(t *testing.T) {
	cs := fake.NewSimpleClientset()
	selectorRecorder(t, cs, func(sel string) ([]corev1.Event, error) { return nil, nil })
	c := NewEventsCollector(cs, evidence.DefaultOptions())

	result, err := c.Collect(context.Background(), collector.Request{Incident: incident()})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Evidence) != 0 {
		t.Errorf("evidence count = %d, want 0", len(result.Evidence))
	}
	if len(result.Warnings) != 1 || !strings.Contains(result.Warnings[0], "no events") {
		t.Errorf("warnings = %v, want no-events warning", result.Warnings)
	}
}

func TestEventsCollectorCapsEvents(t *testing.T) {
	last := time.Date(2026, 11, 15, 12, 0, 0, 0, time.UTC)
	cs := fake.NewSimpleClientset()
	selectorRecorder(t, cs, func(sel string) ([]corev1.Event, error) {
		items := make([]corev1.Event, maxEvents+10)
		for i := range items {
			items[i] = event("web-1", "pod-uid-1", "Normal", "Pulled", "ok", last)
		}
		return items, nil
	})
	c := NewEventsCollector(cs, evidence.DefaultOptions())

	result, err := c.Collect(context.Background(), collector.Request{Incident: incident()})
	if err != nil {
		t.Fatal(err)
	}
	var views []eventView
	if err := json.Unmarshal(result.Evidence[0].Content, &views); err != nil {
		t.Fatal(err)
	}
	if len(views) != maxEvents {
		t.Errorf("views = %d, want %d", len(views), maxEvents)
	}
	if len(result.Warnings) != 1 || !strings.Contains(result.Warnings[0], "capped") {
		t.Errorf("warnings = %v, want cap warning", result.Warnings)
	}
}

func TestEventsCollectorForbidden(t *testing.T) {
	cs := fake.NewSimpleClientset()
	cs.PrependReactor("list", "events", func(action clientgotesting.Action) (bool, runtime.Object, error) {
		return true, nil, apierrors.NewForbidden(schema.GroupResource{Resource: "events"}, "", errors.New("rbac"))
	})
	c := NewEventsCollector(cs, evidence.DefaultOptions())

	result, err := c.Collect(context.Background(), collector.Request{Incident: incident()})
	if err == nil {
		t.Fatal("Collect() error = nil, want forbidden error (caller decides on retries)")
	}
	if len(result.Evidence) != 0 {
		t.Errorf("evidence count = %d, want 0", len(result.Evidence))
	}
	if len(result.Warnings) != 1 || !strings.Contains(result.Warnings[0], "forbidden") {
		t.Errorf("warnings = %v, want classified forbidden warning", result.Warnings)
	}
}

func TestEventsCollectorTransientError(t *testing.T) {
	cs := fake.NewSimpleClientset()
	cs.PrependReactor("list", "events", func(action clientgotesting.Action) (bool, runtime.Object, error) {
		return true, nil, apierrors.NewInternalError(errors.New("boom"))
	})
	c := NewEventsCollector(cs, evidence.DefaultOptions())

	_, err := c.Collect(context.Background(), collector.Request{Incident: incident()})
	if err == nil {
		t.Fatal("Collect() error = nil, want error for transient failure")
	}
}

func TestEventsCollectorContextCanceled(t *testing.T) {
	cs := fake.NewSimpleClientset()
	c := NewEventsCollector(cs, evidence.DefaultOptions())

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := c.Collect(ctx, collector.Request{Incident: incident()})
	if !errors.Is(err, context.Canceled) {
		t.Errorf("Collect() error = %v, want context.Canceled", err)
	}
}
