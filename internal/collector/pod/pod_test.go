package pod

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

func request() collector.Request {
	return collector.Request{
		Incident: incident(),
		Signal:   domain.FailureSignal{Type: "CrashLoopBackOff"},
	}
}

func fixturePod() *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "app",
			Name:      "web-1",
			UID:       types.UID("pod-uid-1"),
			Annotations: map[string]string{
				"note":             "deployment rollout 42",
				"vault.hashed-key": "token=supersecretvalue",
			},
		},
		Spec: corev1.PodSpec{
			NodeName: "node-1",
			Containers: []corev1.Container{
				{
					Name:  "app",
					Image: "nginx:1.0",
					Env: []corev1.EnvVar{
						{Name: "APP_MODE", Value: "debug"},
						{Name: "DB_PASSWORD", Value: "hunter2"},
						{Name: "API_TOKEN", Value: "tok-123456"},
						{Name: "SECRET_REF", ValueFrom: &corev1.EnvVarSource{
							SecretKeyRef: &corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: "db"}, Key: "password"},
						}},
					},
				},
			},
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			ContainerStatuses: []corev1.ContainerStatus{{
				Name:         "app",
				RestartCount: 5,
				State: corev1.ContainerState{
					Waiting: &corev1.ContainerStateWaiting{Reason: "CrashLoopBackOff"},
				},
				LastTerminationState: corev1.ContainerState{
					Terminated: &corev1.ContainerStateTerminated{ExitCode: 1, Reason: "Error"},
				},
			}},
		},
	}
}

func TestPodCollectorCollects(t *testing.T) {
	cs := fake.NewSimpleClientset(fixturePod())
	c := NewPodCollector(cs, evidence.DefaultOptions())

	result, err := c.Collect(context.Background(), request())
	if err != nil {
		t.Fatalf("Collect() error = %v", err)
	}
	if len(result.Evidence) != 1 {
		t.Fatalf("evidence count = %d, want 1", len(result.Evidence))
	}
	ev := result.Evidence[0]
	if ev.Kind != Kind || ev.Source != Source {
		t.Errorf("kind/source = %q/%q, want %q/%q", ev.Kind, ev.Source, Kind, Source)
	}
	if ev.IncidentID != "inc-1" {
		t.Errorf("IncidentID = %q, want inc-1", ev.IncidentID)
	}
	if ev.Resource.UID != "pod-uid-1" {
		t.Errorf("Resource = %+v", ev.Resource)
	}
	if ev.ObservedAt.IsZero() || ev.ObservedAt.Location() != time.UTC {
		t.Errorf("ObservedAt = %v, want non-zero UTC collection time", ev.ObservedAt)
	}
	if ev.ContentHash == "" {
		t.Error("ContentHash is empty")
	}

	var view map[string]any
	if err := json.Unmarshal(ev.Content, &view); err != nil {
		t.Fatalf("content is not valid JSON: %v", err)
	}

	// Sensitive env values are redacted; plain values and Secret
	// references are kept.
	spec := view["spec"].(map[string]any)
	containers := spec["containers"].([]any)
	env := containers[0].(map[string]any)["env"].([]any)
	values := map[string]string{}
	for _, e := range env {
		entry := e.(map[string]any)
		if v, ok := entry["value"].(string); ok {
			values[entry["name"].(string)] = v
		}
	}
	if values["APP_MODE"] != "debug" {
		t.Errorf("APP_MODE = %q, want debug (plain value must be kept)", values["APP_MODE"])
	}
	if values["DB_PASSWORD"] != "[REDACTED]" {
		t.Errorf("DB_PASSWORD = %q, want [REDACTED]", values["DB_PASSWORD"])
	}
	if values["API_TOKEN"] != "[REDACTED]" {
		t.Errorf("API_TOKEN = %q, want [REDACTED]", values["API_TOKEN"])
	}
	if _, ok := values["SECRET_REF"]; ok {
		t.Error("SECRET_REF has a literal value, want valueFrom reference only")
	}

	// Annotation values are redacted.
	meta := view["metadata"].(map[string]any)
	annotations := meta["annotations"].(map[string]any)
	if annotations["note"] != "deployment rollout 42" {
		t.Errorf("annotation note = %q, want unchanged", annotations["note"])
	}
	if strings.Contains(annotations["vault.hashed-key"].(string), "supersecretvalue") {
		t.Errorf("annotation value not redacted: %q", annotations["vault.hashed-key"])
	}
	if !ev.Redacted {
		t.Error("Redacted = false, want true (annotation redacted)")
	}

	// Termination state and restart count are preserved.
	statuses := view["status"].(map[string]any)["containerStatuses"].([]any)
	st := statuses[0].(map[string]any)
	if st["restartCount"].(float64) != 5 {
		t.Errorf("restartCount = %v, want 5", st["restartCount"])
	}
	last := st["lastState"].(map[string]any)["terminated"].(map[string]any)
	if last["reason"] != "Error" || last["exitCode"].(float64) != 1 {
		t.Errorf("last termination = %v, want Error/1", last)
	}
}

func TestPodCollectorPodNotFound(t *testing.T) {
	cs := fake.NewSimpleClientset()
	c := NewPodCollector(cs, evidence.DefaultOptions())

	result, err := c.Collect(context.Background(), request())
	if err != nil {
		t.Fatalf("Collect() error = %v, want nil for not-found race", err)
	}
	if len(result.Evidence) != 0 {
		t.Errorf("evidence count = %d, want 0", len(result.Evidence))
	}
	if len(result.Warnings) != 1 || !strings.Contains(result.Warnings[0], "not_found") {
		t.Errorf("warnings = %v, want classified not_found warning", result.Warnings)
	}
}

func TestPodCollectorForbidden(t *testing.T) {
	cs := fake.NewSimpleClientset()
	cs.PrependReactor("get", "pods", func(action clientgotesting.Action) (bool, runtime.Object, error) {
		return true, nil, apierrors.NewForbidden(schema.GroupResource{Resource: "pods"}, "web-1", errors.New("rbac"))
	})
	c := NewPodCollector(cs, evidence.DefaultOptions())

	result, err := c.Collect(context.Background(), request())
	if err != nil {
		t.Fatalf("Collect() error = %v, want nil for forbidden", err)
	}
	if len(result.Evidence) != 0 {
		t.Errorf("evidence count = %d, want 0", len(result.Evidence))
	}
	if len(result.Warnings) != 1 || !strings.Contains(result.Warnings[0], "forbidden") {
		t.Errorf("warnings = %v, want classified forbidden warning", result.Warnings)
	}
}

func TestPodCollectorTransientError(t *testing.T) {
	cs := fake.NewSimpleClientset()
	cs.PrependReactor("get", "pods", func(action clientgotesting.Action) (bool, runtime.Object, error) {
		return true, nil, apierrors.NewInternalError(errors.New("etcd lost"))
	})
	c := NewPodCollector(cs, evidence.DefaultOptions())

	result, err := c.Collect(context.Background(), request())
	if err == nil {
		t.Fatal("Collect() error = nil, want error for transient failure")
	}
	if len(result.Evidence) != 0 {
		t.Errorf("evidence count = %d, want 0", len(result.Evidence))
	}
	if len(result.Warnings) != 1 || !strings.Contains(result.Warnings[0], "transient") {
		t.Errorf("warnings = %v, want classified transient warning", result.Warnings)
	}
}

func TestPodCollectorContextCanceled(t *testing.T) {
	cs := fake.NewSimpleClientset(fixturePod())
	c := NewPodCollector(cs, evidence.DefaultOptions())

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := c.Collect(ctx, request())
	if !errors.Is(err, context.Canceled) {
		t.Errorf("Collect() error = %v, want context.Canceled", err)
	}
}
