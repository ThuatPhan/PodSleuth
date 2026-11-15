package logs

import (
	"context"
	"errors"
	"strings"
	"testing"

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

func signal(container string) domain.FailureSignal {
	return domain.FailureSignal{Type: "CrashLoopBackOff", Attributes: map[string]string{"container": container}}
}

func podWithContainers(names ...string) *corev1.Pod {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "web-1", UID: types.UID("pod-uid-1")},
	}
	for _, n := range names {
		pod.Status.ContainerStatuses = append(pod.Status.ContainerStatuses, corev1.ContainerStatus{Name: n})
	}
	return pod
}

// logReactor serves log streams and records the requested options.
func logReactor(t *testing.T, cs *fake.Clientset, serve func(opts corev1.PodLogOptions) (string, error)) {
	t.Helper()
	cs.PrependReactor("get", "pods/log", func(action clientgotesting.Action) (bool, runtime.Object, error) {
		generic, ok := action.(clientgotesting.GenericAction)
		if !ok {
			t.Fatalf("expected GenericAction, got %T", action)
		}
		opts, ok := generic.GetValue().(*corev1.PodLogOptions)
		if !ok {
			t.Fatalf("expected *corev1.PodLogOptions, got %T", generic.GetValue())
		}
		text, err := serve(*opts)
		if err != nil {
			return true, nil, err
		}
		return true, &runtime.Unknown{Raw: []byte(text)}, nil
	})
}

func TestLogsCollectorPreviousAndCurrent(t *testing.T) {
	cs := fake.NewSimpleClientset(podWithContainers("app"))
	logReactor(t, cs, func(opts corev1.PodLogOptions) (string, error) {
		if opts.Previous {
			return "previous run: connecting with Bearer 0123456789abcdef012345\n", nil
		}
		return "current run: all good\n", nil
	})
	c := NewLogsCollector(cs, Options{PreviousLogs: true, CurrentLogs: true}, evidence.DefaultOptions())

	result, err := c.Collect(context.Background(), collector.Request{Incident: incident(), Signal: signal("app")})
	if err != nil {
		t.Fatalf("Collect() error = %v", err)
	}
	if len(result.Evidence) != 2 {
		t.Fatalf("evidence count = %d, want 2", len(result.Evidence))
	}

	var prev, cur *domain.Evidence
	for i := range result.Evidence {
		switch result.Evidence[i].Kind {
		case KindPrevious:
			prev = &result.Evidence[i]
		case KindCurrent:
			cur = &result.Evidence[i]
		}
	}
	if prev == nil || cur == nil {
		t.Fatalf("kinds = %q, %q, want previous and current", result.Evidence[0].Kind, result.Evidence[1].Kind)
	}

	if !strings.Contains(prev.Text, "previous run") {
		t.Errorf("previous text = %q", prev.Text)
	}
	if strings.Contains(prev.Text, "0123456789abcdef012345") {
		t.Errorf("previous text contains token: %q", prev.Text)
	}
	if !prev.Redacted {
		t.Error("previous Redacted = false, want true")
	}
	if !strings.Contains(cur.Text, "current run") {
		t.Errorf("current text = %q", cur.Text)
	}
	if cur.Redacted {
		t.Error("current Redacted = true, want false")
	}
	for _, ev := range []*domain.Evidence{prev, cur} {
		if ev.IncidentID != "inc-1" || ev.Source != Source {
			t.Errorf("identity = %q/%q, want inc-1/%s", ev.IncidentID, ev.Source, Source)
		}
		if ev.ContentHash == "" {
			t.Error("ContentHash is empty")
		}
	}
}

func TestLogsCollectorMissingPreviousLogs(t *testing.T) {
	cs := fake.NewSimpleClientset(podWithContainers("app"))
	logReactor(t, cs, func(opts corev1.PodLogOptions) (string, error) {
		if opts.Previous {
			return "", apierrors.NewBadRequest("previous terminated container \"app\" in pod \"web-1\" not found")
		}
		return "current run\n", nil
	})
	c := NewLogsCollector(cs, Options{PreviousLogs: true, CurrentLogs: true}, evidence.DefaultOptions())

	result, err := c.Collect(context.Background(), collector.Request{Incident: incident(), Signal: signal("app")})
	if err != nil {
		t.Fatalf("Collect() error = %v", err)
	}
	if len(result.Evidence) != 1 || result.Evidence[0].Kind != KindCurrent {
		t.Fatalf("evidence = %+v, want only current logs", kinds(result.Evidence))
	}
	if len(result.Warnings) != 1 || !strings.Contains(result.Warnings[0], "has not terminated") {
		t.Errorf("warnings = %v, want missing-previous warning", result.Warnings)
	}
}

func TestLogsCollectorPartialFailureAcrossContainers(t *testing.T) {
	cs := fake.NewSimpleClientset(podWithContainers("a", "b"))
	logReactor(t, cs, func(opts corev1.PodLogOptions) (string, error) {
		if opts.Container == "a" {
			return "", apierrors.NewInternalError(errors.New("kubelet unreachable"))
		}
		return "logs for b\n", nil
	})
	c := NewLogsCollector(cs, Options{PreviousLogs: false, CurrentLogs: true}, evidence.DefaultOptions())

	// No specific container: all containers are targeted.
	result, err := c.Collect(context.Background(), collector.Request{Incident: incident(), Signal: domain.FailureSignal{Type: "CrashLoopBackOff"}})
	if err != nil {
		t.Fatalf("Collect() error = %v", err)
	}
	if len(result.Evidence) != 1 || result.Evidence[0].Text != "logs for b\n" {
		t.Fatalf("evidence = %+v, want only container b logs", result.Evidence)
	}
	if len(result.Warnings) != 1 || !strings.Contains(result.Warnings[0], "container a") {
		t.Errorf("warnings = %v, want warning for container a", result.Warnings)
	}
}

func TestLogsCollectorPodNotFound(t *testing.T) {
	cs := fake.NewSimpleClientset()
	c := NewLogsCollector(cs, Options{PreviousLogs: true, CurrentLogs: true}, evidence.DefaultOptions())

	result, err := c.Collect(context.Background(), collector.Request{Incident: incident(), Signal: signal("app")})
	if err == nil {
		t.Fatal("Collect() error = nil, want not-found error (caller decides on retries)")
	}
	if len(result.Evidence) != 0 {
		t.Errorf("evidence count = %d, want 0", len(result.Evidence))
	}
	if len(result.Warnings) != 1 || !strings.Contains(result.Warnings[0], "not_found") {
		t.Errorf("warnings = %v, want classified not_found warning", result.Warnings)
	}
}

func TestLogsCollectorForbidden(t *testing.T) {
	cs := fake.NewSimpleClientset(podWithContainers("app"))
	cs.PrependReactor("get", "pods", func(action clientgotesting.Action) (bool, runtime.Object, error) {
		return true, nil, apierrors.NewForbidden(schema.GroupResource{Resource: "pods"}, "web-1", errors.New("rbac"))
	})
	c := NewLogsCollector(cs, Options{PreviousLogs: true, CurrentLogs: true}, evidence.DefaultOptions())

	result, err := c.Collect(context.Background(), collector.Request{Incident: incident(), Signal: signal("app")})
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

func TestLogsCollectorTransientError(t *testing.T) {
	cs := fake.NewSimpleClientset(podWithContainers("app"))
	cs.PrependReactor("get", "pods", func(action clientgotesting.Action) (bool, runtime.Object, error) {
		return true, nil, apierrors.NewInternalError(errors.New("boom"))
	})
	c := NewLogsCollector(cs, Options{PreviousLogs: true, CurrentLogs: true}, evidence.DefaultOptions())

	_, err := c.Collect(context.Background(), collector.Request{Incident: incident(), Signal: signal("app")})
	if err == nil {
		t.Fatal("Collect() error = nil, want error for transient failure")
	}
}

func TestLogsCollectorTruncates(t *testing.T) {
	cs := fake.NewSimpleClientset(podWithContainers("app"))
	logReactor(t, cs, func(opts corev1.PodLogOptions) (string, error) {
		return strings.Repeat("x", 100), nil
	})
	c := NewLogsCollector(cs, Options{PreviousLogs: false, CurrentLogs: true}, evidence.Options{MaxLogBytesPerContainer: 10})

	result, err := c.Collect(context.Background(), collector.Request{Incident: incident(), Signal: signal("app")})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Evidence) != 1 {
		t.Fatalf("evidence count = %d, want 1", len(result.Evidence))
	}
	ev := result.Evidence[0]
	if len(ev.Text) != 10 {
		t.Errorf("text length = %d, want 10", len(ev.Text))
	}
	if ev.CollectionStatus != evidence.StatusTruncated {
		t.Errorf("CollectionStatus = %q, want truncated", ev.CollectionStatus)
	}
	if len(result.Warnings) != 1 || !strings.Contains(result.Warnings[0], "truncated") {
		t.Errorf("warnings = %v, want truncation warning", result.Warnings)
	}
}

func TestLogsCollectorContextCanceled(t *testing.T) {
	cs := fake.NewSimpleClientset(podWithContainers("app"))
	c := NewLogsCollector(cs, Options{PreviousLogs: true, CurrentLogs: true}, evidence.DefaultOptions())

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := c.Collect(ctx, collector.Request{Incident: incident(), Signal: signal("app")})
	if !errors.Is(err, context.Canceled) {
		t.Errorf("Collect() error = %v, want context.Canceled", err)
	}
}

func TestLogsCollectorSupports(t *testing.T) {
	c := NewLogsCollector(fake.NewSimpleClientset(), Options{}, evidence.DefaultOptions())
	noLogs := []string{"FailedScheduling", "ImagePullBackOff", "CreateContainerConfigError"}
	for _, typ := range noLogs {
		if c.Supports(domain.FailureSignal{Type: typ}) {
			t.Errorf("Supports(%q) = true, want false", typ)
		}
	}
	withLogs := []string{"CrashLoopBackOff", "OOMKilled", "NonZeroExit", "Evicted"}
	for _, typ := range withLogs {
		if !c.Supports(domain.FailureSignal{Type: typ}) {
			t.Errorf("Supports(%q) = false, want true", typ)
		}
	}
}

func kinds(evs []domain.Evidence) []string {
	out := make([]string, 0, len(evs))
	for _, e := range evs {
		out = append(out, e.Kind)
	}
	return out
}
