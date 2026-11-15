package watcher

import (
	"context"
	"encoding/json"
	"errors"
	goruntime "runtime"
	"strings"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes/fake"
	clientgotesting "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/cache"

	"github.com/ThuatPhan/PodSleuth/internal/domain"
)

// fixedNow is a deterministic observation time.
var fixedNow = time.Date(2026, 11, 15, 12, 0, 0, 0, time.UTC)

// recordingHandler collects observations and errors from a watcher.
type recordingHandler struct {
	mu       sync.Mutex
	obs      []domain.Observation
	errs     []error
	failNext bool
	obsCh    chan domain.Observation
	errCh    chan error
}

func newRecordingHandler() *recordingHandler {
	return &recordingHandler{
		obsCh: make(chan domain.Observation, 100),
		errCh: make(chan error, 100),
	}
}

func (h *recordingHandler) OnObservation(_ context.Context, obs domain.Observation) error {
	h.mu.Lock()
	fail := h.failNext
	h.obs = append(h.obs, obs)
	h.mu.Unlock()
	h.obsCh <- obs
	if fail {
		return errors.New("rejected")
	}
	return nil
}

func (h *recordingHandler) OnWatchError(_ context.Context, err error) {
	h.mu.Lock()
	h.errs = append(h.errs, err)
	h.mu.Unlock()
	h.errCh <- err
}

func (h *recordingHandler) setFailNext(v bool) {
	h.mu.Lock()
	h.failNext = v
	h.mu.Unlock()
}

func (h *recordingHandler) observations() []domain.Observation {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]domain.Observation(nil), h.obs...)
}

// waitObservation waits for the next observation or fails the test.
func (h *recordingHandler) waitObservation(t *testing.T) domain.Observation {
	t.Helper()
	select {
	case obs := <-h.obsCh:
		return obs
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for observation; got %d so far", len(h.observations()))
		return domain.Observation{}
	}
}

// waitNoObservation fails the test if an observation arrives within d.
func (h *recordingHandler) waitNoObservation(t *testing.T, d time.Duration) {
	t.Helper()
	select {
	case obs := <-h.obsCh:
		t.Fatalf("unexpected observation for %s/%s", obs.Resource.Namespace, obs.Resource.Name)
	case <-time.After(d):
	}
}

// newPod builds a Pod fixture with the given identity and resource version.
func newPod(namespace, name, uid, resourceVersion string, labels map[string]string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:       namespace,
			Name:            name,
			UID:             types.UID("uid-" + uid),
			ResourceVersion: resourceVersion,
			Labels:          labels,
		},
		Spec: corev1.PodSpec{
			NodeName: "node-1",
			Containers: []corev1.Container{
				{Name: "app", Image: "nginx:1.0"},
			},
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
		},
	}
}

// startWatcher starts a watcher, waits for cache sync, and registers
// cleanup that cancels the context and verifies Start returns cleanly.
func startWatcher(t *testing.T, w *PodWatcher, h *recordingHandler) context.CancelFunc {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.Start(ctx, h) }()

	select {
	case <-w.Ready():
	case <-time.After(5 * time.Second):
		cancel()
		t.Fatal("timed out waiting for cache sync")
	}

	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("Start returned error: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Error("Start did not return after context cancellation")
		}
	})
	return cancel
}

func TestConvertPod(t *testing.T) {
	pod := newPod("ns-a", "pod-1", "u1", "1000", nil)

	obs, err := convertPod("test-cluster", pod, fixedNow)
	if err != nil {
		t.Fatalf("convertPod() error = %v", err)
	}

	wantRef := domain.ResourceRef{
		ClusterID:  "test-cluster",
		APIVersion: "v1",
		Kind:       "Pod",
		Namespace:  "ns-a",
		Name:       "pod-1",
		UID:        "uid-u1",
	}
	if obs.Resource != wantRef {
		t.Errorf("Resource = %+v, want %+v", obs.Resource, wantRef)
	}
	if !obs.ObservedAt.Equal(fixedNow) {
		t.Errorf("ObservedAt = %v, want %v", obs.ObservedAt, fixedNow)
	}
	if obs.ResourceVersion != "1000" {
		t.Errorf("ResourceVersion = %q, want %q", obs.ResourceVersion, "1000")
	}

	var payload corev1.Pod
	if err := json.Unmarshal(obs.Payload, &payload); err != nil {
		t.Fatalf("payload is not valid Pod JSON: %v", err)
	}
	if payload.Name != "pod-1" || payload.Status.Phase != corev1.PodRunning {
		t.Errorf("payload = %s, want round-trippable Pod", obs.Payload)
	}
}

func TestWatcherPodAdded(t *testing.T) {
	cs := fake.NewSimpleClientset()
	h := newRecordingHandler()
	w := NewPodWatcher(cs, "test-cluster", Options{Now: func() time.Time { return fixedNow }})
	startWatcher(t, w, h)

	pod := newPod("ns-a", "pod-1", "u1", "1000", nil)
	if _, err := cs.CoreV1().Pods("ns-a").Create(context.Background(), pod, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}

	obs := h.waitObservation(t)
	if obs.Resource.Name != "pod-1" || obs.Resource.Namespace != "ns-a" {
		t.Errorf("observation = %+v, want pod ns-a/pod-1", obs.Resource)
	}
	if obs.Resource.UID != "uid-u1" {
		t.Errorf("UID = %q, want uid-u1", obs.Resource.UID)
	}
	if !obs.ObservedAt.Equal(fixedNow) {
		t.Errorf("ObservedAt = %v, want fixed clock value", obs.ObservedAt)
	}
}

func TestWatcherPodUpdated(t *testing.T) {
	cs := fake.NewSimpleClientset()
	h := newRecordingHandler()
	w := NewPodWatcher(cs, "test-cluster", Options{Now: func() time.Time { return fixedNow }})
	startWatcher(t, w, h)

	pod := newPod("ns-a", "pod-1", "u1", "1000", nil)
	if _, err := cs.CoreV1().Pods("ns-a").Create(context.Background(), pod, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	h.waitObservation(t) // initial add

	updated := newPod("ns-a", "pod-1", "u1", "1001", nil)
	updated.Status.Phase = corev1.PodFailed
	if _, err := cs.CoreV1().Pods("ns-a").Update(context.Background(), updated, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}

	obs := h.waitObservation(t)
	if obs.ResourceVersion != "1001" {
		t.Errorf("ResourceVersion = %q, want 1001", obs.ResourceVersion)
	}
	var payload corev1.Pod
	if err := json.Unmarshal(obs.Payload, &payload); err != nil {
		t.Fatal(err)
	}
	if payload.Status.Phase != corev1.PodFailed {
		t.Errorf("payload phase = %v, want Failed", payload.Status.Phase)
	}
}

func TestWatcherSkipsNoOpUpdates(t *testing.T) {
	oldPod := newPod("ns-a", "pod-1", "u1", "1000", nil)
	samePod := newPod("ns-a", "pod-1", "u1", "1000", nil)
	changedPod := newPod("ns-a", "pod-1", "u1", "1001", nil)

	h := newRecordingHandler()
	w := NewPodWatcher(fake.NewSimpleClientset(), "test-cluster", Options{})
	ctx := context.Background()

	w.handleUpdate(ctx, h, oldPod, samePod)
	h.waitNoObservation(t, 50*time.Millisecond)

	w.handleUpdate(ctx, h, oldPod, changedPod)
	obs := h.waitObservation(t)
	if obs.ResourceVersion != "1001" {
		t.Errorf("ResourceVersion = %q, want 1001", obs.ResourceVersion)
	}
}

func TestWatcherPodDeleted(t *testing.T) {
	cs := fake.NewSimpleClientset()
	h := newRecordingHandler()
	w := NewPodWatcher(cs, "test-cluster", Options{Now: func() time.Time { return fixedNow }})
	startWatcher(t, w, h)

	pod := newPod("ns-a", "pod-1", "u1", "1000", nil)
	if _, err := cs.CoreV1().Pods("ns-a").Create(context.Background(), pod, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	h.waitObservation(t) // initial add

	if err := cs.CoreV1().Pods("ns-a").Delete(context.Background(), "pod-1", metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}

	obs := h.waitObservation(t)
	if obs.Resource.Name != "pod-1" {
		t.Errorf("observation = %+v, want deleted pod pod-1", obs.Resource)
	}
}

func TestWatcherDeletedTombstone(t *testing.T) {
	h := newRecordingHandler()
	w := NewPodWatcher(fake.NewSimpleClientset(), "test-cluster", Options{})

	pod := newPod("ns-a", "pod-1", "u1", "1000", nil)
	tombstone := cache.DeletedFinalStateUnknown{Key: "ns-a/pod-1", Obj: pod}
	w.handleDelete(context.Background(), h, tombstone)

	obs := h.waitObservation(t)
	if obs.Resource.Name != "pod-1" || obs.Resource.UID != "uid-u1" {
		t.Errorf("observation = %+v, want pod identity from tombstone", obs.Resource)
	}
	if len(h.errs) != 0 {
		t.Errorf("errors = %v, want none", h.errs)
	}
}

func TestWatcherNamespaceFilter(t *testing.T) {
	cs := fake.NewSimpleClientset()
	h := newRecordingHandler()
	w := NewPodWatcher(cs, "test-cluster", Options{
		Namespace: "watched",
		Now:       func() time.Time { return fixedNow },
	})
	startWatcher(t, w, h)

	// A Pod outside the watched namespace must not be observed.
	other := newPod("other", "pod-x", "ux", "1000", nil)
	if _, err := cs.CoreV1().Pods("other").Create(context.Background(), other, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	h.waitNoObservation(t, 200*time.Millisecond)

	watched := newPod("watched", "pod-1", "u1", "1000", nil)
	if _, err := cs.CoreV1().Pods("watched").Create(context.Background(), watched, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	obs := h.waitObservation(t)
	if obs.Resource.Namespace != "watched" {
		t.Errorf("observation = %+v, want namespace watched", obs.Resource)
	}
}

func TestWatcherLabelSelectorPropagated(t *testing.T) {
	// The fake tracker does not filter by label selector, so assert the
	// selector reaches the list and watch options instead.
	var mu sync.Mutex
	var selectors []string
	cs := fake.NewSimpleClientset()
	cs.PrependReactor("list", "pods", func(action clientgotesting.Action) (bool, k8sruntime.Object, error) {
		mu.Lock()
		defer mu.Unlock()
		selectors = append(selectors, action.(clientgotesting.ListAction).GetListRestrictions().Labels.String())
		return false, nil, nil
	})
	cs.PrependWatchReactor("pods", func(action clientgotesting.Action) (bool, watch.Interface, error) {
		mu.Lock()
		defer mu.Unlock()
		selectors = append(selectors, action.(clientgotesting.WatchAction).GetWatchRestrictions().Labels.String())
		return false, nil, nil
	})

	h := newRecordingHandler()
	w := NewPodWatcher(cs, "test-cluster", Options{LabelSelector: "app=web"})
	startWatcher(t, w, h)

	mu.Lock()
	defer mu.Unlock()
	if len(selectors) == 0 {
		t.Fatal("no list or watch calls captured")
	}
	for i, sel := range selectors {
		if sel != "app=web" {
			t.Errorf("selectors[%d] = %q, want app=web", i, sel)
		}
	}
}

func TestWatcherHandlerErrorReported(t *testing.T) {
	cs := fake.NewSimpleClientset()
	h := newRecordingHandler()
	w := NewPodWatcher(cs, "test-cluster", Options{Now: func() time.Time { return fixedNow }})
	startWatcher(t, w, h)

	// The first observation is rejected by the handler.
	h.setFailNext(true)

	pod := newPod("ns-a", "pod-1", "u1", "1000", nil)
	if _, err := cs.CoreV1().Pods("ns-a").Create(context.Background(), pod, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}

	select {
	case err := <-h.errCh:
		if !strings.Contains(err.Error(), "handler rejected observation") {
			t.Errorf("error = %v, want handler rejection report", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for watch error")
	}
	// Consume the rejected observation still buffered on the channel.
	h.waitObservation(t)

	// The watcher must keep running after a handler error.
	h.setFailNext(false)
	pod2 := newPod("ns-a", "pod-2", "u2", "1001", nil)
	if _, err := cs.CoreV1().Pods("ns-a").Create(context.Background(), pod2, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	obs := h.waitObservation(t)
	if obs.Resource.Name != "pod-2" {
		t.Errorf("observation = %+v, want pod-2 after handler error", obs.Resource)
	}
}

func TestWatcherUnexpectedObjectType(t *testing.T) {
	h := newRecordingHandler()
	w := NewPodWatcher(fake.NewSimpleClientset(), "test-cluster", Options{})
	ctx := context.Background()

	w.handleAdd(ctx, h, &corev1.ConfigMap{})
	w.handleUpdate(ctx, h, &corev1.ConfigMap{}, &corev1.ConfigMap{})
	w.handleDelete(ctx, h, "not-an-object")

	for i, want := range []string{"added", "updated", "deleted"} {
		select {
		case err := <-h.errCh:
			if !strings.Contains(err.Error(), want+" object type") {
				t.Errorf("errors[%d] = %v, want unexpected %s object type", i, err, want)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("timed out waiting for error %d", i)
		}
	}
}

// waitStarted waits until the watcher's Start has begun.
func waitStarted(t *testing.T, w *PodWatcher) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !w.started.Load() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if !w.started.Load() {
		t.Fatal("watcher did not start")
	}
}

func TestWatcherStartGuards(t *testing.T) {
	w := NewPodWatcher(fake.NewSimpleClientset(), "test-cluster", Options{})

	if err := w.Start(context.Background(), nil); err == nil {
		t.Error("Start(nil handler) error = nil, want error")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- w.Start(ctx, newRecordingHandler()) }()
	waitStarted(t, w)
	if err := w.Start(ctx, newRecordingHandler()); err == nil {
		t.Error("second Start error = nil, want already-started error")
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("Start returned error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Start did not return after cancellation")
	}
}

func TestWatcherGracefulShutdown(t *testing.T) {
	before := goruntime.NumGoroutine()

	cs := fake.NewSimpleClientset()
	h := newRecordingHandler()
	w := NewPodWatcher(cs, "test-cluster", Options{Now: func() time.Time { return fixedNow }})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.Start(ctx, h) }()

	select {
	case <-w.Ready():
	case <-time.After(5 * time.Second):
		cancel()
		t.Fatal("timed out waiting for cache sync")
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Start returned error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Start did not return after cancellation")
	}

	// Goroutines owned by the watcher and informer must wind down.
	deadline := time.Now().Add(5 * time.Second)
	for goruntime.NumGoroutine() > before && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if n := goruntime.NumGoroutine(); n > before {
		t.Errorf("goroutines after shutdown = %d, want <= %d (leak)", n, before)
	}
}

func TestWatcherReadyClosesOnEarlyStop(t *testing.T) {
	cs := fake.NewSimpleClientset()
	w := NewPodWatcher(cs, "test-cluster", Options{})

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // stop before Start even runs
	done := make(chan error, 1)
	go func() { done <- w.Start(ctx, newRecordingHandler()) }()

	select {
	case <-w.Ready():
	case <-time.After(5 * time.Second):
		t.Fatal("Ready did not close after early stop; consumers would block forever")
	}
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("Start returned error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Start did not return")
	}
}

func TestDefaultResyncPeriod(t *testing.T) {
	w := NewPodWatcher(fake.NewSimpleClientset(), "test-cluster", Options{})
	if w.opts.ResyncPeriod != DefaultResyncPeriod {
		t.Errorf("ResyncPeriod = %v, want %v", w.opts.ResyncPeriod, DefaultResyncPeriod)
	}
	if w.opts.Now == nil {
		t.Error("Now = nil, want default clock")
	}
}
