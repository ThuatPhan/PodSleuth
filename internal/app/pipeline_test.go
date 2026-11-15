package app

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/ThuatPhan/PodSleuth/internal/detector"
	"github.com/ThuatPhan/PodSleuth/internal/domain"
	"github.com/ThuatPhan/PodSleuth/internal/incident"
	"github.com/ThuatPhan/PodSleuth/internal/watcher"
)

// pipelineEnv wires the full observation path with deterministic
// dependencies.
type pipelineEnv struct {
	clock *fakeClock
	mgr   *incident.Manager
}

func newPipelineEnv() *pipelineEnv {
	clock := &fakeClock{t: time.Date(2026, 11, 15, 12, 0, 0, 0, time.UTC)}
	ids := &idSeq{}
	mgr := incident.NewManager(detector.NewMemoryDeduplicator(), incident.Options{
		Clock: clock.Now,
		NewID: ids.NewID,
	})
	return &pipelineEnv{clock: clock, mgr: mgr}
}

type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

type idSeq struct {
	n int
}

func (s *idSeq) NewID() string {
	s.n++
	return fmt.Sprintf("inc-%d", s.n)
}

// waitFor polls until cond passes or the timeout expires.
func waitFor(t *testing.T, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal(msg)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func crashLoopPod(ns, name, uid, rv string, restarts int32) *corev1.Pod {
	controller := true
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:       ns,
			Name:            name,
			UID:             types.UID(uid),
			ResourceVersion: rv,
			OwnerReferences: []metav1.OwnerReference{{Kind: "ReplicaSet", Name: "web-7d9f", UID: types.UID("rs-uid-1"), Controller: &controller}},
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			Conditions: []corev1.PodCondition{
				{Type: corev1.PodReady, Status: corev1.ConditionFalse},
			},
			ContainerStatuses: []corev1.ContainerStatus{{
				Name:                 "app",
				RestartCount:         restarts,
				State:                corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "CrashLoopBackOff"}},
				LastTerminationState: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 1, Reason: "Error"}},
			}},
		},
	}
}

func TestPipelineWatcherToIncident(t *testing.T) {
	env := newPipelineEnv()
	cs := fake.NewSimpleClientset()
	w := watcher.NewPodWatcher(cs, "test-cluster", watcher.Options{})
	pipeline := NewPipeline(detector.NewPodDetector("test-cluster"), env.mgr)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = w.Start(ctx, pipeline) }()

	select {
	case <-w.Ready():
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for cache sync")
	}

	// A crashlooping Pod produces exactly one incident.
	pod := crashLoopPod("app", "web-1", "pod-uid-1", "1000", 5)
	if _, err := cs.CoreV1().Pods("app").Create(context.Background(), pod, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return len(env.mgr.ListActive()) == 1 }, "timed out waiting for incident creation")

	incidents := env.mgr.ListActive()
	inc := incidents[0]
	if inc.FailureType != detector.TypeCrashLoopBackOff || inc.ContainerName != "app" {
		t.Errorf("incident = %+v, want CrashLoopBackOff on container app", inc)
	}
	if inc.Resource.UID != "pod-uid-1" || inc.ClusterID != "test-cluster" {
		t.Errorf("incident resource = %+v", inc.Resource)
	}

	// Repeated updates of the same Pod must not create duplicates.
	for i := 1; i <= 3; i++ {
		env.clock.Advance(1 * time.Minute)
		updated := crashLoopPod("app", "web-1", "pod-uid-1", fmt.Sprintf("100%d", i+1), int32(5+i))
		if _, err := cs.CoreV1().Pods("app").Update(context.Background(), updated, metav1.UpdateOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	waitFor(t, func() bool {
		active := env.mgr.ListActive()
		return len(active) == 1 && active[0].LastSeenAt.After(inc.LastSeenAt)
	}, "timed out waiting for last-seen update")

	if got := len(env.mgr.List()); got != 1 {
		t.Errorf("total incidents after repeated updates = %d, want 1", got)
	}

	// Deleting the Pod must not resolve the incident.
	if err := cs.CoreV1().Pods("app").Delete(context.Background(), "web-1", metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool {
		// The delete observation has been processed when the incident's
		// resource version reflects the last known state.
		active := env.mgr.ListActive()
		return len(active) == 1 && active[0].Status == domain.StatusDetected
	}, "timed out waiting for delete observation")
	if got, _ := env.mgr.Get(inc.ID); got.Status != domain.StatusDetected {
		t.Errorf("status after pod deletion = %q, want still detected", got.Status)
	}
}

func TestPipelineHealthyPodCreatesNoIncident(t *testing.T) {
	env := newPipelineEnv()
	cs := fake.NewSimpleClientset()
	w := watcher.NewPodWatcher(cs, "test-cluster", watcher.Options{})
	pipeline := NewPipeline(detector.NewPodDetector("test-cluster"), env.mgr)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = w.Start(ctx, pipeline) }()

	select {
	case <-w.Ready():
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for cache sync")
	}

	healthy := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "web-1", UID: types.UID("pod-uid-1"), ResourceVersion: "1000"},
		Status: corev1.PodStatus{
			Phase:      corev1.PodRunning,
			Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}},
			ContainerStatuses: []corev1.ContainerStatus{{
				Name:  "app",
				State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}},
			}},
		},
	}
	if _, err := cs.CoreV1().Pods("app").Create(context.Background(), healthy, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}

	// Give the informer time to deliver the event, then assert nothing
	// was created.
	time.Sleep(300 * time.Millisecond)
	if got := len(env.mgr.List()); got != 0 {
		t.Errorf("healthy pod created %d incidents, want 0", got)
	}
}

// compile-time check that the pipeline satisfies the watcher handler
// contract.
var _ watcher.Handler = (*Pipeline)(nil)
