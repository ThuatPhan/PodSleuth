package detector

import (
	"context"
	"encoding/json"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/ThuatPhan/PodSleuth/internal/domain"
)

// fixture builds a Pod and marshals it exactly like the watcher payload.
func fixture(t *testing.T, meta metav1.ObjectMeta, status corev1.PodStatus) domain.Observation {
	t.Helper()
	pod := &corev1.Pod{ObjectMeta: meta, Status: status}
	payload, err := json.Marshal(pod)
	if err != nil {
		t.Fatal(err)
	}
	return domain.Observation{
		Resource: domain.ResourceRef{
			ClusterID:  "test-cluster",
			APIVersion: "v1",
			Kind:       "Pod",
			Namespace:  meta.Namespace,
			Name:       meta.Name,
			UID:        string(meta.UID),
		},
		ObservedAt:      fixedObsTime,
		ResourceVersion: meta.ResourceVersion,
		Payload:         payload,
	}
}

func meta(ns, name, uid string, owners ...metav1.OwnerReference) metav1.ObjectMeta {
	return metav1.ObjectMeta{
		Namespace:       ns,
		Name:            name,
		UID:             types.UID(uid),
		ResourceVersion: "1000",
		OwnerReferences: owners,
	}
}

func replicaSetOwner(uid string) metav1.OwnerReference {
	controller := true
	return metav1.OwnerReference{
		Kind:       "ReplicaSet",
		Name:       "web-7d9f",
		UID:        types.UID(uid),
		Controller: &controller,
	}
}

func waiting(reason string) corev1.ContainerState {
	return corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: reason, Message: "back-off 5m0s restarting"}}
}

func terminated(exit int32, reason string) corev1.ContainerState {
	return corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: exit, Reason: reason}}
}

func running() corev1.ContainerState {
	return corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}
}

func cstatus(name string, restarts int32, state, last corev1.ContainerState) corev1.ContainerStatus {
	return corev1.ContainerStatus{
		Name:                 name,
		RestartCount:         restarts,
		State:                state,
		LastTerminationState: last,
	}
}

func ready(ready bool) corev1.PodStatus {
	v := corev1.ConditionFalse
	if ready {
		v = corev1.ConditionTrue
	}
	return corev1.PodStatus{
		Phase:      corev1.PodRunning,
		Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: v}},
	}
}

// signalExpect is the subset of a signal a test asserts on.
type signalExpect struct {
	failureType string
	container   string
}

func expectSignals(t *testing.T, got []domain.FailureSignal, want []signalExpect) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("got %d signals %+v, want %d %+v", len(got), got, len(want), want)
	}
	for i, w := range want {
		if got[i].Type != w.failureType {
			t.Errorf("signals[%d].Type = %q, want %q", i, got[i].Type, w.failureType)
		}
		if container := got[i].Attributes["container"]; container != w.container {
			t.Errorf("signals[%d].container = %q, want %q", i, container, w.container)
		}
		if got[i].Fingerprint == "" {
			t.Errorf("signals[%d].Fingerprint is empty", i)
		}
		if got[i].Resource.ClusterID != "test-cluster" {
			t.Errorf("signals[%d].Resource.ClusterID = %q", i, got[i].Resource.ClusterID)
		}
	}
}

func TestDetectorTable(t *testing.T) {
	tests := []struct {
		name string
		obs  func(t *testing.T) domain.Observation
		want []signalExpect
	}{
		{
			name: "crash loop back off",
			obs: func(t *testing.T) domain.Observation {
				return fixture(t, meta("app", "web-1", "pod-uid-1"), corev1.PodStatus{
					Phase:             corev1.PodRunning,
					ContainerStatuses: []corev1.ContainerStatus{cstatus("app", 5, waiting("CrashLoopBackOff"), terminated(1, "Error"))},
				})
			},
			want: []signalExpect{{TypeCrashLoopBackOff, "app"}},
		},
		{
			name: "oom killed in current state",
			obs: func(t *testing.T) domain.Observation {
				return fixture(t, meta("app", "worker-1", "pod-uid-2"), corev1.PodStatus{
					Phase:             corev1.PodRunning,
					ContainerStatuses: []corev1.ContainerStatus{cstatus("worker", 1, terminated(137, "OOMKilled"), corev1.ContainerState{})},
				})
			},
			want: []signalExpect{{TypeOOMKilled, "worker"}},
		},
		{
			name: "oom killed in last state while running",
			obs: func(t *testing.T) domain.Observation {
				return fixture(t, meta("app", "worker-1", "pod-uid-3"), corev1.PodStatus{
					Phase:             corev1.PodRunning,
					ContainerStatuses: []corev1.ContainerStatus{cstatus("worker", 1, running(), terminated(137, "OOMKilled"))},
				})
			},
			want: []signalExpect{{TypeOOMKilled, "worker"}},
		},
		{
			name: "crash loop back off takes precedence over oom last state",
			obs: func(t *testing.T) domain.Observation {
				return fixture(t, meta("app", "web-1", "pod-uid-4"), corev1.PodStatus{
					Phase:             corev1.PodRunning,
					ContainerStatuses: []corev1.ContainerStatus{cstatus("app", 7, waiting("CrashLoopBackOff"), terminated(137, "OOMKilled"))},
				})
			},
			want: []signalExpect{{TypeCrashLoopBackOff, "app"}},
		},
		{
			name: "non-zero exit",
			obs: func(t *testing.T) domain.Observation {
				return fixture(t, meta("app", "job-1", "pod-uid-5"), corev1.PodStatus{
					Phase:             corev1.PodFailed,
					ContainerStatuses: []corev1.ContainerStatus{cstatus("migrate", 0, terminated(1, "Error"), corev1.ContainerState{})},
				})
			},
			want: []signalExpect{{TypeNonZeroExit, "migrate"}},
		},
		{
			name: "non-zero exit in last state while running",
			obs: func(t *testing.T) domain.Observation {
				return fixture(t, meta("app", "web-1", "pod-uid-6"), corev1.PodStatus{
					Phase:             corev1.PodRunning,
					ContainerStatuses: []corev1.ContainerStatus{cstatus("app", 1, running(), terminated(2, "Error"))},
				})
			},
			want: []signalExpect{{TypeNonZeroExit, "app"}},
		},
		{
			name: "completed with exit zero produces no signal",
			obs: func(t *testing.T) domain.Observation {
				return fixture(t, meta("app", "job-1", "pod-uid-7"), corev1.PodStatus{
					Phase:             corev1.PodSucceeded,
					ContainerStatuses: []corev1.ContainerStatus{cstatus("migrate", 0, terminated(0, "Completed"), corev1.ContainerState{})},
				})
			},
			want: nil,
		},
		{
			name: "image pull back off",
			obs: func(t *testing.T) domain.Observation {
				return fixture(t, meta("app", "web-1", "pod-uid-8"), corev1.PodStatus{
					Phase:             corev1.PodPending,
					ContainerStatuses: []corev1.ContainerStatus{cstatus("app", 0, waiting("ImagePullBackOff"), corev1.ContainerState{})},
				})
			},
			want: []signalExpect{{TypeImagePullBackOff, "app"}},
		},
		{
			name: "err image pull",
			obs: func(t *testing.T) domain.Observation {
				return fixture(t, meta("app", "web-1", "pod-uid-9"), corev1.PodStatus{
					Phase:             corev1.PodPending,
					ContainerStatuses: []corev1.ContainerStatus{cstatus("app", 0, waiting("ErrImagePull"), corev1.ContainerState{})},
				})
			},
			want: []signalExpect{{TypeImagePullBackOff, "app"}},
		},
		{
			name: "create container config error",
			obs: func(t *testing.T) domain.Observation {
				return fixture(t, meta("app", "web-1", "pod-uid-10"), corev1.PodStatus{
					Phase:             corev1.PodPending,
					ContainerStatuses: []corev1.ContainerStatus{cstatus("app", 0, waiting("CreateContainerConfigError"), corev1.ContainerState{})},
				})
			},
			want: []signalExpect{{TypeCreateContainerConfigError, "app"}},
		},
		{
			name: "failed scheduling via pod scheduled condition",
			obs: func(t *testing.T) domain.Observation {
				return fixture(t, meta("app", "web-1", "pod-uid-11"), corev1.PodStatus{
					Phase: corev1.PodPending,
					Conditions: []corev1.PodCondition{
						{Type: corev1.PodScheduled, Status: corev1.ConditionFalse, Reason: "Unschedulable", Message: "0/3 nodes are available"},
					},
				})
			},
			want: []signalExpect{{TypeFailedScheduling, ""}},
		},
		{
			name: "pending with pod scheduled true produces no signal",
			obs: func(t *testing.T) domain.Observation {
				return fixture(t, meta("app", "web-1", "pod-uid-12"), corev1.PodStatus{
					Phase: corev1.PodPending,
					Conditions: []corev1.PodCondition{
						{Type: corev1.PodScheduled, Status: corev1.ConditionTrue},
					},
					ContainerStatuses: []corev1.ContainerStatus{cstatus("app", 0, waiting("ContainerCreating"), corev1.ContainerState{})},
				})
			},
			want: nil,
		},
		{
			name: "evicted pod",
			obs: func(t *testing.T) domain.Observation {
				return fixture(t, meta("app", "web-1", "pod-uid-13"), corev1.PodStatus{
					Phase:   corev1.PodFailed,
					Reason:  "Evicted",
					Message: "The node was low on resource: memory",
					ContainerStatuses: []corev1.ContainerStatus{
						cstatus("app", 0, terminated(137, "Evicted"), corev1.ContainerState{}),
					},
				})
			},
			want: []signalExpect{{TypeEvicted, ""}},
		},
		{
			name: "healthy running pod produces no signal",
			obs: func(t *testing.T) domain.Observation {
				return fixture(t, meta("app", "web-1", "pod-uid-14"), ready(true))
			},
			want: nil,
		},
		{
			name: "transient container creating produces no signal",
			obs: func(t *testing.T) domain.Observation {
				return fixture(t, meta("app", "web-1", "pod-uid-15"), corev1.PodStatus{
					Phase:             corev1.PodPending,
					ContainerStatuses: []corev1.ContainerStatus{cstatus("app", 0, waiting("ContainerCreating"), corev1.ContainerState{})},
				})
			},
			want: nil,
		},
		{
			name: "pod initializing produces no signal",
			obs: func(t *testing.T) domain.Observation {
				return fixture(t, meta("app", "web-1", "pod-uid-16"), corev1.PodStatus{
					Phase:             corev1.PodPending,
					ContainerStatuses: []corev1.ContainerStatus{cstatus("app", 0, waiting("PodInitializing"), corev1.ContainerState{})},
				})
			},
			want: nil,
		},
		{
			name: "one failing container among several",
			obs: func(t *testing.T) domain.Observation {
				return fixture(t, meta("app", "web-1", "pod-uid-17"), corev1.PodStatus{
					Phase: corev1.PodRunning,
					ContainerStatuses: []corev1.ContainerStatus{
						cstatus("sidecar", 0, running(), corev1.ContainerState{}),
						cstatus("app", 3, waiting("CrashLoopBackOff"), terminated(1, "Error")),
					},
				})
			},
			want: []signalExpect{{TypeCrashLoopBackOff, "app"}},
		},
		{
			name: "init container image pull failure",
			obs: func(t *testing.T) domain.Observation {
				return fixture(t, meta("app", "web-1", "pod-uid-18"), corev1.PodStatus{
					Phase:                 corev1.PodPending,
					InitContainerStatuses: []corev1.ContainerStatus{cstatus("init-migrate", 0, waiting("ImagePullBackOff"), corev1.ContainerState{})},
				})
			},
			want: []signalExpect{{TypeImagePullBackOff, "init-migrate"}},
		},
		{
			name: "missing status produces no signal",
			obs: func(t *testing.T) domain.Observation {
				return fixture(t, meta("app", "web-1", "pod-uid-19"), corev1.PodStatus{})
			},
			want: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := NewPodDetector("test-cluster")
			signals, err := d.Evaluate(context.Background(), tt.obs(t))
			if err != nil {
				t.Fatalf("Evaluate() error = %v", err)
			}
			expectSignals(t, signals, tt.want)
		})
	}
}

func TestDetectorInvalidPayload(t *testing.T) {
	d := NewPodDetector("test-cluster")
	obs := domain.Observation{
		Resource:   domain.ResourceRef{ClusterID: "c", Kind: "Pod", Namespace: "ns", Name: "p"},
		Payload:    []byte("{not json"),
		ObservedAt: fixedObsTime,
	}
	_, err := d.Evaluate(context.Background(), obs)
	if err == nil {
		t.Fatal("Evaluate() error = nil, want error")
	}
}

func TestFingerprint(t *testing.T) {
	owner := replicaSetOwner("rs-uid")
	pod := func(uid string) *domain.PodView {
		obs := fixture(t, meta("app", "web-1", uid, owner), ready(false))
		p, err := domain.ParsePodView(obs.Payload)
		if err != nil {
			t.Fatal(err)
		}
		return p
	}

	same := fingerprint("c1", pod("pod-uid-a"), "app", TypeCrashLoopBackOff)
	again := fingerprint("c1", pod("pod-uid-a"), "app", TypeCrashLoopBackOff)
	if same != again {
		t.Error("fingerprint not deterministic for identical inputs")
	}

	diffs := map[string]string{
		"different cluster":   fingerprint("c2", pod("pod-uid-a"), "app", TypeCrashLoopBackOff),
		"different pod uid":   fingerprint("c1", pod("pod-uid-b"), "app", TypeCrashLoopBackOff),
		"different container": fingerprint("c1", pod("pod-uid-a"), "sidecar", TypeCrashLoopBackOff),
		"different type":      fingerprint("c1", pod("pod-uid-a"), "app", TypeOOMKilled),
	}
	seen := map[string]string{same: "baseline"}
	for name, fp := range diffs {
		if fp == same {
			t.Errorf("%s: fingerprint identical to baseline", name)
		}
		if prev, ok := seen[fp]; ok {
			t.Errorf("%s: fingerprint collides with %s", name, prev)
		}
		seen[fp] = name
	}
}

func TestFingerprintPodNameNotUsedAlone(t *testing.T) {
	// Two pods with the same name in different namespaces must differ.
	owner := replicaSetOwner("rs-uid")
	build := func(ns, uid string) *domain.PodView {
		obs := fixture(t, meta(ns, "web-1", uid, owner), ready(false))
		p, err := domain.ParsePodView(obs.Payload)
		if err != nil {
			t.Fatal(err)
		}
		return p
	}
	a := fingerprint("c1", build("ns1", "uid-a"), "app", TypeCrashLoopBackOff)
	b := fingerprint("c1", build("ns2", "uid-b"), "app", TypeCrashLoopBackOff)
	if a == b {
		t.Error("same-name pods in different namespaces share a fingerprint")
	}
}
