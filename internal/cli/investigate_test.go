package cli

import (
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	clientgotesting "k8s.io/client-go/testing"

	"github.com/ThuatPhan/PodSleuth/internal/config"
	"github.com/ThuatPhan/PodSleuth/internal/kube"
	"github.com/ThuatPhan/PodSleuth/internal/storage"
	sqlitestore "github.com/ThuatPhan/PodSleuth/internal/storage/sqlite"
)

// investigateDeps wires the CLI to a fake clientset and a temp store
// opened through the injected NewStore constructor.
func investigateDeps(t *testing.T, objects ...runtime.Object) Deps {
	t.Helper()
	client := fake.NewSimpleClientset(objects...)
	path := filepath.Join(t.TempDir(), "podsleuth.db")
	if _, err := sqlitestore.Open(path); err != nil {
		t.Fatal(err)
	}
	return Deps{
		NewClientset: func(*config.Config) (kubernetes.Interface, kube.ContextInfo, error) {
			return client, kube.ContextInfo{Context: "kind-test", Cluster: "kind-test"}, nil
		},
		NewStore: func(string) (storage.Store, error) {
			return sqlitestore.Open(path)
		},
	}
}

func crasherPod() *corev1.Pod {
	controller := true
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:       "default",
			Name:            "crasher",
			UID:             types.UID("pod-uid-crasher"),
			ResourceVersion: "1000",
			OwnerReferences: []metav1.OwnerReference{{Kind: "ReplicaSet", Name: "crasher-rs", UID: "rs-uid-1", Controller: &controller}},
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			ContainerStatuses: []corev1.ContainerStatus{{
				Name:                 "app",
				RestartCount:         5,
				State:                corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "CrashLoopBackOff"}},
				LastTerminationState: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 1, Reason: "Error"}},
			}},
		},
	}
}

func healthyPod() *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "web-1", UID: types.UID("pod-uid-1"), ResourceVersion: "1000"},
		Status: corev1.PodStatus{
			Phase:             corev1.PodRunning,
			ContainerStatuses: []corev1.ContainerStatus{{Name: "app", State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}}},
		},
	}
}

func TestInvestigateCommandMarkdown(t *testing.T) {
	deps := investigateDeps(t, crasherPod())

	out, err := runCommand(t, deps, "investigate", "pod", "crasher", "-n", "default")
	if err != nil {
		t.Fatalf("investigate error = %v", err)
	}
	for _, want := range []string{
		"# PodSleuth Investigation Report",
		"- ID: ",
		"- Failure type: CrashLoopBackOff",
		"- Resource: Pod default/crasher (uid pod-uid-crasher)",
		"### 1. CRASH-001",
		"## Gaps and unknowns",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("report missing %q", want)
		}
	}
}

func TestInvestigateCommandJSON(t *testing.T) {
	deps := investigateDeps(t, crasherPod())

	out, err := runCommand(t, deps, "investigate", "pod", "crasher", "-n", "default", "--output", "json")
	if err != nil {
		t.Fatalf("investigate error = %v", err)
	}
	var doc struct {
		Incident struct {
			ID          string `json:"id"`
			FailureType string `json:"failure_type"`
		} `json:"incident"`
		Hypotheses []struct {
			RuleID string  `json:"rule_id"`
			Score  float64 `json:"score"`
		} `json:"hypotheses"`
	}
	if err := json.Unmarshal([]byte(out), &doc); err != nil {
		t.Fatalf("output is not valid JSON: %v\n%s", err, out)
	}
	if doc.Incident.FailureType != "CrashLoopBackOff" {
		t.Errorf("incident = %+v", doc.Incident)
	}
	if len(doc.Hypotheses) == 0 || doc.Hypotheses[0].RuleID != "CRASH-001" {
		t.Errorf("hypotheses = %+v", doc.Hypotheses)
	}
}

func TestInvestigateCommandPodNotFound(t *testing.T) {
	deps := investigateDeps(t)

	_, err := runCommand(t, deps, "investigate", "pod", "missing", "-n", "default")
	if err == nil {
		t.Fatal("investigate error = nil, want error")
	}
	if exitCodeOf(err) != 2 {
		t.Errorf("exit code = %d, want 2", exitCodeOf(err))
	}
	if !strings.Contains(err.Error(), "pod default/missing not found") {
		t.Errorf("error = %v, want useful not-found message", err)
	}
}

func TestInvestigateCommandHealthyPod(t *testing.T) {
	deps := investigateDeps(t, healthyPod())

	out, err := runCommand(t, deps, "investigate", "pod", "web-1", "-n", "default")
	if err != nil {
		t.Fatalf("investigate error = %v", err)
	}
	if !strings.Contains(out, "No failure detected on pod default/web-1") {
		t.Errorf("output = %q, want healthy message", out)
	}

	jsonOut, err := runCommand(t, deps, "investigate", "pod", "web-1", "-n", "default", "--output", "json")
	if err != nil {
		t.Fatal(err)
	}
	var doc struct {
		Status   string  `json:"status"`
		Incident *string `json:"incident"`
	}
	if err := json.Unmarshal([]byte(jsonOut), &doc); err != nil {
		t.Fatalf("healthy json not parseable: %v\n%s", err, jsonOut)
	}
	if doc.Status != "healthy" || doc.Incident != nil {
		t.Errorf("healthy document = %+v", doc)
	}
}

func TestInvestigateCommandArgsValidation(t *testing.T) {
	deps := investigateDeps(t)

	if _, err := runCommand(t, deps, "investigate", "pod", "crasher"); err == nil || exitCodeOf(err) != 2 {
		t.Errorf("missing -n: err = %v, want exit 2", err)
	}
	if _, err := runCommand(t, deps, "investigate", "deployment", "crasher", "-n", "default"); err == nil || exitCodeOf(err) != 2 {
		t.Errorf("wrong resource type: err = %v, want exit 2", err)
	}
	if _, err := runCommand(t, deps, "investigate", "crasher", "-n", "default"); err == nil || exitCodeOf(err) != 2 {
		t.Errorf("missing pod keyword: err = %v, want exit 2", err)
	}
}

func TestInvestigateCommandPartialCollection(t *testing.T) {
	client := fake.NewSimpleClientset(crasherPod())
	client.PrependReactor("list", "events", func(action clientgotesting.Action) (bool, runtime.Object, error) {
		return true, nil, apierrors.NewForbidden(schema.GroupResource{Resource: "events"}, "", errors.New("rbac"))
	})
	path := filepath.Join(t.TempDir(), "podsleuth.db")
	store, err := sqlitestore.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	deps := Deps{
		NewClientset: func(*config.Config) (kubernetes.Interface, kube.ContextInfo, error) {
			return client, kube.ContextInfo{Context: "kind-test", Cluster: "kind-test"}, nil
		},
		NewStore: func(string) (storage.Store, error) {
			return sqlitestore.Open(path)
		},
	}

	out, err := runCommand(t, deps, "investigate", "pod", "crasher", "-n", "default")
	if err == nil {
		t.Fatal("investigate error = nil, want partial-collection error (exit 3)")
	}
	var partial *partialReportError
	if !errors.As(err, &partial) {
		t.Errorf("error = %v (%T), want *partialReportError", err, err)
	}
	if exitCodeOf(err) != 3 {
		t.Errorf("exit code = %d, want 3", exitCodeOf(err))
	}
	if !strings.Contains(out, "# PodSleuth Investigation Report") {
		t.Errorf("output = %q, want rendered report despite partial collection", out)
	}
}
