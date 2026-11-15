package cli

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/version"

	"github.com/ThuatPhan/PodSleuth/internal/config"
	"github.com/ThuatPhan/PodSleuth/internal/kube"
)

// writeKubeconfig writes a minimal valid kubeconfig fixture.
func writeKubeconfig(t *testing.T) string {
	t.Helper()
	path := t.TempDir() + "/kubeconfig"
	content := `apiVersion: v1
kind: Config
clusters:
- name: test
  cluster:
    server: https://127.0.0.1:6443
contexts:
- name: test
  context:
    cluster: test
    user: test-user
current-context: test
users:
- name: test-user
  user: {}
`
	if err := writeFile(path, content); err != nil {
		t.Fatal(err)
	}
	return path
}

// fakeProber is a controllable Prober for doctor tests.
type fakeProber struct {
	info   *version.Info
	err    error
	called bool
	gotCtx context.Context
}

func (f *fakeProber) ServerVersion(ctx context.Context) (*version.Info, error) {
	f.called = true
	f.gotCtx = ctx
	return f.info, f.err
}

func runCommand(t *testing.T, deps Deps, args ...string) (string, error) {
	t.Helper()
	var out, errOut bytes.Buffer
	root := NewRootCommand(deps)
	root.SetOut(&out)
	root.SetErr(&errOut)
	root.SetArgs(args)
	err := root.ExecuteContext(context.Background())
	return out.String(), err
}

func TestVersionCommand(t *testing.T) {
	out, err := runCommand(t, Deps{Version: "v0.1.0", Commit: "abc123", BuildDate: "2026-01-01"}, "version")
	if err != nil {
		t.Fatalf("version error = %v", err)
	}
	if !strings.Contains(out, "podsleuth v0.1.0") || !strings.Contains(out, "abc123") {
		t.Errorf("output = %q, want version and commit", out)
	}
}

func TestVersionCommandRejectsArgs(t *testing.T) {
	_, err := runCommand(t, Deps{}, "version", "extra")
	if err == nil {
		t.Fatal("version with extra args: error = nil, want error")
	}
	var inv *invalidInputError
	if !errors.As(err, &inv) {
		t.Errorf("error = %T, want *invalidInputError (exit 2)", err)
	}
}

func TestDoctorSuccess(t *testing.T) {
	path := writeKubeconfig(t)
	prober := &fakeProber{info: &version.Info{GitVersion: "v1.37.0"}}

	out, err := runCommand(t, Deps{Prober: prober}, "doctor", "--kubeconfig", path)
	if err != nil {
		t.Fatalf("doctor error = %v", err)
	}
	for _, want := range []string{
		"kubeconfig loaded",
		"current context resolved",
		"API server reachable",
		"server version",
		"v1.37.0",
		"All checks passed.",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("output %q missing %q", out, want)
		}
	}
	if !prober.called {
		t.Error("prober was not called")
	}
}

func TestDoctorContextOverride(t *testing.T) {
	path := t.TempDir() + "/kubeconfig"
	content := `apiVersion: v1
kind: Config
clusters:
- name: dev
  cluster:
    server: https://127.0.0.1:6443
- name: staging
  cluster:
    server: https://staging.example.test:6443
contexts:
- name: dev
  context:
    cluster: dev
    user: dev-user
- name: staging
  context:
    cluster: staging
    user: staging-user
current-context: dev
users:
- name: dev-user
  user: {}
- name: staging-user
  user: {}
`
	if err := writeFile(path, content); err != nil {
		t.Fatal(err)
	}
	prober := &fakeProber{info: &version.Info{GitVersion: "v1.37.0"}}

	out, err := runCommand(t, Deps{Prober: prober}, "doctor", "--kubeconfig", path, "--context", "staging")
	if err != nil {
		t.Fatalf("doctor error = %v", err)
	}
	if !strings.Contains(out, "staging (cluster staging, server https://staging.example.test:6443)") {
		t.Errorf("output = %q, want staging context details", out)
	}
}

// exit4Error is a fake connectivity failure carrying exit code 4.
type exit4Error struct{}

func (exit4Error) Error() string { return "connection refused" }

// ExitCode implements ExitCoder.
func (exit4Error) ExitCode() int { return 4 }

func TestDoctorProberFailure(t *testing.T) {
	path := writeKubeconfig(t)
	prober := &fakeProber{err: exit4Error{}}

	out, err := runCommand(t, Deps{Prober: prober}, "doctor", "--kubeconfig", path)
	if err == nil {
		t.Fatal("doctor error = nil, want error")
	}
	if exitCodeOf(err) != 4 {
		t.Errorf("exit code = %d, want 4", exitCodeOf(err))
	}
	if !strings.Contains(err.Error(), `doctor failed at check "API server"`) {
		t.Errorf("error = %v, want failing check named in error", err)
	}
	if strings.Contains(out, "All checks passed.") {
		t.Errorf("output = %q, want no success message on failure", out)
	}
}

func TestDoctorBadKubeconfigPath(t *testing.T) {
	out, err := runCommand(t, Deps{}, "doctor", "--kubeconfig", "/nonexistent/kubeconfig")
	if err == nil {
		t.Fatal("doctor error = nil, want error")
	}
	if exitCodeOf(err) != 2 {
		t.Errorf("exit code = %d, want 2", exitCodeOf(err))
	}
	if !strings.Contains(err.Error(), "kubeconfig file not found") {
		t.Errorf("error = %v, want actionable not-found message", err)
	}
	if out != "" {
		t.Errorf("output = %q, want empty (failure before any check)", out)
	}
}

func TestDoctorInvalidKubeconfig(t *testing.T) {
	path := t.TempDir() + "/kubeconfig"
	if err := writeFile(path, "this is: [not: valid: kubeconfig"); err != nil {
		t.Fatal(err)
	}

	_, err := runCommand(t, Deps{}, "doctor", "--kubeconfig", path)
	if err == nil {
		t.Fatal("doctor error = nil, want error")
	}
	if exitCodeOf(err) != 2 {
		t.Errorf("exit code = %d, want 2", exitCodeOf(err))
	}
	var cfgErr *kube.ConfigError
	if !errors.As(err, &cfgErr) {
		t.Errorf("error = %T, want *kube.ConfigError", err)
	}
}

func TestDoctorUnknownContext(t *testing.T) {
	path := writeKubeconfig(t)

	_, err := runCommand(t, Deps{}, "doctor", "--kubeconfig", path, "--context", "production")
	if err == nil {
		t.Fatal("doctor error = nil, want error")
	}
	if exitCodeOf(err) != 2 {
		t.Errorf("exit code = %d, want 2", exitCodeOf(err))
	}
	if !strings.Contains(err.Error(), `context "production" not found`) {
		t.Errorf("error = %v, want unknown-context message", err)
	}
}

func TestLoadConfigErrorMapsToExit2(t *testing.T) {
	_, err := config.Load(config.Options{Kubeconfig: "/nonexistent"})
	wrapped := &invalidInputError{err: err}
	if exitCodeOf(wrapped) != 2 {
		t.Errorf("exit code = %d, want 2", exitCodeOf(wrapped))
	}
}
