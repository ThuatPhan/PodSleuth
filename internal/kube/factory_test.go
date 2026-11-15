package kube

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/ThuatPhan/PodSleuth/internal/config"
)

// newTestConfig builds a config pointing at a fixture kubeconfig.
func newTestConfig(path, kubeCtx string) (*config.Config, error) {
	return config.Load(config.Options{Kubeconfig: path, Context: kubeCtx})
}

// writeKubeconfig writes a kubeconfig fixture and returns its path.
func writeKubeconfig(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "kubeconfig")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

// writeKubeconfigWithServer writes a kubeconfig pointing at the given
// server URL.
func writeKubeconfigWithServer(t *testing.T, server string) string {
	t.Helper()
	return writeKubeconfig(t, `apiVersion: v1
kind: Config
clusters:
- name: test
  cluster:
    server: `+server+`
contexts:
- name: test
  context:
    cluster: test
    user: test-user
current-context: test
users:
- name: test-user
  user: {}
`)
}

// newBlockingServer returns a test server whose /version handler blocks
// until the request context is cancelled.
func newBlockingServer(t *testing.T) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	}))
	t.Cleanup(srv.Close)
	return srv
}

const twoContextKubeconfig = `apiVersion: v1
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

func TestCurrentContext(t *testing.T) {
	path := writeKubeconfig(t, twoContextKubeconfig)
	cfg, err := newTestConfig(path, "")
	if err != nil {
		t.Fatal(err)
	}

	info, err := NewFactory(cfg).CurrentContext()
	if err != nil {
		t.Fatalf("CurrentContext() error = %v", err)
	}
	want := ContextInfo{Context: "dev", Cluster: "dev", Server: "https://127.0.0.1:6443"}
	if info != want {
		t.Errorf("CurrentContext() = %+v, want %+v", info, want)
	}
}

func TestCurrentContextOverride(t *testing.T) {
	path := writeKubeconfig(t, twoContextKubeconfig)
	cfg, err := newTestConfig(path, "staging")
	if err != nil {
		t.Fatal(err)
	}

	info, err := NewFactory(cfg).CurrentContext()
	if err != nil {
		t.Fatalf("CurrentContext() error = %v", err)
	}
	want := ContextInfo{Context: "staging", Cluster: "staging", Server: "https://staging.example.test:6443"}
	if info != want {
		t.Errorf("CurrentContext() = %+v, want %+v", info, want)
	}
}

func TestCurrentContextUnknown(t *testing.T) {
	path := writeKubeconfig(t, twoContextKubeconfig)
	cfg, err := newTestConfig(path, "production")
	if err != nil {
		t.Fatal(err)
	}

	_, err = NewFactory(cfg).CurrentContext()
	if err == nil {
		t.Fatal("CurrentContext() error = nil, want error")
	}
	var cfgErr *ConfigError
	if !errors.As(err, &cfgErr) {
		t.Fatalf("error = %T, want *ConfigError", err)
	}
	if !strings.Contains(err.Error(), `context "production" not found`) {
		t.Errorf("error = %v, want unknown-context message", err)
	}
	if !strings.Contains(err.Error(), "dev, staging") {
		t.Errorf("error = %v, want available contexts listed", err)
	}
}

func TestCurrentContextNoContexts(t *testing.T) {
	path := writeKubeconfig(t, "apiVersion: v1\nkind: Config\n")
	cfg, err := newTestConfig(path, "")
	if err != nil {
		t.Fatal(err)
	}

	_, err = NewFactory(cfg).CurrentContext()
	if err == nil {
		t.Fatal("CurrentContext() error = nil, want error")
	}
	if !strings.Contains(err.Error(), "no contexts") {
		t.Errorf("error = %v, want no-contexts message", err)
	}
}

func TestClientConfigInvalidYAML(t *testing.T) {
	path := writeKubeconfig(t, "this is: [not: valid: kubeconfig")
	cfg, err := newTestConfig(path, "")
	if err != nil {
		t.Fatal(err)
	}

	_, err = NewFactory(cfg).ClientConfig()
	if err == nil {
		t.Fatal("ClientConfig() error = nil, want error")
	}
	var cfgErr *ConfigError
	if !errors.As(err, &cfgErr) {
		t.Fatalf("error = %T, want *ConfigError", err)
	}
	if !strings.Contains(err.Error(), "cannot load kubeconfig") {
		t.Errorf("error = %v, want load-failure message", err)
	}
}

func TestClientset(t *testing.T) {
	path := writeKubeconfig(t, twoContextKubeconfig)
	cfg, err := newTestConfig(path, "")
	if err != nil {
		t.Fatal(err)
	}

	cs, err := NewFactory(cfg).Clientset()
	if err != nil {
		t.Fatalf("Clientset() error = %v", err)
	}
	if cs == nil {
		t.Fatal("Clientset() = nil, want clientset")
	}
}

func TestServerVersionUnreachable(t *testing.T) {
	// Port 1 on loopback is closed, so the connection is refused
	// quickly without needing a real cluster.
	path := writeKubeconfig(t, twoContextKubeconfig)
	cfg, err := newTestConfig(path, "")
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err = NewFactory(cfg).ServerVersion(ctx)
	if err == nil {
		t.Fatal("ServerVersion() error = nil, want error")
	}
	var connErr *ConnectivityError
	if !errors.As(err, &connErr) {
		t.Fatalf("error = %T, want *ConnectivityError", err)
	}
	if connErr.Server != "https://127.0.0.1:6443" {
		t.Errorf("Server = %q, want https://127.0.0.1:6443", connErr.Server)
	}
	if !strings.Contains(err.Error(), "cannot reach Kubernetes API server") {
		t.Errorf("error = %v, want actionable connectivity message", err)
	}
	if !strings.Contains(err.Error(), "kind get clusters") {
		t.Errorf("error = %v, want hint mentioning kind get clusters", err)
	}
}

func TestServerVersionCallerCancel(t *testing.T) {
	// The blocking server ensures the request is in flight when the
	// caller cancels, so cancellation passes through unclassified.
	srv := newBlockingServer(t)
	path := writeKubeconfigWithServer(t, srv.URL)
	cfg, err := newTestConfig(path, "")
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err = NewFactory(cfg).ServerVersion(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("ServerVersion() error = %v, want context.Canceled", err)
	}
}

func TestExitCodes(t *testing.T) {
	cfgErr := &ConfigError{}
	if cfgErr.ExitCode() != 2 {
		t.Errorf("ConfigError exit code = %d, want 2", cfgErr.ExitCode())
	}
	connErr := &ConnectivityError{}
	if connErr.ExitCode() != 4 {
		t.Errorf("ConnectivityError exit code = %d, want 4", connErr.ExitCode())
	}
}

// TestFakeClientsetSatisfiesClientsetInterface pins the contract that
// downstream consumers can be tested with the Kubernetes fake client in
// place of a Factory-built clientset.
func TestFakeClientsetSatisfiesClientsetInterface(t *testing.T) {
	var _ kubernetes.Interface = fake.NewSimpleClientset()
}
