package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLoadExplicitPath(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config")
	if err := os.WriteFile(path, []byte("apiVersion: v1\nkind: Config\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	cfg, err := Load(Options{Kubeconfig: path, Context: "staging"})
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.Cluster.Kubeconfig != path {
		t.Errorf("Kubeconfig = %q, want %q", cfg.Cluster.Kubeconfig, path)
	}
	if cfg.Cluster.Context != "staging" {
		t.Errorf("Context = %q, want %q", cfg.Cluster.Context, "staging")
	}
	if cfg.Cluster.ClusterID != DefaultClusterID {
		t.Errorf("ClusterID = %q, want %q", cfg.Cluster.ClusterID, DefaultClusterID)
	}
}

func TestLoadExplicitPathMissing(t *testing.T) {
	_, err := Load(Options{Kubeconfig: filepath.Join(t.TempDir(), "missing")})
	if err == nil {
		t.Fatal("Load() error = nil, want error")
	}
	if !strings.Contains(err.Error(), "kubeconfig file not found") {
		t.Errorf("error = %v, want actionable not-found message", err)
	}
	if !strings.Contains(err.Error(), "--kubeconfig") {
		t.Errorf("error = %v, want hint mentioning --kubeconfig", err)
	}
}

func TestLoadKubeconfigEnv(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "from-env")
	if err := os.WriteFile(path, []byte("apiVersion: v1\nkind: Config\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("KUBECONFIG", path)

	cfg, err := Load(Options{})
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.Cluster.Kubeconfig != path {
		t.Errorf("Kubeconfig = %q, want %q", cfg.Cluster.Kubeconfig, path)
	}
}

func TestLoadExplicitPathOverridesEnv(t *testing.T) {
	dir := t.TempDir()
	explicit := filepath.Join(dir, "explicit")
	if err := os.WriteFile(explicit, []byte("apiVersion: v1\nkind: Config\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("KUBECONFIG", filepath.Join(dir, "from-env"))

	cfg, err := Load(Options{Kubeconfig: explicit})
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.Cluster.Kubeconfig != explicit {
		t.Errorf("Kubeconfig = %q, want %q", cfg.Cluster.Kubeconfig, explicit)
	}
}

func TestLoadDefaultPath(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("KUBECONFIG", "")

	kubeDir := filepath.Join(home, ".kube")
	if err := os.MkdirAll(kubeDir, 0o700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(kubeDir, "config")
	if err := os.WriteFile(path, []byte("apiVersion: v1\nkind: Config\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	cfg, err := Load(Options{})
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.Cluster.Kubeconfig != path {
		t.Errorf("Kubeconfig = %q, want %q", cfg.Cluster.Kubeconfig, path)
	}
}

func TestLoadDefaultPathMissing(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	t.Setenv("KUBECONFIG", "")

	_, err := Load(Options{})
	if err == nil {
		t.Fatal("Load() error = nil, want error")
	}
	if !strings.Contains(err.Error(), "no kubeconfig found") {
		t.Errorf("error = %v, want actionable message", err)
	}
	if !strings.Contains(err.Error(), "kind export kubeconfig") {
		t.Errorf("error = %v, want hint mentioning kind export kubeconfig", err)
	}
}
