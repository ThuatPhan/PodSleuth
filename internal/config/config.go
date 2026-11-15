// Package config loads and validates PodSleuth runtime configuration.
package config

import (
	"fmt"
	"os"
	"path/filepath"
)

// DefaultClusterID is used when no explicit cluster ID is configured.
const DefaultClusterID = "local-dev"

// Config holds PodSleuth runtime configuration.
type Config struct {
	Cluster ClusterConfig
}

// ClusterConfig holds Kubernetes connectivity settings.
type ClusterConfig struct {
	// Kubeconfig is the path to the kubeconfig file. Empty means the
	// standard client-go loading rules apply.
	Kubeconfig string

	// Context overrides the current context from the kubeconfig. Empty
	// means the kubeconfig's current context is used.
	Context string

	// ClusterID identifies the cluster in persisted data.
	ClusterID string
}

// Options controls how configuration is loaded.
type Options struct {
	// Kubeconfig is an explicit kubeconfig path, typically from the
	// --kubeconfig flag.
	Kubeconfig string

	// Context overrides the kubeconfig's current context, typically from
	// the --context flag.
	Context string
}

// Load resolves and validates the PodSleuth configuration.
func Load(opts Options) (*Config, error) {
	path, err := resolveKubeconfigPath(opts.Kubeconfig)
	if err != nil {
		return nil, err
	}

	cfg := &Config{
		Cluster: ClusterConfig{
			Kubeconfig: path,
			Context:    opts.Context,
			ClusterID:  DefaultClusterID,
		},
	}
	return cfg, nil
}

// DefaultStoragePath returns the default SQLite database location
// (~/.local/share/podsleuth/podsleuth.db).
func DefaultStoragePath() (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("cannot determine home directory for the default storage path: %w", err)
	}
	return filepath.Join(home, ".local", "share", "podsleuth", "podsleuth.db"), nil
}

// resolveKubeconfigPath applies the kubeconfig path precedence:
// explicit flag, KUBECONFIG env (first entry), then ~/.kube/config.
func resolveKubeconfigPath(explicit string) (string, error) {
	if explicit != "" {
		if _, err := os.Stat(explicit); err != nil {
			return "", fmt.Errorf("kubeconfig file not found at %q: %w; pass a valid path via --kubeconfig or KUBECONFIG", explicit, err)
		}
		return explicit, nil
	}

	if env := os.Getenv("KUBECONFIG"); env != "" {
		// KUBECONFIG may list multiple paths; client-go merges them, so
		// the existence check is left to the loader.
		return filepath.SplitList(env)[0], nil
	}

	home, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("cannot determine home directory to locate the default kubeconfig: %w; pass a path via --kubeconfig or KUBECONFIG", err)
	}
	path := filepath.Join(home, ".kube", "config")
	if _, err := os.Stat(path); err != nil {
		return "", fmt.Errorf("no kubeconfig found at %q: %w; pass a path via --kubeconfig or KUBECONFIG, or create one with 'kind export kubeconfig' or 'kubectl config view'", path, err)
	}
	return path, nil
}
