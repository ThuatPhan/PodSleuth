// Package kube builds Kubernetes clients from PodSleuth configuration.
// All Kubernetes access is read-only.
package kube

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/version"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/clientcmd/api"

	"github.com/ThuatPhan/PodSleuth/internal/config"
)

// DefaultTimeout bounds Kubernetes API requests.
const DefaultTimeout = 10 * time.Second

// ConfigError reports that the kubeconfig could not be loaded or the
// current context could not be resolved. It maps to exit code 2
// (invalid input/config).
type ConfigError struct {
	msg string
	err error
}

func (e *ConfigError) Error() string { return e.msg }

func (e *ConfigError) Unwrap() error { return e.err }

// ExitCode implements cli.ExitCoder.
func (e *ConfigError) ExitCode() int { return 2 }

// ConnectivityError reports that the Kubernetes API server could not be
// reached or authenticated against. It maps to exit code 4 (Kubernetes
// auth/connectivity prevents investigation).
type ConnectivityError struct {
	Server string
	msg    string
	err    error
}

func (e *ConnectivityError) Error() string { return e.msg }

func (e *ConnectivityError) Unwrap() error { return e.err }

// ExitCode implements cli.ExitCoder.
func (e *ConnectivityError) ExitCode() int { return 4 }

// ContextInfo describes the resolved Kubernetes context. It contains no
// credentials and is safe to print.
type ContextInfo struct {
	Context string
	Cluster string
	Server  string
}

// Factory builds Kubernetes clients from a resolved PodSleuth
// configuration. It is safe for concurrent use.
type Factory struct {
	cfg *config.Config
}

// NewFactory returns a Factory for the given configuration.
func NewFactory(cfg *config.Config) *Factory {
	return &Factory{cfg: cfg}
}

func (f *Factory) loadingRules() *clientcmd.ClientConfigLoadingRules {
	rules := clientcmd.NewDefaultClientConfigLoadingRules()
	if f.cfg.Cluster.Kubeconfig != "" {
		rules.ExplicitPath = f.cfg.Cluster.Kubeconfig
	}
	return rules
}

func (f *Factory) overrides() *clientcmd.ConfigOverrides {
	return &clientcmd.ConfigOverrides{CurrentContext: f.cfg.Cluster.Context}
}

// ClientConfig returns the deferred client-go client configuration.
func (f *Factory) ClientConfig() (clientcmd.ClientConfig, error) {
	cc := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
		f.loadingRules(),
		f.overrides(),
	)
	if err := validateLoadedConfig(cc); err != nil {
		return nil, err
	}
	return cc, nil
}

// validateLoadedConfig forces the kubeconfig to be read and parsed so
// malformed files are reported here instead of at first use.
func validateLoadedConfig(cc clientcmd.ClientConfig) error {
	raw, err := cc.RawConfig()
	if err != nil {
		return &ConfigError{
			msg: fmt.Sprintf("cannot load kubeconfig: %v; check the file is valid YAML and follows the kubeconfig schema (see 'kubectl config view --raw')", err),
			err: err,
		}
	}
	if len(raw.Contexts) == 0 {
		return &ConfigError{
			msg: "kubeconfig contains no contexts; add one with 'kubectl config set-context' or point --kubeconfig at a valid kubeconfig file",
		}
	}
	return nil
}

// RESTConfig returns the REST configuration for direct API access.
func (f *Factory) RESTConfig() (*rest.Config, error) {
	cc, err := f.ClientConfig()
	if err != nil {
		return nil, err
	}
	restCfg, err := cc.ClientConfig()
	if err != nil {
		return nil, &ConfigError{
			msg: fmt.Sprintf("cannot build Kubernetes client configuration from kubeconfig: %v; verify the current context references an existing cluster and user", err),
			err: err,
		}
	}
	restCfg.Timeout = DefaultTimeout
	return restCfg, nil
}

// Clientset returns a Kubernetes clientset.
func (f *Factory) Clientset() (kubernetes.Interface, error) {
	restCfg, err := f.RESTConfig()
	if err != nil {
		return nil, err
	}
	cs, err := kubernetes.NewForConfig(restCfg)
	if err != nil {
		return nil, &ConfigError{
			msg: fmt.Sprintf("cannot create Kubernetes client: %v", err),
			err: err,
		}
	}
	return cs, nil
}

// CurrentContext resolves the active context, honoring the --context
// override. The result contains names and the server URL only.
func (f *Factory) CurrentContext() (ContextInfo, error) {
	cc, err := f.ClientConfig()
	if err != nil {
		return ContextInfo{}, err
	}
	raw, err := cc.RawConfig()
	if err != nil {
		return ContextInfo{}, &ConfigError{
			msg: fmt.Sprintf("cannot load kubeconfig: %v", err),
			err: err,
		}
	}

	name := raw.CurrentContext
	if f.cfg.Cluster.Context != "" {
		name = f.cfg.Cluster.Context
	}
	ctx, ok := raw.Contexts[name]
	if !ok {
		return ContextInfo{}, &ConfigError{
			msg: fmt.Sprintf("context %q not found in kubeconfig; available contexts: %s; pass an existing one via --context or run 'kubectl config use-context'", name, joinNames(raw.Contexts)),
		}
	}
	cluster, ok := raw.Clusters[ctx.Cluster]
	if !ok || cluster == nil {
		return ContextInfo{}, &ConfigError{
			msg: fmt.Sprintf("context %q references cluster %q which is not defined in the kubeconfig; fix the context with 'kubectl config set-context %s'", name, ctx.Cluster, name),
		}
	}
	return ContextInfo{
		Context: name,
		Cluster: ctx.Cluster,
		Server:  cluster.Server,
	}, nil
}

// ServerVersion contacts the API server and returns its version. It
// doubles as the reachability check for the doctor command.
func (f *Factory) ServerVersion(ctx context.Context) (*version.Info, error) {
	info, err := f.CurrentContext()
	if err != nil {
		return nil, err
	}

	restCfg, err := f.RESTConfig()
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(ctx, DefaultTimeout)
	defer cancel()

	v, err := fetchServerVersion(ctx, restCfg)
	if err != nil {
		return nil, connectivityError(info.Server, err)
	}
	return v, nil
}

// fetchServerVersion requests /version through the raw REST client so
// the caller's context governs cancellation. Discovery.ServerVersion
// ignores the caller context.
func fetchServerVersion(ctx context.Context, restCfg *rest.Config) (*version.Info, error) {
	cs, err := kubernetes.NewForConfig(restCfg)
	if err != nil {
		return nil, &ConfigError{
			msg: fmt.Sprintf("cannot create Kubernetes client: %v", err),
			err: err,
		}
	}
	data, err := cs.Discovery().RESTClient().Get().AbsPath("/version").Do(ctx).Raw()
	if err != nil {
		return nil, err
	}
	var v version.Info
	if err := json.Unmarshal(data, &v); err != nil {
		return nil, fmt.Errorf("cannot parse API server version response: %w", err)
	}
	return &v, nil
}

// connectivityError classifies an API request failure. Caller-initiated
// cancellation is passed through; everything else is treated as an
// auth/connectivity problem.
func connectivityError(server string, err error) error {
	if errors.Is(err, context.Canceled) {
		return err
	}
	return &ConnectivityError{
		Server: server,
		msg: fmt.Sprintf("cannot reach Kubernetes API server %s: %v; verify the cluster is running (e.g. 'kind get clusters', 'kubectl cluster-info') and the server address in the kubeconfig is correct",
			server, err),
		err: err,
	}
}

func joinNames(contexts map[string]*api.Context) string {
	names := make([]string, 0, len(contexts))
	for name := range contexts {
		names = append(names, name)
	}
	slices.Sort(names)
	return strings.Join(names, ", ")
}
