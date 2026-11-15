package cli

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/ThuatPhan/PodSleuth/internal/config"
	"github.com/ThuatPhan/PodSleuth/internal/kube"
)

// check is one doctor diagnostic result.
type check struct {
	name   string
	detail string
}

func newDoctorCommand(deps Deps, loadConfig func() (*config.Config, error)) *cobra.Command {
	return &cobra.Command{
		Use:   "doctor",
		Short: "Verify kubeconfig, context and Kubernetes API connectivity",
		Args:  noArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runDoctor(cmd, deps, loadConfig)
		},
	}
}

func runDoctor(cmd *cobra.Command, deps Deps, loadConfig func() (*config.Config, error)) error {
	ctx := cmd.Context()
	out := cmd.OutOrStdout()

	cfg, err := loadConfig()
	if err != nil {
		return &invalidInputError{err: err}
	}
	factory := kube.NewFactory(cfg)

	var checks []check

	if _, err := factory.ClientConfig(); err != nil {
		return reportFailure(checks, "kubeconfig", err)
	}
	checks = append(checks, check{name: "kubeconfig loaded", detail: cfg.Cluster.Kubeconfig})

	info, err := factory.CurrentContext()
	if err != nil {
		return reportFailure(checks, "current context", err)
	}
	checks = append(checks, check{name: "current context resolved", detail: fmt.Sprintf("%s (cluster %s, server %s)", info.Context, info.Cluster, info.Server)})

	prober := deps.Prober
	if prober == nil {
		prober = factory
	}
	v, err := prober.ServerVersion(ctx)
	if err != nil {
		return reportFailure(checks, "API server", err)
	}
	checks = append(checks,
		check{name: "API server reachable", detail: info.Server},
		check{name: "server version", detail: v.GitVersion},
	)

	for _, c := range checks {
		fmt.Fprintf(out, "  \u2713 %s", c.name)
		if c.detail != "" {
			fmt.Fprintf(out, ": %s", c.detail)
		}
		fmt.Fprintln(out)
	}
	fmt.Fprintln(out, "All checks passed.")
	return nil
}

// reportFailure prints the checks that passed and returns the original
// error (wrapped with the failing check name) so its exit code is
// preserved. The caller prints the error once.
func reportFailure(passed []check, name string, err error) error {
	return fmt.Errorf("doctor failed at check %q: %w", name, err)
}
