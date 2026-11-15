// Package cli implements the PodSleuth command-line interface.
package cli

import (
	"context"
	"errors"
	"fmt"

	"github.com/spf13/cobra"
	"k8s.io/apimachinery/pkg/version"

	"github.com/ThuatPhan/PodSleuth/internal/config"
	"github.com/ThuatPhan/PodSleuth/internal/storage"
	sqlitestore "github.com/ThuatPhan/PodSleuth/internal/storage/sqlite"
)

// ExitCoder lets errors control the process exit code.
type ExitCoder interface {
	ExitCode() int
}

// invalidInputError marks user errors (bad flags or arguments) that map
// to exit code 2.
type invalidInputError struct{ err error }

func (e *invalidInputError) Error() string { return e.err.Error() }

func (e *invalidInputError) Unwrap() error { return e.err }

// ExitCode implements ExitCoder.
func (e *invalidInputError) ExitCode() int { return 2 }

// Deps holds the dependencies injected into the CLI. The zero value is
// valid and wires the real implementations.
type Deps struct {
	// Version, Commit and BuildDate are stamped at build time.
	Version   string
	Commit    string
	BuildDate string

	// Prober overrides the connectivity prober used by the doctor
	// command. When nil, the kube factory is used.
	Prober Prober

	// NewStore overrides the storage constructor used by the report
	// command. When nil, the SQLite implementation is used.
	NewStore func(path string) (storage.Store, error)
}

// Prober verifies Kubernetes API connectivity.
type Prober interface {
	ServerVersion(ctx context.Context) (*version.Info, error)
}

// NewRootCommand builds the PodSleuth root command.
func NewRootCommand(deps Deps) *cobra.Command {
	var (
		kubeconfig string
		kubeCtx    string
	)

	root := &cobra.Command{
		Use:           "podsleuth",
		Short:         "Read-only Kubernetes Pod failure investigator",
		SilenceErrors: true,
		SilenceUsage:  true,
	}
	root.SetFlagErrorFunc(func(c *cobra.Command, err error) error {
		return &invalidInputError{err: err}
	})
	root.PersistentFlags().StringVar(&kubeconfig, "kubeconfig", "", "path to the kubeconfig file (default: KUBECONFIG env, then ~/.kube/config)")
	root.PersistentFlags().StringVar(&kubeCtx, "context", "", "Kubernetes context to use (default: current context in the kubeconfig)")

	root.AddCommand(newVersionCommand(deps))
	root.AddCommand(newDoctorCommand(deps, func() (*config.Config, error) {
		return config.Load(config.Options{Kubeconfig: kubeconfig, Context: kubeCtx})
	}))
	root.AddCommand(newReportCommand(deps))

	return root
}

// newStore builds the storage implementation, honoring the injected
// override.
func (d Deps) newStore(path string) (storage.Store, error) {
	if d.NewStore != nil {
		return d.NewStore(path)
	}
	return sqlitestore.Open(path)
}

// Execute runs the PodSleuth CLI with the given arguments.
func Execute(ctx context.Context, deps Deps, args []string) error {
	return NewRootCommand(deps).ExecuteContext(ctx)
}

// exitCodeOf maps an error to a process exit code: 0 success, 1
// unexpected error, 2 invalid input/config, 4 Kubernetes
// auth/connectivity failure.
func exitCodeOf(err error) int {
	var ec ExitCoder
	if errors.As(err, &ec) {
		return ec.ExitCode()
	}
	return 1
}

// noArgs rejects positional arguments as an invalid-input error (exit 2).
func noArgs(cmd *cobra.Command, args []string) error {
	if len(args) > 0 {
		return &invalidInputError{err: fmt.Errorf("unknown command %q for %q", args[0], cmd.CommandPath())}
	}
	return nil
}

// oneArg requires exactly one positional argument.
func oneArg(cmd *cobra.Command, args []string) error {
	if len(args) != 1 {
		return &invalidInputError{err: fmt.Errorf("accepts exactly 1 argument, received %d", len(args))}
	}
	return nil
}
