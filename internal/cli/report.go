package cli

import (
	"errors"
	"fmt"

	"github.com/spf13/cobra"

	"github.com/ThuatPhan/PodSleuth/internal/config"
	"github.com/ThuatPhan/PodSleuth/internal/report"
	"github.com/ThuatPhan/PodSleuth/internal/storage"
)

// partialReportError reports that a report was produced although
// collection was partial. It maps to exit code 3.
type partialReportError struct {
	incidentID string
}

func (e *partialReportError) Error() string {
	return fmt.Sprintf("report for %s produced; collection was partial (non-success evidence status present)", e.incidentID)
}

// ExitCode implements ExitCoder.
func (e *partialReportError) ExitCode() int { return 3 }

func newReportCommand(deps Deps) *cobra.Command {
	var format string
	cmd := &cobra.Command{
		Use:   "report <incident-id>",
		Short: "Render a persisted incident investigation report",
		Args:  oneArg,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runReport(cmd, deps, args[0], format)
		},
	}
	cmd.Flags().StringVar(&format, "output", string(report.FormatMarkdown), "report format: markdown or json")
	return cmd
}

func runReport(cmd *cobra.Command, deps Deps, incidentID, formatFlag string) error {
	ctx := cmd.Context()
	out := cmd.OutOrStdout()

	format, err := report.ParseFormat(formatFlag)
	if err != nil {
		return &invalidInputError{err: err}
	}
	path, err := config.DefaultStoragePath()
	if err != nil {
		return &invalidInputError{err: err}
	}

	store, err := deps.newStore(path)
	if err != nil {
		return fmt.Errorf("open storage at %s: %w", path, err)
	}
	defer store.Close()

	data, err := report.Gather(ctx, store, incidentID)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return &invalidInputError{err: fmt.Errorf("incident %q not found in %s", incidentID, path)}
		}
		return err
	}

	rendered, err := report.Render(data, format)
	if err != nil {
		return err
	}
	if _, err := fmt.Fprint(out, rendered); err != nil {
		return err
	}

	if report.HasPartialCollection(data) {
		return &partialReportError{incidentID: incidentID}
	}
	return nil
}

// resolveDBPath returns the configured storage location (spec §14
// default; the config file loader will override it later).
func resolveDBPath() (string, error) {
	path, err := config.DefaultStoragePath()
	if err != nil {
		return "", fmt.Errorf("cannot resolve the storage path: %w", err)
	}
	return path, nil
}
