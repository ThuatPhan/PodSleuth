package cli

import (
	"fmt"

	"github.com/spf13/cobra"
)

// Build information, overridable via -ldflags at build time.
var (
	appVersion   = "dev"
	appCommit    = "none"
	appBuildDate = "unknown"
)

func newVersionCommand(deps Deps) *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Print the PodSleuth version",
		Args:  noArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			v := deps.Version
			if v == "" {
				v = appVersion
			}
			commit := deps.Commit
			if commit == "" {
				commit = appCommit
			}
			buildDate := deps.BuildDate
			if buildDate == "" {
				buildDate = appBuildDate
			}
			fmt.Fprintf(cmd.OutOrStdout(), "podsleuth %s (commit %s, built %s)\n", v, commit, buildDate)
			return nil
		},
	}
}
