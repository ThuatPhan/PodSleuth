package cli

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/spf13/cobra"

	"github.com/ThuatPhan/PodSleuth/internal/analyzer"
	"github.com/ThuatPhan/PodSleuth/internal/analyzer/rules"
	"github.com/ThuatPhan/PodSleuth/internal/app"
	"github.com/ThuatPhan/PodSleuth/internal/collector"
	"github.com/ThuatPhan/PodSleuth/internal/collector/events"
	"github.com/ThuatPhan/PodSleuth/internal/collector/logs"
	"github.com/ThuatPhan/PodSleuth/internal/collector/pod"
	"github.com/ThuatPhan/PodSleuth/internal/config"
	"github.com/ThuatPhan/PodSleuth/internal/detector"
	"github.com/ThuatPhan/PodSleuth/internal/evidence"
	"github.com/ThuatPhan/PodSleuth/internal/report"
)

func newInvestigateCommand(deps Deps, loadConfig func() (*config.Config, error)) *cobra.Command {
	var (
		format    string
		namespace string
	)
	cmd := &cobra.Command{
		Use:   "investigate pod <pod-name> -n <namespace>",
		Short: "Investigate one Pod on demand and render a report",
		Args:  investigateArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runInvestigate(cmd, deps, loadConfig, args[1], namespace, format)
		},
	}
	cmd.Flags().StringVarP(&namespace, "namespace", "n", "", "namespace of the Pod (required)")
	cmd.Flags().StringVar(&format, "output", string(report.FormatMarkdown), "report format: markdown or json")
	return cmd
}

// investigateArgs validates the spec's positional syntax:
// investigate pod <pod-name>.
func investigateArgs(cmd *cobra.Command, args []string) error {
	if len(args) != 2 {
		return &invalidInputError{err: fmt.Errorf("accepts exactly 2 arguments: pod <pod-name>, received %d", len(args))}
	}
	if args[0] != "pod" {
		return &invalidInputError{err: fmt.Errorf("unknown resource type %q; usage: podsleuth investigate pod <pod-name> -n <namespace>", args[0])}
	}
	return nil
}

func runInvestigate(cmd *cobra.Command, deps Deps, loadConfig func() (*config.Config, error), podName, namespace, formatFlag string) error {
	ctx := cmd.Context()
	out := cmd.OutOrStdout()
	errOut := cmd.ErrOrStderr()

	if namespace == "" {
		return &invalidInputError{err: fmt.Errorf("namespace is required; pass -n <namespace>")}
	}
	format, err := report.ParseFormat(formatFlag)
	if err != nil {
		return &invalidInputError{err: err}
	}
	path, err := resolveDBPath()
	if err != nil {
		return &invalidInputError{err: err}
	}
	cfg, err := loadConfig()
	if err != nil {
		return &invalidInputError{err: err}
	}

	store, err := deps.newStore(path)
	if err != nil {
		return fmt.Errorf("open storage at %s: %w", path, err)
	}
	defer store.Close()

	clientset, ctxInfo, err := deps.newClientset(cfg)
	if err != nil {
		return fmt.Errorf("build Kubernetes client: %w", err)
	}
	clusterID := ctxInfo.Cluster
	if clusterID == "" {
		clusterID = cfg.Cluster.ClusterID
	}
	fmt.Fprintf(errOut, "investigating pod %s/%s on cluster %s (context %s)\n", namespace, podName, clusterID, ctxInfo.Context)

	engine, err := analyzer.NewEngine(rules.All()...)
	if err != nil {
		return fmt.Errorf("build analyzer engine: %w", err)
	}

	inv := app.NewInvestigator(
		clientset,
		clusterID,
		store,
		detector.NewPodDetector(clusterID),
		collector.NewRegistry(
			pod.NewPodCollector(clientset, evidence.DefaultOptions()),
			logs.NewLogsCollector(clientset, logs.Options{PreviousLogs: true, CurrentLogs: true}, evidence.DefaultOptions()),
			events.NewEventsCollector(clientset, evidence.DefaultOptions()),
		),
		engine,
	)

	result, err := inv.Investigate(ctx, namespace, podName)
	if err != nil {
		var nf *app.PodNotFoundError
		if errors.As(err, &nf) {
			return &invalidInputError{err: err}
		}
		return err
	}
	for _, w := range result.Warnings {
		fmt.Fprintf(errOut, "warning: %s\n", w)
	}

	if result.Healthy {
		return writeHealthyOutput(out, format, namespace, podName)
	}

	data, err := report.Gather(ctx, store, result.PrimaryIncidentID())
	if err != nil {
		return fmt.Errorf("gather report data: %w", err)
	}
	rendered, err := report.Render(data, format)
	if err != nil {
		return err
	}
	if _, err := fmt.Fprint(out, rendered); err != nil {
		return err
	}

	if len(result.IncidentIDs) > 1 {
		fmt.Fprintf(errOut, "note: %d incidents created for this pod: %s; the report shows the first\n",
			len(result.IncidentIDs), strings.Join(result.IncidentIDs, ", "))
	}
	if report.HasPartialCollection(data) {
		return &partialReportError{incidentID: result.PrimaryIncidentID()}
	}
	return nil
}

// healthyDocument is the stable JSON shape for a Pod with no detected
// failure.
type healthyDocument struct {
	Status   string  `json:"status"`
	Pod      string  `json:"pod"`
	Incident *string `json:"incident"`
}

func writeHealthyOutput(out io.Writer, format report.Format, namespace, podName string) error {
	switch format {
	case report.FormatJSON:
		doc := healthyDocument{Status: "healthy", Pod: namespace + "/" + podName}
		b, err := json.MarshalIndent(doc, "", "  ")
		if err != nil {
			return err
		}
		_, err = fmt.Fprintln(out, string(b))
		return err
	default:
		_, err := fmt.Fprintf(out, "No failure detected on pod %s/%s.\n\nThe Pod does not match any condition PodSleuth detects; no incident was created.\n", namespace, podName)
		return err
	}
}
