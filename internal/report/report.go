// Package report renders persisted incident investigations as Markdown
// or JSON. It reads through the storage interface only: no Kubernetes
// calls, no SQL, no re-analysis.
package report

import (
	"context"
	"fmt"

	"github.com/ThuatPhan/PodSleuth/internal/domain"
	"github.com/ThuatPhan/PodSleuth/internal/evidence"
	"github.com/ThuatPhan/PodSleuth/internal/storage"
)

// Format is a report output format.
type Format string

// Supported formats.
const (
	FormatMarkdown Format = "markdown"
	FormatJSON     Format = "json"
)

// ParseFormat validates a format flag value. Empty means the default
// (markdown).
func ParseFormat(s string) (Format, error) {
	switch Format(s) {
	case "":
		return FormatMarkdown, nil
	case FormatMarkdown, FormatJSON:
		return Format(s), nil
	default:
		return "", fmt.Errorf("invalid output format %q: supported formats are markdown and json", s)
	}
}

// Data is everything needed to render one investigation report.
type Data struct {
	Incident       domain.Incident
	Evidence       []domain.Evidence
	Hypotheses     []domain.Hypothesis
	Timeline       []domain.TimelineEvent
	CollectionRuns []domain.CollectionRun
}

// Gather loads the report data for an incident from storage. Missing
// analysis, timeline or collection runs are not errors: the report
// renders what exists.
func Gather(ctx context.Context, store storage.Store, incidentID string) (Data, error) {
	if incidentID == "" {
		return Data{}, fmt.Errorf("report: incident id is required")
	}
	incident, err := store.GetIncident(ctx, incidentID)
	if err != nil {
		return Data{}, fmt.Errorf("report: incident %s: %w", incidentID, err)
	}
	evidence, err := store.ListEvidence(ctx, incidentID)
	if err != nil {
		return Data{}, fmt.Errorf("report: evidence for %s: %w", incidentID, err)
	}
	hypotheses, err := store.GetAnalysis(ctx, incidentID)
	if err != nil {
		return Data{}, fmt.Errorf("report: analysis for %s: %w", incidentID, err)
	}
	timeline, err := store.ListTimeline(ctx, incidentID)
	if err != nil {
		return Data{}, fmt.Errorf("report: timeline for %s: %w", incidentID, err)
	}
	runs, err := store.ListCollectionRuns(ctx, incidentID)
	if err != nil {
		return Data{}, fmt.Errorf("report: collection runs for %s: %w", incidentID, err)
	}
	return Data{
		Incident:       incident,
		Evidence:       evidence,
		Hypotheses:     hypotheses,
		Timeline:       timeline,
		CollectionRuns: runs,
	}, nil
}

// Render renders the data in the given format.
func Render(data Data, format Format) (string, error) {
	switch format {
	case FormatMarkdown:
		return renderMarkdown(data), nil
	case FormatJSON:
		return renderJSON(data)
	default:
		return "", fmt.Errorf("report: unsupported format %q", format)
	}
}

// HasPartialCollection reports whether collection was partial: any
// evidence item with a non-success collection status, or any collection
// run that did not succeed.
func HasPartialCollection(data Data) bool {
	for _, ev := range data.Evidence {
		if ev.CollectionStatus != evidence.StatusSuccess {
			return true
		}
	}
	for _, run := range data.CollectionRuns {
		if run.Status != domain.RunStatusSuccess {
			return true
		}
	}
	return false
}
