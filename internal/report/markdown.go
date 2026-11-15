package report

import (
	"fmt"
	"strings"
	"time"

	"github.com/ThuatPhan/PodSleuth/internal/domain"
)

// maxPreview bounds the inline evidence preview in Markdown reports.
const maxPreview = 400

// renderMarkdown renders a terminal-readable Markdown report. Facts,
// hypotheses and unknowns live in separate sections so uncertainty is
// never hidden.
func renderMarkdown(d Data) string {
	var b strings.Builder
	b.WriteString("# PodSleuth Investigation Report\n\n")

	writeIncident(&b, d.Incident)
	writeHypotheses(&b, d.Hypotheses)
	writeTimeline(&b, d.Timeline)
	writeEvidence(&b, d.Evidence)
	writeCollectionRuns(&b, d.CollectionRuns)
	writeGaps(&b, d)

	return b.String()
}

func writeIncident(b *strings.Builder, inc domain.Incident) {
	b.WriteString("## Incident (observed facts)\n\n")
	fmt.Fprintf(b, "- ID: %s\n", inc.ID)
	fmt.Fprintf(b, "- Cluster: %s\n", inc.ClusterID)
	fmt.Fprintf(b, "- Status: %s\n", inc.Status)
	fmt.Fprintf(b, "- Failure type: %s\n", inc.FailureType)
	fmt.Fprintf(b, "- Severity: %s\n", inc.Severity)

	resource := fmt.Sprintf("%s %s/%s", resourceKind(inc.Resource), inc.Resource.Namespace, inc.Resource.Name)
	if inc.Resource.UID != "" {
		resource += fmt.Sprintf(" (uid %s)", inc.Resource.UID)
	}
	fmt.Fprintf(b, "- Resource: %s\n", resource)
	if inc.ContainerName != "" {
		fmt.Fprintf(b, "- Container: %s\n", inc.ContainerName)
	}
	if inc.WorkloadKind != "" {
		workload := fmt.Sprintf("%s/%s", inc.WorkloadKind, inc.WorkloadName)
		if inc.WorkloadUID != "" {
			workload += fmt.Sprintf(" (uid %s)", inc.WorkloadUID)
		}
		fmt.Fprintf(b, "- Workload: %s\n", workload)
	}
	fmt.Fprintf(b, "- First seen: %s\n", rfc3339(inc.FirstSeenAt))
	fmt.Fprintf(b, "- Last seen: %s\n", rfc3339(inc.LastSeenAt))
	if inc.ResolvedAt != nil {
		fmt.Fprintf(b, "- Resolved: %s\n", rfc3339(*inc.ResolvedAt))
	}
	if inc.Summary != "" {
		fmt.Fprintf(b, "\n%s\n", inc.Summary)
	}
}

func writeHypotheses(b *strings.Builder, hypotheses []domain.Hypothesis) {
	b.WriteString("\n## Hypotheses (ranked)\n\n")
	b.WriteString("_Hypotheses are heuristic rankings over collected evidence, not confirmed causes._\n\n")
	if len(hypotheses) == 0 {
		b.WriteString("_No analysis has been persisted for this incident; the root cause remains unknown._\n")
		return
	}
	for i, h := range hypotheses {
		fmt.Fprintf(b, "### %d. %s — score %.2f\n\n", i+1, h.RuleID, h.Score)
		fmt.Fprintf(b, "%s\n\n", h.Summary)
		fmt.Fprintf(b, "- Supporting evidence: %s\n", strings.Join(h.EvidenceIDs, ", "))
		if len(h.MissingEvidence) > 0 {
			fmt.Fprintf(b, "- Missing evidence: %s\n", strings.Join(h.MissingEvidence, "; "))
		}
		if len(h.SuggestedChecks) > 0 {
			b.WriteString("- Suggested checks (read-only):\n")
			for _, c := range h.SuggestedChecks {
				fmt.Fprintf(b, "  - `%s`\n", c)
			}
		}
		b.WriteString("\n")
	}
}

func writeTimeline(b *strings.Builder, events []domain.TimelineEvent) {
	b.WriteString("## Timeline\n\n")
	if len(events) == 0 {
		b.WriteString("_No timeline events recorded._\n")
		return
	}
	for _, ev := range events {
		fmt.Fprintf(b, "- %s %s: %s\n", rfc3339(ev.OccurredAt), ev.EventType, ev.Message)
	}
}

func writeEvidence(b *strings.Builder, items []domain.Evidence) {
	b.WriteString("\n## Evidence\n\n")
	if len(items) == 0 {
		b.WriteString("_No evidence collected._\n")
		return
	}
	for _, ev := range items {
		fmt.Fprintf(b, "### %s — %s (%s)\n\n", ev.ID, ev.Kind, ev.Source)
		fmt.Fprintf(b, "- Observed: %s\n", rfc3339(ev.ObservedAt))
		if ev.EventTime != nil {
			fmt.Fprintf(b, "- Event time: %s\n", rfc3339(*ev.EventTime))
		}
		if ev.Resource.Name != "" {
			fmt.Fprintf(b, "- Resource: %s %s/%s\n", resourceKind(ev.Resource), ev.Resource.Namespace, ev.Resource.Name)
		}
		fmt.Fprintf(b, "- Collection status: %s\n", ev.CollectionStatus)
		if ev.Redacted {
			b.WriteString("- Redacted: yes\n")
		}
		fmt.Fprintf(b, "- Content hash: %s\n", ev.ContentHash)
		switch {
		case ev.Text != "":
			fmt.Fprintf(b, "\n```\n%s\n```\n", preview(ev.Text))
		case len(ev.Content) > 0:
			fmt.Fprintf(b, "\n_Structured content, %d bytes._\n", len(ev.Content))
		}
		b.WriteString("\n")
	}
}

func writeCollectionRuns(b *strings.Builder, runs []domain.CollectionRun) {
	if len(runs) == 0 {
		return
	}
	b.WriteString("## Collection runs\n\n")
	for _, r := range runs {
		fmt.Fprintf(b, "- %s: %s (started %s", r.CollectorName, r.Status, rfc3339(r.StartedAt))
		if r.CompletedAt != nil {
			fmt.Fprintf(b, ", completed %s", rfc3339(*r.CompletedAt))
		}
		if r.ErrorCode != "" {
			fmt.Fprintf(b, ", error %s: %s", r.ErrorCode, r.ErrorMessageRedacted)
		}
		b.WriteString(")\n")
	}
	b.WriteString("\n")
}

func writeGaps(b *strings.Builder, d Data) {
	b.WriteString("## Gaps and unknowns\n\n")
	gaps := gatherGaps(d)
	if len(gaps) == 0 {
		b.WriteString("_None identified._\n")
		return
	}
	for _, g := range gaps {
		fmt.Fprintf(b, "- %s\n", g)
	}
}

func gatherGaps(d Data) []string {
	var gaps []string
	if len(d.Evidence) == 0 {
		gaps = append(gaps, "No evidence was collected for this incident.")
	}
	if len(d.Hypotheses) == 0 {
		gaps = append(gaps, "No RCA analysis has been persisted; the root cause remains unknown.")
	}
	seen := map[string]bool{}
	for _, h := range d.Hypotheses {
		for _, m := range h.MissingEvidence {
			key := h.RuleID + ": " + m
			if !seen[key] {
				seen[key] = true
				gaps = append(gaps, fmt.Sprintf("Missing evidence (%s): %s", h.RuleID, m))
			}
		}
	}
	for _, ev := range d.Evidence {
		if ev.CollectionStatus != "success" {
			gaps = append(gaps, fmt.Sprintf("Evidence %s was collected with status %q.", ev.ID, ev.CollectionStatus))
		}
	}
	return gaps
}

func resourceKind(ref domain.ResourceRef) string {
	if ref.Kind != "" {
		return ref.Kind
	}
	return "Pod"
}

func rfc3339(t time.Time) string {
	return t.UTC().Format(time.RFC3339)
}

func preview(text string) string {
	trimmed := strings.TrimRight(text, "\n")
	if len(trimmed) > maxPreview {
		return trimmed[:maxPreview] + "\n… (truncated preview)"
	}
	return trimmed
}
