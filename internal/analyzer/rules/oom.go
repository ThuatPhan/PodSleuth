package rules

import (
	"context"
	"fmt"
	"strings"

	"github.com/ThuatPhan/PodSleuth/internal/analyzer"
	"github.com/ThuatPhan/PodSleuth/internal/domain"
)

// OOMRule implements OOM-001: last termination reason OOMKilled.
type OOMRule struct{}

// ID implements Rule.
func (r *OOMRule) ID() string { return IDOOM }

// Evaluate implements Rule.
func (r *OOMRule) Evaluate(_ context.Context, input analyzer.Input) (*domain.Hypothesis, error) {
	pod, podEv, ok := podEvidence(input.Evidence)
	if !ok {
		return nil, nil
	}
	for _, cs := range pod.AllContainerStatuses() {
		terminated := terminatedOf(cs)
		if terminated == nil || terminated.Reason != "OOMKilled" {
			continue
		}

		score := analyzer.ScoreDirectStatus
		evidenceIDs := []string{podEv.ID}
		views, eventsEv, hasEvents := eventsEvidence(input.Evidence)
		if hasEvents && anyEvent(views, func(v eventView) bool {
			return strings.Contains(strings.ToUpper(v.Reason), "OOM")
		}) {
			score += analyzer.BoostEvents
			evidenceIDs = append(evidenceIDs, eventsEv.ID)
		}

		summary := fmt.Sprintf("Container %s was terminated by the OOM killer (exit code %d). The configured memory limit is likely relevant; actual peak usage is unknown without metrics.", cs.Name, terminated.ExitCode)
		if pod.IsReady() {
			summary += " The container has since restarted and is currently running."
		}
		return &domain.Hypothesis{
			RuleID:      IDOOM,
			IncidentID:  input.Incident.ID,
			Summary:     summary,
			Score:       analyzer.CapScore(score),
			EvidenceIDs: evidenceIDs,
			MissingEvidence: []string{
				"memory usage metrics (not collected in MVP)",
			},
			SuggestedChecks: checks(input,
				"kubectl describe pod %s -n %s  # Last State: OOMKilled, memory limits",
				"kubectl top pods -n %s  # current usage, if metrics-server is installed",
			),
		}, nil
	}
	return nil, nil
}
