package rules

import (
	"context"
	"fmt"
	"strings"

	"github.com/ThuatPhan/PodSleuth/internal/analyzer"
	"github.com/ThuatPhan/PodSleuth/internal/domain"
)

// CrashRule implements CRASH-001: non-zero termination and/or repeated
// restarts (CrashLoopBackOff), with a log clue when available.
type CrashRule struct{}

// ID implements Rule.
func (r *CrashRule) ID() string { return IDCrash }

// Evaluate implements Rule.
func (r *CrashRule) Evaluate(_ context.Context, input analyzer.Input) (*domain.Hypothesis, error) {
	pod, podEv, ok := podEvidence(input.Evidence)
	if !ok {
		return nil, nil
	}
	for _, cs := range pod.AllContainerStatuses() {
		w, waiting := waitingReason(cs)
		var summary string
		switch {
		case waiting && w == "CrashLoopBackOff":
			summary = fmt.Sprintf("Container %s is in CrashLoopBackOff after %d restarts: the process repeatedly exits or fails to start. Repeated startup failure, not necessarily an application bug.", cs.Name, cs.RestartCount)
		default:
			terminated := terminatedOf(cs)
			if terminated == nil || terminated.ExitCode == 0 ||
				terminated.Reason == "OOMKilled" || terminated.Reason == "Evicted" {
				continue // OOM-001 and EVICT-001 own those terminations
			}
			summary = fmt.Sprintf("Container %s exited unexpectedly with exit code %d (reason %s).", cs.Name, terminated.ExitCode, terminated.Reason)
		}

		score := analyzer.ScoreDirectStatus
		evidenceIDs := []string{podEv.ID}

		if hasLogs(input.Evidence) {
			score += analyzer.BoostLogs
			evidenceIDs = append(evidenceIDs, logEvidenceIDs(input.Evidence)...)
			if _, text, ok := logsEvidence(input.Evidence, "previous-logs"); ok && strings.TrimSpace(text) != "" {
				summary += fmt.Sprintf(" Log clue: %q.", quote(text))
			}
		}
		if pod.IsReady() {
			summary += " The container is currently running; the failure may be transient."
		}

		var missing []string
		if !hasLogs(input.Evidence) {
			missing = append(missing, "previous container logs")
		}

		return &domain.Hypothesis{
			RuleID:          IDCrash,
			IncidentID:      input.Incident.ID,
			Summary:         summary,
			Score:           analyzer.CapScore(score),
			EvidenceIDs:     evidenceIDs,
			MissingEvidence: missing,
			SuggestedChecks: checks(input,
				"kubectl logs %%s -n %%s --previous  # output of the last terminated instance",
				"kubectl describe pod %%s -n %%s  # termination reason and restart count",
			),
		}, nil
	}
	return nil, nil
}
