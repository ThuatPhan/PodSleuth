package rules

import (
	"context"
	"fmt"

	"github.com/ThuatPhan/PodSleuth/internal/analyzer"
	"github.com/ThuatPhan/PodSleuth/internal/domain"
)

// SchedRule implements SCHED-001: FailedScheduling Event and/or the
// PodScheduled=False condition.
type SchedRule struct{}

// ID implements Rule.
func (r *SchedRule) ID() string { return IDSched }

// Evaluate implements Rule.
func (r *SchedRule) Evaluate(_ context.Context, input analyzer.Input) (*domain.Hypothesis, error) {
	views, eventsEv, _ := eventsEvidence(input.Evidence)
	schedEvent, hasSchedEvent := firstEventMessage(views, func(v eventView) bool {
		return v.Reason == "FailedScheduling"
	})

	pod, podEv, hasPod := podEvidence(input.Evidence)
	conditionMatch := false
	var conditionMessage string
	if hasPod {
		for _, c := range pod.Status.Conditions {
			if c.Type == "PodScheduled" && c.Status == "False" {
				conditionMatch = true
				conditionMessage = c.Message
				break
			}
		}
	}

	if !hasSchedEvent && !conditionMatch {
		return nil, nil
	}

	score := analyzer.ScoreEventOnly
	var evidenceIDs []string
	if conditionMatch {
		score = analyzer.ScoreDirectStatus
		evidenceIDs = append(evidenceIDs, podEv.ID)
	}
	if hasSchedEvent {
		if conditionMatch {
			score += analyzer.BoostEvents
		}
		evidenceIDs = append(evidenceIDs, eventsEv.ID)
	}

	qualifier := conditionMessage
	if !hasSchedEvent || qualifier == "" {
		if msg, ok := schedEventMessage(hasSchedEvent, schedEvent); ok {
			qualifier = msg
		}
	}
	summary := "The scheduler could not place the pod: resource requests, constraints, or taints prevented scheduling. This does not necessarily indicate insufficient cluster capacity."
	if qualifier != "" {
		summary = fmt.Sprintf("The scheduler could not place the pod (%s). Resource requests, constraints, or taints prevented scheduling; this does not necessarily indicate insufficient cluster capacity.", quote(qualifier))
	}

	return &domain.Hypothesis{
		RuleID:      IDSched,
		IncidentID:  input.Incident.ID,
		Summary:     summary,
		Score:       analyzer.CapScore(score),
		EvidenceIDs: evidenceIDs,
		MissingEvidence: []string{
			"node capacity, taints and affinity details (node context not collected in MVP)",
		},
		SuggestedChecks: checks(input,
			"kubectl describe pod {pod} -n {ns}  # FailedScheduling event message",
			"kubectl get events --field-selector involvedObject.name={pod} -n {ns}  # scheduling events",
		),
	}, nil
}

func schedEventMessage(has bool, msg string) (string, bool) {
	return msg, has
}
