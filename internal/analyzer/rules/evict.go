package rules

import (
	"context"
	"fmt"

	"github.com/ThuatPhan/PodSleuth/internal/analyzer"
	"github.com/ThuatPhan/PodSleuth/internal/domain"
)

// EvictRule implements EVICT-001: Pod status reason Evicted.
type EvictRule struct{}

// ID implements Rule.
func (r *EvictRule) ID() string { return IDEvict }

// Evaluate implements Rule.
func (r *EvictRule) Evaluate(_ context.Context, input analyzer.Input) (*domain.Hypothesis, error) {
	pod, podEv, ok := podEvidence(input.Evidence)
	if !ok || pod.Status.Reason != "Evicted" {
		return nil, nil
	}

	summary := "The kubelet evicted the pod. Eviction reflects node pressure or policy, not an application failure."
	if pod.Status.Message != "" {
		summary = fmt.Sprintf("The kubelet evicted the pod. Reported reason: %s. Eviction reflects node pressure or policy, not an application failure.", quote(pod.Status.Message))
	}

	return &domain.Hypothesis{
		RuleID:      IDEvict,
		IncidentID:  input.Incident.ID,
		Summary:     summary,
		Score:       analyzer.ScoreDirectStatus,
		EvidenceIDs: []string{podEv.ID},
		MissingEvidence: []string{
			"node conditions and pressure details (node context not collected in MVP)",
		},
		SuggestedChecks: checks(input,
			"kubectl describe pod %%s -n %%s  # eviction reason and node name",
			"kubectl get events -n %%s --field-selector involvedObject.name=%s  # eviction events",
		),
	}, nil
}
