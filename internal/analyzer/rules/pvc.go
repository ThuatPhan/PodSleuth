package rules

import (
	"context"
	"fmt"

	"github.com/ThuatPhan/PodSleuth/internal/analyzer"
	"github.com/ThuatPhan/PodSleuth/internal/domain"
)

// PVCRule implements PVC-001: PVC Pending or a volume mount/attach
// Event.
type PVCRule struct{}

// ID implements Rule.
func (r *PVCRule) ID() string { return IDPVC }

// Evaluate implements Rule.
func (r *PVCRule) Evaluate(_ context.Context, input analyzer.Input) (*domain.Hypothesis, error) {
	views, eventsEv, ok := eventsEvidence(input.Evidence)
	if !ok {
		return nil, nil
	}
	mountEvent, found := firstEventMessage(views, isVolumeEvent)
	if !found {
		return nil, nil
	}

	summary := fmt.Sprintf("Volume attach or mount for the pod failed. Storage binding, provisioning, or mounting may be at fault; the event states the exact symptom. Event: %s.", quote(mountEvent))

	return &domain.Hypothesis{
		RuleID:      IDPVC,
		IncidentID:  input.Incident.ID,
		Summary:     summary,
		Score:       analyzer.ScoreEventOnly,
		EvidenceIDs: []string{eventsEv.ID},
		MissingEvidence: []string{
			"PVC status and storage class details (storage context not collected in MVP)",
		},
		SuggestedChecks: checks(input,
			"kubectl get pvc -n {ns}  # PVC phase",
			"kubectl describe pod {pod} -n {ns}  # mount events",
		),
	}, nil
}

func isVolumeEvent(v eventView) bool {
	switch v.Reason {
	case "FailedMount", "FailedAttachVolume", "FailedDetachVolume":
		return true
	}
	return false
}
