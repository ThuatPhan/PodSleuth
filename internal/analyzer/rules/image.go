package rules

import (
	"context"
	"fmt"
	"strings"

	"github.com/ThuatPhan/PodSleuth/internal/analyzer"
	"github.com/ThuatPhan/PodSleuth/internal/domain"
)

// ImageRule implements IMAGE-001: ImagePullBackOff/ErrImagePull status
// or a pull Event.
type ImageRule struct{}

// ID implements Rule.
func (r *ImageRule) ID() string { return IDImage }

// Evaluate implements Rule.
func (r *ImageRule) Evaluate(_ context.Context, input analyzer.Input) (*domain.Hypothesis, error) {
	views, eventsEv, _ := eventsEvidence(input.Evidence)
	pullEvent, hasPullEvent := firstEventMessage(views, isPullEvent)

	pod, podEv, hasPod := podEvidence(input.Evidence)
	var container, reason, image string
	statusMatch := false
	if hasPod {
		for _, cs := range pod.AllContainerStatuses() {
			if w, ok := waitingReason(cs); ok && (w == "ImagePullBackOff" || w == "ErrImagePull") {
				container, reason, statusMatch = cs.Name, w, true
				if spec := specContainer(pod, cs.Name); spec != nil {
					image = spec.Image
				}
				break
			}
		}
	}

	if !statusMatch && !hasPullEvent {
		return nil, nil
	}

	score := analyzer.ScoreEventOnly
	var evidenceIDs []string
	if statusMatch {
		score = analyzer.ScoreDirectStatus
		evidenceIDs = append(evidenceIDs, podEv.ID)
	}
	if hasPullEvent {
		if statusMatch {
			score += analyzer.BoostEvents
		}
		evidenceIDs = append(evidenceIDs, eventsEv.ID)
	}

	summary := fmt.Sprintf("Image retrieval failed for container %s (reason %s). The image reference, registry availability, or pull credentials may be at fault; this does not prove the image name is invalid.", container, reason)
	if image != "" {
		summary = fmt.Sprintf("Image retrieval failed for container %s (image %q, reason %s). The image reference, registry availability, or pull credentials may be at fault; this does not prove the image name is invalid.", container, image, reason)
	}
	if hasPullEvent && pullEvent != "" {
		summary += fmt.Sprintf(" Event: %s.", quote(pullEvent))
	}
	if !hasPod {
		summary += " Pod status was unavailable at collection time."
	}

	var missing []string
	if !hasPod {
		missing = append(missing, "pod status (pod unavailable at collection)")
	}

	return &domain.Hypothesis{
		RuleID:          IDImage,
		IncidentID:      input.Incident.ID,
		Summary:         summary,
		Score:           analyzer.CapScore(score),
		EvidenceIDs:     evidenceIDs,
		MissingEvidence: missing,
		SuggestedChecks: checks(input,
			"kubectl get pod {pod} -n {ns} -o jsonpath='{.spec.containers[*].image}'  # image references",
			"kubectl describe pod {pod} -n {ns}  # pull failure events",
		),
	}, nil
}

func isPullEvent(v eventView) bool {
	if v.Reason == "PullError" || v.Reason == "ErrImagePull" || v.Reason == "ImagePullBackOff" {
		return true
	}
	return strings.Contains(strings.ToLower(v.Message), "failed to pull image")
}

func specContainer(pod *domain.PodView, name string) *domain.ContainerSpecView {
	for i := range pod.Spec.Containers {
		if pod.Spec.Containers[i].Name == name {
			return &pod.Spec.Containers[i]
		}
	}
	for i := range pod.Spec.InitContainers {
		if pod.Spec.InitContainers[i].Name == name {
			return &pod.Spec.InitContainers[i]
		}
	}
	return nil
}
