package rules

import (
	"context"
	"fmt"
	"strings"

	"github.com/ThuatPhan/PodSleuth/internal/analyzer"
	"github.com/ThuatPhan/PodSleuth/internal/domain"
)

// ConfigRule implements CONFIG-001: CreateContainerConfigError or a
// config-related Event. Secret values are never inspected.
type ConfigRule struct{}

// ID implements Rule.
func (r *ConfigRule) ID() string { return IDConfig }

// Evaluate implements Rule.
func (r *ConfigRule) Evaluate(_ context.Context, input analyzer.Input) (*domain.Hypothesis, error) {
	views, eventsEv, _ := eventsEvidence(input.Evidence)
	configEvent, hasConfigEvent := firstEventMessage(views, isConfigEvent)

	pod, podEv, hasPod := podEvidence(input.Evidence)
	var container, reason string
	statusMatch := false
	if hasPod {
		for _, cs := range pod.AllContainerStatuses() {
			if w, ok := waitingReason(cs); ok && w == "CreateContainerConfigError" {
				container, reason, statusMatch = cs.Name, w, true
				break
			}
		}
	}

	if !statusMatch && !hasConfigEvent {
		return nil, nil
	}

	score := analyzer.ScoreEventOnly
	var evidenceIDs []string
	if statusMatch {
		score = analyzer.ScoreDirectStatus
		evidenceIDs = append(evidenceIDs, podEv.ID)
	}
	if hasConfigEvent {
		if statusMatch {
			score += analyzer.BoostEvents
		}
		evidenceIDs = append(evidenceIDs, eventsEv.ID)
	}

	summary := fmt.Sprintf("Container %s could not be created because a referenced config object or key is unavailable or invalid (reason %s). Secret values were not inspected.", container, reason)
	if hasConfigEvent && configEvent != "" {
		summary += fmt.Sprintf(" Event: %s.", quote(configEvent))
	}
	if !hasPod {
		summary += " Pod status was unavailable at collection time."
	}

	var missing []string
	if !hasPod {
		missing = append(missing, "pod spec config references")
	}

	return &domain.Hypothesis{
		RuleID:          IDConfig,
		IncidentID:      input.Incident.ID,
		Summary:         summary,
		Score:           analyzer.CapScore(score),
		EvidenceIDs:     evidenceIDs,
		MissingEvidence: missing,
		SuggestedChecks: checks(input,
			"kubectl get pod {pod} -n {ns} -o jsonpath='{.spec.containers[*].envFrom}{.spec.containers[*].env[*].valueFrom}'  # config references (names only)",
			"kubectl get configmaps,secrets -n {ns}  # verify referenced objects exist (names only)",
		),
	}, nil
}

func isConfigEvent(v eventView) bool {
	if v.Reason == "CreateContainerConfigError" {
		return true
	}
	msg := strings.ToLower(v.Message)
	return strings.Contains(msg, "configmap") || strings.Contains(msg, "secret")
}
