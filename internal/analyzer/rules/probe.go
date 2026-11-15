package rules

import (
	"context"
	"fmt"
	"strings"

	"github.com/ThuatPhan/PodSleuth/internal/analyzer"
	"github.com/ThuatPhan/PodSleuth/internal/domain"
)

// ProbeRule implements PROBE-001: a probe-related Event plus probe
// spec or restart evidence.
type ProbeRule struct{}

// ID implements Rule.
func (r *ProbeRule) ID() string { return IDProbe }

// Evaluate implements Rule.
func (r *ProbeRule) Evaluate(_ context.Context, input analyzer.Input) (*domain.Hypothesis, error) {
	views, eventsEv, hasEvents := eventsEvidence(input.Evidence)
	if !hasEvents {
		return nil, nil
	}

	var probeEvent eventView
	found := false
	for _, v := range views {
		if isProbeEvent(v) {
			probeEvent = v
			found = true
			break
		}
	}
	if !found {
		return nil, nil
	}

	pod, podEv, hasPod := podEvidence(input.Evidence)
	score := analyzer.ScoreEventOnly
	var evidenceIDs = []string{eventsEv.ID}
	var missing []string

	probeKind := probeKindOf(probeEvent)
	container := probeEvent.InvolvedObject.Name
	if hasPod {
		score += analyzer.BoostEvents
		evidenceIDs = append(evidenceIDs, podEv.ID)
		// Prefer the container the event names; fall back to the first
		// container with a probe of that kind.
		if !podHasProbe(pod, container, probeKind) {
			if name, ok := anyProbedContainer(pod, probeKind); ok {
				container = name
			}
		}
	} else {
		missing = append(missing, "pod spec probe configuration")
	}

	summary := fmt.Sprintf("The %s probe is failing (event: %s). The probe configuration and the application's health disagree; this is a health-check mismatch, not proof of an application bug.", probeKind, quote(probeEvent.Message))
	if hasPod && podHasProbe(pod, container, probeKind) {
		summary += fmt.Sprintf(" Container %s defines a %s probe.", container, probeKind)
	}

	return &domain.Hypothesis{
		RuleID:          IDProbe,
		IncidentID:      input.Incident.ID,
		Summary:         summary,
		Score:           analyzer.CapScore(score),
		EvidenceIDs:     evidenceIDs,
		MissingEvidence: missing,
		SuggestedChecks: checks(input,
			"kubectl describe pod {pod} -n {ns}  # probe configuration and events",
			"kubectl logs {pod} -n {ns} --previous  # output before the probe kill",
		),
	}, nil
}

func isProbeEvent(v eventView) bool {
	switch v.Reason {
	case "Unhealthy", "StartupProbeFailed", "LivenessProbeFailed", "ReadinessProbeFailed":
		return true
	}
	return false
}

// probeKindOf distinguishes startup, liveness and readiness probes
// where the event allows.
func probeKindOf(v eventView) string {
	switch v.Reason {
	case "StartupProbeFailed":
		return "startup"
	case "LivenessProbeFailed":
		return "liveness"
	case "ReadinessProbeFailed":
		return "readiness"
	}
	msg := strings.ToLower(v.Message)
	switch {
	case strings.HasPrefix(msg, "startup probe"):
		return "startup"
	case strings.HasPrefix(msg, "readiness probe"):
		return "readiness"
	default:
		return "liveness"
	}
}

func podHasProbe(pod *domain.PodView, container, kind string) bool {
	return pod.Probe(container, kind) != nil
}

func anyProbedContainer(pod *domain.PodView, kind string) (string, bool) {
	for _, c := range pod.AllSpecContainers() {
		if pod.Probe(c.Name, kind) != nil {
			return c.Name, true
		}
	}
	return "", false
}
