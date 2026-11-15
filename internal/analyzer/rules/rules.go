// Package rules implements the MVP deterministic RCA rules over
// collected evidence. Rules are pure: identical input yields identical
// output, and no rule claims a cause its evidence does not support.
package rules

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/ThuatPhan/PodSleuth/internal/analyzer"
	"github.com/ThuatPhan/PodSleuth/internal/domain"
)

// MVP rule identifiers.
const (
	IDOOM    = "OOM-001"
	IDCrash  = "CRASH-001"
	IDImage  = "IMAGE-001"
	IDConfig = "CONFIG-001"
	IDSched  = "SCHED-001"
	IDProbe  = "PROBE-001"
	IDEvict  = "EVICT-001"
	IDPVC    = "PVC-001"
)

// All returns every MVP rule the current evidence collectors can
// support. NODE-001 and DEPLOY-001 are absent: they require node and
// workload evidence that no MVP collector produces yet.
func All() []analyzer.Rule {
	return []analyzer.Rule{
		&OOMRule{},
		&CrashRule{},
		&ImageRule{},
		&ConfigRule{},
		&SchedRule{},
		&ProbeRule{},
		&EvictRule{},
		&PVCRule{},
	}
}

// maxQuoted bounds quoted untrusted text inside hypothesis summaries.
const maxQuoted = 160

// quote returns a truncated single-line excerpt of untrusted text.
func quote(text string) string {
	line := strings.TrimSpace(text)
	if i := strings.IndexByte(line, '\n'); i >= 0 {
		line = line[:i]
	}
	if len(line) > maxQuoted {
		line = line[:maxQuoted]
	}
	return line
}

// podEvidence returns the first parseable pod evidence item.
func podEvidence(evidence []domain.Evidence) (*domain.PodView, domain.Evidence, bool) {
	for _, ev := range evidence {
		if ev.Kind != "pod" || len(ev.Content) == 0 {
			continue
		}
		pod, err := domain.ParsePodView(ev.Content)
		if err != nil {
			continue
		}
		return pod, ev, true
	}
	return nil, domain.Evidence{}, false
}

// logsEvidence returns the logs evidence of the given kind with the
// most recent observation time.
func logsEvidence(evidence []domain.Evidence, kind string) (domain.Evidence, string, bool) {
	var best domain.Evidence
	found := false
	for _, ev := range evidence {
		if ev.Kind != kind || ev.Text == "" {
			continue
		}
		if !found || ev.ObservedAt.After(best.ObservedAt) {
			best = ev
			found = true
		}
	}
	return best, best.Text, found
}

// hasLogs reports whether any logs evidence exists.
func hasLogs(evidence []domain.Evidence) bool {
	_, _, prev := logsEvidence(evidence, "previous-logs")
	_, _, cur := logsEvidence(evidence, "current-logs")
	return prev || cur
}

// logEvidenceIDs returns the IDs of all logs evidence items.
func logEvidenceIDs(evidence []domain.Evidence) []string {
	var ids []string
	for _, ev := range evidence {
		if ev.Kind == "previous-logs" || ev.Kind == "current-logs" {
			ids = append(ids, ev.ID)
		}
	}
	return ids
}

// eventView mirrors the collector/events evidence content shape. A
// drift-guard test parses collector-produced evidence with this type.
type eventView struct {
	Type               string     `json:"type"`
	Reason             string     `json:"reason"`
	Message            string     `json:"message"`
	Count              int32      `json:"count"`
	FirstAt            *time.Time `json:"first_at,omitempty"`
	LastAt             *time.Time `json:"last_at,omitempty"`
	ReportingComponent string     `json:"reporting_component,omitempty"`
	InvolvedObject     struct {
		Kind      string `json:"kind"`
		Namespace string `json:"namespace"`
		Name      string `json:"name"`
		UID       string `json:"uid"`
	} `json:"involved_object"`
}

// eventsEvidence parses the events evidence item, if any.
func eventsEvidence(evidence []domain.Evidence) ([]eventView, domain.Evidence, bool) {
	for _, ev := range evidence {
		if ev.Kind != "events" || len(ev.Content) == 0 {
			continue
		}
		var views []eventView
		if err := json.Unmarshal(ev.Content, &views); err != nil {
			continue
		}
		return views, ev, true
	}
	return nil, domain.Evidence{}, false
}

// anyEvent reports whether any event matches the predicate.
func anyEvent(views []eventView, match func(eventView) bool) bool {
	for _, v := range views {
		if match(v) {
			return true
		}
	}
	return false
}

// firstEventMessage returns the message of the first matching event.
func firstEventMessage(views []eventView, match func(eventView) bool) (string, bool) {
	for _, v := range views {
		if match(v) {
			return v.Message, true
		}
	}
	return "", false
}

// containerStatus finds a container's status by name across regular
// and init containers.
func containerStatus(pod *domain.PodView, name string) (domain.ContainerStatus, bool) {
	for _, cs := range pod.AllContainerStatuses() {
		if cs.Name == name {
			return cs, true
		}
	}
	return domain.ContainerStatus{}, false
}

// terminatedOf returns the current or last terminated state.
func terminatedOf(cs domain.ContainerStatus) *domain.ContainerTerminated {
	if t := cs.State.Terminated; t != nil {
		return t
	}
	return cs.LastTerminationState.Terminated
}

// waitingReason returns the container's waiting reason, if waiting.
func waitingReason(cs domain.ContainerStatus) (string, bool) {
	if cs.State.Waiting != nil && cs.State.Waiting.Reason != "" {
		return cs.State.Waiting.Reason, true
	}
	return "", false
}

// checks builds read-only kubectl verification commands for the
// incident's pod.
func checks(input analyzer.Input, lines ...string) []string {
	name := input.Incident.Resource.Name
	namespace := input.Incident.Resource.Namespace
	out := make([]string, 0, len(lines))
	for _, l := range lines {
		out = append(out, fmt.Sprintf(l, name, namespace))
	}
	return out
}
