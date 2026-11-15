// Package detector evaluates observations for failure conditions. It
// is pure over its inputs: no Kubernetes API calls, no evidence
// collection, no persistence.
package detector

import (
	"context"
	"fmt"
	"strconv"

	"github.com/ThuatPhan/PodSleuth/internal/domain"
)

// Failure types detected from Pod status.
const (
	TypeCrashLoopBackOff           = "CrashLoopBackOff"
	TypeOOMKilled                  = "OOMKilled"
	TypeNonZeroExit                = "NonZeroExit"
	TypeImagePullBackOff           = "ImagePullBackOff"
	TypeCreateContainerConfigError = "CreateContainerConfigError"
	TypeFailedScheduling           = "FailedScheduling"
	TypeEvicted                    = "Evicted"
)

// Kubernetes status reasons and conditions used by the predicates.
const (
	reasonCrashLoopBackOff           = "CrashLoopBackOff"
	reasonImagePullBackOff           = "ImagePullBackOff"
	reasonErrImagePull               = "ErrImagePull"
	reasonCreateContainerConfigError = "CreateContainerConfigError"
	reasonOOMKilled                  = "OOMKilled"
	reasonEvicted                    = "Evicted"
	conditionPodScheduled            = "PodScheduled"
	conditionFalse                   = "False"
	phasePending                     = "Pending"
)

// maxAttributeMessage bounds untrusted cluster-provided messages in
// signal attributes.
const maxAttributeMessage = 512

// Detector evaluates observations and returns failure signals.
type Detector interface {
	Evaluate(ctx context.Context, obs domain.Observation) ([]domain.FailureSignal, error)
}

// PodDetector detects Pod failure conditions from watcher observations.
type PodDetector struct {
	clusterID string
}

// NewPodDetector returns a PodDetector for the given cluster.
func NewPodDetector(clusterID string) *PodDetector {
	return &PodDetector{clusterID: clusterID}
}

// Evaluate inspects the Pod payload of the observation. Unparsable
// payloads return an error; Pods without failure conditions return no
// signals.
func (d *PodDetector) Evaluate(_ context.Context, obs domain.Observation) ([]domain.FailureSignal, error) {
	pod, err := domain.ParsePodView(obs.Payload)
	if err != nil {
		return nil, fmt.Errorf("detector: observation for %s/%s: %w", obs.Resource.Namespace, obs.Resource.Name, err)
	}

	var signals []domain.FailureSignal
	if sig, ok := d.evictedSignal(obs, pod); ok {
		signals = append(signals, sig)
	}
	if sig, ok := d.schedulingSignal(obs, pod); ok {
		signals = append(signals, sig)
	}
	for _, cs := range pod.AllContainerStatuses() {
		if sig, ok := d.containerSignal(obs, pod, cs); ok {
			signals = append(signals, sig)
		}
	}
	return signals, nil
}

// containerSignal emits at most one signal per container. CrashLoopBackOff
// takes precedence over stale termination evidence: a container in
// backoff has a previous termination, and firing both would split one
// episode into two incidents.
func (d *PodDetector) containerSignal(obs domain.Observation, pod *domain.PodView, cs domain.ContainerStatus) (domain.FailureSignal, bool) {
	switch {
	case cs.State.Waiting != nil && cs.State.Waiting.Reason == reasonCrashLoopBackOff:
		return d.containerFailure(obs, pod, cs, TypeCrashLoopBackOff, reasonCrashLoopBackOff), true
	case cs.State.Waiting != nil && (cs.State.Waiting.Reason == reasonImagePullBackOff || cs.State.Waiting.Reason == reasonErrImagePull):
		return d.containerFailure(obs, pod, cs, TypeImagePullBackOff, cs.State.Waiting.Reason), true
	case cs.State.Waiting != nil && cs.State.Waiting.Reason == reasonCreateContainerConfigError:
		return d.containerFailure(obs, pod, cs, TypeCreateContainerConfigError, cs.State.Waiting.Reason), true
	case cs.State.Terminated != nil && cs.State.Terminated.Reason == reasonOOMKilled:
		return d.containerFailure(obs, pod, cs, TypeOOMKilled, reasonOOMKilled), true
	case cs.LastTerminationState.Terminated != nil && cs.LastTerminationState.Terminated.Reason == reasonOOMKilled:
		return d.containerFailure(obs, pod, cs, TypeOOMKilled, reasonOOMKilled), true
	case cs.State.Terminated != nil && cs.State.Terminated.ExitCode != 0 && cs.State.Terminated.Reason != reasonEvicted:
		return d.containerFailure(obs, pod, cs, TypeNonZeroExit, cs.State.Terminated.Reason), true
	case cs.LastTerminationState.Terminated != nil &&
		cs.LastTerminationState.Terminated.ExitCode != 0 &&
		cs.LastTerminationState.Terminated.Reason != reasonOOMKilled &&
		cs.LastTerminationState.Terminated.Reason != reasonEvicted:
		return d.containerFailure(obs, pod, cs, TypeNonZeroExit, cs.LastTerminationState.Terminated.Reason), true
	}
	return domain.FailureSignal{}, false
}

// evictedSignal detects kubelet eviction via the Pod status reason.
func (d *PodDetector) evictedSignal(obs domain.Observation, pod *domain.PodView) (domain.FailureSignal, bool) {
	if pod.Status.Reason != reasonEvicted {
		return domain.FailureSignal{}, false
	}
	sig := d.podFailure(obs, pod, "", TypeEvicted, reasonEvicted)
	if pod.Status.Message != "" {
		sig.Attributes["message"] = truncate(pod.Status.Message)
	}
	return sig, true
}

// schedulingSignal detects unschedulable Pods. The spec's signal is a
// FailedScheduling scheduler Event, which the detector must not fetch;
// the PodScheduled=False condition carries the same information.
func (d *PodDetector) schedulingSignal(obs domain.Observation, pod *domain.PodView) (domain.FailureSignal, bool) {
	if pod.Status.Phase != phasePending {
		return domain.FailureSignal{}, false
	}
	for _, c := range pod.Status.Conditions {
		if c.Type == conditionPodScheduled && c.Status == conditionFalse {
			sig := d.podFailure(obs, pod, "", TypeFailedScheduling, c.Reason)
			if c.Message != "" {
				sig.Attributes["message"] = truncate(c.Message)
			}
			return sig, true
		}
	}
	return domain.FailureSignal{}, false
}

func (d *PodDetector) containerFailure(obs domain.Observation, pod *domain.PodView, cs domain.ContainerStatus, failureType, reason string) domain.FailureSignal {
	sig := d.podFailure(obs, pod, cs.Name, failureType, reason)
	sig.Attributes["container"] = cs.Name
	sig.Attributes["restart_count"] = strconv.FormatInt(int64(cs.RestartCount), 10)
	if t := terminatedOf(cs); t != nil {
		sig.Attributes["exit_code"] = strconv.FormatInt(int64(t.ExitCode), 10)
	}
	if w := cs.State.Waiting; w != nil && w.Message != "" {
		sig.Attributes["message"] = truncate(w.Message)
	}
	return sig
}

func (d *PodDetector) podFailure(obs domain.Observation, pod *domain.PodView, container, failureType, reason string) domain.FailureSignal {
	return domain.FailureSignal{
		Type:        failureType,
		Reason:      reason,
		Fingerprint: fingerprint(d.clusterID, pod, container, failureType),
		Resource:    obs.Resource,
		ObservedAt:  obs.ObservedAt,
		Attributes:  map[string]string{"reason": reason},
	}
}

func terminatedOf(cs domain.ContainerStatus) *domain.ContainerTerminated {
	if t := cs.State.Terminated; t != nil {
		return t
	}
	return cs.LastTerminationState.Terminated
}

func truncate(s string) string {
	if len(s) <= maxAttributeMessage {
		return s
	}
	return s[:maxAttributeMessage]
}
