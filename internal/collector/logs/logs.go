// Package logs collects container log evidence.
package logs

import (
	"context"
	"fmt"
	"io"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"github.com/ThuatPhan/PodSleuth/internal/collector"
	"github.com/ThuatPhan/PodSleuth/internal/detector"
	"github.com/ThuatPhan/PodSleuth/internal/domain"
	"github.com/ThuatPhan/PodSleuth/internal/evidence"
)

// Evidence kinds and provenance produced by this collector.
const (
	KindPrevious = "previous-logs"
	KindCurrent  = "current-logs"
	Source       = "api-server"
)

// Options controls which log variants are collected.
type Options struct {
	PreviousLogs bool
	CurrentLogs  bool
}

// LogsCollector reads current and previous container logs for the
// incident's containers.
type LogsCollector struct {
	client kubernetes.Interface
	logs   Options
	opts   evidence.Options
}

// NewLogsCollector returns a LogsCollector for the given client.
func NewLogsCollector(client kubernetes.Interface, logOpts Options, evOpts evidence.Options) *LogsCollector {
	return &LogsCollector{client: client, logs: logOpts, opts: evOpts}
}

// Name implements Collector.
func (c *LogsCollector) Name() string { return "logs" }

// Supports implements Collector. Pods whose containers never started
// (scheduling, image pull, config errors) have no logs to collect.
func (c *LogsCollector) Supports(signal domain.FailureSignal) bool {
	switch signal.Type {
	case detector.TypeFailedScheduling, detector.TypeImagePullBackOff, detector.TypeCreateContainerConfigError:
		return false
	}
	return true
}

// Collect implements Collector. Per-container failures become
// warnings; one failing container never discards another's logs.
func (c *LogsCollector) Collect(ctx context.Context, req collector.Request) (collector.Result, error) {
	if err := ctx.Err(); err != nil {
		return collector.Result{}, err
	}
	res := req.Incident.Resource
	pod, err := c.client.CoreV1().Pods(res.Namespace).Get(ctx, res.Name, metav1.GetOptions{})
	if err != nil {
		return terminalOrRetryable("pod get for logs", res, err)
	}

	var result collector.Result
	for _, container := range targetContainers(req.Signal, pod) {
		if ctx.Err() != nil {
			return result, ctx.Err()
		}
		if c.logs.PreviousLogs {
			if err := c.collectOne(ctx, req, pod, container, KindPrevious, true, &result); err != nil {
				return result, err
			}
		}
		if c.logs.CurrentLogs {
			if err := c.collectOne(ctx, req, pod, container, KindCurrent, false, &result); err != nil {
				return result, err
			}
		}
	}
	return result, nil
}

func (c *LogsCollector) collectOne(ctx context.Context, req collector.Request, pod *corev1.Pod, container, kind string, previous bool, result *collector.Result) error {
	stream, err := c.client.CoreV1().Pods(pod.Namespace).GetLogs(pod.Name, &corev1.PodLogOptions{
		Container: container,
		Previous:  previous,
	}).Stream(ctx)
	if err != nil {
		result.Warnings = append(result.Warnings, logWarning(kind, pod, container, err))
		return nil
	}
	defer stream.Close()

	data, err := io.ReadAll(io.LimitReader(stream, int64(c.opts.MaxLogBytesPerContainer)+1))
	if err != nil {
		if collector.ClassifyError(err) == collector.ErrKindCanceled {
			return err
		}
		result.Warnings = append(result.Warnings, logWarning(kind, pod, container, err))
		return nil
	}

	ev, warnings, err := evidence.Normalize(c.opts, evidence.Input{
		Kind:       kind,
		Source:     Source,
		IncidentID: req.Incident.ID,
		Resource:   req.Incident.Resource,
		ObservedAt: time.Now().UTC(),
		Text:       string(data),
	})
	if err != nil {
		result.Warnings = append(result.Warnings, fmt.Sprintf("%s logs for pod %s/%s container %s dropped: %v", kind, pod.Namespace, pod.Name, container, err))
		return nil
	}
	result.Evidence = append(result.Evidence, ev)
	result.Warnings = append(result.Warnings, warnings...)
	return nil
}

func logWarning(kind string, pod *corev1.Pod, container string, err error) string {
	if apierrors.IsBadRequest(err) && strings.Contains(err.Error(), "previous terminated container") {
		return fmt.Sprintf("no previous logs for pod %s/%s container %s: container has not terminated", pod.Namespace, pod.Name, container)
	}
	return fmt.Sprintf("%s logs for pod %s/%s container %s unavailable (%s): %v",
		kind, pod.Namespace, pod.Name, container, collector.ClassifyError(err), err)
}

// targetContainers returns the containers to collect logs for: the
// signal's container when known, otherwise every regular container.
func targetContainers(signal domain.FailureSignal, pod *corev1.Pod) []string {
	if name := signal.Attributes["container"]; name != "" {
		return []string{name}
	}
	names := make([]string, 0, len(pod.Status.ContainerStatuses))
	for _, cs := range pod.Status.ContainerStatuses {
		names = append(names, cs.Name)
	}
	return names
}

// terminalOrRetryable maps an API error to a Result. Terminal errors
// (not found, forbidden) become warnings; transient errors also return
// an error so the caller can retry.
func terminalOrRetryable(op string, res domain.ResourceRef, err error) (collector.Result, error) {
	kind := collector.ClassifyError(err)
	warning := fmt.Sprintf("%s for pod %s/%s failed (%s): %v", op, res.Namespace, res.Name, kind, err)
	switch kind {
	case collector.ErrKindNotFound, collector.ErrKindForbidden, collector.ErrKindUnauthorized:
		return collector.Result{Warnings: []string{warning}}, nil
	case collector.ErrKindCanceled:
		return collector.Result{Warnings: []string{warning}}, err
	default:
		return collector.Result{Warnings: []string{warning}}, fmt.Errorf("%s: %s", op, warning)
	}
}
