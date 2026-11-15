// Package pod collects Pod state evidence.
package pod

import (
	"context"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"github.com/ThuatPhan/PodSleuth/internal/collector"
	"github.com/ThuatPhan/PodSleuth/internal/domain"
	"github.com/ThuatPhan/PodSleuth/internal/evidence"
)

// Evidence kind and provenance produced by this collector.
const (
	Kind   = "pod"
	Source = "api-server"
)

// PodCollector reads the current Pod object for an incident.
type PodCollector struct {
	client kubernetes.Interface
	opts   evidence.Options
}

// NewPodCollector returns a PodCollector for the given client.
func NewPodCollector(client kubernetes.Interface, opts evidence.Options) *PodCollector {
	return &PodCollector{client: client, opts: opts}
}

// Name implements Collector.
func (c *PodCollector) Name() string { return "pod" }

// Supports implements Collector. Every failure signal benefits from
// Pod state.
func (c *PodCollector) Supports(domain.FailureSignal) bool { return true }

// Collect implements Collector. A missing Pod after detection is a
// normal race and surfaces as a warning; Forbidden is persistent and
// also surfaces as a warning. Transient failures return an error so
// the caller can retry.
func (c *PodCollector) Collect(ctx context.Context, req collector.Request) (collector.Result, error) {
	if err := ctx.Err(); err != nil {
		return collector.Result{}, err
	}
	res := req.Incident.Resource
	pod, err := c.client.CoreV1().Pods(res.Namespace).Get(ctx, res.Name, metav1.GetOptions{})
	if err != nil {
		return terminalOrRetryable("pod get", res, err)
	}

	sanitized, redacted := sanitizePod(pod)
	ev, warnings, err := evidence.Normalize(c.opts, evidence.Input{
		Kind:       Kind,
		Source:     Source,
		IncidentID: req.Incident.ID,
		Resource:   res,
		ObservedAt: time.Now().UTC(),
		Content:    sanitized,
		Redacted:   redacted,
	})
	if err != nil {
		return collector.Result{}, fmt.Errorf("pod collector: %w", err)
	}
	return collector.Result{Evidence: []domain.Evidence{ev}, Warnings: warnings}, nil
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

// sanitizePod redacts credential-bearing env values and annotation
// values. Secret references are kept: only values are sensitive, and
// Secret contents are never fetched. It reports whether anything was
// redacted.
func sanitizePod(pod *corev1.Pod) (*corev1.Pod, bool) {
	out := pod.DeepCopy()
	redacted := false
	for i := range out.Spec.Containers {
		if redactEnv(&out.Spec.Containers[i]) {
			redacted = true
		}
	}
	for i := range out.Spec.InitContainers {
		if redactEnv(&out.Spec.InitContainers[i]) {
			redacted = true
		}
	}
	for k, v := range out.Annotations {
		if r, changed := evidence.RedactText(v); changed {
			out.Annotations[k] = r
			redacted = true
		}
	}
	return out, redacted
}

func redactEnv(c *corev1.Container) bool {
	changed := false
	for i, e := range c.Env {
		if e.Value != "" && evidence.SensitiveKeyName(e.Name) {
			c.Env[i].Value = "[REDACTED]"
			changed = true
		}
	}
	return changed
}
