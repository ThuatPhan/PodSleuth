// Package collector defines the evidence collection contract and
// registry. Implementations live in subpackages and read one evidence
// domain each; they must not infer root causes.
package collector

import (
	"context"
	"fmt"
	"time"

	"github.com/ThuatPhan/PodSleuth/internal/domain"
)

// Request describes one collection job.
type Request struct {
	Incident    domain.Incident
	Signal      domain.FailureSignal
	WindowStart time.Time
	WindowEnd   time.Time
}

// Result carries collected evidence and non-fatal warnings. Partial
// failures surface as warnings; they never discard other evidence.
type Result struct {
	Evidence []domain.Evidence
	Warnings []string
}

// Collector reads one evidence domain.
type Collector interface {
	Name() string
	Supports(signal domain.FailureSignal) bool
	Collect(ctx context.Context, req Request) (Result, error)
}

// Registry selects collectors for a signal.
type Registry interface {
	ForSignal(signal domain.FailureSignal) []Collector
}

// APIErrorResult maps a Kubernetes API error to a Result. The warning
// carries the classified failure and the error is always returned so
// the caller can record a failed run and decide on retries using
// ClassifyError.
func APIErrorResult(op string, res domain.ResourceRef, err error) (Result, error) {
	kind := ClassifyError(err)
	warning := fmt.Sprintf("%s for pod %s/%s failed (%s): %v", op, res.Namespace, res.Name, kind, err)
	return Result{Warnings: []string{warning}}, fmt.Errorf("%s: %w", op, err)
}

type registry struct {
	collectors []Collector
}

// NewRegistry returns a Registry over the given collectors, in
// registration order.
func NewRegistry(collectors ...Collector) Registry {
	return &registry{collectors: append([]Collector(nil), collectors...)}
}

// ForSignal implements Registry.
func (r *registry) ForSignal(signal domain.FailureSignal) []Collector {
	var out []Collector
	for _, c := range r.collectors {
		if c.Supports(signal) {
			out = append(out, c)
		}
	}
	return out
}
