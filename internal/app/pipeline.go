// Package app wires PodSleuth components into runnable pipelines.
package app

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/ThuatPhan/PodSleuth/internal/detector"
	"github.com/ThuatPhan/PodSleuth/internal/domain"
	"github.com/ThuatPhan/PodSleuth/internal/incident"
)

// Pipeline consumes watcher observations, detects failures and tracks
// incidents. It implements watcher.Handler.
type Pipeline struct {
	detector  detector.Detector
	incidents *incident.Manager
}

// NewPipeline returns a Pipeline for the given detector and manager.
func NewPipeline(det detector.Detector, incidents *incident.Manager) *Pipeline {
	return &Pipeline{detector: det, incidents: incidents}
}

// OnObservation detects failures in the observation and records them.
func (p *Pipeline) OnObservation(ctx context.Context, obs domain.Observation) error {
	signals, err := p.detector.Evaluate(ctx, obs)
	if err != nil {
		return fmt.Errorf("detection failed for %s/%s: %w", obs.Resource.Namespace, obs.Resource.Name, err)
	}
	return p.incidents.Process(ctx, obs, signals)
}

// OnWatchError logs watch errors; the watcher keeps running.
func (p *Pipeline) OnWatchError(_ context.Context, err error) {
	slog.Error("podsleuth: watch error", "error", err)
}
