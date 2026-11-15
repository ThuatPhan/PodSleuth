// Package analyzer evaluates collected evidence with deterministic
// rules and produces evidence-linked hypotheses. It never fetches
// Kubernetes resources, never touches storage, and never invents
// evidence.
package analyzer

import (
	"context"
	"fmt"
	"math"
	"sort"

	"github.com/ThuatPhan/PodSleuth/internal/domain"
)

// Scoring model: a transparent heuristic ranking score, not a
// calibrated probability. Direct status/termination evidence in the
// Pod object ranks higher than Event-only conclusions; corroborating
// logs or Events increase the score. Temporal-only correlation is not
// used by MVP rules.
const (
	// ScoreDirectStatus is the base score when the Pod object itself
	// shows the failure condition.
	ScoreDirectStatus = 0.7
	// ScoreEventOnly is the base score when only Events support the
	// hypothesis.
	ScoreEventOnly = 0.5
	// BoostLogs is added when container logs corroborate the rule.
	BoostLogs = 0.1
	// BoostEvents is added when Events corroborate a status-based rule.
	BoostEvents = 0.1
	// MaxScore caps every hypothesis score.
	MaxScore = 1.0
)

// CapScore bounds a score to MaxScore and rounds to two decimals so
// persisted scores are stable and free of floating-point noise.
func CapScore(score float64) float64 {
	if score > MaxScore {
		return MaxScore
	}
	return math.Round(score*100) / 100
}

// Input is one analysis job: an incident and its collected evidence.
type Input struct {
	Incident domain.Incident
	Evidence []domain.Evidence
}

// Result is the analysis output.
type Result struct {
	Hypotheses []domain.Hypothesis
	Summary    string
	// Coverage lists the rule IDs that were evaluated.
	Coverage []string
}

// Analyzer evaluates evidence for an incident.
type Analyzer interface {
	Analyze(ctx context.Context, input Input) (Result, error)
}

// Rule is one deterministic RCA rule. Evaluate returns nil when the
// rule's required evidence is absent; it must never invent evidence.
type Rule interface {
	ID() string
	Evaluate(ctx context.Context, input Input) (*domain.Hypothesis, error)
}

// Engine evaluates registered rules and ranks their hypotheses.
type Engine struct {
	rules []Rule
}

// NewEngine returns an Engine over the given rules. A rule ID may be
// registered at most once.
func NewEngine(rules ...Rule) (*Engine, error) {
	seen := make(map[string]bool, len(rules))
	for _, r := range rules {
		if r == nil {
			return nil, fmt.Errorf("analyzer: nil rule")
		}
		if seen[r.ID()] {
			return nil, fmt.Errorf("analyzer: rule %s registered twice", r.ID())
		}
		seen[r.ID()] = true
	}
	return &Engine{rules: append([]Rule(nil), rules...)}, nil
}

// Analyze implements Analyzer. Hypotheses are ranked by score
// (strongest first) with the rule ID as deterministic tie-breaker.
func (e *Engine) Analyze(ctx context.Context, input Input) (Result, error) {
	if input.Incident.ID == "" {
		return Result{}, fmt.Errorf("analyzer: incident id is required")
	}
	known := make(map[string]bool, len(input.Evidence))
	for _, ev := range input.Evidence {
		known[ev.ID] = true
	}

	var out []domain.Hypothesis
	fired := make(map[string]bool, len(e.rules))
	for _, r := range e.rules {
		h, err := r.Evaluate(ctx, input)
		if err != nil {
			return Result{}, fmt.Errorf("analyzer: rule %s: %w", r.ID(), err)
		}
		if h == nil {
			continue
		}
		if fired[h.RuleID] {
			continue
		}
		fired[h.RuleID] = true
		if h.IncidentID == "" {
			h.IncidentID = input.Incident.ID
		}
		if h.IncidentID != input.Incident.ID {
			return Result{}, fmt.Errorf("analyzer: rule %s produced a hypothesis for incident %q, want %q", r.ID(), h.IncidentID, input.Incident.ID)
		}
		if h.RuleID == "" {
			h.RuleID = r.ID()
		}
		if len(h.EvidenceIDs) == 0 {
			return Result{}, fmt.Errorf("analyzer: rule %s produced a hypothesis without evidence", h.RuleID)
		}
		for _, id := range h.EvidenceIDs {
			if !known[id] {
				return Result{}, fmt.Errorf("analyzer: rule %s cites unknown evidence %q", h.RuleID, id)
			}
		}
		h.ID = input.Incident.ID + ":" + h.RuleID
		h.Score = CapScore(h.Score)
		out = append(out, *h)
	}

	sort.SliceStable(out, func(i, j int) bool {
		if out[i].Score != out[j].Score {
			return out[i].Score > out[j].Score
		}
		return out[i].RuleID < out[j].RuleID
	})

	coverage := make([]string, 0, len(e.rules))
	for _, r := range e.rules {
		coverage = append(coverage, r.ID())
	}
	sort.Strings(coverage)

	return Result{
		Hypotheses: out,
		Summary:    summary(input, out),
		Coverage:   coverage,
	}, nil
}

func summary(input Input, hypotheses []domain.Hypothesis) string {
	if len(hypotheses) == 0 {
		return fmt.Sprintf("No rule matched the %d collected evidence item(s); the root cause remains unknown.", len(input.Evidence))
	}
	strongest := hypotheses[0]
	return fmt.Sprintf("%d hypothesis/hypotheses from %d rules over %d evidence item(s); strongest: %s (%s)",
		len(hypotheses), len(hypotheses), len(input.Evidence), strongest.RuleID, strongest.Summary)
}
