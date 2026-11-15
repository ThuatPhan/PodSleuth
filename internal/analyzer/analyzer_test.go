package analyzer

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/ThuatPhan/PodSleuth/internal/domain"
)

// fakeRule is a controllable Rule for engine tests.
type fakeRule struct {
	id  string
	h   *domain.Hypothesis
	err error
}

func (r *fakeRule) ID() string { return r.id }

func (r *fakeRule) Evaluate(_ context.Context, input Input) (*domain.Hypothesis, error) {
	if r.err != nil {
		return nil, r.err
	}
	if r.h == nil {
		return nil, nil
	}
	h := *r.h
	return &h, nil
}

func engineInput() Input {
	return Input{
		Incident: domain.Incident{ID: "inc-1", FailureType: "CrashLoopBackOff"},
		Evidence: []domain.Evidence{{ID: "ev-1", Kind: "pod", IncidentID: "inc-1"}},
	}
}

func TestEngineStampsAndValidates(t *testing.T) {
	rule := &fakeRule{id: "X-001", h: &domain.Hypothesis{
		RuleID:      "X-001",
		Summary:     "test",
		Score:       0.7,
		EvidenceIDs: []string{"ev-1"},
	}}
	e, err := NewEngine(rule)
	if err != nil {
		t.Fatal(err)
	}
	res, err := e.Analyze(context.Background(), engineInput())
	if err != nil {
		t.Fatalf("Analyze() error = %v", err)
	}
	if len(res.Hypotheses) != 1 {
		t.Fatalf("hypotheses = %d, want 1", len(res.Hypotheses))
	}
	h := res.Hypotheses[0]
	if h.ID != "inc-1:X-001" {
		t.Errorf("ID = %q, want inc-1:X-001", h.ID)
	}
	if h.IncidentID != "inc-1" {
		t.Errorf("IncidentID = %q, want inc-1", h.IncidentID)
	}
	if h.Score != 0.7 {
		t.Errorf("Score = %v, want 0.7", h.Score)
	}
	if len(res.Coverage) != 1 || res.Coverage[0] != "X-001" {
		t.Errorf("Coverage = %v, want [X-001]", res.Coverage)
	}
}

func TestEngineRejectsInvalidHypotheses(t *testing.T) {
	tests := []struct {
		name    string
		rule    Rule
		wantErr string
	}{
		{
			name:    "no evidence cited",
			rule:    &fakeRule{id: "X-001", h: &domain.Hypothesis{RuleID: "X-001", Summary: "s", EvidenceIDs: nil}},
			wantErr: "without evidence",
		},
		{
			name:    "unknown evidence cited",
			rule:    &fakeRule{id: "X-001", h: &domain.Hypothesis{RuleID: "X-001", Summary: "s", EvidenceIDs: []string{"ev-missing"}}},
			wantErr: "unknown evidence",
		},
		{
			name:    "wrong incident",
			rule:    &fakeRule{id: "X-001", h: &domain.Hypothesis{RuleID: "X-001", IncidentID: "other", Summary: "s", EvidenceIDs: []string{"ev-1"}}},
			wantErr: "produced a hypothesis for incident",
		},
		{
			name:    "rule error propagates",
			rule:    &fakeRule{id: "X-001", err: errors.New("boom")},
			wantErr: "boom",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e, err := NewEngine(tt.rule)
			if err != nil {
				t.Fatal(err)
			}
			_, err = e.Analyze(context.Background(), engineInput())
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Errorf("Analyze() error = %v, want containing %q", err, tt.wantErr)
			}
		})
	}
}

func TestEngineNewEngineValidation(t *testing.T) {
	if _, err := NewEngine(nil); err == nil {
		t.Error("nil rule: error = nil, want error")
	}
	dup := &fakeRule{id: "X-001"}
	if _, err := NewEngine(dup, dup); err == nil {
		t.Error("duplicate rule: error = nil, want error")
	}
}

func TestEngineRankingDeterministic(t *testing.T) {
	strong := &fakeRule{id: "Z-001", h: &domain.Hypothesis{RuleID: "Z-001", Summary: "strong", Score: 0.8, EvidenceIDs: []string{"ev-1"}}}
	weak := &fakeRule{id: "A-001", h: &domain.Hypothesis{RuleID: "A-001", Summary: "weak", Score: 0.5, EvidenceIDs: []string{"ev-1"}}}
	tie1 := &fakeRule{id: "M-001", h: &domain.Hypothesis{RuleID: "M-001", Summary: "tie m", Score: 0.7, EvidenceIDs: []string{"ev-1"}}}
	tie2 := &fakeRule{id: "B-001", h: &domain.Hypothesis{RuleID: "B-001", Summary: "tie b", Score: 0.7, EvidenceIDs: []string{"ev-1"}}}

	e, err := NewEngine(strong, weak, tie1, tie2)
	if err != nil {
		t.Fatal(err)
	}
	res, err := e.Analyze(context.Background(), engineInput())
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"Z-001", "B-001", "M-001", "A-001"}
	got := make([]string, 0, len(res.Hypotheses))
	for _, h := range res.Hypotheses {
		got = append(got, h.RuleID)
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("ranking = %v, want %v (score desc, rule ID tie-break)", got, want)
	}

	// Identical input must yield identical output.
	again, err := e.Analyze(context.Background(), engineInput())
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(res, again) {
		t.Error("Analyze() is not deterministic across repeated runs")
	}
}

func TestEngineNoMatch(t *testing.T) {
	e, err := NewEngine(&fakeRule{id: "X-001"})
	if err != nil {
		t.Fatal(err)
	}
	res, err := e.Analyze(context.Background(), engineInput())
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Hypotheses) != 0 {
		t.Errorf("hypotheses = %d, want 0", len(res.Hypotheses))
	}
	if !strings.Contains(res.Summary, "root cause remains unknown") {
		t.Errorf("summary = %q, want unknown-cause statement", res.Summary)
	}
	if len(res.Coverage) != 1 || res.Coverage[0] != "X-001" {
		t.Errorf("coverage = %v, want [X-001]", res.Coverage)
	}
}

func TestEngineRequiresIncidentID(t *testing.T) {
	e, err := NewEngine(&fakeRule{id: "X-001"})
	if err != nil {
		t.Fatal(err)
	}
	input := engineInput()
	input.Incident.ID = ""
	if _, err := e.Analyze(context.Background(), input); err == nil {
		t.Error("missing incident id: error = nil, want error")
	}
}

func TestEngineDuplicateRuleFiresOnce(t *testing.T) {
	// Two rules with the same RuleID in their hypotheses: only the
	// first hypothesis is kept.
	a := &fakeRule{id: "R-1", h: &domain.Hypothesis{RuleID: "SAME", Summary: "first", Score: 0.7, EvidenceIDs: []string{"ev-1"}}}
	b := &fakeRule{id: "R-2", h: &domain.Hypothesis{RuleID: "SAME", Summary: "second", Score: 0.9, EvidenceIDs: []string{"ev-1"}}}
	e, err := NewEngine(a, b)
	if err != nil {
		t.Fatal(err)
	}
	res, err := e.Analyze(context.Background(), engineInput())
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Hypotheses) != 1 || res.Hypotheses[0].Summary != "first" {
		t.Errorf("hypotheses = %+v, want only the first", res.Hypotheses)
	}
}
