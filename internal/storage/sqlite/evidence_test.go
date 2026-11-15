package sqlite

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/ThuatPhan/PodSleuth/internal/domain"
	"github.com/ThuatPhan/PodSleuth/internal/storage"
)

func evidenceItem(id, incidentID string) domain.Evidence {
	observed := time.Date(2026, 11, 15, 12, 1, 0, 0, time.UTC)
	eventTime := observed.Add(-30 * time.Second)
	content, _ := json.Marshal(map[string]string{"phase": "Running"})
	return domain.Evidence{
		ID:               id,
		IncidentID:       incidentID,
		Kind:             "pod",
		Source:           "api-server",
		Resource:         domain.ResourceRef{Kind: "Pod", Namespace: "app", Name: "web-1", UID: "pod-uid-1"},
		ObservedAt:       observed,
		EventTime:        &eventTime,
		Content:          content,
		Text:             "",
		ContentHash:      "hash-" + id,
		Redacted:         true,
		CollectionStatus: evidence_StatusSuccess,
	}
}

// evidence_StatusSuccess mirrors the evidence package constant without
// importing it (the storage layer stays independent of collectors).
const evidence_StatusSuccess = "success"

func TestEvidenceRoundTrip(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	if err := s.CreateIncident(ctx, baseIncident("inc-1")); err != nil {
		t.Fatal(err)
	}

	in := evidenceItem("ev-1", "inc-1")
	if err := s.AddEvidence(ctx, []domain.Evidence{in}); err != nil {
		t.Fatalf("AddEvidence() error = %v", err)
	}

	got, err := s.ListEvidence(ctx, "inc-1")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 {
		t.Fatalf("evidence count = %d, want 1", len(got))
	}
	e := got[0]
	if e.ID != "ev-1" || e.IncidentID != "inc-1" || e.Kind != "pod" || e.Source != "api-server" {
		t.Errorf("identity = %+v", e)
	}
	if e.Resource != in.Resource {
		t.Errorf("Resource = %+v, want %+v", e.Resource, in.Resource)
	}
	if !e.ObservedAt.Equal(in.ObservedAt) {
		t.Errorf("ObservedAt = %v, want %v", e.ObservedAt, in.ObservedAt)
	}
	if e.EventTime == nil || !e.EventTime.Equal(*in.EventTime) {
		t.Errorf("EventTime = %v, want %v", e.EventTime, in.EventTime)
	}
	if e.ContentHash != in.ContentHash || !e.Redacted || e.CollectionStatus != "success" {
		t.Errorf("content fields = %+v", e)
	}
	var content map[string]string
	if err := json.Unmarshal(e.Content, &content); err != nil {
		t.Fatalf("content is not valid JSON: %v", err)
	}
	if content["phase"] != "Running" {
		t.Errorf("content = %v", content)
	}
}

func TestEvidenceTextOnly(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	if err := s.CreateIncident(ctx, baseIncident("inc-1")); err != nil {
		t.Fatal(err)
	}

	in := evidenceItem("ev-2", "inc-1")
	in.Kind = "previous-logs"
	in.Content = nil
	in.Text = "previous log line\n"
	in.Redacted = false
	if err := s.AddEvidence(ctx, []domain.Evidence{in}); err != nil {
		t.Fatal(err)
	}

	got, err := s.ListEvidence(ctx, "inc-1")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 {
		t.Fatalf("evidence count = %d, want 1", len(got))
	}
	if got[0].ID != "ev-2" {
		t.Fatalf("evidence = %+v, want ev-2", got[0])
	}
	if got[0].Text != "previous log line\n" {
		t.Errorf("text = %q", got[0].Text)
	}
	if got[0].Content != nil {
		t.Errorf("content = %s, want empty", got[0].Content)
	}
}

func TestEvidenceForeignKey(t *testing.T) {
	s := openTestStore(t)
	err := s.AddEvidence(context.Background(), []domain.Evidence{evidenceItem("ev-x", "missing-incident")})
	if err == nil {
		t.Fatal("AddEvidence() error = nil, want foreign key error")
	}
	if strings.Contains(err.Error(), "referenced record is missing") == false {
		t.Errorf("error = %v, want foreign key context", err)
	}
}

func TestEvidenceBatchAtomic(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	if err := s.CreateIncident(ctx, baseIncident("inc-1")); err != nil {
		t.Fatal(err)
	}

	batch := []domain.Evidence{
		evidenceItem("ev-ok-1", "inc-1"),
		evidenceItem("ev-ok-2", "inc-1"),
		evidenceItem("ev-bad", "missing-incident"), // FK violation rolls back the batch
	}
	err := s.AddEvidence(ctx, batch)
	if err == nil {
		t.Fatal("AddEvidence() error = nil, want foreign key error")
	}

	got, err := s.ListEvidence(ctx, "inc-1")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Errorf("evidence count after rollback = %d, want 0 (batch must be atomic)", len(got))
	}
}

func TestEvidenceDuplicateID(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	if err := s.CreateIncident(ctx, baseIncident("inc-1")); err != nil {
		t.Fatal(err)
	}
	ev := evidenceItem("ev-1", "inc-1")
	if err := s.AddEvidence(ctx, []domain.Evidence{ev}); err != nil {
		t.Fatal(err)
	}
	err := s.AddEvidence(ctx, []domain.Evidence{ev})
	if err == nil || !errors.Is(err, storage.ErrAlreadyExists) {
		t.Errorf("duplicate evidence error = %v, want storage.ErrAlreadyExists", err)
	}
}

func TestAnalysisRoundTrip(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	if err := s.CreateIncident(ctx, baseIncident("inc-1")); err != nil {
		t.Fatal(err)
	}
	if err := s.AddEvidence(ctx, []domain.Evidence{
		evidenceItem("ev-1", "inc-1"),
		evidenceItem("ev-2", "inc-1"),
	}); err != nil {
		t.Fatal(err)
	}

	hypotheses := []domain.Hypothesis{
		{
			ID: "hyp-1", IncidentID: "inc-1", RuleID: "OOM-001",
			Summary: "Memory kill observed", Score: 0.9,
			EvidenceIDs:     []string{"ev-1", "ev-2"},
			MissingEvidence: []string{"memory metrics"},
			SuggestedChecks: []string{"kubectl describe pod web-1"},
		},
		{
			ID: "hyp-2", IncidentID: "inc-1", RuleID: "CRASH-001",
			Summary: "Process exited unexpectedly", Score: 0.5,
			EvidenceIDs: []string{"ev-2"},
		},
	}
	if err := s.SaveAnalysis(ctx, "inc-1", hypotheses); err != nil {
		t.Fatalf("SaveAnalysis() error = %v", err)
	}

	got, err := s.GetAnalysis(ctx, "inc-1")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("hypotheses = %d, want 2", len(got))
	}
	if got[0].ID != "hyp-1" || got[1].ID != "hyp-2" {
		t.Errorf("rank order = %v, want [hyp-1 hyp-2]", hypothesisIDs(got))
	}
	if got[0].Score != 0.9 || got[0].RuleID != "OOM-001" {
		t.Errorf("hypothesis[0] = %+v", got[0])
	}
	if len(got[0].EvidenceIDs) != 2 || got[0].EvidenceIDs[0] != "ev-1" || got[0].EvidenceIDs[1] != "ev-2" {
		t.Errorf("evidence ids = %v", got[0].EvidenceIDs)
	}
	if len(got[0].MissingEvidence) != 1 || got[0].MissingEvidence[0] != "memory metrics" {
		t.Errorf("missing evidence = %v", got[0].MissingEvidence)
	}
	if len(got[0].SuggestedChecks) != 1 {
		t.Errorf("suggested checks = %v", got[0].SuggestedChecks)
	}
	if got[1].MissingEvidence != nil {
		t.Errorf("empty missing evidence = %v, want nil", got[1].MissingEvidence)
	}
}

func TestAnalysisReplace(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	if err := s.CreateIncident(ctx, baseIncident("inc-1")); err != nil {
		t.Fatal(err)
	}
	if err := s.AddEvidence(ctx, []domain.Evidence{evidenceItem("ev-1", "inc-1")}); err != nil {
		t.Fatal(err)
	}

	first := []domain.Hypothesis{{ID: "hyp-1", IncidentID: "inc-1", RuleID: "OOM-001", Summary: "first", Score: 0.9, EvidenceIDs: []string{"ev-1"}}}
	if err := s.SaveAnalysis(ctx, "inc-1", first); err != nil {
		t.Fatal(err)
	}
	second := []domain.Hypothesis{{ID: "hyp-2", IncidentID: "inc-1", RuleID: "CRASH-001", Summary: "second", Score: 0.4, EvidenceIDs: []string{"ev-1"}}}
	if err := s.SaveAnalysis(ctx, "inc-1", second); err != nil {
		t.Fatal(err)
	}

	got, err := s.GetAnalysis(ctx, "inc-1")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].ID != "hyp-2" {
		t.Errorf("analysis after replace = %v, want [hyp-2]", hypothesisIDs(got))
	}
}

func TestAnalysisForeignKeyRollback(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	if err := s.CreateIncident(ctx, baseIncident("inc-1")); err != nil {
		t.Fatal(err)
	}

	// The second hypothesis links unknown evidence: the whole save must
	// roll back, leaving no hypotheses behind.
	hypotheses := []domain.Hypothesis{
		{ID: "hyp-1", IncidentID: "inc-1", RuleID: "OOM-001", Summary: "ok", Score: 0.9, EvidenceIDs: []string{"ev-1"}},
		{ID: "hyp-2", IncidentID: "inc-1", RuleID: "CRASH-001", Summary: "bad link", Score: 0.5, EvidenceIDs: []string{"ev-missing"}},
	}
	err := s.SaveAnalysis(ctx, "inc-1", hypotheses)
	if err == nil {
		t.Fatal("SaveAnalysis() error = nil, want foreign key error")
	}
	got, err := s.GetAnalysis(ctx, "inc-1")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Errorf("hypotheses after rollback = %v, want none", hypothesisIDs(got))
	}
}

func TestTimelineRoundTrip(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	if err := s.CreateIncident(ctx, baseIncident("inc-1")); err != nil {
		t.Fatal(err)
	}
	if err := s.AddEvidence(ctx, []domain.Evidence{evidenceItem("ev-1", "inc-1")}); err != nil {
		t.Fatal(err)
	}

	base := time.Date(2026, 11, 15, 12, 0, 0, 0, time.UTC)
	events := []domain.TimelineEvent{
		{ID: "tl-2", IncidentID: "inc-1", EventType: "signal", OccurredAt: base.Add(time.Minute), Message: "restart observed", Attributes: map[string]string{"restart": "6"}},
		{ID: "tl-1", IncidentID: "inc-1", EventType: "detected", OccurredAt: base, SourceEvidenceID: "ev-1", Message: "CrashLoopBackOff detected"},
	}
	if err := s.AppendTimeline(ctx, events); err != nil {
		t.Fatalf("AppendTimeline() error = %v", err)
	}

	got, err := s.ListTimeline(ctx, "inc-1")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("timeline = %d events, want 2", len(got))
	}
	if got[0].ID != "tl-1" || got[1].ID != "tl-2" {
		t.Errorf("order = %v, want chronological [tl-1 tl-2]", timelineIDs(got))
	}
	if got[0].SourceEvidenceID != "ev-1" {
		t.Errorf("source evidence = %q, want ev-1", got[0].SourceEvidenceID)
	}
	if got[0].Attributes != nil {
		t.Errorf("attributes = %v, want nil", got[0].Attributes)
	}
	if got[1].Attributes["restart"] != "6" {
		t.Errorf("attributes = %v", got[1].Attributes)
	}
}

func TestTimelineForeignKey(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	if err := s.CreateIncident(ctx, baseIncident("inc-1")); err != nil {
		t.Fatal(err)
	}
	// source_evidence_id references evidence; unknown ID must fail.
	err := s.AppendTimeline(ctx, []domain.TimelineEvent{{
		ID: "tl-x", IncidentID: "inc-1", EventType: "signal",
		OccurredAt: time.Now(), SourceEvidenceID: "ev-missing", Message: "m",
	}})
	if err == nil {
		t.Fatal("AppendTimeline() error = nil, want foreign key error")
	}
}

func TestCollectionRunRoundTrip(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	if err := s.CreateIncident(ctx, baseIncident("inc-1")); err != nil {
		t.Fatal(err)
	}

	started := time.Date(2026, 11, 15, 12, 1, 0, 0, time.UTC)
	completed := started.Add(2 * time.Second)
	run := domain.CollectionRun{
		ID: "run-1", IncidentID: "inc-1", CollectorName: "logs", Status: "success",
		StartedAt: started, CompletedAt: &completed,
	}
	if err := s.CreateCollectionRun(ctx, run); err != nil {
		t.Fatalf("CreateCollectionRun() error = %v", err)
	}

	got, err := s.ListCollectionRuns(ctx, "inc-1")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 {
		t.Fatalf("runs = %d, want 1", len(got))
	}
	if got[0].ID != "run-1" || got[0].CollectorName != "logs" || got[0].Status != "success" {
		t.Errorf("run = %+v", got[0])
	}
	if got[0].CompletedAt == nil || !got[0].CompletedAt.Equal(completed) {
		t.Errorf("CompletedAt = %v, want %v", got[0].CompletedAt, completed)
	}
}

func TestCollectionRunNoCompletion(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	if err := s.CreateIncident(ctx, baseIncident("inc-1")); err != nil {
		t.Fatal(err)
	}
	run := domain.CollectionRun{
		ID: "run-2", IncidentID: "inc-1", CollectorName: "events", Status: "running",
		StartedAt: time.Date(2026, 11, 15, 12, 2, 0, 0, time.UTC),
	}
	if err := s.CreateCollectionRun(ctx, run); err != nil {
		t.Fatal(err)
	}
	got, err := s.ListCollectionRuns(ctx, "inc-1")
	if err != nil {
		t.Fatal(err)
	}
	if got[0].CompletedAt != nil {
		t.Errorf("CompletedAt = %v, want nil", got[0].CompletedAt)
	}
}

func TestCollectionRunValidation(t *testing.T) {
	s := openTestStore(t)
	run := domain.CollectionRun{ID: "run-x", IncidentID: "inc-1", Status: "success", StartedAt: time.Now()}
	if err := s.CreateCollectionRun(context.Background(), run); err == nil {
		t.Error("missing collector name: error = nil, want error")
	}
}

func hypothesisIDs(in []domain.Hypothesis) []string {
	out := make([]string, 0, len(in))
	for _, h := range in {
		out = append(out, h.ID)
	}
	return out
}

func timelineIDs(in []domain.TimelineEvent) []string {
	out := make([]string, 0, len(in))
	for _, e := range in {
		out = append(out, e.ID)
	}
	return out
}
