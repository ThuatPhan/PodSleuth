package report

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

var reportTime = time.Date(2026, 11, 15, 12, 0, 0, 0, time.UTC)

func reportIncident() domain.Incident {
	resolved := reportTime.Add(5 * time.Minute)
	return domain.Incident{
		ID:          "inc-1",
		ClusterID:   "test-cluster",
		Fingerprint: "fp-1",
		FailureType: "CrashLoopBackOff",
		Severity:    domain.DefaultSeverity,
		Status:      domain.StatusResolved,
		Resource: domain.ResourceRef{
			ClusterID:  "test-cluster",
			APIVersion: "v1",
			Kind:       "Pod",
			Namespace:  "app",
			Name:       "web-1",
			UID:        "pod-uid-1",
		},
		WorkloadKind:  "ReplicaSet",
		WorkloadName:  "web-7d9f",
		WorkloadUID:   "rs-uid-1",
		ContainerName: "app",
		FirstSeenAt:   reportTime,
		LastSeenAt:    resolved,
		ResolvedAt:    &resolved,
		CreatedAt:     reportTime,
		UpdatedAt:     resolved,
		Summary:       "CrashLoopBackOff on pod app/web-1 container app",
	}
}

func reportEvidence() []domain.Evidence {
	return []domain.Evidence{
		{
			ID:               "ev-1",
			IncidentID:       "inc-1",
			Kind:             "pod",
			Source:           "api-server",
			Resource:         domain.ResourceRef{Kind: "Pod", Namespace: "app", Name: "web-1", UID: "pod-uid-1"},
			ObservedAt:       reportTime,
			Content:          []byte(`{"metadata":{"name":"web-1"}}`),
			ContentHash:      "hash-pod",
			CollectionStatus: "success",
		},
		{
			ID:               "ev-2",
			IncidentID:       "inc-1",
			Kind:             "previous-logs",
			Source:           "api-server",
			ObservedAt:       reportTime.Add(time.Second),
			Text:             "panic: runtime error\nBearer [REDACTED]\n",
			ContentHash:      "hash-logs",
			Redacted:         true,
			CollectionStatus: "success",
		},
	}
}

func reportHypotheses() []domain.Hypothesis {
	return []domain.Hypothesis{
		{
			ID: "inc-1:OOM-001", IncidentID: "inc-1", RuleID: "OOM-001",
			Summary:         "Container app was terminated by the OOM killer (exit code 137).",
			Score:           0.8,
			EvidenceIDs:     []string{"ev-1", "ev-2"},
			MissingEvidence: []string{"memory usage metrics (not collected in MVP)"},
			SuggestedChecks: []string{"kubectl describe pod web-1 -n app"},
		},
		{
			ID: "inc-1:CRASH-001", IncidentID: "inc-1", RuleID: "CRASH-001",
			Summary:     "Container app is in CrashLoopBackOff after 5 restarts.",
			Score:       0.7,
			EvidenceIDs: []string{"ev-2"},
		},
	}
}

func reportTimeline() []domain.TimelineEvent {
	return []domain.TimelineEvent{
		{ID: "tl-1", IncidentID: "inc-1", EventType: "detected", OccurredAt: reportTime, Message: "CrashLoopBackOff detected"},
		{ID: "tl-2", IncidentID: "inc-1", EventType: "signal", OccurredAt: reportTime.Add(time.Minute), Message: "restart observed", Attributes: map[string]string{"restart": "6"}},
	}
}

func reportRuns() []domain.CollectionRun {
	completed := reportTime.Add(2 * time.Second)
	return []domain.CollectionRun{
		{ID: "run-1", IncidentID: "inc-1", CollectorName: "logs", Status: "success", StartedAt: reportTime, CompletedAt: &completed},
	}
}

func fullData() Data {
	return Data{
		Incident:       reportIncident(),
		Evidence:       reportEvidence(),
		Hypotheses:     reportHypotheses(),
		Timeline:       reportTimeline(),
		CollectionRuns: reportRuns(),
	}
}

func TestParseFormat(t *testing.T) {
	tests := []struct {
		in    string
		want  Format
		error bool
	}{
		{in: "markdown", want: FormatMarkdown},
		{in: "json", want: FormatJSON},
		{in: "", want: FormatMarkdown},
		{in: "html", error: true},
		{in: "JSON", error: true},
	}
	for _, tt := range tests {
		got, err := ParseFormat(tt.in)
		if tt.error {
			if err == nil {
				t.Errorf("ParseFormat(%q) error = nil, want error", tt.in)
			}
			continue
		}
		if err != nil {
			t.Errorf("ParseFormat(%q) error = %v", tt.in, err)
		}
		if got != tt.want {
			t.Errorf("ParseFormat(%q) = %q, want %q", tt.in, got, tt.want)
		}
	}
}

func TestMarkdownReport(t *testing.T) {
	out, err := Render(fullData(), FormatMarkdown)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"# PodSleuth Investigation Report",
		"## Incident (observed facts)",
		"- ID: inc-1",
		"- Status: resolved",
		"- Failure type: CrashLoopBackOff",
		"- Resource: Pod app/web-1 (uid pod-uid-1)",
		"- Container: app",
		"- Workload: ReplicaSet/web-7d9f (uid rs-uid-1)",
		"- First seen: 2026-11-15T12:00:00Z",
		"- Resolved: 2026-11-15T12:05:00Z",
		"## Hypotheses (ranked)",
		"not confirmed causes",
		"### 1. OOM-001 — score 0.80",
		"### 2. CRASH-001 — score 0.70",
		"- Supporting evidence: ev-1, ev-2",
		"- Missing evidence: memory usage metrics (not collected in MVP)",
		"- Suggested checks (read-only):",
		"  - `kubectl describe pod web-1 -n app`",
		"## Timeline",
		"- 2026-11-15T12:00:00Z detected: CrashLoopBackOff detected",
		"- 2026-11-15T12:01:00Z signal: restart observed",
		"## Evidence",
		"### ev-1 — pod (api-server)",
		"- Collection status: success",
		"- Content hash: hash-pod",
		"### ev-2 — previous-logs (api-server)",
		"- Redacted: yes",
		"panic: runtime error",
		"## Collection runs",
		"- logs: success (started 2026-11-15T12:00:00Z, completed 2026-11-15T12:00:02Z)",
		"## Gaps and unknowns",
		"Missing evidence (OOM-001): memory usage metrics (not collected in MVP)",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("markdown report missing %q", want)
		}
	}
}

func TestMarkdownReportEmptyIncident(t *testing.T) {
	data := Data{Incident: reportIncident()}
	out, err := Render(data, FormatMarkdown)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"_No analysis has been persisted for this incident; the root cause remains unknown._",
		"_No timeline events recorded._",
		"_No evidence collected._",
		"No RCA analysis has been persisted; the root cause remains unknown.",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("markdown report missing %q", want)
		}
	}
}

func TestMarkdownReportStable(t *testing.T) {
	first, err := Render(fullData(), FormatMarkdown)
	if err != nil {
		t.Fatal(err)
	}
	second, err := Render(fullData(), FormatMarkdown)
	if err != nil {
		t.Fatal(err)
	}
	if first != second {
		t.Error("markdown output is not stable across repeated renders")
	}
}

func TestJSONReport(t *testing.T) {
	out, err := Render(fullData(), FormatJSON)
	if err != nil {
		t.Fatal(err)
	}
	var doc Document
	if err := json.Unmarshal([]byte(out), &doc); err != nil {
		t.Fatalf("output is not valid JSON: %v\n%s", err, out)
	}
	if doc.Incident.ID != "inc-1" {
		t.Errorf("incident id = %q", doc.Incident.ID)
	}
	if len(doc.Hypotheses) != 2 || doc.Hypotheses[0].RuleID != "OOM-001" {
		t.Errorf("hypotheses = %+v", doc.Hypotheses)
	}
	if len(doc.Hypotheses[0].EvidenceIDs) != 2 {
		t.Errorf("evidence links = %v", doc.Hypotheses[0].EvidenceIDs)
	}
	if len(doc.Evidence) != 2 || len(doc.Timeline) != 2 || len(doc.CollectionRuns) != 1 {
		t.Errorf("document = %+v", doc)
	}
	if strings.Contains(out, "\x1b") {
		t.Error("JSON output contains ANSI escape codes")
	}
}

func TestJSONReportStableAndSlicesNotNull(t *testing.T) {
	first, err := Render(fullData(), FormatJSON)
	if err != nil {
		t.Fatal(err)
	}
	second, err := Render(fullData(), FormatJSON)
	if err != nil {
		t.Fatal(err)
	}
	if first != second {
		t.Error("JSON output is not stable across repeated renders")
	}

	empty := Data{Incident: reportIncident()}
	out, err := Render(empty, FormatJSON)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{`"hypotheses": []`, `"evidence": []`, `"timeline": []`, `"collection_runs": []`} {
		if !strings.Contains(out, want) {
			t.Errorf("JSON output missing %q (null slices are not allowed):\n%s", want, out)
		}
	}
}

func TestRenderUnsupportedFormat(t *testing.T) {
	if _, err := Render(fullData(), Format("html")); err == nil {
		t.Error("Render(html) error = nil, want error")
	}
}

func TestGather(t *testing.T) {
	store := &fakeStore{data: fullData()}
	got, err := Gather(context.Background(), store, "inc-1")
	if err != nil {
		t.Fatal(err)
	}
	if got.Incident.ID != "inc-1" || len(got.Evidence) != 2 || len(got.Hypotheses) != 2 {
		t.Errorf("gathered = %+v", got)
	}
}

func TestGatherRequiresIncidentID(t *testing.T) {
	store := &fakeStore{}
	if _, err := Gather(context.Background(), store, ""); err == nil {
		t.Error("Gather(\"\") error = nil, want error")
	}
}

func TestGatherUnknownIncident(t *testing.T) {
	store := &fakeStore{failOn: "GetIncident"}
	_, err := Gather(context.Background(), store, "missing")
	if err == nil || !errors.Is(err, storage.ErrNotFound) {
		t.Errorf("Gather(missing) error = %v, want storage.ErrNotFound", err)
	}
}

func TestGatherStorageErrors(t *testing.T) {
	tests := []struct {
		name    string
		failing string
		wantIn  string
	}{
		{"evidence", "ListEvidence", "evidence"},
		{"analysis", "GetAnalysis", "analysis"},
		{"timeline", "ListTimeline", "timeline"},
		{"runs", "ListCollectionRuns", "collection runs"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := &fakeStore{failOn: tt.failing}
			_, err := Gather(context.Background(), store, "inc-1")
			if err == nil {
				t.Fatalf("Gather() error = nil, want error from %s", tt.failing)
			}
			if !strings.Contains(err.Error(), tt.wantIn) {
				t.Errorf("error = %v, want context naming %q", err, tt.wantIn)
			}
		})
	}
}

func TestHasPartialCollection(t *testing.T) {
	data := fullData()
	if HasPartialCollection(data) {
		t.Error("HasPartialCollection(full) = true, want false")
	}
	data.Evidence = append(data.Evidence, domain.Evidence{
		ID: "ev-3", IncidentID: "inc-1", Kind: "current-logs",
		Text: "truncated", ContentHash: "h", CollectionStatus: "truncated",
	})
	if !HasPartialCollection(data) {
		t.Error("HasPartialCollection(with truncated) = false, want true")
	}
}

// fakeStore is a controllable storage.Store for Gather tests.
type fakeStore struct {
	data   Data
	failOn string
}

func (s *fakeStore) EnsureCluster(context.Context, string, string) error { return nil }

func (s *fakeStore) CreateIncident(context.Context, domain.Incident) error { return nil }

func (s *fakeStore) GetIncident(_ context.Context, id string) (domain.Incident, error) {
	if s.failOn == "GetIncident" {
		return domain.Incident{}, storage.ErrNotFound
	}
	return s.data.Incident, nil
}

func (s *fakeStore) UpdateIncident(context.Context, domain.Incident) error { return nil }

func (s *fakeStore) ListIncidents(context.Context, storage.IncidentFilter) ([]domain.Incident, error) {
	return nil, nil
}

func (s *fakeStore) AddEvidence(context.Context, []domain.Evidence) error { return nil }

func (s *fakeStore) ListEvidence(_ context.Context, _ string) ([]domain.Evidence, error) {
	if s.failOn == "ListEvidence" {
		return nil, errors.New("boom")
	}
	return s.data.Evidence, nil
}

func (s *fakeStore) SaveAnalysis(context.Context, string, []domain.Hypothesis) error { return nil }

func (s *fakeStore) GetAnalysis(_ context.Context, _ string) ([]domain.Hypothesis, error) {
	if s.failOn == "GetAnalysis" {
		return nil, errors.New("boom")
	}
	return s.data.Hypotheses, nil
}

func (s *fakeStore) AppendTimeline(context.Context, []domain.TimelineEvent) error { return nil }

func (s *fakeStore) ListTimeline(_ context.Context, _ string) ([]domain.TimelineEvent, error) {
	if s.failOn == "ListTimeline" {
		return nil, errors.New("boom")
	}
	return s.data.Timeline, nil
}

func (s *fakeStore) CreateCollectionRun(context.Context, domain.CollectionRun) error { return nil }

func (s *fakeStore) ListCollectionRuns(_ context.Context, _ string) ([]domain.CollectionRun, error) {
	if s.failOn == "ListCollectionRuns" {
		return nil, errors.New("boom")
	}
	return s.data.CollectionRuns, nil
}

func (s *fakeStore) Close() error { return nil }
