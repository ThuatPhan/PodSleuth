package report

import (
	"context"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/ThuatPhan/PodSleuth/internal/domain"
	"github.com/ThuatPhan/PodSleuth/internal/storage"
	sqlitestore "github.com/ThuatPhan/PodSleuth/internal/storage/sqlite"
)

// TestEndToEndReportValidation walks the full persistence-to-report
// path: temporary SQLite database, schema initialization, seeded
// cluster/incident/evidence/hypotheses/timeline, close and reopen, and
// both report formats rendered from the reopened store.
func TestEndToEndReportValidation(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "podsleuth.db")

	// 1-2. Temporary database with initialized schema.
	store, err := sqlitestore.Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}

	// 3. Sample cluster.
	if err := store.EnsureCluster(ctx, "kind-podsleuth", "kind-podsleuth"); err != nil {
		t.Fatalf("EnsureCluster() error = %v", err)
	}

	// 4. Sample incident.
	firstSeen := time.Date(2026, 11, 15, 12, 0, 0, 0, time.UTC)
	lastSeen := firstSeen.Add(3 * time.Minute)
	incident := domain.Incident{
		ID:          "e2e-incident-1",
		ClusterID:   "kind-podsleuth",
		Fingerprint: "fp-e2e-1",
		FailureType: "CrashLoopBackOff",
		Severity:    domain.DefaultSeverity,
		Status:      domain.StatusDetected,
		Resource: domain.ResourceRef{
			ClusterID:  "kind-podsleuth",
			APIVersion: "v1",
			Kind:       "Pod",
			Namespace:  "default",
			Name:       "crasher-7f9d",
			UID:        "pod-uid-e2e-1",
		},
		WorkloadKind:  "ReplicaSet",
		WorkloadName:  "crasher-7f9d",
		WorkloadUID:   "rs-uid-e2e-1",
		ContainerName: "app",
		FirstSeenAt:   firstSeen,
		LastSeenAt:    lastSeen,
		CreatedAt:     firstSeen,
		UpdatedAt:     lastSeen,
		Summary:       "CrashLoopBackOff on pod default/crasher-7f9d container app",
	}
	if err := store.CreateIncident(ctx, incident); err != nil {
		t.Fatalf("CreateIncident() error = %v", err)
	}

	// 5. Associated evidence.
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "crasher-7f9d", UID: types.UID("pod-uid-e2e-1")},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			ContainerStatuses: []corev1.ContainerStatus{{
				Name:                 "app",
				RestartCount:         5,
				State:                corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "CrashLoopBackOff"}},
				LastTerminationState: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 1, Reason: "Error"}},
			}},
		},
	}
	podJSON, err := json.Marshal(pod)
	if err != nil {
		t.Fatal(err)
	}
	evidenceItems := []domain.Evidence{
		{
			ID:               "ev-pod",
			IncidentID:       incident.ID,
			Kind:             "pod",
			Source:           "api-server",
			Resource:         domain.ResourceRef{Kind: "Pod", Namespace: "default", Name: "crasher-7f9d", UID: "pod-uid-e2e-1"},
			ObservedAt:       firstSeen.Add(time.Minute),
			Content:          podJSON,
			ContentHash:      "hash-pod",
			CollectionStatus: "success",
		},
		{
			ID:               "ev-logs",
			IncidentID:       incident.ID,
			Kind:             "previous-logs",
			Source:           "api-server",
			ObservedAt:       firstSeen.Add(time.Minute),
			Text:             "panic: runtime error: invalid memory address\nBearer [REDACTED]\n",
			ContentHash:      "hash-logs",
			Redacted:         true,
			CollectionStatus: "success",
		},
		{
			ID:               "ev-events",
			IncidentID:       incident.ID,
			Kind:             "events",
			Source:           "api-server",
			ObservedAt:       firstSeen.Add(time.Minute),
			Content:          []byte(`[{"type":"Warning","reason":"BackOff","message":"back-off restarting failed container","count":5}]`),
			ContentHash:      "hash-events",
			CollectionStatus: "success",
		},
	}
	if err := store.AddEvidence(ctx, evidenceItems); err != nil {
		t.Fatalf("AddEvidence() error = %v", err)
	}

	// 6-7. Two hypotheses with different scores, linked to evidence.
	hypotheses := []domain.Hypothesis{
		{
			ID:          incident.ID + ":CRASH-001",
			IncidentID:  incident.ID,
			RuleID:      "CRASH-001",
			Summary:     "Container app is in CrashLoopBackOff after 5 restarts: the process repeatedly exits or fails to start.",
			Score:       0.8,
			EvidenceIDs: []string{"ev-pod", "ev-logs", "ev-events"},
			MissingEvidence: []string{
				"application startup logs beyond the last termination",
			},
			SuggestedChecks: []string{
				"kubectl logs crasher-7f9d -n default --previous",
			},
		},
		{
			ID:          incident.ID + ":PROBE-001",
			IncidentID:  incident.ID,
			RuleID:      "PROBE-001",
			Summary:     "The liveness probe is failing; the probe configuration and the application's health disagree.",
			Score:       0.5,
			EvidenceIDs: []string{"ev-events"},
		},
	}
	if err := store.SaveAnalysis(ctx, incident.ID, hypotheses); err != nil {
		t.Fatalf("SaveAnalysis() error = %v", err)
	}

	// 8. Timeline events, inserted out of chronological order on
	// purpose: storage must return them chronologically.
	timeline := []domain.TimelineEvent{
		{
			ID: "tl-3", IncidentID: incident.ID, EventType: "collected",
			OccurredAt: firstSeen.Add(2 * time.Minute), Message: "Collected 3 evidence item(s)",
		},
		{
			ID: "tl-1", IncidentID: incident.ID, EventType: "detected",
			OccurredAt: firstSeen, SourceEvidenceID: "ev-pod", Message: "CrashLoopBackOff detected on container app",
		},
		{
			ID: "tl-2", IncidentID: incident.ID, EventType: "signal",
			OccurredAt: firstSeen.Add(time.Minute), Message: "restart count advanced to 5",
			Attributes: map[string]string{"restart": "5"},
		},
	}
	if err := store.AppendTimeline(ctx, timeline); err != nil {
		t.Fatalf("AppendTimeline() error = %v", err)
	}

	// 9. Close and reopen: the report must come from persisted state.
	if err := store.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	reopened, err := sqlitestore.Open(path)
	if err != nil {
		t.Fatalf("reopen error = %v", err)
	}
	defer reopened.Close()

	// 10-11. Both report formats from the reopened database.
	data, err := Gather(ctx, reopened, incident.ID)
	if err != nil {
		t.Fatalf("Gather() error = %v", err)
	}
	markdown, err := Render(data, FormatMarkdown)
	if err != nil {
		t.Fatalf("Render(markdown) error = %v", err)
	}
	jsonOut, err := Render(data, FormatJSON)
	if err != nil {
		t.Fatalf("Render(json) error = %v", err)
	}

	validateIncidentDetails(t, data, markdown, jsonOut)
	validateEvidenceAssociation(t, data, markdown)
	validateRanking(t, data, markdown, jsonOut)
	validateScores(t, data)
	validateEvidenceReferences(t, data)
	validateTimelineChronology(t, data, markdown)
	validateMissingEvidence(t, markdown, jsonOut)
	validateJSONStability(t, data, jsonOut)
	validateStability(t, reopened, incident.ID, markdown, jsonOut)
}

func validateIncidentDetails(t *testing.T, data Data, markdown, jsonOut string) {
	t.Helper()
	for _, want := range []string{
		"- ID: e2e-incident-1",
		"- Cluster: kind-podsleuth",
		"- Status: detected",
		"- Failure type: CrashLoopBackOff",
		"- Resource: Pod default/crasher-7f9d (uid pod-uid-e2e-1)",
		"- Container: app",
		"- Workload: ReplicaSet/crasher-7f9d (uid rs-uid-e2e-1)",
		"- First seen: 2026-11-15T12:00:00Z",
		"- Last seen: 2026-11-15T12:03:00Z",
	} {
		if !strings.Contains(markdown, want) {
			t.Errorf("markdown missing incident detail %q", want)
		}
	}
	if !strings.Contains(jsonOut, `"id": "e2e-incident-1"`) ||
		!strings.Contains(jsonOut, `"failure_type": "CrashLoopBackOff"`) {
		t.Errorf("json missing incident details:\n%s", jsonOut)
	}
	if data.Incident.ID != "e2e-incident-1" || data.Incident.ClusterID != "kind-podsleuth" {
		t.Errorf("gathered incident = %+v", data.Incident)
	}
}

func validateEvidenceAssociation(t *testing.T, data Data, markdown string) {
	t.Helper()
	if len(data.Evidence) != 3 {
		t.Fatalf("evidence count = %d, want 3", len(data.Evidence))
	}
	for _, ev := range data.Evidence {
		if ev.IncidentID != "e2e-incident-1" {
			t.Errorf("evidence %s IncidentID = %q, want e2e-incident-1", ev.ID, ev.IncidentID)
		}
	}
	for _, id := range []string{"ev-pod", "ev-logs", "ev-events"} {
		if !strings.Contains(markdown, "### "+id+" — ") {
			t.Errorf("markdown missing evidence section for %q", id)
		}
	}
	if !strings.Contains(markdown, "panic: runtime error: invalid memory address") {
		t.Error("markdown missing log evidence text")
	}
	if !strings.Contains(markdown, "- Redacted: yes") {
		t.Error("markdown missing redaction flag for log evidence")
	}
}

func validateRanking(t *testing.T, data Data, markdown, jsonOut string) {
	t.Helper()
	if len(data.Hypotheses) != 2 {
		t.Fatalf("hypotheses = %d, want 2", len(data.Hypotheses))
	}
	if data.Hypotheses[0].RuleID != "CRASH-001" || data.Hypotheses[1].RuleID != "PROBE-001" {
		t.Errorf("ranking = %q, %q; want CRASH-001 (0.8) before PROBE-001 (0.5)",
			data.Hypotheses[0].RuleID, data.Hypotheses[1].RuleID)
	}
	crashPos := strings.Index(markdown, "### 1. CRASH-001 — score 0.80")
	probePos := strings.Index(markdown, "### 2. PROBE-001 — score 0.50")
	if crashPos < 0 || probePos < 0 || crashPos > probePos {
		t.Errorf("markdown ranking wrong: crash@%d probe@%d", crashPos, probePos)
	}
	if !strings.Contains(jsonOut, `"rule_id": "CRASH-001"`) || !strings.Contains(jsonOut, `"rule_id": "PROBE-001"`) {
		t.Error("json missing both hypotheses")
	}
}

func validateScores(t *testing.T, data Data) {
	t.Helper()
	if data.Hypotheses[0].Score != 0.8 {
		t.Errorf("CRASH-001 score = %v, want 0.8", data.Hypotheses[0].Score)
	}
	if data.Hypotheses[1].Score != 0.5 {
		t.Errorf("PROBE-001 score = %v, want 0.5", data.Hypotheses[1].Score)
	}
}

func validateEvidenceReferences(t *testing.T, data Data) {
	t.Helper()
	want := map[string][]string{
		// Links are returned sorted by evidence ID (deterministic).
		"CRASH-001": {"ev-events", "ev-logs", "ev-pod"},
		"PROBE-001": {"ev-events"},
	}
	for _, h := range data.Hypotheses {
		ids, ok := want[h.RuleID]
		if !ok {
			t.Fatalf("unexpected rule %q", h.RuleID)
		}
		if len(h.EvidenceIDs) != len(ids) {
			t.Errorf("rule %s evidence = %v, want %v", h.RuleID, h.EvidenceIDs, ids)
			continue
		}
		for i, id := range ids {
			if h.EvidenceIDs[i] != id {
				t.Errorf("rule %s evidence[%d] = %q, want %q", h.RuleID, i, h.EvidenceIDs[i], id)
			}
		}
	}
}

func validateTimelineChronology(t *testing.T, data Data, markdown string) {
	t.Helper()
	if len(data.Timeline) != 3 {
		t.Fatalf("timeline = %d events, want 3", len(data.Timeline))
	}
	wantOrder := []string{"tl-1", "tl-2", "tl-3"}
	for i, id := range wantOrder {
		if data.Timeline[i].ID != id {
			t.Errorf("timeline[%d] = %q, want %q (chronological)", i, data.Timeline[i].ID, id)
		}
	}
	// The markdown must list them in the same chronological order even
	// though they were inserted out of order.
	pos1 := strings.Index(markdown, "12:00:00Z detected")
	pos2 := strings.Index(markdown, "12:01:00Z signal")
	pos3 := strings.Index(markdown, "12:02:00Z collected")
	if pos1 < 0 || pos2 < 0 || pos3 < 0 || !(pos1 < pos2 && pos2 < pos3) {
		t.Errorf("markdown timeline not chronological: %d, %d, %d", pos1, pos2, pos3)
	}
	if !strings.Contains(markdown, "restart count advanced to 5") {
		t.Error("markdown missing timeline message")
	}
}

func validateMissingEvidence(t *testing.T, markdown, jsonOut string) {
	t.Helper()
	if !strings.Contains(markdown, "Missing evidence (CRASH-001): application startup logs beyond the last termination") {
		t.Errorf("markdown missing-evidence gap not rendered:\n%s", markdown)
	}
	if !strings.Contains(jsonOut, `"missing_evidence"`) ||
		!strings.Contains(jsonOut, "application startup logs beyond the last termination") {
		t.Error("json missing_evidence not preserved")
	}
}

func validateJSONStability(t *testing.T, data Data, jsonOut string) {
	t.Helper()
	var doc Document
	if err := json.Unmarshal([]byte(jsonOut), &doc); err != nil {
		t.Fatalf("json output is not parseable: %v", err)
	}
	if doc.Incident.ID != "e2e-incident-1" {
		t.Errorf("parsed incident id = %q", doc.Incident.ID)
	}
	if len(doc.Hypotheses) != 2 || len(doc.Evidence) != 3 || len(doc.Timeline) != 3 {
		t.Errorf("parsed document counts = %d/%d/%d, want 2/3/3",
			len(doc.Hypotheses), len(doc.Evidence), len(doc.Timeline))
	}
	// Evidence references must survive the JSON round trip (sorted by
	// evidence ID, per the storage contract).
	if len(doc.Hypotheses[0].EvidenceIDs) != 3 || doc.Hypotheses[0].EvidenceIDs[0] != "ev-events" {
		t.Errorf("parsed evidence links = %v", doc.Hypotheses[0].EvidenceIDs)
	}
}

func validateStability(t *testing.T, store storage.Store, incidentID, markdown, jsonOut string) {
	t.Helper()
	again, err := Gather(context.Background(), store, incidentID)
	if err != nil {
		t.Fatal(err)
	}
	md2, err := Render(again, FormatMarkdown)
	if err != nil {
		t.Fatal(err)
	}
	js2, err := Render(again, FormatJSON)
	if err != nil {
		t.Fatal(err)
	}
	if md2 != markdown {
		t.Error("markdown output is not stable across repeated gathers")
	}
	if js2 != jsonOut {
		t.Error("json output is not stable across repeated gathers")
	}
}
