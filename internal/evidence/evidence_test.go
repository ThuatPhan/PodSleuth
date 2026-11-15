package evidence

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/ThuatPhan/PodSleuth/internal/domain"
)

var obsTime = time.Date(2026, 11, 15, 12, 0, 0, 0, time.UTC)

func input() Input {
	return Input{
		Kind:       "pod",
		Source:     "api-server",
		IncidentID: "inc-1",
		Resource:   domain.ResourceRef{ClusterID: "c1", Kind: "Pod", Namespace: "ns", Name: "p", UID: "u1"},
		ObservedAt: obsTime,
		Content:    map[string]string{"phase": "Running"},
	}
}

func TestNormalizeContent(t *testing.T) {
	ev, warnings, err := Normalize(DefaultOptions(), input())
	if err != nil {
		t.Fatalf("Normalize() error = %v", err)
	}
	if len(warnings) != 0 {
		t.Errorf("warnings = %v, want none", warnings)
	}
	if ev.ID == "" {
		t.Error("ID is empty")
	}
	if ev.IncidentID != "inc-1" || ev.Kind != "pod" || ev.Source != "api-server" {
		t.Errorf("identity fields = %+v", ev)
	}
	if ev.Resource.UID != "u1" {
		t.Errorf("Resource = %+v", ev.Resource)
	}
	if !ev.ObservedAt.Equal(obsTime) || ev.ObservedAt.Location() != time.UTC {
		t.Errorf("ObservedAt = %v, want UTC %v", ev.ObservedAt, obsTime)
	}
	if ev.CollectionStatus != StatusSuccess {
		t.Errorf("CollectionStatus = %q, want success", ev.CollectionStatus)
	}
	if ev.Redacted {
		t.Error("Redacted = true, want false")
	}

	var content map[string]string
	if err := json.Unmarshal(ev.Content, &content); err != nil {
		t.Fatalf("content is not valid JSON: %v", err)
	}
	if content["phase"] != "Running" {
		t.Errorf("content = %v", content)
	}
}

func TestNormalizeHashDeterministic(t *testing.T) {
	a, _, err := Normalize(DefaultOptions(), input())
	if err != nil {
		t.Fatal(err)
	}
	b, _, err := Normalize(DefaultOptions(), input())
	if err != nil {
		t.Fatal(err)
	}
	if a.ContentHash == "" {
		t.Fatal("ContentHash is empty")
	}
	if a.ContentHash != b.ContentHash {
		t.Errorf("hashes differ for identical content: %q vs %q", a.ContentHash, b.ContentHash)
	}
	if a.ID == b.ID {
		t.Error("IDs should be unique per evidence item")
	}

	changed := input()
	changed.Content = map[string]string{"phase": "Failed"}
	c, _, err := Normalize(DefaultOptions(), changed)
	if err != nil {
		t.Fatal(err)
	}
	if c.ContentHash == a.ContentHash {
		t.Error("hash identical for different content")
	}
}

func TestNormalizeTextRedactsAndFlags(t *testing.T) {
	in := input()
	in.Content = nil
	in.Text = "connecting with Bearer eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxIn0.SflKxwRJSMeKKF2QT4fWpA and password=hunter2\n"
	ev, warnings, err := Normalize(DefaultOptions(), in)
	if err != nil {
		t.Fatal(err)
	}
	if !ev.Redacted {
		t.Error("Redacted = false, want true")
	}
	if strings.Contains(ev.Text, "eyJ") || strings.Contains(ev.Text, "hunter2") {
		t.Errorf("text still contains secrets: %q", ev.Text)
	}
	if !strings.Contains(ev.Text, "[REDACTED]") {
		t.Errorf("text missing redaction marker: %q", ev.Text)
	}
	if len(warnings) != 0 {
		t.Errorf("warnings = %v, want none", warnings)
	}
	if ev.ContentHash == "" {
		t.Error("ContentHash is empty for text evidence")
	}
}

func TestNormalizeTruncatesText(t *testing.T) {
	in := input()
	in.Content = nil
	in.Text = strings.Repeat("x", 100)
	ev, warnings, err := Normalize(Options{MaxLogBytesPerContainer: 10}, in)
	if err != nil {
		t.Fatal(err)
	}
	if len(ev.Text) != 10 {
		t.Errorf("text length = %d, want 10", len(ev.Text))
	}
	if ev.CollectionStatus != StatusTruncated {
		t.Errorf("CollectionStatus = %q, want truncated", ev.CollectionStatus)
	}
	if len(warnings) != 1 || !strings.Contains(warnings[0], "truncated") {
		t.Errorf("warnings = %v, want truncation warning", warnings)
	}
}

func TestNormalizeContentTooLarge(t *testing.T) {
	in := input()
	in.Content = strings.Repeat("y", 100)
	_, _, err := Normalize(Options{MaxEvidenceBytesPerItem: 10}, in)
	if err == nil {
		t.Fatal("Normalize() error = nil, want size-limit error")
	}
	if !strings.Contains(err.Error(), "exceeding") {
		t.Errorf("error = %v, want size-limit message", err)
	}
}

func TestNormalizeValidation(t *testing.T) {
	noKind := input()
	noKind.Kind = ""
	if _, _, err := Normalize(DefaultOptions(), noKind); err == nil {
		t.Error("missing kind: error = nil, want error")
	}

	noIncident := input()
	noIncident.IncidentID = ""
	if _, _, err := Normalize(DefaultOptions(), noIncident); err == nil {
		t.Error("missing incident ID: error = nil, want error")
	}

	noContent := input()
	noContent.Content = nil
	noContent.Text = ""
	if _, _, err := Normalize(DefaultOptions(), noContent); err == nil {
		t.Error("missing content: error = nil, want error")
	}
}

func TestNormalizeEventTimeUTC(t *testing.T) {
	in := input()
	et := time.Date(2026, 11, 15, 13, 30, 0, 0, time.FixedZone("X", 3600))
	in.EventTime = &et
	ev, _, err := Normalize(DefaultOptions(), in)
	if err != nil {
		t.Fatal(err)
	}
	if ev.EventTime == nil || !ev.EventTime.Equal(et.UTC()) {
		t.Errorf("EventTime = %v, want UTC %v", ev.EventTime, et.UTC())
	}
}
