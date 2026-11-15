package report

import (
	"encoding/json"
	"fmt"

	"github.com/ThuatPhan/PodSleuth/internal/domain"
)

// Document is the stable JSON report structure. It reuses the domain
// types and their JSON tags; slices are always emitted (never null).
type Document struct {
	Incident       domain.Incident        `json:"incident"`
	Hypotheses     []domain.Hypothesis    `json:"hypotheses"`
	Evidence       []domain.Evidence      `json:"evidence"`
	Timeline       []domain.TimelineEvent `json:"timeline"`
	CollectionRuns []domain.CollectionRun `json:"collection_runs"`
}

// renderJSON renders the report as indented JSON. Output contains no
// timestamps of its own so identical data yields identical documents.
func renderJSON(d Data) (string, error) {
	doc := Document{
		Incident:       d.Incident,
		Hypotheses:     nonNil(d.Hypotheses),
		Evidence:       nonNil(d.Evidence),
		Timeline:       nonNil(d.Timeline),
		CollectionRuns: nonNil(d.CollectionRuns),
	}
	b, err := json.MarshalIndent(doc, "", "  ")
	if err != nil {
		return "", fmt.Errorf("report: cannot encode JSON report: %w", err)
	}
	return string(b) + "\n", nil
}

func nonNil[T any](in []T) []T {
	if in == nil {
		return []T{}
	}
	return in
}
