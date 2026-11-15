// Package evidence normalizes raw collected content into domain
// evidence: redaction, size limits and content hashing.
package evidence

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"

	"github.com/ThuatPhan/PodSleuth/internal/domain"
)

// Collection status values for evidence items.
const (
	StatusSuccess   = "success"
	StatusTruncated = "truncated"
)

// Default size limits.
const (
	DefaultMaxLogBytesPerContainer = 256 * 1024
	DefaultMaxEvidenceBytesPerItem = 2 * 1024 * 1024
)

// Options bounds evidence sizes.
type Options struct {
	MaxLogBytesPerContainer int
	MaxEvidenceBytesPerItem int
}

// DefaultOptions returns the default limits.
func DefaultOptions() Options {
	return Options{
		MaxLogBytesPerContainer: DefaultMaxLogBytesPerContainer,
		MaxEvidenceBytesPerItem: DefaultMaxEvidenceBytesPerItem,
	}
}

func (o Options) withDefaults() Options {
	if o.MaxLogBytesPerContainer <= 0 {
		o.MaxLogBytesPerContainer = DefaultMaxLogBytesPerContainer
	}
	if o.MaxEvidenceBytesPerItem <= 0 {
		o.MaxEvidenceBytesPerItem = DefaultMaxEvidenceBytesPerItem
	}
	return o
}

// Input is raw collected content before normalization.
type Input struct {
	Kind       string
	Source     string
	IncidentID string
	Resource   domain.ResourceRef
	ObservedAt time.Time
	EventTime  *time.Time

	// Text holds text evidence such as container logs. When set it
	// takes precedence over Content.
	Text string

	// Content holds structured evidence; it must be JSON-serializable.
	Content any

	// Redacted reports that the collector already redacted the content.
	Redacted bool
}

// Normalize redacts, size-caps and hashes raw content into a domain
// evidence item. Warnings describe redaction and truncation; an error
// means the item cannot be used and must not be persisted silently.
func Normalize(opts Options, in Input) (domain.Evidence, []string, error) {
	opts = opts.withDefaults()
	if in.Kind == "" {
		return domain.Evidence{}, nil, fmt.Errorf("evidence kind is required")
	}
	if in.IncidentID == "" {
		return domain.Evidence{}, nil, fmt.Errorf("evidence incident ID is required")
	}

	ev := domain.Evidence{
		ID:               randomID(),
		IncidentID:       in.IncidentID,
		Kind:             in.Kind,
		Source:           in.Source,
		Resource:         in.Resource,
		ObservedAt:       in.ObservedAt.UTC(),
		EventTime:        utcEventTime(in.EventTime),
		Redacted:         in.Redacted,
		CollectionStatus: StatusSuccess,
	}

	var payload []byte
	var warnings []string
	switch {
	case in.Text != "":
		text, changed := RedactText(in.Text)
		ev.Redacted = ev.Redacted || changed
		payload = []byte(text)
		if len(payload) > opts.MaxLogBytesPerContainer {
			payload = payload[:opts.MaxLogBytesPerContainer]
			ev.CollectionStatus = StatusTruncated
			warnings = append(warnings, fmt.Sprintf("%s evidence truncated to %d bytes", in.Kind, len(payload)))
		}
		ev.Text = string(payload)
	case in.Content != nil:
		data, err := json.Marshal(in.Content)
		if err != nil {
			return domain.Evidence{}, nil, fmt.Errorf("cannot serialize %s evidence: %w", in.Kind, err)
		}
		if len(data) > opts.MaxEvidenceBytesPerItem {
			return domain.Evidence{}, nil, fmt.Errorf("%s evidence is %d bytes, exceeding the %d byte item limit", in.Kind, len(data), opts.MaxEvidenceBytesPerItem)
		}
		payload = data
		ev.Content = data
	default:
		return domain.Evidence{}, nil, fmt.Errorf("%s evidence has no content", in.Kind)
	}

	sum := sha256.Sum256(payload)
	ev.ContentHash = hex.EncodeToString(sum[:])
	return ev, warnings, nil
}

func utcEventTime(t *time.Time) *time.Time {
	if t == nil {
		return nil
	}
	u := t.UTC()
	return &u
}

func randomID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return fmt.Sprintf("ev-%x", time.Now().UnixNano())
	}
	return hex.EncodeToString(b[:])
}
