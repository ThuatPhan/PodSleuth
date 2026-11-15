// Package events collects Kubernetes Event evidence for an incident.
package events

import (
	"context"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"github.com/ThuatPhan/PodSleuth/internal/collector"
	"github.com/ThuatPhan/PodSleuth/internal/domain"
	"github.com/ThuatPhan/PodSleuth/internal/evidence"
)

// Evidence kind and provenance produced by this collector.
const (
	Kind   = "events"
	Source = "api-server"
)

// maxEvents bounds a single events evidence item.
const maxEvents = 200

// EventsCollector reads Events related to the incident's Pod, by
// involved object UID first and by name as fallback.
type EventsCollector struct {
	client kubernetes.Interface
	opts   evidence.Options
}

// NewEventsCollector returns an EventsCollector for the given client.
func NewEventsCollector(client kubernetes.Interface, opts evidence.Options) *EventsCollector {
	return &EventsCollector{client: client, opts: opts}
}

// Name implements Collector.
func (c *EventsCollector) Name() string { return "events" }

// Supports implements Collector. Events are relevant context for every
// failure family.
func (c *EventsCollector) Supports(domain.FailureSignal) bool { return true }

// Collect implements Collector.
func (c *EventsCollector) Collect(ctx context.Context, req collector.Request) (collector.Result, error) {
	if err := ctx.Err(); err != nil {
		return collector.Result{}, err
	}
	res := req.Incident.Resource

	items, err := c.listEvents(ctx, res)
	if err != nil {
		return terminalOrRetryable("events list", res, err)
	}

	truncated := false
	if len(items) > maxEvents {
		items = items[:maxEvents]
		truncated = true
	}
	if len(items) == 0 {
		return collector.Result{
			Warnings: []string{fmt.Sprintf("no events found for pod %s/%s", res.Namespace, res.Name)},
		}, nil
	}

	views := make([]eventView, 0, len(items))
	var latest time.Time
	redacted := false
	for i := range items {
		v := viewOf(&items[i])
		views = append(views, v)
		if v.redacted {
			redacted = true
		}
		if t := v.lastTime(); t.After(latest) {
			latest = t
		}
	}

	input := evidence.Input{
		Kind:       Kind,
		Source:     Source,
		IncidentID: req.Incident.ID,
		Resource:   res,
		ObservedAt: time.Now().UTC(),
		Content:    views,
		Redacted:   redacted,
	}
	if !latest.IsZero() {
		input.EventTime = &latest
	}
	ev, warnings, err := evidence.Normalize(c.opts, input)
	if err != nil {
		return collector.Result{}, fmt.Errorf("events collector: %w", err)
	}
	if truncated {
		warnings = append(warnings, fmt.Sprintf("events evidence capped at %d events", maxEvents))
	}
	return collector.Result{Evidence: []domain.Evidence{ev}, Warnings: warnings}, nil
}

// listEvents queries by involved object UID first, then falls back to
// the Pod name. Results selected by name are filtered back to the
// incident's Pod UID client-side.
func (c *EventsCollector) listEvents(ctx context.Context, res domain.ResourceRef) ([]corev1.Event, error) {
	if res.UID != "" {
		list, err := c.list(ctx, res.Namespace, "involvedObject.uid="+res.UID)
		if err != nil {
			return nil, err
		}
		if len(list) > 0 {
			return list, nil
		}
	}
	list, err := c.list(ctx, res.Namespace, "involvedObject.name="+res.Name)
	if err != nil {
		return nil, err
	}
	if res.UID == "" {
		return list, nil
	}
	var matching []corev1.Event
	for _, e := range list {
		if string(e.InvolvedObject.UID) == res.UID {
			matching = append(matching, e)
		}
	}
	return matching, nil
}

func (c *EventsCollector) list(ctx context.Context, namespace, fieldSelector string) ([]corev1.Event, error) {
	list, err := c.client.CoreV1().Events(namespace).List(ctx, metav1.ListOptions{FieldSelector: fieldSelector})
	if err != nil {
		return nil, err
	}
	return list.Items, nil
}

// eventView is the redacted, timestamp-normalized event shape persisted
// as evidence.
type eventView struct {
	Type               string     `json:"type"`
	Reason             string     `json:"reason,omitempty"`
	Message            string     `json:"message,omitempty"`
	Count              int32      `json:"count"`
	FirstAt            *time.Time `json:"first_at,omitempty"`
	LastAt             *time.Time `json:"last_at,omitempty"`
	ReportingComponent string     `json:"reporting_component,omitempty"`
	InvolvedObject     eventRef   `json:"involved_object"`

	// redacted is not serialized; it reports message redaction to the
	// collector.
	redacted bool
}

type eventRef struct {
	Kind      string `json:"kind,omitempty"`
	Namespace string `json:"namespace,omitempty"`
	Name      string `json:"name,omitempty"`
	UID       string `json:"uid,omitempty"`
}

// viewOf normalizes event timestamps across fields: core v1 Events
// carry FirstTimestamp/LastTimestamp, and EventTime is populated by the
// events.k8s.io API.
func viewOf(e *corev1.Event) eventView {
	message, redacted := evidence.RedactText(e.Message)
	v := eventView{
		Type:               e.Type,
		Reason:             e.Reason,
		Message:            message,
		Count:              e.Count,
		ReportingComponent: e.Source.Component,
		redacted:           redacted,
		InvolvedObject: eventRef{
			Kind:      e.InvolvedObject.Kind,
			Namespace: e.InvolvedObject.Namespace,
			Name:      e.InvolvedObject.Name,
			UID:       string(e.InvolvedObject.UID),
		},
	}
	v.FirstAt = firstEventTime(e)
	v.LastAt = lastEventTime(e)
	return v
}

func firstEventTime(e *corev1.Event) *time.Time {
	switch {
	case !e.FirstTimestamp.IsZero():
		t := e.FirstTimestamp.Time.UTC()
		return &t
	case !e.EventTime.IsZero():
		t := e.EventTime.Time.UTC()
		return &t
	case !e.CreationTimestamp.IsZero():
		t := e.CreationTimestamp.Time.UTC()
		return &t
	}
	return nil
}

func lastEventTime(e *corev1.Event) *time.Time {
	switch {
	case !e.LastTimestamp.IsZero():
		t := e.LastTimestamp.Time.UTC()
		return &t
	case !e.EventTime.IsZero():
		t := e.EventTime.Time.UTC()
		return &t
	}
	return nil
}

func (v eventView) lastTime() time.Time {
	if v.LastAt != nil {
		return *v.LastAt
	}
	if v.FirstAt != nil {
		return *v.FirstAt
	}
	return time.Time{}
}

// terminalOrRetryable maps an API error to a Result. Terminal errors
// (forbidden) become warnings; transient errors also return an error
// so the caller can retry.
func terminalOrRetryable(op string, res domain.ResourceRef, err error) (collector.Result, error) {
	kind := collector.ClassifyError(err)
	warning := fmt.Sprintf("%s for pod %s/%s failed (%s): %v", op, res.Namespace, res.Name, kind, err)
	switch kind {
	case collector.ErrKindForbidden, collector.ErrKindUnauthorized:
		return collector.Result{Warnings: []string{warning}}, nil
	case collector.ErrKindCanceled:
		return collector.Result{Warnings: []string{warning}}, err
	default:
		return collector.Result{Warnings: []string{warning}}, fmt.Errorf("%s: %s", op, warning)
	}
}
