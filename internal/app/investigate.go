package app

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"github.com/ThuatPhan/PodSleuth/internal/analyzer"
	"github.com/ThuatPhan/PodSleuth/internal/collector"
	"github.com/ThuatPhan/PodSleuth/internal/detector"
	"github.com/ThuatPhan/PodSleuth/internal/domain"
	"github.com/ThuatPhan/PodSleuth/internal/evidence"
	"github.com/ThuatPhan/PodSleuth/internal/incident"
	"github.com/ThuatPhan/PodSleuth/internal/storage"
)

// PodNotFoundError reports that the requested Pod does not exist.
type PodNotFoundError struct {
	Namespace string
	Name      string
}

func (e *PodNotFoundError) Error() string {
	return fmt.Sprintf("pod %s/%s not found; check the name and namespace", e.Namespace, e.Name)
}

// Investigation is the outcome of one on-demand Pod investigation.
type Investigation struct {
	// IncidentIDs lists the incidents created or reused, in signal
	// order. It is empty when the Pod is healthy.
	IncidentIDs []string
	// Healthy reports that no failure condition was detected; no
	// incident was created.
	Healthy bool
	// Warnings carries non-fatal collection warnings.
	Warnings []string
}

// PrimaryIncidentID returns the incident the report renders.
func (in Investigation) PrimaryIncidentID() string {
	if len(in.IncidentIDs) == 0 {
		return ""
	}
	return in.IncidentIDs[0]
}

// Investigator runs the on-demand investigation pipeline for one Pod:
// fetch → detect → incident (create or reuse) → collect → persist →
// analyze → persist. It performs no retries; the caller owns timeouts
// through the context.
type Investigator struct {
	client     kubernetes.Interface
	clusterID  string
	store      storage.Store
	detector   detector.Detector
	collectors collector.Registry
	analyzer   analyzer.Analyzer
	now        func() time.Time
	newID      func() string
}

// NewInvestigator returns an Investigator wired to the given
// components.
func NewInvestigator(
	client kubernetes.Interface,
	clusterID string,
	store storage.Store,
	det detector.Detector,
	collectors collector.Registry,
	eng analyzer.Analyzer,
) *Investigator {
	return &Investigator{
		client:     client,
		clusterID:  clusterID,
		store:      store,
		detector:   det,
		collectors: collectors,
		analyzer:   eng,
		now:        time.Now,
		newID:      randomID,
	}
}

// Investigate investigates one Pod and persists the results. A healthy
// Pod produces no incident and nothing is persisted.
func (inv *Investigator) Investigate(ctx context.Context, namespace, name string) (Investigation, error) {
	if err := ctx.Err(); err != nil {
		return Investigation{}, err
	}
	if namespace == "" || name == "" {
		return Investigation{}, fmt.Errorf("app: namespace and pod name are required")
	}

	pod, err := inv.client.CoreV1().Pods(namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		if isNotFound(err) {
			return Investigation{}, &PodNotFoundError{Namespace: namespace, Name: name}
		}
		return Investigation{}, fmt.Errorf("app: fetch pod %s/%s: %w", namespace, name, err)
	}

	obs, err := observationFromPod(inv.clusterID, pod, inv.now())
	if err != nil {
		return Investigation{}, err
	}

	signals, err := inv.detector.Evaluate(ctx, obs)
	if err != nil {
		return Investigation{}, fmt.Errorf("app: detect failures on pod %s/%s: %w", namespace, name, err)
	}
	if len(signals) == 0 {
		return Investigation{Healthy: true}, nil
	}

	// Build the incident drafts through the incident manager so
	// construction (ID, severity, summary, workload fields) stays in
	// one place and matches the watch pipeline.
	mgr := incident.NewManager(detector.NewMemoryDeduplicator(), incident.Options{
		Clock: inv.now,
		NewID: inv.newID,
	})
	if err := mgr.Process(ctx, obs, signals); err != nil {
		return Investigation{}, fmt.Errorf("app: build incidents for pod %s/%s: %w", namespace, name, err)
	}
	drafts, err := orderedIncidents(mgr, signals)
	if err != nil {
		return Investigation{}, err
	}

	result := Investigation{Warnings: []string{}}
	for _, draft := range drafts {
		finalID, err := inv.investigateIncident(ctx, draft, &result)
		if err != nil {
			return Investigation{}, err
		}
		result.IncidentIDs = append(result.IncidentIDs, finalID)
	}
	return result, nil
}

// investigateIncident resolves the incident against storage (reusing an
// active incident with the same fingerprint), collects evidence,
// persists everything and runs the analysis. It returns the final
// incident ID, which differs from the draft's when an active incident
// was reused.
func (inv *Investigator) investigateIncident(ctx context.Context, draft domain.Incident, result *Investigation) (string, error) {
	final, reused, err := inv.resolveIncident(ctx, draft)
	if err != nil {
		return "", err
	}
	draft = final

	if err := inv.store.AppendTimeline(ctx, []domain.TimelineEvent{{
		ID:         inv.newID(),
		IncidentID: draft.ID,
		EventType:  "detected",
		OccurredAt: inv.now().UTC(),
		Message:    detectedMessage(draft, reused),
	}}); err != nil {
		return "", fmt.Errorf("app: append detected timeline event for %s: %w", draft.ID, err)
	}

	collected, warnings, err := inv.collect(ctx, draft)
	if err != nil {
		return "", err
	}
	result.Warnings = append(result.Warnings, warnings...)

	if err := inv.store.AddEvidence(ctx, collected); err != nil {
		return "", fmt.Errorf("app: persist evidence for %s: %w", draft.ID, err)
	}

	analysis, err := inv.analyzer.Analyze(ctx, analyzer.Input{Incident: draft, Evidence: collected})
	if err != nil {
		return "", fmt.Errorf("app: analyze incident %s: %w", draft.ID, err)
	}
	if err := inv.store.SaveAnalysis(ctx, draft.ID, analysis.Hypotheses); err != nil {
		return "", fmt.Errorf("app: persist analysis for %s: %w", draft.ID, err)
	}

	if err := inv.store.AppendTimeline(ctx, []domain.TimelineEvent{
		{
			ID:         inv.newID(),
			IncidentID: draft.ID,
			EventType:  "collected",
			OccurredAt: inv.now().UTC(),
			Message: fmt.Sprintf("Collected %d evidence item(s) from %d collector(s), %d warning(s)",
				len(collected), len(inv.collectors.ForSignal(signalFor(draft))), len(warnings)),
		},
		{
			ID:         inv.newID(),
			IncidentID: draft.ID,
			EventType:  "analyzed",
			OccurredAt: inv.now().UTC(),
			Message: fmt.Sprintf("Analysis produced %d hypothesis/hypotheses from %d rules",
				len(analysis.Hypotheses), len(analysis.Coverage)),
		},
	}); err != nil {
		return "", fmt.Errorf("app: append pipeline timeline events for %s: %w", draft.ID, err)
	}
	return draft.ID, nil
}

// resolveIncident reuses the active stored incident with the same
// fingerprint (retry semantics), or creates the draft as a new episode.
func (inv *Investigator) resolveIncident(ctx context.Context, draft domain.Incident) (domain.Incident, bool, error) {
	existing, err := inv.findActiveIncident(ctx, draft.Fingerprint, draft.Resource.Namespace)
	if err != nil {
		return domain.Incident{}, false, err
	}
	if existing != nil {
		existing.LastSeenAt = draft.LastSeenAt
		existing.UpdatedAt = draft.UpdatedAt
		if err := inv.store.UpdateIncident(ctx, *existing); err != nil {
			return domain.Incident{}, false, fmt.Errorf("app: update reused incident %s: %w", existing.ID, err)
		}
		return *existing, true, nil
	}
	if err := inv.store.CreateIncident(ctx, draft); err != nil {
		return domain.Incident{}, false, fmt.Errorf("app: create incident: %w", err)
	}
	return draft, false, nil
}

func (inv *Investigator) findActiveIncident(ctx context.Context, fingerprint, namespace string) (*domain.Incident, error) {
	list, err := inv.store.ListIncidents(ctx, storage.IncidentFilter{Namespace: namespace})
	if err != nil {
		return nil, fmt.Errorf("app: look up active incidents in %s: %w", namespace, err)
	}
	for i := range list {
		in := list[i]
		if in.Fingerprint == fingerprint && in.Status != domain.StatusResolved && in.Status != domain.StatusClosed {
			return &in, nil
		}
	}
	return nil, nil
}

// collect runs every collector supporting the incident's signal,
// persists one collection run per collector and returns the evidence.
// Collector failures become failed runs and warnings; they never
// discard other collectors' evidence.
func (inv *Investigator) collect(ctx context.Context, inc domain.Incident) ([]domain.Evidence, []string, error) {
	signal := signalFor(inc)
	var (
		collected []domain.Evidence
		warnings  []string
	)
	for _, c := range inv.collectors.ForSignal(signal) {
		started := inv.now().UTC()
		result, err := c.Collect(ctx, collector.Request{Incident: inc, Signal: signal})
		completed := inv.now().UTC()

		run := domain.CollectionRun{
			ID:            inv.newID(),
			IncidentID:    inc.ID,
			CollectorName: c.Name(),
			StartedAt:     started,
			CompletedAt:   &completed,
		}
		if err != nil {
			run.Status = domain.RunStatusFailed
			run.ErrorCode = collector.ClassifyError(err)
			run.ErrorMessageRedacted, _ = evidence.RedactText(err.Error())
		} else {
			run.Status = domain.RunStatusSuccess
		}
		if err := inv.store.CreateCollectionRun(ctx, run); err != nil {
			return nil, nil, fmt.Errorf("app: persist collection run for %s: %w", inc.ID, err)
		}

		collected = append(collected, result.Evidence...)
		warnings = append(warnings, result.Warnings...)
		if err != nil && len(result.Warnings) == 0 {
			warnings = append(warnings, fmt.Sprintf("collector %s failed (%s): %v", c.Name(), run.ErrorCode, err))
		}
	}
	return collected, warnings, nil
}

// orderedIncidents returns the manager's drafts in signal order so the
// primary incident is deterministic.
func orderedIncidents(mgr *incident.Manager, signals []domain.FailureSignal) ([]domain.Incident, error) {
	drafts := mgr.ListActive()
	byFingerprint := make(map[string]domain.Incident, len(drafts))
	for _, d := range drafts {
		byFingerprint[d.Fingerprint] = d
	}
	ordered := make([]domain.Incident, 0, len(signals))
	seen := make(map[string]bool, len(signals))
	for _, s := range signals {
		d, ok := byFingerprint[s.Fingerprint]
		if !ok {
			return nil, fmt.Errorf("app: incident for signal %s is missing", s.Fingerprint)
		}
		if !seen[d.ID] {
			seen[d.ID] = true
			ordered = append(ordered, d)
		}
	}
	return ordered, nil
}

func signalFor(inc domain.Incident) domain.FailureSignal {
	return domain.FailureSignal{
		Type:        inc.FailureType,
		Fingerprint: inc.Fingerprint,
		Resource:    inc.Resource,
		Attributes:  map[string]string{"container": inc.ContainerName},
	}
}

func detectedMessage(inc domain.Incident, reused bool) string {
	target := fmt.Sprintf("pod %s/%s", inc.Resource.Namespace, inc.Resource.Name)
	if inc.ContainerName != "" {
		target += " container " + inc.ContainerName
	}
	if reused {
		return fmt.Sprintf("Existing active incident reused for %s (%s)", target, inc.FailureType)
	}
	return fmt.Sprintf("%s detected on %s", inc.FailureType, target)
}

// observationFromPod converts a fetched Pod into the normalized
// observation the detector consumes.
func observationFromPod(clusterID string, pod *corev1.Pod, now time.Time) (domain.Observation, error) {
	payload, err := json.Marshal(pod)
	if err != nil {
		return domain.Observation{}, fmt.Errorf("app: serialize pod %s/%s: %w", pod.Namespace, pod.Name, err)
	}
	return domain.Observation{
		Resource: domain.ResourceRef{
			ClusterID:  clusterID,
			APIVersion: "v1",
			Kind:       "Pod",
			Namespace:  pod.Namespace,
			Name:       pod.Name,
			UID:        string(pod.UID),
		},
		ObservedAt:      now.UTC(),
		ResourceVersion: pod.ResourceVersion,
		Payload:         payload,
	}, nil
}

// isNotFound reports whether the error is a Kubernetes NotFound error.
func isNotFound(err error) bool {
	return apierrors.IsNotFound(err)
}

func randomID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return fmt.Sprintf("id-%x", time.Now().UnixNano())
	}
	return hex.EncodeToString(b[:])
}
