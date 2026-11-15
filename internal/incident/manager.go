// Package incident manages incident episodes: creation,
// deduplication and recovery. It consumes observations and failure
// signals produced upstream and owns no Kubernetes or storage
// dependencies.
package incident

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"sync"
	"time"

	"github.com/ThuatPhan/PodSleuth/internal/detector"
	"github.com/ThuatPhan/PodSleuth/internal/domain"
)

// DefaultStabilization is the recovery stabilization period: a Pod (or
// its workload replacement) must stay Ready this long without a
// matching failure before an incident resolves.
const DefaultStabilization = 2 * time.Minute

// Options configures the Manager.
type Options struct {
	// Stabilization overrides DefaultStabilization.
	Stabilization time.Duration

	// Clock returns the current time. Defaults to time.Now.
	Clock func() time.Time

	// NewID generates incident IDs. Defaults to random hex.
	NewID func() string
}

// recoveryState tracks the consecutive Ready period of an episode.
type recoveryState struct {
	stableSince time.Time
}

// Manager tracks incidents in memory. Persistence arrives with the
// storage feature.
type Manager struct {
	dedup         detector.Deduplicator
	stabilization time.Duration
	now           func() time.Time
	newID         func() string

	mu        sync.Mutex
	incidents map[string]*domain.Incident
	recovery  map[string]*recoveryState
}

// NewManager returns a Manager using the given deduplicator.
func NewManager(dedup detector.Deduplicator, opts Options) *Manager {
	if opts.Stabilization <= 0 {
		opts.Stabilization = DefaultStabilization
	}
	if opts.Clock == nil {
		opts.Clock = time.Now
	}
	if opts.NewID == nil {
		opts.NewID = randomID
	}
	return &Manager{
		dedup:         dedup,
		stabilization: opts.Stabilization,
		now:           opts.Clock,
		newID:         opts.NewID,
		incidents:     make(map[string]*domain.Incident),
		recovery:      make(map[string]*recoveryState),
	}
}

// Process records failure signals and evaluates recovery for one
// observation. Signals with an active episode update the existing
// incident; new fingerprints create incidents. Recovery resolves an
// incident only after its Pod or workload replacement stays Ready for
// the stabilization period without a matching failure; a Pod
// disappearing never resolves anything.
func (m *Manager) Process(ctx context.Context, obs domain.Observation, signals []domain.FailureSignal) error {
	pod, err := domain.ParsePodView(obs.Payload)
	if err != nil {
		return fmt.Errorf("incident: observation for %s/%s: %w", obs.Resource.Namespace, obs.Resource.Name, err)
	}
	now := m.now().UTC()

	m.mu.Lock()
	defer m.mu.Unlock()

	for _, s := range signals {
		if err := m.recordSignal(ctx, s, pod, now); err != nil {
			return err
		}
	}
	m.sweep(ctx, pod, signals, now)
	return nil
}

func (m *Manager) recordSignal(ctx context.Context, s domain.FailureSignal, pod *domain.PodView, now time.Time) error {
	id, found, err := m.dedup.FindActive(ctx, s.Fingerprint)
	if err != nil {
		return fmt.Errorf("incident: dedup lookup for %s: %w", s.Fingerprint, err)
	}
	if found {
		if inc, ok := m.incidents[id]; ok {
			inc.LastSeenAt = now
			m.resetRecovery(id)
		}
		return nil
	}

	inc := m.createIncident(s, pod, now)
	m.incidents[inc.ID] = inc
	if err := m.dedup.MarkEpisode(ctx, s.Fingerprint, inc.ID); err != nil {
		delete(m.incidents, inc.ID)
		return fmt.Errorf("incident: cannot mark episode %s: %w", s.Fingerprint, err)
	}
	return nil
}

func (m *Manager) createIncident(s domain.FailureSignal, pod *domain.PodView, now time.Time) *domain.Incident {
	inc := &domain.Incident{
		ID:          m.newID(),
		ClusterID:   s.Resource.ClusterID,
		Fingerprint: s.Fingerprint,
		FailureType: s.Type,
		Severity:    domain.DefaultSeverity,
		Status:      domain.StatusDetected,
		Resource:    s.Resource,
		FirstSeenAt: now,
		LastSeenAt:  now,
		Summary:     summary(s),
	}
	if container := s.Attributes["container"]; container != "" {
		inc.ContainerName = container
	}
	if o := pod.ControllerOwner(); o != nil {
		inc.WorkloadKind = o.Kind
		inc.WorkloadName = o.Name
		inc.WorkloadUID = o.UID
	}
	return inc
}

// sweep evaluates recovery for active incidents related to the
// observed Pod: the Pod itself or, for controller-managed workloads, a
// replacement Pod with the same workload owner.
func (m *Manager) sweep(ctx context.Context, pod *domain.PodView, signals []domain.FailureSignal, now time.Time) {
	healthy := pod.IsReady()
	for id, inc := range m.incidents {
		if inc.Status == domain.StatusResolved || inc.Status == domain.StatusClosed {
			continue
		}
		if !related(inc, pod) {
			continue
		}
		if healthy && !hasMatchingFailure(inc, signals) {
			rs := m.recoveryFor(id)
			if rs.stableSince.IsZero() {
				rs.stableSince = now
			} else if now.Sub(rs.stableSince) >= m.stabilization {
				m.resolve(ctx, id, now)
			}
		} else {
			m.resetRecovery(id)
		}
	}
}

// related reports whether the observed Pod can contribute to the
// incident's recovery: it is the incident's own Pod or a replacement
// Pod from the same workload.
func related(inc *domain.Incident, pod *domain.PodView) bool {
	if inc.Resource.UID == pod.Metadata.UID {
		return true
	}
	if inc.WorkloadUID == "" {
		return false
	}
	if o := pod.ControllerOwner(); o != nil {
		return o.UID == inc.WorkloadUID && inc.Resource.Namespace == pod.Metadata.Namespace
	}
	return false
}

// hasMatchingFailure reports whether any signal in this observation is
// a new occurrence of the incident's failure in its container context.
func hasMatchingFailure(inc *domain.Incident, signals []domain.FailureSignal) bool {
	for _, s := range signals {
		if s.Fingerprint == inc.Fingerprint {
			return true
		}
		if s.Type == inc.FailureType && s.Attributes["container"] == inc.ContainerName {
			return true
		}
	}
	return false
}

func (m *Manager) resolve(ctx context.Context, id string, now time.Time) {
	inc := m.incidents[id]
	if !domain.CanTransition(inc.Status, domain.StatusResolved) {
		return
	}
	inc.Status = domain.StatusResolved
	resolvedAt := now
	inc.ResolvedAt = &resolvedAt
	delete(m.recovery, id)
	// EndEpisode errors are intentionally ignored: the incident is
	// already resolved, and a stale dedup entry only delays a future
	// episode.
	_ = m.dedup.EndEpisode(ctx, inc.Fingerprint)
}

func (m *Manager) recoveryFor(id string) *recoveryState {
	rs, ok := m.recovery[id]
	if !ok {
		rs = &recoveryState{}
		m.recovery[id] = rs
	}
	return rs
}

func (m *Manager) resetRecovery(id string) {
	delete(m.recovery, id)
}

// Get returns a copy of the incident with the given ID.
func (m *Manager) Get(id string) (domain.Incident, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	inc, ok := m.incidents[id]
	if !ok {
		return domain.Incident{}, false
	}
	return *inc, true
}

// List returns copies of all incidents.
func (m *Manager) List() []domain.Incident {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]domain.Incident, 0, len(m.incidents))
	for _, inc := range m.incidents {
		out = append(out, *inc)
	}
	return out
}

// ListActive returns copies of incidents not yet resolved or closed.
func (m *Manager) ListActive() []domain.Incident {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []domain.Incident
	for _, inc := range m.incidents {
		if inc.Status != domain.StatusResolved && inc.Status != domain.StatusClosed {
			out = append(out, *inc)
		}
	}
	return out
}

func summary(s domain.FailureSignal) string {
	out := fmt.Sprintf("%s on pod %s/%s", s.Type, s.Resource.Namespace, s.Resource.Name)
	if container := s.Attributes["container"]; container != "" {
		out += " container " + container
	}
	if reason := s.Attributes["reason"]; reason != "" && reason != s.Type {
		out += " (reason " + reason + ")"
	}
	return out
}

func randomID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return fmt.Sprintf("inc-%x", time.Now().UnixNano())
	}
	return hex.EncodeToString(b[:])
}
