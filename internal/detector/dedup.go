package detector

import (
	"context"
	"sync"
)

// Deduplicator maps failure fingerprints to active episodes. Repeated
// signals for the same fingerprint belong to the same episode; ending
// an episode lets a later failure start a new one.
type Deduplicator interface {
	FindActive(ctx context.Context, fingerprint string) (incidentID string, found bool, err error)
	MarkEpisode(ctx context.Context, fingerprint, incidentID string) error
	// EndEpisode deactivates a fingerprint after its incident is
	// resolved, so a later failure starts a new episode.
	EndEpisode(ctx context.Context, fingerprint string) error
}

// MemoryDeduplicator is an in-memory Deduplicator. Episode state is
// lost on process restart; persistence arrives with the storage
// feature.
type MemoryDeduplicator struct {
	mu       sync.RWMutex
	episodes map[string]string
}

// NewMemoryDeduplicator returns an empty MemoryDeduplicator.
func NewMemoryDeduplicator() *MemoryDeduplicator {
	return &MemoryDeduplicator{episodes: make(map[string]string)}
}

// FindActive implements Deduplicator.
func (d *MemoryDeduplicator) FindActive(_ context.Context, fingerprint string) (string, bool, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	id, ok := d.episodes[fingerprint]
	return id, ok, nil
}

// MarkEpisode implements Deduplicator.
func (d *MemoryDeduplicator) MarkEpisode(_ context.Context, fingerprint, incidentID string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.episodes[fingerprint] = incidentID
	return nil
}

// EndEpisode implements Deduplicator.
func (d *MemoryDeduplicator) EndEpisode(_ context.Context, fingerprint string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	delete(d.episodes, fingerprint)
	return nil
}
