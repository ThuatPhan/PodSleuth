package detector

import (
	"context"
	"sync"
	"testing"
	"time"
)

// fixedObsTime is the deterministic observation time for fixtures.
var fixedObsTime = time.Date(2026, 11, 15, 12, 0, 0, 0, time.UTC)

func TestMemoryDeduplicatorLifecycle(t *testing.T) {
	d := NewMemoryDeduplicator()
	ctx := context.Background()

	if _, found, err := d.FindActive(ctx, "fp"); err != nil || found {
		t.Fatalf("FindActive before MarkEpisode = (%q, %v, %v), want not found", "", found, err)
	}

	if err := d.MarkEpisode(ctx, "fp", "inc-1"); err != nil {
		t.Fatalf("MarkEpisode() error = %v", err)
	}
	id, found, err := d.FindActive(ctx, "fp")
	if err != nil || !found || id != "inc-1" {
		t.Fatalf("FindActive after MarkEpisode = (%q, %v, %v), want (inc-1, true, nil)", id, found, err)
	}

	if err := d.EndEpisode(ctx, "fp"); err != nil {
		t.Fatalf("EndEpisode() error = %v", err)
	}
	if _, found, _ := d.FindActive(ctx, "fp"); found {
		t.Fatal("FindActive after EndEpisode = found, want not found")
	}
}

func TestMemoryDeduplicatorIndependentFingerprints(t *testing.T) {
	d := NewMemoryDeduplicator()
	ctx := context.Background()
	if err := d.MarkEpisode(ctx, "fp-a", "inc-a"); err != nil {
		t.Fatal(err)
	}
	if _, found, _ := d.FindActive(ctx, "fp-b"); found {
		t.Error("unrelated fingerprint reported active")
	}
}

func TestMemoryDeduplicatorConcurrentAccess(t *testing.T) {
	d := NewMemoryDeduplicator()
	ctx := context.Background()
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			fp := string(rune('a' + n))
			_ = d.MarkEpisode(ctx, fp, "inc")
			_, _, _ = d.FindActive(ctx, fp)
			_ = d.EndEpisode(ctx, fp)
		}(i)
	}
	wg.Wait()
}
