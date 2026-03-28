package spreader

import (
	"fmt"
	"testing"
	"time"
)

func TestNewSpreadScheduler(t *testing.T) {
	s, err := NewSpreadScheduler(10, 60)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if s == nil {
		t.Fatal("expected non-nil scheduler")
	}
}

func TestNewSpreadSchedulerWithNumBuckets(t *testing.T) {
	s, err := NewSpreadScheduler(10, 60, WithNumBuckets(1<<16))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if s == nil {
		t.Fatal("expected non-nil scheduler")
	}
}

func TestScheduleReturnsWithinHorizon(t *testing.T) {
	s, err := NewSpreadScheduler(10, 60)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	now := time.Now()
	d, err := s.Schedule(now, []byte("item-1"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if d < 0 || d >= 60*time.Second {
		t.Fatalf("duration %v out of horizon range [0, 60s)", d)
	}
}

func TestScheduleReturnsDurationAtSecondGranularity(t *testing.T) {
	s, err := NewSpreadScheduler(10, 60)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	now := time.Now()
	d, err := s.Schedule(now, []byte("item-1"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if d%time.Second != 0 {
		t.Fatalf("expected second granularity, got %v", d)
	}
}

func TestScheduleDeterministic(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	s1, _ := NewSpreadScheduler(10, 60)
	s2, _ := NewSpreadScheduler(10, 60)

	d1, err1 := s1.Schedule(now, []byte("same-id"))
	d2, err2 := s2.Schedule(now, []byte("same-id"))

	if err1 != nil || err2 != nil {
		t.Fatalf("unexpected errors: %v, %v", err1, err2)
	}
	if d1 != d2 {
		t.Fatalf("expected deterministic results, got %v and %v", d1, d2)
	}
}

func TestScheduleExceedsHorizon(t *testing.T) {
	// Few buckets + short horizon to force bucket exhaustion.
	s, err := NewSpreadScheduler(1, 2, WithNumBuckets(4))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	var horizonErr error
	for i := range 1000 {
		_, horizonErr = s.Schedule(now, []byte(fmt.Sprintf("item-%d", i)))
		if horizonErr != nil {
			break
		}
	}
	if horizonErr != ErrTimeHorizonExceeded {
		t.Fatalf("expected ErrTimeHorizonExceeded, got %v", horizonErr)
	}
}

func TestScheduleSpreadsAcrossHorizon(t *testing.T) {
	// Few buckets forces collisions, spreading items across seconds.
	s, err := NewSpreadScheduler(1, 300, WithNumBuckets(64))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	seen := make(map[time.Duration]int)
	for i := range 20 {
		d, err := s.Schedule(now, []byte(fmt.Sprintf("item-%d", i)))
		if err != nil {
			t.Fatalf("unexpected error on item %d: %v", i, err)
		}
		seen[d]++
	}
	if len(seen) < 2 {
		t.Fatalf("expected items spread across multiple seconds, got %v", seen)
	}
}

func TestSetPublisherCount(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	// Few buckets so tokens exhaust quickly, making publisher count
	// differences observable.
	s1, _ := NewSpreadScheduler(1, 30, WithNumBuckets(4))
	s2, _ := NewSpreadScheduler(1, 30, WithNumBuckets(4))
	s2.SetPublisherCount(3)

	var diffs int
	for i := range 50 {
		id := []byte(fmt.Sprintf("item-%d", i))
		d1, _ := s1.Schedule(now, id)
		d2, _ := s2.Schedule(now, id)
		if d1 != d2 {
			diffs++
		}
	}

	if diffs == 0 {
		t.Fatal("expected different scheduling with different publisher counts")
	}
}

func TestScheduleConcurrent(t *testing.T) {
	s, err := NewSpreadScheduler(100, 120)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	errs := make(chan error, 50)

	for i := range 50 {
		go func() {
			_, err := s.Schedule(now, []byte(fmt.Sprintf("item-%d", i)))
			errs <- err
		}()
	}

	for range 50 {
		if err := <-errs; err != nil {
			t.Fatalf("unexpected error in concurrent schedule: %v", err)
		}
	}
}
