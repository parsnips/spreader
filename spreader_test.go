package spreader

import (
	"errors"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/webriots/rate"
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
	s, err := NewSpreadScheduler(1, 300, WithNumBuckets(256))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	seen := make(map[time.Duration]int)
	for i := range 100 {
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

func TestNewSpreadSchedulerInvalidRate(t *testing.T) {
	_, err := NewSpreadScheduler(0, 60)
	if err == nil {
		t.Fatal("expected error for zero rate")
	}
}

func TestNewSpreadSchedulerInvalidHorizon(t *testing.T) {
	_, err := NewSpreadScheduler(10, 0)
	if err == nil {
		t.Fatal("expected error for zero horizon")
	}
}

func TestNewSpreadSchedulerPropagatesLimiterError(t *testing.T) {
	orig := newRotatingTokenBucketLimiter
	newRotatingTokenBucketLimiter = func(
		numBuckets uint,
		burstCapacity uint8,
		refillRate float64,
		refillRateUnit time.Duration,
	) (*rate.RotatingTokenBucketLimiter, error) {
		return nil, errors.New("boom")
	}
	defer func() {
		newRotatingTokenBucketLimiter = orig
	}()

	_, err := NewSpreadScheduler(10, 60)
	if err == nil || err.Error() != "boom" {
		t.Fatalf("expected limiter error to propagate, got %v", err)
	}
}

func TestSetPublisherCount(t *testing.T) {
	scheduler, err := NewSpreadScheduler(10, 60)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	s := scheduler.(*spreadScheduler)
	if got := s.limiter.RefillRate(); got != 2550 {
		t.Fatalf("expected initial refill rate of 2550, got %v", got)
	}

	s.SetPublisherCount(3)
	if got := s.limiter.RefillRate(); math.Abs(got-(10.0*255.0/3.0)) > 1e-9 {
		t.Fatalf("expected refill rate of %v, got %v", 10.0*255.0/3.0, got)
	}
}

func TestSetPublisherCountNonPositiveUsesDefault(t *testing.T) {
	scheduler, err := NewSpreadScheduler(10, 60)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	s := scheduler.(*spreadScheduler)
	s.SetPublisherCount(3)
	s.SetPublisherCount(0)

	if got := s.limiter.RefillRate(); got != 2550 {
		t.Fatalf("expected refill rate to reset to default, got %v", got)
	}
}

func TestRefillRateForPublisherCountDefaultsNonPositiveCount(t *testing.T) {
	if got := refillRateForPublisherCount(10, 0); got != 2550 {
		t.Fatalf("expected default refill rate for non-positive count, got %v", got)
	}
}

func TestSetPublisherCountLarge(t *testing.T) {
	scheduler, err := NewSpreadScheduler(850, 900)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	s := scheduler.(*spreadScheduler)
	s.SetPublisherCount(1245)

	if got := s.limiter.RefillRate(); math.Abs(got-(850.0*255.0/1245.0)) > 1e-9 {
		t.Fatalf("expected refill rate of %v, got %v", 850.0*255.0/1245.0, got)
	}
}

func TestSetPublisherCountPreservesConsumedState(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	s, err := NewSpreadScheduler(1, 1, WithNumBuckets(1))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if _, err := s.Schedule(now, []byte("item-0")); err != nil {
		t.Fatalf("expected initial schedule to succeed, got %v", err)
	}

	s.SetPublisherCount(2)

	if _, err := s.Schedule(now, []byte("item-1")); err != ErrTimeHorizonExceeded {
		t.Fatalf("expected consumed state to survive publisher count change, got %v", err)
	}
}

func TestSetPublisherCountNoOpWhenUnchanged(t *testing.T) {
	scheduler, err := NewSpreadScheduler(10, 60)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	s := scheduler.(*spreadScheduler)
	before := s.limiter.RefillRate()
	s.SetPublisherCount(1)

	if got := s.limiter.RefillRate(); got != before {
		t.Fatalf("expected unchanged refill rate, got %v want %v", got, before)
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

func TestScheduleDoesNotMutateCallerIDBuffer(t *testing.T) {
	s, err := NewSpreadScheduler(10, 60)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	backing := make([]byte, 12)
	copy(backing, []byte("item"))
	id := backing[:4]
	want := append([]byte(nil), backing...)

	if _, err := s.Schedule(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), id); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if string(backing) != string(want) {
		t.Fatalf("expected Schedule to leave caller buffer unchanged, got %v want %v", backing, want)
	}
}
