package spreader

import (
	"encoding/binary"
	"errors"
	"sync"
	"time"

	"github.com/webriots/rate"
)

var ErrTimeHorizonExceeded = errors.New("time horizon exceeded")

const (
	defaultLimiterNumBuckets = 1 << 20
	defaultPublisherCount    = 1
	maxTokenWeight           = 255
)

type SpreadScheduler interface {
	Schedule(time.Time, []byte) (time.Duration, error)
	SetPublisherCount(int)
}

type SpreadOption func(*spreadOptions)

type spreadOptions struct {
	numBuckets uint
}

func WithNumBuckets(n uint) SpreadOption {
	return func(o *spreadOptions) {
		o.numBuckets = n
	}
}

func NewSpreadScheduler(
	targetRateLimitSeconds int,
	horizonSeconds int,
	opts ...SpreadOption,
) (SpreadScheduler, error) {
	options := spreadOptions{
		numBuckets: defaultLimiterNumBuckets,
	}
	for _, opt := range opts {
		opt(&options)
	}

	if targetRateLimitSeconds <= 0 {
		return nil, errors.New("targetRateLimitSeconds must be positive")
	}

	limiter, err := rate.NewRotatingTokenBucketLimiter(
		options.numBuckets,
		maxTokenWeight,
		refillRateForPublisherCount(targetRateLimitSeconds, defaultPublisherCount),
		time.Second,
	)
	if err != nil {
		return nil, err
	}

	return &spreadScheduler{
		targetRate:     targetRateLimitSeconds,
		horizonSeconds: horizonSeconds,
		publisherCount: defaultPublisherCount,
		limiter:        limiter,
	}, nil
}

type spreadScheduler struct {
	targetRate     int
	horizonSeconds int
	publisherCount int
	limiter        *rate.RotatingTokenBucketLimiter
	m              sync.Mutex
}

func refillRateForPublisherCount(targetRate, count int) float64 {
	if count < 1 {
		count = defaultPublisherCount
	}

	return float64(targetRate) * float64(maxTokenWeight) / float64(count)
}

// Schedule returns a duration to schedule item up to the time horizon or errors.
func (r *spreadScheduler) Schedule(now time.Time, id []byte) (time.Duration, error) {
	for i := range r.horizonSeconds {
		atTime := now.Truncate(time.Second).Add(time.Duration(i) * time.Second).UnixNano()
		checkId := binary.BigEndian.AppendUint64(id, uint64(atTime))
		if r.limiter.TakeTokens(checkId, maxTokenWeight) {
			return time.Duration(i) * time.Second, nil
		}
	}

	return 0, ErrTimeHorizonExceeded
}

// SetPublisherCount sets the current publisher count in case of running
// in distributed environment and the number of publishers changes.
func (r *spreadScheduler) SetPublisherCount(count int) {
	if count < 1 {
		count = defaultPublisherCount
	}

	r.m.Lock()
	defer r.m.Unlock()
	if count == r.publisherCount {
		return
	}

	// SetRefillRate preserves existing token state while applying the new
	// per-publisher share immediately.
	r.limiter.SetRefillRate(refillRateForPublisherCount(r.targetRate, count))
	r.publisherCount = count
}
