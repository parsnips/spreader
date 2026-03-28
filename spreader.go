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
)

type SpreadScheduler interface {
	Schedule(time.Time, []byte) (time.Duration, error)
	SetPublisherCount(uint8)
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

	limiter, err := rate.NewRotatingTokenBucketLimiter(
		options.numBuckets,
		1,
		float64(targetRateLimitSeconds),
		time.Second,
	)
	if err != nil {
		return nil, err
	}

	return &spreadScheduler{
		horizonSeconds: horizonSeconds,
		publisherCount: defaultPublisherCount,
		limiter:        limiter,
	}, nil
}

type spreadScheduler struct {
	horizonSeconds int
	publisherCount uint8
	limiter        *rate.RotatingTokenBucketLimiter
	m              sync.Mutex
}

// Schedule returns a duration to schedule item up to the time horizon or errors.
func (r *spreadScheduler) Schedule(now time.Time, id []byte) (time.Duration, error) {
	r.m.Lock()
	publisherCount := r.publisherCount
	r.m.Unlock()

	for i := range r.horizonSeconds {
		atTime := now.Truncate(time.Second).Add(time.Duration(i) * time.Second).UnixNano()
		checkId := binary.BigEndian.AppendUint64(id, uint64(atTime))
		if r.limiter.TakeTokens(checkId, publisherCount) {
			return time.Duration(i) * time.Second, nil
		}
	}

	return 0, ErrTimeHorizonExceeded
}

// SetPublisherCount sets the current publisher count in case of running
// in distributed environment and the number of publishers changes.
func (r *spreadScheduler) SetPublisherCount(count uint8) {
	r.m.Lock()
	defer r.m.Unlock()
	r.publisherCount = count
}
