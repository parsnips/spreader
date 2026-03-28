# spreader

[![Go Reference](https://pkg.go.dev/badge/github.com/parsnips/spreader.svg)](https://pkg.go.dev/github.com/parsnips/spreader)
[![Go Report Card](https://goreportcard.com/badge/github.com/parsnips/spreader)](https://goreportcard.com/report/github.com/parsnips/spreader)
[![Coverage Status](https://coveralls.io/repos/github/parsnips/spreader/badge.svg?branch=main)](https://coveralls.io/github/parsnips/spreader?branch=main)

A Go library for rate-limited scheduling that spreads work evenly across a time horizon.

Given an item ID and the current time, `SpreadScheduler` finds the earliest second within a configurable time horizon where the item can be scheduled without exceeding a target rate limit. Scheduling is deterministic — the same ID at the same time always produces the same result, making it safe for use in distributed systems.

## Install

```
go get github.com/parsnips/spreader
```

## Usage

```go
// Create a scheduler: 10 items/second, 60-second horizon.
s, err := spreader.NewSpreadScheduler(10, 60)
if err != nil {
    log.Fatal(err)
}

// Schedule an item. Returns how far into the future it should run.
delay, err := s.Schedule(time.Now(), []byte("item-id"))
if err != nil {
    // errors.Is(err, spreader.ErrTimeHorizonExceeded)
    log.Fatal(err)
}
fmt.Printf("schedule in %v\n", delay)
```

### Options

Configure the number of internal hash buckets (default `1 << 20`):

```go
s, err := spreader.NewSpreadScheduler(10, 60, spreader.WithNumBuckets(1<<16))
```

### Distributed environments

When multiple publishers share the same rate limit, set the publisher count so each instance stays within bounds:

```go
s.SetPublisherCount(3) // 3 publishers splitting the rate limit
```

## Acknowledgments

Built on [webriots/rate](https://github.com/webriots/rate) for the underlying rotating token bucket limiter.

## License

MIT
