# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

`spreader` is a Go library (not a binary) that provides a `SpreadScheduler` for rate-limited scheduling. Given an item ID and current time, it finds the earliest second within a configurable time horizon where the item can be scheduled without exceeding a target rate limit. It uses a rotating token bucket limiter from `github.com/webriots/rate` for deterministic, distributed-safe scheduling.

## Commands

- **Build:** `go build ./...`
- **Test:** `go test ./...`
- **Single test:** `go test -run TestName ./...`

## Architecture

Single-file library (`spreader.go`) exposing the `SpreadScheduler` interface with two methods:
- `Schedule(time.Time, []byte) (time.Duration, error)` — finds the next available second-granularity slot within the horizon
- `SetPublisherCount(int)` — adjusts the per-publisher share for distributed environments, including publisher counts above 255

The scheduler hashes `id + timestamp` into a rotating token bucket to get deterministic, collision-resistant slot assignment. The `publisherCount` multiplier ensures the per-publisher rate stays within bounds when scaling horizontally.
