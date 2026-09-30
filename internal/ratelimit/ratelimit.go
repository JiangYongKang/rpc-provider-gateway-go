// Package ratelimit enforces gateway call quotas: a maximum number of
// concurrent in-flight calls and a sustained requests-per-second limit via a
// simple token bucket. Rejections are explicit and categorized, never silent.
package ratelimit

import (
	"context"
	"errors"
	"sync"
	"time"
)

// Reason identifies which quota rejected a call.
type Reason string

// Rejection reasons.
const (
	ReasonMaxConcurrent Reason = "max_concurrent"
	ReasonRPS           Reason = "requests_per_second"
)

// ErrQuotaExceeded is returned when Acquire cannot take a slot immediately.
type ErrQuotaExceeded struct {
	Reason Reason
}

func (e *ErrQuotaExceeded) Error() string {
	if e.Reason == ReasonRPS {
		return "request rejected: requests-per-second quota exceeded"
	}
	return "request rejected: max concurrent calls quota exceeded"
}

// IsRateLimit reports whether err is a quota rejection.
func IsRateLimit(err error) (*ErrQuotaExceeded, bool) {
	var q *ErrQuotaExceeded
	return q, errors.As(err, &q)
}

// Limiter enforces both quotas.
type Limiter struct {
	maxConcurrent int
	inFlight      chan struct{}

	mu         sync.Mutex
	tokens     float64
	maxTokens  float64
	refillRate float64 // tokens per second
	lastRefill time.Time
	now        func() time.Time
}

// New builds a Limiter. A non-positive value means that dimension is
// unlimited.
func New(maxConcurrent, requestsPerSecond int) *Limiter {
	l := &Limiter{
		maxConcurrent: maxConcurrent,
		now:           time.Now,
	}
	if maxConcurrent > 0 {
		l.inFlight = make(chan struct{}, maxConcurrent)
	}
	if requestsPerSecond > 0 {
		l.maxTokens = float64(requestsPerSecond)
		l.tokens = float64(requestsPerSecond)
		l.refillRate = float64(requestsPerSecond)
		l.lastRefill = time.Now()
	}
	return l
}

// Acquire tries to take a slot without blocking. On success it returns a
// non-nil release func that must be called when the call finishes. On quota
// exhaustion it returns *ErrQuotaExceeded so the caller can reject the call
// explicitly with the right category.
func (l *Limiter) Acquire(ctx context.Context) (func(), error) {
	// 1) concurrency slot
	if l.inFlight != nil {
		select {
		case l.inFlight <- struct{}{}:
		default:
			return nil, &ErrQuotaExceeded{Reason: ReasonMaxConcurrent}
		}
	}
	releaseSlot := func() {
		if l.inFlight != nil {
			<-l.inFlight
		}
	}

	// 2) rate (RPS) token
	if l.refillRate > 0 {
		if !l.takeToken() {
			releaseSlot()
			return nil, &ErrQuotaExceeded{Reason: ReasonRPS}
		}
	}
	return releaseSlot, nil
}

// takeToken attempts to consume one token, refilling based on elapsed time.
func (l *Limiter) takeToken() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := l.now()
	elapsed := now.Sub(l.lastRefill).Seconds()
	if elapsed > 0 {
		l.tokens += elapsed * l.refillRate
		if l.tokens > l.maxTokens {
			l.tokens = l.maxTokens
		}
		l.lastRefill = now
	}
	if l.tokens >= 1 {
		l.tokens--
		return true
	}
	return false
}

// InFlight returns the number of calls currently holding a slot.
func (l *Limiter) InFlight() int {
	if l.inFlight == nil {
		return 0
	}
	return len(l.inFlight)
}
