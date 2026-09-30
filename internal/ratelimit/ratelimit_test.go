package ratelimit

import (
	"context"
	"testing"
	"time"
)

func TestConcurrencyLimitRejectsAndReleases(t *testing.T) {
	l := New(2, 0) // max 2 concurrent, no RPS cap
	r1, err := l.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	r2, err := l.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	_, err = l.Acquire(context.Background())
	if err == nil {
		t.Fatal("expected third concurrent call to be rejected")
	}
	q, ok := IsRateLimit(err)
	if !ok || q.Reason != ReasonMaxConcurrent {
		t.Fatalf("expected max_concurrent rejection, got %v", err)
	}
	r1()
	r3, err := l.Acquire(context.Background())
	if err != nil {
		t.Fatalf("slot should reopen after release: %v", err)
	}
	r3()
	r2()
}

func TestRPSLimitRejectsBurst(t *testing.T) {
	l := New(0, 3) // 3 rps; token bucket starts full
	for i := 0; i < 3; i++ {
		r, err := l.Acquire(context.Background())
		if err != nil {
			t.Fatalf("call %d should pass: %v", i, err)
		}
		r()
	}
	_, err := l.Acquire(context.Background())
	q, ok := IsRateLimit(err)
	if !ok || q.Reason != ReasonRPS {
		t.Fatalf("expected rps rejection, got %v", err)
	}
}

func TestRPSRefillsOverTime(t *testing.T) {
	base := time.Now()
	cur := base
	l := New(0, 10) // 10 rps
	l.now = func() time.Time { return cur }
	l.lastRefill = base
	// drain bucket
	for i := 0; i < 10; i++ {
		if _, err := l.Acquire(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := l.Acquire(context.Background()); err == nil {
		t.Fatal("bucket should be empty")
	}
	// advance 500ms -> 5 tokens
	cur = cur.Add(500 * time.Millisecond)
	for i := 0; i < 5; i++ {
		if _, err := l.Acquire(context.Background()); err != nil {
			t.Fatalf("refilled call %d should pass: %v", i, err)
		}
	}
	if _, err := l.Acquire(context.Background()); err == nil {
		t.Fatal("only ~5 tokens should have refilled")
	}
}

func TestUnlimitedAllowsAll(t *testing.T) {
	l := New(0, 0)
	for i := 0; i < 1000; i++ {
		if _, err := l.Acquire(context.Background()); err != nil {
			t.Fatalf("unexpected rejection: %v", err)
		}
	}
}
