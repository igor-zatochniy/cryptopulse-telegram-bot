package app

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestSentFinalizationRetriesTemporaryFailures(t *testing.T) {
	policy := sentFinalizationPolicy{
		budget: time.Second,
		delays: []time.Duration{0, 0, 0, 0},
	}

	finalizeCalls := 0
	attempts, err := policy.finalize(context.Background(), "test", 42, 7, func(context.Context) error {
		finalizeCalls++
		if finalizeCalls < 3 {
			return errors.New("temporary database error")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("finalize confirmed delivery: %v", err)
	}
	if attempts != 3 || finalizeCalls != 3 {
		t.Fatalf("attempts = %d, calls = %d; want 3, 3", attempts, finalizeCalls)
	}
}

func TestSentFinalizationRetryIsBounded(t *testing.T) {
	policy := sentFinalizationPolicy{
		budget: time.Second,
		delays: []time.Duration{0, 0, 0, 0},
	}

	finalizeCalls := 0
	attempts, err := policy.finalize(context.Background(), "test", 42, 7, func(context.Context) error {
		finalizeCalls++
		return errors.New("database unavailable")
	})
	if err == nil {
		t.Fatal("finalization error is nil")
	}
	if attempts != 5 || finalizeCalls != 5 {
		t.Fatalf("attempts = %d, calls = %d; want 5, 5", attempts, finalizeCalls)
	}
}

func TestSentFinalizationStopsAtBudget(t *testing.T) {
	policy := sentFinalizationPolicy{
		budget: 20 * time.Millisecond,
		delays: []time.Duration{time.Second},
	}

	finalizeCalls := 0
	startedAt := time.Now()
	attempts, err := policy.finalize(context.Background(), "test", 42, 7, func(context.Context) error {
		finalizeCalls++
		return errors.New("database unavailable")
	})
	if err == nil || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("finalization error = %v, want context deadline", err)
	}
	if attempts != 1 || finalizeCalls != 1 {
		t.Fatalf("attempts = %d, calls = %d; want 1, 1", attempts, finalizeCalls)
	}
	if elapsed := time.Since(startedAt); elapsed > 250*time.Millisecond {
		t.Fatalf("finalization exceeded bounded budget: %s", elapsed)
	}
}

func TestSentFinalizationDoesNotRetryLostOwnership(t *testing.T) {
	policy := sentFinalizationPolicy{
		budget: time.Second,
		delays: []time.Duration{0, 0, 0, 0},
	}

	finalizeCalls := 0
	attempts, err := policy.finalize(context.Background(), "test", 42, 7, func(context.Context) error {
		finalizeCalls++
		return errJobOwnershipLost
	})
	if !errors.Is(err, errJobOwnershipLost) {
		t.Fatalf("finalization error = %v, want ownership loss", err)
	}
	if attempts != 1 || finalizeCalls != 1 {
		t.Fatalf("attempts = %d, calls = %d; want 1, 1", attempts, finalizeCalls)
	}
}
