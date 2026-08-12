package app

import (
	"context"
	"errors"
	"testing"

	"github.com/igor-zatochniy/cryptopulse-telegram-bot/internal/workers"
)

func TestDrainRetentionBatchesContinuesUntilShortBatch(t *testing.T) {
	batches := []int64{
		workers.RetentionCleanupLimit,
		workers.RetentionCleanupLimit,
		500,
	}
	call := 0

	deleted, err := drainRetentionBatches(
		context.Background(),
		"retention_test",
		func(context.Context) (int64, error) {
			result := batches[call]
			call++
			return result, nil
		},
	)
	if err != nil {
		t.Fatalf("drain retention batches: %v", err)
	}
	if deleted != 2500 {
		t.Fatalf("deleted rows = %d, want 2500", deleted)
	}
	if call != len(batches) {
		t.Fatalf("cleanup calls = %d, want %d", call, len(batches))
	}
}

func TestDrainRetentionBatchesHonorsCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	deleted, err := drainRetentionBatches(
		ctx,
		"retention_cancel_test",
		func(batchCtx context.Context) (int64, error) {
			return 0, batchCtx.Err()
		},
	)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("cleanup error = %v, want %v", err, context.Canceled)
	}
	if deleted != 0 {
		t.Fatalf("deleted rows = %d, want 0", deleted)
	}
}
