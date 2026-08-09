package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"
)

const sentFinalizationBudget = 5 * time.Second

var sentFinalizationRetryDelays = [...]time.Duration{
	100 * time.Millisecond,
	250 * time.Millisecond,
	500 * time.Millisecond,
	1 * time.Second,
}

type sentFinalizationPolicy struct {
	budget time.Duration
	delays []time.Duration
}

var defaultSentFinalizationPolicy = sentFinalizationPolicy{
	budget: sentFinalizationBudget,
	delays: sentFinalizationRetryDelays[:],
}

// Finalization retries only the durable DB write after Telegram has confirmed delivery.
func (p sentFinalizationPolicy) finalize(
	ctx context.Context,
	outbox string,
	jobID int64,
	telegramMessageID int,
	finalize func(context.Context) error,
) (int, error) {
	if p.budget <= 0 {
		return 0, fmt.Errorf("invalid sent finalization budget %s", p.budget)
	}

	finalCtx, cancel := finalizationContext(ctx, p.budget)
	defer cancel()

	maxAttempts := len(p.delays) + 1
	var lastErr error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		if err := finalCtx.Err(); err != nil {
			return attempt - 1, errors.Join(lastErr, err)
		}

		lastErr = finalize(finalCtx)
		if lastErr == nil || errors.Is(lastErr, errJobOwnershipLost) {
			return attempt, lastErr
		}
		if attempt == maxAttempts {
			return attempt, lastErr
		}

		slog.Warn(
			"Telegram delivery confirmed; database finalization will retry",
			"outbox",
			outbox,
			"job_id",
			jobID,
			"telegram_message_id",
			telegramMessageID,
			"attempt",
			attempt,
			"max_attempts",
			maxAttempts,
			"error",
			lastErr,
		)

		timer := time.NewTimer(p.delays[attempt-1])
		select {
		case <-timer.C:
		case <-finalCtx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return attempt, errors.Join(lastErr, finalCtx.Err())
		}
	}

	return maxAttempts, lastErr
}
