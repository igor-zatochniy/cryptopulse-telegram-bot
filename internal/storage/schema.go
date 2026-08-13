package storage

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"
)

// VerifySchema перевіряє об'єкти БД, без яких поточна версія сервісу не може працювати коректно.
func VerifySchema(ctx context.Context, db *sql.DB) error {
	checkCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	var (
		subscribersTable       bool
		marketPricesTable      bool
		notificationJobsTable  bool
		telegramUpdatesTable   bool
		telegramRepliesTable   bool
		deliveryCooldownColumn bool
		jobClaimTokenColumn    bool
		jobCanceledAtColumn    bool
		updateShardColumn      bool
		marketPriceConstraint  bool
	)

	err := db.QueryRowContext(checkCtx, `SELECT
		to_regclass('public.subscribers') IS NOT NULL,
		to_regclass('public.market_prices') IS NOT NULL,
		to_regclass('public.notification_jobs') IS NOT NULL,
		to_regclass('public.telegram_updates') IS NOT NULL,
		to_regclass('public.telegram_replies') IS NOT NULL,
		EXISTS (
			SELECT 1
			FROM information_schema.columns
			WHERE table_schema = 'public'
			AND table_name = 'subscribers'
			AND column_name = 'delivery_suspended_until'
		),
		EXISTS (
			SELECT 1
			FROM information_schema.columns
			WHERE table_schema = 'public'
			AND table_name = 'notification_jobs'
			AND column_name = 'claim_token'
		),
		EXISTS (
			SELECT 1
			FROM information_schema.columns
			WHERE table_schema = 'public'
			AND table_name = 'notification_jobs'
			AND column_name = 'canceled_at'
		),
		EXISTS (
			SELECT 1
			FROM information_schema.columns
			WHERE table_schema = 'public'
			AND table_name = 'telegram_updates'
			AND column_name = 'shard_id'
		),
		EXISTS (
			SELECT 1
			FROM pg_constraint
			WHERE conrelid = 'public.market_prices'::regclass
			AND conname = 'market_prices_price_check'
		)`).Scan(
		&subscribersTable,
		&marketPricesTable,
		&notificationJobsTable,
		&telegramUpdatesTable,
		&telegramRepliesTable,
		&deliveryCooldownColumn,
		&jobClaimTokenColumn,
		&jobCanceledAtColumn,
		&updateShardColumn,
		&marketPriceConstraint,
	)
	if err != nil {
		return fmt.Errorf("inspect database schema: %w", err)
	}

	required := []struct {
		name  string
		ready bool
	}{
		{"table subscribers", subscribersTable},
		{"table market_prices", marketPricesTable},
		{"table notification_jobs", notificationJobsTable},
		{"table telegram_updates", telegramUpdatesTable},
		{"table telegram_replies", telegramRepliesTable},
		{"column subscribers.delivery_suspended_until", deliveryCooldownColumn},
		{"column notification_jobs.claim_token", jobClaimTokenColumn},
		{"column notification_jobs.canceled_at", jobCanceledAtColumn},
		{"column telegram_updates.shard_id", updateShardColumn},
		{"constraint market_prices_price_check", marketPriceConstraint},
	}

	var missing []string
	for _, object := range required {
		if !object.ready {
			missing = append(missing, object.name)
		}
	}
	if len(missing) > 0 {
		return fmt.Errorf(
			"missing required database objects after migration: %s",
			strings.Join(missing, ", "),
		)
	}

	return nil
}
