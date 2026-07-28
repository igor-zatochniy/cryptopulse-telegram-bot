package app

import (
	"context"
	"log/slog"
	"time"

	appmetrics "github.com/igor-zatochniy/cryptopulse-telegram-bot/internal/metrics"
	"github.com/igor-zatochniy/cryptopulse-telegram-bot/internal/workers"
)

func (a *App) startNotificationRetentionCleaner(ctx context.Context) {
	ticker := time.NewTicker(workers.RetentionCleanupInterval)
	defer ticker.Stop()

	slog.Info("notification job retention cleaner started")
	a.runNotificationRetentionCleanup(ctx)

	for {
		select {
		case <-ticker.C:
			a.runNotificationRetentionCleanup(ctx)
		case <-ctx.Done():
			slog.Info("notification job retention cleaner stopped")
			return
		}
	}
}

func (a *App) runNotificationRetentionCleanup(ctx context.Context) {
	deletedJobs, err := a.cleanupNotificationJobHistory(ctx)
	if err != nil {
		slog.Error("failed to clean notification job history", "error", err)
		return
	}

	deletedUpdates, err := a.cleanupTelegramUpdateHistory(ctx)
	if err != nil {
		slog.Error("failed to clean telegram update inbox history", "error", err)
		return
	}

	if deletedJobs > 0 || deletedUpdates > 0 {
		slog.Info("delivery history cleaned", "notification_jobs", deletedJobs, "telegram_updates", deletedUpdates)
	}
}

func (a *App) cleanupNotificationJobHistory(ctx context.Context) (int64, error) {
	dbCtx, dbCancel := context.WithTimeout(ctx, 10*time.Second)
	defer dbCancel()

	result, err := a.db.ExecContext(
		dbCtx,
		`WITH expired_jobs AS (
			SELECT id
			FROM notification_jobs
			WHERE (
				status = 'sent'
				AND sent_at IS NOT NULL
				AND sent_at < NOW() - $1::interval
			) OR (
				status = 'failed'
				AND failed_at IS NOT NULL
				AND failed_at < NOW() - $2::interval
			)
			ORDER BY COALESCE(sent_at, failed_at) ASC, id ASC
			LIMIT $3
		)
		DELETE FROM notification_jobs AS nj
		USING expired_jobs
		WHERE nj.id = expired_jobs.id`,
		workers.PostgresInterval(workers.NotificationSentRetention),
		workers.PostgresInterval(workers.NotificationFailedRetention),
		workers.RetentionCleanupLimit,
	)
	if err != nil {
		appmetrics.DBOperationsTotal.WithLabelValues("notification_retention_cleanup", "error").Inc()
		return 0, err
	}

	deleted, err := result.RowsAffected()
	if err != nil {
		appmetrics.DBOperationsTotal.WithLabelValues("notification_retention_cleanup", "error").Inc()
		return 0, err
	}

	appmetrics.DBOperationsTotal.WithLabelValues("notification_retention_cleanup", "success").Inc()
	return deleted, nil
}

func (a *App) cleanupTelegramUpdateHistory(ctx context.Context) (int64, error) {
	dbCtx, dbCancel := context.WithTimeout(ctx, 10*time.Second)
	defer dbCancel()

	result, err := a.db.ExecContext(
		dbCtx,
		`WITH expired_updates AS (
			SELECT update_id
			FROM telegram_updates
			WHERE (
				status = 'processed'
				AND processed_at IS NOT NULL
				AND processed_at < NOW() - $1::interval
			) OR (
				status = 'failed'
				AND failed_at IS NOT NULL
				AND failed_at < NOW() - $2::interval
			)
			ORDER BY COALESCE(processed_at, failed_at) ASC, update_id ASC
			LIMIT $3
		)
		DELETE FROM telegram_updates AS tu
		USING expired_updates
		WHERE tu.update_id = expired_updates.update_id`,
		workers.PostgresInterval(workers.TelegramUpdateProcessedRetention),
		workers.PostgresInterval(workers.TelegramUpdateFailedRetention),
		workers.RetentionCleanupLimit,
	)
	if err != nil {
		appmetrics.DBOperationsTotal.WithLabelValues("telegram_update_retention_cleanup", "error").Inc()
		return 0, err
	}

	deleted, err := result.RowsAffected()
	if err != nil {
		appmetrics.DBOperationsTotal.WithLabelValues("telegram_update_retention_cleanup", "error").Inc()
		return 0, err
	}

	appmetrics.DBOperationsTotal.WithLabelValues("telegram_update_retention_cleanup", "success").Inc()
	return deleted, nil
}
