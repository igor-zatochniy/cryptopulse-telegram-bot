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
	cleanupCtx, cleanupCancel := context.WithTimeout(ctx, workers.RetentionCleanupRunTimeout)
	defer cleanupCancel()

	deletedJobs, err := a.cleanupNotificationJobHistory(cleanupCtx)
	if err != nil {
		slog.Error("failed to clean notification job history", "deleted", deletedJobs, "error", err)
	}

	deletedUpdates, err := a.cleanupTelegramUpdateHistory(cleanupCtx)
	if err != nil {
		slog.Error("failed to clean telegram update inbox history", "deleted", deletedUpdates, "error", err)
	}

	deletedReplies, err := a.cleanupTelegramReplyHistory(cleanupCtx)
	if err != nil {
		slog.Error("failed to clean Telegram reply outbox history", "deleted", deletedReplies, "error", err)
	}

	if deletedJobs > 0 || deletedUpdates > 0 || deletedReplies > 0 {
		slog.Info(
			"delivery history cleaned",
			"notification_jobs",
			deletedJobs,
			"telegram_updates",
			deletedUpdates,
			"telegram_replies",
			deletedReplies,
		)
	}
}

func (a *App) cleanupNotificationJobHistory(ctx context.Context) (int64, error) {
	return drainRetentionBatches(ctx, "notification_retention_cleanup", func(dbCtx context.Context) (int64, error) {
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
			) OR (
				status = 'canceled'
				AND canceled_at IS NOT NULL
				AND canceled_at < NOW() - $3::interval
			)
			ORDER BY COALESCE(sent_at, failed_at, canceled_at) ASC, id ASC
			LIMIT $4
		)
		DELETE FROM notification_jobs AS nj
		USING expired_jobs
		WHERE nj.id = expired_jobs.id`,
			workers.PostgresInterval(workers.NotificationSentRetention),
			workers.PostgresInterval(workers.NotificationFailedRetention),
			workers.PostgresInterval(workers.NotificationCanceledRetention),
			workers.RetentionCleanupLimit,
		)
		if err != nil {
			return 0, err
		}
		return result.RowsAffected()
	})
}

func (a *App) cleanupTelegramUpdateHistory(ctx context.Context) (int64, error) {
	return drainRetentionBatches(ctx, "telegram_update_retention_cleanup", func(dbCtx context.Context) (int64, error) {
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
			return 0, err
		}
		return result.RowsAffected()
	})
}

func (a *App) cleanupTelegramReplyHistory(ctx context.Context) (int64, error) {
	return drainRetentionBatches(ctx, "telegram_reply_retention_cleanup", func(dbCtx context.Context) (int64, error) {
		result, err := a.db.ExecContext(
			dbCtx,
			`WITH expired_replies AS (
			SELECT id
			FROM telegram_replies
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
		DELETE FROM telegram_replies AS tr
		USING expired_replies
		WHERE tr.id = expired_replies.id`,
			workers.PostgresInterval(workers.TelegramReplySentRetention),
			workers.PostgresInterval(workers.TelegramReplyFailedRetention),
			workers.RetentionCleanupLimit,
		)
		if err != nil {
			return 0, err
		}
		return result.RowsAffected()
	})
}

func drainRetentionBatches(
	ctx context.Context,
	operation string,
	deleteBatch func(context.Context) (int64, error),
) (int64, error) {
	cleanupCtx, cleanupCancel := context.WithTimeout(ctx, workers.RetentionTableCleanupTimeout)
	defer cleanupCancel()

	var totalDeleted int64
	for {
		deleted, err := deleteBatch(cleanupCtx)
		if err != nil {
			appmetrics.DBOperationsTotal.WithLabelValues(operation, "error").Inc()
			return totalDeleted, err
		}

		totalDeleted += deleted
		if deleted < int64(workers.RetentionCleanupLimit) {
			appmetrics.DBOperationsTotal.WithLabelValues(operation, "success").Inc()
			return totalDeleted, nil
		}
	}
}
