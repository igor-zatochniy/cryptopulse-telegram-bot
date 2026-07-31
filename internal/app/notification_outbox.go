package app

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"

	appmetrics "github.com/igor-zatochniy/cryptopulse-telegram-bot/internal/metrics"
	apptelegram "github.com/igor-zatochniy/cryptopulse-telegram-bot/internal/telegram"
	"github.com/igor-zatochniy/cryptopulse-telegram-bot/internal/workers"
)

func (a *App) alertWorker(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()

	pollTicker := time.NewTicker(workers.NotificationJobPollInterval)
	defer pollTicker.Stop()

	for {
		job, err := a.claimPendingNotificationJob(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}

			slog.Error("failed to claim pending notification job", "error", err)
			select {
			case <-pollTicker.C:
				continue
			case <-ctx.Done():
				return
			}
		}

		if job != nil {
			a.processNotificationJob(ctx, *job)
			continue
		}

		select {
		case <-pollTicker.C:
		case <-ctx.Done():
			return
		}
	}
}

func ensureCurrentJobClaimUpdated(result sql.Result, operation string) error {
	affectedRows, err := result.RowsAffected()
	if err != nil {
		appmetrics.DBOperationsTotal.WithLabelValues(operation, "error").Inc()
		return err
	}
	if affectedRows == 0 {
		appmetrics.DBOperationsTotal.WithLabelValues(operation, "stale_claim").Inc()
		return errJobOwnershipLost
	}
	return nil
}

func (a *App) createCronNotificationJobs(ctx context.Context) (int, error) {
	dbCtx, dbCancel := context.WithTimeout(ctx, 5*time.Second)
	defer dbCancel()

	tx, err := a.db.BeginTx(dbCtx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		appmetrics.DBOperationsTotal.WithLabelValues("create_notification_jobs", "error").Inc()
		return 0, err
	}
	defer func() {
		_ = tx.Rollback()
	}()

	rows, err := tx.QueryContext(dbCtx, `WITH claim_clock AS (
		SELECT NOW() AS claimed_at
	), due AS (
		SELECT s.chat_id,
		       COALESCE(s.language_code, 'ua') AS language_code,
		       claim_clock.claimed_at
		FROM subscribers AS s
		CROSS JOIN claim_clock
		WHERE s.is_subscribed = TRUE
		AND date_trunc('minute', COALESCE(s.last_sent, TIMESTAMPTZ 'epoch')) <= date_trunc('minute', claim_clock.claimed_at) - (COALESCE(s.interval_minutes, 60) * INTERVAL '1 minute')
		AND (s.cron_claimed_until IS NULL OR s.cron_claimed_until < claim_clock.claimed_at)
		AND (s.delivery_suspended_until IS NULL OR s.delivery_suspended_until <= claim_clock.claimed_at)
		AND NOT EXISTS (
			SELECT 1
			FROM notification_jobs AS nj
			WHERE nj.chat_id = s.chat_id
			AND nj.status IN ('pending', 'sending')
		)
		ORDER BY s.last_sent ASC NULLS FIRST
		LIMIT $1
		FOR UPDATE OF s SKIP LOCKED
	), claimed AS (
		UPDATE subscribers AS s
		SET cron_claimed_until = due.claimed_at + INTERVAL '15 minute'
		FROM due
		WHERE s.chat_id = due.chat_id
		RETURNING s.chat_id, due.language_code, due.claimed_at
	)
	SELECT chat_id, language_code, claimed_at FROM claimed`, workers.CronBatchLimit)
	if err != nil {
		appmetrics.DBOperationsTotal.WithLabelValues("create_notification_jobs", "error").Inc()
		return 0, err
	}
	defer rows.Close()

	type rowData struct {
		chatID int64
		lang   string
		at     time.Time
	}

	var dueRows []rowData
	for rows.Next() {
		var r rowData
		if err := rows.Scan(&r.chatID, &r.lang, &r.at); err != nil {
			appmetrics.DBOperationsTotal.WithLabelValues("create_notification_jobs", "error").Inc()
			return 0, err
		}
		dueRows = append(dueRows, r)
	}
	if err := rows.Err(); err != nil {
		appmetrics.DBOperationsTotal.WithLabelValues("create_notification_jobs", "error").Inc()
		return 0, err
	}

	for _, due := range dueRows {
		pricesTextLocal := a.getFormattedPricesFromCache(due.lang)
		header := fmt.Sprintf(
			apptelegram.Text(due.lang, "alert_hdr"),
			a.formatScheduledNotificationTime(due.at),
		)
		text := fmt.Sprintf(
			"%s\n\n%s\n\n_%s_",
			header,
			pricesTextLocal,
			apptelegram.Text(due.lang, "dynamics"),
		)

		if _, err := tx.ExecContext(
			dbCtx,
			`INSERT INTO notification_jobs (
				chat_id,
				language_code,
				message_text,
				scheduled_at,
				status,
				next_attempt_at
			) VALUES ($1, $2, $3, $4, 'pending', $4)`,
			due.chatID,
			due.lang,
			text,
			due.at,
		); err != nil {
			appmetrics.DBOperationsTotal.WithLabelValues("create_notification_jobs", "error").Inc()
			return 0, err
		}
	}

	if err := tx.Commit(); err != nil {
		appmetrics.DBOperationsTotal.WithLabelValues("create_notification_jobs", "error").Inc()
		return 0, err
	}

	appmetrics.DBOperationsTotal.WithLabelValues("create_notification_jobs", "success").Inc()
	return len(dueRows), nil
}

func (a *App) formatScheduledNotificationTime(scheduledAt time.Time) string {
	return scheduledAt.In(a.kyivLoc).Format("15:04")
}

func (a *App) claimPendingNotificationJob(ctx context.Context) (*NotificationJob, error) {
	dbCtx, dbCancel := context.WithTimeout(ctx, 5*time.Second)
	defer dbCancel()

	tx, err := a.db.BeginTx(dbCtx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		appmetrics.DBOperationsTotal.WithLabelValues("claim_notification_job", "error").Inc()
		return nil, err
	}
	defer func() {
		_ = tx.Rollback()
	}()

	var job NotificationJob
	err = tx.QueryRowContext(dbCtx, `WITH next_job AS (
		SELECT nj.id
		FROM notification_jobs AS nj
		WHERE nj.status IN ('pending', 'sending')
		AND nj.next_attempt_at <= NOW()
		AND (
			nj.status = 'pending'
			OR nj.claimed_until IS NULL
			OR nj.claimed_until < NOW()
		)
		ORDER BY nj.scheduled_at ASC, nj.id ASC
		LIMIT 1
		FOR UPDATE OF nj SKIP LOCKED
	), claimed AS (
		UPDATE notification_jobs AS nj
		SET status = 'sending',
		    attempts = nj.attempts + 1,
		    claim_token = gen_random_uuid(),
		    claimed_until = NOW() + $1::interval,
		    updated_at = NOW()
		FROM next_job
		WHERE nj.id = next_job.id
		RETURNING nj.id, nj.chat_id, nj.language_code, nj.message_text, nj.claim_token::text, nj.scheduled_at, nj.attempts
	)
	SELECT id, chat_id, language_code, message_text, claim_token, scheduled_at, attempts FROM claimed`, workers.PostgresInterval(workers.NotificationJobClaimWindow)).Scan(
		&job.ID,
		&job.ChatID,
		&job.Lang,
		&job.Text,
		&job.ClaimToken,
		&job.ScheduledAt,
		&job.Attempts,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		appmetrics.DBOperationsTotal.WithLabelValues("claim_notification_job", "error").Inc()
		return nil, err
	}

	if err := tx.Commit(); err != nil {
		appmetrics.DBOperationsTotal.WithLabelValues("claim_notification_job", "error").Inc()
		return nil, err
	}

	appmetrics.DBOperationsTotal.WithLabelValues("claim_notification_job", "success").Inc()
	return &job, nil
}

func (a *App) processNotificationJob(ctx context.Context, job NotificationJob) {
	if processingErr := ctx.Err(); processingErr != nil {
		if markErr := a.markNotificationJobRetry(ctx, job, processingErr); markErr != nil {
			slog.Error("failed to defer canceled notification job", "job_id", job.ID, "error", markErr)
		}
		return
	}

	lockCtx, lockCancel := context.WithTimeout(ctx, 10*time.Second)
	lockConn, lockKey, err := a.acquireTelegramChatAdvisoryLock(lockCtx, job.ChatID)
	lockCancel()
	if err != nil {
		a.deferNotificationJob(ctx, job, fmt.Errorf("acquire telegram chat lock: %w", err))
		return
	}
	defer releaseTelegramChatAdvisoryLock(ctx, lockConn, lockKey)

	subscribed, err := a.isSubscribed(ctx, job.ChatID)
	if err != nil {
		a.deferNotificationJob(ctx, job, fmt.Errorf("check subscription before delivery: %w", err))
		return
	}
	if !subscribed {
		if err := a.markNotificationJobCanceled(ctx, job); err != nil {
			if errors.Is(err, errJobOwnershipLost) {
				slog.Info("notification job was canceled before delivery", "job_id", job.ID)
			} else {
				slog.Error("failed to persist canceled notification job", "job_id", job.ID, "error", err)
			}
		}
		appmetrics.CronDeliveriesTotal.WithLabelValues("canceled").Inc()
		return
	}

	msg := tgbotapi.NewMessage(job.ChatID, job.Text)
	msg.ParseMode = "Markdown"
	msg.ReplyMarkup = apptelegram.RefreshKeyboard(job.Lang)

	if _, err := a.bot.Send(msg); err != nil {
		errorType := "transient"
		slog.Error("failed to send scheduled alert", "chat_id", job.ChatID, "error", err)

		if apptelegram.IsPermanentSendError(err) {
			errorType = "permanent"
			if markErr := a.markNotificationJobFailed(ctx, job, err, true); markErr != nil {
				if errors.Is(markErr, errJobOwnershipLost) {
					slog.Warn("ignored stale notification failure result", "job_id", job.ID, "attempts", job.Attempts)
				} else {
					slog.Error("failed to persist permanent notification failure", "chat_id", job.ChatID, "error", markErr)
				}
			}
		} else if job.Attempts >= workers.NotificationJobMaxAttempts {
			errorType = "exhausted"
			if markErr := a.markNotificationJobFailed(ctx, job, err, false); markErr != nil {
				if errors.Is(markErr, errJobOwnershipLost) {
					slog.Warn("ignored stale notification failure result", "job_id", job.ID, "attempts", job.Attempts)
				} else {
					slog.Error("failed to persist exhausted notification failure", "chat_id", job.ChatID, "error", markErr)
				}
			}
		} else {
			if markErr := a.markNotificationJobRetry(ctx, job, err); markErr != nil {
				if errors.Is(markErr, errJobOwnershipLost) {
					slog.Warn("ignored stale notification retry result", "job_id", job.ID, "attempts", job.Attempts)
				} else {
					slog.Error("failed to persist notification retry", "chat_id", job.ChatID, "error", markErr)
				}
			}
		}

		appmetrics.TelegramSendErrorsTotal.WithLabelValues(errorType).Inc()
		appmetrics.CronDeliveriesTotal.WithLabelValues("failed_" + errorType).Inc()
		return
	}

	if err := a.markNotificationJobSent(ctx, job); err != nil {
		if errors.Is(err, errJobOwnershipLost) {
			slog.Warn("ignored stale notification success result", "job_id", job.ID, "attempts", job.Attempts)
			appmetrics.CronDeliveriesTotal.WithLabelValues("sent_stale_claim").Inc()
			return
		}

		slog.Error("failed to persist successful notification delivery", "chat_id", job.ChatID, "error", err)
		appmetrics.CronDeliveriesTotal.WithLabelValues("sent_persist_error").Inc()
		return
	}

	appmetrics.CronDeliveriesTotal.WithLabelValues("sent").Inc()
}

func (a *App) deferNotificationJob(ctx context.Context, job NotificationJob, processingErr error) {
	if job.Attempts >= workers.NotificationJobMaxAttempts {
		if err := a.markNotificationJobFailed(ctx, job, processingErr, false); err != nil {
			if errors.Is(err, errJobOwnershipLost) {
				slog.Warn("ignored stale notification failure result", "job_id", job.ID, "attempts", job.Attempts)
			} else {
				slog.Error("failed to persist notification failure", "job_id", job.ID, "error", err)
			}
		}
		appmetrics.CronDeliveriesTotal.WithLabelValues("failed_exhausted").Inc()
		return
	}

	if err := a.markNotificationJobRetry(ctx, job, processingErr); err != nil {
		if errors.Is(err, errJobOwnershipLost) {
			slog.Warn("ignored stale notification retry result", "job_id", job.ID, "attempts", job.Attempts)
		} else {
			slog.Error("failed to persist notification retry", "job_id", job.ID, "error", err)
		}
	}
	appmetrics.CronDeliveriesTotal.WithLabelValues("retry").Inc()
}

func (a *App) markNotificationJobSent(ctx context.Context, job NotificationJob) error {
	dbCtx, dbCancel := finalizationContext(ctx, 5*time.Second)
	defer dbCancel()

	tx, err := a.db.BeginTx(dbCtx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		appmetrics.DBOperationsTotal.WithLabelValues("mark_notification_sent", "error").Inc()
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()

	result, err := tx.ExecContext(
		dbCtx,
		`UPDATE notification_jobs
		 SET status = 'sent',
		     sent_at = NOW(),
		     claim_token = NULL,
		     claimed_until = NULL,
		     last_error = NULL,
		     updated_at = NOW()
		 WHERE id = $1
		 AND status = 'sending'
		 AND claim_token = $2::uuid
		 AND attempts = $3`,
		job.ID,
		job.ClaimToken,
		job.Attempts,
	)
	if err != nil {
		appmetrics.DBOperationsTotal.WithLabelValues("mark_notification_sent", "error").Inc()
		return err
	}
	if err := ensureCurrentJobClaimUpdated(result, "mark_notification_sent"); err != nil {
		return err
	}

	if _, err := tx.ExecContext(
		dbCtx,
		`UPDATE subscribers
		 SET last_sent = date_trunc('minute', $2::timestamptz),
		     cron_claimed_until = NULL,
		     delivery_suspended_until = NULL
		 WHERE chat_id = $1`,
		job.ChatID,
		job.ScheduledAt,
	); err != nil {
		appmetrics.DBOperationsTotal.WithLabelValues("mark_notification_sent", "error").Inc()
		return err
	}

	if err := tx.Commit(); err != nil {
		appmetrics.DBOperationsTotal.WithLabelValues("mark_notification_sent", "error").Inc()
		return err
	}

	appmetrics.DBOperationsTotal.WithLabelValues("mark_notification_sent", "success").Inc()
	return nil
}

func (a *App) markNotificationJobRetry(ctx context.Context, job NotificationJob, sendErr error) error {
	dbCtx, dbCancel := finalizationContext(ctx, 5*time.Second)
	defer dbCancel()

	tx, err := a.db.BeginTx(dbCtx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		appmetrics.DBOperationsTotal.WithLabelValues("mark_notification_retry", "error").Inc()
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()

	delay := workers.RetryDelay(job.Attempts)
	result, err := tx.ExecContext(
		dbCtx,
		`UPDATE notification_jobs
		 SET status = 'pending',
		     claim_token = NULL,
		     claimed_until = NULL,
		     next_attempt_at = NOW() + $2::interval,
		     last_error = $3,
		     updated_at = NOW()
		 WHERE id = $1
		 AND status = 'sending'
		 AND claim_token = $4::uuid
		 AND attempts = $5`,
		job.ID,
		workers.PostgresInterval(delay),
		workers.TruncateError(sendErr.Error(), 500),
		job.ClaimToken,
		job.Attempts,
	)
	if err != nil {
		appmetrics.DBOperationsTotal.WithLabelValues("mark_notification_retry", "error").Inc()
		return err
	}
	if err := ensureCurrentJobClaimUpdated(result, "mark_notification_retry"); err != nil {
		return err
	}

	// Retry захищає сам outbox job, тому subscriber claim можна звільнити
	// і не блокувати майбутні cron cycles довше, ніж потрібно.
	if _, err := tx.ExecContext(
		dbCtx,
		`UPDATE subscribers
		 SET cron_claimed_until = NULL
		 WHERE chat_id = $1`,
		job.ChatID,
	); err != nil {
		appmetrics.DBOperationsTotal.WithLabelValues("mark_notification_retry", "error").Inc()
		return err
	}

	if err := tx.Commit(); err != nil {
		appmetrics.DBOperationsTotal.WithLabelValues("mark_notification_retry", "error").Inc()
		return err
	}

	appmetrics.DBOperationsTotal.WithLabelValues("mark_notification_retry", "success").Inc()
	return nil
}

func (a *App) markNotificationJobCanceled(ctx context.Context, job NotificationJob) error {
	dbCtx, dbCancel := finalizationContext(ctx, 5*time.Second)
	defer dbCancel()

	tx, err := a.db.BeginTx(dbCtx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		appmetrics.DBOperationsTotal.WithLabelValues("mark_notification_canceled", "error").Inc()
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()

	result, err := tx.ExecContext(
		dbCtx,
		`UPDATE notification_jobs
		 SET status = 'canceled',
		     canceled_at = NOW(),
		     claim_token = NULL,
		     claimed_until = NULL,
		     last_error = 'subscriber is not active',
		     updated_at = NOW()
		 WHERE id = $1
		 AND status = 'sending'
		 AND claim_token = $2::uuid
		 AND attempts = $3`,
		job.ID,
		job.ClaimToken,
		job.Attempts,
	)
	if err != nil {
		appmetrics.DBOperationsTotal.WithLabelValues("mark_notification_canceled", "error").Inc()
		return err
	}
	if err := ensureCurrentJobClaimUpdated(result, "mark_notification_canceled"); err != nil {
		return err
	}

	if _, err := tx.ExecContext(
		dbCtx,
		`UPDATE subscribers
		 SET cron_claimed_until = NULL
		 WHERE chat_id = $1`,
		job.ChatID,
	); err != nil {
		appmetrics.DBOperationsTotal.WithLabelValues("mark_notification_canceled", "error").Inc()
		return err
	}

	if err := tx.Commit(); err != nil {
		appmetrics.DBOperationsTotal.WithLabelValues("mark_notification_canceled", "error").Inc()
		return err
	}

	appmetrics.DBOperationsTotal.WithLabelValues("mark_notification_canceled", "success").Inc()
	return nil
}

func (a *App) markNotificationJobFailed(
	ctx context.Context,
	job NotificationJob,
	sendErr error,
	permanent bool,
) error {
	dbCtx, dbCancel := finalizationContext(ctx, 5*time.Second)
	defer dbCancel()

	tx, err := a.db.BeginTx(dbCtx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		appmetrics.DBOperationsTotal.WithLabelValues("mark_notification_failed", "error").Inc()
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()

	result, err := tx.ExecContext(
		dbCtx,
		`UPDATE notification_jobs
		 SET status = 'failed',
		     failed_at = NOW(),
		     claim_token = NULL,
		     claimed_until = NULL,
		     last_error = $2,
		     updated_at = NOW()
		 WHERE id = $1
		 AND status = 'sending'
		 AND claim_token = $3::uuid
		 AND attempts = $4`,
		job.ID,
		workers.TruncateError(sendErr.Error(), 500),
		job.ClaimToken,
		job.Attempts,
	)
	if err != nil {
		appmetrics.DBOperationsTotal.WithLabelValues("mark_notification_failed", "error").Inc()
		return err
	}
	if err := ensureCurrentJobClaimUpdated(result, "mark_notification_failed"); err != nil {
		return err
	}

	if permanent {
		if _, err := tx.ExecContext(
			dbCtx,
			`UPDATE subscribers
			 SET is_subscribed = FALSE,
			     cron_claimed_until = NULL,
			     delivery_suspended_until = NULL
			 WHERE chat_id = $1`,
			job.ChatID,
		); err != nil {
			appmetrics.DBOperationsTotal.WithLabelValues("mark_notification_failed", "error").Inc()
			return err
		}
	} else {
		if _, err := tx.ExecContext(
			dbCtx,
			`UPDATE subscribers
			 SET cron_claimed_until = NULL,
			     delivery_suspended_until = NOW() + $2::interval
			 WHERE chat_id = $1`,
			job.ChatID,
			workers.PostgresInterval(workers.NotificationFailureCooldown),
		); err != nil {
			appmetrics.DBOperationsTotal.WithLabelValues("mark_notification_failed", "error").Inc()
			return err
		}
	}

	if err := tx.Commit(); err != nil {
		appmetrics.DBOperationsTotal.WithLabelValues("mark_notification_failed", "error").Inc()
		return err
	}

	appmetrics.DBOperationsTotal.WithLabelValues("mark_notification_failed", "success").Inc()
	return nil
}
