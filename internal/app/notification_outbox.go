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

	rows, err := tx.QueryContext(dbCtx, `SELECT nj.id, nj.chat_id
		FROM notification_jobs AS nj
		WHERE nj.status IN ('pending', 'sending')
		AND nj.next_attempt_at <= NOW()
		AND (
			nj.status = 'pending'
			OR nj.claimed_until IS NULL
			OR nj.claimed_until < NOW()
		)
		ORDER BY nj.scheduled_at ASC, nj.id ASC
		LIMIT $1`, workers.OutboxClaimCandidateLimit)
	if err != nil {
		appmetrics.DBOperationsTotal.WithLabelValues("claim_notification_job", "error").Inc()
		return nil, err
	}
	type notificationCandidate struct {
		id     int64
		chatID int64
	}
	candidates := make([]notificationCandidate, 0, workers.OutboxClaimCandidateLimit)
	for rows.Next() {
		var candidate notificationCandidate
		if err := rows.Scan(&candidate.id, &candidate.chatID); err != nil {
			_ = rows.Close()
			appmetrics.DBOperationsTotal.WithLabelValues("claim_notification_job", "error").Inc()
			return nil, err
		}
		candidates = append(candidates, candidate)
	}
	if err := rows.Close(); err != nil {
		appmetrics.DBOperationsTotal.WithLabelValues("claim_notification_job", "error").Inc()
		return nil, err
	}
	if err := rows.Err(); err != nil {
		appmetrics.DBOperationsTotal.WithLabelValues("claim_notification_job", "error").Inc()
		return nil, err
	}

	for _, candidate := range candidates {
		locked, err := tryTelegramChatTransactionLock(dbCtx, tx, candidate.chatID)
		if err != nil {
			appmetrics.DBOperationsTotal.WithLabelValues("claim_notification_job", "error").Inc()
			return nil, err
		}
		if !locked {
			continue
		}

		var job NotificationJob
		err = tx.QueryRowContext(dbCtx, `UPDATE notification_jobs AS nj
		SET status = 'sending',
		    attempts = nj.attempts + 1,
		    claim_token = gen_random_uuid(),
		    claimed_until = NOW() + $1::interval,
		    updated_at = NOW()
		WHERE nj.id = $2
		AND nj.status IN ('pending', 'sending')
		AND nj.next_attempt_at <= NOW()
		AND (
			nj.status = 'pending'
			OR nj.claimed_until IS NULL
			OR nj.claimed_until < NOW()
		)
		RETURNING nj.id, nj.chat_id, nj.language_code, nj.message_text, nj.claim_token::text, nj.scheduled_at, nj.attempts
	`, workers.PostgresInterval(workers.NotificationJobClaimWindow), candidate.id).Scan(
			&job.ID,
			&job.ChatID,
			&job.Lang,
			&job.Text,
			&job.ClaimToken,
			&job.ScheduledAt,
			&job.Attempts,
		)
		if errors.Is(err, sql.ErrNoRows) {
			continue
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

	appmetrics.DBOperationsTotal.WithLabelValues("claim_notification_job", "no_claimable_candidate").Inc()
	return nil, nil
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

	subscriptionCtx, subscriptionCancel := context.WithTimeout(
		ctx,
		workers.NotificationSubscriptionCheckTimeout,
	)
	subscribed, err := a.isSubscribed(subscriptionCtx, job.ChatID)
	subscriptionCancel()
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

	claimCtx, claimCancel := context.WithTimeout(ctx, workers.NotificationClaimValidationTimeout)
	err = a.ensureNotificationJobClaimCurrent(claimCtx, job)
	claimCancel()
	if err != nil {
		if errors.Is(err, errJobOwnershipLost) {
			slog.Warn("skipped notification send for stale claim", "job_id", job.ID, "attempts", job.Attempts)
			appmetrics.CronDeliveriesTotal.WithLabelValues("skipped_stale_claim").Inc()
			return
		}
		a.deferNotificationJob(ctx, job, fmt.Errorf("validate notification claim before delivery: %w", err))
		return
	}

	msg := tgbotapi.NewMessage(job.ChatID, notificationMessageForDelivery(job, time.Now().UTC()))
	msg.ParseMode = "Markdown"
	msg.ReplyMarkup = apptelegram.RefreshKeyboard(job.Lang)

	sentMessage, err := a.bot.Send(msg)
	if err != nil {
		errorType := "transient"
		safeErr := a.safeTelegramError(err)
		slog.Error("failed to send scheduled alert", "chat_id", job.ChatID, "error", safeErr)

		if apptelegram.IsPermanentSendError(err) {
			errorType = "permanent"
			if markErr := a.markNotificationJobFailed(ctx, job, safeErr, true); markErr != nil {
				if errors.Is(markErr, errJobOwnershipLost) {
					slog.Warn("ignored stale notification failure result", "job_id", job.ID, "attempts", job.Attempts)
				} else {
					slog.Error("failed to persist permanent notification failure", "chat_id", job.ChatID, "error", markErr)
				}
			}
		} else if job.Attempts >= workers.NotificationJobMaxAttempts {
			errorType = "exhausted"
			if markErr := a.markNotificationJobFailed(ctx, job, safeErr, false); markErr != nil {
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

	finalizationAttempts, err := defaultSentFinalizationPolicy.finalize(
		ctx,
		"notification",
		job.ID,
		sentMessage.MessageID,
		func(finalCtx context.Context) error {
			return a.markNotificationJobSentOnce(finalCtx, job)
		},
	)
	if err != nil {
		if errors.Is(err, errJobOwnershipLost) {
			slog.Warn("ignored stale notification success result", "job_id", job.ID, "attempts", job.Attempts)
			appmetrics.CronDeliveriesTotal.WithLabelValues("sent_stale_claim").Inc()
			return
		}

		slog.Error(
			"Telegram notification delivered but database finalization failed",
			"job_id",
			job.ID,
			"chat_id",
			job.ChatID,
			"telegram_message_id",
			sentMessage.MessageID,
			"finalization_attempts",
			finalizationAttempts,
			"error",
			err,
		)
		appmetrics.CronDeliveriesTotal.WithLabelValues("sent_persist_error").Inc()
		return
	}

	appmetrics.CronDeliveriesTotal.WithLabelValues("sent").Inc()
}

func notificationMessageForDelivery(job NotificationJob, deliveredAt time.Time) string {
	if job.ScheduledAt.IsZero() || priceAge(deliveredAt, job.ScheduledAt) <= priceFreshnessLimit {
		return job.Text
	}

	return fmt.Sprintf("%s\n\n%s", job.Text, apptelegram.Text(job.Lang, "delay_warning"))
}

func (a *App) ensureNotificationJobClaimCurrent(ctx context.Context, job NotificationJob) error {
	var current bool
	err := a.db.QueryRowContext(
		ctx,
		`SELECT EXISTS (
			SELECT 1
			FROM notification_jobs
			WHERE id = $1
			AND status = 'sending'
			AND claim_token = $2::uuid
			AND attempts = $3
			AND claimed_until > NOW() + $4::interval
		)`,
		job.ID,
		job.ClaimToken,
		job.Attempts,
		workers.PostgresInterval(workers.TelegramSendLeaseSafetyWindow),
	).Scan(&current)
	if err != nil {
		appmetrics.DBOperationsTotal.WithLabelValues("validate_notification_claim", "error").Inc()
		return err
	}
	if !current {
		appmetrics.DBOperationsTotal.WithLabelValues("validate_notification_claim", "stale_claim").Inc()
		return errJobOwnershipLost
	}

	appmetrics.DBOperationsTotal.WithLabelValues("validate_notification_claim", "success").Inc()
	return nil
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

func (a *App) markNotificationJobSentOnce(ctx context.Context, job NotificationJob) error {
	tx, err := a.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		appmetrics.DBOperationsTotal.WithLabelValues("mark_notification_sent", "error").Inc()
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()

	result, err := tx.ExecContext(
		ctx,
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
		ctx,
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
	safeErr := a.safeTelegramError(sendErr)

	tx, err := a.db.BeginTx(dbCtx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		appmetrics.DBOperationsTotal.WithLabelValues("mark_notification_retry", "error").Inc()
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()

	delay := telegramRetryDelay(job.Attempts, sendErr)
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
		workers.TruncateError(safeErr.Error(), 500),
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
	safeErr := a.safeTelegramError(sendErr)

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
		workers.TruncateError(safeErr.Error(), 500),
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
