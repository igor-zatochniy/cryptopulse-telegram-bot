package app

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"

	appmetrics "github.com/igor-zatochniy/cryptopulse-telegram-bot/internal/metrics"
	apptelegram "github.com/igor-zatochniy/cryptopulse-telegram-bot/internal/telegram"
	"github.com/igor-zatochniy/cryptopulse-telegram-bot/internal/workers"
)

const (
	telegramReplySendMessage = "send_message"
	telegramReplyEditMessage = "edit_message"
)

type TelegramReply struct {
	Operation   string
	ChatID      int64
	MessageID   int
	Text        string
	ReplyMarkup string
}

type TelegramReplyJob struct {
	ID          int64
	Operation   string
	ChatID      int64
	MessageID   int
	Text        string
	ReplyMarkup sql.NullString
	ClaimToken  string
	Attempts    int
}

type telegramCallbackAnswer struct {
	CallbackID string
	Text       string
}

type telegramReplyCollector struct {
	replies   []TelegramReply
	callbacks []telegramCallbackAnswer
	err       error
}

type telegramReplyCollectorContextKey struct{}

func withTelegramReplyCollector(ctx context.Context, collector *telegramReplyCollector) context.Context {
	return context.WithValue(ctx, telegramReplyCollectorContextKey{}, collector)
}

func telegramReplyCollectorFromContext(ctx context.Context) *telegramReplyCollector {
	collector, _ := ctx.Value(telegramReplyCollectorContextKey{}).(*telegramReplyCollector)
	return collector
}

func (c *telegramReplyCollector) add(reply TelegramReply) {
	if c.err != nil {
		return
	}
	c.replies = append(c.replies, reply)
}

func (c *telegramReplyCollector) addCallback(callbackID, text string) {
	if c.err != nil {
		return
	}
	c.callbacks = append(c.callbacks, telegramCallbackAnswer{
		CallbackID: callbackID,
		Text:       text,
	})
}

func (c *telegramReplyCollector) setError(err error) {
	if c.err == nil {
		c.err = err
	}
}

func (a *App) flushCallbackAnswers(answers []telegramCallbackAnswer) {
	for _, answer := range answers {
		if _, err := a.bot.Request(tgbotapi.NewCallback(answer.CallbackID, answer.Text)); err != nil {
			appmetrics.TelegramSendErrorsTotal.WithLabelValues("callback_answer").Inc()
			slog.Error(
				"failed to answer callback query",
				"callback_id",
				answer.CallbackID,
				"error",
				a.safeTelegramError(err),
			)
		}
	}
}

func encodeTelegramReplyMarkup(markup interface{}) (string, error) {
	if markup == nil {
		return "", nil
	}

	switch value := markup.(type) {
	case tgbotapi.InlineKeyboardMarkup:
		encoded, err := json.Marshal(value)
		return string(encoded), err
	case *tgbotapi.InlineKeyboardMarkup:
		if value == nil {
			return "", nil
		}
		encoded, err := json.Marshal(value)
		return string(encoded), err
	default:
		return "", fmt.Errorf("unsupported Telegram reply markup type %T", markup)
	}
}

func (a *App) replyWorker(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()

	pollTicker := time.NewTicker(workers.TelegramReplyPollInterval)
	defer pollTicker.Stop()

	for {
		job, err := a.claimPendingTelegramReply(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}

			slog.Error("failed to claim Telegram reply", "error", err)
			select {
			case <-pollTicker.C:
			case <-ctx.Done():
				return
			}
			continue
		}

		if job != nil {
			a.processTelegramReply(ctx, *job)
			continue
		}

		select {
		case <-pollTicker.C:
		case <-ctx.Done():
			return
		}
	}
}

func (a *App) claimPendingTelegramReply(ctx context.Context) (*TelegramReplyJob, error) {
	dbCtx, dbCancel := context.WithTimeout(ctx, 5*time.Second)
	defer dbCancel()

	tx, err := a.db.BeginTx(dbCtx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		appmetrics.DBOperationsTotal.WithLabelValues("claim_telegram_reply", "error").Inc()
		return nil, err
	}
	defer func() {
		_ = tx.Rollback()
	}()

	rows, err := tx.QueryContext(dbCtx, `SELECT tr.id, tr.chat_id
		FROM telegram_replies AS tr
		WHERE tr.status IN ('pending', 'sending')
		AND tr.next_attempt_at <= NOW()
		AND (
			tr.status = 'pending'
			OR tr.claimed_until IS NULL
			OR tr.claimed_until < NOW()
		)
		AND NOT EXISTS (
			SELECT 1
			FROM telegram_replies AS earlier
			WHERE earlier.chat_id = tr.chat_id
			AND earlier.status IN ('pending', 'sending')
			AND earlier.id < tr.id
		)
		ORDER BY tr.id ASC
		LIMIT $1`, workers.OutboxClaimCandidateLimit)
	if err != nil {
		appmetrics.DBOperationsTotal.WithLabelValues("claim_telegram_reply", "error").Inc()
		return nil, err
	}
	type replyCandidate struct {
		id     int64
		chatID int64
	}
	candidates := make([]replyCandidate, 0, workers.OutboxClaimCandidateLimit)
	for rows.Next() {
		var candidate replyCandidate
		if err := rows.Scan(&candidate.id, &candidate.chatID); err != nil {
			_ = rows.Close()
			appmetrics.DBOperationsTotal.WithLabelValues("claim_telegram_reply", "error").Inc()
			return nil, err
		}
		candidates = append(candidates, candidate)
	}
	if err := rows.Close(); err != nil {
		appmetrics.DBOperationsTotal.WithLabelValues("claim_telegram_reply", "error").Inc()
		return nil, err
	}
	if err := rows.Err(); err != nil {
		appmetrics.DBOperationsTotal.WithLabelValues("claim_telegram_reply", "error").Inc()
		return nil, err
	}

	for _, candidate := range candidates {
		locked, err := tryTelegramChatTransactionLock(dbCtx, tx, candidate.chatID)
		if err != nil {
			appmetrics.DBOperationsTotal.WithLabelValues("claim_telegram_reply", "error").Inc()
			return nil, err
		}
		if !locked {
			continue
		}

		var job TelegramReplyJob
		err = tx.QueryRowContext(dbCtx, `UPDATE telegram_replies AS tr
		SET status = 'sending',
		    attempts = tr.attempts + 1,
		    claim_token = gen_random_uuid(),
		    claimed_until = NOW() + $1::interval,
		    updated_at = NOW()
		WHERE tr.id = $2
		AND tr.status IN ('pending', 'sending')
		AND tr.next_attempt_at <= NOW()
		AND (
			tr.status = 'pending'
			OR tr.claimed_until IS NULL
			OR tr.claimed_until < NOW()
		)
		AND NOT EXISTS (
			SELECT 1
			FROM telegram_replies AS earlier
			WHERE earlier.chat_id = tr.chat_id
			AND earlier.status IN ('pending', 'sending')
			AND earlier.id < tr.id
		)
		RETURNING tr.id,
		          tr.operation,
		          tr.chat_id,
		          tr.message_id,
		          tr.message_text,
		          tr.reply_markup::text,
		          tr.claim_token::text,
		          tr.attempts
	`, workers.PostgresInterval(workers.TelegramReplyClaimWindow), candidate.id).Scan(
			&job.ID,
			&job.Operation,
			&job.ChatID,
			&job.MessageID,
			&job.Text,
			&job.ReplyMarkup,
			&job.ClaimToken,
			&job.Attempts,
		)
		if errors.Is(err, sql.ErrNoRows) {
			continue
		}
		if err != nil {
			appmetrics.DBOperationsTotal.WithLabelValues("claim_telegram_reply", "error").Inc()
			return nil, err
		}

		if err := tx.Commit(); err != nil {
			appmetrics.DBOperationsTotal.WithLabelValues("claim_telegram_reply", "error").Inc()
			return nil, err
		}

		appmetrics.DBOperationsTotal.WithLabelValues("claim_telegram_reply", "success").Inc()
		return &job, nil
	}

	appmetrics.DBOperationsTotal.WithLabelValues("claim_telegram_reply", "no_claimable_candidate").Inc()
	return nil, nil
}

func (a *App) processTelegramReply(ctx context.Context, job TelegramReplyJob) {
	lockCtx, lockCancel := context.WithTimeout(ctx, workers.TelegramChatLockTimeout)
	lockConn, lockKey, err := a.acquireTelegramChatAdvisoryLock(lockCtx, job.ChatID)
	lockCancel()
	if err != nil {
		a.deferTelegramReply(ctx, job, fmt.Errorf("acquire telegram chat lock: %w", err))
		return
	}
	defer releaseTelegramChatAdvisoryLock(ctx, lockConn, lockKey)

	claimCtx, claimCancel := context.WithTimeout(ctx, workers.TelegramReplyClaimValidationTimeout)
	err = a.ensureTelegramReplyClaimCurrent(claimCtx, job)
	claimCancel()
	if err != nil {
		if errors.Is(err, errJobOwnershipLost) {
			slog.Warn("skipped Telegram reply for stale claim", "reply_id", job.ID, "attempts", job.Attempts)
			appmetrics.TelegramRepliesTotal.WithLabelValues("skipped_stale_claim").Inc()
			return
		}
		a.deferTelegramReply(ctx, job, fmt.Errorf("validate Telegram reply claim before delivery: %w", err))
		return
	}

	telegramMessageID, sendErr := a.sendTelegramReply(job)
	if sendErr == nil || (job.Operation == telegramReplyEditMessage && isTelegramMessageNotModified(sendErr)) {
		if telegramMessageID == 0 && job.Operation == telegramReplyEditMessage {
			telegramMessageID = job.MessageID
		}

		finalizationAttempts, err := defaultSentFinalizationPolicy.finalize(
			ctx,
			"telegram_reply",
			job.ID,
			telegramMessageID,
			func(finalCtx context.Context) error {
				return a.markTelegramReplySentOnce(finalCtx, job)
			},
		)
		if err != nil {
			if errors.Is(err, errJobOwnershipLost) {
				slog.Warn("ignored stale Telegram reply success", "reply_id", job.ID, "attempts", job.Attempts)
				appmetrics.TelegramRepliesTotal.WithLabelValues("sent_stale_claim").Inc()
				return
			}
			slog.Error(
				"Telegram reply delivered but database finalization failed",
				"reply_id",
				job.ID,
				"chat_id",
				job.ChatID,
				"telegram_message_id",
				telegramMessageID,
				"finalization_attempts",
				finalizationAttempts,
				"error",
				err,
			)
			appmetrics.TelegramRepliesTotal.WithLabelValues("sent_persist_error").Inc()
			return
		}

		appmetrics.TelegramRepliesTotal.WithLabelValues("sent").Inc()
		return
	}

	safeErr := a.safeTelegramError(sendErr)
	errorType := "transient"
	if apptelegram.IsPermanentSendError(sendErr) {
		errorType = "permanent"
		if err := a.markTelegramReplyFailed(ctx, job, safeErr); err != nil {
			logTelegramReplyFinalizationError("failure", job, err)
		}
	} else if job.Attempts >= workers.TelegramReplyMaxAttempts {
		errorType = "exhausted"
		if err := a.markTelegramReplyFailed(ctx, job, safeErr); err != nil {
			logTelegramReplyFinalizationError("failure", job, err)
		}
	} else if err := a.markTelegramReplyRetry(ctx, job, sendErr); err != nil {
		logTelegramReplyFinalizationError("retry", job, err)
	}

	appmetrics.TelegramSendErrorsTotal.WithLabelValues("interactive_" + errorType).Inc()
	appmetrics.TelegramRepliesTotal.WithLabelValues("failed_" + errorType).Inc()
	slog.Error(
		"failed to deliver Telegram reply",
		"reply_id",
		job.ID,
		"chat_id",
		job.ChatID,
		"type",
		errorType,
		"error",
		safeErr,
	)
}

func (a *App) ensureTelegramReplyClaimCurrent(ctx context.Context, job TelegramReplyJob) error {
	var current bool
	err := a.db.QueryRowContext(
		ctx,
		`SELECT EXISTS (
			SELECT 1
			FROM telegram_replies
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
		appmetrics.DBOperationsTotal.WithLabelValues("validate_telegram_reply_claim", "error").Inc()
		return err
	}
	if !current {
		appmetrics.DBOperationsTotal.WithLabelValues("validate_telegram_reply_claim", "stale_claim").Inc()
		return errJobOwnershipLost
	}

	appmetrics.DBOperationsTotal.WithLabelValues("validate_telegram_reply_claim", "success").Inc()
	return nil
}

func (a *App) deferTelegramReply(ctx context.Context, job TelegramReplyJob, processingErr error) {
	if job.Attempts >= workers.TelegramReplyMaxAttempts {
		if err := a.markTelegramReplyFailed(ctx, job, a.safeTelegramError(processingErr)); err != nil {
			logTelegramReplyFinalizationError("failure", job, err)
		}
		return
	}

	if err := a.markTelegramReplyRetry(ctx, job, processingErr); err != nil {
		logTelegramReplyFinalizationError("retry", job, err)
	}
}

func (a *App) sendTelegramReply(job TelegramReplyJob) (int, error) {
	var markup *tgbotapi.InlineKeyboardMarkup
	if job.ReplyMarkup.Valid && job.ReplyMarkup.String != "" {
		var decoded tgbotapi.InlineKeyboardMarkup
		if err := json.Unmarshal([]byte(job.ReplyMarkup.String), &decoded); err != nil {
			return 0, fmt.Errorf("decode Telegram reply markup: %w", err)
		}
		markup = &decoded
	}

	switch job.Operation {
	case telegramReplySendMessage:
		msg := tgbotapi.NewMessage(job.ChatID, job.Text)
		msg.ParseMode = "Markdown"
		if markup != nil {
			msg.ReplyMarkup = *markup
		}
		sentMessage, err := a.bot.Send(msg)
		return sentMessage.MessageID, err
	case telegramReplyEditMessage:
		edit := tgbotapi.NewEditMessageText(job.ChatID, job.MessageID, job.Text)
		edit.ParseMode = "Markdown"
		if markup != nil {
			edit.ReplyMarkup = markup
		}
		sentMessage, err := a.bot.Send(edit)
		return sentMessage.MessageID, err
	default:
		return 0, fmt.Errorf("unsupported Telegram reply operation %q", job.Operation)
	}
}

func isTelegramMessageNotModified(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(strings.ToLower(err.Error()), "message is not modified")
}

func logTelegramReplyFinalizationError(operation string, job TelegramReplyJob, err error) {
	if errors.Is(err, errJobOwnershipLost) {
		slog.Warn(
			"ignored stale Telegram reply "+operation,
			"reply_id",
			job.ID,
			"attempts",
			job.Attempts,
		)
		return
	}
	slog.Error(
		"failed to persist Telegram reply "+operation,
		"reply_id",
		job.ID,
		"error",
		err,
	)
}

func (a *App) markTelegramReplySentOnce(ctx context.Context, job TelegramReplyJob) error {
	result, err := a.db.ExecContext(
		ctx,
		`UPDATE telegram_replies
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
		appmetrics.DBOperationsTotal.WithLabelValues("mark_telegram_reply_sent", "error").Inc()
		return err
	}
	if err := ensureCurrentJobClaimUpdated(result, "mark_telegram_reply_sent"); err != nil {
		return err
	}

	appmetrics.DBOperationsTotal.WithLabelValues("mark_telegram_reply_sent", "success").Inc()
	return nil
}

func (a *App) markTelegramReplyRetry(ctx context.Context, job TelegramReplyJob, sendErr error) error {
	dbCtx, dbCancel := finalizationContext(ctx, 5*time.Second)
	defer dbCancel()
	safeErr := a.safeTelegramError(sendErr)

	result, err := a.db.ExecContext(
		dbCtx,
		`UPDATE telegram_replies
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
		workers.PostgresInterval(telegramRetryDelay(job.Attempts, sendErr)),
		workers.TruncateError(safeErr.Error(), 500),
		job.ClaimToken,
		job.Attempts,
	)
	if err != nil {
		appmetrics.DBOperationsTotal.WithLabelValues("mark_telegram_reply_retry", "error").Inc()
		return err
	}
	if err := ensureCurrentJobClaimUpdated(result, "mark_telegram_reply_retry"); err != nil {
		return err
	}

	appmetrics.DBOperationsTotal.WithLabelValues("mark_telegram_reply_retry", "success").Inc()
	return nil
}

func (a *App) markTelegramReplyFailed(ctx context.Context, job TelegramReplyJob, sendErr error) error {
	dbCtx, dbCancel := finalizationContext(ctx, 5*time.Second)
	defer dbCancel()
	safeErr := a.safeTelegramError(sendErr)

	result, err := a.db.ExecContext(
		dbCtx,
		`UPDATE telegram_replies
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
		appmetrics.DBOperationsTotal.WithLabelValues("mark_telegram_reply_failed", "error").Inc()
		return err
	}
	if err := ensureCurrentJobClaimUpdated(result, "mark_telegram_reply_failed"); err != nil {
		return err
	}

	appmetrics.DBOperationsTotal.WithLabelValues("mark_telegram_reply_failed", "success").Inc()
	return nil
}
