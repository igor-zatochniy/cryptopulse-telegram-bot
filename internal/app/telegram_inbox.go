package app

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"log/slog"
	"strconv"
	"sync"
	"time"

	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"

	appmetrics "github.com/igor-zatochniy/cryptopulse-telegram-bot/internal/metrics"
	"github.com/igor-zatochniy/cryptopulse-telegram-bot/internal/workers"
)

func telegramUpdateChatID(update tgbotapi.Update) (int64, bool) {
	if update.Message != nil && update.Message.Chat != nil {
		return update.Message.Chat.ID, true
	}
	if update.CallbackQuery != nil && update.CallbackQuery.Message != nil && update.CallbackQuery.Message.Chat != nil {
		return update.CallbackQuery.Message.Chat.ID, true
	}
	if update.EditedMessage != nil && update.EditedMessage.Chat != nil {
		return update.EditedMessage.Chat.ID, true
	}
	if update.ChannelPost != nil && update.ChannelPost.Chat != nil {
		return update.ChannelPost.Chat.ID, true
	}
	if update.EditedChannelPost != nil && update.EditedChannelPost.Chat != nil {
		return update.EditedChannelPost.Chat.ID, true
	}
	return 0, false
}

func telegramShardIndex(chatID int64, shardCount int) int {
	if shardCount <= 1 {
		return 0
	}

	idx := chatID % int64(shardCount)
	if idx < 0 {
		idx += int64(shardCount)
	}
	return int(idx)
}

func telegramWorkerShardIDs(workerID int, workerCount int) []int32 {
	if workerCount < 1 || workerID < 0 || workerID >= workerCount {
		return nil
	}

	shardIDs := make([]int32, 0, workers.TelegramUpdateShardCount/workerCount+1)
	for shardID := workerID; shardID < workers.TelegramUpdateShardCount; shardID += workerCount {
		shardIDs = append(shardIDs, int32(shardID))
	}
	return shardIDs
}

func (a *App) saveTelegramUpdate(ctx context.Context, update tgbotapi.Update, payload []byte) (bool, error) {
	chatID, ok := telegramUpdateChatID(update)
	if !ok {
		chatID = 0
	}
	shardID := telegramShardIndex(chatID, workers.TelegramUpdateShardCount)

	dbCtx, dbCancel := context.WithTimeout(ctx, 5*time.Second)
	defer dbCancel()

	result, err := a.db.ExecContext(
		dbCtx,
		`INSERT INTO telegram_updates (
			update_id,
			chat_id,
			shard_id,
			payload,
			status,
			next_attempt_at
		) VALUES ($1, $2, $3, $4::jsonb, 'pending', NOW())
		ON CONFLICT (update_id) DO NOTHING`,
		int64(update.UpdateID),
		chatID,
		shardID,
		string(payload),
	)
	if err != nil {
		appmetrics.DBOperationsTotal.WithLabelValues("save_telegram_update", "error").Inc()
		return false, err
	}

	inserted, err := result.RowsAffected()
	if err != nil {
		appmetrics.DBOperationsTotal.WithLabelValues("save_telegram_update", "error").Inc()
		return false, err
	}

	appmetrics.DBOperationsTotal.WithLabelValues("save_telegram_update", "success").Inc()
	return inserted > 0, nil
}

func (a *App) claimPendingTelegramUpdateForWorker(
	ctx context.Context,
	workerID int,
	workerCount int,
) (*TelegramUpdateJob, error) {
	if workerCount < 1 || workerID < 0 || workerID >= workerCount {
		return nil, fmt.Errorf("invalid Telegram worker partition %d/%d", workerID, workerCount)
	}
	shardIDs := telegramWorkerShardIDs(workerID, workerCount)
	if len(shardIDs) == 0 {
		return nil, nil
	}

	dbCtx, dbCancel := context.WithTimeout(ctx, 5*time.Second)
	defer dbCancel()

	tx, err := a.db.BeginTx(dbCtx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		appmetrics.DBOperationsTotal.WithLabelValues("claim_telegram_update", "error").Inc()
		return nil, err
	}
	defer func() {
		_ = tx.Rollback()
	}()

	var job TelegramUpdateJob
	err = tx.QueryRowContext(dbCtx, `WITH next_update AS (
		SELECT tu.update_id
		FROM telegram_updates AS tu
		WHERE tu.shard_id = ANY($1::integer[])
		AND tu.status IN ('pending', 'processing')
		AND tu.next_attempt_at <= NOW()
		AND (
			tu.status = 'pending'
			OR tu.claimed_until IS NULL
			OR tu.claimed_until < NOW()
		)
		AND NOT EXISTS (
			SELECT 1
			FROM telegram_updates AS earlier
			WHERE earlier.chat_id = tu.chat_id
			AND earlier.status IN ('pending', 'processing')
			AND earlier.update_id < tu.update_id
		)
		ORDER BY tu.update_id ASC
		LIMIT 1
		FOR UPDATE OF tu SKIP LOCKED
	), claimed AS (
		UPDATE telegram_updates AS tu
		SET status = 'processing',
		    attempts = tu.attempts + 1,
		    claimed_until = NOW() + $2::interval,
		    updated_at = NOW()
		FROM next_update
		WHERE tu.update_id = next_update.update_id
		RETURNING tu.update_id, tu.chat_id, tu.payload::text, tu.attempts
	)
	SELECT update_id, chat_id, payload, attempts FROM claimed`,
		shardIDs,
		workers.PostgresInterval(workers.TelegramUpdateClaimWindow),
	).Scan(
		&job.UpdateID,
		&job.ChatID,
		&job.Payload,
		&job.Attempts,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		appmetrics.DBOperationsTotal.WithLabelValues("claim_telegram_update", "error").Inc()
		return nil, err
	}

	if err := tx.Commit(); err != nil {
		appmetrics.DBOperationsTotal.WithLabelValues("claim_telegram_update", "error").Inc()
		return nil, err
	}

	appmetrics.DBOperationsTotal.WithLabelValues("claim_telegram_update", "success").Inc()
	return &job, nil
}

func (a *App) updateWorkerPartition(
	ctx context.Context,
	wg *sync.WaitGroup,
	workerID int,
	workerCount int,
) {
	defer wg.Done()

	pollTicker := time.NewTicker(workers.TelegramUpdatePollInterval)
	defer pollTicker.Stop()

	for {
		job, err := a.claimPendingTelegramUpdateForWorker(ctx, workerID, workerCount)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			slog.Error(
				"failed to claim Telegram update",
				"worker_id",
				workerID,
				"worker_count",
				workerCount,
				"error",
				err,
			)
			select {
			case <-pollTicker.C:
			case <-ctx.Done():
				return
			}
			continue
		}

		if job != nil {
			a.processTelegramUpdateJob(ctx, *job)
			continue
		}

		select {
		case <-pollTicker.C:
		case <-ctx.Done():
			return
		}
	}
}

func telegramChatAdvisoryLockKey(chatID int64) int64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(workers.TelegramChatLockPrefix))
	_, _ = h.Write([]byte(strconv.FormatInt(chatID, 10)))
	return int64(h.Sum64())
}

func (a *App) acquireTelegramChatAdvisoryLock(ctx context.Context, chatID int64) (*sql.Conn, int64, error) {
	conn, err := a.lockDatabase().Conn(ctx)
	if err != nil {
		appmetrics.DBOperationsTotal.WithLabelValues("acquire_telegram_chat_lock", "error").Inc()
		return nil, 0, err
	}

	lockKey := telegramChatAdvisoryLockKey(chatID)
	if _, err := conn.ExecContext(ctx, `SELECT pg_advisory_lock($1)`, lockKey); err != nil {
		_ = conn.Close()
		appmetrics.DBOperationsTotal.WithLabelValues("acquire_telegram_chat_lock", "error").Inc()
		return nil, lockKey, err
	}

	appmetrics.DBOperationsTotal.WithLabelValues("acquire_telegram_chat_lock", "success").Inc()
	return conn, lockKey, nil
}

func releaseTelegramChatAdvisoryLock(ctx context.Context, conn *sql.Conn, lockKey int64) {
	if conn == nil {
		return
	}

	releaseCtx, cancel := finalizationContext(ctx, 2*time.Second)
	defer cancel()

	if _, err := conn.ExecContext(releaseCtx, `SELECT pg_advisory_unlock($1)`, lockKey); err != nil {
		slog.Error("failed to release telegram chat advisory lock", "error", err)
	}
	if err := conn.Close(); err != nil {
		slog.Error("failed to close telegram chat advisory lock connection", "error", err)
	}
}

func (a *App) processTelegramUpdateJob(ctx context.Context, job TelegramUpdateJob) {
	var update tgbotapi.Update
	if err := json.Unmarshal([]byte(job.Payload), &update); err != nil {
		if markErr := a.markTelegramUpdateFailed(ctx, job, err); markErr != nil {
			slog.Error("failed to persist invalid telegram update", "update_id", job.UpdateID, "error", markErr)
		}
		appmetrics.WebhookUpdatesTotal.WithLabelValues("failed_invalid_payload").Inc()
		return
	}

	lockCtx, lockCancel := context.WithTimeout(ctx, 10*time.Second)
	lockConn, lockKey, err := a.acquireTelegramChatAdvisoryLock(lockCtx, job.ChatID)
	lockCancel()
	if err != nil {
		a.markTelegramUpdateProcessingError(ctx, job, fmt.Errorf("acquire telegram chat lock: %w", err))
		return
	}
	defer releaseTelegramChatAdvisoryLock(ctx, lockConn, lockKey)

	processCtx, processCancel := context.WithTimeout(ctx, 10*time.Second)
	defer processCancel()

	tx, err := a.db.BeginTx(processCtx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		a.markTelegramUpdateProcessingError(ctx, job, fmt.Errorf("begin telegram update transaction: %w", err))
		return
	}
	defer func() {
		_ = tx.Rollback()
	}()

	var processingErr error
	replyCollector := &telegramReplyCollector{}
	func() {
		defer func() {
			if recovered := recover(); recovered != nil {
				processingErr = fmt.Errorf("telegram update processing panic: %v", recovered)
			}
		}()

		processCtx = withTelegramReplyCollector(processCtx, replyCollector)
		if err := a.processTelegramUpdateWithDB(processCtx, tx, update); err != nil {
			processingErr = err
			return
		}
		if replyCollector.err != nil {
			processingErr = fmt.Errorf("prepare Telegram reply: %w", replyCollector.err)
		}
	}()

	if processingErr != nil {
		_ = tx.Rollback()
		a.markTelegramUpdateProcessingError(ctx, job, processingErr)
		return
	}

	if err := a.completeTelegramUpdateTx(processCtx, tx, job, replyCollector.replies); err != nil {
		_ = tx.Rollback()
		if errors.Is(err, errJobOwnershipLost) {
			slog.Warn("ignored stale telegram update success result", "update_id", job.UpdateID, "attempts", job.Attempts)
			appmetrics.WebhookUpdatesTotal.WithLabelValues("processed_stale_claim").Inc()
			return
		}

		slog.Error("failed to persist processed telegram update", "update_id", job.UpdateID, "error", err)
		appmetrics.WebhookUpdatesTotal.WithLabelValues("processed_persist_error").Inc()
		a.markTelegramUpdateProcessingError(ctx, job, err)
		return
	}
	if err := tx.Commit(); err != nil {
		appmetrics.DBOperationsTotal.WithLabelValues("complete_telegram_update", "error").Inc()
		slog.Error("failed to commit processed telegram update", "update_id", job.UpdateID, "error", err)
		a.markTelegramUpdateProcessingError(ctx, job, err)
		return
	}

	observeCompletedTelegramUpdate(len(replyCollector.replies))
	a.flushCallbackAnswers(replyCollector.callbacks)
	appmetrics.WebhookUpdatesTotal.WithLabelValues("processed").Inc()
}

func (a *App) markTelegramUpdateProcessingError(
	ctx context.Context,
	job TelegramUpdateJob,
	processingErr error,
) {
	if job.Attempts >= workers.TelegramUpdateMaxAttempts {
		if markErr := a.markTelegramUpdateFailed(ctx, job, processingErr); markErr != nil {
			if errors.Is(markErr, errJobOwnershipLost) {
				slog.Warn("ignored stale telegram update failure result", "update_id", job.UpdateID, "attempts", job.Attempts)
			} else {
				slog.Error("failed to persist telegram update failure", "update_id", job.UpdateID, "error", markErr)
			}
		}
		appmetrics.WebhookUpdatesTotal.WithLabelValues("failed_exhausted").Inc()
		return
	}

	if markErr := a.markTelegramUpdateRetry(ctx, job, processingErr); markErr != nil {
		if errors.Is(markErr, errJobOwnershipLost) {
			slog.Warn("ignored stale telegram update retry result", "update_id", job.UpdateID, "attempts", job.Attempts)
		} else {
			slog.Error("failed to persist telegram update retry", "update_id", job.UpdateID, "error", markErr)
		}
	}
	appmetrics.WebhookUpdatesTotal.WithLabelValues("retry").Inc()
}

func (a *App) completeTelegramUpdateTx(
	ctx context.Context,
	tx *sql.Tx,
	job TelegramUpdateJob,
	replies []TelegramReply,
) error {

	for sequenceNo, reply := range replies {
		if _, err := tx.ExecContext(
			ctx,
			`INSERT INTO telegram_replies (
				source_update_id,
				sequence_no,
				chat_id,
				operation,
				message_id,
				message_text,
				reply_markup,
				status,
				next_attempt_at
			) VALUES ($1, $2, $3, $4, $5, $6, NULLIF($7, '')::jsonb, 'pending', NOW())
			ON CONFLICT (source_update_id, sequence_no) DO NOTHING`,
			job.UpdateID,
			sequenceNo,
			reply.ChatID,
			reply.Operation,
			reply.MessageID,
			reply.Text,
			reply.ReplyMarkup,
		); err != nil {
			appmetrics.DBOperationsTotal.WithLabelValues("complete_telegram_update", "error").Inc()
			return err
		}
	}

	result, err := tx.ExecContext(
		ctx,
		`UPDATE telegram_updates
		 SET status = 'processed',
		     processed_at = NOW(),
		     claimed_until = NULL,
		     last_error = NULL,
		     updated_at = NOW()
		 WHERE update_id = $1
		 AND status = 'processing'
		 AND attempts = $2`,
		job.UpdateID,
		job.Attempts,
	)
	if err != nil {
		appmetrics.DBOperationsTotal.WithLabelValues("complete_telegram_update", "error").Inc()
		return err
	}
	if err := ensureCurrentJobClaimUpdated(result, "complete_telegram_update"); err != nil {
		return err
	}

	return nil
}

func observeCompletedTelegramUpdate(replyCount int) {
	appmetrics.DBOperationsTotal.WithLabelValues("complete_telegram_update", "success").Inc()
	if replyCount > 0 {
		appmetrics.TelegramRepliesTotal.WithLabelValues("queued").Add(float64(replyCount))
	}
}

func (a *App) markTelegramUpdateRetry(
	ctx context.Context,
	job TelegramUpdateJob,
	processingErr error,
) error {
	dbCtx, dbCancel := finalizationContext(ctx, 5*time.Second)
	defer dbCancel()

	delay := workers.RetryDelay(job.Attempts)
	result, err := a.db.ExecContext(
		dbCtx,
		`UPDATE telegram_updates
		 SET status = 'pending',
		     claimed_until = NULL,
		     next_attempt_at = NOW() + $2::interval,
		     last_error = $3,
		     updated_at = NOW()
		 WHERE update_id = $1
		 AND status = 'processing'
		 AND attempts = $4`,
		job.UpdateID,
		workers.PostgresInterval(delay),
		workers.TruncateError(processingErr.Error(), 500),
		job.Attempts,
	)
	if err != nil {
		appmetrics.DBOperationsTotal.WithLabelValues("mark_telegram_update_retry", "error").Inc()
		return err
	}
	if err := ensureCurrentJobClaimUpdated(result, "mark_telegram_update_retry"); err != nil {
		return err
	}

	appmetrics.DBOperationsTotal.WithLabelValues("mark_telegram_update_retry", "success").Inc()
	return nil
}

func (a *App) markTelegramUpdateFailed(
	ctx context.Context,
	job TelegramUpdateJob,
	processingErr error,
) error {
	dbCtx, dbCancel := finalizationContext(ctx, 5*time.Second)
	defer dbCancel()

	result, err := a.db.ExecContext(
		dbCtx,
		`UPDATE telegram_updates
		 SET status = 'failed',
		     failed_at = NOW(),
		     claimed_until = NULL,
		     last_error = $2,
		     updated_at = NOW()
		 WHERE update_id = $1
		 AND status = 'processing'
		 AND attempts = $3`,
		job.UpdateID,
		workers.TruncateError(processingErr.Error(), 500),
		job.Attempts,
	)
	if err != nil {
		appmetrics.DBOperationsTotal.WithLabelValues("mark_telegram_update_failed", "error").Inc()
		return err
	}
	if err := ensureCurrentJobClaimUpdated(result, "mark_telegram_update_failed"); err != nil {
		return err
	}

	appmetrics.DBOperationsTotal.WithLabelValues("mark_telegram_update_failed", "success").Inc()
	return nil
}
