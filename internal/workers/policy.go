// Package workers визначає спільну retry та lease policy фонових обробників.
package workers

import (
	"fmt"
	"time"
)

const (
	CronBatchLimit                   = 100
	TelegramUpdateShardCount         = 64
	DefaultTelegramUpdateWorkerCount = 10
	TelegramUpdatePollInterval       = 2 * time.Second
	TelegramUpdateClaimWindow        = 45 * time.Second
	TelegramUpdateMaxAttempts        = 3
	TelegramUpdateProcessedRetention = 7 * 24 * time.Hour
	TelegramUpdateFailedRetention    = 30 * 24 * time.Hour
	TelegramReplyWorkerCount         = 5
	TelegramReplyPollInterval        = 2 * time.Second
	TelegramReplyClaimWindow         = 45 * time.Second
	TelegramReplyMaxAttempts         = 3
	TelegramReplySentRetention       = 7 * 24 * time.Hour
	TelegramReplyFailedRetention     = 30 * 24 * time.Hour

	// NotificationJobClaimWindow покриває 10s Telegram timeout і збереження результату в БД.
	NotificationJobClaimWindow          = 45 * time.Second
	NotificationJobPollInterval         = 2 * time.Second
	NotificationJobMaxAttempts          = 3
	NotificationFailureCooldown         = 15 * time.Minute
	RetentionCleanupInterval            = time.Hour
	NotificationSentRetention           = 30 * 24 * time.Hour
	NotificationFailedRetention         = 90 * 24 * time.Hour
	NotificationCanceledRetention       = 30 * 24 * time.Hour
	RetentionCleanupLimit               = 1000
	CronAdvisoryLockKey           int64 = 0x63726f6e6c6f636b
	TelegramChatLockPrefix              = "cryptopulse:telegram-chat:"
)

// PostgresInterval перетворює duration на безпечний interval-параметр PostgreSQL.
func PostgresInterval(duration time.Duration) string {
	if duration <= 0 {
		return "0 seconds"
	}
	return fmt.Sprintf("%d seconds", int(duration.Seconds()))
}

// TruncateError обмежує текст помилки перед збереженням у durable queue.
func TruncateError(text string, limit int) string {
	if limit <= 0 || len(text) <= limit {
		return text
	}
	return text[:limit]
}

// RetryDelay повертає лінійний backoff із верхньою межею десять хвилин.
func RetryDelay(attempts int) time.Duration {
	if attempts < 1 {
		attempts = 1
	}

	delay := time.Duration(attempts) * time.Minute
	if delay > 10*time.Minute {
		return 10 * time.Minute
	}
	return delay
}
