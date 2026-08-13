// Package workers визначає спільну retry та lease policy фонових обробників.
package workers

import (
	"fmt"
	"time"
)

const (
	DatabaseQueryPoolMaxOpenConnections = 16
	DatabaseQueryPoolMaxIdleConnections = 8
	DatabaseLockPoolMaxOpenConnections  = 12
	DatabaseLockPoolMaxIdleConnections  = 2
	NotificationWorkerCount             = 3
	TelegramReplyWorkerCount            = 3
	CronLockConnectionReserve           = 1
	LockPoolOperationalReserve          = 1
	MaxTelegramUpdateWorkerCount        = DatabaseLockPoolMaxOpenConnections - NotificationWorkerCount - TelegramReplyWorkerCount - CronLockConnectionReserve - LockPoolOperationalReserve
	CronBatchLimit                      = 100
	TelegramUpdateShardCount            = 64
	DefaultTelegramUpdateWorkerCount    = MaxTelegramUpdateWorkerCount
	TelegramUpdatePollInterval          = 2 * time.Second
	TelegramUpdateClaimWindow           = 45 * time.Second
	TelegramUpdateMaxAttempts           = 3
	TelegramUpdateProcessedRetention    = 7 * 24 * time.Hour
	TelegramUpdateFailedRetention       = 30 * 24 * time.Hour
	TelegramReplyPollInterval           = 2 * time.Second
	TelegramReplyClaimWindow            = 45 * time.Second
	TelegramReplyClaimValidationTimeout = 5 * time.Second
	TelegramReplyMaxAttempts            = 3
	TelegramReplySentRetention          = 7 * 24 * time.Hour
	TelegramReplyFailedRetention        = 30 * 24 * time.Hour

	// NotificationJobClaimWindow покриває advisory lock, перевірки БД, Telegram timeout і фіналізацію.
	NotificationJobClaimWindow                 = 45 * time.Second
	NotificationSubscriptionCheckTimeout       = 5 * time.Second
	NotificationClaimValidationTimeout         = 5 * time.Second
	TelegramSendLeaseSafetyWindow              = 15 * time.Second
	TelegramChatLockTimeout                    = 10 * time.Second
	NotificationJobPollInterval                = 2 * time.Second
	NotificationJobMaxAttempts                 = 3
	NotificationFailureCooldown                = 15 * time.Minute
	RetentionCleanupInterval                   = time.Hour
	RetentionCleanupRunTimeout                 = 30 * time.Second
	RetentionTableCleanupTimeout               = 10 * time.Second
	NotificationSentRetention                  = 30 * 24 * time.Hour
	NotificationFailedRetention                = 90 * 24 * time.Hour
	NotificationCanceledRetention              = 30 * 24 * time.Hour
	RetentionCleanupLimit                      = 1000
	CronAdvisoryLockKey                  int64 = 0x63726f6e6c6f636b
	TelegramChatLockPrefix                     = "cryptopulse:telegram-chat:"
	GracefulShutdownTimeout                    = 30 * time.Second
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
