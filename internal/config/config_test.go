package config

import (
	"testing"

	"github.com/igor-zatochniy/cryptopulse-telegram-bot/internal/workers"
)

func TestLoadUsesDefaultTelegramUpdateWorkerCount(t *testing.T) {
	setRequiredEnvironment(t)
	t.Setenv("TELEGRAM_UPDATE_WORKERS", "")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if cfg.TelegramUpdateWorkers != workers.DefaultTelegramUpdateWorkerCount {
		t.Fatalf(
			"update workers = %d, want %d",
			cfg.TelegramUpdateWorkers,
			workers.DefaultTelegramUpdateWorkerCount,
		)
	}
}

func TestLoadAcceptsConfiguredTelegramUpdateWorkerCount(t *testing.T) {
	setRequiredEnvironment(t)
	t.Setenv("TELEGRAM_UPDATE_WORKERS", "4")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if cfg.TelegramUpdateWorkers != 4 {
		t.Fatalf("update workers = %d, want 4", cfg.TelegramUpdateWorkers)
	}
}

func TestLoadRejectsTelegramUpdateWorkerCountAboveShardCount(t *testing.T) {
	setRequiredEnvironment(t)
	t.Setenv("TELEGRAM_UPDATE_WORKERS", "65")

	if _, err := Load(); err == nil {
		t.Fatal("expected invalid worker count error")
	}
}

func setRequiredEnvironment(t *testing.T) {
	t.Helper()

	t.Setenv("DATABASE_URL", "postgres://example")
	t.Setenv("TELEGRAM_APITOKEN", "test-token")
	t.Setenv("WEBHOOK_SECRET_TOKEN", "webhook-secret")
	t.Setenv("CRON_SECRET", "cron-secret")
	t.Setenv("METRICS_SECRET", "metrics-secret")
}
