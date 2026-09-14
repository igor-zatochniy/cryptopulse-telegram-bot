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

func TestLoadRejectsTelegramUpdateWorkerCountAboveConnectionBudget(t *testing.T) {
	setRequiredEnvironment(t)
	t.Setenv("TELEGRAM_UPDATE_WORKERS", "5")

	if _, err := Load(); err == nil {
		t.Fatal("expected invalid worker count error")
	}
}

func TestLoadRejectsMissingMetricsSecret(t *testing.T) {
	setRequiredEnvironment(t)
	t.Setenv("METRICS_SECRET", "")

	if _, err := Load(); err == nil {
		t.Fatal("expected missing METRICS_SECRET error")
	}
}

func TestLoadRejectsMetricsSecretMatchingCronSecret(t *testing.T) {
	setRequiredEnvironment(t)
	t.Setenv("METRICS_SECRET", "cron-secret")

	if _, err := Load(); err == nil {
		t.Fatal("expected shared metrics and cron secret error")
	}
}

func setRequiredEnvironment(t *testing.T) {
	t.Helper()

	t.Setenv("DATABASE_URL", "postgres://example")
	t.Setenv("TELEGRAM_APITOKEN", "test-token")
	t.Setenv("WEBHOOK_SECRET_TOKEN", "webhook-secret")
	t.Setenv("CRON_SECRET", "cron-secret")
	t.Setenv("METRICS_SECRET", "metrics-secret")
	t.Setenv("PRICE_ALERTS_ENABLED", "")
}

func TestPriceAlertsRequireExplicitEnablement(t *testing.T) {
	setRequiredEnvironment(t)
	for _, value := range []string{"", "false", "true", "invalid"} {
		t.Setenv("PRICE_ALERTS_ENABLED", value)
		cfg, err := Load()
		if value == "invalid" {
			if err == nil {
				t.Fatal("accepted invalid PRICE_ALERTS_ENABLED")
			}
			continue
		}
		if err != nil || cfg.PriceAlertsEnabled != (value == "true") {
			t.Fatalf("flag %q: enabled=%v, err=%v", value, cfg.PriceAlertsEnabled, err)
		}
	}
}
