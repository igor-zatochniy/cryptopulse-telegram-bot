// Package config завантажує та перевіряє конфігурацію процесу.
package config

import (
	"fmt"
	"os"
	"strconv"

	"github.com/joho/godotenv"

	"github.com/igor-zatochniy/cryptopulse-telegram-bot/internal/workers"
)

const defaultPort = "8080"

// Config містить обов'язкові runtime-параметри застосунку.
type Config struct {
	DatabaseURL           string
	TelegramToken         string
	WebhookSecret         string
	CronSecret            string
	MetricsSecret         string
	Port                  string
	TelegramUpdateWorkers int
}

// Load читає локальний .env, якщо він існує, та перевіряє обов'язкові змінні.
func Load() (Config, error) {
	_ = godotenv.Load()

	updateWorkers, err := optionalPositiveInt(
		"TELEGRAM_UPDATE_WORKERS",
		workers.DefaultTelegramUpdateWorkerCount,
	)
	if err != nil {
		return Config{}, err
	}
	if updateWorkers > workers.MaxTelegramUpdateWorkerCount {
		return Config{}, fmt.Errorf(
			"TELEGRAM_UPDATE_WORKERS must not exceed %d",
			workers.MaxTelegramUpdateWorkerCount,
		)
	}

	cfg := Config{
		DatabaseURL:           os.Getenv("DATABASE_URL"),
		TelegramToken:         os.Getenv("TELEGRAM_APITOKEN"),
		WebhookSecret:         os.Getenv("WEBHOOK_SECRET_TOKEN"),
		CronSecret:            os.Getenv("CRON_SECRET"),
		MetricsSecret:         os.Getenv("METRICS_SECRET"),
		Port:                  Port(),
		TelegramUpdateWorkers: updateWorkers,
	}
	if cfg.MetricsSecret == "" {
		cfg.MetricsSecret = cfg.CronSecret
	}

	required := []struct {
		name  string
		value string
	}{
		{"DATABASE_URL", cfg.DatabaseURL},
		{"TELEGRAM_APITOKEN", cfg.TelegramToken},
		{"WEBHOOK_SECRET_TOKEN", cfg.WebhookSecret},
		{"CRON_SECRET", cfg.CronSecret},
	}
	for _, variable := range required {
		if variable.value == "" {
			return Config{}, fmt.Errorf("required environment variable %s is missing", variable.name)
		}
	}

	return cfg, nil
}

// Port повертає HTTP-порт із безпечним значенням за замовчуванням.
func Port() string {
	if port := os.Getenv("PORT"); port != "" {
		return port
	}
	return defaultPort
}

func optionalPositiveInt(name string, defaultValue int) (int, error) {
	raw := os.Getenv(name)
	if raw == "" {
		return defaultValue, nil
	}

	value, err := strconv.Atoi(raw)
	if err != nil || value < 1 {
		return 0, fmt.Errorf("%s must be a positive integer", name)
	}
	return value, nil
}
