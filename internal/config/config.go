// Package config завантажує та перевіряє конфігурацію процесу.
package config

import (
	"fmt"
	"os"

	"github.com/joho/godotenv"
)

const defaultPort = "8080"

// Config містить обов'язкові runtime-параметри застосунку.
type Config struct {
	DatabaseURL   string
	TelegramToken string
	WebhookSecret string
	CronSecret    string
	MetricsSecret string
	Port          string
}

// Load читає локальний .env, якщо він існує, та перевіряє обов'язкові змінні.
func Load() (Config, error) {
	_ = godotenv.Load()

	cfg := Config{
		DatabaseURL:   os.Getenv("DATABASE_URL"),
		TelegramToken: os.Getenv("TELEGRAM_APITOKEN"),
		WebhookSecret: os.Getenv("WEBHOOK_SECRET_TOKEN"),
		CronSecret:    os.Getenv("CRON_SECRET"),
		MetricsSecret: os.Getenv("METRICS_SECRET"),
		Port:          Port(),
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
