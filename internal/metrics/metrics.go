// Package metrics містить Prometheus-метрики застосунку.
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	CronRunsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "cryptopulse_cron_runs_total",
			Help: "Total number of cron endpoint executions by result status.",
		},
		[]string{"status"},
	)
	CronClaimedSubscribersTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "cryptopulse_cron_claimed_subscribers_total",
			Help: "Total number of subscribers claimed by cron batches.",
		},
	)
	CronDeliveriesTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "cryptopulse_cron_deliveries_total",
			Help: "Total number of scheduled Telegram delivery attempts by result status.",
		},
		[]string{"status"},
	)
	WebhookUpdatesTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "cryptopulse_webhook_updates_total",
			Help: "Total number of Telegram webhook updates by result status.",
		},
		[]string{"status"},
	)
	TelegramSendErrorsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "cryptopulse_telegram_send_errors_total",
			Help: "Total number of Telegram send/edit errors by type.",
		},
		[]string{"type"},
	)
	TelegramRepliesTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "cryptopulse_telegram_replies_total",
			Help: "Total number of durable interactive Telegram replies by result status.",
		},
		[]string{"status"},
	)
	BinanceRequestsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "cryptopulse_binance_requests_total",
			Help: "Total number of Binance ticker requests by symbol and result status.",
		},
		[]string{"symbol", "status"},
	)
	PriceAgeSeconds = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "cryptopulse_price_age_seconds",
			Help: "Age in seconds of the latest successfully fetched price by symbol.",
		},
		[]string{"symbol"},
	)
	DBOperationsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "cryptopulse_db_operations_total",
			Help: "Total number of database operations by operation name and result status.",
		},
		[]string{"operation", "status"},
	)
)
