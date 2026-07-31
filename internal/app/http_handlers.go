package app

import (
	"context"
	"crypto/subtle"
	"database/sql"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"golang.org/x/time/rate"

	"github.com/igor-zatochniy/cryptopulse-telegram-bot/internal/httpserver"
	appmetrics "github.com/igor-zatochniy/cryptopulse-telegram-bot/internal/metrics"
	"github.com/igor-zatochniy/cryptopulse-telegram-bot/internal/workers"
)

// --- HTTP-ОБРОБНИКИ ТА MIDDLEWARE ---

// Handler збирає HTTP routes та transport middleware застосунку.
func (a *App) Handler() http.Handler {
	cronLimiter := rate.NewLimiter(rate.Every(30*time.Second), 5)
	webhookLimiter := httpserver.NewClientRateLimiter(rate.Limit(50), 100, 10*time.Minute)

	mux := http.NewServeMux()
	mux.HandleFunc("/live", httpserver.Method(http.MethodGet, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("OK"))
	}))
	mux.HandleFunc("/ready", httpserver.Method(http.MethodGet, func(w http.ResponseWriter, r *http.Request) {
		dbCheckCtx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		if err := a.db.PingContext(dbCheckCtx); err != nil {
			appmetrics.DBOperationsTotal.WithLabelValues("readiness_ping", "error").Inc()
			slog.Error("readiness check failed", "error", err)
			http.Error(w, "Database Unreachable", http.StatusServiceUnavailable)
			return
		}
		appmetrics.DBOperationsTotal.WithLabelValues("readiness_ping", "success").Inc()
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("Ready"))
	}))
	mux.HandleFunc("/metrics", httpserver.Method(http.MethodGet, a.metricsAuthMiddleware(promhttp.Handler().ServeHTTP)))
	mux.HandleFunc(
		"/cron",
		httpserver.Method(
			http.MethodPost,
			a.cronAuthMiddleware(
				httpserver.GlobalRateLimit(
					cronLimiter,
					a.producerMiddleware(a.handleCron),
				),
			),
		),
	)
	mux.HandleFunc(
		"/webhook",
		httpserver.Method(
			http.MethodPost,
			a.webhookAuthMiddleware(
				httpserver.ClientRateLimit(
					webhookLimiter,
					a.producerMiddleware(a.handleWebhook),
				),
			),
		),
	)
	return mux
}

func (a *App) producerMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Рахуємо активні HTTP producer-и, щоб shutdown дочекався збереження accepted work у PostgreSQL.
		a.producerMu.Lock()
		if a.shuttingDown {
			a.producerMu.Unlock()
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte("503 Service Unavailable: Server shutting down"))
			return
		}
		a.producerWG.Add(1)
		a.producerMu.Unlock()

		defer a.producerWG.Done()
		next(w, r)
	}
}

func (a *App) stopAcceptingProducers() {
	a.producerMu.Lock()
	a.shuttingDown = true
	a.producerMu.Unlock()
}

func (a *App) waitForProducers(ctx context.Context) error {
	done := make(chan struct{})
	go func() {
		a.producerWG.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func safeSecretCompare(inputToken, expectedSecret string) bool {
	if expectedSecret == "" {
		return false
	}
	// Порівнюємо секрети у сталий час, щоб не відкривати timing side-channel.
	return subtle.ConstantTimeCompare([]byte(inputToken), []byte(expectedSecret)) == 1
}

func bearerToken(r *http.Request) string {
	const prefix = "Bearer "
	authHeader := r.Header.Get("Authorization")
	if !strings.HasPrefix(authHeader, prefix) {
		return ""
	}
	return strings.TrimPrefix(authHeader, prefix)
}

func (a *App) cronAuthMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !safeSecretCompare(bearerToken(r), a.cronSecret) {
			appmetrics.CronRunsTotal.WithLabelValues("unauthorized").Inc()
			slog.Warn("unauthorized cron request", "remote_ip", r.RemoteAddr)
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		next(w, r)
	}
}

func (a *App) webhookAuthMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		providedSecret := r.Header.Get("X-Telegram-Bot-Api-Secret-Token")
		if !safeSecretCompare(providedSecret, a.webhookSecret) {
			appmetrics.WebhookUpdatesTotal.WithLabelValues("unauthorized").Inc()
			slog.Warn("unauthorized webhook request", "remote_ip", r.RemoteAddr)
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		next(w, r)
	}
}

func (a *App) acquireCronAdvisoryLock(ctx context.Context) (*sql.Conn, bool, error) {
	conn, err := a.db.Conn(ctx)
	if err != nil {
		return nil, false, err
	}

	var acquired bool
	if err := conn.QueryRowContext(ctx, `SELECT pg_try_advisory_lock($1)`, workers.CronAdvisoryLockKey).Scan(&acquired); err != nil {
		_ = conn.Close()
		return nil, false, err
	}

	if !acquired {
		_ = conn.Close()
		return nil, false, nil
	}

	return conn, true, nil
}

func releaseCronAdvisoryLock(ctx context.Context, conn *sql.Conn) {
	if conn == nil {
		return
	}

	releaseCtx, cancel := finalizationContext(ctx, 2*time.Second)
	defer cancel()

	if _, err := conn.ExecContext(releaseCtx, `SELECT pg_advisory_unlock($1)`, workers.CronAdvisoryLockKey); err != nil {
		slog.Error("failed to release cron advisory lock", "error", err)
	}
	if err := conn.Close(); err != nil {
		slog.Error("failed to close cron advisory lock connection", "error", err)
	}
}

func (a *App) handleCron(w http.ResponseWriter, r *http.Request) {
	lockConn, acquired, err := a.acquireCronAdvisoryLock(r.Context())
	if err != nil {
		appmetrics.CronRunsTotal.WithLabelValues("lock_error").Inc()
		slog.Error("failed to acquire cron advisory lock", "error", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	if !acquired {
		appmetrics.CronRunsTotal.WithLabelValues("conflict").Inc()
		slog.Warn(
			"prevented overlapping cron job execution across replicas, request discarded",
			"remote_ip",
			r.RemoteAddr,
		)
		w.WriteHeader(http.StatusConflict)
		_, _ = w.Write([]byte("Cron execution already in progress"))
		return
	}
	defer releaseCronAdvisoryLock(r.Context(), lockConn)

	slog.Info("valid cron trigger received, creating durable notification jobs")
	ctx := r.Context()

	createdJobs, err := a.createCronNotificationJobs(ctx)
	if err != nil {
		appmetrics.CronRunsTotal.WithLabelValues("create_jobs_error").Inc()
		slog.Error("failed to create durable notification jobs", "error", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	if createdJobs == 0 {
		appmetrics.CronRunsTotal.WithLabelValues("no_jobs").Inc()
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("No notification jobs created"))
		return
	}

	appmetrics.CronClaimedSubscribersTotal.Add(float64(createdJobs))
	appmetrics.CronRunsTotal.WithLabelValues("accepted").Inc()
	slog.Info("cron batch accepted and durably stored", "jobs", createdJobs)
	w.WriteHeader(http.StatusAccepted)
	_, _ = w.Write([]byte("Cron batch accepted"))
}

func (a *App) metricsAuthMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		authHeader := r.Header.Get("Authorization")
		var providedToken string
		if strings.HasPrefix(authHeader, "Bearer ") {
			providedToken = authHeader[7:]
		}

		if !safeSecretCompare(providedToken, a.metricsSecret) {
			slog.Warn("unauthorized metrics endpoint access blocked", "remote_ip", r.RemoteAddr)
			w.WriteHeader(http.StatusUnauthorized)
			return
		}

		next(w, r)
	}
}

func (a *App) handleWebhook(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)

	payload, err := io.ReadAll(r.Body)
	if err != nil {
		appmetrics.WebhookUpdatesTotal.WithLabelValues("bad_request").Inc()
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	var update tgbotapi.Update
	if err := json.Unmarshal(payload, &update); err != nil {
		appmetrics.WebhookUpdatesTotal.WithLabelValues("bad_request").Inc()
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	if update.UpdateID == 0 {
		appmetrics.WebhookUpdatesTotal.WithLabelValues("bad_request").Inc()
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	inserted, err := a.saveTelegramUpdate(r.Context(), update, payload)
	if err != nil {
		appmetrics.WebhookUpdatesTotal.WithLabelValues("persist_error").Inc()
		slog.Error("failed to persist telegram update", "update_id", update.UpdateID, "error", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	if inserted {
		appmetrics.WebhookUpdatesTotal.WithLabelValues("accepted").Inc()
	} else {
		appmetrics.WebhookUpdatesTotal.WithLabelValues("duplicate").Inc()
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("OK"))
}
