package app

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"

	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"

	"github.com/igor-zatochniy/cryptopulse-telegram-bot/internal/config"
	"github.com/igor-zatochniy/cryptopulse-telegram-bot/internal/httpserver"
	"github.com/igor-zatochniy/cryptopulse-telegram-bot/internal/storage"
	"github.com/igor-zatochniy/cryptopulse-telegram-bot/internal/workers"
)

// Run створює залежності, запускає server і координує graceful shutdown.
func Run(ctx context.Context, cfg config.Config) error {
	kyivLocation, err := time.LoadLocation("Europe/Kyiv")
	if err != nil {
		slog.Warn("failed to load Europe/Kyiv timezone", "error", err)
		kyivLocation = time.FixedZone("Kyiv", 3*60*60)
	}

	database, err := storage.Open(ctx, cfg.DatabaseURL)
	if err != nil {
		return fmt.Errorf("database unreachable: %w", err)
	}
	defer database.Close()

	appliedMigrations, err := storage.ApplyMigrations(ctx, database)
	if err != nil {
		return fmt.Errorf("database migration failed: %w", err)
	}
	if appliedMigrations > 0 {
		slog.Info("database migrations applied", "count", appliedMigrations)
	}

	if err := storage.VerifySchema(ctx, database); err != nil {
		return fmt.Errorf("database schema incompatible: %w", err)
	}

	telegramHTTPClient := &http.Client{Timeout: 10 * time.Second}
	bot, err := tgbotapi.NewBotAPIWithClient(cfg.TelegramToken, tgbotapi.APIEndpoint, telegramHTTPClient)
	if err != nil {
		return fmt.Errorf("initialize Telegram API: %w", err)
	}

	application := &App{
		db:            database,
		bot:           bot,
		priceCache:    &PriceCache{store: make(map[string]PriceEntry)},
		kyivLoc:       kyivLocation,
		httpClient:    newMarketHTTPClient(),
		webhookSecret: cfg.WebhookSecret,
		cronSecret:    cfg.CronSecret,
		metricsSecret: cfg.MetricsSecret,
	}

	runCtx, stopRun := context.WithCancel(ctx)
	defer stopRun()
	workerCtx, stopWorkers := context.WithCancel(context.WithoutCancel(runCtx))
	defer stopWorkers()

	application.WarmupCache(runCtx)
	go application.startPriceTicker(runCtx)
	go application.startNotificationRetentionCleaner(runCtx)

	var telegramWG sync.WaitGroup
	for workerID := 0; workerID < cfg.TelegramUpdateWorkers; workerID++ {
		telegramWG.Add(1)
		go application.updateWorkerPartition(
			workerCtx,
			&telegramWG,
			workerID,
			cfg.TelegramUpdateWorkers,
		)
	}

	var replyWG sync.WaitGroup
	for range workers.TelegramReplyWorkerCount {
		replyWG.Add(1)
		go application.replyWorker(workerCtx, &replyWG)
	}

	var notificationWG sync.WaitGroup
	const notificationWorkerCount = 5
	for range notificationWorkerCount {
		notificationWG.Add(1)
		go application.alertWorker(workerCtx, &notificationWG)
	}

	server := httpserver.New(cfg.Port, application.Handler())
	serverErr := make(chan error, 1)
	go func() {
		slog.Info("HTTP server started", "port", cfg.Port)
		if listenErr := server.ListenAndServe(); listenErr != nil && listenErr != http.ErrServerClosed {
			serverErr <- listenErr
			stopRun()
		}
	}()

	var runErr error
	select {
	case <-runCtx.Done():
		if ctx.Err() == nil {
			runErr = <-serverErr
		}
	case runErr = <-serverErr:
		stopRun()
	}

	slog.Info("shutdown started")
	application.stopAcceptingProducers()

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	if err := server.Shutdown(shutdownCtx); err != nil {
		slog.Error("graceful HTTP shutdown failed", "error", err)
		if closeErr := server.Close(); closeErr != nil {
			slog.Error("forced HTTP shutdown failed", "error", closeErr)
		}
	}
	shutdownCancel()

	producerCtx, producerCancel := context.WithTimeout(context.Background(), 15*time.Second)
	if err := application.waitForProducers(producerCtx); err != nil {
		slog.Warn("producer drain timed out", "error", err)
	}
	producerCancel()

	stopWorkers()
	telegramWG.Wait()
	replyWG.Wait()
	notificationWG.Wait()
	slog.Info("background workers stopped")

	if runErr != nil {
		return fmt.Errorf("HTTP server stopped unexpectedly: %w", runErr)
	}
	return nil
}

func newMarketHTTPClient() *http.Client {
	return &http.Client{
		Timeout: 5 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        100,
			MaxIdleConnsPerHost: 30,
			IdleConnTimeout:     90 * time.Second,
		},
	}
}
