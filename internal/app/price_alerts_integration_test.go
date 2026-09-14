//go:build integration

package app

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/igor-zatochniy/cryptopulse-telegram-bot/internal/storage"
	"github.com/igor-zatochniy/cryptopulse-telegram-bot/internal/workers"
	"github.com/igor-zatochniy/cryptopulse-telegram-bot/migrations"
	"github.com/pressly/goose/v3"
)

func processAlertCommandForTest(t *testing.T, app *App, updateID int, chatID int64, command string) {
	t.Helper()
	update := commandUpdateWithID(updateID, chatID, command)
	update.Message.Entities[0].Length = len(strings.Fields(command)[0])
	saveIntegrationTelegramUpdate(t, app, update)
	job, err := app.claimPendingTelegramUpdateForWorker(context.Background(), 0, 1)
	if err != nil || job == nil {
		t.Fatalf("claim update: job=%v, err=%v", job, err)
	}
	app.processTelegramUpdateJob(context.Background(), *job)
}

func requireSQL(t *testing.T, db *sql.DB, query string, args ...any) {
	t.Helper()
	if _, err := db.Exec(query, args...); err != nil {
		t.Fatalf("SQL: %v", err)
	}
}

func assertSQLCount(t *testing.T, db *sql.DB, want int, query string, args ...any) {
	t.Helper()
	var got int
	if err := db.QueryRow(query, args...).Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("query count=%d, want %d: %s", got, want, query)
	}
}

func TestIntegrationPriceAlertCreationIsAtomicAndDeduplicated(t *testing.T) {
	db := setupIntegrationDB(t)
	app := newIntegrationApp(t, db, newFakeTelegramBot(t, nil))
	app.priceAlertsEnabled = true
	requireSQL(t, db, `ALTER TABLE telegram_replies ADD CONSTRAINT reject_price_alert_reply CHECK (source_update_id <> 80001)`)
	processAlertCommandForTest(t, app, 80001, 800, "/alerts BTC +5")
	assertSQLCount(t, db, 0, `SELECT COUNT(*) FROM price_alerts`)
	assertTelegramUpdateStatus(t, db, 80001, "pending")
	assertTelegramReplyCount(t, db, 80001, 0)

	requireSQL(t, db, `ALTER TABLE telegram_replies DROP CONSTRAINT reject_price_alert_reply`)
	requireSQL(t, db, `UPDATE telegram_updates SET next_attempt_at = NOW() WHERE update_id = 80001`)
	job, err := app.claimPendingTelegramUpdateForWorker(context.Background(), 0, 1)
	if err != nil || job == nil {
		t.Fatalf("retry claim: %v, %v", job, err)
	}
	app.processTelegramUpdateJob(context.Background(), *job)
	assertTelegramUpdateStatus(t, db, 80001, "processed")
	assertTelegramReplyCount(t, db, 80001, 1)
	assertSQLCount(t, db, 1, `SELECT COUNT(*) FROM price_alerts WHERE base_price = 100 AND target_price = 105`)
	assertNoSubscriberRow(t, db, 800)
	processAlertCommandForTest(t, app, 80002, 800, "/allerts BTC +5")
	assertSQLCount(t, db, 1, `SELECT COUNT(*) FROM price_alerts`)
}

func TestIntegrationPriceAlertThresholdsAreOneShotAcrossReplicas(t *testing.T) {
	db := setupIntegrationDB(t)
	app := newIntegrationApp(t, db, newFakeTelegramBot(t, nil))
	other := newIntegrationApp(t, db, app.bot)
	app.priceAlertsEnabled, other.priceAlertsEnabled = true, true
	processAlertCommandForTest(t, app, 80101, 801, "/alerts BTC +5")
	processAlertCommandForTest(t, app, 80102, 801, "/alerts BTC -5")
	if n, err := app.evaluatePriceAlerts(context.Background(), "BTCUSDT", 104.99, time.Now().UTC()); n != 0 || err != nil {
		t.Fatalf("early trigger=%d, %v", n, err)
	}
	results := make(chan error, 2)
	for _, replica := range []*App{app, other} {
		go func(a *App) {
			_, err := a.evaluatePriceAlerts(context.Background(), "BTCUSDT", 105, time.Now().UTC())
			results <- err
		}(replica)
	}
	for range 2 {
		if err := <-results; err != nil {
			t.Fatal(err)
		}
	}
	assertSQLCount(t, db, 1, `SELECT COUNT(*) FROM notification_jobs WHERE kind = 'price_alert'`)
	if n, err := app.evaluatePriceAlerts(context.Background(), "BTCUSDT", 95, time.Now().UTC()); n != 1 || err != nil {
		t.Fatalf("fall trigger=%d, %v", n, err)
	}
	if n, err := app.evaluatePriceAlerts(context.Background(), "BTCUSDT", 120, time.Now().UTC()); n != 0 || err != nil {
		t.Fatalf("repeated trigger=%d, %v", n, err)
	}
	assertSQLCount(t, db, 2, `SELECT COUNT(*) FROM notification_jobs WHERE kind = 'price_alert'`)
	assertSQLCount(t, db, 2, `SELECT COUNT(*) FROM price_alerts WHERE base_price = 100 AND status = 'triggered'`)
}

func TestIntegrationPriceAlertTriggerRollsBackWhenEnqueueFails(t *testing.T) {
	db := setupIntegrationDB(t)
	app := newIntegrationApp(t, db, newFakeTelegramBot(t, nil))
	app.priceAlertsEnabled = true
	processAlertCommandForTest(t, app, 80201, 802, "/alerts ETH +5")
	requireSQL(t, db, `ALTER TABLE notification_jobs ADD CONSTRAINT reject_price_alert_job CHECK (kind <> 'price_alert')`)
	if _, err := app.evaluatePriceAlerts(context.Background(), "ETHUSDT", 106, time.Now().UTC()); err == nil {
		t.Fatal("expected enqueue error")
	}
	assertSQLCount(t, db, 1, `SELECT COUNT(*) FROM price_alerts WHERE status = 'active' AND trigger_price IS NULL`)
	assertSQLCount(t, db, 0, `SELECT COUNT(*) FROM notification_jobs`)
	requireSQL(t, db, `ALTER TABLE notification_jobs DROP CONSTRAINT reject_price_alert_job`)
	if n, err := app.evaluatePriceAlerts(context.Background(), "ETHUSDT", 106, time.Now().UTC()); n != 1 || err != nil {
		t.Fatalf("enqueue after recovery=%d, %v", n, err)
	}
}

func TestIntegrationPriceAlertDeliveryDoesNotChangeScheduledCadence(t *testing.T) {
	db := setupIntegrationDB(t)
	bot, server := newFakeTelegramBotWithServer(t, nil)
	app := newIntegrationApp(t, db, bot)
	app.priceAlertsEnabled = true
	old := time.Now().UTC().Add(-5 * time.Minute).Truncate(time.Second)
	insertSubscriber(t, db, 803, true, 1, "ua", old)
	processAlertCommandForTest(t, app, 80301, 803, "/alerts SOL +10")
	if n, err := app.evaluatePriceAlerts(context.Background(), "SOLUSDT", 110, time.Now().UTC()); n != 1 || err != nil {
		t.Fatalf("trigger=%d, %v", n, err)
	}
	if n, err := app.createCronNotificationJobs(context.Background()); n != 1 || err != nil {
		t.Fatalf("scheduled job blocked by price alert: %d, %v", n, err)
	}
	// Перевіряємо доставку алерта першою незалежно від різниці годинників host і PostgreSQL.
	requireSQL(t, db, `UPDATE notification_jobs SET next_attempt_at = NOW() + INTERVAL '1 hour' WHERE kind = 'scheduled'`)
	job, err := app.claimPendingNotificationJob(context.Background())
	if err != nil || job == nil || job.Kind != notificationPriceAlert {
		t.Fatalf("claim price alert: %+v, %v", job, err)
	}
	app.processNotificationJob(context.Background(), *job)
	assertLastSentUnchanged(t, db, 803, old)
	assertClaimActive(t, db, 803)
	assertSQLCount(t, db, 1, `SELECT COUNT(*) FROM notification_jobs WHERE kind = 'price_alert' AND status = 'sent'`)
	requireSQL(t, db, `UPDATE notification_jobs SET next_attempt_at = NOW() WHERE kind = 'scheduled'`)
	job, err = app.claimPendingNotificationJob(context.Background())
	if err != nil || job == nil || job.Kind != "scheduled" {
		t.Fatalf("claim scheduled job: %+v, %v", job, err)
	}
	app.processNotificationJob(context.Background(), *job)
	assertLastSentAdvanced(t, db, 803, old)
	if server.callCount("sendMessage") != 2 {
		t.Fatalf("send count=%d", server.callCount("sendMessage"))
	}
}

func TestIntegrationPriceAlertUnsubscribeAndCancelIsolation(t *testing.T) {
	db := setupIntegrationDB(t)
	bot, server := newFakeTelegramBotWithServer(t, nil)
	app := newIntegrationApp(t, db, bot)
	app.priceAlertsEnabled = true
	insertSubscriber(t, db, 804, true, 1, "ua", time.Now().Add(-time.Hour))
	processAlertCommandForTest(t, app, 80401, 804, "/alerts BNB -5")
	if _, err := app.evaluatePriceAlerts(context.Background(), "BNBUSDT", 95, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	if _, err := app.createCronNotificationJobs(context.Background()); err != nil {
		t.Fatal(err)
	}
	processAlertCommandForTest(t, app, 80402, 804, "/unsubscribe")
	assertSubscribed(t, db, 804, false)
	assertSQLCount(t, db, 1, `SELECT COUNT(*) FROM notification_jobs WHERE kind = 'scheduled' AND status = 'canceled'`)
	assertSQLCount(t, db, 1, `SELECT COUNT(*) FROM notification_jobs WHERE kind = 'price_alert' AND status = 'pending'`)
	job, err := app.claimPendingNotificationJob(context.Background())
	if err != nil || job == nil {
		t.Fatalf("claim: %v, %v", job, err)
	}
	app.priceAlertsEnabled = false
	app.processNotificationJob(context.Background(), *job)
	if server.callCount("sendMessage") != 1 {
		t.Fatal("queued price alert not delivered after disabling creation/unsubscribing")
	}

	app.priceAlertsEnabled = true
	processAlertCommandForTest(t, app, 80403, 804, "/alerts ETH +5")
	if _, err := app.evaluatePriceAlerts(context.Background(), "ETHUSDT", 105, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	job, err = app.claimPendingNotificationJob(context.Background())
	if err != nil || job == nil {
		t.Fatalf("claim for cancellation: %v, %v", job, err)
	}
	if canceled, err := app.cancelPriceAlert(context.Background(), db, 999, job.PriceAlertID); canceled || err != nil {
		t.Fatalf("another chat canceled alert: %v, %v", canceled, err)
	}
	callback := languageCallbackUpdate(804, "cancel-alert", fmt.Sprintf("alerts_del_%d", job.PriceAlertID))
	callback.UpdateID = 80404
	saveIntegrationTelegramUpdate(t, app, callback)
	inbox, err := app.claimPendingTelegramUpdateForWorker(context.Background(), 0, 1)
	if err != nil || inbox == nil {
		t.Fatalf("cancel inbox: %v, %v", inbox, err)
	}
	app.processTelegramUpdateJob(context.Background(), *inbox)
	app.processNotificationJob(context.Background(), *job)
	if server.callCount("sendMessage") != 1 {
		t.Fatal("canceled alert was sent")
	}
	assertSQLCount(t, db, 1, `SELECT COUNT(*) FROM notification_jobs WHERE price_alert_id = $1 AND status = 'canceled'`, job.PriceAlertID)
}

func TestIntegrationPriceAlertFailuresPreserveSubscriberState(t *testing.T) {
	for _, code := range []int{http.StatusTooManyRequests, http.StatusForbidden} {
		t.Run(fmt.Sprint(code), func(t *testing.T) {
			db := setupIntegrationDB(t)
			bot := newFakeTelegramBot(t, func(int64) fakeTelegramResponse {
				return fakeTelegramResponse{OK: false, ErrorCode: code, Description: "delivery failed"}
			})
			app := newIntegrationApp(t, db, bot)
			app.priceAlertsEnabled = true
			old := time.Now().UTC().Add(-time.Hour).Truncate(time.Second)
			insertSubscriber(t, db, 805, true, 1, "ua", old)
			requireSQL(t, db, `UPDATE subscribers SET cron_claimed_until = NOW() + INTERVAL '15 minutes', delivery_suspended_until = NOW() + INTERVAL '1 hour'`)
			processAlertCommandForTest(t, app, 80501, 805, "/alerts BTC +5")
			if _, err := app.evaluatePriceAlerts(context.Background(), "BTCUSDT", 105, time.Now().UTC()); err != nil {
				t.Fatal(err)
			}
			for attempt := 1; attempt <= workers.NotificationJobMaxAttempts; attempt++ {
				job, err := app.claimPendingNotificationJob(context.Background())
				if err != nil || job == nil {
					t.Fatalf("claim attempt %d: %v, %v", attempt, job, err)
				}
				app.processNotificationJob(context.Background(), *job)
				assertLastSentUnchanged(t, db, 805, old)
				assertSubscribed(t, db, 805, true)
				assertClaimActive(t, db, 805)
				assertDeliverySuspended(t, db, 805)
				if code == http.StatusForbidden {
					break
				}
				requireSQL(t, db, `UPDATE notification_jobs SET next_attempt_at = NOW()`)
			}
			assertSQLCount(t, db, 1, `SELECT COUNT(*) FROM notification_jobs WHERE status = 'failed'`)
			if n, err := app.evaluatePriceAlerts(context.Background(), "BTCUSDT", 110, time.Now().UTC()); n != 0 || err != nil {
				t.Fatalf("exhausted alert was recreated: %d, %v", n, err)
			}
		})
	}
}

func TestIntegrationPriceAlertValidationLimitsAndRetention(t *testing.T) {
	db := setupIntegrationDB(t)
	app := newIntegrationApp(t, db, newFakeTelegramBot(t, nil))
	processAlertCommandForTest(t, app, 80600, 806, "/alerts BTC +5")
	assertSQLCount(t, db, 0, `SELECT COUNT(*) FROM price_alerts`)
	app.priceAlertsEnabled = true
	app.priceCache.StoreAt("BTCUSDT", 100, time.Now().Add(-2*time.Minute))
	processAlertCommandForTest(t, app, 80601, 806, "/alerts BTC +5")
	assertSQLCount(t, db, 0, `SELECT COUNT(*) FROM price_alerts`)
	app.priceCache.Store("BTCUSDT", 100)
	for i := 1; i <= 11; i++ {
		processAlertCommandForTest(t, app, 80610+i, 806, fmt.Sprintf("/alerts BTC +%d", i))
	}
	assertSQLCount(t, db, 10, `SELECT COUNT(*) FROM price_alerts WHERE status = 'active'`)
	if n, err := app.evaluatePriceAlerts(context.Background(), "BTCUSDT", 120, time.Now().UTC()); n != 10 || err != nil {
		t.Fatalf("batch trigger=%d, %v", n, err)
	}
	processAlertCommandForTest(t, app, 80650, 806, "/alerts ETH +5")
	assertSQLCount(t, db, 10, `SELECT COUNT(*) FROM price_alerts`)
	requireSQL(t, db, `UPDATE price_alerts SET updated_at = NOW() - INTERVAL '100 days'`)
	if deleted, err := app.cleanupPriceAlertHistory(context.Background()); deleted != 0 || err != nil {
		t.Fatalf("deleted alerts with outstanding jobs: %d, %v", deleted, err)
	}
	requireSQL(t, db, `UPDATE notification_jobs SET status = 'sent', sent_at = NOW() - INTERVAL '100 days'`)
	if deleted, err := app.cleanupNotificationJobHistory(context.Background()); deleted != 10 || err != nil {
		t.Fatalf("job cleanup=%d, %v", deleted, err)
	}
	if deleted, err := app.cleanupPriceAlertHistory(context.Background()); deleted != 10 || err != nil {
		t.Fatalf("alert cleanup=%d, %v", deleted, err)
	}
}

func TestIntegrationPriceAlertButtonsAndPriceTicker(t *testing.T) {
	db := setupIntegrationDB(t)
	app := newIntegrationApp(t, db, newFakeTelegramBot(t, nil))
	app.priceAlertsEnabled = true
	callback := languageCallbackUpdate(807, "create-alert", "alerts_add_BTC_+5")
	callback.UpdateID = 80701
	saveIntegrationTelegramUpdate(t, app, callback)
	job, err := app.claimPendingTelegramUpdateForWorker(context.Background(), 0, 1)
	if err != nil || job == nil {
		t.Fatalf("callback claim: %v, %v", job, err)
	}
	app.processTelegramUpdateJob(context.Background(), *job)
	assertTelegramUpdateStatus(t, db, 80701, "processed")
	assertSQLCount(t, db, 1, `SELECT COUNT(*) FROM price_alerts WHERE target_price = 105 AND chat_id = 807`)
	app.httpClient = &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(strings.NewReader(fmt.Sprintf(`{"symbol":%q,"price":"105"}`, request.URL.Query().Get("symbol")))),
			Header:     make(http.Header),
		}, nil
	})}
	app.fetchAndCachePrices(context.Background())
	assertSQLCount(t, db, 1, `SELECT COUNT(*) FROM notification_jobs WHERE kind = 'price_alert' AND status = 'pending'`)
	processAlertCommandForTest(t, app, 80702, 807, "/allerts list")
	assertTelegramReplyCount(t, db, 80702, 1)
}

func TestIntegrationPriceAlertRetryRecoversWithoutSubscription(t *testing.T) {
	db := setupIntegrationDB(t)
	attempts := 0
	bot := newFakeTelegramBot(t, func(int64) fakeTelegramResponse {
		attempts++
		if attempts == 1 {
			return fakeTelegramResponse{OK: false, ErrorCode: 429, Description: "temporary rate limit"}
		}
		return fakeTelegramResponse{OK: true}
	})
	app := newIntegrationApp(t, db, bot)
	app.priceAlertsEnabled = true
	processAlertCommandForTest(t, app, 80801, 808, "/alerts ETH -5")
	if _, err := app.evaluatePriceAlerts(context.Background(), "ETHUSDT", 95, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	job, err := app.claimPendingNotificationJob(context.Background())
	if err != nil || job == nil {
		t.Fatalf("first claim: %v, %v", job, err)
	}
	app.processNotificationJob(context.Background(), *job)
	assertSQLCount(t, db, 1, `SELECT COUNT(*) FROM notification_jobs WHERE status = 'pending' AND attempts = 1`)
	requireSQL(t, db, `UPDATE notification_jobs SET next_attempt_at = NOW()`)
	other := newIntegrationApp(t, db, bot)
	job, err = other.claimPendingNotificationJob(context.Background())
	if err != nil || job == nil {
		t.Fatalf("recovery claim: %v, %v", job, err)
	}
	other.processNotificationJob(context.Background(), *job)
	assertSQLCount(t, db, 1, `SELECT COUNT(*) FROM notification_jobs WHERE status = 'sent' AND attempts = 2`)
	assertNoSubscriberRow(t, db, 808)
	if attempts != 2 {
		t.Fatalf("send attempts = %d", attempts)
	}
}

func TestIntegrationPriceAlertMigrationUpgradeAndGuardedRollback(t *testing.T) {
	db := setupIntegrationDB(t)
	provider, err := goose.NewProvider(goose.DialectPostgres, db, migrations.Files)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := provider.DownTo(context.Background(), 9); err != nil {
		t.Fatal(err)
	}
	insertSubscriber(t, db, 809, true, 1, "ua", time.Now().Add(-time.Hour))
	requireSQL(t, db, `INSERT INTO notification_jobs (chat_id, language_code, message_text, scheduled_at) VALUES (809, 'ua', 'scheduled', NOW())`)
	if _, err := storage.ApplyMigrations(context.Background(), db); err != nil {
		t.Fatal(err)
	}
	assertSQLCount(t, db, 1, `SELECT COUNT(*) FROM notification_jobs WHERE kind = 'scheduled' AND price_alert_id IS NULL`)
	assertSubscribed(t, db, 809, true)
	app := newIntegrationApp(t, db, newFakeTelegramBot(t, nil))
	app.priceAlertsEnabled = true
	processAlertCommandForTest(t, app, 80901, 809, "/alerts BTC +5")
	provider, err = goose.NewProvider(goose.DialectPostgres, db, migrations.Files)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := provider.DownTo(context.Background(), 9); err == nil {
		t.Fatal("rollback discarded live alert data")
	}
	assertSQLCount(t, db, 1, `SELECT COUNT(*) FROM price_alerts`)
	if err := storage.VerifySchema(context.Background(), db); err != nil {
		t.Fatal(err)
	}
	for _, statement := range []string{
		`UPDATE price_alerts SET base_price = 'NaN'`,
		`UPDATE price_alerts SET target_price = 'Infinity'`,
		`UPDATE price_alerts SET target_price = 0`,
		`UPDATE price_alerts SET change_percent = 'NaN'`,
		`UPDATE price_alerts SET change_percent = -100`,
		`UPDATE price_alerts SET symbol = 'UNKNOWN'`,
	} {
		if _, err := db.Exec(statement); err == nil {
			t.Errorf("constraint accepted: %s", statement)
		}
	}
}

func TestIntegrationPriceAlertMenuPreservesCommandsAcrossLanguages(t *testing.T) {
	bot := newFakeTelegramBot(t, nil)
	languages := make(map[string]bool)
	bot.Client = &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		body := `{"ok":true,"result":[{"command":"custom","description":"Existing command"}]}`
		if strings.HasSuffix(request.URL.Path, "/setMyCommands") {
			if err := request.ParseForm(); err != nil {
				t.Fatal(err)
			}
			var commands []struct {
				Command string `json:"command"`
			}
			if err := json.Unmarshal([]byte(request.Form.Get("commands")), &commands); err != nil {
				t.Fatal(err)
			}
			if len(commands) != 2 || commands[0].Command != "custom" || commands[1].Command != "alerts" {
				t.Fatalf("unexpected menu: %+v", commands)
			}
			languages[request.Form.Get("language_code")] = true
			body = `{"ok":true,"result":true}`
		}
		return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body))}, nil
	})}
	app := &App{bot: bot, priceAlertsEnabled: true}
	app.syncPriceAlertMenu(context.Background())
	for _, lang := range []string{"", "uk", "en", "ru"} {
		if !languages[lang] {
			t.Errorf("menu missing language %q", lang)
		}
	}
}

func TestIntegrationPriceAlertBatchKeepsCommittedProgress(t *testing.T) {
	db := setupIntegrationDB(t)
	app := newIntegrationApp(t, db, newFakeTelegramBot(t, nil))
	app.priceAlertsEnabled = true
	processAlertCommandForTest(t, app, 81001, 810, "/alerts BTC +5")
	processAlertCommandForTest(t, app, 81002, 811, "/alerts BTC +5")
	insertSubscriber(t, db, 810, false, 60, "en", time.Now())
	requireSQL(t, db, `ALTER TABLE notification_jobs ADD CONSTRAINT reject_second_alert CHECK (chat_id <> 811)`)
	if n, err := app.evaluatePriceAlerts(context.Background(), "BTCUSDT", 105, time.Now().UTC()); n != 1 || err == nil {
		t.Fatalf("partial progress=%d, %v; want one commit followed by failure", n, err)
	}
	assertSQLCount(t, db, 1, `SELECT COUNT(*) FROM price_alerts WHERE chat_id = 810 AND status = 'triggered'`)
	assertSQLCount(t, db, 1, `SELECT COUNT(*) FROM price_alerts WHERE chat_id = 811 AND status = 'active'`)
	assertSQLCount(t, db, 1, `SELECT COUNT(*) FROM notification_jobs WHERE chat_id = 810 AND language_code = 'en'`)
	requireSQL(t, db, `ALTER TABLE notification_jobs DROP CONSTRAINT reject_second_alert`)
	if n, err := app.evaluatePriceAlerts(context.Background(), "BTCUSDT", 105, time.Now().UTC()); n != 1 || err != nil {
		t.Fatalf("recovered progress=%d, %v", n, err)
	}
	assertSQLCount(t, db, 2, `SELECT COUNT(*) FROM notification_jobs`)
}
