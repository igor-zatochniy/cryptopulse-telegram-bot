//go:build integration

package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
)

const (
	integrationDBName   = "cryptopulse"
	integrationDBUser   = "cryptopulse_user"
	integrationDBPass   = "integration_password"
	integrationBotToken = "integration-token"
)

type fakeTelegramResponse struct {
	OK          bool
	ErrorCode   int
	Description string
}

type fakeTelegramServer struct {
	server *httptest.Server
	mu     sync.Mutex
	calls  []string
	send   func(chatID int64) fakeTelegramResponse
}

func newFakeTelegramBot(t *testing.T, send func(chatID int64) fakeTelegramResponse) *tgbotapi.BotAPI {
	t.Helper()

	fake := &fakeTelegramServer{send: send}
	fake.server = httptest.NewServer(http.HandlerFunc(fake.handle))
	t.Cleanup(fake.server.Close)

	bot, err := tgbotapi.NewBotAPIWithClient(
		integrationBotToken,
		fake.server.URL+"/bot%s/%s",
		fake.server.Client(),
	)
	if err != nil {
		t.Fatalf("create fake telegram bot: %v", err)
	}

	return bot
}

func (s *fakeTelegramServer) handle(w http.ResponseWriter, r *http.Request) {
	method := r.URL.Path[strings.LastIndex(r.URL.Path, "/")+1:]

	s.mu.Lock()
	s.calls = append(s.calls, method)
	s.mu.Unlock()

	w.Header().Set("Content-Type", "application/json")

	switch method {
	case "getMe":
		writeTelegramJSON(w, map[string]any{
			"ok": true,
			"result": map[string]any{
				"id":         100500,
				"is_bot":     true,
				"first_name": "Integration",
				"username":   "integration_bot",
			},
		})
	case "answerCallbackQuery":
		writeTelegramJSON(w, map[string]any{
			"ok":     true,
			"result": true,
		})
	case "sendMessage":
		if err := r.ParseForm(); err != nil {
			writeTelegramJSON(w, map[string]any{
				"ok":          false,
				"error_code":  http.StatusBadRequest,
				"description": err.Error(),
			})
			return
		}

		chatID := parseChatID(r.Form)
		if s.send != nil {
			resp := s.send(chatID)
			if !resp.OK {
				writeTelegramJSON(w, map[string]any{
					"ok":          false,
					"error_code":  resp.ErrorCode,
					"description": resp.Description,
				})
				return
			}
		}

		writeTelegramJSON(w, map[string]any{
			"ok": true,
			"result": map[string]any{
				"message_id": 1,
				"date":       1,
				"chat": map[string]any{
					"id":   chatID,
					"type": "private",
				},
			},
		})
	default:
		writeTelegramJSON(w, map[string]any{
			"ok":     true,
			"result": true,
		})
	}
}

func writeTelegramJSON(w http.ResponseWriter, payload map[string]any) {
	_ = json.NewEncoder(w).Encode(payload)
}

func parseChatID(form url.Values) int64 {
	chatID, _ := strconv.ParseInt(form.Get("chat_id"), 10, 64)
	return chatID
}

func setupIntegrationDB(t *testing.T) *sql.DB {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	container, err := postgres.Run(
		ctx,
		"postgres:16-alpine",
		postgres.WithDatabase(integrationDBName),
		postgres.WithUsername(integrationDBUser),
		postgres.WithPassword(integrationDBPass),
		postgres.BasicWaitStrategies(),
	)
	if err != nil {
		skipOrFailUnavailableTestcontainer(t, err)
	}
	t.Cleanup(func() {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer shutdownCancel()
		if err := container.Terminate(shutdownCtx); err != nil {
			t.Logf("terminate postgres testcontainer: %v", err)
		}
	})

	connString, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("postgres connection string: %v", err)
	}

	db, err := sql.Open("pgx", connString)
	if err != nil {
		t.Fatalf("open postgres: %v", err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Logf("close postgres db: %v", err)
		}
	})

	db.SetMaxOpenConns(5)
	db.SetMaxIdleConns(5)

	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("ping postgres: %v", err)
	}

	migration, err := os.ReadFile("migrations/001_init_schema.sql")
	if err != nil {
		t.Fatalf("read migration: %v", err)
	}
	if _, err := db.ExecContext(ctx, string(migration)); err != nil {
		t.Fatalf("apply migration: %v", err)
	}

	return db
}

func skipOrFailUnavailableTestcontainer(t *testing.T, err error) {
	t.Helper()

	if os.Getenv("CI") == "true" {
		t.Fatalf("postgres testcontainer unavailable in CI: %v", err)
	}

	t.Skipf("postgres testcontainer unavailable: %v", err)
}

func newIntegrationApp(t *testing.T, db *sql.DB, bot *tgbotapi.BotAPI) *App {
	t.Helper()

	cache, err := lru.New[int64, string](1000)
	if err != nil {
		t.Fatalf("create language cache: %v", err)
	}

	app := &App{
		db:                   db,
		bot:                  bot,
		priceCache:           &PriceCache{store: make(map[string]PriceEntry)},
		langCache:            cache,
		telegramUpdateShards: newTelegramUpdateShards(telegramShardCount, telegramShardBuffer),
		kyivLoc:              time.UTC,
		httpClient:           http.DefaultClient,
		webhookSecret:        "webhook-secret",
		cronSecret:           "cron-secret",
	}

	for _, coin := range trackedCoins {
		app.priceCache.Store(coin.Symbol, 100)
	}

	return app
}

func TestIntegrationSubscribeAfterLanguageSelection(t *testing.T) {
	db := setupIntegrationDB(t)
	bot := newFakeTelegramBot(t, nil)
	app := newIntegrationApp(t, db, bot)

	const chatID int64 = 101

	app.processTelegramUpdate(context.Background(), tgbotapi.Update{
		CallbackQuery: &tgbotapi.CallbackQuery{
			ID:   "set-language",
			Data: "setlang_en",
			Message: &tgbotapi.Message{
				Chat: &tgbotapi.Chat{ID: chatID},
			},
		},
	})

	assertSubscriberState(t, db, chatID, false, 60, "en")

	app.processTelegramUpdate(context.Background(), tgbotapi.Update{
		Message: &tgbotapi.Message{
			Text: "/subscribe",
			Chat: &tgbotapi.Chat{ID: chatID},
			Entities: []tgbotapi.MessageEntity{
				{Type: "bot_command", Offset: 0, Length: len("/subscribe")},
			},
		},
	})

	assertSubscriberState(t, db, chatID, true, 60, "en")
	assertClaimCleared(t, db, chatID)
}

func TestIntegrationIntervalRequiresSubscriptionAndUpdatesSubscribedUser(t *testing.T) {
	db := setupIntegrationDB(t)
	bot := newFakeTelegramBot(t, nil)
	app := newIntegrationApp(t, db, bot)

	const inactiveChatID int64 = 201

	app.processTelegramUpdate(context.Background(), tgbotapi.Update{
		Message: &tgbotapi.Message{
			Text: "/interval",
			Chat: &tgbotapi.Chat{ID: inactiveChatID},
			Entities: []tgbotapi.MessageEntity{
				{Type: "bot_command", Offset: 0, Length: len("/interval")},
			},
		},
	})

	assertNoSubscriberRow(t, db, inactiveChatID)

	const activeChatID int64 = 202
	oldLastSent := time.Now().Add(-2 * time.Hour).UTC()
	insertSubscriber(t, db, activeChatID, true, 60, "ua", oldLastSent)

	app.processTelegramUpdate(context.Background(), tgbotapi.Update{
		CallbackQuery: &tgbotapi.CallbackQuery{
			ID:   "set-interval",
			Data: "int_5",
			Message: &tgbotapi.Message{
				Chat: &tgbotapi.Chat{ID: activeChatID},
			},
		},
	})

	assertSubscriberState(t, db, activeChatID, true, 5, "ua")

	var newLastSent time.Time
	if err := db.QueryRow("SELECT last_sent FROM subscribers WHERE chat_id = $1", activeChatID).Scan(&newLastSent); err != nil {
		t.Fatalf("select interval last_sent: %v", err)
	}
	if !newLastSent.After(oldLastSent) {
		t.Fatalf("last_sent was not advanced: got %s, old %s", newLastSent, oldLastSent)
	}
}

func TestIntegrationTelegramWorkersPreserveSameChatOrder(t *testing.T) {
	db := setupIntegrationDB(t)
	bot := newFakeTelegramBot(t, nil)
	app := newIntegrationApp(t, db, bot)

	const chatID int64 = 306

	firstStarted := make(chan struct{})
	releaseFirst := make(chan struct{})
	var once sync.Once

	app.beforeTelegramUpdate = func(ctx context.Context, update tgbotapi.Update) {
		if update.Message == nil || update.Message.Text != "/subscribe" {
			return
		}

		once.Do(func() {
			close(firstStarted)
		})

		select {
		case <-releaseFirst:
		case <-ctx.Done():
		}
	}

	runCtx, cancel := context.WithCancel(context.Background())
	var workerWG sync.WaitGroup
	for _, updates := range app.telegramUpdateShards {
		workerWG.Add(1)
		go app.updateWorker(runCtx, &workerWG, updates)
	}
	t.Cleanup(func() {
		cancel()
		for _, updates := range app.telegramUpdateShards {
			close(updates)
		}
		workerWG.Wait()
	})

	if !app.enqueueTelegramUpdate(context.Background(), commandUpdate(chatID, "/subscribe")) {
		t.Fatal("enqueue subscribe update")
	}

	select {
	case <-firstStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("subscribe update did not start")
	}

	if !app.enqueueTelegramUpdate(context.Background(), commandUpdate(chatID, "/unsubscribe")) {
		t.Fatal("enqueue unsubscribe update")
	}

	close(releaseFirst)
	waitForSubscribed(t, db, chatID, false)
}

func TestIntegrationCronClaimAndTelegramDeliveryOutcomes(t *testing.T) {
	db := setupIntegrationDB(t)
	bot := newFakeTelegramBot(t, func(chatID int64) fakeTelegramResponse {
		switch chatID {
		case 302:
			return fakeTelegramResponse{
				OK:          false,
				ErrorCode:   http.StatusTooManyRequests,
				Description: "Too Many Requests: retry later",
			}
		case 303:
			return fakeTelegramResponse{
				OK:          false,
				ErrorCode:   http.StatusForbidden,
				Description: "Forbidden: bot was blocked by the user",
			}
		default:
			return fakeTelegramResponse{OK: true}
		}
	})
	app := newIntegrationApp(t, db, bot)

	oldLastSent := time.Now().Add(-2 * time.Hour).UTC()
	insertSubscriber(t, db, 301, true, 1, "ua", oldLastSent)
	insertSubscriber(t, db, 302, true, 1, "ua", oldLastSent)
	insertSubscriber(t, db, 303, true, 1, "ua", oldLastSent)

	runCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var workerWG sync.WaitGroup
	workerWG.Add(1)
	go app.alertWorker(runCtx, &workerWG)
	t.Cleanup(workerWG.Wait)

	req := httptest.NewRequest(http.MethodPost, "/cron", nil)
	req.Header.Set("Authorization", "Bearer cron-secret")
	rec := httptest.NewRecorder()

	app.handleCron(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("cron status = %d, body = %q", rec.Code, rec.Body.String())
	}
	received301 := waitForNotificationJobStatus(t, db, 301, "sent")
	pending302 := waitForNotificationJobStatus(t, db, 302, "pending")
	failed303 := waitForNotificationJobStatus(t, db, 303, "failed")

	if received301.Attempts != 1 {
		t.Fatalf("job 301 attempts = %d, want 1", received301.Attempts)
	}
	if pending302.Attempts != 1 {
		t.Fatalf("job 302 attempts = %d, want 1", pending302.Attempts)
	}
	if failed303.Attempts != 1 {
		t.Fatalf("job 303 attempts = %d, want 1", failed303.Attempts)
	}
	if !pending302.LastError.Valid || !strings.Contains(pending302.LastError.String, "retry later") {
		t.Fatalf("job 302 last_error = %+v, want transient retry message", pending302.LastError)
	}
	if !failed303.LastError.Valid || !strings.Contains(failed303.LastError.String, "blocked") {
		t.Fatalf("job 303 last_error = %+v, want permanent block message", failed303.LastError)
	}

	assertLastSentAdvanced(t, db, 301, oldLastSent)
	assertClaimCleared(t, db, 301)
	assertSubscribed(t, db, 301, true)

	assertLastSentUnchanged(t, db, 302, oldLastSent)
	assertClaimCleared(t, db, 302)
	assertSubscribed(t, db, 302, true)

	assertLastSentUnchanged(t, db, 303, oldLastSent)
	assertClaimCleared(t, db, 303)
	assertSubscribed(t, db, 303, false)
}

func TestIntegrationCronReturnsBeforeTelegramDeliveryCompletes(t *testing.T) {
	db := setupIntegrationDB(t)
	sendStarted := make(chan struct{})
	releaseSend := make(chan struct{})
	var once sync.Once

	bot := newFakeTelegramBot(t, func(chatID int64) fakeTelegramResponse {
		once.Do(func() {
			close(sendStarted)
		})
		<-releaseSend
		return fakeTelegramResponse{OK: true}
	})
	app := newIntegrationApp(t, db, bot)

	const chatID int64 = 305
	oldLastSent := time.Now().Add(-2 * time.Hour).UTC()
	insertSubscriber(t, db, chatID, true, 1, "ua", oldLastSent)

	runCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var workerWG sync.WaitGroup
	workerWG.Add(1)
	go app.alertWorker(runCtx, &workerWG)
	t.Cleanup(workerWG.Wait)

	req := httptest.NewRequest(http.MethodPost, "/cron", nil)
	req.Header.Set("Authorization", "Bearer cron-secret")
	rec := httptest.NewRecorder()

	done := make(chan int, 1)
	go func() {
		app.handleCron(rec, req)
		done <- rec.Code
	}()

	select {
	case code := <-done:
		if code != http.StatusAccepted {
			t.Fatalf("cron status = %d, body = %q", code, rec.Body.String())
		}
	case <-time.After(1 * time.Second):
		t.Fatal("cron handler waited for Telegram delivery")
	}

	select {
	case <-sendStarted:
	case <-time.After(1 * time.Second):
		t.Fatal("telegram delivery did not start")
	}

	stateDuringSend := waitForNotificationJobStatus(t, db, chatID, "sending")
	if stateDuringSend.Attempts != 1 {
		t.Fatalf("job attempts while sending = %d, want 1", stateDuringSend.Attempts)
	}

	close(releaseSend)
	waitForNotificationJobStatus(t, db, chatID, "sent")
	assertLastSentAdvanced(t, db, chatID, oldLastSent)
}

func TestIntegrationCronRecordsClaimMinuteAsLastSent(t *testing.T) {
	db := setupIntegrationDB(t)
	bot := newFakeTelegramBot(t, nil)
	app := newIntegrationApp(t, db, bot)

	const chatID int64 = 304
	oldLastSent := time.Now().Add(-2 * time.Hour).UTC()
	insertSubscriber(t, db, chatID, true, 1, "ua", oldLastSent)

	runCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var workerWG sync.WaitGroup
	workerWG.Add(1)
	go app.alertWorker(runCtx, &workerWG)
	t.Cleanup(workerWG.Wait)

	req := httptest.NewRequest(http.MethodPost, "/cron", nil)
	req.Header.Set("Authorization", "Bearer cron-secret")
	rec := httptest.NewRecorder()
	app.handleCron(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("cron status = %d, body = %q", rec.Code, rec.Body.String())
	}

	job := waitForNotificationJobStatus(t, db, chatID, "sent")
	lastSent := selectLastSent(t, db, chatID)
	expectedLastSent := job.ScheduledAt.Truncate(time.Minute)
	if lastSent.Sub(expectedLastSent).Abs() > time.Second {
		t.Fatalf("last_sent = %s, want close to claimed minute %s", lastSent, expectedLastSent)
	}
	assertClaimCleared(t, db, chatID)
}

func TestIntegrationExhaustedTransientFailureSuspendsSubscriber(t *testing.T) {
	db := setupIntegrationDB(t)
	bot := newFakeTelegramBot(t, func(chatID int64) fakeTelegramResponse {
		return fakeTelegramResponse{
			OK:          false,
			ErrorCode:   http.StatusTooManyRequests,
			Description: "Too Many Requests: retry later",
		}
	})
	app := newIntegrationApp(t, db, bot)

	const chatID int64 = 306
	oldLastSent := time.Now().Add(-2 * time.Hour).UTC()
	insertSubscriber(t, db, chatID, true, 1, "ua", oldLastSent)

	created, err := app.createCronNotificationJobs(context.Background())
	if err != nil {
		t.Fatalf("create cron notification jobs: %v", err)
	}
	if created != 1 {
		t.Fatalf("created jobs = %d, want 1", created)
	}

	for attempt := 1; attempt <= notificationJobMaxAttempts; attempt++ {
		job, err := app.claimPendingNotificationJob(context.Background())
		if err != nil {
			t.Fatalf("claim notification job attempt %d: %v", attempt, err)
		}
		if job == nil {
			t.Fatalf("claim notification job attempt %d returned nil", attempt)
		}

		app.processNotificationJob(*job)

		if attempt < notificationJobMaxAttempts {
			if _, err := db.Exec(
				"UPDATE notification_jobs SET next_attempt_at = NOW() - INTERVAL '1 second' WHERE id = $1",
				job.ID,
			); err != nil {
				t.Fatalf("advance retry attempt %d: %v", attempt, err)
			}
		}
	}

	failedJob := waitForNotificationJobStatus(t, db, chatID, "failed")
	if failedJob.Attempts != notificationJobMaxAttempts {
		t.Fatalf("failed job attempts = %d, want %d", failedJob.Attempts, notificationJobMaxAttempts)
	}

	assertLastSentUnchanged(t, db, chatID, oldLastSent)
	assertClaimCleared(t, db, chatID)
	assertDeliverySuspended(t, db, chatID)

	createdAgain, err := app.createCronNotificationJobs(context.Background())
	if err != nil {
		t.Fatalf("create cron notification jobs after suspension: %v", err)
	}
	if createdAgain != 0 {
		t.Fatalf("created jobs after suspension = %d, want 0", createdAgain)
	}
	assertNotificationJobCount(t, db, chatID, 1)
}

func TestIntegrationNotificationJobRetentionCleanup(t *testing.T) {
	db := setupIntegrationDB(t)
	bot := newFakeTelegramBot(t, nil)
	app := newIntegrationApp(t, db, bot)

	insertNotificationJobForRetention(t, db, 701, "sent", time.Now().Add(-31*24*time.Hour), sql.NullTime{})
	insertNotificationJobForRetention(t, db, 702, "sent", time.Now().Add(-29*24*time.Hour), sql.NullTime{})
	insertNotificationJobForRetention(t, db, 703, "failed", time.Now().Add(-91*24*time.Hour), sql.NullTime{})
	insertNotificationJobForRetention(t, db, 704, "failed", time.Now().Add(-89*24*time.Hour), sql.NullTime{})
	insertNotificationJobForRetention(t, db, 705, "pending", time.Now().Add(-120*24*time.Hour), sql.NullTime{})
	insertNotificationJobForRetention(t, db, 706, "sending", time.Now().Add(-120*24*time.Hour), sql.NullTime{Time: time.Now().Add(-1 * time.Hour), Valid: true})

	deleted, err := app.cleanupNotificationJobHistory(context.Background())
	if err != nil {
		t.Fatalf("cleanup notification job history: %v", err)
	}
	if deleted != 2 {
		t.Fatalf("deleted jobs = %d, want 2", deleted)
	}

	assertNoNotificationJobs(t, db, 701)
	assertNotificationJobCount(t, db, 702, 1)
	assertNoNotificationJobs(t, db, 703)
	assertNotificationJobCount(t, db, 704, 1)
	assertNotificationJobCount(t, db, 705, 1)
	assertNotificationJobCount(t, db, 706, 1)
}

func TestIntegrationCronUsesPostgresAdvisoryLock(t *testing.T) {
	db := setupIntegrationDB(t)
	bot := newFakeTelegramBot(t, nil)
	app := newIntegrationApp(t, db, bot)

	const chatID int64 = 307
	oldLastSent := time.Now().Add(-2 * time.Hour).UTC()
	insertSubscriber(t, db, chatID, true, 1, "ua", oldLastSent)

	lockConn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatalf("open advisory lock connection: %v", err)
	}
	t.Cleanup(func() {
		unlockCtx, unlockCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer unlockCancel()
		_, _ = lockConn.ExecContext(unlockCtx, `SELECT pg_advisory_unlock($1)`, cronAdvisoryLockKey)
		_ = lockConn.Close()
	})

	lockCtx, lockCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer lockCancel()
	var acquired bool
	if err := lockConn.QueryRowContext(lockCtx, `SELECT pg_try_advisory_lock($1)`, cronAdvisoryLockKey).Scan(&acquired); err != nil {
		t.Fatalf("acquire advisory lock: %v", err)
	}
	if !acquired {
		t.Fatal("failed to acquire advisory lock in test setup")
	}

	req := httptest.NewRequest(http.MethodPost, "/cron", nil)
	req.Header.Set("Authorization", "Bearer cron-secret")
	rec := httptest.NewRecorder()

	app.handleCron(rec, req)

	if rec.Code != http.StatusConflict {
		t.Fatalf("cron status = %d, body = %q", rec.Code, rec.Body.String())
	}
	assertNoNotificationJobs(t, db, chatID)
	assertLastSentUnchanged(t, db, chatID, oldLastSent)
}

func insertSubscriber(t *testing.T, db *sql.DB, chatID int64, subscribed bool, interval int, lang string, lastSent time.Time) {
	t.Helper()

	_, err := db.Exec(
		`INSERT INTO subscribers (chat_id, interval_minutes, last_sent, language_code, is_subscribed)
		 VALUES ($1, $2, $3, $4, $5)`,
		chatID,
		interval,
		lastSent,
		lang,
		subscribed,
	)
	if err != nil {
		t.Fatalf("insert subscriber %d: %v", chatID, err)
	}
}

func insertNotificationJobForRetention(t *testing.T, db *sql.DB, chatID int64, status string, terminalAt time.Time, claimedUntil sql.NullTime) {
	t.Helper()

	var sentAt sql.NullTime
	var failedAt sql.NullTime
	switch status {
	case "sent":
		sentAt = sql.NullTime{Time: terminalAt, Valid: true}
	case "failed":
		failedAt = sql.NullTime{Time: terminalAt, Valid: true}
	}

	_, err := db.Exec(
		`INSERT INTO notification_jobs (
			chat_id,
			language_code,
			message_text,
			scheduled_at,
			status,
			attempts,
			claimed_until,
			next_attempt_at,
			sent_at,
			failed_at,
			last_error
		) VALUES ($1, 'ua', 'retention test', $2, $3, 1, $4, NOW(), $5, $6, 'retention test error')`,
		chatID,
		terminalAt,
		status,
		claimedUntil,
		sentAt,
		failedAt,
	)
	if err != nil {
		t.Fatalf("insert notification job %d/%s: %v", chatID, status, err)
	}
}

func commandUpdate(chatID int64, command string) tgbotapi.Update {
	return tgbotapi.Update{
		Message: &tgbotapi.Message{
			Text: command,
			Chat: &tgbotapi.Chat{ID: chatID},
			Entities: []tgbotapi.MessageEntity{
				{Type: "bot_command", Offset: 0, Length: len(command)},
			},
		},
	}
}

func waitForSubscribed(t *testing.T, db *sql.DB, chatID int64, want bool) {
	t.Helper()

	deadline := time.After(5 * time.Second)
	tick := time.NewTicker(20 * time.Millisecond)
	defer tick.Stop()

	for {
		var got bool
		err := db.QueryRow("SELECT is_subscribed FROM subscribers WHERE chat_id = $1", chatID).Scan(&got)
		if err == nil && got == want {
			return
		}

		select {
		case <-deadline:
			if err != nil {
				t.Fatalf("subscriber %d did not reach subscribed=%v: %v", chatID, want, err)
			}
			t.Fatalf("subscriber %d did not reach subscribed=%v", chatID, want)
		case <-tick.C:
		}
	}
}

func assertSubscriberState(t *testing.T, db *sql.DB, chatID int64, wantSubscribed bool, wantInterval int, wantLang string) {
	t.Helper()

	var (
		gotSubscribed bool
		gotInterval   int
		gotLang       string
	)
	err := db.QueryRow(
		"SELECT is_subscribed, interval_minutes, language_code FROM subscribers WHERE chat_id = $1",
		chatID,
	).Scan(&gotSubscribed, &gotInterval, &gotLang)
	if err != nil {
		t.Fatalf("select subscriber %d: %v", chatID, err)
	}

	if gotSubscribed != wantSubscribed || gotInterval != wantInterval || gotLang != wantLang {
		t.Fatalf(
			"subscriber %d = subscribed:%v interval:%d lang:%s, want subscribed:%v interval:%d lang:%s",
			chatID,
			gotSubscribed,
			gotInterval,
			gotLang,
			wantSubscribed,
			wantInterval,
			wantLang,
		)
	}
}

func assertNoSubscriberRow(t *testing.T, db *sql.DB, chatID int64) {
	t.Helper()

	var count int
	if err := db.QueryRow("SELECT COUNT(*) FROM subscribers WHERE chat_id = $1", chatID).Scan(&count); err != nil {
		t.Fatalf("count subscriber %d: %v", chatID, err)
	}
	if count != 0 {
		t.Fatalf("subscriber %d row exists, want none", chatID)
	}
}

func assertSubscribed(t *testing.T, db *sql.DB, chatID int64, want bool) {
	t.Helper()

	var got bool
	if err := db.QueryRow("SELECT is_subscribed FROM subscribers WHERE chat_id = $1", chatID).Scan(&got); err != nil {
		t.Fatalf("select subscribed %d: %v", chatID, err)
	}
	if got != want {
		t.Fatalf("subscriber %d is_subscribed = %v, want %v", chatID, got, want)
	}
}

func assertClaimCleared(t *testing.T, db *sql.DB, chatID int64) {
	t.Helper()

	var isNull bool
	if err := db.QueryRow("SELECT cron_claimed_until IS NULL FROM subscribers WHERE chat_id = $1", chatID).Scan(&isNull); err != nil {
		t.Fatalf("select cron claim %d: %v", chatID, err)
	}
	if !isNull {
		t.Fatalf("subscriber %d cron_claimed_until is not NULL", chatID)
	}
}

func assertDeliverySuspended(t *testing.T, db *sql.DB, chatID int64) {
	t.Helper()

	var suspendedUntil sql.NullTime
	if err := db.QueryRow("SELECT delivery_suspended_until FROM subscribers WHERE chat_id = $1", chatID).Scan(&suspendedUntil); err != nil {
		t.Fatalf("select delivery suspension %d: %v", chatID, err)
	}
	if !suspendedUntil.Valid {
		t.Fatalf("subscriber %d delivery_suspended_until is NULL", chatID)
	}
	if !suspendedUntil.Time.After(time.Now().Add(-1 * time.Second)) {
		t.Fatalf("subscriber %d delivery_suspended_until = %s, want future time", chatID, suspendedUntil.Time)
	}
}

func assertClaimActive(t *testing.T, db *sql.DB, chatID int64) {
	t.Helper()

	var isNull bool
	if err := db.QueryRow("SELECT cron_claimed_until IS NULL FROM subscribers WHERE chat_id = $1", chatID).Scan(&isNull); err != nil {
		t.Fatalf("select cron claim %d: %v", chatID, err)
	}
	if isNull {
		t.Fatalf("subscriber %d cron_claimed_until is NULL, want active claim", chatID)
	}
}

func assertLastSentAdvanced(t *testing.T, db *sql.DB, chatID int64, oldLastSent time.Time) {
	t.Helper()

	lastSent := selectLastSent(t, db, chatID)
	if !lastSent.After(oldLastSent) {
		t.Fatalf("subscriber %d last_sent = %s, want after %s", chatID, lastSent, oldLastSent)
	}
}

func assertLastSentUnchanged(t *testing.T, db *sql.DB, chatID int64, oldLastSent time.Time) {
	t.Helper()

	lastSent := selectLastSent(t, db, chatID)
	if lastSent.Sub(oldLastSent).Abs() > time.Second {
		t.Fatalf("subscriber %d last_sent = %s, want close to %s", chatID, lastSent, oldLastSent)
	}
}

func selectLastSent(t *testing.T, db *sql.DB, chatID int64) time.Time {
	t.Helper()

	var lastSent time.Time
	if err := db.QueryRow("SELECT last_sent FROM subscribers WHERE chat_id = $1", chatID).Scan(&lastSent); err != nil {
		t.Fatalf("select last_sent %d: %v", chatID, err)
	}
	return lastSent
}

type notificationJobState struct {
	Status        string
	Attempts      int
	ScheduledAt   time.Time
	NextAttemptAt time.Time
	ClaimedUntil  sql.NullTime
	SentAt        sql.NullTime
	FailedAt      sql.NullTime
	LastError     sql.NullString
}

func waitForNotificationJobStatus(t *testing.T, db *sql.DB, chatID int64, wantStatus string) notificationJobState {
	t.Helper()

	deadline := time.After(10 * time.Second)
	tick := time.NewTicker(25 * time.Millisecond)
	defer tick.Stop()

	for {
		job, err := selectNotificationJobState(t, db, chatID)
		if err == nil && job.Status == wantStatus {
			return job
		}

		select {
		case <-deadline:
			if err != nil {
				t.Fatalf("notification job %d did not reach status %q: %v", chatID, wantStatus, err)
			}
			t.Fatalf("notification job %d did not reach status %q, last status = %q", chatID, wantStatus, job.Status)
		case <-tick.C:
		}
	}
}

func selectNotificationJobState(t *testing.T, db *sql.DB, chatID int64) (notificationJobState, error) {
	t.Helper()

	var job notificationJobState
	err := db.QueryRow(
		`SELECT status, attempts, scheduled_at, next_attempt_at, claimed_until, sent_at, failed_at, last_error
		 FROM notification_jobs
		 WHERE chat_id = $1
		 ORDER BY id DESC
		 LIMIT 1`,
		chatID,
	).Scan(
		&job.Status,
		&job.Attempts,
		&job.ScheduledAt,
		&job.NextAttemptAt,
		&job.ClaimedUntil,
		&job.SentAt,
		&job.FailedAt,
		&job.LastError,
	)
	return job, err
}

func assertNoNotificationJobs(t *testing.T, db *sql.DB, chatID int64) {
	t.Helper()

	assertNotificationJobCount(t, db, chatID, 0)
}

func assertNotificationJobCount(t *testing.T, db *sql.DB, chatID int64, want int) {
	t.Helper()

	var count int
	if err := db.QueryRow("SELECT COUNT(*) FROM notification_jobs WHERE chat_id = $1", chatID).Scan(&count); err != nil {
		t.Fatalf("count notification jobs %d: %v", chatID, err)
	}
	if count != want {
		t.Fatalf("notification jobs for chat %d = %d, want %d", chatID, count, want)
	}
}

func TestIntegrationPermanentTelegramErrorClassifier(t *testing.T) {
	err := &tgbotapi.Error{
		Code:    http.StatusForbidden,
		Message: "Forbidden: bot was blocked by the user",
	}
	if !isPermanentTelegramSendError(err) {
		t.Fatal("forbidden Telegram error was not classified as permanent")
	}

	transientErr := &tgbotapi.Error{
		Code:    http.StatusTooManyRequests,
		Message: "Too Many Requests: retry later",
	}
	if isPermanentTelegramSendError(transientErr) {
		t.Fatal("429 Telegram error was classified as permanent")
	}
}
