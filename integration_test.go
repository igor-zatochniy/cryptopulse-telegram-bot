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
	integrationDBName   = "ethbot"
	integrationDBUser   = "ethbot_user"
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
		db:                 db,
		bot:                bot,
		priceCache:         &PriceCache{store: make(map[string]PriceEntry)},
		langCache:          cache,
		telegramUpdateChan: make(chan tgbotapi.Update, 100),
		cronJobChan:        make(chan Job, 100),
		kyivLoc:            time.UTC,
		httpClient:         http.DefaultClient,
		webhookSecret:      "webhook-secret",
		cronSecret:         "cron-secret",
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

	app.processTelegramUpdate(tgbotapi.Update{
		CallbackQuery: &tgbotapi.CallbackQuery{
			ID:   "set-language",
			Data: "setlang_en",
			Message: &tgbotapi.Message{
				Chat: &tgbotapi.Chat{ID: chatID},
			},
		},
	})

	assertSubscriberState(t, db, chatID, false, 60, "en")

	app.processTelegramUpdate(tgbotapi.Update{
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

	app.processTelegramUpdate(tgbotapi.Update{
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

	app.processTelegramUpdate(tgbotapi.Update{
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
	t.Cleanup(func() {
		cancel()
		close(app.cronJobChan)
		workerWG.Wait()
	})

	req := httptest.NewRequest(http.MethodPost, "/cron", nil)
	req.Header.Set("Authorization", "Bearer cron-secret")
	rec := httptest.NewRecorder()

	app.handleCron(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("cron status = %d, body = %q", rec.Code, rec.Body.String())
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

func TestIntegrationCronRecordsClaimMinuteAsLastSent(t *testing.T) {
	db := setupIntegrationDB(t)
	bot := newFakeTelegramBot(t, nil)
	app := newIntegrationApp(t, db, bot)

	const chatID int64 = 304
	oldLastSent := time.Now().Add(-2 * time.Hour).UTC()
	insertSubscriber(t, db, chatID, true, 1, "ua", oldLastSent)

	subs, err := app.claimDueSubscribers(context.Background())
	if err != nil {
		t.Fatalf("claim due subscribers: %v", err)
	}
	if len(subs) != 1 {
		t.Fatalf("claimed subscribers = %d, want 1", len(subs))
	}
	if subs[0].ID != chatID {
		t.Fatalf("claimed chat id = %d, want %d", subs[0].ID, chatID)
	}
	if subs[0].ClaimedAt.IsZero() {
		t.Fatal("claimed_at is zero")
	}

	if err := app.markCronDeliveriesSent([]int64{chatID}, subs[0].ClaimedAt); err != nil {
		t.Fatalf("mark cron delivery sent: %v", err)
	}

	lastSent := selectLastSent(t, db, chatID)
	expectedLastSent := subs[0].ClaimedAt.Truncate(time.Minute)
	if lastSent.Sub(expectedLastSent).Abs() > time.Second {
		t.Fatalf("last_sent = %s, want close to claimed minute %s", lastSent, expectedLastSent)
	}
	assertClaimCleared(t, db, chatID)
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
