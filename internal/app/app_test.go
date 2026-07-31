package app

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"golang.org/x/time/rate"

	"github.com/igor-zatochniy/cryptopulse-telegram-bot/internal/httpserver"
	"github.com/igor-zatochniy/cryptopulse-telegram-bot/internal/workers"
)

func TestMetricsAuthMiddleware(t *testing.T) {
	application := &App{metricsSecret: "test-secret"}

	tests := []struct {
		name       string
		authHeader string
		wantStatus int
		wantCalled bool
	}{
		{name: "missing authorization", wantStatus: http.StatusUnauthorized},
		{
			name:       "wrong bearer token",
			authHeader: "Bearer wrong-secret",
			wantStatus: http.StatusUnauthorized,
		},
		{
			name:       "valid bearer token",
			authHeader: "Bearer test-secret",
			wantStatus: http.StatusNoContent,
			wantCalled: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			called := false
			handler := application.metricsAuthMiddleware(func(w http.ResponseWriter, _ *http.Request) {
				called = true
				w.WriteHeader(http.StatusNoContent)
			})

			request := httptest.NewRequest(http.MethodGet, "/metrics", nil)
			if test.authHeader != "" {
				request.Header.Set("Authorization", test.authHeader)
			}
			response := httptest.NewRecorder()
			handler(response, request)

			if response.Code != test.wantStatus {
				t.Fatalf("status = %d, want %d", response.Code, test.wantStatus)
			}
			if called != test.wantCalled {
				t.Fatalf("called = %v, want %v", called, test.wantCalled)
			}
		})
	}
}

func TestCronAuthenticationPrecedesRateLimit(t *testing.T) {
	application := &App{cronSecret: "cron-secret"}
	limiter := rate.NewLimiter(rate.Every(time.Hour), 1)
	handler := application.cronAuthMiddleware(
		httpserver.GlobalRateLimit(limiter, func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusNoContent)
		}),
	)

	unauthorized := httptest.NewRequest(http.MethodPost, "/cron", nil)
	unauthorizedRecorder := httptest.NewRecorder()
	handler(unauthorizedRecorder, unauthorized)
	if unauthorizedRecorder.Code != http.StatusUnauthorized {
		t.Fatalf("unauthorized status = %d, want %d", unauthorizedRecorder.Code, http.StatusUnauthorized)
	}

	authorized := httptest.NewRequest(http.MethodPost, "/cron", nil)
	authorized.Header.Set("Authorization", "Bearer cron-secret")
	authorizedRecorder := httptest.NewRecorder()
	handler(authorizedRecorder, authorized)
	if authorizedRecorder.Code != http.StatusNoContent {
		t.Fatalf("authorized status = %d, want %d", authorizedRecorder.Code, http.StatusNoContent)
	}
}

func TestWebhookAuthenticationPrecedesClientRateLimit(t *testing.T) {
	application := &App{webhookSecret: "webhook-secret"}
	limiter := httpserver.NewClientRateLimiter(rate.Every(time.Hour), 1, time.Minute)
	handler := application.webhookAuthMiddleware(
		httpserver.ClientRateLimit(limiter, func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusNoContent)
		}),
	)

	unauthorized := httptest.NewRequest(http.MethodPost, "/webhook", nil)
	unauthorized.RemoteAddr = "192.0.2.10:1234"
	unauthorizedRecorder := httptest.NewRecorder()
	handler(unauthorizedRecorder, unauthorized)
	if unauthorizedRecorder.Code != http.StatusUnauthorized {
		t.Fatalf("unauthorized status = %d, want %d", unauthorizedRecorder.Code, http.StatusUnauthorized)
	}

	authorized := httptest.NewRequest(http.MethodPost, "/webhook", nil)
	authorized.RemoteAddr = unauthorized.RemoteAddr
	authorized.Header.Set("X-Telegram-Bot-Api-Secret-Token", "webhook-secret")
	authorizedRecorder := httptest.NewRecorder()
	handler(authorizedRecorder, authorized)
	if authorizedRecorder.Code != http.StatusNoContent {
		t.Fatalf("authorized status = %d, want %d", authorizedRecorder.Code, http.StatusNoContent)
	}
}

func TestScheduledNotificationTimeUsesKyivDST(t *testing.T) {
	kyivLocation, err := time.LoadLocation("Europe/Kyiv")
	if err != nil {
		t.Fatalf("load Europe/Kyiv timezone: %v", err)
	}
	application := &App{kyivLoc: kyivLocation}

	tests := []struct {
		name        string
		scheduledAt time.Time
		want        string
	}{
		{
			name:        "winter uses UTC plus two",
			scheduledAt: time.Date(2026, time.January, 15, 12, 0, 0, 0, time.UTC),
			want:        "14:00",
		},
		{
			name:        "summer uses UTC plus three",
			scheduledAt: time.Date(2026, time.July, 15, 12, 0, 0, 0, time.UTC),
			want:        "15:00",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := application.formatScheduledNotificationTime(test.scheduledAt); got != test.want {
				t.Fatalf("formatted time = %q, want %q", got, test.want)
			}
		})
	}
}

func TestFinalizationContextSurvivesParentCancellation(t *testing.T) {
	type contextKey string
	const requestIDKey contextKey = "request-id"

	parent, cancelParent := context.WithCancel(
		context.WithValue(context.Background(), requestIDKey, "req-42"),
	)
	cancelParent()

	finalCtx, cancelFinal := finalizationContext(parent, time.Second)
	defer cancelFinal()

	if err := finalCtx.Err(); err != nil {
		t.Fatalf("finalization context canceled with parent: %v", err)
	}
	if got := finalCtx.Value(requestIDKey); got != "req-42" {
		t.Fatalf("request id = %v, want req-42", got)
	}
}

func TestFormattedPricesUsesSourceTimestampAndMarksStaleData(t *testing.T) {
	kyivLocation := time.FixedZone("Kyiv", 3*60*60)
	updatedAt := time.Date(2026, time.July, 28, 10, 30, 0, 0, time.UTC)
	now := updatedAt.Add(priceFreshnessLimit + time.Second)
	application := &App{
		priceCache: &PriceCache{store: make(map[string]PriceEntry)},
		kyivLoc:    kyivLocation,
	}
	application.priceCache.StoreAt("BTCUSDT", 62500, updatedAt)

	got := application.getFormattedPricesFromCacheAt("ua", now)
	if !strings.Contains(got, "13:30:00") {
		t.Fatalf("formatted prices do not contain source timestamp: %q", got)
	}
	if !strings.Contains(got, "Частина даних застаріла") {
		t.Fatalf("formatted prices do not mark stale data: %q", got)
	}
	if !strings.Contains(got, "⚠️ BTC") {
		t.Fatalf("stale price does not use warning marker: %q", got)
	}
}

func TestFormattedPricesDoesNotMarkFreshDataAsStale(t *testing.T) {
	updatedAt := time.Date(2026, time.July, 28, 10, 30, 0, 0, time.UTC)
	application := &App{
		priceCache: &PriceCache{store: make(map[string]PriceEntry)},
		kyivLoc:    time.UTC,
	}
	application.priceCache.StoreAt("BTCUSDT", 62500, updatedAt)

	got := application.getFormattedPricesFromCacheAt("en", updatedAt.Add(30*time.Second))
	if strings.Contains(got, "Some data is stale") {
		t.Fatalf("fresh data marked as stale: %q", got)
	}
}

func TestInteractiveRepliesAreCollectedWithoutCallingTelegram(t *testing.T) {
	collector := &telegramReplyCollector{}
	ctx := withTelegramReplyCollector(context.Background(), collector)
	application := &App{}

	application.sendSafeMessage(ctx, 42, "message", nil)
	application.editSafeMessage(ctx, 42, 7, "edited", nil)

	if collector.err != nil {
		t.Fatalf("collect replies: %v", collector.err)
	}
	if len(collector.replies) != 2 {
		t.Fatalf("collected replies = %d, want 2", len(collector.replies))
	}
	if collector.replies[0].Operation != telegramReplySendMessage {
		t.Fatalf("first operation = %q, want %q", collector.replies[0].Operation, telegramReplySendMessage)
	}
	if collector.replies[1].Operation != telegramReplyEditMessage {
		t.Fatalf("second operation = %q, want %q", collector.replies[1].Operation, telegramReplyEditMessage)
	}
}

func TestTelegramWorkersCoverEveryPersistentShard(t *testing.T) {
	const workerCount = 10
	owners := make(map[int32]int, workers.TelegramUpdateShardCount)

	for workerID := 0; workerID < workerCount; workerID++ {
		for _, shardID := range telegramWorkerShardIDs(workerID, workerCount) {
			owners[shardID]++
		}
	}

	if len(owners) != workers.TelegramUpdateShardCount {
		t.Fatalf("covered shards = %d, want %d", len(owners), workers.TelegramUpdateShardCount)
	}
	for shardID := int32(0); shardID < workers.TelegramUpdateShardCount; shardID++ {
		if owners[shardID] != 1 {
			t.Fatalf("shard %d owner count = %d, want 1", shardID, owners[shardID])
		}
	}
}
