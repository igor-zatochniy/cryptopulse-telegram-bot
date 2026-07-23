package main

import (
	"context"
	"crypto/subtle"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
	_ "time/tzdata"

	"log/slog"

	// Сучасний драйвер pgx/v5 через stdlib-обгортку
	_ "github.com/jackc/pgx/v5/stdlib"

	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
	"github.com/joho/godotenv"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"golang.org/x/time/rate"
)

// --- ТИПИ ДАНИХ І КЕШ ІЗ СИНХРОНІЗАЦІЄЮ ---

type Subscriber struct {
	ID        int64
	Lang      string
	ClaimedAt time.Time
}

type PriceEntry struct {
	Current  float64
	Previous float64
}

type PriceCache struct {
	mu    sync.RWMutex
	store map[string]PriceEntry
}

func (c *PriceCache) Load(symbol string) (PriceEntry, bool) {
	c.mu.RLock()
	val, ok := c.store[symbol]
	c.mu.RUnlock()
	return val, ok
}

func (c *PriceCache) Store(symbol string, newPrice float64) {
	c.mu.Lock()
	defer c.mu.Unlock()

	oldEntry, ok := c.store[symbol]
	if !ok {
		c.store[symbol] = PriceEntry{Current: newPrice, Previous: newPrice}
		return
	}

	c.store[symbol] = PriceEntry{
		Current:  newPrice,
		Previous: oldEntry.Current,
	}
}

type NotificationJob struct {
	ID          int64
	ChatID      int64
	Lang        string
	Text        string
	ClaimToken  string
	ScheduledAt time.Time
	Attempts    int
}

type TelegramUpdateJob struct {
	UpdateID int64
	ChatID   int64
	Payload  string
	Attempts int
}

type clientRateLimitEntry struct {
	limiter  *rate.Limiter
	lastSeen time.Time
}

type ClientRateLimiter struct {
	mu          sync.Mutex
	limit       rate.Limit
	burst       int
	ttl         time.Duration
	lastCleanup time.Time
	clients     map[string]*clientRateLimitEntry
}

func newClientRateLimiter(limit rate.Limit, burst int, ttl time.Duration) *ClientRateLimiter {
	if ttl <= 0 {
		ttl = 10 * time.Minute
	}

	return &ClientRateLimiter{
		limit:   limit,
		burst:   burst,
		ttl:     ttl,
		clients: make(map[string]*clientRateLimitEntry),
	}
}

func (l *ClientRateLimiter) Allow(clientKey string) bool {
	if clientKey == "" {
		clientKey = "unknown"
	}

	now := time.Now()

	l.mu.Lock()
	if now.Sub(l.lastCleanup) >= l.ttl {
		for key, entry := range l.clients {
			if now.Sub(entry.lastSeen) >= l.ttl {
				delete(l.clients, key)
			}
		}
		l.lastCleanup = now
	}

	entry, ok := l.clients[clientKey]
	if !ok {
		entry = &clientRateLimitEntry{
			limiter: rate.NewLimiter(l.limit, l.burst),
		}
		l.clients[clientKey] = entry
	}
	entry.lastSeen = now
	limiter := entry.limiter
	l.mu.Unlock()

	return limiter.Allow()
}

// --- СТРУКТУРА ЗАСТОСУНКУ (DEPENDENCY INJECTION) ---

type App struct {
	db            *sql.DB
	bot           *tgbotapi.BotAPI
	priceCache    *PriceCache
	kyivLoc       *time.Location
	httpClient    *http.Client
	webhookSecret string
	cronSecret    string
	producerMu    sync.Mutex
	producerWG    sync.WaitGroup
	shuttingDown  bool
}

var trackedCoins = []struct {
	Symbol string
	Label  string
}{
	{"BTCUSDT", "BTC"},
	{"ETHUSDT", "ETH"},
	{"SOLUSDT", "SOL"},
	{"BNBUSDT", "BNB"},
	{"USDTUAH", "USDT"},
}

const cronBatchLimit = 100
const telegramUpdateWorkerCount = 20
const telegramUpdatePollInterval = 2 * time.Second
const telegramUpdateClaimWindow = 45 * time.Second
const telegramUpdateMaxAttempts = 3
const telegramUpdateProcessedRetention = 7 * 24 * time.Hour
const telegramUpdateFailedRetention = 30 * 24 * time.Hour

// Lease покриває одну Telegram-доставку: 10s HTTP timeout + 5s DB persist timeout із запасом.
const notificationJobClaimWindow = 45 * time.Second
const notificationJobPollInterval = 2 * time.Second
const notificationJobMaxAttempts = 3

// Після вичерпання transient retries робимо паузу, щоб не створювати новий failed job кожну хвилину.
const notificationFailureCooldown = 15 * time.Minute
const notificationRetentionCleanupInterval = time.Hour
const notificationSentRetention = 30 * 24 * time.Hour
const notificationFailedRetention = 90 * 24 * time.Hour
const notificationRetentionCleanupLimit = 1000
const cronAdvisoryLockKey int64 = 0x63726f6e6c6f636b
const telegramChatAdvisoryLockPrefix = "cryptopulse:telegram-chat:"

var errJobOwnershipLost = errors.New("job ownership lost")

var (
	cronRunsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "cryptopulse_cron_runs_total",
			Help: "Total number of cron endpoint executions by result status.",
		},
		[]string{"status"},
	)
	cronClaimedSubscribersTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "cryptopulse_cron_claimed_subscribers_total",
			Help: "Total number of subscribers claimed by cron batches.",
		},
	)
	cronDeliveriesTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "cryptopulse_cron_deliveries_total",
			Help: "Total number of scheduled Telegram delivery attempts by result status.",
		},
		[]string{"status"},
	)
	webhookUpdatesTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "cryptopulse_webhook_updates_total",
			Help: "Total number of Telegram webhook updates by result status.",
		},
		[]string{"status"},
	)
	telegramSendErrorsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "cryptopulse_telegram_send_errors_total",
			Help: "Total number of Telegram send/edit errors by type.",
		},
		[]string{"type"},
	)
	binanceRequestsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "cryptopulse_binance_requests_total",
			Help: "Total number of Binance ticker requests by symbol and result status.",
		},
		[]string{"symbol", "status"},
	)
	dbOperationsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "cryptopulse_db_operations_total",
			Help: "Total number of database operations by operation name and result status.",
		},
		[]string{"operation", "status"},
	)
)

var allowedLanguages = map[string]bool{
	"ua": true,
	"en": true,
	"ru": true,
}

// --- СЛОВНИК ПЕРЕКЛАДІВ ---
var messages = map[string]map[string]string{
	"ua": {
		"welcome":         "Вітаю! 🖖 Твій крипто-асистент уже на зв’язку! ⚡️\n\n🔹 Live-курси: BTC, ETH, SOL, BNB, USDT.\n🔹 Smart-сповіщення: Обирай частоту (1 хв – 24 год).\n🔹 UAH-маркет: Курс USDT до гривні.\n\nТисни **/subscribe** для старту!",
		"subscribe":       "✅ Підписка активована! Частота: 1 год. Змінити: /interval",
		"subscribe_first": "⚠️ Спочатку активуйте підписку: /subscribe",
		"unsubscribe":     "❌ Ви відписалися від розсилки. Налаштування мови збережено.",
		"price_hdr":       "💰 *Актуальні курси:*",
		"interval_m":      "⚙️ *Оберіть частоту повідомлень:*",
		"interval_set":    "✅ Тепер я буду надсилати курс кожні %d %s.",
		"lang_sel":        "🌍 *Оберіть мову:*",
		"lang_fixed":      "✅ Мову змінено на Українську!",
		"updated":         "🕒 *Оновлено о %s (Київ)*",
		"alert_hdr":       "🕒 *Планове оновлення (%s)*",
		"dynamics":        "Динаміка цін за останні 15с",
		"unit_m":          "хв",
		"unit_h":          "год",
		"btn_upd":         "🔄 Оновити",
		"db_err":          "❌ Виникла технічна помилка при збереженні даних. Будь ласка, спробуйте пізніше.",
		"no_data":         "немає даних",
	},
	"en": {
		"welcome":         "Welcome! 🖖 Your crypto assistant is online! ⚡️\n\n🔹 Live rates: BTC, ETH, SOL, BNB, USDT.\n🔹 Smart alerts: Frequency (1 min – 24h).\n🔹 UAH market: USDT to UAH rate.\n\nPress **/subscribe** to start!",
		"subscribe":       "✅ Subscription activated! Frequency: 1h. Change: /interval",
		"subscribe_first": "⚠️ Please subscribe first: /subscribe",
		"unsubscribe":     "❌ You have unsubscribed. Language settings saved.",
		"price_hdr":       "💰 *Current rates:*",
		"interval_m":      "⚙️ *Choose alert frequency:*",
		"interval_set":    "✅ Now I will send the rates every %d %s.",
		"lang_sel":        "🌍 *Select your language:*",
		"lang_fixed":      "✅ Language changed to English!",
		"updated":         "🕒 *Updated at %s (Kyiv)*",
		"alert_hdr":       "🕒 *Scheduled update (%s)*",
		"dynamics":        "Price dynamics (last 15s)",
		"unit_m":          "min",
		"unit_h":          "h",
		"btn_upd":         "🔄 Update",
		"db_err":          "❌ A technical error occurred while saving data. Please try again later.",
		"no_data":         "no data available",
	},
	"ru": {
		"welcome":         "Привет! 🖖 Твой крипто-ассистент уже на связи! ⚡️\n\n🔹 Live-курсы: BTC, ETH, SOL, BNB, USDT.\n🔹 Smart-уведомления: Частота (1 мин – 24 ч).\n🔹 UAH-маркет: Курс USDT к грывне.\n\nЖми **/subscribe** для старта!",
		"subscribe":       "✅ Подписка активирована! Частота: 1 ч. Изменить: /interval",
		"subscribe_first": "⚠️ Сначала активируйте подписку: /subscribe",
		"unsubscribe":     "❌ Вы отписались от рассылки. Настройки языка сохранены.",
		"price_hdr":       "💰 *Актуальные курсы:*",
		"interval_m":      "⚙️ *Выберите частоту уведомлений:*",
		"interval_set":    "✅ Теперь я буду присылать курс каждые %d %s.",
		"lang_sel":        "🌍 *Выберите язык:*",
		"lang_fixed":      "✅ Язык изменен на Русский!",
		"updated":         "🕒 *Обновлено в %s (Киев)*",
		"alert_hdr":       "🕒 *Плановое обновление (%s)*",
		"dynamics":        "Динамика цен за последние 15с",
		"unit_m":          "мин",
		"unit_h":          "ч",
		"btn_upd":         "🔄 Update",
		"db_err":          "❌ Произошла техническая ошибка при сохранении данных. Пожалуйста, попробуйте позже.",
		"no_data":         "нет данных",
	},
}

func getMsgText(lang, key string) string {
	if m, ok := messages[lang]; ok {
		if text, exist := m[key]; exist && text != "" {
			return text
		}
	}
	if text, exist := messages["ua"][key]; exist && text != "" {
		return text
	}
	return "⚠️ [Missing Translation]"
}

// --- БЕЗПЕЧНІ ОБГОРТКИ ДЛЯ TELEGRAM ---

func (a *App) sendSafeMessage(chatID int64, text string, markup interface{}) {
	msg := tgbotapi.NewMessage(chatID, text)
	msg.ParseMode = "Markdown"
	if markup != nil {
		msg.ReplyMarkup = markup
	}
	if _, err := a.bot.Send(msg); err != nil {
		telegramSendErrorsTotal.WithLabelValues("interactive_message").Inc()
		slog.Error("failed to send message", "chat_id", chatID, "error", err)
	}
}

func (a *App) editSafeMessage(
	chatID int64,
	messageID int,
	text string,
	markup *tgbotapi.InlineKeyboardMarkup,
) {
	edit := tgbotapi.NewEditMessageText(chatID, messageID, text)
	edit.ParseMode = "Markdown"
	if markup != nil {
		edit.ReplyMarkup = markup
	}
	if _, err := a.bot.Send(edit); err != nil {
		telegramSendErrorsTotal.WithLabelValues("interactive_edit").Inc()
		slog.Error(
			"failed to edit message",
			"message_id",
			messageID,
			"chat_id",
			chatID,
			"error",
			err,
		)
	}
}

// Підтверджує callback без тексту, щоб Telegram закрив індикатор завантаження без спливного повідомлення.
func (a *App) acknowledgeCallback(callbackID string) {
	_, _ = a.bot.Request(tgbotapi.NewCallback(callbackID, ""))
}

func getRefreshKeyboard(lang string) *tgbotapi.InlineKeyboardMarkup {
	text := getMsgText(lang, "btn_upd")
	kb := tgbotapi.NewInlineKeyboardMarkup(
		tgbotapi.NewInlineKeyboardRow(tgbotapi.NewInlineKeyboardButtonData(text, "refresh_price")),
	)
	return &kb
}

func getIntervalKeyboard(lang string) tgbotapi.InlineKeyboardMarkup {
	m := getMsgText(lang, "unit_m")
	h := getMsgText(lang, "unit_h")
	return tgbotapi.NewInlineKeyboardMarkup(
		tgbotapi.NewInlineKeyboardRow(
			tgbotapi.NewInlineKeyboardButtonData("1 "+m, "int_1"),
			tgbotapi.NewInlineKeyboardButtonData("5 "+m, "int_5"),
			tgbotapi.NewInlineKeyboardButtonData("10 "+m, "int_10"),
		),
		tgbotapi.NewInlineKeyboardRow(
			tgbotapi.NewInlineKeyboardButtonData("15 "+m, "int_15"),
			tgbotapi.NewInlineKeyboardButtonData("30 "+m, "int_30"),
			tgbotapi.NewInlineKeyboardButtonData("1 "+h, "int_60"),
		),
		tgbotapi.NewInlineKeyboardRow(
			tgbotapi.NewInlineKeyboardButtonData("3 "+h, "int_180"),
			tgbotapi.NewInlineKeyboardButtonData("6 "+h, "int_360"),
			tgbotapi.NewInlineKeyboardButtonData("12 "+h, "int_720"),
		),
	)
}

var langKeyboard = tgbotapi.NewInlineKeyboardMarkup(
	tgbotapi.NewInlineKeyboardRow(
		tgbotapi.NewInlineKeyboardButtonData("🇺🇦 UA", "setlang_ua"),
		tgbotapi.NewInlineKeyboardButtonData("🇺🇸 EN", "setlang_en"),
		tgbotapi.NewInlineKeyboardButtonData("🇷🇺 RU", "setlang_ru"),
	),
)

// --- ФОНОВЕ ОПИТУВАННЯ BINANCE API ---

func (a *App) startPriceTicker(ctx context.Context) {
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()

	slog.Info("background crypto price ticker service started")
	a.fetchAndCachePrices(ctx)

	for {
		select {
		case <-ticker.C:
			a.fetchAndCachePrices(ctx)
		case <-ctx.Done():
			slog.Info("background price ticker successfully stopped")
			return
		}
	}
}

func (a *App) startNotificationRetentionCleaner(ctx context.Context) {
	ticker := time.NewTicker(notificationRetentionCleanupInterval)
	defer ticker.Stop()

	slog.Info("notification job retention cleaner started")
	a.runNotificationRetentionCleanup(ctx)

	for {
		select {
		case <-ticker.C:
			a.runNotificationRetentionCleanup(ctx)
		case <-ctx.Done():
			slog.Info("notification job retention cleaner stopped")
			return
		}
	}
}

func (a *App) runNotificationRetentionCleanup(ctx context.Context) {
	deletedJobs, err := a.cleanupNotificationJobHistory(ctx)
	if err != nil {
		slog.Error("failed to clean notification job history", "error", err)
		return
	}

	deletedUpdates, err := a.cleanupTelegramUpdateHistory(ctx)
	if err != nil {
		slog.Error("failed to clean telegram update inbox history", "error", err)
		return
	}

	if deletedJobs > 0 || deletedUpdates > 0 {
		slog.Info("delivery history cleaned", "notification_jobs", deletedJobs, "telegram_updates", deletedUpdates)
	}
}

func (a *App) cleanupNotificationJobHistory(ctx context.Context) (int64, error) {
	dbCtx, dbCancel := context.WithTimeout(ctx, 10*time.Second)
	defer dbCancel()

	result, err := a.db.ExecContext(
		dbCtx,
		`WITH expired_jobs AS (
			SELECT id
			FROM notification_jobs
			WHERE (
				status = 'sent'
				AND sent_at IS NOT NULL
				AND sent_at < NOW() - $1::interval
			) OR (
				status = 'failed'
				AND failed_at IS NOT NULL
				AND failed_at < NOW() - $2::interval
			)
			ORDER BY COALESCE(sent_at, failed_at) ASC, id ASC
			LIMIT $3
		)
		DELETE FROM notification_jobs AS nj
		USING expired_jobs
		WHERE nj.id = expired_jobs.id`,
		postgresIntervalString(notificationSentRetention),
		postgresIntervalString(notificationFailedRetention),
		notificationRetentionCleanupLimit,
	)
	if err != nil {
		dbOperationsTotal.WithLabelValues("notification_retention_cleanup", "error").Inc()
		return 0, err
	}

	deleted, err := result.RowsAffected()
	if err != nil {
		dbOperationsTotal.WithLabelValues("notification_retention_cleanup", "error").Inc()
		return 0, err
	}

	dbOperationsTotal.WithLabelValues("notification_retention_cleanup", "success").Inc()
	return deleted, nil
}

func (a *App) cleanupTelegramUpdateHistory(ctx context.Context) (int64, error) {
	dbCtx, dbCancel := context.WithTimeout(ctx, 10*time.Second)
	defer dbCancel()

	result, err := a.db.ExecContext(
		dbCtx,
		`WITH expired_updates AS (
			SELECT update_id
			FROM telegram_updates
			WHERE (
				status = 'processed'
				AND processed_at IS NOT NULL
				AND processed_at < NOW() - $1::interval
			) OR (
				status = 'failed'
				AND failed_at IS NOT NULL
				AND failed_at < NOW() - $2::interval
			)
			ORDER BY COALESCE(processed_at, failed_at) ASC, update_id ASC
			LIMIT $3
		)
		DELETE FROM telegram_updates AS tu
		USING expired_updates
		WHERE tu.update_id = expired_updates.update_id`,
		postgresIntervalString(telegramUpdateProcessedRetention),
		postgresIntervalString(telegramUpdateFailedRetention),
		notificationRetentionCleanupLimit,
	)
	if err != nil {
		dbOperationsTotal.WithLabelValues("telegram_update_retention_cleanup", "error").Inc()
		return 0, err
	}

	deleted, err := result.RowsAffected()
	if err != nil {
		dbOperationsTotal.WithLabelValues("telegram_update_retention_cleanup", "error").Inc()
		return 0, err
	}

	dbOperationsTotal.WithLabelValues("telegram_update_retention_cleanup", "success").Inc()
	return deleted, nil
}

func (a *App) fetchAndCachePrices(ctx context.Context) {
	var wg sync.WaitGroup
	for _, coin := range trackedCoins {
		wg.Add(1)
		go func(c struct{ Symbol, Label string }) {
			defer wg.Done()

			url := fmt.Sprintf("https://api.binance.com/api/v3/ticker/price?symbol=%s", c.Symbol)
			req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
			if err != nil {
				binanceRequestsTotal.WithLabelValues(c.Symbol, "request_create_error").Inc()
				slog.Error("failed to create ticker request", "symbol", c.Symbol, "error", err)
				return
			}

			resp, err := a.httpClient.Do(req)
			if err != nil {
				binanceRequestsTotal.WithLabelValues(c.Symbol, "request_error").Inc()
				slog.Error("binance standard fetch failed", "symbol", c.Symbol, "error", err)
				return
			}
			defer resp.Body.Close()

			if resp.StatusCode != http.StatusOK {
				binanceRequestsTotal.WithLabelValues(c.Symbol, "bad_status").Inc()
				slog.Error(
					"binance gateway returned non-200 status code",
					"symbol",
					c.Symbol,
					"status",
					resp.StatusCode,
				)
				return
			}

			var data struct {
				Price string `json:"price"`
			}
			limitedBody := io.LimitReader(resp.Body, 102400)
			if err := json.NewDecoder(limitedBody).Decode(&data); err != nil {
				binanceRequestsTotal.WithLabelValues(c.Symbol, "decode_error").Inc()
				slog.Error(
					"failed to decode binance ticker payload",
					"symbol",
					c.Symbol,
					"error",
					err,
				)
				return
			}

			price, err := strconv.ParseFloat(data.Price, 64)
			if err != nil {
				binanceRequestsTotal.WithLabelValues(c.Symbol, "parse_error").Inc()
				slog.Error(
					"failed to parse standard float rate value",
					"symbol",
					c.Symbol,
					"error",
					err,
				)
				return
			}

			a.priceCache.Store(c.Symbol, price)

			dbCtx, dbCancel := context.WithTimeout(ctx, 2*time.Second)
			_, err = a.db.ExecContext(
				dbCtx,
				`INSERT INTO market_prices (symbol, price) VALUES ($1, $2)
				 ON CONFLICT (symbol) DO UPDATE SET price = EXCLUDED.price, updated_at = NOW()`,
				c.Symbol,
				price,
			)
			dbCancel()

			if err != nil {
				dbOperationsTotal.WithLabelValues("price_upsert", "error").Inc()
				slog.Error(
					"failed to persist fetched price",
					"symbol",
					c.Symbol,
					"error",
					err,
				)
				return
			}

			dbOperationsTotal.WithLabelValues("price_upsert", "success").Inc()
			binanceRequestsTotal.WithLabelValues(c.Symbol, "success").Inc()
		}(coin)
	}
	wg.Wait()
}

func (a *App) getFormattedPricesFromCache(lang string) string {
	results := make([]string, len(trackedCoins))
	for idx, coin := range trackedCoins {
		entry, ok := a.priceCache.Load(coin.Symbol)
		if !ok {
			results[idx] = fmt.Sprintf("⚪️ %s: %s", coin.Label, getMsgText(lang, "no_data"))
			continue
		}

		emoji := "⚪️"
		percentChange := 0.0

		if entry.Previous > 0 {
			percentChange = ((entry.Current - entry.Previous) / entry.Previous) * 100
		}

		if percentChange > 0.001 {
			emoji = "🟢"
		} else if percentChange < -0.001 {
			emoji = "🔴"
		}

		var trendStr string
		if percentChange > 0 {
			trendStr = fmt.Sprintf("+%.2f%%", percentChange)
		} else {
			trendStr = fmt.Sprintf("%.2f%%", percentChange)
		}

		if coin.Symbol == "USDTUAH" {
			results[idx] = fmt.Sprintf(
				"%s %s: *₴%.2f* (`%s`)",
				emoji,
				coin.Label,
				entry.Current,
				trendStr,
			)
		} else {
			results[idx] = fmt.Sprintf("%s %s: *$%.2f* (`%s`)", emoji, coin.Label, entry.Current, trendStr)
		}
	}
	return strings.Join(results, "\n")
}

func (a *App) getLang(ctx context.Context, chatID int64) string {
	var lang string
	err := a.db.QueryRowContext(ctx, "SELECT language_code FROM subscribers WHERE chat_id = $1", chatID).
		Scan(&lang)
	if err != nil {
		return "ua"
	}

	if !allowedLanguages[lang] {
		return "ua"
	}
	return lang
}

func (a *App) isSubscribed(ctx context.Context, chatID int64) (bool, error) {
	var subscribed bool
	err := a.db.QueryRowContext(ctx, "SELECT is_subscribed FROM subscribers WHERE chat_id = $1", chatID).
		Scan(&subscribed)
	if errors.Is(err, sql.ErrNoRows) {
		dbOperationsTotal.WithLabelValues("check_subscription", "not_found").Inc()
		return false, nil
	}
	if err != nil {
		dbOperationsTotal.WithLabelValues("check_subscription", "error").Inc()
		return false, err
	}

	if subscribed {
		dbOperationsTotal.WithLabelValues("check_subscription", "active").Inc()
	} else {
		dbOperationsTotal.WithLabelValues("check_subscription", "inactive").Inc()
	}

	return subscribed, nil
}

// --- ПРОГРІВ КЕШУ З БД ---

func (a *App) WarmupCache(ctx context.Context) {
	pricesCtx, pricesCancel := context.WithTimeout(ctx, 5*time.Second)
	defer pricesCancel()

	rows, err := a.db.QueryContext(pricesCtx, "SELECT symbol, price FROM market_prices")
	if err != nil {
		slog.Error("failed to load price cache from database", "error", err)
		return
	}
	defer rows.Close()

	for rows.Next() {
		var s string
		var p float64
		if err := rows.Scan(&s, &p); err != nil {
			slog.Error("failed to scan price cache row", "error", err)
			continue
		}
		a.priceCache.Store(s, p)
	}

	if err := rows.Err(); err != nil {
		slog.Error("failed while iterating price cache rows", "error", err)
	}

	slog.Info(
		"price cache warmup completed",
		"prices",
		len(trackedCoins),
	)
}

// --- ДОВГОТРИВАЛИЙ ПУЛ ВОРКЕРІВ ДЛЯ CRON І TELEGRAM-ОНОВЛЕНЬ ---

func (a *App) alertWorker(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()

	pollTicker := time.NewTicker(notificationJobPollInterval)
	defer pollTicker.Stop()

	for {
		job, err := a.claimPendingNotificationJob(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}

			slog.Error("failed to claim pending notification job", "error", err)
			select {
			case <-pollTicker.C:
				continue
			case <-ctx.Done():
				return
			}
		}

		if job != nil {
			a.processNotificationJob(*job)
			continue
		}

		select {
		case <-pollTicker.C:
		case <-ctx.Done():
			return
		}
	}
}

func postgresIntervalString(d time.Duration) string {
	if d <= 0 {
		return "0 seconds"
	}
	return fmt.Sprintf("%d seconds", int(d.Seconds()))
}

func truncateErrorText(text string, limit int) string {
	if limit <= 0 || len(text) <= limit {
		return text
	}
	return text[:limit]
}

func retryDelayForAttempt(attempts int) time.Duration {
	if attempts < 1 {
		attempts = 1
	}

	delay := time.Duration(attempts) * time.Minute
	if delay > 10*time.Minute {
		delay = 10 * time.Minute
	}
	return delay
}

func ensureCurrentJobClaimUpdated(result sql.Result, operation string) error {
	affectedRows, err := result.RowsAffected()
	if err != nil {
		dbOperationsTotal.WithLabelValues(operation, "error").Inc()
		return err
	}
	if affectedRows == 0 {
		dbOperationsTotal.WithLabelValues(operation, "stale_claim").Inc()
		return errJobOwnershipLost
	}
	return nil
}

func (a *App) createCronNotificationJobs(ctx context.Context) (int, error) {
	dbCtx, dbCancel := context.WithTimeout(ctx, 5*time.Second)
	defer dbCancel()

	tx, err := a.db.BeginTx(dbCtx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		dbOperationsTotal.WithLabelValues("create_notification_jobs", "error").Inc()
		return 0, err
	}
	defer func() {
		_ = tx.Rollback()
	}()

	rows, err := tx.QueryContext(dbCtx, `WITH claim_clock AS (
		SELECT NOW() AS claimed_at
	), due AS (
		SELECT s.chat_id,
		       COALESCE(s.language_code, 'ua') AS language_code,
		       claim_clock.claimed_at
		FROM subscribers AS s
		CROSS JOIN claim_clock
		WHERE s.is_subscribed = TRUE
		AND date_trunc('minute', COALESCE(s.last_sent, TIMESTAMPTZ 'epoch')) <= date_trunc('minute', claim_clock.claimed_at) - (COALESCE(s.interval_minutes, 60) * INTERVAL '1 minute')
		AND (s.cron_claimed_until IS NULL OR s.cron_claimed_until < claim_clock.claimed_at)
		AND (s.delivery_suspended_until IS NULL OR s.delivery_suspended_until <= claim_clock.claimed_at)
		AND NOT EXISTS (
			SELECT 1
			FROM notification_jobs AS nj
			WHERE nj.chat_id = s.chat_id
			AND nj.status IN ('pending', 'sending')
		)
		ORDER BY s.last_sent ASC NULLS FIRST
		LIMIT $1
		FOR UPDATE OF s SKIP LOCKED
	), claimed AS (
		UPDATE subscribers AS s
		SET cron_claimed_until = due.claimed_at + INTERVAL '15 minute'
		FROM due
		WHERE s.chat_id = due.chat_id
		RETURNING s.chat_id, due.language_code, due.claimed_at
	)
	SELECT chat_id, language_code, claimed_at FROM claimed`, cronBatchLimit)
	if err != nil {
		dbOperationsTotal.WithLabelValues("create_notification_jobs", "error").Inc()
		return 0, err
	}
	defer rows.Close()

	type rowData struct {
		chatID int64
		lang   string
		at     time.Time
	}

	var dueRows []rowData
	for rows.Next() {
		var r rowData
		if err := rows.Scan(&r.chatID, &r.lang, &r.at); err != nil {
			dbOperationsTotal.WithLabelValues("create_notification_jobs", "error").Inc()
			return 0, err
		}
		dueRows = append(dueRows, r)
	}
	if err := rows.Err(); err != nil {
		dbOperationsTotal.WithLabelValues("create_notification_jobs", "error").Inc()
		return 0, err
	}

	for _, due := range dueRows {
		pricesTextLocal := a.getFormattedPricesFromCache(due.lang)
		header := fmt.Sprintf(getMsgText(due.lang, "alert_hdr"), due.at.Format("15:04"))
		text := fmt.Sprintf(
			"%s\n\n%s\n\n_%s_",
			header,
			pricesTextLocal,
			getMsgText(due.lang, "dynamics"),
		)

		if _, err := tx.ExecContext(
			dbCtx,
			`INSERT INTO notification_jobs (
				chat_id,
				language_code,
				message_text,
				scheduled_at,
				status,
				next_attempt_at
			) VALUES ($1, $2, $3, $4, 'pending', $4)`,
			due.chatID,
			due.lang,
			text,
			due.at,
		); err != nil {
			dbOperationsTotal.WithLabelValues("create_notification_jobs", "error").Inc()
			return 0, err
		}
	}

	if err := tx.Commit(); err != nil {
		dbOperationsTotal.WithLabelValues("create_notification_jobs", "error").Inc()
		return 0, err
	}

	dbOperationsTotal.WithLabelValues("create_notification_jobs", "success").Inc()
	return len(dueRows), nil
}

func (a *App) claimPendingNotificationJob(ctx context.Context) (*NotificationJob, error) {
	dbCtx, dbCancel := context.WithTimeout(ctx, 5*time.Second)
	defer dbCancel()

	tx, err := a.db.BeginTx(dbCtx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		dbOperationsTotal.WithLabelValues("claim_notification_job", "error").Inc()
		return nil, err
	}
	defer func() {
		_ = tx.Rollback()
	}()

	var job NotificationJob
	err = tx.QueryRowContext(dbCtx, `WITH next_job AS (
		SELECT nj.id
		FROM notification_jobs AS nj
		WHERE nj.status IN ('pending', 'sending')
		AND nj.next_attempt_at <= NOW()
		AND (
			nj.status = 'pending'
			OR nj.claimed_until IS NULL
			OR nj.claimed_until < NOW()
		)
		ORDER BY nj.scheduled_at ASC, nj.id ASC
		LIMIT 1
		FOR UPDATE OF nj SKIP LOCKED
	), claimed AS (
		UPDATE notification_jobs AS nj
		SET status = 'sending',
		    attempts = nj.attempts + 1,
		    claim_token = gen_random_uuid(),
		    claimed_until = NOW() + $1::interval,
		    updated_at = NOW()
		FROM next_job
		WHERE nj.id = next_job.id
		RETURNING nj.id, nj.chat_id, nj.language_code, nj.message_text, nj.claim_token::text, nj.scheduled_at, nj.attempts
	)
	SELECT id, chat_id, language_code, message_text, claim_token, scheduled_at, attempts FROM claimed`, postgresIntervalString(notificationJobClaimWindow)).Scan(
		&job.ID,
		&job.ChatID,
		&job.Lang,
		&job.Text,
		&job.ClaimToken,
		&job.ScheduledAt,
		&job.Attempts,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		dbOperationsTotal.WithLabelValues("claim_notification_job", "error").Inc()
		return nil, err
	}

	if err := tx.Commit(); err != nil {
		dbOperationsTotal.WithLabelValues("claim_notification_job", "error").Inc()
		return nil, err
	}

	dbOperationsTotal.WithLabelValues("claim_notification_job", "success").Inc()
	return &job, nil
}

func (a *App) processNotificationJob(job NotificationJob) {
	msg := tgbotapi.NewMessage(job.ChatID, job.Text)
	msg.ParseMode = "Markdown"
	msg.ReplyMarkup = getRefreshKeyboard(job.Lang)

	if _, err := a.bot.Send(msg); err != nil {
		errorType := "transient"
		slog.Error("failed to send scheduled alert", "chat_id", job.ChatID, "error", err)

		if isPermanentTelegramSendError(err) {
			errorType = "permanent"
			if markErr := a.markNotificationJobFailed(job, err, true); markErr != nil {
				if errors.Is(markErr, errJobOwnershipLost) {
					slog.Warn("ignored stale notification failure result", "job_id", job.ID, "attempts", job.Attempts)
				} else {
					slog.Error("failed to persist permanent notification failure", "chat_id", job.ChatID, "error", markErr)
				}
			}
		} else if job.Attempts >= notificationJobMaxAttempts {
			errorType = "exhausted"
			if markErr := a.markNotificationJobFailed(job, err, false); markErr != nil {
				if errors.Is(markErr, errJobOwnershipLost) {
					slog.Warn("ignored stale notification failure result", "job_id", job.ID, "attempts", job.Attempts)
				} else {
					slog.Error("failed to persist exhausted notification failure", "chat_id", job.ChatID, "error", markErr)
				}
			}
		} else {
			if markErr := a.markNotificationJobRetry(job, err); markErr != nil {
				if errors.Is(markErr, errJobOwnershipLost) {
					slog.Warn("ignored stale notification retry result", "job_id", job.ID, "attempts", job.Attempts)
				} else {
					slog.Error("failed to persist notification retry", "chat_id", job.ChatID, "error", markErr)
				}
			}
		}

		telegramSendErrorsTotal.WithLabelValues(errorType).Inc()
		cronDeliveriesTotal.WithLabelValues("failed_" + errorType).Inc()
		return
	}

	if err := a.markNotificationJobSent(job); err != nil {
		if errors.Is(err, errJobOwnershipLost) {
			slog.Warn("ignored stale notification success result", "job_id", job.ID, "attempts", job.Attempts)
			cronDeliveriesTotal.WithLabelValues("sent_stale_claim").Inc()
			return
		}

		slog.Error("failed to persist successful notification delivery", "chat_id", job.ChatID, "error", err)
		cronDeliveriesTotal.WithLabelValues("sent_persist_error").Inc()
		return
	}

	cronDeliveriesTotal.WithLabelValues("sent").Inc()
}

func (a *App) markNotificationJobSent(job NotificationJob) error {
	dbCtx, dbCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer dbCancel()

	tx, err := a.db.BeginTx(dbCtx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		dbOperationsTotal.WithLabelValues("mark_notification_sent", "error").Inc()
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()

	result, err := tx.ExecContext(
		dbCtx,
		`UPDATE notification_jobs
		 SET status = 'sent',
		     sent_at = NOW(),
		     claim_token = NULL,
		     claimed_until = NULL,
		     last_error = NULL,
		     updated_at = NOW()
		 WHERE id = $1
		 AND status = 'sending'
		 AND claim_token = $2::uuid
		 AND attempts = $3`,
		job.ID,
		job.ClaimToken,
		job.Attempts,
	)
	if err != nil {
		dbOperationsTotal.WithLabelValues("mark_notification_sent", "error").Inc()
		return err
	}
	if err := ensureCurrentJobClaimUpdated(result, "mark_notification_sent"); err != nil {
		return err
	}

	if _, err := tx.ExecContext(
		dbCtx,
		`UPDATE subscribers
		 SET last_sent = date_trunc('minute', $2::timestamptz),
		     cron_claimed_until = NULL,
		     delivery_suspended_until = NULL
		 WHERE chat_id = $1`,
		job.ChatID,
		job.ScheduledAt,
	); err != nil {
		dbOperationsTotal.WithLabelValues("mark_notification_sent", "error").Inc()
		return err
	}

	if err := tx.Commit(); err != nil {
		dbOperationsTotal.WithLabelValues("mark_notification_sent", "error").Inc()
		return err
	}

	dbOperationsTotal.WithLabelValues("mark_notification_sent", "success").Inc()
	return nil
}

func (a *App) markNotificationJobRetry(job NotificationJob, sendErr error) error {
	dbCtx, dbCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer dbCancel()

	tx, err := a.db.BeginTx(dbCtx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		dbOperationsTotal.WithLabelValues("mark_notification_retry", "error").Inc()
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()

	delay := retryDelayForAttempt(job.Attempts)
	result, err := tx.ExecContext(
		dbCtx,
		`UPDATE notification_jobs
		 SET status = 'pending',
		     claim_token = NULL,
		     claimed_until = NULL,
		     next_attempt_at = NOW() + $2::interval,
		     last_error = $3,
		     updated_at = NOW()
		 WHERE id = $1
		 AND status = 'sending'
		 AND claim_token = $4::uuid
		 AND attempts = $5`,
		job.ID,
		postgresIntervalString(delay),
		truncateErrorText(sendErr.Error(), 500),
		job.ClaimToken,
		job.Attempts,
	)
	if err != nil {
		dbOperationsTotal.WithLabelValues("mark_notification_retry", "error").Inc()
		return err
	}
	if err := ensureCurrentJobClaimUpdated(result, "mark_notification_retry"); err != nil {
		return err
	}

	// Retry захищає сам outbox job, тому subscriber claim можна звільнити
	// і не блокувати майбутні cron cycles довше, ніж потрібно.
	if _, err := tx.ExecContext(
		dbCtx,
		`UPDATE subscribers
		 SET cron_claimed_until = NULL
		 WHERE chat_id = $1`,
		job.ChatID,
	); err != nil {
		dbOperationsTotal.WithLabelValues("mark_notification_retry", "error").Inc()
		return err
	}

	if err := tx.Commit(); err != nil {
		dbOperationsTotal.WithLabelValues("mark_notification_retry", "error").Inc()
		return err
	}

	dbOperationsTotal.WithLabelValues("mark_notification_retry", "success").Inc()
	return nil
}

func (a *App) markNotificationJobFailed(job NotificationJob, sendErr error, permanent bool) error {
	dbCtx, dbCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer dbCancel()

	tx, err := a.db.BeginTx(dbCtx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		dbOperationsTotal.WithLabelValues("mark_notification_failed", "error").Inc()
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()

	result, err := tx.ExecContext(
		dbCtx,
		`UPDATE notification_jobs
		 SET status = 'failed',
		     failed_at = NOW(),
		     claim_token = NULL,
		     claimed_until = NULL,
		     last_error = $2,
		     updated_at = NOW()
		 WHERE id = $1
		 AND status = 'sending'
		 AND claim_token = $3::uuid
		 AND attempts = $4`,
		job.ID,
		truncateErrorText(sendErr.Error(), 500),
		job.ClaimToken,
		job.Attempts,
	)
	if err != nil {
		dbOperationsTotal.WithLabelValues("mark_notification_failed", "error").Inc()
		return err
	}
	if err := ensureCurrentJobClaimUpdated(result, "mark_notification_failed"); err != nil {
		return err
	}

	if permanent {
		if _, err := tx.ExecContext(
			dbCtx,
			`UPDATE subscribers
			 SET is_subscribed = FALSE,
			     cron_claimed_until = NULL,
			     delivery_suspended_until = NULL
			 WHERE chat_id = $1`,
			job.ChatID,
		); err != nil {
			dbOperationsTotal.WithLabelValues("mark_notification_failed", "error").Inc()
			return err
		}
	} else {
		if _, err := tx.ExecContext(
			dbCtx,
			`UPDATE subscribers
			 SET cron_claimed_until = NULL,
			     delivery_suspended_until = NOW() + $2::interval
			 WHERE chat_id = $1`,
			job.ChatID,
			postgresIntervalString(notificationFailureCooldown),
		); err != nil {
			dbOperationsTotal.WithLabelValues("mark_notification_failed", "error").Inc()
			return err
		}
	}

	if err := tx.Commit(); err != nil {
		dbOperationsTotal.WithLabelValues("mark_notification_failed", "error").Inc()
		return err
	}

	dbOperationsTotal.WithLabelValues("mark_notification_failed", "success").Inc()
	return nil
}

func telegramUpdateChatID(update tgbotapi.Update) (int64, bool) {
	if update.Message != nil && update.Message.Chat != nil {
		return update.Message.Chat.ID, true
	}
	if update.CallbackQuery != nil && update.CallbackQuery.Message != nil && update.CallbackQuery.Message.Chat != nil {
		return update.CallbackQuery.Message.Chat.ID, true
	}
	if update.EditedMessage != nil && update.EditedMessage.Chat != nil {
		return update.EditedMessage.Chat.ID, true
	}
	if update.ChannelPost != nil && update.ChannelPost.Chat != nil {
		return update.ChannelPost.Chat.ID, true
	}
	if update.EditedChannelPost != nil && update.EditedChannelPost.Chat != nil {
		return update.EditedChannelPost.Chat.ID, true
	}
	return 0, false
}

func telegramShardIndex(chatID int64, shardCount int) int {
	if shardCount <= 1 {
		return 0
	}

	idx := chatID % int64(shardCount)
	if idx < 0 {
		idx += int64(shardCount)
	}
	return int(idx)
}

func (a *App) saveTelegramUpdate(ctx context.Context, update tgbotapi.Update, payload []byte) (bool, error) {
	chatID, ok := telegramUpdateChatID(update)
	if !ok {
		chatID = 0
	}
	shardID := telegramShardIndex(chatID, telegramUpdateWorkerCount)

	dbCtx, dbCancel := context.WithTimeout(ctx, 5*time.Second)
	defer dbCancel()

	result, err := a.db.ExecContext(
		dbCtx,
		`INSERT INTO telegram_updates (
			update_id,
			chat_id,
			shard_id,
			payload,
			status,
			next_attempt_at
		) VALUES ($1, $2, $3, $4::jsonb, 'pending', NOW())
		ON CONFLICT (update_id) DO NOTHING`,
		int64(update.UpdateID),
		chatID,
		shardID,
		string(payload),
	)
	if err != nil {
		dbOperationsTotal.WithLabelValues("save_telegram_update", "error").Inc()
		return false, err
	}

	inserted, err := result.RowsAffected()
	if err != nil {
		dbOperationsTotal.WithLabelValues("save_telegram_update", "error").Inc()
		return false, err
	}

	dbOperationsTotal.WithLabelValues("save_telegram_update", "success").Inc()
	return inserted > 0, nil
}

func (a *App) claimPendingTelegramUpdate(ctx context.Context, shardID int) (*TelegramUpdateJob, error) {
	dbCtx, dbCancel := context.WithTimeout(ctx, 5*time.Second)
	defer dbCancel()

	tx, err := a.db.BeginTx(dbCtx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		dbOperationsTotal.WithLabelValues("claim_telegram_update", "error").Inc()
		return nil, err
	}
	defer func() {
		_ = tx.Rollback()
	}()

	var job TelegramUpdateJob
	err = tx.QueryRowContext(dbCtx, `WITH next_update AS (
		SELECT tu.update_id
		FROM telegram_updates AS tu
		WHERE tu.shard_id = $1
		AND tu.status IN ('pending', 'processing')
		AND tu.next_attempt_at <= NOW()
		AND (
			tu.status = 'pending'
			OR tu.claimed_until IS NULL
			OR tu.claimed_until < NOW()
		)
		AND NOT EXISTS (
			SELECT 1
			FROM telegram_updates AS earlier
			WHERE earlier.chat_id = tu.chat_id
			AND earlier.status IN ('pending', 'processing')
			AND earlier.update_id < tu.update_id
		)
		ORDER BY tu.update_id ASC
		LIMIT 1
		FOR UPDATE OF tu SKIP LOCKED
	), claimed AS (
		UPDATE telegram_updates AS tu
		SET status = 'processing',
		    attempts = tu.attempts + 1,
		    claimed_until = NOW() + $2::interval,
		    updated_at = NOW()
		FROM next_update
		WHERE tu.update_id = next_update.update_id
		RETURNING tu.update_id, tu.chat_id, tu.payload::text, tu.attempts
	)
	SELECT update_id, chat_id, payload, attempts FROM claimed`, shardID, postgresIntervalString(telegramUpdateClaimWindow)).Scan(
		&job.UpdateID,
		&job.ChatID,
		&job.Payload,
		&job.Attempts,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		dbOperationsTotal.WithLabelValues("claim_telegram_update", "error").Inc()
		return nil, err
	}

	if err := tx.Commit(); err != nil {
		dbOperationsTotal.WithLabelValues("claim_telegram_update", "error").Inc()
		return nil, err
	}

	dbOperationsTotal.WithLabelValues("claim_telegram_update", "success").Inc()
	return &job, nil
}

func (a *App) updateWorker(ctx context.Context, wg *sync.WaitGroup, shardID int) {
	defer wg.Done()

	pollTicker := time.NewTicker(telegramUpdatePollInterval)
	defer pollTicker.Stop()

	for {
		job, err := a.claimPendingTelegramUpdate(ctx, shardID)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			slog.Error("failed to claim telegram update", "shard_id", shardID, "error", err)
			select {
			case <-pollTicker.C:
			case <-ctx.Done():
				return
			}
			continue
		}

		if job != nil {
			a.processTelegramUpdateJob(*job)
			continue
		}

		select {
		case <-pollTicker.C:
		case <-ctx.Done():
			return
		}
	}
}

func telegramChatAdvisoryLockKey(chatID int64) int64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(telegramChatAdvisoryLockPrefix))
	_, _ = h.Write([]byte(strconv.FormatInt(chatID, 10)))
	return int64(h.Sum64())
}

func (a *App) acquireTelegramChatAdvisoryLock(ctx context.Context, chatID int64) (*sql.Conn, int64, error) {
	conn, err := a.db.Conn(ctx)
	if err != nil {
		dbOperationsTotal.WithLabelValues("acquire_telegram_chat_lock", "error").Inc()
		return nil, 0, err
	}

	lockKey := telegramChatAdvisoryLockKey(chatID)
	if _, err := conn.ExecContext(ctx, `SELECT pg_advisory_lock($1)`, lockKey); err != nil {
		_ = conn.Close()
		dbOperationsTotal.WithLabelValues("acquire_telegram_chat_lock", "error").Inc()
		return nil, lockKey, err
	}

	dbOperationsTotal.WithLabelValues("acquire_telegram_chat_lock", "success").Inc()
	return conn, lockKey, nil
}

func releaseTelegramChatAdvisoryLock(conn *sql.Conn, lockKey int64) {
	if conn == nil {
		return
	}

	releaseCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if _, err := conn.ExecContext(releaseCtx, `SELECT pg_advisory_unlock($1)`, lockKey); err != nil {
		slog.Error("failed to release telegram chat advisory lock", "error", err)
	}
	if err := conn.Close(); err != nil {
		slog.Error("failed to close telegram chat advisory lock connection", "error", err)
	}
}

func (a *App) processTelegramUpdateJob(job TelegramUpdateJob) {
	var update tgbotapi.Update
	if err := json.Unmarshal([]byte(job.Payload), &update); err != nil {
		if markErr := a.markTelegramUpdateFailed(job, err); markErr != nil {
			slog.Error("failed to persist invalid telegram update", "update_id", job.UpdateID, "error", markErr)
		}
		webhookUpdatesTotal.WithLabelValues("failed_invalid_payload").Inc()
		return
	}

	lockCtx, lockCancel := context.WithTimeout(context.Background(), 10*time.Second)
	lockConn, lockKey, err := a.acquireTelegramChatAdvisoryLock(lockCtx, job.ChatID)
	lockCancel()
	if err != nil {
		a.markTelegramUpdateProcessingError(job, fmt.Errorf("acquire telegram chat lock: %w", err))
		return
	}
	defer releaseTelegramChatAdvisoryLock(lockConn, lockKey)

	var processingErr error
	func() {
		defer func() {
			if recovered := recover(); recovered != nil {
				processingErr = fmt.Errorf("telegram update processing panic: %v", recovered)
			}
		}()

		processCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := a.processTelegramUpdate(processCtx, update); err != nil {
			processingErr = err
		}
	}()

	if processingErr != nil {
		a.markTelegramUpdateProcessingError(job, processingErr)
		return
	}

	if err := a.markTelegramUpdateProcessed(job); err != nil {
		if errors.Is(err, errJobOwnershipLost) {
			slog.Warn("ignored stale telegram update success result", "update_id", job.UpdateID, "attempts", job.Attempts)
			webhookUpdatesTotal.WithLabelValues("processed_stale_claim").Inc()
			return
		}

		slog.Error("failed to persist processed telegram update", "update_id", job.UpdateID, "error", err)
		webhookUpdatesTotal.WithLabelValues("processed_persist_error").Inc()
		return
	}

	webhookUpdatesTotal.WithLabelValues("processed").Inc()
}

func (a *App) markTelegramUpdateProcessingError(job TelegramUpdateJob, processingErr error) {
	if job.Attempts >= telegramUpdateMaxAttempts {
		if markErr := a.markTelegramUpdateFailed(job, processingErr); markErr != nil {
			if errors.Is(markErr, errJobOwnershipLost) {
				slog.Warn("ignored stale telegram update failure result", "update_id", job.UpdateID, "attempts", job.Attempts)
			} else {
				slog.Error("failed to persist telegram update failure", "update_id", job.UpdateID, "error", markErr)
			}
		}
		webhookUpdatesTotal.WithLabelValues("failed_exhausted").Inc()
		return
	}

	if markErr := a.markTelegramUpdateRetry(job, processingErr); markErr != nil {
		if errors.Is(markErr, errJobOwnershipLost) {
			slog.Warn("ignored stale telegram update retry result", "update_id", job.UpdateID, "attempts", job.Attempts)
		} else {
			slog.Error("failed to persist telegram update retry", "update_id", job.UpdateID, "error", markErr)
		}
	}
	webhookUpdatesTotal.WithLabelValues("retry").Inc()
}

func (a *App) markTelegramUpdateProcessed(job TelegramUpdateJob) error {
	dbCtx, dbCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer dbCancel()

	result, err := a.db.ExecContext(
		dbCtx,
		`UPDATE telegram_updates
		 SET status = 'processed',
		     processed_at = NOW(),
		     claimed_until = NULL,
		     last_error = NULL,
		     updated_at = NOW()
		 WHERE update_id = $1
		 AND status = 'processing'
		 AND attempts = $2`,
		job.UpdateID,
		job.Attempts,
	)
	if err != nil {
		dbOperationsTotal.WithLabelValues("mark_telegram_update_processed", "error").Inc()
		return err
	}
	if err := ensureCurrentJobClaimUpdated(result, "mark_telegram_update_processed"); err != nil {
		return err
	}

	dbOperationsTotal.WithLabelValues("mark_telegram_update_processed", "success").Inc()
	return nil
}

func (a *App) markTelegramUpdateRetry(job TelegramUpdateJob, processingErr error) error {
	dbCtx, dbCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer dbCancel()

	delay := retryDelayForAttempt(job.Attempts)
	result, err := a.db.ExecContext(
		dbCtx,
		`UPDATE telegram_updates
		 SET status = 'pending',
		     claimed_until = NULL,
		     next_attempt_at = NOW() + $2::interval,
		     last_error = $3,
		     updated_at = NOW()
		 WHERE update_id = $1
		 AND status = 'processing'
		 AND attempts = $4`,
		job.UpdateID,
		postgresIntervalString(delay),
		truncateErrorText(processingErr.Error(), 500),
		job.Attempts,
	)
	if err != nil {
		dbOperationsTotal.WithLabelValues("mark_telegram_update_retry", "error").Inc()
		return err
	}
	if err := ensureCurrentJobClaimUpdated(result, "mark_telegram_update_retry"); err != nil {
		return err
	}

	dbOperationsTotal.WithLabelValues("mark_telegram_update_retry", "success").Inc()
	return nil
}

func (a *App) markTelegramUpdateFailed(job TelegramUpdateJob, processingErr error) error {
	dbCtx, dbCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer dbCancel()

	result, err := a.db.ExecContext(
		dbCtx,
		`UPDATE telegram_updates
		 SET status = 'failed',
		     failed_at = NOW(),
		     claimed_until = NULL,
		     last_error = $2,
		     updated_at = NOW()
		 WHERE update_id = $1
		 AND status = 'processing'
		 AND attempts = $3`,
		job.UpdateID,
		truncateErrorText(processingErr.Error(), 500),
		job.Attempts,
	)
	if err != nil {
		dbOperationsTotal.WithLabelValues("mark_telegram_update_failed", "error").Inc()
		return err
	}
	if err := ensureCurrentJobClaimUpdated(result, "mark_telegram_update_failed"); err != nil {
		return err
	}

	dbOperationsTotal.WithLabelValues("mark_telegram_update_failed", "success").Inc()
	return nil
}

// --- HTTP-ОБРОБНИКИ ТА MIDDLEWARE ---

func methodMiddleware(method string, next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != method {
			w.Header().Set("Allow", method)
			w.WriteHeader(http.StatusMethodNotAllowed)
			_, _ = w.Write([]byte("405 Method Not Allowed"))
			return
		}
		next(w, r)
	}
}
func (a *App) rateLimitMiddleware(limiter *rate.Limiter, next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !limiter.Allow() {
			slog.Warn(
				"global rate limit exceeded, dropping request",
				"endpoint",
				r.URL.Path,
				"remote_ip",
				r.RemoteAddr,
			)
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = w.Write([]byte("Too Many Requests"))
			return
		}
		next(w, r)
	}
}

func requestClientKey(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err == nil && host != "" {
		return host
	}
	if r.RemoteAddr != "" {
		return r.RemoteAddr
	}
	return "unknown"
}

func (a *App) clientRateLimitMiddleware(limiter *ClientRateLimiter, next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		clientKey := requestClientKey(r)
		if !limiter.Allow(clientKey) {
			slog.Warn(
				"client rate limit exceeded, dropping request",
				"endpoint",
				r.URL.Path,
				"remote_ip",
				clientKey,
			)
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = w.Write([]byte("Too Many Requests"))
			return
		}
		next(w, r)
	}
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

func (a *App) acquireCronAdvisoryLock(ctx context.Context) (*sql.Conn, bool, error) {
	conn, err := a.db.Conn(ctx)
	if err != nil {
		return nil, false, err
	}

	var acquired bool
	if err := conn.QueryRowContext(ctx, `SELECT pg_try_advisory_lock($1)`, cronAdvisoryLockKey).Scan(&acquired); err != nil {
		_ = conn.Close()
		return nil, false, err
	}

	if !acquired {
		_ = conn.Close()
		return nil, false, nil
	}

	return conn, true, nil
}

func releaseCronAdvisoryLock(conn *sql.Conn) {
	if conn == nil {
		return
	}

	releaseCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if _, err := conn.ExecContext(releaseCtx, `SELECT pg_advisory_unlock($1)`, cronAdvisoryLockKey); err != nil {
		slog.Error("failed to release cron advisory lock", "error", err)
	}
	if err := conn.Close(); err != nil {
		slog.Error("failed to close cron advisory lock connection", "error", err)
	}
}

func isPermanentTelegramSendError(err error) bool {
	// Постійні Telegram-помилки означають, що користувач недоступний і його треба відписати від cron-розсилки.
	var tgErr *tgbotapi.Error
	if errors.As(err, &tgErr) {
		msg := strings.ToLower(tgErr.Message)

		if tgErr.Code == http.StatusForbidden {
			return true
		}

		if tgErr.Code == http.StatusBadRequest {
			return strings.Contains(msg, "chat not found") ||
				strings.Contains(msg, "user is deactivated")
		}

		return false
	}

	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "chat not found") ||
		strings.Contains(msg, "bot was blocked") ||
		strings.Contains(msg, "user is deactivated") ||
		strings.Contains(msg, "bot can't initiate conversation")
}

func (a *App) markSubscriberUnsubscribed(chatID int64) {
	dbCtx, dbCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer dbCancel()

	if _, err := a.db.ExecContext(
		dbCtx,
		"UPDATE subscribers SET is_subscribed = FALSE, cron_claimed_until = NULL, delivery_suspended_until = NULL WHERE chat_id = $1",
		chatID,
	); err != nil {
		dbOperationsTotal.WithLabelValues("mark_unsubscribed", "error").Inc()
		slog.Error("failed to mark subscriber as unsubscribed", "chat_id", chatID, "error", err)
		return
	}

	dbOperationsTotal.WithLabelValues("mark_unsubscribed", "success").Inc()
}

func (a *App) handleCron(w http.ResponseWriter, r *http.Request) {
	authHeader := r.Header.Get("Authorization")
	var providedToken string
	if strings.HasPrefix(authHeader, "Bearer ") {
		providedToken = authHeader[7:]
	}

	if a.cronSecret != "" && !safeSecretCompare(providedToken, a.cronSecret) {
		cronRunsTotal.WithLabelValues("unauthorized").Inc()
		slog.Warn(
			"unauthorized block cron endpoint execution access triggered",
			"remote_ip",
			r.RemoteAddr,
		)
		w.WriteHeader(http.StatusUnauthorized)
		return
	}

	lockConn, acquired, err := a.acquireCronAdvisoryLock(r.Context())
	if err != nil {
		cronRunsTotal.WithLabelValues("lock_error").Inc()
		slog.Error("failed to acquire cron advisory lock", "error", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	if !acquired {
		cronRunsTotal.WithLabelValues("conflict").Inc()
		slog.Warn(
			"prevented overlapping cron job execution across replicas, request discarded",
			"remote_ip",
			r.RemoteAddr,
		)
		w.WriteHeader(http.StatusConflict)
		_, _ = w.Write([]byte("Cron execution already in progress"))
		return
	}
	defer releaseCronAdvisoryLock(lockConn)

	slog.Info("valid cron trigger received, creating durable notification jobs")
	ctx := r.Context()

	createdJobs, err := a.createCronNotificationJobs(ctx)
	if err != nil {
		cronRunsTotal.WithLabelValues("create_jobs_error").Inc()
		slog.Error("failed to create durable notification jobs", "error", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	if createdJobs == 0 {
		cronRunsTotal.WithLabelValues("no_jobs").Inc()
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("No notification jobs created"))
		return
	}

	cronClaimedSubscribersTotal.Add(float64(createdJobs))
	cronRunsTotal.WithLabelValues("accepted").Inc()
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

		if !safeSecretCompare(providedToken, a.cronSecret) {
			slog.Warn("unauthorized metrics endpoint access blocked", "remote_ip", r.RemoteAddr)
			w.WriteHeader(http.StatusUnauthorized)
			return
		}

		next(w, r)
	}
}

func (a *App) handleWebhook(w http.ResponseWriter, r *http.Request) {
	providedSecret := r.Header.Get("X-Telegram-Bot-Api-Secret-Token")
	if a.webhookSecret != "" && !safeSecretCompare(providedSecret, a.webhookSecret) {
		webhookUpdatesTotal.WithLabelValues("unauthorized").Inc()
		slog.Warn("unauthorized webhook attempt blocked", "remote_ip", r.RemoteAddr)
		w.WriteHeader(http.StatusUnauthorized)
		return
	}

	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)

	payload, err := io.ReadAll(r.Body)
	if err != nil {
		webhookUpdatesTotal.WithLabelValues("bad_request").Inc()
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	var update tgbotapi.Update
	if err := json.Unmarshal(payload, &update); err != nil {
		webhookUpdatesTotal.WithLabelValues("bad_request").Inc()
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	if update.UpdateID == 0 {
		webhookUpdatesTotal.WithLabelValues("bad_request").Inc()
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	inserted, err := a.saveTelegramUpdate(r.Context(), update, payload)
	if err != nil {
		webhookUpdatesTotal.WithLabelValues("persist_error").Inc()
		slog.Error("failed to persist telegram update", "update_id", update.UpdateID, "error", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	if inserted {
		webhookUpdatesTotal.WithLabelValues("accepted").Inc()
	} else {
		webhookUpdatesTotal.WithLabelValues("duplicate").Inc()
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("OK"))
}

func (a *App) processTelegramUpdate(ctx context.Context, update tgbotapi.Update) (processErr error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	if update.CallbackQuery != nil {
		if update.CallbackQuery.Message == nil {
			slog.Warn(
				"received inline callback query without message context object",
				"callback_id",
				update.CallbackQuery.ID,
			)
			return
		}

		data := update.CallbackQuery.Data
		chatID := update.CallbackQuery.Message.Chat.ID
		callbackID := update.CallbackQuery.ID

		if strings.HasPrefix(data, "setlang_") {
			newLang := data[8:]

			if !allowedLanguages[newLang] {
				slog.Error(
					"invalid language selection",
					"chat_id",
					chatID,
					"payload",
					newLang,
				)
				_, _ = a.bot.Request(tgbotapi.NewCallback(callbackID, "Invalid Language"))
				return
			}

			if _, err := a.db.ExecContext(ctx, `INSERT INTO subscribers (chat_id, interval_minutes, last_sent, language_code, is_subscribed)
                     VALUES ($1, 60, NOW() - INTERVAL '2 minute', $2, FALSE)
                     ON CONFLICT (chat_id) DO UPDATE SET language_code = EXCLUDED.language_code`, chatID, newLang); err != nil {
				dbOperationsTotal.WithLabelValues("set_language", "error").Inc()
				slog.Error(
					"failed to save language settings",
					"chat_id",
					chatID,
					"error",
					err,
				)
				_, _ = a.bot.Request(tgbotapi.NewCallback(callbackID, "Error"))
				a.sendSafeMessage(chatID, getMsgText("ua", "db_err"), nil)
				return err
			}

			dbOperationsTotal.WithLabelValues("set_language", "success").Inc()
			a.acknowledgeCallback(callbackID)
			a.sendSafeMessage(chatID, getMsgText(newLang, "lang_fixed"), nil)
			return
		}

		lang := a.getLang(ctx, chatID)

		if strings.HasPrefix(data, "int_") {
			minutes, err := strconv.Atoi(data[4:])
			if err != nil || minutes < 1 || minutes > 1440 {
				// Передаємо створену групу як атрибут у метод Warn
				slog.Warn("callback data validation failed",
					slog.Group("security_alert",
						slog.String("reason", "malicious_callback_range_violation"),
						slog.Int64("chat_id", chatID),
						slog.String("payload", data),
					),
				)
				_, _ = a.bot.Request(tgbotapi.NewCallback(callbackID, "Invalid Range"))
				return
			}

			result, err := a.db.ExecContext(ctx, "UPDATE subscribers SET interval_minutes = $1, last_sent = NOW() WHERE chat_id = $2 AND is_subscribed = TRUE", minutes, chatID)
			if err != nil {
				dbOperationsTotal.WithLabelValues("update_interval", "error").Inc()
				slog.Error(
					"failed to update notification frequency interval",
					"chat_id",
					chatID,
					"error",
					err,
				)
				_, _ = a.bot.Request(tgbotapi.NewCallback(callbackID, "Error"))
				a.sendSafeMessage(chatID, getMsgText(lang, "db_err"), nil)
				return err
			}

			affectedRows, err := result.RowsAffected()
			if err != nil {
				dbOperationsTotal.WithLabelValues("update_interval", "error").Inc()
				slog.Error(
					"failed to inspect interval update result",
					"chat_id",
					chatID,
					"error",
					err,
				)
				_, _ = a.bot.Request(tgbotapi.NewCallback(callbackID, "Error"))
				a.sendSafeMessage(chatID, getMsgText(lang, "db_err"), nil)
				return err
			}

			if affectedRows == 0 {
				dbOperationsTotal.WithLabelValues("update_interval", "inactive").Inc()
				slog.Info("interval update rejected for inactive subscriber", "chat_id", chatID)
				_, _ = a.bot.Request(tgbotapi.NewCallback(callbackID, getMsgText(lang, "subscribe_first")))
				a.sendSafeMessage(chatID, getMsgText(lang, "subscribe_first"), nil)
				return
			}

			dbOperationsTotal.WithLabelValues("update_interval", "success").Inc()
			unit := getMsgText(lang, "unit_m")
			val := minutes
			if minutes >= 60 {
				unit = getMsgText(lang, "unit_h")
				val = minutes / 60
			}
			a.acknowledgeCallback(callbackID)
			a.sendSafeMessage(chatID, fmt.Sprintf(getMsgText(lang, "interval_set"), val, unit), nil)
			return
		}

		if data == "refresh_price" {
			prices := a.getFormattedPricesFromCache(lang)
			t := time.Now().In(a.kyivLoc).Format("15:04:05")
			text := fmt.Sprintf(
				getMsgText(lang, "updated")+"\n\n%s\n\n_%s_",
				t,
				prices,
				getMsgText(lang, "dynamics"),
			)

			a.editSafeMessage(
				chatID,
				update.CallbackQuery.Message.MessageID,
				text,
				getRefreshKeyboard(lang),
			)
			a.acknowledgeCallback(callbackID)
		}
		return
	}

	if update.Message == nil {
		return
	}
	chatID := update.Message.Chat.ID
	lang := a.getLang(ctx, chatID)

	cmd := update.Message.Command()
	if strings.Contains(cmd, "@") {
		cmd = strings.Split(cmd, "@")[0]
	}

	switch cmd {
	case "start":
		a.sendSafeMessage(chatID, getMsgText(lang, "welcome"), nil)
	case "language":
		a.sendSafeMessage(chatID, getMsgText(lang, "lang_sel"), langKeyboard)
	case "subscribe":
		if _, err := a.db.ExecContext(ctx, `INSERT INTO subscribers (chat_id, interval_minutes, last_sent, language_code, is_subscribed)
                 VALUES ($1, 60, NOW() - INTERVAL '2 minute', $2, TRUE)
                 ON CONFLICT (chat_id) DO UPDATE SET
                     interval_minutes = COALESCE(subscribers.interval_minutes, EXCLUDED.interval_minutes),
                     last_sent = COALESCE(subscribers.last_sent, EXCLUDED.last_sent),
                     language_code = EXCLUDED.language_code,
                     is_subscribed = TRUE,
                     delivery_suspended_until = NULL`, chatID, lang); err != nil {
			dbOperationsTotal.WithLabelValues("subscribe", "error").Inc()
			slog.Error("subscriber activation failed", "chat_id", chatID, "error", err)
			a.sendSafeMessage(chatID, getMsgText(lang, "db_err"), nil)
			return err
		}
		dbOperationsTotal.WithLabelValues("subscribe", "success").Inc()
		a.sendSafeMessage(chatID, getMsgText(lang, "subscribe"), nil)
	case "unsubscribe":
		if _, err := a.db.ExecContext(ctx, "UPDATE subscribers SET is_subscribed = FALSE, cron_claimed_until = NULL, delivery_suspended_until = NULL WHERE chat_id = $1", chatID); err != nil {
			dbOperationsTotal.WithLabelValues("unsubscribe", "error").Inc()
			slog.Error("deactivation sql command failed", "chat_id", chatID, "error", err)
			a.sendSafeMessage(chatID, getMsgText(lang, "db_err"), nil)
			return err
		}
		dbOperationsTotal.WithLabelValues("unsubscribe", "success").Inc()
		a.sendSafeMessage(chatID, getMsgText(lang, "unsubscribe"), nil)
	case "interval":
		subscribed, err := a.isSubscribed(ctx, chatID)
		if err != nil {
			slog.Error("failed to check subscription status before interval menu", "chat_id", chatID, "error", err)
			a.sendSafeMessage(chatID, getMsgText(lang, "db_err"), nil)
			return err
		}
		if !subscribed {
			a.sendSafeMessage(chatID, getMsgText(lang, "subscribe_first"), nil)
			return
		}

		a.sendSafeMessage(chatID, getMsgText(lang, "interval_m"), getIntervalKeyboard(lang))
	case "price":
		prices := a.getFormattedPricesFromCache(lang)
		text := fmt.Sprintf(getMsgText(lang, "price_hdr")+"\n\n%s", prices)
		a.sendSafeMessage(chatID, text, getRefreshKeyboard(lang))
	}

	return nil
}

func runHealthcheck() {
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	client := &http.Client{Timeout: 2 * time.Second}
	resp, err := client.Get("http://127.0.0.1:" + port + "/live")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		fmt.Fprintf(os.Stderr, "unexpected healthcheck status: %d\n", resp.StatusCode)
		os.Exit(1)
	}
}
func main() {
	if len(os.Args) > 1 && os.Args[1] == "healthcheck" {
		runHealthcheck()
		return
	}

	_ = godotenv.Load()

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	kyivLocation, tzErr := time.LoadLocation("Europe/Kyiv")
	if tzErr != nil {
		slog.Error(
			"failed to parse Europe/Kyiv timezone, safety fallback activated",
			"error",
			tzErr,
		)
		kyivLocation = time.FixedZone("Kyiv", 3*60*60)
	}

	customHTTPClient := &http.Client{
		Timeout: 5 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        100,
			MaxIdleConnsPerHost: 30,
			IdleConnTimeout:     90 * time.Second,
		},
	}

	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		slog.Error("configuration error: DATABASE_URL missing")
		os.Exit(1)
	}

	rawDB, err := sql.Open("pgx", databaseURL)
	if err != nil {
		slog.Error("failed to initialize connection pool", "error", err)
		os.Exit(1)
	}
	rawDB.SetMaxOpenConns(25)
	rawDB.SetMaxIdleConns(25)
	rawDB.SetConnMaxLifetime(5 * time.Minute)
	rawDB.SetConnMaxIdleTime(5 * time.Minute)

	pingCtx, pingCancel := context.WithTimeout(context.Background(), 5*time.Second)
	if err := rawDB.PingContext(pingCtx); err != nil {
		pingCancel()
		slog.Error("database unreachable", "error", err)
		os.Exit(1)
	}
	pingCancel()

	isolatedBotToken := os.Getenv("TELEGRAM_APITOKEN")
	if isolatedBotToken == "" {
		slog.Error("configuration error: TELEGRAM_APITOKEN missing")
		os.Exit(1)
	}

	telegramHTTPClient := &http.Client{Timeout: 10 * time.Second}
	tgBot, err := tgbotapi.NewBotAPIWithClient(isolatedBotToken, tgbotapi.APIEndpoint, telegramHTTPClient)
	if err != nil {
		slog.Error("failed to initialize bot API", "error", err)
		os.Exit(1)
	}

	webhookSecretToken := os.Getenv("WEBHOOK_SECRET_TOKEN")
	if webhookSecretToken == "" {
		slog.Error("configuration error: WEBHOOK_SECRET_TOKEN missing")
		os.Exit(1)
	}

	cronSecretToken := os.Getenv("CRON_SECRET")
	if cronSecretToken == "" {
		slog.Error("configuration error: CRON_SECRET missing")
		os.Exit(1)
	}

	app := &App{
		db:            rawDB,
		bot:           tgBot,
		priceCache:    &PriceCache{store: make(map[string]PriceEntry)},
		kyivLoc:       kyivLocation,
		httpClient:    customHTTPClient,
		webhookSecret: webhookSecretToken,
		cronSecret:    cronSecretToken,
	}

	runCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	workerCtx, stopWorkers := context.WithCancel(context.Background())
	defer stopWorkers()

	app.WarmupCache(runCtx)

	go app.startPriceTicker(runCtx)
	go app.startNotificationRetentionCleaner(runCtx)

	var telegramWG sync.WaitGroup
	for shardID := 0; shardID < telegramUpdateWorkerCount; shardID++ {
		telegramWG.Add(1)
		go app.updateWorker(workerCtx, &telegramWG, shardID)
	}

	var cronWG sync.WaitGroup
	for i := 1; i <= 5; i++ {
		cronWG.Add(1)
		go app.alertWorker(workerCtx, &cronWG)
	}

	cronLimiter := rate.NewLimiter(rate.Every(30*time.Second), 5)
	webhookLimiter := newClientRateLimiter(rate.Limit(50), 100, 10*time.Minute)

	mux := http.NewServeMux()
	mux.HandleFunc("/live", methodMiddleware(http.MethodGet, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("OK"))
	}))
	mux.HandleFunc("/ready", methodMiddleware(http.MethodGet, func(w http.ResponseWriter, r *http.Request) {
		dbCheckCtx, dbCheckCancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer dbCheckCancel()

		if err := app.db.PingContext(dbCheckCtx); err != nil {
			dbOperationsTotal.WithLabelValues("readiness_ping", "error").Inc()
			slog.Error("readiness check failed: database down", "error", err)
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte("503 Service Unavailable: Database Unreachable"))
			return
		}

		dbOperationsTotal.WithLabelValues("readiness_ping", "success").Inc()
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("Ready"))
	}))
	mux.HandleFunc("/metrics", methodMiddleware(http.MethodGet, app.metricsAuthMiddleware(promhttp.Handler().ServeHTTP)))
	mux.HandleFunc("/cron", methodMiddleware(http.MethodPost, app.producerMiddleware(app.rateLimitMiddleware(cronLimiter, app.handleCron))))
	mux.HandleFunc("/webhook", methodMiddleware(http.MethodPost, app.producerMiddleware(app.clientRateLimitMiddleware(webhookLimiter, app.handleWebhook))))

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	srv := &http.Server{
		Addr:              ":" + port,
		Handler:           mux,
		ReadTimeout:       5 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       120 * time.Second,
		ReadHeaderTimeout: 2 * time.Second,
	}

	go func() {
		slog.Info("HTTP server started", "port", port)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("server termination error", "error", err)
			stop()
		}
	}()

	<-runCtx.Done()
	slog.Info("shutdown signal intercepted")

	// Спочатку забороняємо нові producer-и, потім drain-имо handler-и.
	// Durable inbox/outbox у PostgreSQL повторно підхопить незавершені jobs після lease timeout.
	app.stopAcceptingProducers()

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		slog.Error("server shutdown error", "error", err)
		if closeErr := srv.Close(); closeErr != nil {
			slog.Error("server forced close error", "error", closeErr)
		}
	}

	producerCtx, producerCancel := context.WithTimeout(context.Background(), 15*time.Second)
	producersDrained := true
	if err := app.waitForProducers(producerCtx); err != nil {
		producersDrained = false
		slog.Error("producer drain timeout before worker stop", "error", err)
	}
	producerCancel()

	if !producersDrained {
		slog.Warn("stopping workers while some producers may still be finishing")
	}

	stopWorkers()
	telegramWG.Wait()
	cronWG.Wait()
	slog.Info("background worker pools stopped")

	rawDB.Close()
	slog.Info("database connections closed. process exited successfully.")
}
