package main

import (
	"context"
	"crypto/subtle"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"log/slog"

	_ "github.com/jackc/pgx/v5/stdlib"

	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/joho/godotenv"
	"golang.org/x/time/rate"
)


type Subscriber struct {
	ID   int64
	Lang string
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

type SafeIDCollector struct {
	mu  sync.Mutex
	ids []int64
}

func (c *SafeIDCollector) Add(id int64) {
	c.mu.Lock()
	c.ids = append(c.ids, id)
	c.mu.Unlock()
}

func (c *SafeIDCollector) FlushIDs() []int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.ids) == 0 {
		return nil
	}
	result := make([]int64, len(c.ids))
	copy(result, c.ids)
	c.ids = nil
	return result
}

type Job struct {
	ChatID    int64
	Lang      string
	Text      string
	Collector *SafeIDCollector
	DoneFunc  func()
}


type App struct {
	db                 *sql.DB
	bot                *tgbotapi.BotAPI
	priceCache         *PriceCache
	langCache          *lru.Cache[int64, string]
	telegramUpdateChan chan tgbotapi.Update
	cronJobChan        chan Job
	kyivLoc            *time.Location
	httpClient         *http.Client
	webhookSecret      string
	cronSecret         string
	isCronRunning      atomic.Bool
	producerMu         sync.Mutex
	producerWG         sync.WaitGroup
	shuttingDown       bool
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

var allowedLanguages = map[string]bool{
	"ua": true,
	"en": true,
	"ru": true,
}

var messages = map[string]map[string]string{
	"ua": {
		"welcome":      "Вітаю! 🖖 Твій крипто-асистент уже на зв’язку! ⚡️\n\n🔹 Live-курси: BTC, ETH, SOL, BNB, USDT.\n🔹 Smart-сповіщення: Обирай частоту (1 хв – 24 год).\n🔹 UAH-маркет: Курс USDT до гривні.\n\nТисни **/subscribe** для старту!",
		"subscribe":    "✅ Підписка активована! Частота: 1 год. Змінити: /interval",
		"unsubscribe":  "❌ Ви відписалися від розсилки. Налаштування мови збережено.",
		"price_hdr":    "💰 *Актуальні курси:*",
		"interval_m":   "⚙️ *Оберіть частоту повідомлень:*",
		"interval_set": "✅ Тепер я буду надсилати курс кожні %d %s.",
		"lang_sel":     "🌍 *Оберіть мову:*",
		"lang_fixed":   "✅ Мову змінено на Українську!",
		"updated":      "🕒 *Оновлено о %s (Київ)*",
		"alert_hdr":    "🕒 *Планове оновлення (%s)*",
		"dynamics":     "Динаміка цін за останні 15с",
		"unit_m":       "хв",
		"unit_h":       "год",
		"btn_upd":      "🔄 Оновити",
		"db_err":       "❌ Виникла технічна помилка при збереженні даних. Будь ласка, спробуйте пізніше.",
		"no_data":      "немає даних",
	},
	"en": {
		"welcome":      "Welcome! 🖖 Your crypto assistant is online! ⚡️\n\n🔹 Live rates: BTC, ETH, SOL, BNB, USDT.\n🔹 Smart alerts: Frequency (1 min – 24h).\n🔹 UAH market: USDT to UAH rate.\n\nPress **/subscribe** to start!",
		"subscribe":    "✅ Subscription activated! Frequency: 1h. Change: /interval",
		"unsubscribe":  "❌ You have unsubscribed. Language settings saved.",
		"price_hdr":    "💰 *Current rates:*",
		"interval_m":   "⚙️ *Choose alert frequency:*",
		"interval_set": "✅ Now I will send the rates every %d %s.",
		"lang_sel":     "🌍 *Select your language:*",
		"lang_fixed":   "✅ Language changed to English!",
		"updated":      "🕒 *Updated at %s (Kyiv)*",
		"alert_hdr":    "🕒 *Scheduled update (%s)*",
		"dynamics":     "Price dynamics (last 15s)",
		"unit_m":       "min",
		"unit_h":       "h",
		"btn_upd":      "🔄 Update",
		"db_err":       "❌ A technical error occurred while saving data. Please try again later.",
		"no_data":      "no data available",
	},
	"ru": {
		"welcome":      "Привет! 🖖 Твой крипто-ассистент уже на связи! ⚡️\n\n🔹 Live-курсы: BTC, ETH, SOL, BNB, USDT.\n🔹 Smart-уведомления: Частота (1 мин – 24 ч).\n🔹 UAH-маркет: Курс USDT к грывне.\n\nЖми **/subscribe** для старта!",
		"subscribe":    "✅ Подписка активирована! Частота: 1 ч. Изменить: /interval",
		"unsubscribe":  "❌ Вы отписались от рассылки. Настройки языка сохранены.",
		"price_hdr":    "💰 *Актуальные курсы:*",
		"interval_m":   "⚙️ *Выберите частоту уведомлений:*",
		"interval_set": "✅ Теперь я буду присылать курс каждые %d %s.",
		"lang_sel":     "🌍 *Выберите язык:*",
		"lang_fixed":   "✅ Язык изменен на Русский!",
		"updated":      "🕒 *Обновлено в %s (Киев)*",
		"alert_hdr":    "🕒 *Плановое обновление (%s)*",
		"dynamics":     "Динамика цен за последние 15с",
		"unit_m":       "мин",
		"unit_h":       "ч",
		"btn_upd":      "🔄 Update",
		"db_err":       "❌ Произошла техническая ошибка при сохранении данных. Пожалуйста, попробуйте позже.",
		"no_data":      "нет данных",
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


func (a *App) sendSafeMessage(chatID int64, text string, markup interface{}) {
	msg := tgbotapi.NewMessage(chatID, text)
	msg.ParseMode = "Markdown"
	if markup != nil {
		msg.ReplyMarkup = markup
	}
	if _, err := a.bot.Send(msg); err != nil {
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

func (a *App) fetchAndCachePrices(ctx context.Context) {
	var wg sync.WaitGroup
	for _, coin := range trackedCoins {
		wg.Add(1)
		go func(c struct{ Symbol, Label string }) {
			defer wg.Done()

			url := fmt.Sprintf("https://api.binance.com/api/v3/ticker/price?symbol=%s", c.Symbol)
			req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
			if err != nil {
				slog.Error("failed to create ticker request", "symbol", c.Symbol, "error", err)
				return
			}

			resp, err := a.httpClient.Do(req)
			if err != nil {
				slog.Error("binance standard fetch failed", "symbol", c.Symbol, "error", err)
				return
			}
			defer resp.Body.Close()

			if resp.StatusCode != http.StatusOK {
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
				slog.Error(
					"failed to sync newly fetched price values to data nodes",
					"symbol",
					c.Symbol,
					"error",
					err,
				)
			}
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
	if lang, ok := a.langCache.Get(chatID); ok {
		return lang
	}

	var lang string
	err := a.db.QueryRowContext(ctx, "SELECT language_code FROM subscribers WHERE chat_id = $1", chatID).
		Scan(&lang)
	if err != nil {
		return "ua"
	}

	a.langCache.Add(chatID, lang)
	return lang
}


func (a *App) WarmupCache(ctx context.Context) {
	pricesCtx, pricesCancel := context.WithTimeout(ctx, 5*time.Second)
	defer pricesCancel()

	rows, err := a.db.QueryContext(pricesCtx, "SELECT symbol, price FROM market_prices")
	if err != nil {
		slog.Error("warmup query engine pricing schema error", "error", err)
		return
	}
	defer rows.Close()

	for rows.Next() {
		var s string
		var p float64
		if err := rows.Scan(&s, &p); err != nil {
			slog.Error("failed to scan warmup row chunk data", "error", err)
			continue
		}
		a.priceCache.Store(s, p)
	}

	if err := rows.Err(); err != nil {
		slog.Error("warmup rows compilation iteration error", "error", err)
	}

	langsCtx, langsCancel := context.WithTimeout(ctx, 5*time.Second)
	defer langsCancel()

	subRows, err := a.db.QueryContext(langsCtx, "SELECT chat_id, language_code FROM subscribers")
	if err != nil {
		slog.Error("failed to execute dynamic warmup subscription cache codes", "error", err)
		return
	}
	defer subRows.Close()

	var loadedLangs int
	for subRows.Next() {
		var id int64
		var code string
		if err := subRows.Scan(&id, &code); err == nil {
			a.langCache.Add(id, code)
			loadedLangs++
		}
	}
	slog.Info(
		"cache schema initialization completed successfully",
		"prices",
		len(trackedCoins),
		"langs",
		loadedLangs,
	)
}


func (a *App) alertWorker(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		select {
		case job, ok := <-a.cronJobChan:
			if !ok {
				return
			}

			func() {
				defer job.DoneFunc()

				msg := tgbotapi.NewMessage(job.ChatID, job.Text)
				msg.ParseMode = "Markdown"
				msg.ReplyMarkup = getRefreshKeyboard(job.Lang)

				if _, err := a.bot.Send(msg); err != nil {
					slog.Error(
						"failed to send scheduled alert telegram packet",
						"chat_id",
						job.ChatID,
						"error",
						err,
					)

					if isPermanentTelegramSendError(err) {
						a.markSubscriberUnsubscribed(job.ChatID)
					}

					return
				}

				job.Collector.Add(job.ChatID)
			}()
		case <-ctx.Done():
			return
		}
	}
}
func (a *App) updateWorker(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		select {
		case update, ok := <-a.telegramUpdateChan:
			if !ok {
				return
			}
			a.processTelegramUpdate(update)
		case <-ctx.Done():
			return
		}
	}
}


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
				"rate limit exceeded, dropping request",
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

func (a *App) producerMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
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
	return subtle.ConstantTimeCompare([]byte(inputToken), []byte(expectedSecret)) == 1
}

func isPermanentTelegramSendError(err error) bool {
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
		"UPDATE subscribers SET is_subscribed = FALSE, cron_claimed_until = NULL WHERE chat_id = $1",
		chatID,
	); err != nil {
		slog.Error("failed to mark subscriber as unsubscribed", "chat_id", chatID, "error", err)
	}
}

func subscriberIDs(subs []Subscriber) []int64 {
	ids := make([]int64, 0, len(subs))
	for _, sub := range subs {
		ids = append(ids, sub.ID)
	}
	return ids
}

func (a *App) claimDueSubscribers(ctx context.Context) ([]Subscriber, error) {
	dbCtx, dbCancel := context.WithTimeout(ctx, 5*time.Second)
	defer dbCancel()

	tx, err := a.db.BeginTx(dbCtx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	rows, err := tx.QueryContext(dbCtx, `WITH due AS (
		SELECT chat_id
		FROM subscribers
		WHERE is_subscribed = TRUE
		AND COALESCE(last_sent, TIMESTAMP 'epoch') <= NOW() - (COALESCE(interval_minutes, 60) * INTERVAL '1 minute') + INTERVAL '59 second'
		AND (cron_claimed_until IS NULL OR cron_claimed_until < NOW())
		ORDER BY last_sent ASC NULLS FIRST
		LIMIT 5000
		FOR UPDATE SKIP LOCKED
	)
	UPDATE subscribers AS s
	SET cron_claimed_until = NOW() + INTERVAL '2 minute'
	FROM due
	WHERE s.chat_id = due.chat_id
	RETURNING s.chat_id, COALESCE(s.language_code, 'ua')`)
	if err != nil {
		return nil, err
	}

	var subs []Subscriber
	for rows.Next() {
		var sub Subscriber
		if err := rows.Scan(&sub.ID, &sub.Lang); err != nil {
			_ = rows.Close()
			return nil, err
		}
		subs = append(subs, sub)
	}

	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return nil, err
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}

	return subs, nil
}

func (a *App) markCronDeliveriesSent(ids []int64) error {
	if len(ids) == 0 {
		return nil
	}

	dbCtx, dbCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer dbCancel()

	_, err := a.db.ExecContext(
		dbCtx,
		"UPDATE subscribers SET last_sent = NOW(), cron_claimed_until = NULL WHERE chat_id = ANY($1)",
		any(ids),
	)
	return err
}

func (a *App) releaseCronClaims(ids []int64) error {
	if len(ids) == 0 {
		return nil
	}

	dbCtx, dbCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer dbCancel()

	_, err := a.db.ExecContext(
		dbCtx,
		"UPDATE subscribers SET cron_claimed_until = NULL WHERE chat_id = ANY($1)",
		any(ids),
	)
	return err
}
func (a *App) handleCron(w http.ResponseWriter, r *http.Request) {
	authHeader := r.Header.Get("Authorization")
	var providedToken string
	if strings.HasPrefix(authHeader, "Bearer ") {
		providedToken = authHeader[7:]
	}

	if a.cronSecret != "" && !safeSecretCompare(providedToken, a.cronSecret) {
		slog.Warn(
			"unauthorized block cron endpoint execution access triggered",
			"remote_ip",
			r.RemoteAddr,
		)
		w.WriteHeader(http.StatusUnauthorized)
		return
	}

	if !a.isCronRunning.CompareAndSwap(false, true) {
		slog.Warn(
			"prevented overlapping cron job execution, request discarded",
			"remote_ip",
			r.RemoteAddr,
		)
		w.WriteHeader(http.StatusConflict)
		_, _ = w.Write([]byte("Cron execution already in progress"))
		return
	}
	defer a.isCronRunning.Store(false)

	slog.Info("valid cron trigger received, claiming due subscriber batch")
	ctx := r.Context()

	currentTime := time.Now().In(a.kyivLoc).Format("15:04")
	subs, err := a.claimDueSubscribers(ctx)
	if err != nil {
		slog.Error("failed to claim notification target batch", "error", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	if len(subs) == 0 {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("No subscribers to notify"))
		return
	}

	allIDs := subscriberIDs(subs)
	collector := &SafeIDCollector{ids: make([]int64, 0, len(subs))}
	var dispatchWG sync.WaitGroup

	for _, s := range subs {
		pricesTextLocal := a.getFormattedPricesFromCache(s.Lang)
		header := fmt.Sprintf(getMsgText(s.Lang, "alert_hdr"), currentTime)
		text := fmt.Sprintf(
			"%s\n\n%s\n\n_%s_",
			header,
			pricesTextLocal,
			getMsgText(s.Lang, "dynamics"),
		)

		dispatchWG.Add(1)
		job := Job{
			ChatID:    s.ID,
			Lang:      s.Lang,
			Text:      text,
			Collector: collector,
			DoneFunc:  func() { dispatchWG.Done() },
		}

		select {
		case a.cronJobChan <- job:
		case <-ctx.Done():
			dispatchWG.Done()
			slog.Warn(
				"cron worker channel submission canceled due to context expiration/shutdown",
				"chat_id",
				s.ID,
			)
			return
		}
	}

	doneChan := make(chan struct{})
	go func() {
		dispatchWG.Wait()
		close(doneChan)
	}()

	select {
	case <-doneChan:
		slog.Info("all cron dispatch batch routines reported completion tasks successfully")
	case <-time.After(12 * time.Second):
		slog.Error("cron worker tracking process hit execution pool barrier timeout limits, saving completed deliveries")

		partialIDs := collector.FlushIDs()
		if err := a.markCronDeliveriesSent(partialIDs); err != nil {
			slog.Error("failed to save partial success timestamps after cron timeout", "error", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusGatewayTimeout)
		_, _ = w.Write([]byte("Cron execution pool barrier timeout hit. Partial data saved."))
		return
	}

	successIDs := collector.FlushIDs()
	if err := a.markCronDeliveriesSent(successIDs); err != nil {
		slog.Error("failed to update timestamps for successful cron deliveries", "error", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	if err := a.releaseCronClaims(allIDs); err != nil {
		slog.Error("failed to release cron claims after dispatch completion", "error", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("Cron processing completed"))
}
func (a *App) handleWebhook(w http.ResponseWriter, r *http.Request) {
	providedSecret := r.Header.Get("X-Telegram-Bot-Api-Secret-Token")
	if a.webhookSecret != "" && !safeSecretCompare(providedSecret, a.webhookSecret) {
		slog.Warn("unauthorized webhook attempt blocked", "remote_ip", r.RemoteAddr)
		w.WriteHeader(http.StatusUnauthorized)
		return
	}

	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)

	var update tgbotapi.Update
	if err := json.NewDecoder(r.Body).Decode(&update); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	select {
	case a.telegramUpdateChan <- update:
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("OK"))
	case <-time.After(1 * time.Second):
		slog.Warn(
			"telegram incoming update channel queue full, dropping packet and backpressuring",
			"update_id",
			update.UpdateID,
		)
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte("503 Service Unavailable: Internal buffer saturated"))
	}
}

func (a *App) processTelegramUpdate(update tgbotapi.Update) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
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
					"security execution block triggered: invalid language query string",
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
				slog.Error(
					"failed to save language settings for user node",
					"chat_id",
					chatID,
					"error",
					err,
				)
				_, _ = a.bot.Request(tgbotapi.NewCallback(callbackID, "Error"))
				a.sendSafeMessage(chatID, getMsgText("ua", "db_err"), nil)
				return
			}

			a.langCache.Add(chatID, newLang)
			_, _ = a.bot.Request(tgbotapi.NewCallback(callbackID, "OK"))
			a.sendSafeMessage(chatID, getMsgText(newLang, "lang_fixed"), nil)
			return
		}

		lang := a.getLang(ctx, chatID)

		if strings.HasPrefix(data, "int_") {
			minutes, err := strconv.Atoi(data[4:])
			if err != nil || minutes < 1 || minutes > 1440 {
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

			if _, err := a.db.ExecContext(ctx, "UPDATE subscribers SET interval_minutes = $1, last_sent = NOW() WHERE chat_id = $2", minutes, chatID); err != nil {
				slog.Error(
					"failed to update notification frequency interval",
					"chat_id",
					chatID,
					"error",
					err,
				)
				_, _ = a.bot.Request(tgbotapi.NewCallback(callbackID, "Error"))
				a.sendSafeMessage(chatID, getMsgText(lang, "db_err"), nil)
				return
			}

			unit := getMsgText(lang, "unit_m")
			val := minutes
			if minutes >= 60 {
				unit = getMsgText(lang, "unit_h")
				val = minutes / 60
			}
			_, _ = a.bot.Request(tgbotapi.NewCallback(callbackID, "OK"))
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
			_, _ = a.bot.Request(tgbotapi.NewCallback(callbackID, "OK"))
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
                     is_subscribed = TRUE`, chatID, lang); err != nil {
			slog.Error("subscriber activation failed", "chat_id", chatID, "error", err)
			a.sendSafeMessage(chatID, getMsgText(lang, "db_err"), nil)
			return
		}
		a.sendSafeMessage(chatID, getMsgText(lang, "subscribe"), nil)
	case "unsubscribe":
		if _, err := a.db.ExecContext(ctx, "UPDATE subscribers SET is_subscribed = FALSE, cron_claimed_until = NULL WHERE chat_id = $1", chatID); err != nil {
			slog.Error("deactivation sql command failed", "chat_id", chatID, "error", err)
			a.sendSafeMessage(chatID, getMsgText(lang, "db_err"), nil)
			return
		}
		a.sendSafeMessage(chatID, getMsgText(lang, "unsubscribe"), nil)
	case "interval":
		a.sendSafeMessage(chatID, getMsgText(lang, "interval_m"), getIntervalKeyboard(lang))
	case "price":
		prices := a.getFormattedPricesFromCache(lang)
		text := fmt.Sprintf(getMsgText(lang, "price_hdr")+"\n\n%s", prices)
		a.sendSafeMessage(chatID, text, getRefreshKeyboard(lang))
	}
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

	rawDB, err := sql.Open("pgx", os.Getenv("DATABASE_URL"))
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

	tgBot, err := tgbotapi.NewBotAPI(isolatedBotToken)
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

	lruCacheInstance, cacheErr := lru.New[int64, string](50000)
	if cacheErr != nil {
		slog.Error("failed to initialize LRU cache store", "error", cacheErr)
		os.Exit(1)
	}

	telegramUpdateChanInstance := make(chan tgbotapi.Update, 1000)
	cronJobChanInstance := make(chan Job, 5000)

	app := &App{
		db:                 rawDB,
		bot:                tgBot,
		priceCache:         &PriceCache{store: make(map[string]PriceEntry)},
		langCache:          lruCacheInstance,
		telegramUpdateChan: telegramUpdateChanInstance,
		cronJobChan:        cronJobChanInstance,
		kyivLoc:            kyivLocation,
		httpClient:         customHTTPClient,
		webhookSecret:      webhookSecretToken,
		cronSecret:         cronSecretToken,
	}

	runCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	app.WarmupCache(runCtx)

	go app.startPriceTicker(runCtx)

	var telegramWG sync.WaitGroup
	for i := 1; i <= 20; i++ {
		telegramWG.Add(1)
		go app.updateWorker(runCtx, &telegramWG)
	}

	var cronWG sync.WaitGroup
	for i := 1; i <= 5; i++ {
		cronWG.Add(1)
		go app.alertWorker(runCtx, &cronWG)
	}

	cronLimiter := rate.NewLimiter(rate.Every(30*time.Second), 5)
	webhookLimiter := rate.NewLimiter(rate.Limit(50), 100)

	mux := http.NewServeMux()
	mux.HandleFunc("/live", methodMiddleware(http.MethodGet, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("OK"))
	}))
	mux.HandleFunc("/ready", methodMiddleware(http.MethodGet, func(w http.ResponseWriter, r *http.Request) {
		dbCheckCtx, dbCheckCancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer dbCheckCancel()

		if err := app.db.PingContext(dbCheckCtx); err != nil {
			slog.Error("readiness check failed: database down", "error", err)
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte("503 Service Unavailable: Database Unreachable"))
			return
		}

		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("Ready"))
	}))
	mux.HandleFunc("/cron", methodMiddleware(http.MethodPost, app.producerMiddleware(app.rateLimitMiddleware(cronLimiter, app.handleCron))))
	mux.HandleFunc("/webhook", methodMiddleware(http.MethodPost, app.producerMiddleware(app.rateLimitMiddleware(webhookLimiter, app.handleWebhook))))

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
		slog.Error("producer drain timeout before channel close", "error", err)
	}
	producerCancel()

	if producersDrained {
		close(app.telegramUpdateChan)
		close(app.cronJobChan)
	} else {
		slog.Warn("skipping channel close because HTTP producers are still active")
	}

	telegramWG.Wait()
	cronWG.Wait()
	slog.Info("background worker pools stopped")

	rawDB.Close()
	slog.Info("database connections closed. process exited successfully.")
}
