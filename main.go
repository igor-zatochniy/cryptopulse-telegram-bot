package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
	lru "github.com/hashicorp/golang-lru/v2"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/joho/godotenv"
)

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

type App struct {
	db         *sql.DB
	bot        *tgbotapi.BotAPI
	priceCache *PriceCache
	langCache  *lru.Cache[int64, string]
	kyivLoc    *time.Location
	httpClient *http.Client
}

var muMsg sync.RWMutex

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

type Job struct {
	ChatID int64
	Lang   string
	Text   string
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
		"dynamics":     "Динаміка зафіксована",
		"no_data":      "немає даних",
		"unit_m":       "хв",
		"unit_h":       "год",
		"btn_upd":      "🔄 Оновити",
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
		"dynamics":     "Dynamics fixed",
		"no_data":      "no data available",
		"unit_m":       "min",
		"unit_h":       "h",
		"btn_upd":      "🔄 Update",
	},
	"ru": {
		"welcome":      "Привет! 🖖 Твой крипто-ассистент уже на связи! ⚡️\n\n🔹 Live-курсы: BTC, ETH, SOL, BNB, USDT.\n🔹 Smart-уведомления: Частота (1 мин – 24 ч).\n🔹 UAH-маркет: Курс USDT к гривне.\n\nЖми **/subscribe** для старта!",
		"subscribe":    "✅ Подписка активирована! Частота: 1 ч. Изменить: /interval",
		"unsubscribe":  "❌ Вы отписались от рассылки. Настройки языка сохранены.",
		"price_hdr":    "💰 *Актуальные курсы:*",
		"interval_m":   "⚙️ *Выберите частоту уведомлений:*",
		"interval_set": "✅ Теперь я буду присылать курс каждые %d %s.",
		"lang_sel":     "🌍 *Выберите язык:*",
		"lang_fixed":   "✅ Язык изменен на Русский!",
		"updated":      "🕒 *Обновлено в %s (Киев)*",
		"alert_hdr":    "🕒 *Плановое обеспечение (%s)*",
		"dynamics":     "Динамика зафиксирована",
		"no_data":      "нет данных",
		"unit_m":       "мин",
		"unit_h":       "ч",
		"btn_upd":      "🔄 Обновить",
	},
}

func getMsgText(lang, key string) string {
	muMsg.RLock()
	defer muMsg.RUnlock()
	if m, ok := messages[lang]; ok {
		return m[key]
	}
	return messages["ua"][key]
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

	log.Println("[Prices] Background price ticker started")
	a.fetchAndCachePrices(ctx)

	for {
		select {
		case <-ticker.C:
			a.fetchAndCachePrices(ctx)
		case <-ctx.Done():
			log.Println("[Prices] Background price ticker stopped")
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
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
			if err != nil {
				log.Printf("[Prices] Failed to create request for %s: %v", c.Symbol, err)
				return
			}

			resp, err := a.httpClient.Do(req)
			if err != nil {
				log.Printf("[Prices] Binance request failed for %s: %v", c.Symbol, err)
				return
			}
			defer resp.Body.Close()

			if resp.StatusCode != http.StatusOK {
				log.Printf("[Prices] Binance returned status %d for %s", resp.StatusCode, c.Symbol)
				return
			}

			var data struct {
				Price string `json:"price"`
			}
			if err := json.NewDecoder(io.LimitReader(resp.Body, 102400)).Decode(&data); err != nil {
				log.Printf("[Prices] Failed to decode Binance response for %s: %v", c.Symbol, err)
				return
			}

			price, err := strconv.ParseFloat(data.Price, 64)
			if err != nil {
				log.Printf("[Prices] Failed to parse Binance price for %s: %v", c.Symbol, err)
				return
			}

			a.priceCache.Store(c.Symbol, price)

			dbCtx, dbCancel := context.WithTimeout(ctx, 2*time.Second)
			_, err = a.db.ExecContext(
				dbCtx,
				`INSERT INTO market_prices (symbol, price) VALUES ($1, $2)
				 ON CONFLICT (symbol) DO UPDATE SET price = EXCLUDED.price`,
				c.Symbol,
				price,
			)
			dbCancel()
			if err != nil {
				log.Printf("[Prices] Failed to persist %s: %v", c.Symbol, err)
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

		trend := fmt.Sprintf("%.2f%%", percentChange)
		if percentChange > 0 {
			trend = fmt.Sprintf("+%.2f%%", percentChange)
		}
		if coin.Symbol == "USDTUAH" {
			results[idx] = fmt.Sprintf("%s %s: *₴%.2f* (`%s`)", emoji, coin.Label, entry.Current, trend)
		} else {
			results[idx] = fmt.Sprintf("%s %s: *$%.2f* (`%s`)", emoji, coin.Label, entry.Current, trend)
		}
	}
	return strings.Join(results, "\n")
}

func (a *App) initDB(ctx context.Context) {
	var err error
	a.db, err = sql.Open("pgx", os.Getenv("DATABASE_URL"))
	if err != nil {
		log.Fatal(err)
	}
	a.db.SetMaxOpenConns(25)
	a.db.SetMaxIdleConns(25)
	a.db.SetConnMaxLifetime(5 * time.Minute)
	a.db.SetConnMaxIdleTime(5 * time.Minute)

	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := a.db.PingContext(pingCtx); err != nil {
		log.Fatal("database unreachable: ", err)
	}
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
		log.Printf("[Cache] Failed to warm up prices: %v", err)
		return
	}
	defer rows.Close()

	for rows.Next() {
		var symbol string
		var price float64
		if err := rows.Scan(&symbol, &price); err != nil {
			log.Printf("[Cache] Failed to scan price: %v", err)
			continue
		}
		a.priceCache.Store(symbol, price)
	}

	langsCtx, langsCancel := context.WithTimeout(ctx, 5*time.Second)
	defer langsCancel()
	subRows, err := a.db.QueryContext(langsCtx, "SELECT chat_id, language_code FROM subscribers")
	if err != nil {
		log.Printf("[Cache] Failed to warm up languages: %v", err)
		return
	}
	defer subRows.Close()

	for subRows.Next() {
		var chatID int64
		var lang string
		if err := subRows.Scan(&chatID, &lang); err == nil {
			a.langCache.Add(chatID, lang)
		}
	}
}

// --- ПУЛ ВОРКЕРОВ ДЛЯ КРОНА ---
func (a *App) alertWorker(baseCtx context.Context, jobs <-chan Job, wg *sync.WaitGroup) {
	defer wg.Done()
	for job := range jobs {
		msg := tgbotapi.NewMessage(job.ChatID, job.Text)
		msg.ParseMode = "Markdown"
		msg.ReplyMarkup = getRefreshKeyboard(job.Lang)

		if _, err := a.bot.Send(msg); err == nil {
			dbCtx, dbCancel := context.WithTimeout(baseCtx, 2*time.Second)
			_, dbErr := a.db.ExecContext(
				dbCtx,
				"UPDATE subscribers SET last_sent = NOW() WHERE chat_id = $1",
				job.ChatID,
			)
			if dbErr != nil {
				log.Printf("[DB Error] Не удалось обновить last_sent для %d: %v", job.ChatID, dbErr)
			}
			dbCancel()
		} else {
			log.Printf("Failed to send cron alert to %d: %v", job.ChatID, err)
		}
	}
}

func (a *App) handleCron(w http.ResponseWriter, r *http.Request) {
	log.Println("[Cron] Получен запрос от планировщика. Запуск рассылки...")
	ctx := r.Context()

	currentTime := time.Now().In(a.kyivLoc).Format("15:04")

	rows, err := a.db.QueryContext(ctx, `SELECT chat_id, language_code FROM subscribers
                               WHERE is_subscribed = TRUE
                               AND last_sent <= NOW() - (interval_minutes * INTERVAL '1 minute') + INTERVAL '5 seconds'`)
	if err != nil {
		log.Println("DB Error:", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	jobs := make(chan Job, 500)
	var workerWG sync.WaitGroup

	for i := 1; i <= 5; i++ {
		workerWG.Add(1)
		go a.alertWorker(context.Background(), jobs, &workerWG)
	}

	for rows.Next() {
		var id int64
		var lang string
		if err := rows.Scan(&id, &lang); err == nil {
			text := fmt.Sprintf(
				getMsgText(lang, "alert_hdr")+"\n\n%s\n\n_%s_",
				currentTime,
				a.getFormattedPricesFromCache(lang),
				getMsgText(lang, "dynamics"),
			)

			sendCtx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
			select {
			case jobs <- Job{ChatID: id, Lang: lang, Text: text}:
			case <-sendCtx.Done():
			}
			cancel()
		}
	}

	close(jobs)
	workerWG.Wait()

	w.WriteHeader(http.StatusOK)
	w.Write([]byte("Cron executed successfully"))
}

func (a *App) handleWebhook(w http.ResponseWriter, r *http.Request) {
	var update tgbotapi.Update
	if err := json.NewDecoder(r.Body).Decode(&update); err != nil {
		log.Printf("[Webhook Error] Ошибка декодирования JSON: %v", err)
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	w.WriteHeader(http.StatusOK)

	go a.processTelegramUpdate(update)
}

func (a *App) processTelegramUpdate(update tgbotapi.Update) {
	if update.CallbackQuery != nil {
		data := update.CallbackQuery.Data
		chatID := update.CallbackQuery.Message.Chat.ID

		if strings.HasPrefix(data, "setlang_") {
			newLang := data[8:]
			_, _ = a.db.Exec(`INSERT INTO subscribers (chat_id, language_code) VALUES ($1, $2)
                     ON CONFLICT (chat_id) DO UPDATE SET language_code = $2`, chatID, newLang)
			_, _ = a.bot.Request(tgbotapi.NewCallback(update.CallbackQuery.ID, "OK"))
			_, _ = a.bot.Send(tgbotapi.NewMessage(chatID, getMsgText(newLang, "lang_fixed")))
			return
		}

		lang := a.getLang(context.Background(), chatID)

		if strings.HasPrefix(data, "int_") {
			minutes, _ := strconv.Atoi(data[4:])
			_, _ = a.db.Exec(
				"UPDATE subscribers SET interval_minutes = $1, last_sent = NOW() WHERE chat_id = $2",
				minutes,
				chatID,
			)
			unit := getMsgText(lang, "unit_m")
			val := minutes
			if minutes >= 60 {
				unit = getMsgText(lang, "unit_h")
				val = minutes / 60
			}
			_, _ = a.bot.Request(tgbotapi.NewCallback(update.CallbackQuery.ID, "OK"))
			_, _ = a.bot.Send(
				tgbotapi.NewMessage(
					chatID,
					fmt.Sprintf(getMsgText(lang, "interval_set"), val, unit),
				),
			)
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
			edit := tgbotapi.NewEditMessageText(
				chatID,
				update.CallbackQuery.Message.MessageID,
				text,
			)
			edit.ParseMode = "Markdown"
			edit.ReplyMarkup = getRefreshKeyboard(lang)
			_, _ = a.bot.Send(edit)
			_, _ = a.bot.Request(tgbotapi.NewCallback(update.CallbackQuery.ID, "OK"))
		}
		return
	}

	if update.Message == nil {
		return
	}
	chatID := update.Message.Chat.ID
	lang := a.getLang(context.Background(), chatID)

	switch update.Message.Command() {
	case "start":
		msg := tgbotapi.NewMessage(chatID, getMsgText(lang, "welcome"))
		msg.ParseMode = "Markdown"
		_, _ = a.bot.Send(msg)
	case "language":
		msg := tgbotapi.NewMessage(chatID, getMsgText(lang, "lang_sel"))
		msg.ReplyMarkup = langKeyboard
		_, _ = a.bot.Send(msg)
	case "subscribe":
		_, _ = a.db.Exec(
			`INSERT INTO subscribers (chat_id, interval_minutes, last_sent, language_code, is_subscribed)
                 VALUES ($1, 60, NOW(), 'ua', TRUE) ON CONFLICT (chat_id) DO UPDATE SET is_subscribed = TRUE`,
			chatID,
		)
		_, _ = a.bot.Send(tgbotapi.NewMessage(chatID, getMsgText(lang, "subscribe")))
	case "unsubscribe":
		_, _ = a.db.Exec("UPDATE subscribers SET is_subscribed = FALSE WHERE chat_id = $1", chatID)
		_, _ = a.bot.Send(tgbotapi.NewMessage(chatID, getMsgText(lang, "unsubscribe")))
	case "interval":
		msg := tgbotapi.NewMessage(chatID, getMsgText(lang, "interval_m"))
		msg.ParseMode = "Markdown"
		msg.ReplyMarkup = getIntervalKeyboard(lang)
		_, _ = a.bot.Send(msg)
	case "price":
		prices := a.getFormattedPricesFromCache(lang)
		text := fmt.Sprintf(getMsgText(lang, "price_hdr")+"\n\n%s", prices)
		msg := tgbotapi.NewMessage(chatID, text)
		msg.ParseMode = "Markdown"
		msg.ReplyMarkup = getRefreshKeyboard(lang)
		_, _ = a.bot.Send(msg)
	}
}

func main() {
	_ = godotenv.Load()

	app := &App{
		kyivLoc:    time.FixedZone("Kyiv", 2*60*60),
		httpClient: &http.Client{Timeout: 4 * time.Second},
	}
	cache, err := lru.New[int64, string](50000)
	if err != nil {
		log.Fatal(err)
	}
	app.priceCache = &PriceCache{store: make(map[string]PriceEntry)}
	app.langCache = cache
	app.initDB(context.Background())
	app.WarmupCache(context.Background())
	go app.startPriceTicker(context.Background())

	botToken := os.Getenv("TELEGRAM_APITOKEN")
	if botToken == "" {
		log.Fatal("TELEGRAM_APITOKEN is not set")
	}

	tgBot, err := tgbotapi.NewBotAPI(botToken)
	if err != nil {
		log.Fatal(err)
	}
	app.bot = tgBot
	log.Printf("[Success] Бот авторизован под именем: %s", app.bot.Self.UserName)

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("Bot server is running perfectly!"))
	})

	// Роут для крона
	http.HandleFunc("/cron", app.handleCron)

	http.HandleFunc("/webhook/"+botToken, app.handleWebhook)

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	log.Printf("[Info] HTTP Сервер запущен на порту %s", port)
	if err := http.ListenAndServe(":"+port, nil); err != nil {
		log.Fatalf("[Critical] Ошибка запуска сервера: %v", err)
	}
}
