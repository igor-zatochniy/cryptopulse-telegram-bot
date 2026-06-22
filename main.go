package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
	"github.com/joho/godotenv"
	_ "github.com/lib/pq"
)

var (
	db       *sql.DB
	kyivLoc  = time.FixedZone("Kyiv", 2*60*60)
	bot      *tgbotapi.BotAPI
	muMsg    sync.RWMutex
	botToken string
)

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

func getAllPricesFormatted(ctx context.Context) string {
	var wg sync.WaitGroup
	results := make([]string, len(trackedCoins))
	client := http.Client{Timeout: 4 * time.Second}

	for i, coin := range trackedCoins {
		wg.Add(1)
		go func(idx int, c struct{ Symbol, Label string }) {
			defer wg.Done()
			url := fmt.Sprintf("https://api.binance.com/api/v3/ticker/price?symbol=%s", c.Symbol)

			req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
			if err != nil {
				results[idx] = fmt.Sprintf("⚪️ %s: err", c.Label)
				return
			}

			resp, err := client.Do(req)
			if err != nil {
				results[idx] = fmt.Sprintf("⚪️ %s: err", c.Label)
				return
			}
			defer resp.Body.Close()

			var data struct {
				Price string `json:"price"`
			}
			if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
				results[idx] = fmt.Sprintf("⚪️ %s: err", c.Label)
				return
			}

			currentPrice, _ := strconv.ParseFloat(data.Price, 64)

			var lastPrice float64
			_ = db.QueryRowContext(ctx, "SELECT price FROM market_prices WHERE symbol = $1", c.Symbol).
				Scan(&lastPrice)

			emoji := "⚪️"
			trend := "0.00%"
			if lastPrice > 0 {
				diff := ((currentPrice - lastPrice) / lastPrice) * 100
				if diff > 0.01 {
					emoji = "🟢"
					trend = fmt.Sprintf("+%.2f%%", diff)
				} else if diff < -0.01 {
					emoji = "🔴"
					trend = fmt.Sprintf("%.2f%%", diff)
				}
			}

			_, _ = db.ExecContext(ctx, `INSERT INTO market_prices (symbol, price) VALUES ($1, $2)
				 ON CONFLICT (symbol) DO UPDATE SET price = EXCLUDED.price`, c.Symbol, currentPrice)

			if c.Symbol == "USDTUAH" {
				results[idx] = fmt.Sprintf(
					"%s %s: *₴%.2f* (%s)",
					emoji,
					c.Label,
					currentPrice,
					trend,
				)
			} else {
				results[idx] = fmt.Sprintf("%s %s: *$%.2f* (%s)", emoji, c.Label, currentPrice, trend)
			}
		}(i, coin)
	}

	wg.Wait()
	return strings.Join(results, "\n")
}

func initDB() {
	var err error
	db, err = sql.Open("postgres", os.Getenv("DATABASE_URL"))
	if err != nil {
		log.Fatal(err)
	}
	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(25)

	_, _ = db.Exec(`CREATE TABLE IF NOT EXISTS subscribers (
		chat_id BIGINT PRIMARY KEY,
		interval_minutes INT DEFAULT 60,
		last_sent TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
		language_code TEXT DEFAULT 'ua',
		is_subscribed BOOLEAN DEFAULT FALSE
	);`)
	_, _ = db.Exec(
		`CREATE TABLE IF NOT EXISTS market_prices (symbol TEXT PRIMARY KEY, price DOUBLE PRECISION);`,
	)
}

func getLang(chatID int64) string {
	var lang string
	err := db.QueryRow("SELECT language_code FROM subscribers WHERE chat_id = $1", chatID).
		Scan(&lang)
	if err != nil {
		return "ua"
	}
	return lang
}

// --- ПУЛ ВОРКЕРОВ ДЛЯ КРОНА ---
func alertWorker(baseCtx context.Context, jobs <-chan Job, wg *sync.WaitGroup) {
	defer wg.Done()
	for job := range jobs {
		msg := tgbotapi.NewMessage(job.ChatID, job.Text)
		msg.ParseMode = "Markdown"
		msg.ReplyMarkup = getRefreshKeyboard(job.Lang)

		if _, err := bot.Send(msg); err == nil {
			dbCtx, dbCancel := context.WithTimeout(baseCtx, 2*time.Second)
			_, dbErr := db.ExecContext(
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


func handleCron(w http.ResponseWriter, r *http.Request) {
	log.Println("[Cron] Получен запрос от планировщика. Запуск рассылки...")
	ctx := r.Context()

	pricesText := getAllPricesFormatted(ctx)
	currentTime := time.Now().In(kyivLoc).Format("15:04")

	rows, err := db.QueryContext(ctx, `SELECT chat_id, language_code FROM subscribers
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
		go alertWorker(context.Background(), jobs, &workerWG)
	}

	for rows.Next() {
		var id int64
		var lang string
		if err := rows.Scan(&id, &lang); err == nil {
			text := fmt.Sprintf(
				getMsgText(lang, "alert_hdr")+"\n\n%s\n\n_%s_",
				currentTime,
				pricesText,
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

func handleWebhook(w http.ResponseWriter, r *http.Request) {
	var update tgbotapi.Update
	if err := json.NewDecoder(r.Body).Decode(&update); err != nil {
		log.Printf("[Webhook Error] Ошибка декодирования JSON: %v", err)
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	w.WriteHeader(http.StatusOK)

	go processTelegramUpdate(update)
}

func processTelegramUpdate(update tgbotapi.Update) {
	if update.CallbackQuery != nil {
		data := update.CallbackQuery.Data
		chatID := update.CallbackQuery.Message.Chat.ID

		if strings.HasPrefix(data, "setlang_") {
			newLang := data[8:]
			_, _ = db.Exec(`INSERT INTO subscribers (chat_id, language_code) VALUES ($1, $2)
                     ON CONFLICT (chat_id) DO UPDATE SET language_code = $2`, chatID, newLang)
			_, _ = bot.Request(tgbotapi.NewCallback(update.CallbackQuery.ID, "OK"))
			_, _ = bot.Send(tgbotapi.NewMessage(chatID, getMsgText(newLang, "lang_fixed")))
			return
		}

		lang := getLang(chatID)

		if strings.HasPrefix(data, "int_") {
			minutes, _ := strconv.Atoi(data[4:])
			_, _ = db.Exec(
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
			_, _ = bot.Request(tgbotapi.NewCallback(update.CallbackQuery.ID, "OK"))
			_, _ = bot.Send(
				tgbotapi.NewMessage(
					chatID,
					fmt.Sprintf(getMsgText(lang, "interval_set"), val, unit),
				),
			)
			return
		}

		if data == "refresh_price" {
			prices := getAllPricesFormatted(context.Background())
			t := time.Now().In(kyivLoc).Format("15:04:05")
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
			_, _ = bot.Send(edit)
			_, _ = bot.Request(tgbotapi.NewCallback(update.CallbackQuery.ID, "OK"))
		}
		return
	}

	if update.Message == nil {
		return
	}
	chatID := update.Message.Chat.ID
	lang := getLang(chatID)

	switch update.Message.Command() {
	case "start":
		msg := tgbotapi.NewMessage(chatID, getMsgText(lang, "welcome"))
		msg.ParseMode = "Markdown"
		_, _ = bot.Send(msg)
	case "language":
		msg := tgbotapi.NewMessage(chatID, getMsgText(lang, "lang_sel"))
		msg.ReplyMarkup = langKeyboard
		_, _ = bot.Send(msg)
	case "subscribe":
		_, _ = db.Exec(
			`INSERT INTO subscribers (chat_id, interval_minutes, last_sent, language_code, is_subscribed)
                 VALUES ($1, 60, NOW(), 'ua', TRUE) ON CONFLICT (chat_id) DO UPDATE SET is_subscribed = TRUE`,
			chatID,
		)
		_, _ = bot.Send(tgbotapi.NewMessage(chatID, getMsgText(lang, "subscribe")))
	case "unsubscribe":
		_, _ = db.Exec("UPDATE subscribers SET is_subscribed = FALSE WHERE chat_id = $1", chatID)
		_, _ = bot.Send(tgbotapi.NewMessage(chatID, getMsgText(lang, "unsubscribe")))
	case "interval":
		msg := tgbotapi.NewMessage(chatID, getMsgText(lang, "interval_m"))
		msg.ParseMode = "Markdown"
		msg.ReplyMarkup = getIntervalKeyboard(lang)
		_, _ = bot.Send(msg)
	case "price":
		prices := getAllPricesFormatted(context.Background())
		text := fmt.Sprintf(getMsgText(lang, "price_hdr")+"\n\n%s", prices)
		msg := tgbotapi.NewMessage(chatID, text)
		msg.ParseMode = "Markdown"
		msg.ReplyMarkup = getRefreshKeyboard(lang)
		_, _ = bot.Send(msg)
	}
}

func main() {
	_ = godotenv.Load()
	initDB()

	botToken = os.Getenv("TELEGRAM_APITOKEN")
	if botToken == "" {
		log.Fatal("TELEGRAM_APITOKEN is not set")
	}

	var err error
	bot, err = tgbotapi.NewBotAPI(botToken)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("[Success] Бот авторизован под именем: %s", bot.Self.UserName)

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("Bot server is running perfectly!"))
	})

	// Роут для крона
	http.HandleFunc("/cron", handleCron)

	http.HandleFunc("/webhook/"+botToken, handleWebhook)

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	log.Printf("[Info] HTTP Сервер запущен на порту %s", port)
	if err := http.ListenAndServe(":"+port, nil); err != nil {
		log.Fatalf("[Critical] Ошибка запуска сервера: %v", err)
	}
}
