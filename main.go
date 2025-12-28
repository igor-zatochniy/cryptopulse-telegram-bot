package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"

	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
	"github.com/joho/godotenv"
	_ "github.com/lib/pq"
)

var db *sql.DB
var kyivLoc = time.FixedZone("Kyiv", 2*60*60)

var messages = map[string]map[string]string{
	"ua": {
		"welcome":      "Вітаю! 🖖 Твій крипто-асистент уже на зв’язку! ⚡️\n\nХочеш тримати руку на пульсі ринку? Я допоможу!\n\n🔹 Live-курси: BTC, ETH, USDT за лічені секунди.\n🔹 Smart-сповіщення: Сам обирай, як часто отримувати апдейти (1–24 год).\n🔹 UAH-маркет: Слідкуй за реальним курсом USDT до гривні.\n🔹 Stability: Стабільна робота та збереження твоїх пресетів.\n\n🔥 Не гай часу! Тисни **/subscribe** та отримуй профіт від актуальної інформації!",
		"subscribe":    "✅ Підписка активована! Частота: 1 год. Змінити: /interval",
		"unsubscribe":  "❌ Ви відписалися від розсилки. Налаштування мови збережено.",
		"price_hdr":    "💰 *Актуальні курси:*",
		"interval_m":   "⚙️ *Оберіть частоту автоматичних повідомлень:*",
		"interval_set": "✅ Тепер я буду надсилати курс кожні %d %s.",
		"lang_sel":     "🌍 *Оберіть мову:*",
		"lang_fixed":   "✅ Мову змінено на Українську!",
		"updated":      "🕒 *Оновлено о %s (Київ)*",
		"alert_hdr":    "🕒 *Планове оновлення (%s)*",
		"dynamics":     " Динаміка зафіксована",
		"unit_m":       "хв",
		"unit_h":       "год",
		"btn_upd":      "🔄 Оновити",
	},
	"en": {
		"welcome":      "Welcome! 🖖 Your crypto assistant is online! ⚡️\n\nWant to keep your finger on the pulse of the market? I'll help!\n\n🔹 Live rates: BTC, ETH, USDT in seconds.\n🔹 Smart alerts: Choose frequency (1 min – 24h).\n🔹 UAH market: Follow the real USDT to UAH rate.\n🔹 Stability: Stable operation and saving your presets.\n\n🔥 Don't waste time! Press **/subscribe** and profit from up-to-date information!",
		"subscribe":    "✅ Subscription activated! Frequency: 1h. Change: /interval",
		"unsubscribe":  "❌ You have unsubscribed. Language settings saved.",
		"price_hdr":    "💰 *Current rates:*",
		"interval_m":   "⚙️ *Choose alert frequency:*",
		"interval_set": "✅ Now I will send the rates every %d %s.",
		"lang_sel":     "🌍 *Select your language:*",
		"lang_fixed":   "✅ Language changed to English!",
		"updated":      "🕒 *Updated at %s (Kyiv)*",
		"alert_hdr":    "🕒 *Scheduled update (%s)*",
		"dynamics":     " Dynamics fixed",
		"unit_m":       "min",
		"unit_h":       "h",
		"btn_upd":      "🔄 Update",
	},
	"ru": {
		"welcome":      "Привет! 🖖 Твой крипто-ассистент уже на связи! ⚡️\n\nХочешь держать руку на пульсе рынка? Я помогу!\n\n🔹 Live-курсы: BTC, ETH, USDT за считанные секунды.\n🔹 Smart-уведомления: Сам выбирай, как часто получать апдейты (1–24 ч).\n🔹 UAH-маркет: Следи за реальным курсом USDT к гривне.\n🔹 Stability: Стабильная работа и сохранение твоих пресетов.\n\n🔥 Не теряй времени! Жми **/subscribe** и получай профит от актуальной информации!",
		"subscribe":    "✅ Подписка активирована! Частота: 1 ч. Изменить: /interval",
		"unsubscribe":  "❌ Вы отписались от рассылки. Настройки языка сохранены.",
		"price_hdr":    "💰 *Актуальные курсы:*",
		"interval_m":   "⚙️ *Выберите частоту уведомлений:*",
		"interval_set": "✅ Теперь я буду присылать курс каждые %d %s.",
		"lang_sel":     "🌍 *Выберите язык:*",
		"lang_fixed":   "✅ Язык изменен на Русский!",
		"updated":      "🕒 *Обновлено в %s (Киев)*",
		"alert_hdr":    "🕒 *Плановое обновление (%s)*",
		"dynamics":     " Динамика зафиксирована",
		"unit_m":       "мин",
		"unit_h":       "ч",
		"btn_upd":      "🔄 Обновить",
	},
}


func getRefreshKeyboard(lang string) *tgbotapi.InlineKeyboardMarkup {
	text := messages[lang]["btn_upd"]
	kb := tgbotapi.NewInlineKeyboardMarkup(
		tgbotapi.NewInlineKeyboardRow(tgbotapi.NewInlineKeyboardButtonData(text, "refresh_price")),
	)
	return &kb
}

func getIntervalKeyboard(lang string) tgbotapi.InlineKeyboardMarkup {
	m := messages[lang]["unit_m"]
	h := messages[lang]["unit_h"]
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


func getLang(chatID int64) string {
	var lang string
	err := db.QueryRow("SELECT language_code FROM subscribers WHERE chat_id = $1", chatID).Scan(&lang)
	if err != nil {
		return "ua"
	}
	return lang
}

func getPriceWithTrend(pair string, label string) string {
	url := fmt.Sprintf("https://api.binance.com/api/v3/ticker/price?symbol=%s", pair)
	client := http.Client{Timeout: 10 * time.Second}
	resp, err := client.Get(url)
	if err != nil {
		return fmt.Sprintf("⚪️ %s: err", label)
	}
	defer resp.Body.Close()

	var data struct {
		Price string `json:"price"`
	}
	json.NewDecoder(resp.Body).Decode(&data)
	currentPrice, _ := strconv.ParseFloat(data.Price, 64)

	var lastPrice float64
	db.QueryRow("SELECT price FROM market_prices WHERE symbol = $1", pair).Scan(&lastPrice)

	emoji := "⚪️"
	trend := "0.00%"
	if lastPrice > 0 {
		diff := ((currentPrice - lastPrice) / lastPrice) * 100
		if diff > 0.01 {
			emoji = "🟢"
			trend = fmt.Sprintf("+%.2f%%", diff)
		}
		if diff < -0.01 {
			emoji = "🔴"
			trend = fmt.Sprintf("%.2f%%", diff)
		}
	}
	db.Exec(`INSERT INTO market_prices (symbol, price) VALUES ($1, $2) ON CONFLICT (symbol) DO UPDATE SET price = EXCLUDED.price`, pair, currentPrice)

	if pair == "USDTUAH" {
		return fmt.Sprintf("%s %s: *₴%.2f* (%s)", emoji, label, currentPrice, trend)
	}
	return fmt.Sprintf("%s %s: *$%.2f* (%s)", emoji, label, currentPrice, trend)
}

func initDB() {
	var err error
	db, err = sql.Open("postgres", os.Getenv("DATABASE_URL"))
	if err != nil {
		log.Fatal(err)
	}

	db.Exec(`CREATE TABLE IF NOT EXISTS subscribers (
		chat_id BIGINT PRIMARY KEY, 
		interval_minutes INT DEFAULT 60, 
		last_sent TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP, 
		language_code TEXT DEFAULT 'ua',
		is_subscribed BOOLEAN DEFAULT FALSE
	);`)
	db.Exec(`ALTER TABLE subscribers ADD COLUMN IF NOT EXISTS language_code TEXT DEFAULT 'ua';`)
	db.Exec(`ALTER TABLE subscribers ADD COLUMN IF NOT EXISTS is_subscribed BOOLEAN DEFAULT FALSE;`)
	db.Exec(`CREATE TABLE IF NOT EXISTS market_prices (symbol TEXT PRIMARY KEY, price DOUBLE PRECISION);`)
}

func startPriceAlerts(bot *tgbotapi.BotAPI) {
	ticker := time.NewTicker(30 * time.Second)
	for range ticker.C {
		rows, err := db.Query(`SELECT chat_id, language_code FROM subscribers 
                               WHERE is_subscribed = TRUE 
                               AND last_sent <= NOW() - (interval_minutes * INTERVAL '1 minute') + INTERVAL '10 seconds'`)
		if err != nil {
			continue
		}

		btc := getPriceWithTrend("BTCUSDT", "BTC")
		eth := getPriceWithTrend("ETHUSDT", "ETH")
		usdt := getPriceWithTrend("USDTUAH", "USDT")
		currentTime := time.Now().In(kyivLoc).Format("15:04")

		for rows.Next() {
			var id int64
			var lang string
			if err := rows.Scan(&id, &lang); err == nil {
				text := fmt.Sprintf(messages[lang]["alert_hdr"]+"\n\n%s\n%s\n%s\n\n_%s_", currentTime, btc, eth, usdt, messages[lang]["dynamics"])
				msg := tgbotapi.NewMessage(id, text)
				msg.ParseMode = "Markdown"
				msg.ReplyMarkup = getRefreshKeyboard(lang)
				bot.Send(msg)
				db.Exec("UPDATE subscribers SET last_sent = NOW() WHERE chat_id = $1", id)
			}
		}
		rows.Close()
	}
}

func main() {
	_ = godotenv.Load()
	initDB()
	bot, _ := tgbotapi.NewBotAPI(os.Getenv("TELEGRAM_APITOKEN"))

	go startPriceAlerts(bot)

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) { fmt.Fprintf(w, "✅ Бот працює!") })
	go http.ListenAndServe(":"+os.Getenv("PORT"), nil)

	u := tgbotapi.NewUpdate(0)
	u.Timeout = 60
	updates := bot.GetUpdatesChan(u)

	for update := range updates {
		if update.CallbackQuery != nil {
			data := update.CallbackQuery.Data
			chatID := update.CallbackQuery.Message.Chat.ID

			if len(data) > 8 && data[:8] == "setlang_" {
				newLang := data[8:]
				db.Exec(`INSERT INTO subscribers (chat_id, language_code) VALUES ($1, $2) 
                         ON CONFLICT (chat_id) DO UPDATE SET language_code = $2`, chatID, newLang)
				bot.Request(tgbotapi.NewCallback(update.CallbackQuery.ID, "OK"))
				bot.Send(tgbotapi.NewMessage(chatID, messages[newLang]["lang_fixed"]))
				continue
			}

			lang := getLang(chatID)

			if len(data) > 4 && data[:4] == "int_" {
				minutes, _ := strconv.Atoi(data[4:])
				db.Exec("UPDATE subscribers SET interval_minutes = $1, last_sent = NOW() WHERE chat_id = $2", minutes, chatID)
				unit := messages[lang]["unit_m"]
				val := minutes
				if minutes >= 60 {
					unit = messages[lang]["unit_h"]
					val = minutes / 60
				}
				bot.Request(tgbotapi.NewCallback(update.CallbackQuery.ID, "OK"))
				bot.Send(tgbotapi.NewMessage(chatID, fmt.Sprintf(messages[lang]["interval_set"], val, unit)))
				continue
			}

			if data == "refresh_price" {
				btc := getPriceWithTrend("BTCUSDT", "BTC")
				eth := getPriceWithTrend("ETHUSDT", "ETH")
				usdt := getPriceWithTrend("USDTUAH", "USDT")
				t := time.Now().In(kyivLoc).Format("15:04:05")
				text := fmt.Sprintf(messages[lang]["updated"]+"\n\n%s\n%s\n%s\n\n_%s_", t, btc, eth, usdt, messages[lang]["dynamics"])
				edit := tgbotapi.NewEditMessageText(chatID, update.CallbackQuery.Message.MessageID, text)
				edit.ParseMode = "Markdown"
				edit.ReplyMarkup = getRefreshKeyboard(lang)
				bot.Send(edit)
				bot.Request(tgbotapi.NewCallback(update.CallbackQuery.ID, "OK"))
			}
			continue
		}

		if update.Message == nil {
			continue
		}
		chatID := update.Message.Chat.ID
		lang := getLang(chatID)

		switch update.Message.Command() {
		case "start":
			msg := tgbotapi.NewMessage(chatID, messages[lang]["welcome"])
			msg.ParseMode = "Markdown"
			bot.Send(msg)

		case "language":
			msg := tgbotapi.NewMessage(chatID, messages[lang]["lang_sel"])
			msg.ReplyMarkup = langKeyboard
			bot.Send(msg)

		case "subscribe":
			db.Exec(`INSERT INTO subscribers (chat_id, interval_minutes, last_sent, language_code, is_subscribed) 
                     VALUES ($1, 60, NOW(), 'ua', TRUE) 
                     ON CONFLICT (chat_id) DO UPDATE SET is_subscribed = TRUE, last_sent = NOW()`, chatID)
			bot.Send(tgbotapi.NewMessage(chatID, messages[lang]["subscribe"]))

		case "unsubscribe":
			db.Exec("UPDATE subscribers SET is_subscribed = FALSE WHERE chat_id = $1", chatID)
			bot.Send(tgbotapi.NewMessage(chatID, messages[lang]["unsubscribe"]))

		case "interval":
			msg := tgbotapi.NewMessage(chatID, messages[lang]["interval_m"])
			msg.ParseMode = "Markdown"
			msg.ReplyMarkup = getIntervalKeyboard(lang)
			bot.Send(msg)

		case "price":
			btc := getPriceWithTrend("BTCUSDT", "BTC")
			eth := getPriceWithTrend("ETHUSDT", "ETH")
			usdt := getPriceWithTrend("USDTUAH", "USDT")
			text := fmt.Sprintf(messages[lang]["price_hdr"]+"\n\n%s\n%s\n%s", btc, eth, usdt)
			msg := tgbotapi.NewMessage(chatID, text)
			msg.ParseMode = "Markdown"
			msg.ReplyMarkup = getRefreshKeyboard(lang)
			bot.Send(msg)
		}
	}
}
