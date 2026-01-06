package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
	"github.com/joho/godotenv"
	_ "github.com/lib/pq"
)

var db *sql.DB
var kyivLoc = time.FixedZone("Kyiv", 2*60*60)

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
		"alert_hdr":    "🕒 *Плановое обновление (%s)*",
		"dynamics":     "Динамика зафиксирована",
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


func getAllPricesFormatted() string {
	var results []string
	client := http.Client{Timeout: 5 * time.Second}

	for _, coin := range trackedCoins {
		url := fmt.Sprintf("https://api.binance.com/api/v3/ticker/price?symbol=%s", coin.Symbol)
		resp, err := client.Get(url)
		if err != nil {
			results = append(results, fmt.Sprintf("⚪️ %s: err", coin.Label))
			continue
		}

		var data struct {
			Price string `json:"price"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
			resp.Body.Close()
			results = append(results, fmt.Sprintf("⚪️ %s: err", coin.Label))
			continue
		}
		resp.Body.Close()

		currentPrice, _ := strconv.ParseFloat(data.Price, 64)

		var lastPrice float64
		_ = db.QueryRow("SELECT price FROM market_prices WHERE symbol = $1", coin.Symbol).Scan(&lastPrice)

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

		db.Exec(`INSERT INTO market_prices (symbol, price) VALUES ($1, $2) 
                 ON CONFLICT (symbol) DO UPDATE SET price = EXCLUDED.price`, coin.Symbol, currentPrice)

		if coin.Symbol == "USDTUAH" {
			results = append(results, fmt.Sprintf("%s %s: *₴%.2f* (%s)", emoji, coin.Label, currentPrice, trend))
		} else {
			results = append(results, fmt.Sprintf("%s %s: *$%.2f* (%s)", emoji, coin.Label, currentPrice, trend))
		}
	}
	return strings.Join(results, "\n")
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
	db.Exec(`CREATE TABLE IF NOT EXISTS market_prices (symbol TEXT PRIMARY KEY, price DOUBLE PRECISION);`)
}

func getLang(chatID int64) string {
	var lang string
	err := db.QueryRow("SELECT language_code FROM subscribers WHERE chat_id = $1", chatID).Scan(&lang)
	if err != nil {
		return "ua"
	}
	return lang
}


func startPriceAlerts(bot *tgbotapi.BotAPI) {
	ticker := time.NewTicker(30 * time.Second)
	for range ticker.C {
		pricesText := getAllPricesFormatted()
		currentTime := time.Now().In(kyivLoc).Format("15:04")

		rows, err := db.Query(`SELECT chat_id, language_code FROM subscribers 
                               WHERE is_subscribed = TRUE 
                               AND last_sent <= NOW() - (interval_minutes * INTERVAL '1 minute') + INTERVAL '5 seconds'`)
		if err != nil {
			log.Println("DB Error:", err)
			continue
		}

		for rows.Next() {
			var id int64
			var lang string
			if err := rows.Scan(&id, &lang); err == nil {
				text := fmt.Sprintf(messages[lang]["alert_hdr"]+"\n\n%s\n\n_%s_", currentTime, pricesText, messages[lang]["dynamics"])
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

	botToken := os.Getenv("TELEGRAM_APITOKEN")
	if botToken == "" {
		log.Fatal("TELEGRAM_APITOKEN is not set")
	}

	bot, err := tgbotapi.NewBotAPI(botToken)
	if err != nil {
		log.Fatal(err)
	}

	go startPriceAlerts(bot)

	// Health check для Koyeb
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) { fmt.Fprintf(w, "Bot is alive!") })
	go http.ListenAndServe(":"+port, nil)

	u := tgbotapi.NewUpdate(0)
	u.Timeout = 60
	updates := bot.GetUpdatesChan(u)

	for update := range updates {
		if update.CallbackQuery != nil {
			data := update.CallbackQuery.Data
			chatID := update.CallbackQuery.Message.Chat.ID

			if strings.HasPrefix(data, "setlang_") {
				newLang := data[8:]
				db.Exec(`INSERT INTO subscribers (chat_id, language_code) VALUES ($1, $2) 
                         ON CONFLICT (chat_id) DO UPDATE SET language_code = $2`, chatID, newLang)
				bot.Request(tgbotapi.NewCallback(update.CallbackQuery.ID, "OK"))
				bot.Send(tgbotapi.NewMessage(chatID, messages[newLang]["lang_fixed"]))
				continue
			}

			lang := getLang(chatID)

			if strings.HasPrefix(data, "int_") {
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
				prices := getAllPricesFormatted()
				t := time.Now().In(kyivLoc).Format("15:04:05")
				text := fmt.Sprintf(messages[lang]["updated"]+"\n\n%s\n\n_%s_", t, prices, messages[lang]["dynamics"])
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
                     VALUES ($1, 60, NOW(), 'ua', TRUE) ON CONFLICT (chat_id) DO UPDATE SET is_subscribed = TRUE`, chatID)
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
			prices := getAllPricesFormatted()
			text := fmt.Sprintf(messages[lang]["price_hdr"]+"\n\n%s", prices)
			msg := tgbotapi.NewMessage(chatID, text)
			msg.ParseMode = "Markdown"
			msg.ReplyMarkup = getRefreshKeyboard(lang)
			bot.Send(msg)
		}
	}
}
