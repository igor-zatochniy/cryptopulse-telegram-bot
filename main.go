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

// Клавіатура для оновлення
var refreshKeyboard = tgbotapi.NewInlineKeyboardMarkup(
	tgbotapi.NewInlineKeyboardRow(
		tgbotapi.NewInlineKeyboardButtonData("🔄 Оновити зараз", "refresh_price"),
	),
)

// Клавіатура вибору інтервалу
var intervalKeyboard = tgbotapi.NewInlineKeyboardMarkup(
	tgbotapi.NewInlineKeyboardRow(
		tgbotapi.NewInlineKeyboardButtonData("1 год", "int_1"),
		tgbotapi.NewInlineKeyboardButtonData("3 год", "int_3"),
		tgbotapi.NewInlineKeyboardButtonData("6 год", "int_6"),
	),
	tgbotapi.NewInlineKeyboardRow(
		tgbotapi.NewInlineKeyboardButtonData("12 год", "int_12"),
		tgbotapi.NewInlineKeyboardButtonData("24 год", "int_24"),
	),
)

type BinancePrice struct {
	Symbol string `json:"symbol"`
	Price  string `json:"price"`
}

// Функція отримання курсу з округленням
func getPrice(pair string) (string, error) {
	url := fmt.Sprintf("https://api.binance.com/api/v3/ticker/price?symbol=%s", pair)
	client := http.Client{Timeout: 10 * time.Second}
	resp, err := client.Get(url)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	var data BinancePrice
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		return "", err
	}

	priceFloat, err := strconv.ParseFloat(data.Price, 64)
	if err != nil {
		return data.Price, nil
	}
	return fmt.Sprintf("%.2f", priceFloat), nil
}

// Ініціалізація та оновлення структури БД
func initDB() {
	var err error
	connStr := os.Getenv("DATABASE_URL")
	db, err = sql.Open("postgres", connStr)
	if err != nil {
		log.Fatal(err)
	}

	// Створюємо таблицю, якщо її немає
	db.Exec(`CREATE TABLE IF NOT EXISTS subscribers (chat_id BIGINT PRIMARY KEY);`)

	// Додаємо нові колонки, якщо вони ще не існують
	db.Exec(`ALTER TABLE subscribers ADD COLUMN IF NOT EXISTS interval_hours INT DEFAULT 1;`)
	db.Exec(`ALTER TABLE subscribers ADD COLUMN IF NOT EXISTS last_sent TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP;`)

	log.Println("✅ База даних готова та оновлена.")
}

// Розумна розсилка за індивідуальни інтервалами
func startPriceAlerts(bot *tgbotapi.BotAPI) {
	ticker := time.NewTicker(1 * time.Hour) // Перевірка бази щогодини
	for range ticker.C {
		// Вибираємо користувачів, яким час надсилати повідомлення
		rows, err := db.Query(`
			SELECT chat_id, interval_hours FROM subscribers 
			WHERE last_sent <= NOW() - (interval_hours * INTERVAL '1 hour')
		`)
		if err != nil {
			log.Println("Помилка запиту розсилки:", err)
			continue
		}

		btc, _ := getPrice("BTCUSDT")
		eth, _ := getPrice("ETHUSDT")
		usdt, _ := getPrice("USDTUAH")
		currentTime := time.Now().In(kyivLoc).Format("15:04")

		text := fmt.Sprintf("🕒 *Планове оновлення (%s)*\n\n🟠 BTC: *$%s*\n🔹 ETH: *$%s*\n💵 USDT: *%s UAH*", currentTime, btc, eth, usdt)

		for rows.Next() {
			var id int64
			var interval int
			if err := rows.Scan(&id, &interval); err == nil {
				msg := tgbotapi.NewMessage(id, text)
				msg.ParseMode = "Markdown"
				msg.ReplyMarkup = refreshKeyboard
				bot.Send(msg)

				// Оновлюємо час останньої відправки в БД
				db.Exec("UPDATE subscribers SET last_sent = NOW() WHERE chat_id = $1", id)
			}
		}
		rows.Close()
	}
}

func main() {
	_ = godotenv.Load()
	initDB()

	bot, err := tgbotapi.NewBotAPI(os.Getenv("TELEGRAM_APITOKEN"))
	if err != nil {
		log.Panic(err)
	}

	log.Printf("Бот запущений: %s", bot.Self.UserName)

	go startPriceAlerts(bot)

	// Health Check для Koyeb
	go func() {
		port := os.Getenv("PORT")
		if port == "" {
			port = "8000"
		}
		http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
			fmt.Fprintf(w, "Крипто-бот працює!")
		})
		http.ListenAndServe(":"+port, nil)
	}()

	u := tgbotapi.NewUpdate(0)
	u.Timeout = 60
	updates := bot.GetUpdatesChan(u)

	for update := range updates {
		// Обробка натискань на кнопки
		if update.CallbackQuery != nil {
			data := update.CallbackQuery.Data
			chatID := update.CallbackQuery.Message.Chat.ID

			// Зміна інтервалу
			if len(data) > 4 && data[:4] == "int_" {
				hours, _ := strconv.Atoi(data[4:])
				db.Exec("UPDATE subscribers SET interval_hours = $1, last_sent = NOW() WHERE chat_id = $2", hours, chatID)

				bot.Request(tgbotapi.NewCallback(update.CallbackQuery.ID, "Інтервал змінено!"))
				bot.Send(tgbotapi.NewMessage(chatID, fmt.Sprintf("✅ Тепер я буду надсилати курс кожні %d год.", hours)))
			}

			// Кнопка оновити зараз
			if data == "refresh_price" {
				btc, _ := getPrice("BTCUSDT")
				eth, _ := getPrice("ETHUSDT")
				usdt, _ := getPrice("USDTUAH")
				t := time.Now().In(kyivLoc).Format("15:04:05")

				newText := fmt.Sprintf("🕒 *Оновлено о %s (Київ)*\n\n🟠 BTC: *$%s*\n🔹 ETH: *$%s*\n💵 USDT: *%s UAH*", t, btc, eth, usdt)

				edit := tgbotapi.NewEditMessageText(chatID, update.CallbackQuery.Message.MessageID, newText)
				edit.ParseMode = "Markdown"
				edit.ReplyMarkup = &refreshKeyboard
				bot.Send(edit)
				bot.Request(tgbotapi.NewCallback(update.CallbackQuery.ID, "Оновлено!"))
			}
			continue
		}

		if update.Message == nil {
			continue
		}
		chatID := update.Message.Chat.ID

		switch update.Message.Command() {
		case "start":
			text := "👋 *Вітаю!*\nЯ допоможу стежити за крипто-ринком.\n\n" +
				"/subscribe — підписатися на розсилку\n" +
				"/interval — обрати частоту (1-24 год)\n" +
				"/price — отримати курс зараз"
			msg := tgbotapi.NewMessage(chatID, text)
			msg.ParseMode = "Markdown"
			bot.Send(msg)

		case "subscribe":
			// При підписці ставимо дефолтний 1 годину
			db.Exec("INSERT INTO subscribers (chat_id, interval_hours, last_sent) VALUES ($1, 1, NOW()) ON CONFLICT (chat_id) DO UPDATE SET last_sent = NOW()", chatID)
			bot.Send(tgbotapi.NewMessage(chatID, "✅ Підписка активована! Використовуй /interval, щоб змінити частоту (за замовчуванням 1 год)."))

		case "interval":
			msg := tgbotapi.NewMessage(chatID, "⚙️ *Оберіть, як часто надсилати курс:*")
			msg.ParseMode = "Markdown"
			msg.ReplyMarkup = intervalKeyboard
			bot.Send(msg)

		case "price":
			btc, _ := getPrice("BTCUSDT")
			eth, _ := getPrice("ETHUSDT")
			usdt, _ := getPrice("USDTUAH")
			text := fmt.Sprintf("💰 *Актуальні курси:*\n\n🟠 BTC: *$%s*\n🔹 ETH: *$%s*\n💵 USDT: *%s UAH*", btc, eth, usdt)
			msg := tgbotapi.NewMessage(chatID, text)
			msg.ParseMode = "Markdown"
			msg.ReplyMarkup = refreshKeyboard
			bot.Send(msg)
		}
	}
}
