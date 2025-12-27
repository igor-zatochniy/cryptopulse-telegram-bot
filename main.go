package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"sync"
	"time"

	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
	"github.com/joho/godotenv"
)

type BinancePrice struct {
	Symbol string `json:"symbol"`
	Price  string `json:"price"`
}

type Subscribers struct {
	mu    sync.Mutex
	chats map[int64]bool
}

var subs = Subscribers{chats: make(map[int64]bool)}

func getETHPrice() (string, error) {
	client := http.Client{Timeout: 10 * time.Second}
	resp, err := client.Get("https://api.binance.com/api/v3/ticker/price?symbol=ETHUSDT")
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	var data BinancePrice
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		return "", err
	}
	return data.Price, nil
}

func startPriceAlerts(bot *tgbotapi.BotAPI) {
	ticker := time.NewTicker(3 * time.Hour)
	for range ticker.C {
		price, err := getETHPrice()
		if err != nil {
			log.Println("Помилка при отриманні ціни для розсилки:", err)
			continue
		}

		text := fmt.Sprintf("🕒 *Регулярне оновлення*\nКурс Ethereum (ETH): *$%s*", price)

		subs.mu.Lock()
		for chatID := range subs.chats {
			msg := tgbotapi.NewMessage(chatID, text)
			msg.ParseMode = "Markdown"
			bot.Send(msg)
		}
		subs.mu.Unlock()
	}
}

func main() {
	_ = godotenv.Load()

	botToken := os.Getenv("TELEGRAM_APITOKEN")
	if botToken == "" {
		log.Fatal("Критична помилка: TELEGRAM_APITOKEN не знайдено")
	}

	bot, err := tgbotapi.NewBotAPI(botToken)
	if err != nil {
		log.Panic(err)
	}

	log.Printf("Авторизовано як %s", bot.Self.UserName)

	go startPriceAlerts(bot)

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "Бот %s працює у фоновому режимі!", bot.Self.UserName)
	})

	port := os.Getenv("PORT")
	if port == "" {
		port = "10000"
	}

	go func() {
		log.Printf("HTTP-сервер запущено на порту %s", port)
		if err := http.ListenAndServe("0.0.0.0:"+port, nil); err != nil {
			log.Fatal("Помилка сервера:", err)
		}
	}()

	u := tgbotapi.NewUpdate(0)
	u.Timeout = 60
	updates := bot.GetUpdatesChan(u)

	for update := range updates {
		if update.Message == nil {
			continue
		}

		chatID := update.Message.Chat.ID

		switch update.Message.Command() {
		case "start":
			msg := tgbotapi.NewMessage(
				chatID,
				"Привіт! Я бот-індикатор курсу ETH.\n\nКоманди:\n/price — дізнатися поточний курс\n/subscribe — отримувати сповіщення кожні 3 години\n/unsubscribe — скасувати підписку",
			)
			bot.Send(msg)

		case "subscribe":
			subs.mu.Lock()
			subs.chats[chatID] = true
			subs.mu.Unlock()
			bot.Send(
				tgbotapi.NewMessage(
					chatID,
					"✅ Ви підписалися на розсилку курсу ETH (кожні 3 години).",
				),
			)

		case "unsubscribe":
			subs.mu.Lock()
			delete(subs.chats, chatID)
			subs.mu.Unlock()
			bot.Send(tgbotapi.NewMessage(chatID, "❌ Ви скасували підписку."))

		case "price":
			price, err := getETHPrice()
			text := ""
			if err != nil {
				text = "Вибачте, не вдалося отримати дані від біржі."
			} else {
				text = fmt.Sprintf("💰 Поточний курс ETH: *$%s*", price)
			}
			msg := tgbotapi.NewMessage(chatID, text)
			msg.ParseMode = "Markdown"
			bot.Send(msg)
		}
	}
}
