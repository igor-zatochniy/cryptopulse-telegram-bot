// Package app поєднує бізнес-сценарії CryptoPulse та керує їх життєвим циклом.
package app

import (
	"database/sql"
	"errors"
	"log/slog"
	"net/http"
	"sync"
	"time"

	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"

	appmetrics "github.com/igor-zatochniy/cryptopulse-telegram-bot/internal/metrics"
) // --- ТИПИ ДАНИХ І КЕШ ІЗ СИНХРОНІЗАЦІЄЮ ---

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

// --- СТРУКТУРА ЗАСТОСУНКУ (DEPENDENCY INJECTION) ---

type App struct {
	db            *sql.DB
	bot           *tgbotapi.BotAPI
	priceCache    *PriceCache
	kyivLoc       *time.Location
	httpClient    *http.Client
	webhookSecret string
	cronSecret    string
	metricsSecret string
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

var errJobOwnershipLost = errors.New("job ownership lost")

// --- БЕЗПЕЧНІ ОБГОРТКИ ДЛЯ TELEGRAM ---

func (a *App) sendSafeMessage(chatID int64, text string, markup interface{}) {
	msg := tgbotapi.NewMessage(chatID, text)
	msg.ParseMode = "Markdown"
	if markup != nil {
		msg.ReplyMarkup = markup
	}
	if _, err := a.bot.Send(msg); err != nil {
		appmetrics.TelegramSendErrorsTotal.WithLabelValues("interactive_message").Inc()
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
		appmetrics.TelegramSendErrorsTotal.WithLabelValues("interactive_edit").Inc()
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

// --- ФОНОВЕ ОПИТУВАННЯ BINANCE API ---
