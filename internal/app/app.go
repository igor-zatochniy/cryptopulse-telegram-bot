// Package app поєднує бізнес-сценарії CryptoPulse та керує їх життєвим циклом.
package app

import (
	"context"
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
	Current   float64
	Previous  float64
	UpdatedAt time.Time
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
	c.StoreAt(symbol, newPrice, time.Now().UTC())
}

func (c *PriceCache) StoreAt(symbol string, newPrice float64, updatedAt time.Time) {
	if updatedAt.IsZero() {
		updatedAt = time.Now().UTC()
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	oldEntry, ok := c.store[symbol]
	if !ok {
		c.store[symbol] = PriceEntry{
			Current:   newPrice,
			Previous:  newPrice,
			UpdatedAt: updatedAt,
		}
		return
	}

	c.store[symbol] = PriceEntry{
		Current:   newPrice,
		Previous:  oldEntry.Current,
		UpdatedAt: updatedAt,
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

type databaseExecutor interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// --- СТРУКТУРА ЗАСТОСУНКУ (DEPENDENCY INJECTION) ---

type App struct {
	db            *sql.DB
	lockDB        *sql.DB
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

func (a *App) lockDatabase() *sql.DB {
	if a.lockDB != nil {
		return a.lockDB
	}
	return a.db
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

func (a *App) sendSafeMessage(ctx context.Context, chatID int64, text string, markup interface{}) {
	if collector := telegramReplyCollectorFromContext(ctx); collector != nil {
		encodedMarkup, err := encodeTelegramReplyMarkup(markup)
		if err != nil {
			collector.setError(err)
			return
		}
		collector.add(TelegramReply{
			Operation:   telegramReplySendMessage,
			ChatID:      chatID,
			Text:        text,
			ReplyMarkup: encodedMarkup,
		})
		return
	}

	msg := tgbotapi.NewMessage(chatID, text)
	msg.ParseMode = "Markdown"
	if markup != nil {
		msg.ReplyMarkup = markup
	}
	if _, err := a.bot.Send(msg); err != nil {
		appmetrics.TelegramSendErrorsTotal.WithLabelValues("interactive_message").Inc()
		slog.Error("failed to send message", "chat_id", chatID, "error", a.safeTelegramError(err))
	}
}

func (a *App) editSafeMessage(
	ctx context.Context,
	chatID int64,
	messageID int,
	text string,
	markup *tgbotapi.InlineKeyboardMarkup,
) {
	if collector := telegramReplyCollectorFromContext(ctx); collector != nil {
		encodedMarkup, err := encodeTelegramReplyMarkup(markup)
		if err != nil {
			collector.setError(err)
			return
		}
		collector.add(TelegramReply{
			Operation:   telegramReplyEditMessage,
			ChatID:      chatID,
			MessageID:   messageID,
			Text:        text,
			ReplyMarkup: encodedMarkup,
		})
		return
	}

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
			a.safeTelegramError(err),
		)
	}
}

// Підтверджує callback без тексту після фіксації пов'язаної DB-транзакції.
func (a *App) acknowledgeCallback(ctx context.Context, callbackID string) {
	a.answerCallback(ctx, callbackID, "")
}

func (a *App) answerCallback(ctx context.Context, callbackID, text string) {
	if collector := telegramReplyCollectorFromContext(ctx); collector != nil {
		collector.addCallback(callbackID, text)
		return
	}

	if _, err := a.bot.Request(tgbotapi.NewCallback(callbackID, text)); err != nil {
		appmetrics.TelegramSendErrorsTotal.WithLabelValues("callback_answer").Inc()
		slog.Error("failed to answer callback query", "callback_id", callbackID, "error", a.safeTelegramError(err))
	}
}

// --- ФОНОВЕ ОПИТУВАННЯ BINANCE API ---
