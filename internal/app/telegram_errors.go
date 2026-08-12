package app

import (
	"errors"
	"net/url"
	"strings"
	"time"

	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"

	"github.com/igor-zatochniy/cryptopulse-telegram-bot/internal/workers"
)

const redactedTelegramToken = "[REDACTED]"

func redactTelegramError(err error, token string) error {
	if err == nil || token == "" {
		return err
	}

	message := err.Error()
	for _, value := range []string{token, url.PathEscape(token), url.QueryEscape(token)} {
		if value != "" {
			message = strings.ReplaceAll(message, value, redactedTelegramToken)
		}
	}
	return errors.New(message)
}

func (a *App) safeTelegramError(err error) error {
	if a == nil || a.bot == nil {
		return err
	}
	return redactTelegramError(err, a.bot.Token)
}

func telegramRetryDelay(attempts int, sendErr error) time.Duration {
	delay := workers.RetryDelay(attempts)

	var telegramErr *tgbotapi.Error
	if errors.As(sendErr, &telegramErr) && telegramErr.RetryAfter > 0 {
		requestedDelay := time.Duration(telegramErr.RetryAfter) * time.Second
		if requestedDelay > delay {
			return requestedDelay
		}
	}

	return delay
}
