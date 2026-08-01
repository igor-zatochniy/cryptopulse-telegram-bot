package app

import (
	"errors"
	"net/url"
	"strings"
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
