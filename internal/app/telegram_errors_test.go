package app

import (
	"context"
	"errors"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
)

func TestRedactTelegramErrorRemovesTokenFromTransportError(t *testing.T) {
	const token = "123456789:super-secret-token"
	err := &url.Error{
		Op:  "Post",
		URL: "https://api.telegram.org/bot" + token + "/sendMessage",
		Err: context.DeadlineExceeded,
	}

	redacted := redactTelegramError(err, token)
	if redacted == nil {
		t.Fatal("redacted error is nil")
	}
	if strings.Contains(redacted.Error(), token) {
		t.Fatalf("redacted error contains Telegram token: %q", redacted)
	}
	if !strings.Contains(redacted.Error(), redactedTelegramToken) {
		t.Fatalf("redacted error does not contain marker: %q", redacted)
	}
}

func TestRedactTelegramErrorRemovesEncodedToken(t *testing.T) {
	const token = "123456789:super-secret-token"
	err := errors.New("request failed for " + url.QueryEscape(token))

	redacted := redactTelegramError(err, token)
	if strings.Contains(redacted.Error(), url.QueryEscape(token)) {
		t.Fatalf("redacted error contains encoded Telegram token: %q", redacted)
	}
}

func TestTelegramRetryDelayHonorsLongestBackoff(t *testing.T) {
	tests := []struct {
		name       string
		attempts   int
		retryAfter int
		want       time.Duration
	}{
		{name: "Telegram delay wins", attempts: 1, retryAfter: 300, want: 5 * time.Minute},
		{name: "local delay wins", attempts: 2, retryAfter: 30, want: 2 * time.Minute},
		{name: "missing Telegram delay", attempts: 1, want: time.Minute},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			sendErr := &tgbotapi.Error{
				Code:    http.StatusTooManyRequests,
				Message: "Too Many Requests",
				ResponseParameters: tgbotapi.ResponseParameters{
					RetryAfter: test.retryAfter,
				},
			}
			if got := telegramRetryDelay(test.attempts, sendErr); got != test.want {
				t.Fatalf("retry delay = %s, want %s", got, test.want)
			}
		})
	}
}
