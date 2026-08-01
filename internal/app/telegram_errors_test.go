package app

import (
	"context"
	"errors"
	"net/url"
	"strings"
	"testing"
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
