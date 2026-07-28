package httpserver

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"golang.org/x/time/rate"
)

func TestMethodRejectsWrongMethod(t *testing.T) {
	handler := Method(http.MethodPost, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})

	request := httptest.NewRequest(http.MethodGet, "/cron", nil)
	response := httptest.NewRecorder()
	handler(response, request)

	if response.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusMethodNotAllowed)
	}
	if allow := response.Header().Get("Allow"); allow != http.MethodPost {
		t.Fatalf("Allow = %q, want %q", allow, http.MethodPost)
	}
}

func TestClientRateLimitIsolatesClients(t *testing.T) {
	limiter := NewClientRateLimiter(rate.Every(time.Hour), 1, time.Minute)
	calls := 0
	handler := ClientRateLimit(limiter, func(w http.ResponseWriter, _ *http.Request) {
		calls++
		w.WriteHeader(http.StatusNoContent)
	})

	requests := []struct {
		remoteAddr string
		wantStatus int
	}{
		{"192.0.2.10:1000", http.StatusNoContent},
		{"192.0.2.10:2000", http.StatusTooManyRequests},
		{"192.0.2.11:1000", http.StatusNoContent},
	}
	for _, item := range requests {
		request := httptest.NewRequest(http.MethodPost, "/webhook", nil)
		request.RemoteAddr = item.remoteAddr
		response := httptest.NewRecorder()
		handler(response, request)
		if response.Code != item.wantStatus {
			t.Fatalf("remote %s status = %d, want %d", item.remoteAddr, response.Code, item.wantStatus)
		}
	}

	if calls != 2 {
		t.Fatalf("handler calls = %d, want 2", calls)
	}
}

func TestClientKeyStripsPort(t *testing.T) {
	request := httptest.NewRequest(http.MethodPost, "/webhook", nil)
	request.RemoteAddr = "203.0.113.7:44321"

	if got := ClientKey(request); got != "203.0.113.7" {
		t.Fatalf("client key = %q, want %q", got, "203.0.113.7")
	}
}
