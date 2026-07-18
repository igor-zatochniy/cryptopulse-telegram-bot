package main

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"golang.org/x/time/rate"
)

func TestMetricsAuthMiddleware(t *testing.T) {
	app := &App{cronSecret: "test-secret"}
	called := false
	handler := app.metricsAuthMiddleware(func(w http.ResponseWriter, r *http.Request) {
		called = true
		w.WriteHeader(http.StatusNoContent)
	})

	tests := []struct {
		name       string
		authHeader string
		wantStatus int
		wantCalled bool
	}{
		{
			name:       "missing authorization",
			wantStatus: http.StatusUnauthorized,
			wantCalled: false,
		},
		{
			name:       "wrong bearer token",
			authHeader: "Bearer wrong-secret",
			wantStatus: http.StatusUnauthorized,
			wantCalled: false,
		},
		{
			name:       "valid bearer token",
			authHeader: "Bearer test-secret",
			wantStatus: http.StatusNoContent,
			wantCalled: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			called = false
			req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
			if tt.authHeader != "" {
				req.Header.Set("Authorization", tt.authHeader)
			}
			rec := httptest.NewRecorder()

			handler(rec, req)

			if rec.Code != tt.wantStatus {
				t.Fatalf("status = %d, want %d", rec.Code, tt.wantStatus)
			}
			if called != tt.wantCalled {
				t.Fatalf("called = %v, want %v", called, tt.wantCalled)
			}
		})
	}
}

func TestMethodMiddlewareRejectsWrongMethod(t *testing.T) {
	handler := methodMiddleware(http.MethodPost, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})

	req := httptest.NewRequest(http.MethodGet, "/cron", nil)
	rec := httptest.NewRecorder()

	handler(rec, req)

	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusMethodNotAllowed)
	}
	if allow := rec.Header().Get("Allow"); allow != http.MethodPost {
		t.Fatalf("Allow = %q, want %q", allow, http.MethodPost)
	}
}

func TestClientRateLimitMiddlewareIsolatesClients(t *testing.T) {
	app := &App{}
	limiter := newClientRateLimiter(rate.Every(time.Hour), 1, time.Minute)

	calls := 0
	handler := app.clientRateLimitMiddleware(limiter, func(w http.ResponseWriter, r *http.Request) {
		calls++
		w.WriteHeader(http.StatusNoContent)
	})

	first := httptest.NewRequest(http.MethodPost, "/webhook", nil)
	first.RemoteAddr = "192.0.2.10:1000"
	firstRec := httptest.NewRecorder()
	handler(firstRec, first)
	if firstRec.Code != http.StatusNoContent {
		t.Fatalf("first client status = %d, want %d", firstRec.Code, http.StatusNoContent)
	}

	sameClient := httptest.NewRequest(http.MethodPost, "/webhook", nil)
	sameClient.RemoteAddr = "192.0.2.10:2000"
	sameClientRec := httptest.NewRecorder()
	handler(sameClientRec, sameClient)
	if sameClientRec.Code != http.StatusTooManyRequests {
		t.Fatalf("same client status = %d, want %d", sameClientRec.Code, http.StatusTooManyRequests)
	}

	otherClient := httptest.NewRequest(http.MethodPost, "/webhook", nil)
	otherClient.RemoteAddr = "192.0.2.11:1000"
	otherClientRec := httptest.NewRecorder()
	handler(otherClientRec, otherClient)
	if otherClientRec.Code != http.StatusNoContent {
		t.Fatalf("other client status = %d, want %d", otherClientRec.Code, http.StatusNoContent)
	}

	if calls != 2 {
		t.Fatalf("handler calls = %d, want 2", calls)
	}
}

func TestRequestClientKeyStripsPort(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "/webhook", nil)
	req.RemoteAddr = "203.0.113.7:44321"

	if got := requestClientKey(req); got != "203.0.113.7" {
		t.Fatalf("client key = %q, want %q", got, "203.0.113.7")
	}
}
