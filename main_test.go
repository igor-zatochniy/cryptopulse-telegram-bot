package main

import (
	"net/http"
	"net/http/httptest"
	"testing"
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
