package app

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestMetricsAuthMiddleware(t *testing.T) {
	application := &App{metricsSecret: "test-secret"}

	tests := []struct {
		name       string
		authHeader string
		wantStatus int
		wantCalled bool
	}{
		{name: "missing authorization", wantStatus: http.StatusUnauthorized},
		{
			name:       "wrong bearer token",
			authHeader: "Bearer wrong-secret",
			wantStatus: http.StatusUnauthorized,
		},
		{
			name:       "valid bearer token",
			authHeader: "Bearer test-secret",
			wantStatus: http.StatusNoContent,
			wantCalled: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			called := false
			handler := application.metricsAuthMiddleware(func(w http.ResponseWriter, _ *http.Request) {
				called = true
				w.WriteHeader(http.StatusNoContent)
			})

			request := httptest.NewRequest(http.MethodGet, "/metrics", nil)
			if test.authHeader != "" {
				request.Header.Set("Authorization", test.authHeader)
			}
			response := httptest.NewRecorder()
			handler(response, request)

			if response.Code != test.wantStatus {
				t.Fatalf("status = %d, want %d", response.Code, test.wantStatus)
			}
			if called != test.wantCalled {
				t.Fatalf("called = %v, want %v", called, test.wantCalled)
			}
		})
	}
}

func TestFinalizationContextSurvivesParentCancellation(t *testing.T) {
	type contextKey string
	const requestIDKey contextKey = "request-id"

	parent, cancelParent := context.WithCancel(
		context.WithValue(context.Background(), requestIDKey, "req-42"),
	)
	cancelParent()

	finalCtx, cancelFinal := finalizationContext(parent, time.Second)
	defer cancelFinal()

	if err := finalCtx.Err(); err != nil {
		t.Fatalf("finalization context canceled with parent: %v", err)
	}
	if got := finalCtx.Value(requestIDKey); got != "req-42" {
		t.Fatalf("request id = %v, want req-42", got)
	}
}
