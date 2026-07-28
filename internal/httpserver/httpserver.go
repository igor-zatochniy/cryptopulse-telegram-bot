// Package httpserver містить HTTP transport, middleware та healthcheck-клієнт.
package httpserver

import (
	"context"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"time"

	"golang.org/x/time/rate"
)

type clientRateLimitEntry struct {
	limiter  *rate.Limiter
	lastSeen time.Time
}

// ClientRateLimiter ізолює rate limit budget кожного клієнта.
type ClientRateLimiter struct {
	mu          sync.Mutex
	limit       rate.Limit
	burst       int
	ttl         time.Duration
	lastCleanup time.Time
	clients     map[string]*clientRateLimitEntry
}

// NewClientRateLimiter створює limiter із автоматичним очищенням неактивних клієнтів.
func NewClientRateLimiter(limit rate.Limit, burst int, ttl time.Duration) *ClientRateLimiter {
	if ttl <= 0 {
		ttl = 10 * time.Minute
	}

	return &ClientRateLimiter{
		limit:   limit,
		burst:   burst,
		ttl:     ttl,
		clients: make(map[string]*clientRateLimitEntry),
	}
}

// Allow перевіряє budget окремого клієнта.
func (l *ClientRateLimiter) Allow(clientKey string) bool {
	if clientKey == "" {
		clientKey = "unknown"
	}

	now := time.Now()
	l.mu.Lock()
	if now.Sub(l.lastCleanup) >= l.ttl {
		for key, entry := range l.clients {
			if now.Sub(entry.lastSeen) >= l.ttl {
				delete(l.clients, key)
			}
		}
		l.lastCleanup = now
	}

	entry, ok := l.clients[clientKey]
	if !ok {
		entry = &clientRateLimitEntry{limiter: rate.NewLimiter(l.limit, l.burst)}
		l.clients[clientKey] = entry
	}
	entry.lastSeen = now
	limiter := entry.limiter
	l.mu.Unlock()

	return limiter.Allow()
}

// Method дозволяє handler-у приймати лише заданий HTTP method.
func Method(method string, next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != method {
			w.Header().Set("Allow", method)
			w.WriteHeader(http.StatusMethodNotAllowed)
			_, _ = w.Write([]byte("405 Method Not Allowed"))
			return
		}
		next(w, r)
	}
}

// GlobalRateLimit виконує process-level load shedding.
func GlobalRateLimit(limiter *rate.Limiter, next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !limiter.Allow() {
			slog.Warn("global rate limit exceeded", "endpoint", r.URL.Path, "remote_ip", r.RemoteAddr)
			http.Error(w, "Too Many Requests", http.StatusTooManyRequests)
			return
		}
		next(w, r)
	}
}

// ClientRateLimit застосовує незалежний rate limit до кожного remote client.
func ClientRateLimit(limiter *ClientRateLimiter, next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		clientKey := ClientKey(r)
		if !limiter.Allow(clientKey) {
			slog.Warn("client rate limit exceeded", "endpoint", r.URL.Path, "remote_ip", clientKey)
			http.Error(w, "Too Many Requests", http.StatusTooManyRequests)
			return
		}
		next(w, r)
	}
}

// ClientKey нормалізує RemoteAddr до IP-адреси без порту.
func ClientKey(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err == nil && host != "" {
		return host
	}
	if r.RemoteAddr != "" {
		return r.RemoteAddr
	}
	return "unknown"
}

// New створює HTTP server з production timeout-ами.
func New(port string, handler http.Handler) *http.Server {
	return &http.Server{
		Addr:              ":" + port,
		Handler:           handler,
		ReadTimeout:       5 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       120 * time.Second,
		ReadHeaderTimeout: 2 * time.Second,
	}
}

// Healthcheck перевіряє локальний liveness endpoint.
func Healthcheck(ctx context.Context, port string) error {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://127.0.0.1:"+port+"/live", nil)
	if err != nil {
		return err
	}

	response, err := (&http.Client{Timeout: 2 * time.Second}).Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		return &StatusError{Code: response.StatusCode}
	}
	return nil
}

// StatusError описує неуспішну відповідь healthcheck endpoint.
type StatusError struct {
	Code int
}

func (e *StatusError) Error() string {
	return http.StatusText(e.Code)
}
