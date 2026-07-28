package app

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	appmetrics "github.com/igor-zatochniy/cryptopulse-telegram-bot/internal/metrics"
	apptelegram "github.com/igor-zatochniy/cryptopulse-telegram-bot/internal/telegram"
)

func (a *App) startPriceTicker(ctx context.Context) {
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()

	slog.Info("background crypto price ticker service started")
	a.fetchAndCachePrices(ctx)

	for {
		select {
		case <-ticker.C:
			a.fetchAndCachePrices(ctx)
		case <-ctx.Done():
			slog.Info("background price ticker successfully stopped")
			return
		}
	}
}

func (a *App) fetchAndCachePrices(ctx context.Context) {
	var wg sync.WaitGroup
	for _, coin := range trackedCoins {
		wg.Add(1)
		go func(c struct{ Symbol, Label string }) {
			defer wg.Done()

			url := fmt.Sprintf("https://api.binance.com/api/v3/ticker/price?symbol=%s", c.Symbol)
			req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
			if err != nil {
				appmetrics.BinanceRequestsTotal.WithLabelValues(c.Symbol, "request_create_error").Inc()
				slog.Error("failed to create ticker request", "symbol", c.Symbol, "error", err)
				return
			}

			resp, err := a.httpClient.Do(req)
			if err != nil {
				appmetrics.BinanceRequestsTotal.WithLabelValues(c.Symbol, "request_error").Inc()
				slog.Error("binance standard fetch failed", "symbol", c.Symbol, "error", err)
				return
			}
			defer resp.Body.Close()

			if resp.StatusCode != http.StatusOK {
				appmetrics.BinanceRequestsTotal.WithLabelValues(c.Symbol, "bad_status").Inc()
				slog.Error(
					"binance gateway returned non-200 status code",
					"symbol",
					c.Symbol,
					"status",
					resp.StatusCode,
				)
				return
			}

			var data struct {
				Price string `json:"price"`
			}
			limitedBody := io.LimitReader(resp.Body, 102400)
			if err := json.NewDecoder(limitedBody).Decode(&data); err != nil {
				appmetrics.BinanceRequestsTotal.WithLabelValues(c.Symbol, "decode_error").Inc()
				slog.Error(
					"failed to decode binance ticker payload",
					"symbol",
					c.Symbol,
					"error",
					err,
				)
				return
			}

			price, err := strconv.ParseFloat(data.Price, 64)
			if err != nil {
				appmetrics.BinanceRequestsTotal.WithLabelValues(c.Symbol, "parse_error").Inc()
				slog.Error(
					"failed to parse standard float rate value",
					"symbol",
					c.Symbol,
					"error",
					err,
				)
				return
			}

			a.priceCache.Store(c.Symbol, price)

			dbCtx, dbCancel := context.WithTimeout(ctx, 2*time.Second)
			_, err = a.db.ExecContext(
				dbCtx,
				`INSERT INTO market_prices (symbol, price) VALUES ($1, $2)
				 ON CONFLICT (symbol) DO UPDATE SET price = EXCLUDED.price, updated_at = NOW()`,
				c.Symbol,
				price,
			)
			dbCancel()

			if err != nil {
				appmetrics.DBOperationsTotal.WithLabelValues("price_upsert", "error").Inc()
				slog.Error(
					"failed to persist fetched price",
					"symbol",
					c.Symbol,
					"error",
					err,
				)
				return
			}

			appmetrics.DBOperationsTotal.WithLabelValues("price_upsert", "success").Inc()
			appmetrics.BinanceRequestsTotal.WithLabelValues(c.Symbol, "success").Inc()
		}(coin)
	}
	wg.Wait()
}

func (a *App) getFormattedPricesFromCache(lang string) string {
	results := make([]string, len(trackedCoins))
	for idx, coin := range trackedCoins {
		entry, ok := a.priceCache.Load(coin.Symbol)
		if !ok {
			results[idx] = fmt.Sprintf("⚪️ %s: %s", coin.Label, apptelegram.Text(lang, "no_data"))
			continue
		}

		emoji := "⚪️"
		percentChange := 0.0

		if entry.Previous > 0 {
			percentChange = ((entry.Current - entry.Previous) / entry.Previous) * 100
		}

		if percentChange > 0.001 {
			emoji = "🟢"
		} else if percentChange < -0.001 {
			emoji = "🔴"
		}

		var trendStr string
		if percentChange > 0 {
			trendStr = fmt.Sprintf("+%.2f%%", percentChange)
		} else {
			trendStr = fmt.Sprintf("%.2f%%", percentChange)
		}

		if coin.Symbol == "USDTUAH" {
			results[idx] = fmt.Sprintf(
				"%s %s: *₴%.2f* (`%s`)",
				emoji,
				coin.Label,
				entry.Current,
				trendStr,
			)
		} else {
			results[idx] = fmt.Sprintf("%s %s: *$%.2f* (`%s`)", emoji, coin.Label, entry.Current, trendStr)
		}
	}
	return strings.Join(results, "\n")
}

func (a *App) getLang(ctx context.Context, chatID int64) string {
	var lang string
	err := a.db.QueryRowContext(ctx, "SELECT language_code FROM subscribers WHERE chat_id = $1", chatID).
		Scan(&lang)
	if err != nil {
		return "ua"
	}

	if !apptelegram.AllowedLanguage(lang) {
		return "ua"
	}
	return lang
}

func (a *App) isSubscribed(ctx context.Context, chatID int64) (bool, error) {
	var subscribed bool
	err := a.db.QueryRowContext(ctx, "SELECT is_subscribed FROM subscribers WHERE chat_id = $1", chatID).
		Scan(&subscribed)
	if errors.Is(err, sql.ErrNoRows) {
		appmetrics.DBOperationsTotal.WithLabelValues("check_subscription", "not_found").Inc()
		return false, nil
	}
	if err != nil {
		appmetrics.DBOperationsTotal.WithLabelValues("check_subscription", "error").Inc()
		return false, err
	}

	if subscribed {
		appmetrics.DBOperationsTotal.WithLabelValues("check_subscription", "active").Inc()
	} else {
		appmetrics.DBOperationsTotal.WithLabelValues("check_subscription", "inactive").Inc()
	}

	return subscribed, nil
}

// --- ПРОГРІВ КЕШУ З БД ---

func (a *App) WarmupCache(ctx context.Context) {
	pricesCtx, pricesCancel := context.WithTimeout(ctx, 5*time.Second)
	defer pricesCancel()

	rows, err := a.db.QueryContext(pricesCtx, "SELECT symbol, price FROM market_prices")
	if err != nil {
		slog.Error("failed to load price cache from database", "error", err)
		return
	}
	defer rows.Close()

	for rows.Next() {
		var s string
		var p float64
		if err := rows.Scan(&s, &p); err != nil {
			slog.Error("failed to scan price cache row", "error", err)
			continue
		}
		a.priceCache.Store(s, p)
	}

	if err := rows.Err(); err != nil {
		slog.Error("failed while iterating price cache rows", "error", err)
	}

	slog.Info(
		"price cache warmup completed",
		"prices",
		len(trackedCoins),
	)
}

// --- ДОВГОТРИВАЛИЙ ПУЛ ВОРКЕРІВ ДЛЯ CRON І TELEGRAM-ОНОВЛЕНЬ ---
