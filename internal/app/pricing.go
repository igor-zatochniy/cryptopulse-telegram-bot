package app

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	appmetrics "github.com/igor-zatochniy/cryptopulse-telegram-bot/internal/metrics"
	apptelegram "github.com/igor-zatochniy/cryptopulse-telegram-bot/internal/telegram"
)

const priceFreshnessLimit = time.Minute

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

			price, err := parseMarketPrice(data.Price)
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

			fetchedAt := time.Now().UTC()
			a.priceCache.StoreAt(c.Symbol, price, fetchedAt)

			dbCtx, dbCancel := context.WithTimeout(ctx, 2*time.Second)
			_, err = a.db.ExecContext(
				dbCtx,
				`INSERT INTO market_prices (symbol, price, updated_at) VALUES ($1, $2, $3)
				 ON CONFLICT (symbol) DO UPDATE
				 SET price = EXCLUDED.price, updated_at = EXCLUDED.updated_at`,
				c.Symbol,
				price,
				fetchedAt,
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
	a.observePriceAges(time.Now().UTC())
}

func parseMarketPrice(raw string) (float64, error) {
	price, err := strconv.ParseFloat(raw, 64)
	if err != nil {
		return 0, err
	}
	if !isValidMarketPrice(price) {
		return 0, fmt.Errorf("price must be finite and greater than zero")
	}
	return price, nil
}

func isValidMarketPrice(price float64) bool {
	return !math.IsNaN(price) && !math.IsInf(price, 0) && price > 0
}

func (a *App) getFormattedPricesFromCache(lang string) string {
	return a.getFormattedPricesFromCacheAt(lang, time.Now().UTC())
}

func (a *App) getFormattedPricesFromCacheAt(lang string, now time.Time) string {
	results := make([]string, len(trackedCoins))
	var oldestUpdate time.Time
	hasData := false
	hasStaleData := false

	for idx, coin := range trackedCoins {
		entry, ok := a.priceCache.Load(coin.Symbol)
		if !ok {
			results[idx] = fmt.Sprintf("⚪️ %s: %s", coin.Label, apptelegram.Text(lang, "no_data"))
			continue
		}

		hasData = true
		if !entry.UpdatedAt.IsZero() && (oldestUpdate.IsZero() || entry.UpdatedAt.Before(oldestUpdate)) {
			oldestUpdate = entry.UpdatedAt
		}

		emoji := "⚪️"
		percentChange := 0.0
		age := priceAge(now, entry.UpdatedAt)
		stale := entry.UpdatedAt.IsZero() || age > priceFreshnessLimit
		if stale {
			hasStaleData = true
			emoji = "⚠️"
		}

		if entry.Previous > 0 {
			percentChange = ((entry.Current - entry.Previous) / entry.Previous) * 100
		}

		if !stale && percentChange > 0.001 {
			emoji = "🟢"
		} else if !stale && percentChange < -0.001 {
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

	if hasData && !oldestUpdate.IsZero() {
		results = append(
			results,
			"",
			fmt.Sprintf(
				apptelegram.Text(lang, "price_data_time"),
				oldestUpdate.In(a.kyivLoc).Format("15:04:05"),
			),
		)
	}
	if hasStaleData {
		results = append(results, apptelegram.Text(lang, "stale_data"))
	}

	a.observePriceAges(now)
	return strings.Join(results, "\n")
}

func priceAge(now, updatedAt time.Time) time.Duration {
	if updatedAt.IsZero() {
		return 0
	}

	age := now.Sub(updatedAt)
	if age < 0 {
		return 0
	}
	return age
}

func (a *App) observePriceAges(now time.Time) {
	for _, coin := range trackedCoins {
		entry, ok := a.priceCache.Load(coin.Symbol)
		if !ok || entry.UpdatedAt.IsZero() {
			continue
		}
		appmetrics.PriceAgeSeconds.WithLabelValues(coin.Symbol).Set(priceAge(now, entry.UpdatedAt).Seconds())
	}
}

func (a *App) getLangWithDB(ctx context.Context, db databaseExecutor, chatID int64) string {
	var lang string
	err := db.QueryRowContext(ctx, "SELECT language_code FROM subscribers WHERE chat_id = $1", chatID).
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
	return a.isSubscribedWithDB(ctx, a.db, chatID)
}

func (a *App) isSubscribedWithDB(
	ctx context.Context,
	db databaseExecutor,
	chatID int64,
) (bool, error) {
	var subscribed bool
	err := db.QueryRowContext(ctx, "SELECT is_subscribed FROM subscribers WHERE chat_id = $1", chatID).
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

	rows, err := a.db.QueryContext(pricesCtx, "SELECT symbol, price, updated_at FROM market_prices")
	if err != nil {
		slog.Error("failed to load price cache from database", "error", err)
		return
	}
	defer rows.Close()

	for rows.Next() {
		var s string
		var p float64
		var updatedAt time.Time
		if err := rows.Scan(&s, &p, &updatedAt); err != nil {
			slog.Error("failed to scan price cache row", "error", err)
			continue
		}
		if !isValidMarketPrice(p) {
			slog.Warn("skipped invalid persisted market price", "symbol", s)
			continue
		}
		a.priceCache.StoreAt(s, p, updatedAt)
	}

	if err := rows.Err(); err != nil {
		slog.Error("failed while iterating price cache rows", "error", err)
	}
	a.observePriceAges(time.Now().UTC())

	slog.Info(
		"price cache warmup completed",
		"prices",
		len(trackedCoins),
	)
}

// --- ДОВГОТРИВАЛИЙ ПУЛ ВОРКЕРІВ ДЛЯ CRON І TELEGRAM-ОНОВЛЕНЬ ---
