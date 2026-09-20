//go:build integration

package app

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

func mockSuccessfulQuotes(a *App, price string) {
	a.httpClient = &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Body: io.NopCloser(strings.NewReader(fmt.Sprintf(`{"symbol":%q,"price":%q}`,
				request.URL.Query().Get("symbol"), price))),
			Header: make(http.Header),
		}, nil
	})}
}

func TestIntegrationPriceAlertDatabaseClockSkew(t *testing.T) {
	for _, offset := range []int{-3600, 3600} {
		t.Run(fmt.Sprintf("database_offset_%d_seconds", offset), func(t *testing.T) {
			db := setupIntegrationDB(t)
			// Одна session з підміненою функцією часу лише в ізольованій тестовій БД.
			// Системні годинники та pg_catalog залишаються незмінними.
			db.SetMaxOpenConns(1)
			db.SetMaxIdleConns(1)
			requireSQL(t, db, `CREATE SCHEMA clock_skew`)
			requireSQL(t, db, fmt.Sprintf(`CREATE FUNCTION clock_skew.clock_timestamp()
				RETURNS timestamptz LANGUAGE SQL VOLATILE AS $$
				SELECT pg_catalog.clock_timestamp() + (%d * INTERVAL '1 second') $$`, offset))
			requireSQL(t, db, `SET search_path = clock_skew, public, pg_catalog`)
			a := newIntegrationApp(t, db, newFakeTelegramBot(t, nil))
			a.priceAlertsEnabled = true
			ctx := context.Background()
			startedAt, err := a.beginPriceObservation(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if delta := time.Until(startedAt).Seconds() - float64(offset); delta < -5 || delta > 5 {
				t.Fatalf("database clock offset not applied: %v", startedAt)
			}

			// Старий запит завершується й записує ціну вже після створення алерта.
			if _, reason, err := a.createPriceAlert(ctx, db, 82001, 820, "ua", "BTCUSDT", 5); reason != "" || err != nil {
				t.Fatalf("create with skewed database clock: %q, %v", reason, err)
			}
			assertSQLCount(t, db, 1, `SELECT COUNT(*) FROM price_alerts WHERE status = 'active'`)
			storedAt, err := a.persistMarketPrice(ctx, "BTCUSDT", 105)
			if err != nil {
				t.Fatal(err)
			}
			oldQuote := priceObservation{StartedAt: startedAt, StoredAt: storedAt}
			if n, err := a.evaluatePriceAlerts(ctx, "BTCUSDT", 105, oldQuote); n != 0 || err != nil {
				t.Fatalf("pre-creation observation triggered alert: %d, %v", n, err)
			}
			if n, err := evaluatePriceAlertQuoteForTest(a, "BTCUSDT", 105); n != 1 || err != nil {
				t.Fatalf("post-creation observation did not trigger: %d, %v", n, err)
			}

			if _, reason, err := a.createPriceAlert(ctx, db, 82002, 820, "ua", "ETHUSDT", 5); reason != "" || err != nil {
				t.Fatalf("create ETH alert: %q, %v", reason, err)
			}
			mockSuccessfulQuotes(a, "105")
			a.fetchAndCachePrices(ctx)
			assertSQLCount(t, db, 2, `SELECT COUNT(*) FROM notification_jobs WHERE kind = 'price_alert'`)
			var persistedAt time.Time
			if err := db.QueryRow(`SELECT updated_at FROM market_prices WHERE symbol = 'ETHUSDT'`).Scan(&persistedAt); err != nil {
				t.Fatal(err)
			}
			entry, ok := a.priceCache.Load("ETHUSDT")
			if !ok || entry.Current != 105 || !entry.UpdatedAt.Equal(persistedAt) {
				t.Fatalf("cache did not use database timestamp: %+v, stored=%v", entry, persistedAt)
			}

			for i, age := range []time.Duration{-2 * time.Minute, 2 * time.Minute} {
				a.priceCache.StoreAt("SOLUSDT", 100, persistedAt.Add(age))
				_, reason, err := a.createPriceAlert(ctx, db, int64(82010+i), 821, "ua", "SOLUSDT", 5)
				if reason != "stale" || err != nil {
					t.Fatalf("invalid base age %v accepted: reason=%q, err=%v", age, reason, err)
				}
			}
		})
	}
}

func TestIntegrationPriceAlertCreationUsesStatementTime(t *testing.T) {
	db := setupIntegrationDB(t)
	a := newIntegrationApp(t, db, newFakeTelegramBot(t, nil))
	a.priceAlertsEnabled = true
	ctx := context.Background()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()
	var transactionStart time.Time
	if err := tx.QueryRow(`SELECT NOW()`).Scan(&transactionStart); err != nil {
		t.Fatal(err)
	}
	startedAt, err := a.beginPriceObservation(ctx)
	if err != nil {
		t.Fatal(err)
	}
	alert, reason, err := a.createPriceAlert(ctx, tx, 82201, 822, "ua", "BTCUSDT", 5)
	if err != nil || reason != "" {
		t.Fatalf("create in existing transaction: %q, %v", reason, err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	var createdAt time.Time
	if err := db.QueryRow(`SELECT created_at FROM price_alerts WHERE id = $1`, alert.ID).Scan(&createdAt); err != nil {
		t.Fatal(err)
	}
	if !startedAt.After(transactionStart) || !createdAt.After(startedAt) {
		t.Fatalf("creation was backdated: transaction=%v observation=%v created=%v", transactionStart, startedAt, createdAt)
	}
	storedAt, err := a.persistMarketPrice(ctx, "BTCUSDT", 105)
	if err != nil {
		t.Fatal(err)
	}
	if n, err := a.evaluatePriceAlerts(ctx, "BTCUSDT", 105, priceObservation{StartedAt: startedAt, StoredAt: storedAt}); n != 0 || err != nil {
		t.Fatalf("quote predating INSERT triggered alert: %d, %v", n, err)
	}
	if n, err := evaluatePriceAlertQuoteForTest(a, "BTCUSDT", 105); n != 1 || err != nil {
		t.Fatalf("next observation did not trigger: %d, %v", n, err)
	}
}

func TestIntegrationPriceAlertRejectsStaleAndFutureObservations(t *testing.T) {
	db := setupIntegrationDB(t)
	a := newIntegrationApp(t, db, newFakeTelegramBot(t, nil))
	a.priceAlertsEnabled = true
	processAlertCommandForTest(t, a, 82301, 823, "/alerts BTC +5")
	requireSQL(t, db, `UPDATE price_alerts SET created_at = NOW() - INTERVAL '1 hour', base_price_at = NOW() - INTERVAL '1 hour'`)
	ctx := context.Background()
	now, err := a.beginPriceObservation(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, observation := range []priceObservation{
		{StartedAt: now.Add(-2 * time.Minute), StoredAt: now},
		{StartedAt: now.Add(time.Minute), StoredAt: now.Add(time.Minute)},
	} {
		if n, err := a.evaluatePriceAlerts(ctx, "BTCUSDT", 105, observation); n != 0 || err != nil {
			t.Fatalf("invalid observation triggered alert: %d, %v", n, err)
		}
	}
	assertSQLCount(t, db, 0, `SELECT COUNT(*) FROM notification_jobs`)
	if n, err := evaluatePriceAlertQuoteForTest(a, "BTCUSDT", 105); n != 1 || err != nil {
		t.Fatalf("fresh observation did not trigger: %d, %v", n, err)
	}
}

func TestIntegrationPricePersistenceFailurePreservesCacheAndAlert(t *testing.T) {
	db := setupIntegrationDB(t)
	a := newIntegrationApp(t, db, newFakeTelegramBot(t, nil))
	a.priceAlertsEnabled = true
	processAlertCommandForTest(t, a, 82401, 824, "/alerts BTC +5")
	before, _ := a.priceCache.Load("BTCUSDT")
	requireSQL(t, db, `ALTER TABLE market_prices ADD CONSTRAINT reject_btc_quote CHECK (symbol <> 'BTCUSDT')`)
	mockSuccessfulQuotes(a, "105")
	a.fetchAndCachePrices(context.Background())
	after, _ := a.priceCache.Load("BTCUSDT")
	if after != before {
		t.Fatalf("failed persistence changed cache: before=%+v after=%+v", before, after)
	}
	assertSQLCount(t, db, 1, `SELECT COUNT(*) FROM price_alerts WHERE status = 'active'`)
	assertSQLCount(t, db, 0, `SELECT COUNT(*) FROM notification_jobs`)
}
