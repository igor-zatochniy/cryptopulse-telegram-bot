//go:build integration

package app

import (
	"context"
	"errors"
	"net/http"
	"testing"

	"github.com/igor-zatochniy/cryptopulse-telegram-bot/internal/storage"
)

func TestIntegrationObservedCrossingSurvivesBusyChatAndRestart(t *testing.T) {
	db := setupIntegrationDB(t)
	a := newIntegrationApp(t, db, newFakeTelegramBot(t, nil))
	a.priceAlertsEnabled = true
	processAlertCommandForTest(t, a, 990001, 9900, "/alerts BTC +5")
	ctx := context.Background()
	conn, key, err := a.acquireTelegramChatAdvisoryLock(ctx, 9900)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if conn != nil {
			releaseTelegramChatAdvisoryLock(ctx, conn, key)
		}
	}()
	startedAt, err := a.beginPriceObservation(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := a.persistObservedMarketPrice(ctx, "BTCUSDT", 106, startedAt); err != nil {
		t.Fatal(err)
	}
	if n, err := a.processPriceAlertCrossings(ctx); n != 0 || err != nil {
		t.Fatalf("busy chat: %d, %v", n, err)
	}
	assertSQLCount(t, db, 1, `SELECT COUNT(*) FROM price_alert_crossings WHERE price = 106`)
	releaseTelegramChatAdvisoryLock(ctx, conn, key)
	conn = nil
	if _, err := a.persistMarketPrice(ctx, "BTCUSDT", 104); err != nil {
		t.Fatal(err)
	}
	// Нова replica не має попереднього in-memory observation; Binance також недоступний.
	restarted := newIntegrationApp(t, db, a.bot)
	restarted.httpClient = &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return nil, errors.New("Binance unavailable")
	})}
	restarted.fetchAndCachePrices(ctx)
	assertSQLCount(t, db, 1, `SELECT COUNT(*) FROM notification_jobs WHERE kind = 'price_alert'`)
	assertSQLCount(t, db, 1, `SELECT COUNT(*) FROM price_alerts WHERE trigger_price = 106 AND status = 'triggered'`)
	assertSQLCount(t, db, 0, `SELECT COUNT(*) FROM price_alert_crossings`)
}

func TestIntegrationCrossingCaptureHasNoDeliveryBatchLimit(t *testing.T) {
	db := setupIntegrationDB(t)
	a := newIntegrationApp(t, db, newFakeTelegramBot(t, nil))
	a.priceAlertsEnabled = true
	requireSQL(t, db, `INSERT INTO price_alerts
		(chat_id, source_update_id, symbol, language_code, change_percent, base_price, base_price_at, target_price, created_at)
		SELECT n, n, 'BTCUSDT', 'ua', 5, 100, NOW() - INTERVAL '2 seconds', 105, NOW() - INTERVAL '2 seconds'
		FROM generate_series(100001,100101) AS n`)
	ctx := context.Background()
	conn, _, err := a.acquireTelegramChatAdvisoryLock(ctx, 100001)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if conn != nil {
			_ = storage.DiscardConnection(conn)
		}
	}()
	startedAt, err := a.beginPriceObservation(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := a.persistObservedMarketPrice(ctx, "BTCUSDT", 106, startedAt); err != nil {
		t.Fatal(err)
	}
	assertSQLCount(t, db, 101, `SELECT COUNT(*) FROM price_alert_crossings`)
	if _, err := a.persistMarketPrice(ctx, "BTCUSDT", 104); err != nil {
		t.Fatal(err)
	}
	for range 5 {
		if _, err := a.processPriceAlertCrossings(ctx); err != nil && !errors.Is(err, context.DeadlineExceeded) {
			t.Fatal(err)
		}
	}
	assertSQLCount(t, db, 100, `SELECT COUNT(*) FROM notification_jobs`)
	assertSQLCount(t, db, 1, `SELECT COUNT(*) FROM price_alert_crossings`)
	if err := storage.DiscardConnection(conn); err != nil {
		t.Fatal(err)
	}
	conn = nil
	if n, err := a.processPriceAlertCrossings(ctx); n != 1 || err != nil {
		t.Fatalf("remaining crossing: %d, %v", n, err)
	}
	assertSQLCount(t, db, 101, `SELECT COUNT(*) FROM notification_jobs`)
}

func TestIntegrationCrossingPersistenceIsAtomicWithPrice(t *testing.T) {
	db := setupIntegrationDB(t)
	a := newIntegrationApp(t, db, newFakeTelegramBot(t, nil))
	a.priceAlertsEnabled = true
	processAlertCommandForTest(t, a, 993001, 9930, "/alerts BTC +5")
	if _, err := a.persistMarketPrice(context.Background(), "BTCUSDT", 100); err != nil {
		t.Fatal(err)
	}
	requireSQL(t, db, `ALTER TABLE price_alert_crossings ADD CONSTRAINT reject_crossing CHECK (FALSE)`)
	mockSuccessfulQuotes(a, "106")
	a.fetchAndCachePrices(context.Background())
	assertSQLCount(t, db, 1, `SELECT COUNT(*) FROM market_prices WHERE symbol = 'BTCUSDT' AND price = 100`)
	assertSQLCount(t, db, 0, `SELECT COUNT(*) FROM notification_jobs`)
	if entry, _ := a.priceCache.Load("BTCUSDT"); entry.Current != 100 {
		t.Fatalf("cache accepted uncommitted crossing: %+v", entry)
	}
}

func TestIntegrationCanceledObservedCrossingDoesNotSend(t *testing.T) {
	db := setupIntegrationDB(t)
	a := newIntegrationApp(t, db, newFakeTelegramBot(t, nil))
	a.priceAlertsEnabled = true
	processAlertCommandForTest(t, a, 994001, 9940, "/alerts BTC +5")
	ctx := context.Background()
	startedAt, err := a.beginPriceObservation(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := a.persistObservedMarketPrice(ctx, "BTCUSDT", 106, startedAt); err != nil {
		t.Fatal(err)
	}
	processOrderedUpdateForTest(t, a, orderedCallback(994002, 9940, "alerts_del_1"))
	if _, err := a.processPriceAlertCrossings(ctx); err != nil {
		t.Fatal(err)
	}
	assertSQLCount(t, db, 0, `SELECT COUNT(*) FROM notification_jobs`)
	assertSQLCount(t, db, 0, `SELECT COUNT(*) FROM price_alert_crossings`)
	assertSQLCount(t, db, 1, `SELECT COUNT(*) FROM price_alerts WHERE status = 'canceled'`)
}
