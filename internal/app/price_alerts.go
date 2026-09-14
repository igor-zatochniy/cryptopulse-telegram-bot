package app

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"
	"time"

	apptelegram "github.com/igor-zatochniy/cryptopulse-telegram-bot/internal/telegram"
)

const (
	notificationPriceAlert = "price_alert"
	maxActivePriceAlerts   = 10
	priceAlertBatchLimit   = 100
)

var alertPercentPattern = regexp.MustCompile(`^[+-]?[0-9]{1,4}([.,][0-9]{1,2})?%?$`)

type priceAlert struct {
	ID       int64
	ChatID   int64
	Symbol   string
	Lang     string
	Percent  float64
	Base     float64
	BaseAt   time.Time
	Target   float64
	Status   string
	Delivery string
}

func alertSymbol(raw string) (string, bool) {
	symbol := strings.ToUpper(raw)
	if !strings.HasSuffix(symbol, "USDT") {
		symbol += "USDT"
	}
	switch symbol {
	case "BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT":
		return symbol, true
	default:
		return "", false
	}
}

func parseAlertPercent(raw string) (float64, error) {
	if !alertPercentPattern.MatchString(raw) {
		return 0, errors.New("invalid percentage format")
	}
	value, err := strconv.ParseFloat(strings.ReplaceAll(strings.TrimSuffix(raw, "%"), ",", "."), 64)
	if err != nil || value == 0 || value < -99.99 || value > 1000 {
		return 0, errors.New("percentage outside supported range")
	}
	return value, nil
}

func alertTarget(base, percent float64) (float64, error) {
	if !isValidMarketPrice(base) || math.IsNaN(percent) || math.IsInf(percent, 0) ||
		percent == 0 || percent < -99.99 || percent > 1000 {
		return 0, errors.New("invalid alert price or percentage")
	}
	// Множення перед діленням не додає похибку від попереднього округлення 1 + X/100.
	target := base * (10000 + math.Round(percent*100)) / 10000
	if !isValidMarketPrice(target) || (percent > 0 && target <= base) || (percent < 0 && target >= base) {
		return 0, errors.New("invalid alert target")
	}
	return target, nil
}

func (a *App) createPriceAlert(ctx context.Context, db databaseExecutor, updateID, chatID int64, lang, symbol string, percent float64) (priceAlert, string, error) {
	entry, ok := a.priceCache.Load(symbol)
	if !ok || entry.UpdatedAt.IsZero() || entry.UpdatedAt.After(time.Now().UTC()) ||
		priceAge(time.Now().UTC(), entry.UpdatedAt) > priceFreshnessLimit {
		return priceAlert{}, "stale", nil
	}
	target, err := alertTarget(entry.Current, percent)
	if err != nil {
		return priceAlert{}, "invalid", nil
	}

	// Inbox утримує chat lock: перевірка ліміту й INSERT серіалізовані між replicas.
	var count int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM price_alerts AS pa
		WHERE pa.chat_id = $1 AND (pa.status = 'active' OR EXISTS (
			SELECT 1 FROM notification_jobs WHERE price_alert_id = pa.id AND status IN ('pending', 'sending')
		))`, chatID).Scan(&count); err != nil {
		return priceAlert{}, "", err
	}
	if count >= maxActivePriceAlerts {
		return priceAlert{}, "limit", nil
	}
	alert := priceAlert{ChatID: chatID, Symbol: symbol, Lang: lang, Percent: percent, Base: entry.Current, BaseAt: entry.UpdatedAt, Target: target}
	err = db.QueryRowContext(ctx, `INSERT INTO price_alerts
		(chat_id, source_update_id, symbol, language_code, change_percent, base_price, base_price_at, target_price)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		ON CONFLICT DO NOTHING RETURNING id`, chatID, updateID, symbol, lang, percent, entry.Current, entry.UpdatedAt, target).Scan(&alert.ID)
	if errors.Is(err, sql.ErrNoRows) {
		return priceAlert{}, "duplicate", nil
	}
	return alert, "", err
}

func (a *App) listPriceAlerts(ctx context.Context, db databaseExecutor, chatID int64) ([]priceAlert, error) {
	rows, err := db.QueryContext(ctx, `SELECT pa.id, pa.symbol, pa.change_percent, pa.base_price,
		pa.target_price, pa.status, COALESCE(nj.status, '')
		FROM price_alerts AS pa LEFT JOIN notification_jobs AS nj ON nj.price_alert_id = pa.id
		WHERE pa.chat_id = $1 ORDER BY
		(pa.status = 'active' OR nj.status IN ('pending', 'sending')) DESC NULLS LAST, pa.id DESC LIMIT 20`, chatID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var alerts []priceAlert
	for rows.Next() {
		var alert priceAlert
		if err := rows.Scan(&alert.ID, &alert.Symbol, &alert.Percent, &alert.Base, &alert.Target, &alert.Status, &alert.Delivery); err != nil {
			return nil, err
		}
		alerts = append(alerts, alert)
	}
	return alerts, rows.Err()
}

func (a *App) cancelPriceAlert(ctx context.Context, db databaseExecutor, chatID, id int64) (bool, error) {
	// Той самий chat lock утримує sender; вже відправлене повідомлення не відкликаємо.
	result, err := db.ExecContext(ctx, `UPDATE price_alerts AS pa SET status = 'canceled', updated_at = NOW()
		WHERE pa.id = $1 AND pa.chat_id = $2 AND (pa.status = 'active' OR EXISTS (
			SELECT 1 FROM notification_jobs WHERE price_alert_id = pa.id AND status IN ('pending', 'sending')
		))`, id, chatID)
	if err != nil {
		return false, err
	}
	count, err := result.RowsAffected()
	if err != nil || count == 0 {
		return false, err
	}
	_, err = db.ExecContext(ctx, `UPDATE notification_jobs SET status = 'canceled', canceled_at = NOW(),
		claim_token = NULL, claimed_until = NULL, updated_at = NOW(), last_error = 'price alert canceled'
		WHERE price_alert_id = $1 AND chat_id = $2 AND kind = 'price_alert' AND status IN ('pending', 'sending')`, id, chatID)
	return err == nil, err
}

func (a *App) notificationEnabled(ctx context.Context, job NotificationJob) (bool, error) {
	if job.Kind != notificationPriceAlert {
		return a.isSubscribed(ctx, job.ChatID)
	}
	var enabled bool
	err := a.db.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM price_alerts
		WHERE id = $1 AND chat_id = $2 AND status = 'triggered')`, job.PriceAlertID, job.ChatID).Scan(&enabled)
	return enabled, err
}

func (a *App) evaluatePriceAlerts(ctx context.Context, symbol string, price float64, fetchedAt time.Time) (int, error) {
	if !a.priceAlertsEnabled {
		return 0, nil
	}
	if _, ok := alertSymbol(symbol); !ok {
		return 0, nil
	}
	if !isValidMarketPrice(price) || fetchedAt.IsZero() || fetchedAt.After(time.Now().UTC()) ||
		priceAge(time.Now().UTC(), fetchedAt) > priceFreshnessLimit {
		return 0, errors.New("cannot evaluate alerts with an invalid or stale quote")
	}
	dbCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	rows, err := a.db.QueryContext(dbCtx, `SELECT id, chat_id FROM price_alerts WHERE symbol = $1
		AND status = 'active' AND created_at <= $3 AND base_price_at <= $3
		AND ((change_percent > 0 AND target_price <= $2) OR (change_percent < 0 AND target_price >= $2))
		ORDER BY id LIMIT $4`, symbol, price, fetchedAt, priceAlertBatchLimit)
	if err != nil {
		return 0, err
	}
	var candidates []priceAlert
	for rows.Next() {
		var candidate priceAlert
		if err := rows.Scan(&candidate.ID, &candidate.ChatID); err != nil {
			_ = rows.Close()
			return 0, err
		}
		candidates = append(candidates, candidate)
	}
	err = rows.Err()
	_ = rows.Close()
	if err != nil {
		return 0, err
	}
	created := 0
	for _, candidate := range candidates {
		triggered, err := a.triggerPriceAlert(dbCtx, candidate, price, fetchedAt)
		if err != nil {
			return created, err
		}
		if triggered {
			created++
		}
	}
	return created, nil
}

// Кожне спрацювання фіксується окремо: timeout пачки не скасовує вже створені jobs.
func (a *App) triggerPriceAlert(ctx context.Context, candidate priceAlert, price float64, fetchedAt time.Time) (bool, error) {
	tx, err := a.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer func() { _ = tx.Rollback() }()
	locked, err := tryTelegramChatTransactionLock(ctx, tx, candidate.ChatID)
	if err != nil || !locked {
		return false, err
	}
	var alert priceAlert
	err = tx.QueryRowContext(ctx, `UPDATE price_alerts SET status = 'triggered', trigger_price = $2,
		triggered_at = $3, updated_at = NOW() WHERE id = $1 AND status = 'active'
		RETURNING id, chat_id, symbol,
		COALESCE((SELECT s.language_code FROM subscribers AS s WHERE s.chat_id = price_alerts.chat_id), language_code),
		change_percent, base_price, base_price_at, target_price`,
		candidate.ID, price, fetchedAt).Scan(&alert.ID, &alert.ChatID, &alert.Symbol, &alert.Lang,
		&alert.Percent, &alert.Base, &alert.BaseAt, &alert.Target)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	text := fmt.Sprintf(apptelegram.AlertText(alert.Lang, "triggered_message"),
		strings.TrimSuffix(alert.Symbol, "USDT"), alert.Percent, alert.Base, alert.Target, price,
		fetchedAt.In(a.kyivLoc).Format("02.01.2006 15:04:05"))
	if _, err := tx.ExecContext(ctx, `INSERT INTO notification_jobs
		(chat_id, language_code, message_text, scheduled_at, next_attempt_at, kind, price_alert_id)
		VALUES ($1, $2, $3, $4, NOW(), 'price_alert', $5)`,
		alert.ChatID, alert.Lang, text, fetchedAt, alert.ID); err != nil {
		return false, err
	}
	if err := tx.Commit(); err != nil {
		return false, err
	}
	return true, nil
}
