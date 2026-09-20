package app

import (
	"context"
	"math"
	"testing"
	"time"

	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
)

func TestAlertPercentValidation(t *testing.T) {
	for _, raw := range []string{"NaN", "Inf", "-Inf", "0", "-0", "-100", "+1001", "1e2", "1.001", "1_0", "", "5;DROP", "--5"} {
		if _, err := parseAlertPercent(raw); err == nil {
			t.Errorf("accepted invalid percentage %q", raw)
		}
	}
	for raw, want := range map[string]float64{"+5": 5, "-7.5": -7.5, "1,25%": 1.25, "-99.99": -99.99, "1000": 1000, "0.01": 0.01} {
		got, err := parseAlertPercent(raw)
		if err != nil || got != want {
			t.Errorf("parse %q = %v, %v; want %v", raw, got, err, want)
		}
	}
}

func TestAlertTargets(t *testing.T) {
	for _, tc := range []struct{ base, percent, target float64 }{
		{60000, 5, 63000}, {60000, -5, 57000}, {100, 10, 110}, {100, -99.99, 0.01}, {100, 1000, 1100},
	} {
		got, err := alertTarget(tc.base, tc.percent)
		if err != nil || math.Abs(got-tc.target) > 1e-8 {
			t.Errorf("target(%v, %v) = %v, %v; want %v", tc.base, tc.percent, got, err, tc.target)
		}
	}
	for _, invalid := range []float64{0, -1, math.NaN(), math.Inf(1), math.Inf(-1), math.MaxFloat64} {
		if _, err := alertTarget(invalid, 5); err == nil {
			t.Errorf("accepted base %v", invalid)
		}
	}
	for _, invalid := range []float64{0, -100, 1001, math.NaN(), math.Inf(1)} {
		if _, err := alertTarget(100, invalid); err == nil {
			t.Errorf("accepted percentage %v", invalid)
		}
	}
}

func TestPriceAlertEvaluatorRejectsInvalidQuoteBeforeDatabase(t *testing.T) {
	a := &App{priceAlertsEnabled: true}
	now := time.Now()
	valid := priceObservation{StartedAt: now, StoredAt: now}
	for _, price := range []float64{math.NaN(), math.Inf(1), 0, -1} {
		if _, err := a.evaluatePriceAlerts(context.Background(), "BTCUSDT", price, valid); err == nil {
			t.Errorf("accepted invalid quote %v", price)
		}
	}
	for _, observation := range []priceObservation{
		{}, {StartedAt: now}, {StoredAt: now}, {StartedAt: now, StoredAt: now.Add(-time.Second)},
	} {
		if _, err := a.evaluatePriceAlerts(context.Background(), "BTCUSDT", 100, observation); err == nil {
			t.Errorf("accepted quote timestamps %v", observation)
		}
	}
	a.priceAlertsEnabled = false
	if count, err := a.evaluatePriceAlerts(context.Background(), "BTCUSDT", 105, valid); count != 0 || err != nil {
		t.Fatalf("disabled evaluator = %d, %v", count, err)
	}
}

func TestAlertCommandMenuPreservesExistingCommands(t *testing.T) {
	commands := []tgbotapi.BotCommand{{Command: "custom", Description: "Existing command"}}
	updated := withPriceAlertCommand(commands, "Price alerts")
	updated = withPriceAlertCommand(updated, "Updated description")
	if len(commands) != 1 || len(updated) != 2 || updated[0] != commands[0] || updated[1].Command != "alerts" || updated[1].Description != "Updated description" {
		t.Fatalf("unexpected commands: %+v", updated)
	}
}
