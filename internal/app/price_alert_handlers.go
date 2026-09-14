package app

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
	apptelegram "github.com/igor-zatochniy/cryptopulse-telegram-bot/internal/telegram"
)

func (a *App) showPriceAlertMenu(ctx context.Context, chatID int64, lang string) {
	key := "menu"
	if !a.priceAlertsEnabled {
		key = "disabled"
	}
	a.sendSafeMessage(ctx, chatID, apptelegram.AlertText(lang, key), apptelegram.AlertsKeyboard(lang))
}

func (a *App) handlePriceAlertCommand(ctx context.Context, db databaseExecutor, update tgbotapi.Update, lang string) error {
	chatID := update.Message.Chat.ID
	args := strings.Fields(update.Message.CommandArguments())
	if len(args) == 0 {
		a.showPriceAlertMenu(ctx, chatID, lang)
		return nil
	}
	if len(args) == 1 && args[0] == "list" {
		return a.showPriceAlertList(ctx, db, chatID, lang)
	}
	if len(args) != 2 {
		a.sendSafeMessage(ctx, chatID, apptelegram.AlertText(lang, "invalid"), nil)
		return nil
	}
	return a.addPriceAlertAndReply(ctx, db, int64(update.UpdateID), chatID, lang, args[0], args[1])
}

func (a *App) handlePriceAlertCallback(ctx context.Context, db databaseExecutor, update tgbotapi.Update, lang string) error {
	callback := update.CallbackQuery
	chatID := callback.Message.Chat.ID
	defer a.acknowledgeCallback(ctx, callback.ID)
	data := callback.Data
	switch {
	case data == "alerts_menu":
		a.showPriceAlertMenu(ctx, chatID, lang)
	case data == "alerts_list":
		return a.showPriceAlertList(ctx, db, chatID, lang)
	case strings.HasPrefix(data, "alerts_del_"):
		id, err := strconv.ParseInt(strings.TrimPrefix(data, "alerts_del_"), 10, 64)
		if err != nil || id <= 0 {
			a.sendSafeMessage(ctx, chatID, apptelegram.AlertText(lang, "not_found"), nil)
			return nil
		}
		canceled, err := a.cancelPriceAlert(ctx, db, chatID, id)
		if err != nil {
			return err
		}
		key := "not_found"
		if canceled {
			key = "canceled"
		}
		a.sendSafeMessage(ctx, chatID, apptelegram.AlertText(lang, key), nil)
	case !a.priceAlertsEnabled:
		a.showPriceAlertMenu(ctx, chatID, lang)
	case strings.HasPrefix(data, "alerts_coin_"):
		symbol, ok := alertSymbol(strings.TrimPrefix(data, "alerts_coin_"))
		if !ok {
			a.sendSafeMessage(ctx, chatID, apptelegram.AlertText(lang, "invalid"), nil)
			return nil
		}
		coin := strings.TrimSuffix(symbol, "USDT")
		a.sendSafeMessage(ctx, chatID, fmt.Sprintf(apptelegram.AlertText(lang, "coin"), coin), apptelegram.AlertPercentKeyboard(lang, coin))
	case strings.HasPrefix(data, "alerts_add_"):
		parts := strings.Split(strings.TrimPrefix(data, "alerts_add_"), "_")
		if len(parts) != 2 {
			a.sendSafeMessage(ctx, chatID, apptelegram.AlertText(lang, "invalid"), nil)
			return nil
		}
		return a.addPriceAlertAndReply(ctx, db, int64(update.UpdateID), chatID, lang, parts[0], parts[1])
	default:
		a.sendSafeMessage(ctx, chatID, apptelegram.AlertText(lang, "invalid"), nil)
	}
	return nil
}

func (a *App) addPriceAlertAndReply(ctx context.Context, db databaseExecutor, updateID, chatID int64, lang, rawSymbol, rawPercent string) error {
	if !a.priceAlertsEnabled {
		a.showPriceAlertMenu(ctx, chatID, lang)
		return nil
	}
	symbol, ok := alertSymbol(rawSymbol)
	percent, err := parseAlertPercent(rawPercent)
	if !ok || err != nil {
		a.sendSafeMessage(ctx, chatID, apptelegram.AlertText(lang, "invalid"), nil)
		return nil
	}
	alert, problem, err := a.createPriceAlert(ctx, db, updateID, chatID, lang, symbol, percent)
	if err != nil {
		return err
	}
	if problem != "" {
		a.sendSafeMessage(ctx, chatID, apptelegram.AlertText(lang, problem), nil)
		return nil
	}
	text := fmt.Sprintf(apptelegram.AlertText(lang, "created"), alert.ID, strings.TrimSuffix(symbol, "USDT"),
		percent, alert.Base, alert.Target, alert.BaseAt.In(a.kyivLoc).Format("02.01.2006 15:04:05"))
	a.sendSafeMessage(ctx, chatID, text, apptelegram.AlertsKeyboard(lang))
	return nil
}

func (a *App) showPriceAlertList(ctx context.Context, db databaseExecutor, chatID int64, lang string) error {
	alerts, err := a.listPriceAlerts(ctx, db, chatID)
	if err != nil {
		return err
	}
	if len(alerts) == 0 {
		a.sendSafeMessage(ctx, chatID, apptelegram.AlertText(lang, "empty"), apptelegram.AlertsKeyboard(lang))
		return nil
	}
	lines := []string{"*" + apptelegram.AlertText(lang, "list") + "*"}
	var rows [][]tgbotapi.InlineKeyboardButton
	for _, alert := range alerts {
		state := alert.Status
		if alert.Delivery != "" {
			state = alert.Delivery
		}
		if state == "canceled" {
			state = "canceled_state"
		}
		lines = append(lines, fmt.Sprintf("#%d %s %+.2f%%: %.8g → %.8g USDT (%s)", alert.ID,
			strings.TrimSuffix(alert.Symbol, "USDT"), alert.Percent, alert.Base, alert.Target, apptelegram.AlertText(lang, state)))
		if alert.Status == "active" || alert.Delivery == "pending" || alert.Delivery == "sending" {
			rows = append(rows, tgbotapi.NewInlineKeyboardRow(tgbotapi.NewInlineKeyboardButtonData(
				fmt.Sprintf(apptelegram.AlertText(lang, "cancel"), alert.ID), fmt.Sprintf("alerts_del_%d", alert.ID))))
		}
	}
	rows = append(rows, tgbotapi.NewInlineKeyboardRow(tgbotapi.NewInlineKeyboardButtonData(apptelegram.AlertText(lang, "back"), "alerts_menu")))
	a.sendSafeMessage(ctx, chatID, strings.Join(lines, "\n"), tgbotapi.NewInlineKeyboardMarkup(rows...))
	return nil
}
