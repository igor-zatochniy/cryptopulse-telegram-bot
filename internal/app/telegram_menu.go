package app

import (
	"context"
	"log/slog"

	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
)

// Оновлюємо лише default scope; власні команди й спеціальні chat scopes не видаляємо.
func (a *App) syncPriceAlertMenu(ctx context.Context) {
	if !a.priceAlertsEnabled {
		return
	}
	scope := tgbotapi.NewBotCommandScopeDefault()
	var defaults []tgbotapi.BotCommand
	for _, locale := range []struct{ code, description string }{
		{"", "Зростання або падіння ціни на X%"},
		{"uk", "Зростання або падіння ціни на X%"},
		{"en", "Price rise or fall by X%"},
		{"ru", "Зростання або падіння ціни на X%"},
	} {
		if ctx.Err() != nil {
			return
		}
		commands, err := a.bot.GetMyCommandsWithConfig(tgbotapi.NewGetMyCommandsWithScopeAndLanguage(scope, locale.code))
		if err != nil {
			slog.Warn("failed to read Telegram command menu", "language", locale.code, "error", a.safeTelegramError(err))
			return
		}
		if len(commands) == 0 {
			commands = append([]tgbotapi.BotCommand(nil), defaults...)
		}
		if len(commands) == 0 {
			commands = []tgbotapi.BotCommand{
				{Command: "start", Description: "Start"}, {Command: "price", Description: "Current prices"},
				{Command: "subscribe", Description: "Subscribe"}, {Command: "unsubscribe", Description: "Unsubscribe"},
				{Command: "interval", Description: "Update interval"}, {Command: "language", Description: "Language"},
			}
		}
		commands = withPriceAlertCommand(commands, locale.description)
		if len(commands) > 100 {
			slog.Warn("Telegram command menu is full; add alerts manually", "language", locale.code)
			continue
		}
		if ctx.Err() != nil {
			return
		}
		if _, err := a.bot.Request(tgbotapi.NewSetMyCommandsWithScopeAndLanguage(scope, locale.code, commands...)); err != nil {
			slog.Warn("failed to update Telegram command menu", "language", locale.code, "error", a.safeTelegramError(err))
			return
		}
		if locale.code == "" {
			defaults = commands
		}
	}
}

func withPriceAlertCommand(commands []tgbotapi.BotCommand, description string) []tgbotapi.BotCommand {
	result := append([]tgbotapi.BotCommand(nil), commands...)
	for i := range result {
		if result[i].Command == "alerts" {
			result[i].Description = description
			return result
		}
	}
	return append(result, tgbotapi.BotCommand{Command: "alerts", Description: description})
}
