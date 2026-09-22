package app

import (
	"context"
	"database/sql"
	"strconv"
	"strings"

	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"

	apptelegram "github.com/igor-zatochniy/cryptopulse-telegram-bot/internal/telegram"
)

func telegramMutationSetting(update tgbotapi.Update) string {
	if query := update.CallbackQuery; query != nil {
		if query.Message == nil || query.Message.Chat == nil {
			return ""
		}
		if strings.HasPrefix(query.Data, "setlang_") && apptelegram.AllowedLanguage(strings.TrimPrefix(query.Data, "setlang_")) {
			return "language"
		}
		if strings.HasPrefix(query.Data, "int_") {
			minutes, err := strconv.Atoi(strings.TrimPrefix(query.Data, "int_"))
			if err == nil && minutes >= 1 && minutes <= 1440 {
				return "interval"
			}
		}
		return ""
	}
	if update.Message != nil {
		switch update.Message.Command() {
		case "subscribe", "unsubscribe":
			return "subscription"
		}
	}
	return ""
}

func allowTelegramMutation(ctx context.Context, tx *sql.Tx, job TelegramUpdateJob, update tgbotapi.Update) (bool, error) {
	setting := telegramMutationSetting(update)
	if setting == "" {
		return true, nil
	}
	if setting == "interval" {
		// Старий callback не змінює cadence новішої підписки або відписки.
		var superseded bool
		if err := tx.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM telegram_mutation_versions
			WHERE chat_id = $1 AND setting = 'subscription' AND (stream_epoch, update_id) > ($2, $3))`,
			job.ChatID, job.StreamEpoch, job.UpdateID).Scan(&superseded); err != nil {
			return false, err
		}
		if superseded {
			return false, nil
		}
	}
	// Версія та mutation входять до inbox transaction: помилка reply persistence відкочує обидві.
	result, err := tx.ExecContext(ctx, `INSERT INTO telegram_mutation_versions (chat_id, setting, stream_epoch, update_id)
		VALUES ($1, $2, $3, $4) ON CONFLICT (chat_id, setting) DO UPDATE
		SET stream_epoch = EXCLUDED.stream_epoch, update_id = EXCLUDED.update_id
		WHERE (telegram_mutation_versions.stream_epoch, telegram_mutation_versions.update_id)
		< (EXCLUDED.stream_epoch, EXCLUDED.update_id)`, job.ChatID, setting, job.StreamEpoch, job.UpdateID)
	if err != nil {
		return false, err
	}
	count, err := result.RowsAffected()
	return count > 0, err
}
