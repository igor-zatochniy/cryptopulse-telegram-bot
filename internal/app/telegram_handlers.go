package app

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"

	appmetrics "github.com/igor-zatochniy/cryptopulse-telegram-bot/internal/metrics"
	apptelegram "github.com/igor-zatochniy/cryptopulse-telegram-bot/internal/telegram"
)

func (a *App) processTelegramUpdateWithDB(
	ctx context.Context,
	db databaseExecutor,
	update tgbotapi.Update,
) (processErr error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	if update.CallbackQuery != nil {
		if update.CallbackQuery.Message == nil {
			slog.Warn(
				"received inline callback query without message context object",
				"callback_id",
				update.CallbackQuery.ID,
			)
			return
		}

		data := update.CallbackQuery.Data
		chatID := update.CallbackQuery.Message.Chat.ID
		callbackID := update.CallbackQuery.ID

		if strings.HasPrefix(data, "setlang_") {
			newLang := data[8:]

			if !apptelegram.AllowedLanguage(newLang) {
				slog.Error(
					"invalid language selection",
					"chat_id",
					chatID,
					"payload",
					newLang,
				)
				a.answerCallback(ctx, callbackID, "Invalid Language")
				return
			}

			if _, err := db.ExecContext(ctx, `INSERT INTO subscribers (chat_id, interval_minutes, last_sent, language_code, is_subscribed)
                     VALUES ($1, 60, NOW() - INTERVAL '2 minute', $2, FALSE)
                     ON CONFLICT (chat_id) DO UPDATE SET language_code = EXCLUDED.language_code`, chatID, newLang); err != nil {
				appmetrics.DBOperationsTotal.WithLabelValues("set_language", "error").Inc()
				slog.Error(
					"failed to save language settings",
					"chat_id",
					chatID,
					"error",
					err,
				)
				a.answerCallback(ctx, callbackID, "Error")
				a.sendSafeMessage(ctx, chatID, apptelegram.Text("ua", "db_err"), nil)
				return err
			}

			appmetrics.DBOperationsTotal.WithLabelValues("set_language", "success").Inc()
			a.acknowledgeCallback(ctx, callbackID)
			a.sendSafeMessage(ctx, chatID, apptelegram.Text(newLang, "lang_fixed"), nil)
			return
		}

		lang := a.getLangWithDB(ctx, db, chatID)

		if strings.HasPrefix(data, "int_") {
			minutes, err := strconv.Atoi(data[4:])
			if err != nil || minutes < 1 || minutes > 1440 {
				// Передаємо створену групу як атрибут у метод Warn
				slog.Warn("callback data validation failed",
					slog.Group("security_alert",
						slog.String("reason", "malicious_callback_range_violation"),
						slog.Int64("chat_id", chatID),
						slog.String("payload", data),
					),
				)
				a.answerCallback(ctx, callbackID, "Invalid Range")
				return
			}

			result, err := db.ExecContext(ctx, "UPDATE subscribers SET interval_minutes = $1, last_sent = NOW() WHERE chat_id = $2 AND is_subscribed = TRUE", minutes, chatID)
			if err != nil {
				appmetrics.DBOperationsTotal.WithLabelValues("update_interval", "error").Inc()
				slog.Error(
					"failed to update notification frequency interval",
					"chat_id",
					chatID,
					"error",
					err,
				)
				a.answerCallback(ctx, callbackID, "Error")
				a.sendSafeMessage(ctx, chatID, apptelegram.Text(lang, "db_err"), nil)
				return err
			}

			affectedRows, err := result.RowsAffected()
			if err != nil {
				appmetrics.DBOperationsTotal.WithLabelValues("update_interval", "error").Inc()
				slog.Error(
					"failed to inspect interval update result",
					"chat_id",
					chatID,
					"error",
					err,
				)
				a.answerCallback(ctx, callbackID, "Error")
				a.sendSafeMessage(ctx, chatID, apptelegram.Text(lang, "db_err"), nil)
				return err
			}

			if affectedRows == 0 {
				appmetrics.DBOperationsTotal.WithLabelValues("update_interval", "inactive").Inc()
				slog.Info("interval update rejected for inactive subscriber", "chat_id", chatID)
				a.answerCallback(ctx, callbackID, apptelegram.Text(lang, "subscribe_first"))
				a.sendSafeMessage(ctx, chatID, apptelegram.Text(lang, "subscribe_first"), nil)
				return
			}

			appmetrics.DBOperationsTotal.WithLabelValues("update_interval", "success").Inc()
			unit := apptelegram.Text(lang, "unit_m")
			val := minutes
			if minutes >= 60 {
				unit = apptelegram.Text(lang, "unit_h")
				val = minutes / 60
			}
			a.acknowledgeCallback(ctx, callbackID)
			a.sendSafeMessage(ctx, chatID, fmt.Sprintf(apptelegram.Text(lang, "interval_set"), val, unit), nil)
			return
		}

		if data == "refresh_price" {
			prices := a.getFormattedPricesFromCache(lang)
			text := fmt.Sprintf(
				apptelegram.Text(lang, "price_hdr")+"\n\n%s\n\n_%s_",
				prices,
				apptelegram.Text(lang, "dynamics"),
			)

			a.editSafeMessage(
				ctx,
				chatID,
				update.CallbackQuery.Message.MessageID,
				text,
				apptelegram.RefreshKeyboard(lang),
			)
			a.acknowledgeCallback(ctx, callbackID)
		}
		return
	}

	if update.Message == nil {
		return
	}
	chatID := update.Message.Chat.ID
	lang := a.getLangWithDB(ctx, db, chatID)

	cmd := update.Message.Command()
	if strings.Contains(cmd, "@") {
		cmd = strings.Split(cmd, "@")[0]
	}

	switch cmd {
	case "start":
		a.sendSafeMessage(ctx, chatID, apptelegram.Text(lang, "welcome"), nil)
	case "language":
		a.sendSafeMessage(ctx, chatID, apptelegram.Text(lang, "lang_sel"), apptelegram.LanguageKeyboard())
	case "subscribe":
		if _, err := db.ExecContext(ctx, `INSERT INTO subscribers (chat_id, interval_minutes, last_sent, language_code, is_subscribed)
                 VALUES ($1, 60, NOW() - INTERVAL '2 minute', $2, TRUE)
                 ON CONFLICT (chat_id) DO UPDATE SET
                     interval_minutes = COALESCE(subscribers.interval_minutes, EXCLUDED.interval_minutes),
                     last_sent = COALESCE(subscribers.last_sent, EXCLUDED.last_sent),
                     language_code = EXCLUDED.language_code,
                     is_subscribed = TRUE,
                     delivery_suspended_until = NULL`, chatID, lang); err != nil {
			appmetrics.DBOperationsTotal.WithLabelValues("subscribe", "error").Inc()
			slog.Error("subscriber activation failed", "chat_id", chatID, "error", err)
			a.sendSafeMessage(ctx, chatID, apptelegram.Text(lang, "db_err"), nil)
			return err
		}
		appmetrics.DBOperationsTotal.WithLabelValues("subscribe", "success").Inc()
		a.sendSafeMessage(ctx, chatID, apptelegram.Text(lang, "subscribe"), nil)
	case "unsubscribe":
		if err := a.unsubscribe(ctx, db, chatID); err != nil {
			appmetrics.DBOperationsTotal.WithLabelValues("unsubscribe", "error").Inc()
			slog.Error("deactivation sql command failed", "chat_id", chatID, "error", err)
			a.sendSafeMessage(ctx, chatID, apptelegram.Text(lang, "db_err"), nil)
			return err
		}
		appmetrics.DBOperationsTotal.WithLabelValues("unsubscribe", "success").Inc()
		a.sendSafeMessage(ctx, chatID, apptelegram.Text(lang, "unsubscribe"), nil)
	case "interval":
		subscribed, err := a.isSubscribedWithDB(ctx, db, chatID)
		if err != nil {
			slog.Error("failed to check subscription status before interval menu", "chat_id", chatID, "error", err)
			a.sendSafeMessage(ctx, chatID, apptelegram.Text(lang, "db_err"), nil)
			return err
		}
		if !subscribed {
			a.sendSafeMessage(ctx, chatID, apptelegram.Text(lang, "subscribe_first"), nil)
			return
		}

		a.sendSafeMessage(ctx, chatID, apptelegram.Text(lang, "interval_m"), apptelegram.IntervalKeyboard(lang))
	case "price":
		prices := a.getFormattedPricesFromCache(lang)
		text := fmt.Sprintf(apptelegram.Text(lang, "price_hdr")+"\n\n%s", prices)
		a.sendSafeMessage(ctx, chatID, text, apptelegram.RefreshKeyboard(lang))
	}

	return nil
}

func (a *App) unsubscribe(ctx context.Context, db databaseExecutor, chatID int64) error {
	if _, err := db.ExecContext(
		ctx,
		`UPDATE subscribers
		 SET is_subscribed = FALSE,
		     cron_claimed_until = NULL,
		     delivery_suspended_until = NULL
		 WHERE chat_id = $1`,
		chatID,
	); err != nil {
		return err
	}

	if _, err := db.ExecContext(
		ctx,
		`UPDATE notification_jobs
		 SET status = 'canceled',
		     canceled_at = NOW(),
		     claim_token = NULL,
		     claimed_until = NULL,
		     last_error = 'subscription canceled',
		     updated_at = NOW()
		 WHERE chat_id = $1
		 AND status IN ('pending', 'sending')`,
		chatID,
	); err != nil {
		return err
	}

	return nil
}
