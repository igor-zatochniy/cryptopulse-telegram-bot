// Package telegram містить Telegram UI, локалізацію та класифікацію API-помилок.
package telegram

import (
	"errors"
	"net/http"
	"strings"

	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
)

var allowedLanguages = map[string]bool{
	"ua": true,
	"en": true,
	"ru": true,
}

var messages = map[string]map[string]string{
	"ua": {
		"welcome":         "Вітаю! 🖖 Твій крипто-асистент уже на зв’язку! ⚡️\n\n🔹 Live-курси: BTC, ETH, SOL, BNB, USDT.\n🔹 Smart-сповіщення: Обирай частоту (1 хв – 24 год).\n🔹 UAH-маркет: Курс USDT до гривні.\n\nТисни **/subscribe** для старту!",
		"subscribe":       "✅ Підписка активована! Змінити частоту: /interval",
		"subscribe_first": "⚠️ Спочатку активуйте підписку: /subscribe",
		"unsubscribe":     "❌ Ви відписалися від розсилки. Налаштування мови збережено.",
		"price_hdr":       "💰 *Курси криптовалют:*",
		"interval_m":      "⚙️ *Оберіть частоту повідомлень:*",
		"interval_set":    "✅ Тепер я буду надсилати курс кожні %d %s.",
		"lang_sel":        "🌍 *Оберіть мову:*",
		"lang_fixed":      "✅ Мову змінено на Українську!",
		"price_data_time": "🕒 _Дані станом на %s (Київ)_",
		"stale_data":      "⚠️ Частина даних застаріла: останнє успішне оновлення було понад 1 хв тому.",
		"alert_hdr":       "🕒 *Планове оновлення (%s)*",
		"dynamics":        "Зміна від попереднього успішного оновлення",
		"delay_warning":   "⚠️ Доставку затримано; знімок цін може бути застарілим.",
		"unit_m":          "хв",
		"unit_h":          "год",
		"btn_upd":         "🔄 Оновити",
		"db_err":          "❌ Виникла технічна помилка при збереженні даних. Будь ласка, спробуйте пізніше.",
		"no_data":         "немає даних",
	},
	"en": {
		"welcome":         "Welcome! 🖖 Your crypto assistant is online! ⚡️\n\n🔹 Live rates: BTC, ETH, SOL, BNB, USDT.\n🔹 Smart alerts: Frequency (1 min – 24h).\n🔹 UAH market: USDT to UAH rate.\n\nPress **/subscribe** to start!",
		"subscribe":       "✅ Subscription activated! Change frequency: /interval",
		"subscribe_first": "⚠️ Please subscribe first: /subscribe",
		"unsubscribe":     "❌ You have unsubscribed. Language settings saved.",
		"price_hdr":       "💰 *Cryptocurrency rates:*",
		"interval_m":      "⚙️ *Choose alert frequency:*",
		"interval_set":    "✅ Now I will send the rates every %d %s.",
		"lang_sel":        "🌍 *Select your language:*",
		"lang_fixed":      "✅ Language changed to English!",
		"price_data_time": "🕒 _Data as of %s (Kyiv)_",
		"stale_data":      "⚠️ Some data is stale: the last successful update was more than 1 minute ago.",
		"alert_hdr":       "🕒 *Scheduled update (%s)*",
		"dynamics":        "Change since the previous successful update",
		"delay_warning":   "⚠️ Delivery was delayed; this price snapshot may be stale.",
		"unit_m":          "min",
		"unit_h":          "h",
		"btn_upd":         "🔄 Update",
		"db_err":          "❌ A technical error occurred while saving data. Please try again later.",
		"no_data":         "no data available",
	},
	"ru": {
		"welcome":         "Привет! 🖖 Твой крипто-ассистент уже на связи! ⚡️\n\n🔹 Live-курсы: BTC, ETH, SOL, BNB, USDT.\n🔹 Smart-уведомления: Частота (1 мин – 24 ч).\n🔹 UAH-маркет: Курс USDT к гривне.\n\nЖми **/subscribe** для старта!",
		"subscribe":       "✅ Подписка активирована! Изменить частоту: /interval",
		"subscribe_first": "⚠️ Сначала активируйте подписку: /subscribe",
		"unsubscribe":     "❌ Вы отписались от рассылки. Настройки языка сохранены.",
		"price_hdr":       "💰 *Курсы криптовалют:*",
		"interval_m":      "⚙️ *Выберите частоту уведомлений:*",
		"interval_set":    "✅ Теперь я буду присылать курс каждые %d %s.",
		"lang_sel":        "🌍 *Выберите язык:*",
		"lang_fixed":      "✅ Язык изменен на Русский!",
		"price_data_time": "🕒 _Данные на %s (Киев)_",
		"stale_data":      "⚠️ Часть данных устарела: последнее успешное обновление было более 1 минуты назад.",
		"alert_hdr":       "🕒 *Плановое обновление (%s)*",
		"dynamics":        "Изменение с предыдущего успешного обновления",
		"delay_warning":   "⚠️ Доставка задержалась; снимок цен может быть устаревшим.",
		"unit_m":          "мин",
		"unit_h":          "ч",
		"btn_upd":         "🔄 Update",
		"db_err":          "❌ Произошла техническая ошибка при сохранении данных. Пожалуйста, попробуйте позже.",
		"no_data":         "нет данных",
	},
}

// AllowedLanguage перевіряє підтримуваний language code.
func AllowedLanguage(language string) bool {
	return allowedLanguages[language]
}

// Text повертає переклад або український fallback.
func Text(language, key string) string {
	if languageMessages, ok := messages[language]; ok {
		if text, exists := languageMessages[key]; exists && text != "" {
			return text
		}
	}
	if text, exists := messages["ua"][key]; exists && text != "" {
		return text
	}
	return "⚠️ [Missing Translation]"
}

// RefreshKeyboard створює кнопку ручного оновлення курсу.
func RefreshKeyboard(language string) *tgbotapi.InlineKeyboardMarkup {
	keyboard := tgbotapi.NewInlineKeyboardMarkup(
		tgbotapi.NewInlineKeyboardRow(
			tgbotapi.NewInlineKeyboardButtonData(Text(language, "btn_upd"), "refresh_price"),
		),
	)
	return &keyboard
}

// IntervalKeyboard створює клавіатуру доступних інтервалів сповіщень.
func IntervalKeyboard(language string) tgbotapi.InlineKeyboardMarkup {
	minutes := Text(language, "unit_m")
	hours := Text(language, "unit_h")
	return tgbotapi.NewInlineKeyboardMarkup(
		tgbotapi.NewInlineKeyboardRow(
			tgbotapi.NewInlineKeyboardButtonData("1 "+minutes, "int_1"),
			tgbotapi.NewInlineKeyboardButtonData("5 "+minutes, "int_5"),
			tgbotapi.NewInlineKeyboardButtonData("10 "+minutes, "int_10"),
		),
		tgbotapi.NewInlineKeyboardRow(
			tgbotapi.NewInlineKeyboardButtonData("15 "+minutes, "int_15"),
			tgbotapi.NewInlineKeyboardButtonData("30 "+minutes, "int_30"),
			tgbotapi.NewInlineKeyboardButtonData("1 "+hours, "int_60"),
		),
		tgbotapi.NewInlineKeyboardRow(
			tgbotapi.NewInlineKeyboardButtonData("3 "+hours, "int_180"),
			tgbotapi.NewInlineKeyboardButtonData("6 "+hours, "int_360"),
			tgbotapi.NewInlineKeyboardButtonData("12 "+hours, "int_720"),
		),
	)
}

// LanguageKeyboard створює клавіатуру підтримуваних мов.
func LanguageKeyboard() tgbotapi.InlineKeyboardMarkup {
	return tgbotapi.NewInlineKeyboardMarkup(
		tgbotapi.NewInlineKeyboardRow(
			tgbotapi.NewInlineKeyboardButtonData("🇺🇦 UA", "setlang_ua"),
			tgbotapi.NewInlineKeyboardButtonData("🇺🇸 EN", "setlang_en"),
			tgbotapi.NewInlineKeyboardButtonData("🇷🇺 RU", "setlang_ru"),
		),
	)
}

// IsPermanentSendError визначає помилки, після яких Telegram chat більше недоступний.
func IsPermanentSendError(err error) bool {
	var telegramError *tgbotapi.Error
	if errors.As(err, &telegramError) {
		message := strings.ToLower(telegramError.Message)
		if telegramError.Code == http.StatusForbidden {
			return true
		}
		if telegramError.Code == http.StatusBadRequest {
			return strings.Contains(message, "chat not found") ||
				strings.Contains(message, "user is deactivated")
		}
		return false
	}

	message := strings.ToLower(err.Error())
	return strings.Contains(message, "chat not found") ||
		strings.Contains(message, "bot was blocked") ||
		strings.Contains(message, "user is deactivated") ||
		strings.Contains(message, "bot can't initiate conversation")
}
