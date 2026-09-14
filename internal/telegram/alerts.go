package telegram

import (
	"fmt"

	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
)

var alertMessages = map[string]map[string]string{
	"ua": {
		"menu": "*Цінові алерти*\nОберіть монету. Відсоток рахується від ціни під час створення. Алерт одноразовий.\nВласний відсоток: `/alerts BTC +7.5` або `/alerts ETH -12`.",
		"coin": "*%s: оберіть зміну ціни*",
		"up":   "Зростання на %s%%", "down": "Падіння на %s%%",
		"list": "Алерти чату", "back": "Монети", "cancel": "Скасувати #%d",
		"created":           "*Алерт #%d створено: %s %+.2f%%*\nБазова ціна: %.8g USDT\nПоріг: %.8g USDT\nДані: %s (Київ).\nПоріг фіксований; перевірка за отриманими котируваннями, не за кожною біржовою угодою.",
		"triggered_message": "*Ціновий алерт: %s %+.2f%%*\nБазова ціна: %.8g USDT\nПоріг: %.8g USDT\nЦіна при спрацюванні: %.8g USDT\nДані: %s (Київ).\nАлерт одноразовий. Новий: /alerts",
		"invalid":           "Формат: `/alerts BTC +7.5`. Монети: BTC, ETH, SOL, BNB. Зростання: 0.01–1000%; падіння: 0.01–99.99%. До двох знаків після крапки.",
		"stale":             "Немає свіжої коректної ціни. Алерт не створено; спробуйте після оновлення котирувань.",
		"limit":             "Ліміт: 10 активних алертів або повідомлень, що очікують доставки. Скасуйте зайві через /alerts.",
		"duplicate":         "Такий активний алерт уже існує, або цю команду вже оброблено.",
		"disabled":          "Створення та перевірку нових алертів вимкнено. Раніше створені повідомлення можуть ще доставлятися; їх можна скасувати у списку.",
		"empty":             "Алертів немає.", "canceled": "Алерт скасовано.", "not_found": "Активного алерта в цьому чаті немає. Уже надіслане повідомлення не відкликається.",
		"independent": "Цінові алерти незалежні від регулярної розсилки. Перегляд і скасування: /alerts.",
		"active":      "активний", "triggered": "спрацював", "pending": "очікує доставки", "sending": "доставляється", "sent": "доставлено", "failed": "не доставлено", "canceled_state": "скасовано",
	},
	"en": {
		"menu": "*Price alerts*\nChoose a coin. The percentage is relative to the price when the alert is created. Each alert fires once.\nCustom percentage: `/alerts BTC +7.5` or `/alerts ETH -12`.",
		"coin": "*%s: choose a price change*",
		"up":   "Rise by %s%%", "down": "Fall by %s%%",
		"list": "Chat alerts", "back": "Coins", "cancel": "Cancel #%d",
		"created":           "*Alert #%d created: %s %+.2f%%*\nBase price: %.8g USDT\nTarget: %.8g USDT\nData: %s (Kyiv).\nThe target is fixed; checks use sampled quotes, not every exchange trade.",
		"triggered_message": "*Price alert: %s %+.2f%%*\nBase price: %.8g USDT\nTarget: %.8g USDT\nPrice at trigger: %.8g USDT\nData: %s (Kyiv).\nThis is a one-shot alert. New alert: /alerts",
		"invalid":           "Format: `/alerts BTC +7.5`. Coins: BTC, ETH, SOL, BNB. Rise: 0.01–1000%; fall: 0.01–99.99%. Up to two decimal places.",
		"stale":             "No fresh valid price is available. Alert not created; try again after quotes update.",
		"limit":             "Limit: 10 active alerts or queued deliveries. Cancel unwanted alerts with /alerts.",
		"duplicate":         "This active alert already exists, or this command has already been processed.",
		"disabled":          "New alert creation and evaluation are disabled. Previously queued messages may still be delivered; you can cancel them in the list.",
		"empty":             "No alerts.", "canceled": "Alert canceled.", "not_found": "No active alert in this chat. Messages already sent cannot be recalled.",
		"independent": "Price alerts are independent of regular updates. View and cancel them with /alerts.",
		"active":      "active", "triggered": "triggered", "pending": "awaiting delivery", "sending": "sending", "sent": "delivered", "failed": "delivery failed", "canceled_state": "canceled",
	},
	"ru": {
		"menu": "*Ценовые алерты*\nВыберите монету. Процент считается от цены при создании. Алерт одноразовый.\nСвой процент: `/alerts BTC +7.5` или `/alerts ETH -12`.",
		"coin": "*%s: выберите изменение цены*",
		"up":   "Рост на %s%%", "down": "Падение на %s%%",
		"list": "Алерты чата", "back": "Монеты", "cancel": "Отменить #%d",
		"created":           "*Алерт #%d создан: %s %+.2f%%*\nБазовая цена: %.8g USDT\nПорог: %.8g USDT\nДанные: %s (Киев).\nПорог фиксирован; проверка по полученным котировкам, не по каждой биржевой сделке.",
		"triggered_message": "*Ценовой алерт: %s %+.2f%%*\nБазовая цена: %.8g USDT\nПорог: %.8g USDT\nЦена при срабатывании: %.8g USDT\nДанные: %s (Киев).\nАлерт одноразовый. Новый: /alerts",
		"invalid":           "Формат: `/alerts BTC +7.5`. Монеты: BTC, ETH, SOL, BNB. Рост: 0.01–1000%; падение: 0.01–99.99%. До двух знаков после точки.",
		"stale":             "Нет свежей корректной цены. Алерт не создан; попробуйте после обновления котировок.",
		"limit":             "Лимит: 10 активных алертов или сообщений в очереди. Отмените лишние через /alerts.",
		"duplicate":         "Такой активный алерт уже существует, или эта команда уже обработана.",
		"disabled":          "Создание и проверка новых алертов выключены. Сообщения из очереди могут ещё доставляться; их можно отменить в списке.",
		"empty":             "Алертов нет.", "canceled": "Алерт отменён.", "not_found": "Активного алерта в этом чате нет. Уже отправленное сообщение не отзывается.",
		"independent": "Ценовые алерты независимы от регулярной рассылки. Просмотр и отмена: /alerts.",
		"active":      "активный", "triggered": "сработал", "pending": "ожидает доставки", "sending": "доставляется", "sent": "доставлено", "failed": "не доставлено", "canceled_state": "отменён",
	},
}

func AlertText(lang, key string) string {
	if text := alertMessages[lang][key]; text != "" {
		return text
	}
	return alertMessages["ua"][key]
}

func AlertsKeyboard(lang string) tgbotapi.InlineKeyboardMarkup {
	return tgbotapi.NewInlineKeyboardMarkup(
		tgbotapi.NewInlineKeyboardRow(tgbotapi.NewInlineKeyboardButtonData("BTC", "alerts_coin_BTC"), tgbotapi.NewInlineKeyboardButtonData("ETH", "alerts_coin_ETH")),
		tgbotapi.NewInlineKeyboardRow(tgbotapi.NewInlineKeyboardButtonData("SOL", "alerts_coin_SOL"), tgbotapi.NewInlineKeyboardButtonData("BNB", "alerts_coin_BNB")),
		tgbotapi.NewInlineKeyboardRow(tgbotapi.NewInlineKeyboardButtonData(AlertText(lang, "list"), "alerts_list")),
	)
}

func AlertPercentKeyboard(lang, coin string) tgbotapi.InlineKeyboardMarkup {
	var rows [][]tgbotapi.InlineKeyboardButton
	for _, percent := range []string{"5", "10", "20"} {
		rows = append(rows, tgbotapi.NewInlineKeyboardRow(
			tgbotapi.NewInlineKeyboardButtonData(fmt.Sprintf(AlertText(lang, "up"), percent), "alerts_add_"+coin+"_+"+percent),
			tgbotapi.NewInlineKeyboardButtonData(fmt.Sprintf(AlertText(lang, "down"), percent), "alerts_add_"+coin+"_-"+percent),
		))
	}
	rows = append(rows, tgbotapi.NewInlineKeyboardRow(tgbotapi.NewInlineKeyboardButtonData(AlertText(lang, "back"), "alerts_menu")))
	return tgbotapi.NewInlineKeyboardMarkup(rows...)
}
