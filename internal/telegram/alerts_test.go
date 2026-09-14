package telegram

import (
	"fmt"
	"strings"
	"testing"
)

func TestAlertTranslationsAndCallbackSizes(t *testing.T) {
	for lang, messages := range alertMessages {
		for key := range alertMessages["ua"] {
			if messages[key] == "" {
				t.Errorf("missing %s/%s", lang, key)
			}
		}
		for _, keyboard := range []string{"BTC", "ETH", "SOL", "BNB"} {
			for _, row := range AlertPercentKeyboard(lang, keyboard).InlineKeyboard {
				for _, button := range row {
					if button.CallbackData == nil || len(*button.CallbackData) > 64 || strings.Contains(button.Text, "%!") {
						t.Errorf("invalid %s button: %+v", lang, button)
					}
				}
			}
		}
		for _, text := range []string{
			fmt.Sprintf(AlertText(lang, "created"), 1, "BTC", 5.0, 100.0, 105.0, "14.09.2026 12:00:00"),
			fmt.Sprintf(AlertText(lang, "triggered_message"), "BTC", -5.0, 100.0, 95.0, 94.0, "14.09.2026 12:00:00"),
		} {
			if strings.Contains(text, "%!") || len(text) > 4096 {
				t.Errorf("invalid %s message: %s", lang, text)
			}
		}
	}
}
