package telegram

import "testing"

func TestSubscriptionMessagesDoNotPromiseFixedInterval(t *testing.T) {
	want := map[string]string{
		"ua": "✅ Підписка активована! Змінити частоту: /interval",
		"en": "✅ Subscription activated! Change frequency: /interval",
		"ru": "✅ Подписка активирована! Изменить частоту: /interval",
	}

	for language, expected := range want {
		if got := Text(language, "subscribe"); got != expected {
			t.Errorf("subscription message for %s = %q, want %q", language, got, expected)
		}
	}
}

func TestDynamicsMessagesDescribePreviousSuccessfulUpdate(t *testing.T) {
	want := map[string]string{
		"ua": "Зміна від попереднього успішного оновлення",
		"en": "Change since the previous successful update",
		"ru": "Изменение с предыдущего успешного обновления",
	}

	for language, expected := range want {
		if got := Text(language, "dynamics"); got != expected {
			t.Errorf("dynamics message for %s = %q, want %q", language, got, expected)
		}
	}
}
