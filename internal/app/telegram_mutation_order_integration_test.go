//go:build integration

package app

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"

	"github.com/igor-zatochniy/cryptopulse-telegram-bot/internal/storage"
	"github.com/igor-zatochniy/cryptopulse-telegram-bot/migrations"
	"github.com/pressly/goose/v3"
)

func processOrderedUpdateForTest(t *testing.T, a *App, update tgbotapi.Update) {
	t.Helper()
	saveIntegrationTelegramUpdate(t, a, update)
	job, err := a.claimPendingTelegramUpdateForWorker(context.Background(), 0, 1)
	if err != nil || job == nil {
		t.Fatalf("claim: %+v, %v", job, err)
	}
	a.processTelegramUpdateJob(context.Background(), *job)
	assertTelegramUpdateStatus(t, a.db, int64(update.UpdateID), "processed")
}

func orderedCallback(id int, chatID int64, data string) tgbotapi.Update {
	update := languageCallbackUpdate(chatID, "ordered-callback", data)
	update.UpdateID = id
	return update
}

func TestIntegrationLateSubscribeCannotReverseUnsubscribe(t *testing.T) {
	db := setupIntegrationDB(t)
	a := newIntegrationApp(t, db, newFakeTelegramBot(t, nil))
	insertSubscriber(t, db, 9910, true, 5, "ua", time.Now())
	processOrderedUpdateForTest(t, a, commandUpdateWithID(991002, 9910, "/unsubscribe"))
	other := newIntegrationApp(t, db, a.bot)
	processOrderedUpdateForTest(t, other, commandUpdateWithID(991001, 9910, "/subscribe"))
	assertSubscribed(t, db, 9910, false)
	assertTelegramReplyCount(t, db, 991001, 0)
	assertSQLCount(t, db, 1, `SELECT COUNT(*) FROM telegram_mutation_versions WHERE setting = 'subscription' AND update_id = 991002`)
}

func TestIntegrationLateSettingsAreIndependentAndVersioned(t *testing.T) {
	db := setupIntegrationDB(t)
	a := newIntegrationApp(t, db, newFakeTelegramBot(t, nil))
	processOrderedUpdateForTest(t, a, commandUpdateWithID(100, 9950, "/subscribe"))
	processOrderedUpdateForTest(t, a, orderedCallback(120, 9950, "int_15"))
	processOrderedUpdateForTest(t, a, orderedCallback(110, 9950, "int_5"))
	processOrderedUpdateForTest(t, a, orderedCallback(115, 9950, "setlang_en"))
	processOrderedUpdateForTest(t, a, orderedCallback(105, 9950, "setlang_ru"))
	assertSQLCount(t, db, 1, `SELECT COUNT(*) FROM subscribers WHERE chat_id = 9950 AND interval_minutes = 15 AND language_code = 'en'`)
	assertTelegramReplyCount(t, db, 110, 0)
	assertTelegramReplyCount(t, db, 105, 0)
	processOrderedUpdateForTest(t, a, commandUpdateWithID(140, 9950, "/unsubscribe"))
	processOrderedUpdateForTest(t, a, commandUpdateWithID(150, 9950, "/subscribe"))
	processOrderedUpdateForTest(t, a, orderedCallback(130, 9950, "int_30"))
	assertSQLCount(t, db, 1, `SELECT COUNT(*) FROM subscribers WHERE chat_id = 9950 AND is_subscribed AND interval_minutes = 15`)
	assertTelegramReplyCount(t, db, 130, 0)
}

func TestIntegrationNewTelegramEpochAcceptsLowerIDs(t *testing.T) {
	db := setupIntegrationDB(t)
	a := newIntegrationApp(t, db, newFakeTelegramBot(t, nil))
	processOrderedUpdateForTest(t, a, commandUpdateWithID(996002, 9960, "/subscribe"))
	requireSQL(t, db, `UPDATE telegram_update_stream SET last_received_at = clock_timestamp() - INTERVAL '8 days'`)
	other := newIntegrationApp(t, db, a.bot)
	processOrderedUpdateForTest(t, other, commandUpdateWithID(42, 9960, "/unsubscribe"))
	assertSubscribed(t, db, 9960, false)
	assertSQLCount(t, db, 1, `SELECT COUNT(*) FROM telegram_mutation_versions WHERE chat_id = 9960 AND stream_epoch = 1 AND update_id = 42`)
	processOrderedUpdateForTest(t, other, commandUpdateWithID(43, 9960, "/subscribe"))
	assertSubscribed(t, db, 9960, true)
	update := commandUpdateWithID(996002, 9960, "/subscribe")
	payload, err := json.Marshal(update)
	if err != nil {
		t.Fatal(err)
	}
	if inserted, err := other.saveTelegramUpdate(context.Background(), update, payload); err != nil || inserted {
		t.Fatalf("duplicate: %v, %v", inserted, err)
	}
	assertSQLCount(t, db, 1, `SELECT COUNT(*) FROM telegram_update_stream WHERE epoch = 1`)
}

func TestIntegrationMutationVersionRollsBackWithReply(t *testing.T) {
	db := setupIntegrationDB(t)
	a := newIntegrationApp(t, db, newFakeTelegramBot(t, nil))
	requireSQL(t, db, `ALTER TABLE telegram_replies ADD CONSTRAINT reject_versioned_reply CHECK (source_update_id <> 997001)`)
	saveIntegrationTelegramUpdate(t, a, commandUpdateWithID(997001, 9970, "/subscribe"))
	job, err := a.claimPendingTelegramUpdateForWorker(context.Background(), 0, 1)
	if err != nil || job == nil {
		t.Fatalf("claim: %v, %v", job, err)
	}
	a.processTelegramUpdateJob(context.Background(), *job)
	assertNoSubscriberRow(t, db, 9970)
	assertSQLCount(t, db, 0, `SELECT COUNT(*) FROM telegram_mutation_versions`)
	assertTelegramUpdateStatus(t, db, 997001, "pending")
	requireSQL(t, db, `ALTER TABLE telegram_replies DROP CONSTRAINT reject_versioned_reply`)
	requireSQL(t, db, `UPDATE telegram_updates SET next_attempt_at = NOW()`)
	job, err = a.claimPendingTelegramUpdateForWorker(context.Background(), 0, 1)
	if err != nil || job == nil {
		t.Fatalf("retry claim: %v, %v", job, err)
	}
	a.processTelegramUpdateJob(context.Background(), *job)
	assertSubscribed(t, db, 9970, true)
	assertSQLCount(t, db, 1, `SELECT COUNT(*) FROM telegram_mutation_versions`)
}

func TestIntegrationMutationVersionMigrationBackfillsHistory(t *testing.T) {
	db := setupIntegrationDB(t)
	ctx := context.Background()
	provider, err := goose.NewProvider(goose.DialectPostgres, db, migrations.Files)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := provider.DownTo(ctx, 10); err != nil {
		t.Fatal(err)
	}
	update := commandUpdateWithID(998002, 9980, "/unsubscribe")
	payload, err := json.Marshal(update)
	if err != nil {
		t.Fatal(err)
	}
	requireSQL(t, db, `INSERT INTO telegram_updates (update_id, chat_id, shard_id, payload, status, processed_at)
		VALUES (998002, 9980, 0, $1, 'processed', NOW())`, string(payload))
	if _, err := storage.ApplyMigrations(ctx, db); err != nil {
		t.Fatal(err)
	}
	a := newIntegrationApp(t, db, newFakeTelegramBot(t, nil))
	processOrderedUpdateForTest(t, a, commandUpdateWithID(998001, 9980, "/subscribe"))
	assertNoSubscriberRow(t, db, 9980)
	assertTelegramReplyCount(t, db, 998001, 0)
	if err := storage.VerifySchema(ctx, db); err != nil {
		t.Fatal(err)
	}
}
