-- +goose Up
ALTER TABLE telegram_updates ADD COLUMN stream_epoch BIGINT NOT NULL DEFAULT 0 CHECK (stream_epoch >= 0);

CREATE TABLE telegram_update_stream (
    singleton BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton),
    epoch BIGINT NOT NULL DEFAULT 0 CHECK (epoch >= 0),
    last_received_at TIMESTAMPTZ NOT NULL
);
INSERT INTO telegram_update_stream (last_received_at)
SELECT COALESCE(MAX(created_at), clock_timestamp()) FROM telegram_updates;

CREATE TABLE telegram_mutation_versions (
    chat_id BIGINT NOT NULL,
    setting TEXT NOT NULL CHECK (setting IN ('subscription', 'interval', 'language')),
    stream_epoch BIGINT NOT NULL CHECK (stream_epoch >= 0),
    update_id BIGINT NOT NULL,
    PRIMARY KEY (chat_id, setting)
);

-- Відновлюємо доступні версії вже виконаних команд перед першим запуском нового коду.
INSERT INTO telegram_mutation_versions (chat_id, setting, stream_epoch, update_id)
SELECT chat_id, setting, 0, MAX(update_id)
FROM (
    SELECT chat_id, update_id,
        CASE
            WHEN payload->'message'->>'text' ~ '^/(subscribe|unsubscribe)(@[A-Za-z0-9_]+)?([[:space:]]|$)'
                THEN 'subscription'
            WHEN payload->'callback_query'->>'data' IN ('setlang_ua', 'setlang_en', 'setlang_ru')
                THEN 'language'
            WHEN payload->'callback_query'->>'data' ~ '^int_[0-9]{1,4}$'
                THEN CASE WHEN substring(payload->'callback_query'->>'data' FROM 5)::integer BETWEEN 1 AND 1440
                    THEN 'interval' END
        END AS setting
    FROM telegram_updates WHERE status = 'processed'
) AS mutations
WHERE setting IS NOT NULL GROUP BY chat_id, setting;

CREATE INDEX telegram_updates_chat_epoch_pending_idx
    ON telegram_updates (chat_id, stream_epoch, update_id)
    WHERE status IN ('pending', 'processing');

-- +goose Down
-- +goose StatementBegin
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM telegram_mutation_versions)
       OR EXISTS (SELECT 1 FROM telegram_updates WHERE stream_epoch > 0) THEN
        RAISE EXCEPTION 'Cannot discard Telegram command ordering state';
    END IF;
END $$;
-- +goose StatementEnd
DROP INDEX telegram_updates_chat_epoch_pending_idx;
DROP TABLE telegram_mutation_versions;
DROP TABLE telegram_update_stream;
ALTER TABLE telegram_updates DROP COLUMN stream_epoch;
