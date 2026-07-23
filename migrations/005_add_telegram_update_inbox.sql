-- +goose Up
-- +goose StatementBegin
CREATE TABLE IF NOT EXISTS telegram_updates (
    update_id BIGINT PRIMARY KEY,
    chat_id BIGINT NOT NULL DEFAULT 0,
    shard_id INTEGER NOT NULL DEFAULT 0,
    payload JSONB NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    attempts INTEGER NOT NULL DEFAULT 0,
    claimed_until TIMESTAMPTZ,
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    processed_at TIMESTAMPTZ,
    failed_at TIMESTAMPTZ,
    last_error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT telegram_updates_status_check
        CHECK (status IN ('pending', 'processing', 'processed', 'failed')),
    CONSTRAINT telegram_updates_attempts_check
        CHECK (attempts >= 0),
    CONSTRAINT telegram_updates_shard_id_check
        CHECK (shard_id >= 0)
);

CREATE INDEX IF NOT EXISTS idx_telegram_updates_pending
ON telegram_updates (shard_id, next_attempt_at, claimed_until, update_id)
WHERE status IN ('pending', 'processing');

CREATE INDEX IF NOT EXISTS idx_telegram_updates_chat_open
ON telegram_updates (chat_id, update_id)
WHERE status IN ('pending', 'processing');

CREATE INDEX IF NOT EXISTS idx_telegram_updates_processed_retention
ON telegram_updates (processed_at)
WHERE status = 'processed' AND processed_at IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_telegram_updates_failed_retention
ON telegram_updates (failed_at)
WHERE status = 'failed' AND failed_at IS NOT NULL;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP INDEX IF EXISTS idx_telegram_updates_failed_retention;
DROP INDEX IF EXISTS idx_telegram_updates_processed_retention;
DROP INDEX IF EXISTS idx_telegram_updates_chat_open;
DROP INDEX IF EXISTS idx_telegram_updates_pending;
DROP TABLE IF EXISTS telegram_updates;
-- +goose StatementEnd
