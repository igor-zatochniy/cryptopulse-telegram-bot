-- +goose Up
-- +goose StatementBegin
CREATE TABLE IF NOT EXISTS telegram_replies (
    id BIGSERIAL PRIMARY KEY,
    source_update_id BIGINT NOT NULL,
    sequence_no INTEGER NOT NULL,
    chat_id BIGINT NOT NULL,
    operation TEXT NOT NULL,
    message_id BIGINT NOT NULL DEFAULT 0,
    message_text TEXT NOT NULL,
    reply_markup JSONB,
    status TEXT NOT NULL DEFAULT 'pending',
    attempts INTEGER NOT NULL DEFAULT 0,
    claim_token UUID,
    claimed_until TIMESTAMPTZ,
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    sent_at TIMESTAMPTZ,
    failed_at TIMESTAMPTZ,
    last_error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT telegram_replies_source_sequence_unique
        UNIQUE (source_update_id, sequence_no),
    CONSTRAINT telegram_replies_operation_check
        CHECK (operation IN ('send_message', 'edit_message')),
    CONSTRAINT telegram_replies_message_id_check
        CHECK (
            (operation = 'send_message' AND message_id = 0)
            OR (operation = 'edit_message' AND message_id > 0)
        ),
    CONSTRAINT telegram_replies_status_check
        CHECK (status IN ('pending', 'sending', 'sent', 'failed')),
    CONSTRAINT telegram_replies_attempts_check
        CHECK (attempts >= 0),
    CONSTRAINT telegram_replies_sequence_check
        CHECK (sequence_no >= 0)
);

CREATE INDEX IF NOT EXISTS idx_telegram_replies_pending
ON telegram_replies (next_attempt_at, claimed_until, id)
WHERE status IN ('pending', 'sending');

CREATE INDEX IF NOT EXISTS idx_telegram_replies_chat_open
ON telegram_replies (chat_id, id)
WHERE status IN ('pending', 'sending');

CREATE INDEX IF NOT EXISTS idx_telegram_replies_sent_retention
ON telegram_replies (sent_at)
WHERE status = 'sent' AND sent_at IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_telegram_replies_failed_retention
ON telegram_replies (failed_at)
WHERE status = 'failed' AND failed_at IS NOT NULL;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP INDEX IF EXISTS idx_telegram_replies_failed_retention;
DROP INDEX IF EXISTS idx_telegram_replies_sent_retention;
DROP INDEX IF EXISTS idx_telegram_replies_chat_open;
DROP INDEX IF EXISTS idx_telegram_replies_pending;
DROP TABLE IF EXISTS telegram_replies;
-- +goose StatementEnd
