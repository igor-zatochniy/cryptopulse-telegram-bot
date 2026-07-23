-- +goose Up
-- +goose StatementBegin
CREATE TABLE IF NOT EXISTS notification_jobs (
    id BIGSERIAL PRIMARY KEY,
    chat_id BIGINT NOT NULL,
    language_code TEXT NOT NULL DEFAULT 'ua',
    message_text TEXT NOT NULL,
    scheduled_at TIMESTAMPTZ NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    attempts INTEGER NOT NULL DEFAULT 0,
    claimed_until TIMESTAMPTZ,
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    sent_at TIMESTAMPTZ,
    failed_at TIMESTAMPTZ,
    last_error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT notification_jobs_status_check
        CHECK (status IN ('pending', 'sending', 'sent', 'failed')),
    CONSTRAINT notification_jobs_attempts_check
        CHECK (attempts >= 0)
);

CREATE INDEX IF NOT EXISTS idx_notification_jobs_pending
ON notification_jobs (next_attempt_at, claimed_until, scheduled_at, id)
WHERE status IN ('pending', 'sending');

CREATE INDEX IF NOT EXISTS idx_notification_jobs_chat_status
ON notification_jobs (chat_id, status);
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP INDEX IF EXISTS idx_notification_jobs_chat_status;
DROP INDEX IF EXISTS idx_notification_jobs_pending;
DROP TABLE IF EXISTS notification_jobs;
-- +goose StatementEnd
