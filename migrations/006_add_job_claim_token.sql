-- +goose Up
-- +goose StatementBegin
CREATE EXTENSION IF NOT EXISTS pgcrypto;

ALTER TABLE notification_jobs
    ADD COLUMN IF NOT EXISTS claim_token UUID;

UPDATE notification_jobs
SET claim_token = NULL
WHERE status <> 'sending';

CREATE UNIQUE INDEX IF NOT EXISTS notification_jobs_one_active_per_chat
ON notification_jobs (chat_id)
WHERE status IN ('pending', 'sending');
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP INDEX IF EXISTS notification_jobs_one_active_per_chat;

ALTER TABLE notification_jobs
    DROP COLUMN IF EXISTS claim_token;
-- +goose StatementEnd
