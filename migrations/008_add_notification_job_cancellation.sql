-- +goose Up
-- +goose StatementBegin
ALTER TABLE notification_jobs
    ADD COLUMN IF NOT EXISTS canceled_at TIMESTAMPTZ;

ALTER TABLE notification_jobs
    DROP CONSTRAINT IF EXISTS notification_jobs_status_check;

ALTER TABLE notification_jobs
    ADD CONSTRAINT notification_jobs_status_check
    CHECK (status IN ('pending', 'sending', 'sent', 'failed', 'canceled'));

CREATE INDEX IF NOT EXISTS idx_notification_jobs_canceled_retention
ON notification_jobs (canceled_at)
WHERE status = 'canceled' AND canceled_at IS NOT NULL;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
UPDATE notification_jobs
SET
    status = 'failed',
    failed_at = COALESCE(failed_at, canceled_at, NOW()),
    last_error = COALESCE(last_error, 'notification job cancellation rolled back')
WHERE status = 'canceled';

DROP INDEX IF EXISTS idx_notification_jobs_canceled_retention;

ALTER TABLE notification_jobs
    DROP CONSTRAINT IF EXISTS notification_jobs_status_check;

ALTER TABLE notification_jobs
    ADD CONSTRAINT notification_jobs_status_check
    CHECK (status IN ('pending', 'sending', 'sent', 'failed'));

ALTER TABLE notification_jobs
    DROP COLUMN IF EXISTS canceled_at;
-- +goose StatementEnd
