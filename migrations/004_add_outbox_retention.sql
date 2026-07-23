-- +goose Up
-- +goose StatementBegin
CREATE INDEX IF NOT EXISTS idx_notification_jobs_sent_retention
ON notification_jobs (sent_at)
WHERE status = 'sent' AND sent_at IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_notification_jobs_failed_retention
ON notification_jobs (failed_at)
WHERE status = 'failed' AND failed_at IS NOT NULL;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP INDEX IF EXISTS idx_notification_jobs_failed_retention;
DROP INDEX IF EXISTS idx_notification_jobs_sent_retention;
-- +goose StatementEnd
