-- +goose Up
-- +goose StatementBegin
ALTER TABLE subscribers
    ADD COLUMN IF NOT EXISTS delivery_suspended_until TIMESTAMPTZ;

DROP INDEX IF EXISTS idx_subscribers_cron_due;

CREATE INDEX IF NOT EXISTS idx_subscribers_cron_due
ON subscribers (last_sent ASC NULLS FIRST, cron_claimed_until)
INCLUDE (chat_id, language_code, interval_minutes, delivery_suspended_until)
WHERE is_subscribed = TRUE;

CREATE INDEX IF NOT EXISTS idx_subscribers_delivery_suspended_until
ON subscribers (delivery_suspended_until)
WHERE is_subscribed = TRUE AND delivery_suspended_until IS NOT NULL;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP INDEX IF EXISTS idx_subscribers_delivery_suspended_until;
DROP INDEX IF EXISTS idx_subscribers_cron_due;

CREATE INDEX IF NOT EXISTS idx_subscribers_cron_due
ON subscribers (last_sent ASC NULLS FIRST, cron_claimed_until)
INCLUDE (chat_id, language_code, interval_minutes)
WHERE is_subscribed = TRUE;

ALTER TABLE subscribers
    DROP COLUMN IF EXISTS delivery_suspended_until;
-- +goose StatementEnd
