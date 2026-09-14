-- +goose Up
CREATE TABLE price_alerts (
    id BIGSERIAL PRIMARY KEY,
    chat_id BIGINT NOT NULL,
    source_update_id BIGINT NOT NULL UNIQUE,
    symbol TEXT NOT NULL CHECK (symbol IN ('BTCUSDT', 'ETHUSDT', 'SOLUSDT', 'BNBUSDT')),
    language_code TEXT NOT NULL CHECK (language_code IN ('ua', 'en', 'ru')),
    change_percent NUMERIC(7,2) NOT NULL CHECK (
        change_percent <> 0 AND change_percent >= -99.99 AND change_percent <= 1000
    ),
    base_price DOUBLE PRECISION NOT NULL CHECK (base_price > 0 AND base_price < 'Infinity'::float8),
    base_price_at TIMESTAMPTZ NOT NULL,
    target_price DOUBLE PRECISION NOT NULL CHECK (target_price > 0 AND target_price < 'Infinity'::float8),
    status TEXT NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'triggered', 'canceled')),
    trigger_price DOUBLE PRECISION CHECK (trigger_price > 0 AND trigger_price < 'Infinity'::float8),
    triggered_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CHECK ((change_percent > 0 AND target_price > base_price) OR
           (change_percent < 0 AND target_price < base_price)),
    CHECK (status <> 'triggered' OR (trigger_price IS NOT NULL AND triggered_at IS NOT NULL))
);

CREATE INDEX price_alerts_active_symbol ON price_alerts (symbol, target_price, id) WHERE status = 'active';
CREATE INDEX price_alerts_chat ON price_alerts (chat_id, id);
CREATE INDEX price_alerts_retention ON price_alerts (updated_at, id) WHERE status IN ('triggered', 'canceled');
CREATE UNIQUE INDEX price_alerts_one_active_threshold
    ON price_alerts (chat_id, symbol, change_percent) WHERE status = 'active';

ALTER TABLE notification_jobs
    ADD COLUMN kind TEXT NOT NULL DEFAULT 'scheduled' CHECK (kind IN ('scheduled', 'price_alert')),
    ADD COLUMN price_alert_id BIGINT REFERENCES price_alerts(id),
    ADD CONSTRAINT notification_jobs_kind_reference CHECK (
        (kind = 'scheduled' AND price_alert_id IS NULL) OR
        (kind = 'price_alert' AND price_alert_id IS NOT NULL)
    );

DROP INDEX notification_jobs_one_active_per_chat;
CREATE UNIQUE INDEX notification_jobs_one_active_per_chat
    ON notification_jobs (chat_id) WHERE kind = 'scheduled' AND status IN ('pending', 'sending');
CREATE UNIQUE INDEX notification_jobs_one_per_price_alert
    ON notification_jobs (price_alert_id) WHERE price_alert_id IS NOT NULL;

-- +goose Down
-- Відкат дозволений лише після явного видалення історії нової функції оператором.
-- +goose StatementBegin
DO $$ BEGIN
    IF EXISTS (SELECT 1 FROM price_alerts) OR EXISTS (SELECT 1 FROM notification_jobs WHERE kind <> 'scheduled') THEN
        RAISE EXCEPTION 'price alert data exists; disable alerts and review rollback before removing schema';
    END IF;
END $$;
-- +goose StatementEnd
DROP INDEX notification_jobs_one_active_per_chat;
CREATE UNIQUE INDEX notification_jobs_one_active_per_chat
    ON notification_jobs (chat_id) WHERE status IN ('pending', 'sending');
ALTER TABLE notification_jobs DROP CONSTRAINT notification_jobs_kind_reference,
    DROP COLUMN price_alert_id, DROP COLUMN kind;
DROP TABLE price_alerts;
