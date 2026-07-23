-- +goose Up
-- +goose StatementBegin
CREATE TABLE IF NOT EXISTS subscribers (
    chat_id BIGINT,
    interval_minutes INTEGER,
    last_sent TIMESTAMPTZ,
    language_code TEXT,
    is_subscribed BOOLEAN,
    cron_claimed_until TIMESTAMPTZ
);

ALTER TABLE subscribers
    ADD COLUMN IF NOT EXISTS chat_id BIGINT,
    ADD COLUMN IF NOT EXISTS interval_minutes INTEGER,
    ADD COLUMN IF NOT EXISTS last_sent TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS language_code TEXT,
    ADD COLUMN IF NOT EXISTS is_subscribed BOOLEAN,
    ADD COLUMN IF NOT EXISTS cron_claimed_until TIMESTAMPTZ;

UPDATE subscribers
SET
    interval_minutes = CASE
        WHEN interval_minutes IS NULL OR interval_minutes < 1 OR interval_minutes > 1440 THEN 60
        ELSE interval_minutes
    END,
    last_sent = COALESCE(last_sent, NOW() - INTERVAL '2 minute'),
    language_code = CASE
        WHEN language_code IN ('ua', 'en', 'ru') THEN language_code
        ELSE 'ua'
    END,
    is_subscribed = COALESCE(is_subscribed, FALSE);

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM subscribers WHERE chat_id IS NULL) THEN
        RAISE EXCEPTION 'subscribers.chat_id contains NULL values; fix data before applying constraints';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM subscribers
        GROUP BY chat_id
        HAVING COUNT(*) > 1
    ) THEN
        RAISE EXCEPTION 'subscribers.chat_id contains duplicate values; fix data before applying unique index';
    END IF;
END $$;

ALTER TABLE subscribers
    ALTER COLUMN interval_minutes SET DEFAULT 60,
    ALTER COLUMN interval_minutes SET NOT NULL,
    ALTER COLUMN last_sent SET DEFAULT (NOW() - INTERVAL '2 minute'),
    ALTER COLUMN last_sent SET NOT NULL,
    ALTER COLUMN language_code SET DEFAULT 'ua',
    ALTER COLUMN language_code SET NOT NULL,
    ALTER COLUMN is_subscribed SET DEFAULT FALSE,
    ALTER COLUMN is_subscribed SET NOT NULL,
    ALTER COLUMN chat_id SET NOT NULL;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'subscribers'::regclass
        AND conname = 'subscribers_interval_minutes_check'
    ) THEN
        ALTER TABLE subscribers
            ADD CONSTRAINT subscribers_interval_minutes_check
            CHECK (interval_minutes BETWEEN 1 AND 1440);
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'subscribers'::regclass
        AND conname = 'subscribers_language_code_check'
    ) THEN
        ALTER TABLE subscribers
            ADD CONSTRAINT subscribers_language_code_check
            CHECK (language_code IN ('ua', 'en', 'ru'));
    END IF;
END $$;

CREATE UNIQUE INDEX IF NOT EXISTS subscribers_chat_id_uidx
ON subscribers (chat_id);

CREATE INDEX IF NOT EXISTS idx_subscribers_cron_due
ON subscribers (last_sent ASC NULLS FIRST, cron_claimed_until)
INCLUDE (chat_id, language_code, interval_minutes)
WHERE is_subscribed = TRUE;

CREATE TABLE IF NOT EXISTS market_prices (
    symbol TEXT,
    price DOUBLE PRECISION,
    updated_at TIMESTAMPTZ
);

ALTER TABLE market_prices
    ADD COLUMN IF NOT EXISTS symbol TEXT,
    ADD COLUMN IF NOT EXISTS price DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ;

UPDATE market_prices
SET updated_at = COALESCE(updated_at, NOW());

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM market_prices WHERE symbol IS NULL OR symbol = '') THEN
        RAISE EXCEPTION 'market_prices.symbol contains NULL or empty values; fix data before applying constraints';
    END IF;

    IF EXISTS (SELECT 1 FROM market_prices WHERE price IS NULL OR price < 0) THEN
        RAISE EXCEPTION 'market_prices.price contains NULL or negative values; fix data before applying constraints';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM market_prices
        GROUP BY symbol
        HAVING COUNT(*) > 1
    ) THEN
        RAISE EXCEPTION 'market_prices.symbol contains duplicate values; fix data before applying unique index';
    END IF;
END $$;

ALTER TABLE market_prices
    ALTER COLUMN symbol SET NOT NULL,
    ALTER COLUMN price SET NOT NULL,
    ALTER COLUMN updated_at SET DEFAULT NOW(),
    ALTER COLUMN updated_at SET NOT NULL;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'market_prices'::regclass
        AND conname = 'market_prices_price_check'
    ) THEN
        ALTER TABLE market_prices
            ADD CONSTRAINT market_prices_price_check
            CHECK (price >= 0);
    END IF;
END $$;

CREATE UNIQUE INDEX IF NOT EXISTS market_prices_symbol_uidx
ON market_prices (symbol);
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP INDEX IF EXISTS market_prices_symbol_uidx;
DROP TABLE IF EXISTS market_prices;
DROP INDEX IF EXISTS idx_subscribers_cron_due;
DROP INDEX IF EXISTS subscribers_chat_id_uidx;
DROP TABLE IF EXISTS subscribers;
-- +goose StatementEnd
