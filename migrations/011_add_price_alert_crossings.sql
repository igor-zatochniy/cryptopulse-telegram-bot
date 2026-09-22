-- +goose Up
CREATE TABLE price_alert_crossings (
    alert_id BIGINT PRIMARY KEY REFERENCES price_alerts(id) ON DELETE CASCADE,
    price DOUBLE PRECISION NOT NULL CHECK (price > 0 AND price < 'Infinity'::double precision),
    observed_at TIMESTAMPTZ NOT NULL
);

-- +goose Down
-- +goose StatementBegin
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM price_alert_crossings) THEN
        RAISE EXCEPTION 'Cannot remove unprocessed price alert crossings';
    END IF;
END $$;
-- +goose StatementEnd
DROP TABLE price_alert_crossings;
