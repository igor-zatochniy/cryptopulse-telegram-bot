-- +goose Up
-- +goose StatementBegin
DELETE FROM market_prices
WHERE NOT (
    price > 0
    AND price < 'Infinity'::double precision
    AND price <> 'NaN'::double precision
);

ALTER TABLE market_prices
    DROP CONSTRAINT IF EXISTS market_prices_price_check;

ALTER TABLE market_prices
    ADD CONSTRAINT market_prices_price_check
    CHECK (
        price > 0
        AND price < 'Infinity'::double precision
        AND price <> 'NaN'::double precision
    );
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
ALTER TABLE market_prices
    DROP CONSTRAINT IF EXISTS market_prices_price_check;

ALTER TABLE market_prices
    ADD CONSTRAINT market_prices_price_check
    CHECK (price >= 0);
-- +goose StatementEnd
