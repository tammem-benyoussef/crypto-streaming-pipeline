-- One row per symbol per minute. The primary key makes writes idempotent:
-- writing the same window twice updates the row instead of duplicating it.
CREATE TABLE IF NOT EXISTS live.bars_1m (
    symbol              TEXT             NOT NULL,
    window_start        TIMESTAMPTZ      NOT NULL,
    window_end          TIMESTAMPTZ      NOT NULL,
    trade_count         INTEGER          NOT NULL,
    base_volume         DOUBLE PRECISION NOT NULL,
    quote_volume        DOUBLE PRECISION NOT NULL,
    vwap                DOUBLE PRECISION,
    open                DOUBLE PRECISION,
    high                DOUBLE PRECISION,
    low                 DOUBLE PRECISION,
    close               DOUBLE PRECISION,
    rolling_volatility  DOUBLE PRECISION,
    PRIMARY KEY (symbol, window_start)
);

