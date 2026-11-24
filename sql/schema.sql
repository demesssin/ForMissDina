-- SQLite schema for processed hourly candles.
-- PRIMARY KEY (symbol, open_time) makes every candle unique -> repeated loads cannot create duplicates.
CREATE TABLE IF NOT EXISTS klines (
    symbol           TEXT    NOT NULL,
    base_asset       TEXT    NOT NULL,
    quote_asset      TEXT    NOT NULL,
    interval         TEXT    NOT NULL,
    open_time        TEXT    NOT NULL,          -- ISO datetime, UTC: YYYY-MM-DD HH:MM:SS
    close_time       TEXT    NOT NULL,
    open             REAL    NOT NULL CHECK (open  > 0),
    high             REAL    NOT NULL CHECK (high  > 0),
    low              REAL    NOT NULL CHECK (low   > 0),
    close            REAL    NOT NULL CHECK (close > 0),
    volume           REAL    NOT NULL CHECK (volume >= 0),
    quote_volume     REAL    NOT NULL CHECK (quote_volume >= 0),
    trades_count     INTEGER NOT NULL CHECK (trades_count >= 0),
    log_return       REAL,                      -- NULL for the first candle of each symbol
    price_range_pct  REAL,
    body_pct         REAL,
    price_trend      TEXT    CHECK (price_trend IN ('up', 'down', 'flat')),
    volatility_level TEXT    CHECK (volatility_level IN ('low', 'medium', 'high')),
    volume_zscore    REAL,
    coin_group       TEXT,
    hour             INTEGER CHECK (hour BETWEEN 0 AND 23),
    day_of_week      TEXT,
    PRIMARY KEY (symbol, open_time)
);
CREATE INDEX IF NOT EXISTS idx_klines_open_time ON klines (open_time);
