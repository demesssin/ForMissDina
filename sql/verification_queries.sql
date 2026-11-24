-- Q1: total number of stored rows
SELECT COUNT(*) AS total_rows FROM klines;

-- Q2: rows per symbol with first and last candle time
SELECT symbol, COUNT(*) AS candles, MIN(open_time) AS first_candle, MAX(open_time) AS last_candle
FROM klines GROUP BY symbol ORDER BY symbol;

-- Q3: duplicate check on the primary key (must return 0 rows)
SELECT symbol, open_time, COUNT(*) AS copies
FROM klines GROUP BY symbol, open_time HAVING COUNT(*) > 1;

-- Q4: average hourly volatility and trading intensity per symbol
SELECT symbol, ROUND(AVG(price_range_pct), 3) AS avg_range_pct,
       ROUND(AVG(quote_volume), 0) AS avg_quote_volume, ROUND(AVG(trades_count), 0) AS avg_trades
FROM klines GROUP BY symbol ORDER BY avg_range_pct DESC;

-- Q5: top 5 candles by traded value (USDT)
SELECT symbol, open_time, ROUND(quote_volume, 0) AS quote_volume, ROUND(price_range_pct, 3) AS range_pct
FROM klines ORDER BY quote_volume DESC LIMIT 5;

-- Q6: integrity check - invalid prices or inconsistent OHLC (must return 0 rows)
SELECT COUNT(*) AS bad_rows FROM klines
WHERE high < low OR high < open OR high < close OR low > open OR low > close OR open <= 0;

-- Q7: distribution of volatility levels and trends
SELECT volatility_level, price_trend, COUNT(*) AS candles
FROM klines GROUP BY volatility_level, price_trend ORDER BY volatility_level, price_trend;
