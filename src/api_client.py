
import logging
import sys
import time

import pandas as pd
import requests

from src import config

log = logging.getLogger("api_client")

KLINE_FIELDS = ["open_time", "open", "high", "low", "close", "volume", "close_time",
                "quote_volume", "trades_count", "taker_buy_base_volume", "taker_buy_quote_volume"]


def fetch_klines(symbol: str, session: requests.Session) -> list:
    """Request klines for one symbol with retries, rate-limit handling and mirror fallback."""
    last_error = "unknown error"
    params = {"symbol": symbol, "interval": config.INTERVAL, "limit": config.LIMIT + 1}
    for base_url in config.API_BASE_URLS:
        url = base_url + config.API_ENDPOINT
        for attempt in range(1, config.MAX_RETRIES + 1):
            try:
                response = session.get(url, params=params, timeout=config.REQUEST_TIMEOUT)
            except (requests.Timeout, requests.ConnectionError) as exc:
                last_error = f"{type(exc).__name__} on {base_url}"
                log.warning("%s attempt %d/%d: %s", symbol, attempt, config.MAX_RETRIES, last_error)
                time.sleep(2 * attempt)
                continue

            if response.status_code == 200:
                try:
                    payload = response.json()
                except ValueError:
                    last_error = f"invalid JSON from {base_url}"
                    break
                if isinstance(payload, list) and payload:
                    return payload
                last_error = f"empty/unexpected payload from {base_url}"
                break

            if response.status_code in (418, 429):          
                try:
                    wait = int(response.headers.get("Retry-After", 5))
                except ValueError:
                    wait = 5
                log.warning("%s: HTTP %d, waiting %ds", symbol, response.status_code, wait)
                time.sleep(min(wait, 60))
                last_error = f"HTTP {response.status_code} on {base_url}"
                continue

            if response.status_code >= 500:                 # server problem: retry
                last_error = f"HTTP {response.status_code} on {base_url}"
                log.warning("%s attempt %d/%d: %s", symbol, attempt, config.MAX_RETRIES, last_error)
                time.sleep(2 * attempt)
                continue

           
            last_error = f"HTTP {response.status_code} on {base_url}: {response.text[:150]}"
            log.warning("%s: %s", symbol, last_error)
            break
    raise RuntimeError(f"Could not fetch {symbol}: {last_error}")


def rows_to_records(symbol: str, rows: list, now_ms: int) -> list:
    """Turn list-of-lists into dicts, drop the unfinished candle, keep the last LIMIT candles."""
    records = []
    for row in rows:
        record = dict(zip(KLINE_FIELDS, row[:len(KLINE_FIELDS)]))
        if int(record["close_time"]) > now_ms:             
            continue
        record["symbol"] = symbol
        record["interval"] = config.INTERVAL
        records.append(record)
    return records[-config.LIMIT:]


def collect_all() -> pd.DataFrame:
    now_ms = int(time.time() * 1000)
    all_records, failed_symbols = [], []
    with requests.Session() as session:
        for symbol in config.SYMBOLS:
            try:
                rows = fetch_klines(symbol, session)
            except RuntimeError as exc:
                log.error(str(exc))
                failed_symbols.append(symbol)
                continue
            records = rows_to_records(symbol, rows, now_ms)
            log.info("%-9s -> %d records", symbol, len(records))
            all_records.extend(records)
            time.sleep(0.2)                                  
    if failed_symbols:
        log.warning("Failed symbols: %s", failed_symbols)
    columns = ["symbol", "interval"] + KLINE_FIELDS
    return pd.DataFrame(all_records, columns=columns)


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    df = collect_all()
    if len(df) < config.MIN_RECORDS:
        log.error("Collected only %d records (< %d). Check internet access / API availability.",
                  len(df), config.MIN_RECORDS)
        return 1
    config.DATA_DIR.mkdir(exist_ok=True)
    df.to_csv(config.RAW_CSV, index=False)                  
    config.update_stats(collected=int(len(df)))
    print(f"Collected: {len(df)} records -> {config.RAW_CSV}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
