"""Stage 5: load cleaned data into SQLite (idempotent).

Idempotency: PRIMARY KEY (symbol, open_time) + 'INSERT ... ON CONFLICT DO NOTHING'.
Loading the same dataset twice inserts 0 new rows the second time and changes nothing.

Usage: python -m src.loader           # loads twice and proves idempotency
       python -m src.loader --once    # loads once
"""
import argparse
import logging
import sqlite3
import sys

import numpy as np
import pandas as pd

from src import config

log = logging.getLogger("loader")

LOAD_COLUMNS = ["symbol", "base_asset", "quote_asset", "interval", "open_time", "close_time",
                "open", "high", "low", "close", "volume", "quote_volume", "trades_count",
                "log_return", "price_range_pct", "body_pct", "price_trend", "volatility_level",
                "volume_zscore", "coin_group", "hour", "day_of_week"]

INSERT_SQL = (f"INSERT INTO klines ({', '.join(LOAD_COLUMNS)}) "
              f"VALUES ({', '.join('?' for _ in LOAD_COLUMNS)}) "
              f"ON CONFLICT(symbol, open_time) DO NOTHING")


def _py(value):
    """NaN -> None (SQL NULL); numpy scalars -> Python scalars (sqlite3 cannot store numpy types)."""
    if isinstance(value, np.generic):
        value = value.item()
    if isinstance(value, float) and np.isnan(value):
        return None
    return value


def init_db(conn: sqlite3.Connection) -> None:
    conn.executescript((config.SQL_DIR / "schema.sql").read_text(encoding="utf-8"))


def snapshot(conn: sqlite3.Connection) -> dict:
    """Row count + checksums, used to prove the stored data did not change."""
    count, sum_close, sum_volume, sum_quote = conn.execute(
        "SELECT COUNT(*), ROUND(SUM(close), 6), ROUND(SUM(volume), 6), ROUND(SUM(quote_volume), 6) "
        "FROM klines").fetchone()
    return {"rows": count, "sum_close": sum_close, "sum_volume": sum_volume, "sum_quote_volume": sum_quote}


def load_dataframe(conn: sqlite3.Connection, df: pd.DataFrame) -> int:
    """Insert rows; returns the number of rows actually inserted (conflicts are skipped)."""
    rows = [tuple(_py(v) for v in row) for row in df[LOAD_COLUMNS].itertuples(index=False, name=None)]
    changes_before = conn.total_changes
    with conn:                                           # one transaction: all or nothing
        conn.executemany(INSERT_SQL, rows)
    return conn.total_changes - changes_before


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser()
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args()

    if not config.CLEAN_CSV.exists():
        log.error("%s not found. Run the cleaner first.", config.CLEAN_CSV)
        return 1
    df = pd.read_csv(config.CLEAN_CSV)
    df["open_time"] = pd.to_datetime(df["open_time"]).dt.strftime("%Y-%m-%d %H:%M:%S")
    df["close_time"] = pd.to_datetime(df["close_time"]).dt.strftime("%Y-%m-%d %H:%M:%S")

    config.DATA_DIR.mkdir(exist_ok=True)
    runs = 1 if args.once else 2
    results = []
    try:
        conn = sqlite3.connect(config.DB_PATH)
        init_db(conn)
        for run in range(1, runs + 1):
            before = snapshot(conn)
            inserted = load_dataframe(conn, df)
            after = snapshot(conn)
            results.append({"inserted": inserted, "before": before, "after": after})
            print(f"Load #{run}: rows in file={len(df)}, inserted={inserted}, "
                  f"table rows {before['rows']} -> {after['rows']}, "
                  f"SUM(close)={after['sum_close']}, SUM(volume)={after['sum_volume']}")
        conn.close()
    except sqlite3.Error as exc:
        log.error("SQLite error: %s", exc)
        return 1

    stats = {"db_rows": results[-1]["after"]["rows"], "load1_inserted": results[0]["inserted"]}
    if runs == 2:
        identical = results[0]["after"] == results[1]["after"]
        stats.update(load2_inserted=results[1]["inserted"], idempotent=bool(identical))
        print("IDEMPOTENCY:", "PASSED - second load inserted 0 rows and checksums are identical"
              if identical and results[1]["inserted"] == 0 else "FAILED")
        if not (identical and results[1]["inserted"] == 0):
            config.update_stats(**stats)
            return 1
    config.update_stats(**stats)
    return 0


if __name__ == "__main__":
    sys.exit(main())
