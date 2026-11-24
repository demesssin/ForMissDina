"""Stage 3: data validation with Pandera.

Input : data/consumed_data.csv (what came out of Kafka)
Output: data/valid_data.csv     - rows that passed every HARD rule
        data/invalid_records.csv - quarantined rows + the list of broken rules

Handling strategy
  * HARD rule violation  -> row is quarantined (written to invalid_records.csv, not processed further).
  * Duplicate key (symbol, open_time) is a SOFT rule: the row is reported but passed on, because
    removing duplicates is the job of the cleaning stage (cleaner.py), where it is demonstrated.
  * Rows flagged is_test=True are ALWAYS excluded from the final data (safety net).

The 10 validation rules (see build_schema):
  1 open_time present and valid datetime       6 high >= max(open, close) and low <= min(open, close)
  2 symbol in allowed list                     7 close_time > open_time
  3 open/high/low/close > 0                    8 (symbol, open_time) unique   [soft]
  4 volume (and quote_volume) >= 0             9 interval == "1h"
  5 trades_count whole number >= 0            10 open_time not in the future

Usage: python -m src.validator [--self-test]
"""
import argparse
import logging
import sys
import time

import pandas as pd

try:                                   # newer Pandera versions
    import pandera.pandas as pa
except ImportError:                    # Pandera 0.20.x
    import pandera as pa
from pandera.errors import SchemaErrors

from src import config

log = logging.getLogger("validator")

UNIQUE_CHECK_NAME = "unique_symbol_open_time"
PRICE_COLUMNS = ["open", "high", "low", "close"]
NUMERIC_COLUMNS = PRICE_COLUMNS + ["volume", "quote_volume", "trades_count"]
REQUIRED_COLUMNS = ["symbol", "interval", "open_time", "close_time"] + NUMERIC_COLUMNS


def parse_types(raw: pd.DataFrame) -> pd.DataFrame:
    """Convert raw text/number columns to proper types. Unparseable values become NaN/NaT,
    which the schema then reports as 'not_nullable' violations (no crash, no silent fixing)."""
    missing = [c for c in REQUIRED_COLUMNS if c not in raw.columns]
    if missing:
        raise ValueError(f"Input data is missing required columns: {missing}")
    df = raw.copy().reset_index(drop=True)
    for col in ("open_time", "close_time"):
        millis = pd.to_numeric(df[col], errors="coerce")
        df[col] = pd.to_datetime(millis, unit="ms", errors="coerce").astype("datetime64[ns]")
    for col in NUMERIC_COLUMNS:
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("float64")
    if "is_test" in df.columns:
        df["is_test"] = df["is_test"].astype(str).str.lower().eq("true")
    else:
        df["is_test"] = False
    return df


def build_schema() -> pa.DataFrameSchema:
    not_in_future = pa.Check(
        lambda s: s <= pd.Timestamp.now(tz="UTC").tz_localize(None),
        name="not_in_future", error="open_time is in the future")
    is_text = pa.Check(lambda s: s.map(lambda v: isinstance(v, str)),
                       name="is_string", error="value is not text")
    whole_number = pa.Check(lambda s: s % 1 == 0,
                            name="whole_number", error="trades_count must be an integer")

    return pa.DataFrameSchema(
        columns={
            "symbol": pa.Column(checks=[is_text, pa.Check.isin(config.SYMBOLS)], nullable=False),
            "interval": pa.Column(checks=[pa.Check.isin([config.INTERVAL])], nullable=False),
            "open_time": pa.Column("datetime64[ns]", checks=[not_in_future], nullable=False),
            "close_time": pa.Column("datetime64[ns]", nullable=False),
            "open": pa.Column("float64", checks=[pa.Check.gt(0)], nullable=False),
            "high": pa.Column("float64", checks=[pa.Check.gt(0)], nullable=False),
            "low": pa.Column("float64", checks=[pa.Check.gt(0)], nullable=False),
            "close": pa.Column("float64", checks=[pa.Check.gt(0)], nullable=False),
            "volume": pa.Column("float64", checks=[pa.Check.ge(0)], nullable=False),
            "quote_volume": pa.Column("float64", checks=[pa.Check.ge(0)], nullable=False),
            "trades_count": pa.Column("float64", checks=[pa.Check.ge(0), whole_number],
                                      nullable=False),
            "is_test": pa.Column("bool"),
        },
        checks=[
            pa.Check(lambda df: df["high"] >= df[["open", "close"]].max(axis=1),
                     name="high_ge_open_close", error="high < max(open, close)"),
            pa.Check(lambda df: df["low"] <= df[["open", "close"]].min(axis=1),
                     name="low_le_open_close", error="low > min(open, close)"),
            pa.Check(lambda df: df["close_time"] > df["open_time"],
                     name="close_time_after_open_time", error="close_time <= open_time"),
            pa.Check(lambda df: ~df.duplicated(subset=["symbol", "open_time"], keep="first"),
                     name=UNIQUE_CHECK_NAME, error="duplicate (symbol, open_time)"),
        ],
        strict=False,
        coerce=False,
    )


def run_validation(raw: pd.DataFrame) -> dict:
    """Validate a DataFrame. Returns a dict with valid/invalid frames and counters."""
    typed = parse_types(raw)
    schema = build_schema()
    try:
        schema.validate(typed, lazy=True)               # lazy=True collects ALL errors at once
        failure_cases = pd.DataFrame(columns=["index", "column", "check", "failure_case"])
    except SchemaErrors as exc:
        failure_cases = exc.failure_cases.copy()

    if len(failure_cases):
        if failure_cases["index"].isna().any():
            raise RuntimeError("Schema-level (non row-specific) validation errors:\n"
                               + failure_cases[failure_cases["index"].isna()].to_string())
        failure_cases["index"] = failure_cases["index"].astype(int)
        failure_cases["column"] = failure_cases["column"].fillna("dataframe").astype(str)
        failure_cases["reason"] = failure_cases["column"] + ":" + failure_cases["check"].astype(str)

    is_soft = failure_cases["check"].eq(UNIQUE_CHECK_NAME) if len(failure_cases) else pd.Series(dtype=bool)
    hard = failure_cases[~is_soft] if len(failure_cases) else failure_cases
    soft = failure_cases[is_soft] if len(failure_cases) else failure_cases

    reasons = (hard.groupby("index")["reason"].agg(lambda s: "; ".join(sorted(set(s))))
               if len(hard) else pd.Series(dtype=object))
    hard_idx = set(reasons.index)
    test_idx = set(typed.index[typed["is_test"]])
    leaked_tests = test_idx - hard_idx                   # test rows the schema did NOT catch
    quarantine_idx = hard_idx | test_idx                 # tests are always excluded

    invalid = raw.reset_index(drop=True).loc[sorted(quarantine_idx)].copy()
    invalid["errors"] = [reasons.get(i, "is_test_record (not caught by schema)") for i in invalid.index]
    valid = typed.drop(index=sorted(quarantine_idx)).drop(columns=["is_test"])

    return {
        "valid": valid.reset_index(drop=True),
        "invalid": invalid.reset_index(drop=True),
        "failure_cases": failure_cases,
        "duplicates_flagged": len(set(soft["index"]) - quarantine_idx) if len(soft) else 0,
        "tests_total": len(test_idx),
        "tests_caught_by_schema": len(test_idx & hard_idx),
        "tests_missed_by_schema": len(leaked_tests),
    }


def run_self_test() -> dict:
    """Prove that the schema catches deliberately invalid records (and accepts good ones)."""
    base = (int(time.time() * 1000) // 3_600_000) * 3_600_000 - 10 * 3_600_000
    def row(i, **kw):
        r = {"symbol": "BTCUSDT", "interval": "1h", "open_time": base + i * 3_600_000,
             "close_time": base + i * 3_600_000 + 3_599_999, "open": 100.0, "high": 110.0,
             "low": 95.0, "close": 105.0, "volume": 10.0, "quote_volume": 1000.0,
             "trades_count": 50, "is_test": False}
        r.update(kw)
        return r
    rows = [row(0), row(1), row(2),                                   # 3 valid
            row(3, close=-5.0, is_test=True),                         # negative price
            row(4, symbol="FAKECOIN", is_test=True),                  # unknown symbol
            row(5, high=50.0, is_test=True)]                          # high < low
    result = run_validation(pd.DataFrame(rows))
    assert len(result["valid"]) == 3, f"expected 3 valid, got {len(result['valid'])}"
    assert len(result["invalid"]) == 3, f"expected 3 invalid, got {len(result['invalid'])}"
    assert result["tests_caught_by_schema"] == 3, "schema must detect all 3 test records"
    print("Self-test OK: 3 valid accepted, 3 deliberately invalid records detected:")
    print(result["invalid"][["symbol", "open", "high", "low", "close", "errors"]].to_string(index=False))
    return result


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser()
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()
    if args.self_test:
        run_self_test()
        return 0

    if not config.CONSUMED_CSV.exists():
        log.error("%s not found. Run the Kafka consumer first.", config.CONSUMED_CSV)
        return 1
    raw = pd.read_csv(config.CONSUMED_CSV)
    result = run_validation(raw)
    valid, invalid, fc = result["valid"], result["invalid"], result["failure_cases"]

    valid.to_csv(config.VALID_CSV, index=False)
    invalid.to_csv(config.INVALID_CSV, index=False)

    print("\n=== Validation errors by rule ===")
    if len(fc):
        print(fc.groupby("reason").size().rename("rows").to_string())
    else:
        print("(none)")
    print("\n=== Quarantined (invalid) records ===")
    if len(invalid):
        shown = [c for c in ["symbol", "open_time", "open", "high", "low", "close", "volume",
                             "is_test", "errors"] if c in invalid.columns]
        print(invalid[shown].head(15).to_string(index=False))
    print(f"\nTest records injected: {result['tests_total']}, "
          f"caught by Pandera schema: {result['tests_caught_by_schema']}")
    print(f"Duplicates flagged (passed on to cleaner): {result['duplicates_flagged']}")
    print(f"Valid: {len(valid)}, Invalid: {len(invalid)}")

    config.update_stats(valid=len(valid), invalid=len(invalid),
                        duplicates_flagged=result["duplicates_flagged"],
                        tests_caught_by_schema=result["tests_caught_by_schema"])
    return 0 if result["tests_missed_by_schema"] == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
