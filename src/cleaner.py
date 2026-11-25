import logging
import sys

import numpy as np
import pandas as pd

from src import config

log = logging.getLogger("cleaner")

LARGE_CAP = {"BTC", "ETH", "BNB"}
FINAL_COLUMNS = ["symbol", "base_asset", "quote_asset", "interval", "open_time", "close_time",
                 "open", "high", "low", "close", "volume", "quote_volume", "trades_count",
                 "log_return", "price_range_pct", "body_pct", "price_trend", "volatility_level",
                 "volume_zscore", "coin_group", "hour", "day_of_week"]
WEEKDAYS = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]


def clean(df: pd.DataFrame) -> tuple:
    report = {"rows_in": int(len(df))}

    print("\n[1] MISSING VALUES")
    missing = df.isna().sum()
    print(missing[missing > 0].to_string() if missing.any() else "No missing values found.")
    critical = ["symbol", "open_time", "close_time", "open", "high", "low", "close"]
    before = len(df)
    df = df.dropna(subset=critical)                      # no price/time -> record is unusable
    df = df.fillna({"volume": 0.0, "quote_volume": 0.0, "trades_count": 0})  # no trades -> 0
    report["rows_dropped_missing"] = int(before - len(df))
    print(f"WHY: rows without price/time cannot be analysed (dropped {before - len(df)}); "
          f"missing volume/trades means no trading, so 0 is a correct fill.")

    print("\n[2] TEXT NORMALIZATION")
    df["symbol"] = df["symbol"].astype(str).str.strip().str.upper()
    df["interval"] = df["interval"].astype(str).str.strip().str.lower()
    df["quote_asset"] = np.where(df["symbol"].str.endswith("USDT"), "USDT", "OTHER")
    df["base_asset"] = df["symbol"].str.replace(r"USDT$", "", regex=True)
    print("WHY: inconsistent case/whitespace would split one coin into several groups. "
          "base_asset/quote_asset are extracted from the pair name.")
    print("symbols:", sorted(df["symbol"].unique()))

    print("\n[3] DUPLICATES")
    dup_mask = df.duplicated(subset=["symbol", "open_time"], keep="first")
    report["duplicates_removed"] = int(dup_mask.sum())
    print(f"Duplicate (symbol, open_time) rows found: {int(dup_mask.sum())}")
    if dup_mask.any():
        print(df[dup_mask][["symbol", "open_time", "close"]].head().to_string(index=False))
    df = df[~dup_mask].copy()
    print("WHY: a duplicated candle would double-count volume and bias averages/correlations.")

    print("\n[4] DATA TYPE CONVERSION")
    df["open_time"] = pd.to_datetime(df["open_time"])
    df["close_time"] = pd.to_datetime(df["close_time"])
    for col in ["open", "high", "low", "close", "volume", "quote_volume"]:
        df[col] = df[col].astype("float64")
    df["trades_count"] = df["trades_count"].astype("int64")
    df["symbol"] = df["symbol"].astype("category")
    df = df.sort_values(["symbol", "open_time"]).reset_index(drop=True)
    print(df.dtypes.to_string())
    print("WHY: datetimes enable time operations, int/float save memory and allow maths, "
          "category speeds up GroupBy; sorting is required for shift()-based returns.")

    print("\n[5] NUMPY OPERATIONS")
    prev_close = df.groupby("symbol", observed=True)["close"].shift(1)       
    df["log_return"] = np.log(df["close"] / prev_close)                      
    df["price_range_pct"] = (df["high"] - df["low"]) / df["open"] * 100      
    df["body_pct"] = (df["close"] - df["open"]) / df["open"] * 100
    df["price_trend"] = np.select([df["body_pct"] > 0.1, df["body_pct"] < -0.1],
                                  ["up", "down"], default="flat")
    p33, p66 = np.percentile(df["price_range_pct"], [33.3, 66.6])
    df["volatility_level"] = np.select([df["price_range_pct"] <= p33, df["price_range_pct"] <= p66],
                                       ["low", "medium"], default="high")
    df["volume_zscore"] = df.groupby("symbol", observed=True)["volume"].transform(
        lambda s: (s - np.mean(s)) / np.std(s))
    df["coin_group"] = np.where(df["base_asset"].isin(LARGE_CAP), "large_cap", "alt")
    df["hour"] = df["open_time"].dt.hour
    df["day_of_week"] = df["open_time"].dt.day_name()
    print(f"volatility thresholds (price_range_pct): low <= {p33:.3f} < medium <= {p66:.3f} < high")
    print("WHY: log returns are additive and comparable across coins; range% normalizes volatility "
          "by price level; z-score makes volume comparable between BTC and DOGE.")

    print("\n[6] FILTERING (3 conditions)")
    mask = ((df["volume"] > df["volume"].median())
            & (df["price_range_pct"] > 1.0)
            & (df["trades_count"] > 1000))
    high_activity = df[mask]
    report["high_activity_candles"] = int(len(high_activity))
    print(f"Candles with volume > median AND range > 1% AND trades > 1000: {len(high_activity)} "
          f"({len(high_activity) / len(df):.1%})")
    print("WHY: isolates 'hot' hours with real market activity for the volatility analysis.")

    print("\n[7] GROUPBY + AGGREGATION")
    by_symbol = df.groupby("symbol", observed=True).agg(
        candles=("close", "size"),
        avg_price_range_pct=("price_range_pct", "mean"),
        max_price_range_pct=("price_range_pct", "max"),
        std_log_return=("log_return", "std"),
        avg_trades=("trades_count", "mean"),
        total_quote_volume=("quote_volume", "sum")).round(4)
    by_hour = df.groupby("hour").agg(
        candles=("close", "size"),
        avg_quote_volume=("quote_volume", "mean"),
        avg_volume_zscore=("volume_zscore", "mean"),
        avg_price_range_pct=("price_range_pct", "mean")).round(4)
    by_weekday = (df.groupby("day_of_week").agg(
        avg_price_range_pct=("price_range_pct", "mean"),
        avg_volume_zscore=("volume_zscore", "mean"))
        .reindex(WEEKDAYS).round(4))
    print("-- by symbol --");  print(by_symbol.to_string())
    print("-- by weekday --"); print(by_weekday.to_string())

    print("\n[8] RESHAPING (pivot_table -> melt)")
    pivot = df.pivot_table(index="hour", columns="symbol", values="volume_zscore",
                           aggfunc="mean", observed=True).round(4)
    melted = pivot.reset_index().melt(id_vars="hour", var_name="symbol",
                                      value_name="avg_volume_zscore")
    print(f"pivot_table shape: {pivot.shape} (hours x symbols); melted back to long format: {melted.shape}")
    print(pivot.iloc[:5, :4].to_string())
    print("WHY: the wide 'hour x coin' table feeds a heatmap; melt returns it to tidy form for SQL/seaborn.")

    for name, table in {"agg_by_symbol": by_symbol, "agg_by_hour": by_hour,
                        "agg_by_weekday": by_weekday, "pivot_hour_symbol": pivot}.items():
        table.to_csv(config.DATA_DIR / f"{name}.csv")
    melted.to_csv(config.DATA_DIR / "melted_hour_symbol.csv", index=False)
    high_activity.to_csv(config.DATA_DIR / "high_activity_candles.csv", index=False)

    df["symbol"] = df["symbol"].astype(str)
    out = df[FINAL_COLUMNS].copy()
    report["rows_out"] = int(len(out))
    return out, report


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    if not config.VALID_CSV.exists():
        log.error("%s not found. Run the validator first.", config.VALID_CSV)
        return 1
    df = pd.read_csv(config.VALID_CSV, parse_dates=["open_time", "close_time"])
    cleaned, report = clean(df)
    cleaned.to_csv(config.CLEAN_CSV, index=False)
    config.update_stats(cleaned=report["rows_out"], duplicates_removed=report["duplicates_removed"],
                        rows_dropped_missing=report["rows_dropped_missing"],
                        high_activity_candles=report["high_activity_candles"])
    print(f"\nRows in: {report['rows_in']}, duplicates removed: {report['duplicates_removed']}, "
          f"rows after cleaning: {report['rows_out']} -> {config.CLEAN_CSV}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
