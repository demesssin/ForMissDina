import logging
import sqlite3
import sys

import matplotlib
matplotlib.use("Agg")                     
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns

from src import config

log = logging.getLogger("analysis")
sns.set_theme(style="whitegrid")
WEEKDAYS = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]


def load_data() -> pd.DataFrame:
    with sqlite3.connect(config.DB_PATH) as conn:
        return pd.read_sql("SELECT * FROM klines", conn, parse_dates=["open_time", "close_time"])


def save(fig, name: str) -> str:
    config.FIG_DIR.mkdir(exist_ok=True)
    path = config.FIG_DIR / name
    fig.tight_layout()
    fig.savefig(path, dpi=150)
    plt.close(fig)
    print(f"saved {path}")
    return name


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    if not config.DB_PATH.exists():
        log.error("%s not found. Run the loader first.", config.DB_PATH)
        return 1
    df = load_data()
    findings = []

    print("\n=== DATASET OVERVIEW ===")
    print("shape:", df.shape)
    print(df.dtypes.to_string())
    numeric_cols = ["open", "high", "low", "close", "volume", "quote_volume", "trades_count",
                    "log_return", "price_range_pct", "volume_zscore"]
    print("\n=== DESCRIPTIVE STATISTICS ===")
    print(df[numeric_cols].describe().round(4).T.to_string())
    print("\n=== CATEGORICAL COUNTS ===")
    for col in ["symbol", "coin_group", "price_trend", "volatility_level"]:
        print(f"\n{col}:\n{df[col].value_counts().to_string()}")
    print("\nvolatility_level x symbol:\n", pd.crosstab(df["symbol"], df["volatility_level"]).to_string())

    by_symbol = df.groupby("symbol").agg(
        avg_range=("price_range_pct", "mean"), std_ret=("log_return", "std"),
        avg_quote_vol=("quote_volume", "mean"), avg_trades=("trades_count", "mean")).round(4)
    print("\n=== GROUPED BY SYMBOL ===\n", by_symbol.sort_values("avg_range", ascending=False).to_string())
    by_group = df.groupby("coin_group")[["price_range_pct", "trades_count"]].mean().round(3)
    print("\n=== GROUPED BY coin_group ===\n", by_group.to_string())
    by_weekday = df.groupby("day_of_week")[["price_range_pct", "volume_zscore"]].mean().reindex(WEEKDAYS).round(4)
    print("\n=== GROUPED BY WEEKDAY ===\n", by_weekday.to_string())

    most_vol, least_vol = by_symbol["avg_range"].idxmax(), by_symbol["avg_range"].idxmin()
    spearman = by_symbol["avg_range"].rank().corr(by_symbol["avg_quote_vol"].rank())
    findings.append(f"Q1. The most volatile coin by mean hourly price range is {most_vol} "
                    f"({by_symbol.loc[most_vol, 'avg_range']:.3f}%), the least volatile is {least_vol} "
                    f"({by_symbol.loc[least_vol, 'avg_range']:.3f}%). Spearman rank correlation between "
                    f"mean volatility and mean USDT turnover across coins: {spearman:.2f}.")

    prices = df.pivot_table(index="open_time", columns="symbol", values="close")
    fig, ax = plt.subplots(figsize=(11, 5))
    for sym in [s for s in ["BTCUSDT", "ETHUSDT", "SOLUSDT", "DOGEUSDT"] if s in prices.columns]:
        series = prices[sym].dropna()
        ax.plot(series.index, series / series.iloc[0] * 100, label=sym, linewidth=1.4)
    ax.axhline(100, color="grey", linestyle="--", linewidth=0.8)
    ax.set_title("Close price normalized to 100 at the start of the period")
    ax.set_xlabel("Time (UTC)"); ax.set_ylabel("Index (start = 100)"); ax.legend(title="Symbol")
    save(fig, "01_normalized_prices.png")
    end_vals = {s: float(prices[s].dropna().iloc[-1] / prices[s].dropna().iloc[0] * 100)
                for s in prices.columns}
    best, worst = max(end_vals, key=end_vals.get), min(end_vals, key=end_vals.get)
    findings.append(f"Fig 1. Over the period {best} performed best (index {end_vals[best]:.1f}) and "
                    f"{worst} worst (index {end_vals[worst]:.1f}); BTC ended at {end_vals.get('BTCUSDT', float('nan')):.1f}.")

    order = by_symbol["avg_range"].sort_values(ascending=False).index.tolist()
    fig, ax = plt.subplots(figsize=(11, 5))
    sns.boxplot(data=df, x="symbol", y="price_range_pct", order=order, hue="symbol",
                legend=False, showfliers=False, ax=ax)
    ax.set_title("Hourly price range (%) by coin (outliers hidden)")
    ax.set_xlabel("Symbol"); ax.set_ylabel("(high - low) / open, %")
    plt.setp(ax.get_xticklabels(), rotation=30)
    save(fig, "02_volatility_boxplot.png")
    findings.append(f"Fig 2. Median hourly range ranges from {df.groupby('symbol')['price_range_pct'].median().min():.2f}% "
                    f"to {df.groupby('symbol')['price_range_pct'].median().max():.2f}%; coins are ordered on the plot "
                    f"from most volatile ({order[0]}) to least volatile ({order[-1]}) by mean range.")

    returns = df.pivot_table(index="open_time", columns="symbol", values="log_return")
    corr = returns.corr()
    fig, ax = plt.subplots(figsize=(9, 7))
    sns.heatmap(corr, annot=True, fmt=".2f", cmap="coolwarm", vmin=min(0, float(corr.min().min())), vmax=1, ax=ax,
                cbar_kws={"label": "Pearson correlation"})
    ax.set_title("Correlation of hourly log returns")
    ax.set_xlabel("Symbol"); ax.set_ylabel("Symbol")
    save(fig, "03_returns_correlation.png")
    pairs = corr.where(~np.eye(len(corr), dtype=bool)).stack()
    strongest, weakest = pairs.idxmax(), pairs.idxmin()
    findings.append(f"Q3/Fig 3. Mean pairwise correlation of hourly returns is {pairs.mean():.2f}. "
                    f"Strongest pair: {strongest[0]}-{strongest[1]} ({pairs.max():.2f}); weakest: "
                    f"{weakest[0]}-{weakest[1]} ({pairs.min():.2f}). "
                    + ("Returns are strongly positively correlated, so diversification inside this basket is limited."
                       if pairs.mean() > 0.5 else "Correlation is weak or moderate in this sample."))

    heat = df.pivot_table(index="hour", columns="symbol", values="volume_zscore", aggfunc="mean")
    fig, ax = plt.subplots(figsize=(11, 7))
    sns.heatmap(heat, cmap="YlOrRd", ax=ax, cbar_kws={"label": "mean volume z-score"})
    ax.set_title("Relative trading volume by hour of day (UTC) and coin")
    ax.set_xlabel("Symbol"); ax.set_ylabel("Hour of day (UTC)")
    save(fig, "04_hourly_activity_heatmap.png")
    hour_profile = heat.mean(axis=1)
    top_hours = hour_profile.sort_values(ascending=False).head(3).index.tolist()
    low_hours = hour_profile.sort_values().head(3).index.tolist()
    findings.append(f"Q2/Fig 4. Trading activity peaks at hours {top_hours} UTC and is lowest at "
                    f"{low_hours} UTC (mean volume z-score {hour_profile.max():.2f} vs {hour_profile.min():.2f}).")

    fig, axes = plt.subplots(1, 2, figsize=(12, 4.5))
    for ax, sym in zip(axes, [s for s in ["BTCUSDT", "ETHUSDT"] if s in df["symbol"].unique()]):
        data = df.loc[df["symbol"] == sym, "log_return"].dropna()
        sns.histplot(data, bins=60, stat="density", kde=True, ax=ax, color="steelblue", label="Histogram + KDE")
        grid = np.linspace(data.min(), data.max(), 300)
        mu, sd = data.mean(), data.std()
        ax.plot(grid, np.exp(-0.5 * ((grid - mu) / sd) ** 2) / (sd * np.sqrt(2 * np.pi)),
                color="crimson", linestyle="--", label="Normal distribution")
        ax.set_title(f"{sym}: distribution of hourly log returns")
        ax.set_xlabel("Hourly log return"); ax.set_ylabel("Density"); ax.legend()
    save(fig, "05_return_distribution.png")
    kurt = df.groupby("symbol")["log_return"].apply(lambda s: s.dropna().kurt())
    findings.append(f"Q4/Fig 5. Excess kurtosis of hourly returns is positive for {int((kurt > 0).sum())} of "
                    f"{len(kurt)} coins (max {kurt.max():.1f} for {kurt.idxmax()}). "
                    + ("Positive excess kurtosis means fatter tails than the normal curve: extreme hourly moves "
                       "are more likely than a normal model predicts." if (kurt > 0).mean() > 0.5
                       else "Tails are not clearly fatter than normal in this sample."))

    fig, ax = plt.subplots(figsize=(8, 6))
    ax.scatter(by_symbol["avg_quote_vol"], by_symbol["avg_range"], s=90, color="darkcyan")
    for sym, row in by_symbol.iterrows():
        ax.annotate(sym.replace("USDT", ""), (row["avg_quote_vol"], row["avg_range"]),
                    textcoords="offset points", xytext=(6, 4))
    ax.set_xscale("log")
    ax.set_title("Mean hourly volatility vs mean hourly turnover")
    ax.set_xlabel("Mean quote volume per hour, USDT (log scale)"); ax.set_ylabel("Mean price range, %")
    save(fig, "06_volatility_vs_volume.png")
    findings.append(f"Fig 6. Each point is one coin; Spearman correlation between turnover and volatility is "
                    f"{spearman:.2f} ({'liquid coins tend to be calmer' if spearman < 0 else 'more liquid coins tend to be more volatile'}).")

    print("\n=== FINDINGS ===")
    for line in findings:
        print("-", line)
    (config.FIG_DIR / "eda_summary.md").write_text(
        "# EDA findings (auto-generated from the database)\n\n" + "\n".join(f"- {l}" for l in findings) + "\n",
        encoding="utf-8")
    return 0


if __name__ == "__main__":
    sys.exit(main())
