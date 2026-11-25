import sys

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.patches import FancyBboxPatch

from src import config


def main() -> int:
    boxes = [("Binance API\n/api/v3/klines", "#f4d35e"), ("api_client.py\nraw_data.csv", "#9bc1bc"),
             ("producer.py", "#ee964b"), ("Kafka topic\ncrypto_klines", "#f95738"),
             ("consumer.py\nconsumed_data.csv", "#ee964b"), ("validator.py\n(Pandera)", "#8ecae6"),
             ("cleaner.py\npandas+NumPy", "#8ecae6"), ("loader.py\nSQLite output.db", "#a7c957"),
             ("analysis.py\nfigures/*.png", "#cdb4db")]
    fig, ax = plt.subplots(figsize=(14, 4.8))
    ax.set_xlim(0, 15); ax.set_ylim(0, 6); ax.axis("off")
    positions = [(0.2, 4), (3.2, 4), (6.2, 4), (9.2, 4), (12.2, 4),
                 (12.2, 1.2), (9.2, 1.2), (6.2, 1.2), (3.2, 1.2)]
    for (text, color), (x, y) in zip(boxes, positions):
        ax.add_patch(FancyBboxPatch((x, y), 2.5, 1.3, boxstyle="round,pad=0.05", fc=color, ec="black"))
        ax.text(x + 1.25, y + 0.65, text, ha="center", va="center", fontsize=10)
    for (x1, y1), (x2, y2) in zip(positions[:-1], positions[1:]):
        if y1 == y2:
            start, end = (x1 + 2.55, y1 + 0.65), (x2 - 0.05, y2 + 0.65)
            if x2 < x1:
                start, end = (x1 - 0.05, y1 + 0.65), (x2 + 2.55, y2 + 0.65)
        else:
            start, end = (x1 + 1.25, y1 - 0.05), (x2 + 1.25, y2 + 1.35)
        ax.annotate("", xy=end, xytext=start, arrowprops=dict(arrowstyle="->", lw=1.8))
    ax.set_title("Pipeline architecture: API -> Kafka -> Pandera -> pandas/NumPy -> SQLite -> EDA", fontsize=12)
    config.FIG_DIR.mkdir(exist_ok=True)
    fig.savefig(config.FIG_DIR / "architecture.png", dpi=150, bbox_inches="tight")
    print(f"saved {config.FIG_DIR / 'architecture.png'}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
