"""Central configuration: paths, API and Kafka settings, and shared pipeline statistics.

All paths are relative to the repository root, so the project runs from any clone location.
No secrets are stored here (Binance public market data needs no API key).
"""
import json
import os
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT / "data"
FIG_DIR = ROOT / "figures"
SQL_DIR = ROOT / "sql"
DOCS_DIR = ROOT / "docs"

RAW_CSV = DATA_DIR / "raw_data.csv"             
CONSUMED_CSV = DATA_DIR / "consumed_data.csv"   
VALID_CSV = DATA_DIR / "valid_data.csv"          
INVALID_CSV = DATA_DIR / "invalid_records.csv"   
CLEAN_CSV = DATA_DIR / "clean_data.csv"          
DB_PATH = DATA_DIR / "output.db"
STATS_JSON = DATA_DIR / "pipeline_stats.json" 


API_BASE_URLS = [
    "https://api.binance.com",
    "https://data-api.binance.vision",  
    "https://api1.binance.com",
]
API_ENDPOINT = "/api/v3/klines"
SYMBOLS = ["BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT",
           "ADAUSDT", "DOGEUSDT", "AVAXUSDT", "TRXUSDT", "LINKUSDT"]
INTERVAL = "1h"
LIMIT = 500                 
REQUEST_TIMEOUT = 10        
MAX_RETRIES = 3
MIN_RECORDS = 200          


KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "crypto_klines")
KAFKA_PARTITIONS = 3
KAFKA_REPLICATION = 1
CONSUMER_FIRST_MSG_TIMEOUT_S = 30   
CONSUMER_IDLE_TIMEOUT_S = 15        



def load_stats() -> dict:
    if STATS_JSON.exists():
        return json.loads(STATS_JSON.read_text(encoding="utf-8"))
    return {}


def update_stats(**kwargs) -> dict:
    stats = load_stats()
    stats.update(kwargs)
    DATA_DIR.mkdir(exist_ok=True)
    STATS_JSON.write_text(json.dumps(stats, indent=2, default=str), encoding="utf-8")
    return stats


def reset_stats() -> None:
    DATA_DIR.mkdir(exist_ok=True)
    STATS_JSON.write_text("{}", encoding="utf-8")
