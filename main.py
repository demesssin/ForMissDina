import argparse
import subprocess
import sys
import os

from src import config


def run(title: str, module: str, *extra: str) -> None:
    print(f"\n{'=' * 78}\n>>> {title}\n{'=' * 78}", flush=True)
    env = dict(os.environ, PYTHONUTF8="1")
    result = subprocess.run([sys.executable, "-m", module, *extra], cwd=config.ROOT, env=env)
    if result.returncode != 0:
        print(f"\n[PIPELINE STOPPED] stage '{title}' failed with exit code {result.returncode}.")
        if module in ("src.producer", "src.consumer"):
            print("Hint: is Kafka running?  ->  docker compose up -d   (wait ~20 s), then check 'docker ps'.")
        sys.exit(result.returncode)


def main() -> None:
    parser = argparse.ArgumentParser(description="Crypto data pipeline")
    parser.add_argument("--skip-api", action="store_true")
    parser.add_argument("--keep-db", action="store_true")
    parser.add_argument("--no-inject", action="store_true")
    args = parser.parse_args()

    config.reset_stats()
    if config.DB_PATH.exists() and not args.keep_db:
        config.DB_PATH.unlink()
        print(f"Removed old database {config.DB_PATH.name} (fresh run; use --keep-db to keep it)")

    if args.skip_api:
        if not config.RAW_CSV.exists():
            sys.exit("data/raw_data.csv not found; run without --skip-api first.")
        with open(config.RAW_CSV, encoding="utf-8") as f:
            config.update_stats(collected=sum(1 for _ in f) - 1)
        print("Skipping API collection, using existing data/raw_data.csv")
    else:
        run("1. API collection (Binance klines)", "src.api_client")

    run("2a. Kafka producer", "src.producer", *(["--no-inject"] if args.no_inject else []))
    run("2b. Kafka consumer", "src.consumer")
    run("3a. Pandera validation self-test (3 deliberately invalid records)", "src.validator", "--self-test")
    run("3b. Pandera validation of consumed data", "src.validator")
    run("4. Cleaning and transformation (pandas + NumPy)", "src.cleaner")
    run("5. SQLite loading (run twice: idempotency test)", "src.loader")
    run("6. Exploratory data analysis and figures", "src.analysis")
    run("7. Architecture diagram", "src.diagram")
    run("8. Pipeline verification", "src.verify")
    print("\nPIPELINE FINISHED SUCCESSFULLY. See figures/, data/output.db, docs/pipeline_summary.md")


if __name__ == "__main__":
    main()
