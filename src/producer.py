import argparse
import json
import logging
import math
import sys
from datetime import datetime, timezone

import numpy as np
import pandas as pd
from confluent_kafka import KafkaException, Producer

from src import config
from src.kafka_utils import ensure_topic, wait_for_broker
from src.test_data import build_duplicates, build_invalid_test_records

log = logging.getLogger("producer")


def _to_python(value):
    """JSON-safe scalar: NaN -> None, numpy numbers -> Python numbers."""
    if isinstance(value, np.generic):
        value = value.item()
    if isinstance(value, float) and math.isnan(value):
        return None
    return value


def load_records() -> list:
    if not config.RAW_CSV.exists():
        raise FileNotFoundError(f"{config.RAW_CSV} not found. Run 'python -m src.api_client' first.")
    df = pd.read_csv(config.RAW_CSV)
    records = [{k: _to_python(v) for k, v in row.items()} for row in df.to_dict("records")]
    for record in records:
        record["is_test"] = False
    return records


def serialize(record: dict) -> bytes:
    """JSON serialization (dict -> UTF-8 bytes)."""
    return json.dumps(record, default=_to_python).encode("utf-8")


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser()
    parser.add_argument("--no-inject", action="store_true",
                        help="do not add invalid test records / duplicates")
    args = parser.parse_args()

    records = load_records()
    collected = len(records)
    injected_tests = injected_dups = 0
    if not args.no_inject:
        duplicates = build_duplicates(records)
        tests = build_invalid_test_records(records[0])
        records = records + duplicates + tests
        injected_dups, injected_tests = len(duplicates), len(tests)
        log.info("Injected %d duplicates and %d invalid test records", injected_dups, injected_tests)

    try:
        admin = wait_for_broker()
        ensure_topic(admin)
        producer = Producer({"bootstrap.servers": config.KAFKA_BOOTSTRAP,
                             "acks": "all", "enable.idempotence": True, "linger.ms": 20})
    except (ConnectionError, KafkaException) as exc:
        log.error("Kafka connection problem: %s", exc)
        return 1

    run_id = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    counters = {"published": 0, "failed": 0}

    def on_delivery(err, msg):                     
        if err is not None:
            counters["failed"] += 1
            if counters["failed"] <= 5:
                log.error("Delivery failed: %s", err)
        else:
            counters["published"] += 1

    for record in records:
        try:
            payload = serialize(record)
        except (TypeError, ValueError) as exc:     
            counters["failed"] += 1
            log.error("Cannot serialize record: %s", exc)
            continue
        while True:
            try:
                producer.produce(config.KAFKA_TOPIC, key=str(record["symbol"]).encode("utf-8"),
                                 value=payload, headers=[("run_id", run_id.encode("utf-8"))],
                                 on_delivery=on_delivery)
                break
            except BufferError:                   
                producer.poll(1.0)
            except KafkaException as exc:          
                counters["failed"] += 1
                log.error("Produce error: %s", exc)
                break
        producer.poll(0)

    remaining = producer.flush(60)
    if remaining:
        counters["failed"] += remaining
        log.error("%d messages were not delivered before timeout", remaining)

    config.update_stats(run_id=run_id, topic=config.KAFKA_TOPIC, collected=collected,
                        published=counters["published"], publish_failed=counters["failed"],
                        injected_tests=injected_tests, injected_duplicates=injected_dups)
    print(f"Published: {counters['published']}, Failed: {counters['failed']} "
          f"(topic={config.KAFKA_TOPIC}, run_id={run_id})")
    return 0 if counters["failed"] == 0 and counters["published"] > 0 else 1


if __name__ == "__main__":
    sys.exit(main())
