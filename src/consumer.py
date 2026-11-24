"""Stage 2b: Kafka CONSUMER (separate script).

Reads this run's messages from the topic, deserializes JSON, saves data/consumed_data.csv.

Termination conditions (so the pipeline finishes):
  1. consumed == number published by the producer, OR
  2. no new message for CONSUMER_IDLE_TIMEOUT_S seconds (CONSUMER_FIRST_MSG_TIMEOUT_S before the first one).

Usage:  python -m src.consumer
"""
import json
import logging
import sys
import time

import pandas as pd
from confluent_kafka import Consumer, KafkaError, KafkaException

from src import config
from src.kafka_utils import wait_for_broker

log = logging.getLogger("consumer")


def deserialize(raw: bytes) -> dict:
    """JSON deserialization (UTF-8 bytes -> dict). Raises ValueError on a corrupted message."""
    return json.loads(raw.decode("utf-8"))


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    stats = config.load_stats()
    run_id = stats.get("run_id")
    expected = stats.get("published")
    if not run_id:
        log.warning("No run_id in stats: producer has not run in this session; reading ALL messages.")

    try:
        wait_for_broker()
        consumer = Consumer({"bootstrap.servers": config.KAFKA_BOOTSTRAP,
                             "group.id": f"crypto-consumer-{run_id or 'manual'}",
                             "auto.offset.reset": "earliest",
                             "enable.auto.commit": False})
        consumer.subscribe([config.KAFKA_TOPIC])
    except (ConnectionError, KafkaException) as exc:
        log.error("Kafka connection problem: %s", exc)
        return 1

    records, corrupted, other_runs, kafka_errors = [], 0, 0, 0
    last_message_at = time.time()
    got_first = False
    try:
        while True:
            msg = consumer.poll(1.0)
            idle_limit = config.CONSUMER_IDLE_TIMEOUT_S if got_first else config.CONSUMER_FIRST_MSG_TIMEOUT_S
            if msg is None:
                if time.time() - last_message_at > idle_limit:
                    log.info("No new messages for %ds -> stopping", idle_limit)
                    break
                continue
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    kafka_errors += 1
                    log.error("Consumer error: %s", msg.error())
                continue

            got_first = True
            last_message_at = time.time()
            headers = dict(msg.headers() or [])
            if run_id and headers.get("run_id", b"").decode("utf-8", "ignore") != run_id:
                other_runs += 1                       # message from an older pipeline run
                continue
            try:
                records.append(deserialize(msg.value()))
            except (ValueError, UnicodeDecodeError) as exc:   # JSONDecodeError is a ValueError
                corrupted += 1
                log.error("Corrupted message skipped (partition %s, offset %s): %s",
                          msg.partition(), msg.offset(), exc)
                continue

            if len(records) % 1000 == 0:
                log.info("Consumed %d messages...", len(records))
            if expected is not None and len(records) + corrupted >= expected:
                log.info("Received all %d expected messages -> stopping", expected)
                break
    finally:
        consumer.close()

    if not records:
        log.error("No messages consumed. Did the producer run successfully?")
        return 1
    config.DATA_DIR.mkdir(exist_ok=True)
    pd.DataFrame(records).to_csv(config.CONSUMED_CSV, index=False)
    config.update_stats(consumed=len(records), consumed_corrupted=corrupted,
                        consumed_skipped_other_runs=other_runs, consumer_kafka_errors=kafka_errors)
    print(f"Consumed: {len(records)} (corrupted skipped: {corrupted}, "
          f"older-run messages skipped: {other_runs}) -> {config.CONSUMED_CSV}")
    return 0 if (expected is None or len(records) == expected) else 1


if __name__ == "__main__":
    sys.exit(main())
