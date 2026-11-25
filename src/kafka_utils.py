import logging
import time

from confluent_kafka import KafkaException
from confluent_kafka.admin import AdminClient, NewTopic

from src import config

log = logging.getLogger("kafka_utils")


def wait_for_broker(timeout_s: int = 60) -> AdminClient:
    """Block until the broker answers a metadata request (the container needs ~10-20 s to start)."""
    admin = AdminClient({"bootstrap.servers": config.KAFKA_BOOTSTRAP})
    deadline = time.time() + timeout_s
    while True:
        try:
            admin.list_topics(timeout=5)
            log.info("Connected to Kafka broker at %s", config.KAFKA_BOOTSTRAP)
            return admin
        except KafkaException as exc:
            if time.time() > deadline:
                raise ConnectionError(
                    f"Kafka broker not reachable at {config.KAFKA_BOOTSTRAP}. "
                    f"Run 'docker compose up -d' and wait ~20 seconds. ({exc})") from exc
            log.warning("Broker not ready yet, retrying...")
            time.sleep(3)


def ensure_topic(admin: AdminClient) -> None:
    """Create the topic if it does not exist yet (idempotent)."""
    if config.KAFKA_TOPIC not in admin.list_topics(timeout=10).topics:
        futures = admin.create_topics([NewTopic(config.KAFKA_TOPIC,
                                                num_partitions=config.KAFKA_PARTITIONS,
                                                replication_factor=config.KAFKA_REPLICATION)])
        for topic, future in futures.items():
            try:
                future.result()
                log.info("Topic '%s' created (%d partitions)", topic, config.KAFKA_PARTITIONS)
            except KafkaException as exc:
                if "TOPIC_ALREADY_EXISTS" not in str(exc):
                    raise
    deadline = time.time() + 20                             
    while config.KAFKA_TOPIC not in admin.list_topics(timeout=10).topics:
        if time.time() > deadline:
            raise RuntimeError(f"Topic '{config.KAFKA_TOPIC}' did not appear in broker metadata")
        time.sleep(1)
