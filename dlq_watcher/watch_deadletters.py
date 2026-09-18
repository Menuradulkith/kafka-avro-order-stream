"""Tails the dead-letter topic and prints every permanently-failed order."""
import logging
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))

from confluent_kafka import DeserializingConsumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import StringDeserializer

import settings

logging.basicConfig(level=logging.INFO, format="%(asctime)s [deadletter] %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def header_value(headers, name: str, default: str = "unspecified") -> str:
    for key, value in headers or []:
        if key == name:
            return value.decode("utf-8")
    return default


def run():
    registry_client = SchemaRegistryClient({"url": settings.REGISTRY_URL})
    deserializer = AvroDeserializer(registry_client, settings.read_schema())

    consumer = DeserializingConsumer(
        {
            "bootstrap.servers": settings.KAFKA_BROKER,
            "key.deserializer": StringDeserializer("utf_8"),
            "value.deserializer": deserializer,
            "group.id": settings.DEADLETTER_GROUP,
            "auto.offset.reset": "earliest",
        }
    )
    consumer.subscribe([settings.DEADLETTER_TOPIC])

    logger.info("watching '%s' for permanently-failed orders...", settings.DEADLETTER_TOPIC)
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                logger.error("poll error: %s", msg.error())
                continue

            reason = header_value(msg.headers(), "reason")
            logger.warning("dead-lettered order=%s reason=%s", msg.value(), reason)
            consumer.commit(msg)
    except KeyboardInterrupt:
        logger.info("stopping dead-letter watcher")
    finally:
        consumer.close()


if __name__ == "__main__":
    run()
