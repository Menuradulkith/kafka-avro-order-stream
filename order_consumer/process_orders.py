"""Consumes order events, keeps per-product running averages, retries
transient failures, and routes unrecoverable orders to the dead-letter topic.
"""
import logging
import sys
import time
from collections import defaultdict
from pathlib import Path
from random import random

sys.path.append(str(Path(__file__).resolve().parent.parent))

from confluent_kafka import DeserializingConsumer, SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer, AvroSerializer
from confluent_kafka.serialization import StringDeserializer, StringSerializer

import settings

logging.basicConfig(level=logging.INFO, format="%(asctime)s [consumer] %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

RETRY_LIMIT = 4
RETRY_BASE_DELAY = 0.15
FLAKY_DOWNSTREAM_RATE = 0.2  # chance a valid order still fails transiently, for demo purposes


class OrderIsInvalid(Exception):
    """Raised when an order fails validation and can never succeed, no matter how many retries."""


class DownstreamHiccup(Exception):
    """Raised for a simulated, recoverable downstream failure."""


class PriceTracker:
    """Keeps a running average price per product, plus a store-wide average."""

    def __init__(self):
        self._totals = defaultdict(float)
        self._counts = defaultdict(int)

    def record(self, product: str, price: float) -> tuple[float, float]:
        self._totals[product] += price
        self._counts[product] += 1
        self._totals["__all__"] += price
        self._counts["__all__"] += 1
        return self.average_for(product), self.average_for("__all__")

    def average_for(self, key: str) -> float:
        count = self._counts[key]
        return (self._totals[key] / count) if count else 0.0


def validate(order: dict) -> None:
    if order["price"] <= 0:
        raise OrderIsInvalid(f"price must be positive, got {order['price']}")
    if not order["product"]:
        raise OrderIsInvalid("product name is empty")


def attempt_processing(order: dict, tracker: PriceTracker) -> tuple[float, float]:
    validate(order)
    if random() < FLAKY_DOWNSTREAM_RATE:
        raise DownstreamHiccup("simulated downstream timeout")
    return tracker.record(order["product"], order["price"])


def build_deadletter_producer() -> SerializingProducer:
    registry_client = SchemaRegistryClient({"url": settings.REGISTRY_URL})
    serializer = AvroSerializer(registry_client, settings.read_schema())
    return SerializingProducer(
        {
            "bootstrap.servers": settings.KAFKA_BROKER,
            "key.serializer": StringSerializer("utf_8"),
            "value.serializer": serializer,
        }
    )


def route_to_deadletter(producer: SerializingProducer, order: dict, reason: str) -> None:
    producer.produce(
        topic=settings.DEADLETTER_TOPIC,
        key=order["orderId"],
        value=order,
        headers=[("reason", reason.encode("utf-8"))],
    )
    producer.poll(0)
    logger.error("routed order %s to dead-letter topic: %s", order["orderId"], reason)


def process_with_retries(order: dict, tracker: PriceTracker, deadletter_producer: SerializingProducer) -> None:
    tries = 0
    while True:
        try:
            product_avg, overall_avg = attempt_processing(order, tracker)
            logger.info(
                "order=%s product=%s price=%.2f product_avg=%.2f overall_avg=%.2f",
                order["orderId"], order["product"], order["price"], product_avg, overall_avg,
            )
            return
        except OrderIsInvalid as exc:
            route_to_deadletter(deadletter_producer, order, str(exc))
            return
        except DownstreamHiccup as exc:
            tries += 1
            if tries > RETRY_LIMIT:
                route_to_deadletter(deadletter_producer, order, f"gave up after {RETRY_LIMIT} retries: {exc}")
                return
            wait = RETRY_BASE_DELAY * (2 ** (tries - 1))
            logger.warning(
                "order=%s hit a transient error (try %s/%s): %s -- retrying in %.2fs",
                order["orderId"], tries, RETRY_LIMIT, exc, wait,
            )
            time.sleep(wait)


def build_consumer() -> DeserializingConsumer:
    registry_client = SchemaRegistryClient({"url": settings.REGISTRY_URL})
    deserializer = AvroDeserializer(registry_client, settings.read_schema())
    return DeserializingConsumer(
        {
            "bootstrap.servers": settings.KAFKA_BROKER,
            "key.deserializer": StringDeserializer("utf_8"),
            "value.deserializer": deserializer,
            "group.id": settings.CONSUMER_GROUP,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )


def run():
    consumer = build_consumer()
    consumer.subscribe([settings.ORDERS_TOPIC])
    deadletter_producer = build_deadletter_producer()
    tracker = PriceTracker()

    logger.info("listening on '%s', dead-letter topic is '%s'", settings.ORDERS_TOPIC, settings.DEADLETTER_TOPIC)
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                logger.error("poll error: %s", msg.error())
                continue

            order = msg.value()
            if order is None:
                consumer.commit(msg)
                continue

            process_with_retries(order, tracker, deadletter_producer)
            consumer.commit(msg)
    except KeyboardInterrupt:
        logger.info("stopping consumer")
    finally:
        deadletter_producer.flush()
        consumer.close()


if __name__ == "__main__":
    run()
