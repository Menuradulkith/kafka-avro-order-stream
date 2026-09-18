"""Generates random order events and publishes them as Avro to Kafka."""
import argparse
import logging
import sys
import time
import uuid
from pathlib import Path
from random import choice, uniform

sys.path.append(str(Path(__file__).resolve().parent.parent))

from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer

import settings

logging.basicConfig(level=logging.INFO, format="%(asctime)s [producer] %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

CATALOG = ["Keyboard", "Monitor", "Headset", "Webcam", "Mousepad", "Dock"]


def next_order_id(counter: int) -> str:
    return f"ORD-{counter:05d}"


def random_order(counter: int, force_invalid: bool) -> dict:
    if force_invalid:
        # A price of 0 (or less) can never be a legitimate sale — the
        # consumer treats this as unrecoverable and skips straight to DLQ.
        return {"orderId": next_order_id(counter), "product": choice(CATALOG), "price": 0.0}
    return {
        "orderId": next_order_id(counter),
        "product": choice(CATALOG),
        "price": round(uniform(9.99, 799.99), 2),
    }


def on_delivery(err, msg):
    if err is not None:
        logger.error("delivery failed key=%s err=%s", msg.key(), err)
        return
    logger.info("sent key=%s -> topic=%s partition=%s", msg.key(), msg.topic(), msg.partition())


def build_producer() -> SerializingProducer:
    registry_client = SchemaRegistryClient({"url": settings.REGISTRY_URL})
    serializer = AvroSerializer(registry_client, settings.read_schema())
    return SerializingProducer(
        {
            "bootstrap.servers": settings.KAFKA_BROKER,
            "key.serializer": StringSerializer("utf_8"),
            "value.serializer": serializer,
        }
    )


def run(rate_per_second: float, invalid_every: int):
    producer = build_producer()
    delay = 1.0 / rate_per_second
    counter = 0
    run_id = uuid.uuid4().hex[:6]
    logger.info("starting run %s, ~%.2f orders/sec", run_id, rate_per_second)

    try:
        while True:
            counter += 1
            force_invalid = invalid_every > 0 and counter % invalid_every == 0
            order = random_order(counter, force_invalid)

            producer.produce(
                topic=settings.ORDERS_TOPIC,
                key=order["orderId"],
                value=order,
                on_delivery=on_delivery,
            )
            producer.poll(0)

            if force_invalid:
                logger.warning("emitted a deliberately invalid order: %s", order)

            time.sleep(delay)
    except KeyboardInterrupt:
        logger.info("stopping producer")
    finally:
        producer.flush()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate random order events onto Kafka")
    parser.add_argument("--rate", type=float, default=1.0, help="orders per second (default: 1.0)")
    parser.add_argument(
        "--invalid-every",
        type=int,
        default=6,
        help="emit a permanently-invalid order every N messages, 0 to disable (default: 6)",
    )
    args = parser.parse_args()
    run(rate_per_second=args.rate, invalid_every=args.invalid_every)
