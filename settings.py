"""Shared connection settings for every component in the order-stream demo."""
from pathlib import Path

KAFKA_BROKER = "localhost:9092"
REGISTRY_URL = "http://localhost:8091"

ORDERS_TOPIC = "order-events"
DEADLETTER_TOPIC = "order-events.deadletter"

CONSUMER_GROUP = "order-stream-workers"
DEADLETTER_GROUP = "order-stream-deadletter-watchers"

SCHEMA_FILE = Path(__file__).resolve().parent / "schema" / "order.avsc"


def read_schema() -> str:
    return SCHEMA_FILE.read_text()
