# order-stream

A small Kafka pipeline that generates order events, serializes them with
Avro via Confluent Schema Registry, and processes them with real-time
per-product price aggregation, retries for transient failures, and a
dead-letter topic for orders that can never succeed.

## Layout

```
order-stream/
  schema/order.avsc          Avro schema for an order event
  order_producer/            generates and publishes random orders
  order_consumer/            consumes, aggregates, retries, dead-letters
  dlq_watcher/                tails the dead-letter topic
  settings.py                 shared connection settings
  docker-compose.yml          Kafka broker + Schema Registry + UI
```

## How it works

1. `order_producer/generate_orders.py` builds a random order (`orderId`,
   `product`, `price`), Avro-encodes it against `schema/order.avsc`, and
   publishes it to the `order-events` topic. Every Nth order is deliberately
   given a price of `0.0` to exercise the dead-letter path on demand.
2. `order_consumer/process_orders.py` reads each order and:
   - validates it (`price <= 0` or empty `product` -> permanently invalid,
     no retry can fix bad data)
   - otherwise processes it, occasionally simulating a flaky downstream
     dependency (`FLAKY_DOWNSTREAM_RATE`) to exercise retries
   - on a simulated downstream hiccup, retries up to `RETRY_LIMIT` times
     with exponential backoff before giving up and dead-lettering
   - on success, updates a `PriceTracker` that keeps a running average
     **per product** as well as a store-wide average, and logs both
3. Anything that can't be processed lands on `order-events.deadletter`
   with a `reason` header. `dlq_watcher/watch_deadletters.py` tails that
   topic and prints what failed and why.

Offsets are committed manually, only after an order is fully handled
(processed or dead-lettered), so a consumer crash mid-message re-delivers
it rather than silently dropping it.

## Running it

### 1. Start Kafka

```powershell
docker compose up -d
docker compose ps   # wait until broker/registry are healthy
```

This also creates the `order-events` and `order-events.deadletter` topics
via the one-shot `topics-bootstrap` container.

### 2. Install dependencies

```powershell
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```

### 3. Run the three components (separate terminals, venv active in each)

```powershell
# Terminal 1
python order_consumer/process_orders.py

# Terminal 2
python dlq_watcher/watch_deadletters.py

# Terminal 3
python order_producer/generate_orders.py --rate 1 --invalid-every 6
```

You should see the consumer print a running average per product as orders
arrive, occasional retry warnings, and every 6th order being dead-lettered
for a zero price.

### 4. Inspect via the UI

`http://localhost:8080` shows both topics, the decoded Avro messages, the
registered schema, and both consumer groups.

### Stopping

```powershell
docker compose down
```


