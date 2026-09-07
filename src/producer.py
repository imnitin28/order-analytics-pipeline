"""
Generates fake e-commerce order events and publishes them to the `orders`
Kafka topic, keyed by user_id. Occasionally injects "suspicious" orders
(large amount / odd hour) for the consumer's fraud-flagging logic to catch.
"""
import argparse
import json
import logging
import os
import random
import time
import uuid
from datetime import datetime, timezone

from faker import Faker
from kafka import KafkaProducer
from pythonjsonlogger import jsonlogger

logger = logging.getLogger("producer")
handler = logging.StreamHandler()
handler.setFormatter(jsonlogger.JsonFormatter(
    "%(asctime)s %(levelname)s %(name)s %(message)s"
))
logger.addHandler(handler)
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))

fake = Faker()

CATEGORIES = ["electronics", "clothing", "home", "books", "toys", "grocery"]

BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
ORDERS_TOPIC = os.environ.get("ORDERS_TOPIC", "orders")


def make_order(suspicious: bool = False) -> dict:
    now = datetime.now(timezone.utc)
    amount = round(random.uniform(500, 2000), 2) if suspicious else round(random.uniform(5, 300), 2)

    return {
        "order_id": str(uuid.uuid4()),
        "user_id": fake.random_int(min=1000, max=9999),
        "category": random.choice(CATEGORIES),
        "amount": amount,
        "timestamp": now.isoformat(),
        "hour_utc": now.hour,
    }


def build_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        key_serializer=lambda k: str(k).encode("utf-8"),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        retries=5,
        linger_ms=50,
    )


def run(rate: float, suspicious_ratio: float):
    producer = build_producer()
    logger.info("producer started", extra={"bootstrap_servers": BOOTSTRAP_SERVERS, "topic": ORDERS_TOPIC})

    sent = 0
    try:
        while True:
            suspicious = random.random() < suspicious_ratio
            order = make_order(suspicious=suspicious)

            producer.send(ORDERS_TOPIC, key=order["user_id"], value=order)
            sent += 1

            logger.info(
                "order published",
                extra={"order_id": order["order_id"], "amount": order["amount"],
                       "category": order["category"], "suspicious_hint": suspicious, "total_sent": sent},
            )

            time.sleep(1.0 / rate)
    except KeyboardInterrupt:
        logger.info("producer shutting down", extra={"total_sent": sent})
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--rate", type=float, default=3.0, help="orders per second")
    parser.add_argument("--suspicious-ratio", type=float, default=0.05, help="fraction of orders that look suspicious")
    args = parser.parse_args()

    run(rate=args.rate, suspicious_ratio=args.suspicious_ratio)
