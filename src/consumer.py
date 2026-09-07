"""
Consumes the `orders` topic, aggregates revenue-by-category into Postgres,
flags suspicious orders (large amount or odd hour) and republishes them to
`fraud_alerts`. Run multiple replicas with the same GROUP_ID to see Kafka
rebalance partitions across them.
"""
import json
import logging
import os
import time

import psycopg2
from kafka import KafkaConsumer, KafkaProducer
from pythonjsonlogger import jsonlogger

logger = logging.getLogger("consumer")
handler = logging.StreamHandler()
handler.setFormatter(jsonlogger.JsonFormatter(
    "%(asctime)s %(levelname)s %(name)s %(message)s"
))
logger.addHandler(handler)
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))

BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
ORDERS_TOPIC = os.environ.get("ORDERS_TOPIC", "orders")
FRAUD_TOPIC = os.environ.get("FRAUD_TOPIC", "fraud_alerts")
GROUP_ID = os.environ.get("CONSUMER_GROUP_ID", "order-analytics-consumers")

PG_HOST = os.environ.get("POSTGRES_HOST", "localhost")
PG_PORT = os.environ.get("POSTGRES_PORT", "5432")
PG_DB = os.environ.get("POSTGRES_DB", "orders_db")
PG_USER = os.environ.get("POSTGRES_USER", "orders_app")
PG_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "change-me-locally")

# Fraud heuristic thresholds
LARGE_AMOUNT_THRESHOLD = 500.0
ODD_HOURS = set(range(0, 6))  # midnight-6am UTC


def get_pg_connection(retries: int = 10, delay: float = 3.0):
    last_err = None
    for attempt in range(1, retries + 1):
        try:
            conn = psycopg2.connect(
                host=PG_HOST, port=PG_PORT, dbname=PG_DB,
                user=PG_USER, password=PG_PASSWORD,
            )
            conn.autocommit = True
            return conn
        except psycopg2.OperationalError as e:
            last_err = e
            logger.warning("postgres not ready, retrying", extra={"attempt": attempt})
            time.sleep(delay)
    raise last_err


def ensure_schema(conn):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS revenue_by_category (
                category TEXT PRIMARY KEY,
                total_revenue NUMERIC(12, 2) NOT NULL DEFAULT 0,
                order_count BIGINT NOT NULL DEFAULT 0,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
            );
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS fraud_alerts (
                order_id TEXT PRIMARY KEY,
                user_id INTEGER NOT NULL,
                category TEXT NOT NULL,
                amount NUMERIC(12, 2) NOT NULL,
                reason TEXT NOT NULL,
                order_timestamp TIMESTAMPTZ NOT NULL,
                flagged_at TIMESTAMPTZ NOT NULL DEFAULT now()
            );
        """)


def is_suspicious(order: dict) -> str | None:
    reasons = []
    if order["amount"] >= LARGE_AMOUNT_THRESHOLD:
        reasons.append("large_amount")
    if order["hour_utc"] in ODD_HOURS:
        reasons.append("odd_hour")
    return ",".join(reasons) if reasons else None


def upsert_revenue(conn, category: str, amount: float):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO revenue_by_category (category, total_revenue, order_count, updated_at)
            VALUES (%s, %s, 1, now())
            ON CONFLICT (category)
            DO UPDATE SET
                total_revenue = revenue_by_category.total_revenue + EXCLUDED.total_revenue,
                order_count = revenue_by_category.order_count + 1,
                updated_at = now();
        """, (category, amount))


def insert_fraud_alert(conn, order: dict, reason: str):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO fraud_alerts (order_id, user_id, category, amount, reason, order_timestamp)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (order_id) DO NOTHING;
        """, (order["order_id"], order["user_id"], order["category"],
              order["amount"], reason, order["timestamp"]))


def run():
    conn = get_pg_connection()
    ensure_schema(conn)

    consumer = KafkaConsumer(
        ORDERS_TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id=GROUP_ID,
        key_deserializer=lambda k: k.decode("utf-8") if k else None,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=True,
    )
    fraud_producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    logger.info("consumer started", extra={"group_id": GROUP_ID, "topic": ORDERS_TOPIC})

    for message in consumer:
        order = message.value
        partition = message.partition

        try:
            upsert_revenue(conn, order["category"], order["amount"])

            reason = is_suspicious(order)
            if reason:
                insert_fraud_alert(conn, order, reason)
                fraud_producer.send(FRAUD_TOPIC, value={**order, "reason": reason})
                logger.warning(
                    "fraud flagged",
                    extra={"order_id": order["order_id"], "reason": reason,
                           "amount": order["amount"], "partition": partition},
                )
            else:
                logger.info(
                    "order processed",
                    extra={"order_id": order["order_id"], "category": order["category"],
                           "partition": partition},
                )
        except Exception:
            logger.exception("failed to process order", extra={"order_id": order.get("order_id")})


if __name__ == "__main__":
    run()
