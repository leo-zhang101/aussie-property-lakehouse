import json
import time
import random
import uuid
from datetime import datetime, timezone
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    linger_ms=10,
    batch_size=32768
)

states = ["NSW", "VIC", "QLD", "WA", "SA", "TAS"]
merchant_categories = [
    "groceries", "fuel", "travel", "electronics",
    "dining", "fashion", "health", "utilities"
]
payment_methods = ["card", "bank_transfer", "wallet"]
channels = ["mobile_app", "web", "pos_terminal"]
device_types = ["ios", "android", "web"]
transaction_statuses = ["approved", "declined", "pending"]
currencies = ["AUD", "AUD", "AUD", "AUD", "USD", "SGD"]


def generate_event() -> dict:
    amount = round(random.uniform(5.0, 5000.0), 2)
    is_international = random.random() < 0.08

    base_risk = random.uniform(0.01, 0.4)
    if amount > 3000:
        base_risk += 0.2
    if is_international:
        base_risk += 0.25

    risk_score = round(min(base_risk, 0.99), 2)

    event = {
        "event_id": str(uuid.uuid4()),
        "event_ts": datetime.now(timezone.utc).isoformat(),
        "transaction_id": f"TXN{random.randint(10000000, 99999999)}",
        "customer_id": random.randint(100000, 999999),
        "merchant_id": random.randint(10000, 99999),
        "merchant_category": random.choice(merchant_categories),
        "payment_method": random.choice(payment_methods),
        "currency": random.choice(currencies),
        "amount": amount,
        "state": random.choice(states),
        "channel": random.choice(channels),
        "device_type": random.choice(device_types),
        "transaction_status": random.choice(transaction_statuses),
        "is_international": is_international,
        "risk_score": risk_score
    }

    return event


def main():
    total_events = 200000
    sleep_seconds = 0.005   # 约 200 条/秒
    count = 0
    start_time = time.time()

    print(f"Starting producer for {total_events} events...")

    while count < total_events:
        event = generate_event()
        producer.send("payment_events_v2", event)
        count += 1

        if count % 1000 == 0:
            elapsed = time.time() - start_time
            rate = count / elapsed if elapsed > 0 else 0
            print(f"sent {count} events | elapsed={elapsed:.2f}s | rate={rate:.2f} events/s")

        time.sleep(sleep_seconds)

    producer.flush()

    elapsed = time.time() - start_time
    rate = count / elapsed if elapsed > 0 else 0

    print("Producer finished successfully")
    print(f"total_events={count}")
    print(f"elapsed_seconds={elapsed:.2f}")
    print(f"avg_rate={rate:.2f} events/s")


if __name__ == "__main__":
    main()
