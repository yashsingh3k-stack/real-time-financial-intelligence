import argparse
import json
import random
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

CITIES = ["Bengaluru", "Mumbai", "Delhi", "Pune", "Hyderabad"]
CATEGORIES = ["groceries", "electronics", "fuel", "travel", "restaurants"]
CHANNELS = ["mobile", "web", "pos"]
TRANSACTION_TYPES = ["purchase", "refund"]


def make_transaction(index, now):
    event_time = now - timedelta(
        seconds=random.randint(0, 90 * 24 * 60 * 60)
    )

    return {
        "event_id": str(uuid.uuid4()),
        "transaction_id": f"txn-{index:09d}",
        "customer_id": f"cust-{random.randint(1, 20000):05d}",
        "event_time": event_time.isoformat(),
        "ingested_at": datetime.now(timezone.utc).isoformat(),
        "amount": round(random.uniform(100, 50000), 2),
        "currency": "INR",
        "merchant_category": random.choice(CATEGORIES),
        "city": random.choice(CITIES),
        "channel": random.choice(CHANNELS),
        "device_id": f"device-{random.randint(1, 10000):05d}",
        "transaction_type": random.choices(
            TRANSACTION_TYPES, weights=[80, 20], k=1
        )[0],
    }


def main():
    parser = argparse.ArgumentParser(
        description="Generate synthetic financial transactions"
    )
    parser.add_argument("--count", type=int, default=1_000_000)
    parser.add_argument(
        "--output",
        default="data/samples/transactions_1000000.jsonl",
    )
    args = parser.parse_args()

    if args.count < 1:
        parser.error("--count must be positive")

    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    now = datetime.now(timezone.utc)

    with output.open("w", encoding="utf-8") as file:
        for index in range(1, args.count + 1):
            record = make_transaction(index, now)
            file.write(json.dumps(record, separators=(",", ":")) + "\n")

            if index % 100_000 == 0:
                print(f"Generated {index:,} transactions")

    size_mb = output.stat().st_size / (1024 * 1024)
    print(f"Generation complete: {args.count:,} transactions")
    print(f"File: {output.resolve()}")
    print(f"Size: {size_mb:.2f} MB")


if __name__ == "__main__":
    main()
