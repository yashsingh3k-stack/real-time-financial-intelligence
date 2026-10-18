import json
import random
import uuid
from datetime import datetime, timezone
from pathlib import Path

OUTPUT = Path("data/samples/transactions.jsonl")

CITIES = ["Bengaluru", "Mumbai", "Delhi", "Pune", "Hyderabad"]
CATEGORIES = ["groceries", "electronics", "fuel", "travel", "restaurants"]
CHANNELS = ["mobile", "web", "pos"]

def make_transaction(index):
    now = datetime.now(timezone.utc).isoformat()
    return {
        "event_id": str(uuid.uuid4()),
        "transaction_id": f"txn-{index:06d}",
        "customer_id": f"cust-{random.randint(1, 200):04d}",
        "event_time": now,
        "ingested_at": datetime.now(timezone.utc).isoformat(),
        "amount": round(random.uniform(100, 50000), 2),
        "currency": "INR",
        "merchant_category": random.choice(CATEGORIES),
        "city": random.choice(CITIES),
        "channel": random.choice(CHANNELS),
        "device_id": f"device-{random.randint(1, 100):03d}",
        "transaction_type": random.choice(["purchase", "refund"])
    }

def main():
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)

    with OUTPUT.open("w", encoding="utf-8") as file:
        for index in range(1, 101):
            file.write(json.dumps(make_transaction(index)) + "\n")

    print(f"Generated 100 transactions: {OUTPUT}")

if __name__ == "__main__":
    main()
