import json
import random
from datetime import datetime, timedelta
from pathlib import Path


OUTPUT_PATH = Path("src/data/raw/snackshack_reviews.jsonl")


SAMPLE_TEXTS = [
    "The burger was amazing and the fries were crispy.",
    "Terrible service. I waited 45 minutes.",
    "Pizza was okay, nothing special.",
    "Great tacos! Email me at customer@example.com", # Add personal identifiable information
    "Call me at 555-123-4567, I have feedback.",
    "",
    "Good",
    "This place was fantastic. The staff were friendly and the food was fresh.",
    "Worst experience ever!!!",
    "I loved the ramen but the restaurant was too loud.",
    "asdfasdfasdfasdf",
    "La comida estuvo deliciosa y el servicio excelente.",
]


SOURCES = ["mobile_app", "web", "partner_feed"]


def random_date() -> str:
    base = datetime(2026, 5, 1)
    offset = timedelta(days=random.randint(0, 7), hours=random.randint(0, 23))
    return (base + offset).isoformat() + "Z"


def generate_record(i: int) -> dict:
    text = random.choice(SAMPLE_TEXTS)

    return {
        "id": f"r{i:04d}",
        "source": random.choice(SOURCES),
        "text": text,
        "created_at": random_date(),
        "rating": random.choice([1, 2, 3, 4, 5, None]),
    }


def main() -> None:
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)

    records = [generate_record(i) for i in range(1, 201)]

    # Add intentional duplicate examples.
    records.append(records[0])
    records.append(records[5])
    records.append({
        "id": "bad_record",
        "source": "mobile_app",
        "text": None,
        "created_at": "not-a-date", # Add an intentional date error
        "rating": 99, # Add invalid rating
    })

    with OUTPUT_PATH.open("w") as f:
        for record in records:
            f.write(json.dumps(record) + "\n")

    print(f"Wrote {len(records)} records to {OUTPUT_PATH}")


if __name__ == "__main__":
    main()