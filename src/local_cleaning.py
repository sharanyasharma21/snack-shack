import json
from pathlib import Path

from src.quality_rules import (
    normalize_text,
    contains_pii,
    is_valid_rating,
    is_valid_length,
    is_probably_english,
    compute_quality_score,
)

# Run each review through the validators, reject if any fail, attach quality score to ones that pass
# Writes out 3 files: cleaned, rejected, and metrics

RAW_PATH = Path("src/data/raw/snackshack_reviews.jsonl")
PROCESSED_PATH = Path("src/data/processed/local_cleaned_reviews.jsonl")
REJECTED_PATH = Path("src/data/rejected/local_rejected_reviews.jsonl")
METRICS_PATH = Path("src/data/metrics/local_metrics.json")


def main() -> None:
    seen_texts = set()

    accepted = []
    rejected = []

    metrics = {
        "raw_count": 0,
        "accepted_count": 0,
        "rejected_count": 0,
        "duplicate_count": 0,
        "pii_count": 0,
        "invalid_rating_count": 0,
        "invalid_length_count": 0,
        "non_english_count": 0,
    }

    with RAW_PATH.open("r") as f:
        for line in f:
            metrics["raw_count"] += 1
            record = json.loads(line)

            text = record.get("text")
            rating = record.get("rating")

            had_pii = contains_pii(text)
            normalized_text = normalize_text(text)

            reject_reasons = []

            if normalized_text in seen_texts:
                reject_reasons.append("duplicate")
                metrics["duplicate_count"] += 1

            if had_pii:
                metrics["pii_count"] += 1

            if not is_valid_rating(rating):
                reject_reasons.append("invalid_rating")
                metrics["invalid_rating_count"] += 1

            if not is_valid_length(normalized_text):
                reject_reasons.append("invalid_length")
                metrics["invalid_length_count"] += 1

            if not is_probably_english(normalized_text):
                reject_reasons.append("non_english")
                metrics["non_english_count"] += 1

            if reject_reasons:
                record["reject_reasons"] = reject_reasons
                rejected.append(record)
                continue

            seen_texts.add(normalized_text)

            clean_record = {
                "id": record["id"],
                "source": record["source"],
                "text": normalized_text,
                "rating": rating,
                "text_length": len(normalized_text),
                "quality_score": compute_quality_score(normalized_text, rating, had_pii),
                "dataset_version": "2026_05_10",
            }

            accepted.append(clean_record)

    metrics["accepted_count"] = len(accepted)
    metrics["rejected_count"] = len(rejected)

    PROCESSED_PATH.parent.mkdir(parents=True, exist_ok=True)
    REJECTED_PATH.parent.mkdir(parents=True, exist_ok=True)
    METRICS_PATH.parent.mkdir(parents=True, exist_ok=True)

    with PROCESSED_PATH.open("w") as f:
        for record in accepted:
            f.write(json.dumps(record) + "\n")

    with REJECTED_PATH.open("w") as f:
        for record in rejected:
            f.write(json.dumps(record) + "\n")

    with METRICS_PATH.open("w") as f:
        json.dump(metrics, f, indent=2)

    print(json.dumps(metrics, indent=2))


if __name__ == "__main__":
    main()