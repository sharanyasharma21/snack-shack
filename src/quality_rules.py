import re
from typing import Optional

# Define patterns and rules for all reviews data collected 

EMAIL_PATTERN = re.compile(r"\b[\w\.-]+@[\w\.-]+\.\w+\b")
PHONE_PATTERN = re.compile(r"\b\d{3}[-.]?\d{3}[-.]?\d{4}\b")


def normalize_text(text: Optional[str]) -> str:
    if not text:
        return ""

    text = text.lower().strip()
    text = EMAIL_PATTERN.sub("[EMAIL]", text)
    text = PHONE_PATTERN.sub("[PHONE]", text)

    # Basic whitespace cleanup.
    text = re.sub(r"\s+", " ", text)

    return text


def contains_pii(text: Optional[str]) -> bool:
    if not text:
        return False

    return bool(EMAIL_PATTERN.search(text) or PHONE_PATTERN.search(text))


def is_valid_rating(rating: Optional[int]) -> bool:
    return rating in {1, 2, 3, 4, 5}


def is_valid_length(text: Optional[str], min_length: int = 10, max_length: int = 500) -> bool:
    if not text:
        return False

    return min_length <= len(text) <= max_length


def is_probably_english(text: Optional[str]) -> bool:
    """
    Very simple heuristic for this demo.
    In production, use a real language identification model.
    """
    if not text:
        return False

    common_english_words = {"the", "was", "and", "but", "service", "food", "great", "worst"}
    words = set(text.lower().split())

    return len(words.intersection(common_english_words)) > 0

# Calculate quality score starting at 1.0, penalize for short text, PII, and invalid rating

def compute_quality_score(text: str, rating: int, had_pii: bool) -> float:
    score = 1.0

    if len(text) < 20:
        score -= 0.3

    if had_pii:
        score -= 0.2

    if rating is None:
        score -= 0.2

    if "asdf" in text:
        score -= 0.5

    return max(score, 0.0)