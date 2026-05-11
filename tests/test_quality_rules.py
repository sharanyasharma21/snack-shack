from src.quality_rules import (
    normalize_text,
    contains_pii,
    is_valid_rating,
    is_valid_length,
    is_probably_english,
    compute_quality_score,
)

# Safety checks for the quality rules to make sure the rules are defined correctly and working as expected, run pytest to run these tests

def test_normalize_text_replaces_email():
    text = "Email me at Person@Test.com"
    assert normalize_text(text) == "email me at [EMAIL]"


def test_contains_pii_detects_email():
    assert contains_pii("hello test@example.com") is True


def test_contains_pii_detects_phone():
    assert contains_pii("call 555-123-4567") is True


def test_valid_rating():
    assert is_valid_rating(5) is True
    assert is_valid_rating(99) is False


def test_valid_length():
    assert is_valid_length("this is long enough") is True
    assert is_valid_length("bad") is False


def test_probably_english():
    assert is_probably_english("the food was great") is True


def test_quality_score_penalizes_bad_text():
    score = compute_quality_score("asdfasdf", 5, False)
    assert score < 1.0