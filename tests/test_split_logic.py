import pytest
from datetime import date

# Example split adjustment logic for unit test
def apply_split(price, split_numerator, split_denominator):
    """Adjusts the price for a split event."""
    return price * (split_denominator / split_numerator)

@pytest.mark.parametrize("orig_price,numerator,denominator,expected", [
    (100.0, 2, 1, 50.0),     # 2-for-1 split
    (200.0, 3, 2, 133.33333333333334), # 3-for-2 split
    (50.0, 1, 4, 200.0),    # 1-for-4 reverse split
    (120.0, 1, 1, 120.0),   # no split
])
def test_apply_split(orig_price, numerator, denominator, expected):
    adjusted = apply_split(orig_price, numerator, denominator)
    assert pytest.approx(adjusted, rel=1e-6) == expected

# Test full adjustment history (multiple splits)
def apply_splits(price, splits):
    for numerator, denominator in splits:
        price = apply_split(price, numerator, denominator)
    return price

def test_multiple_splits():
    orig_price = 100.0
    splits = [(2, 1), (3, 2)]  # 2-for-1, then 3-for-2
    # After first: 100 -> 50; after second: 50 -> 33.333...
    adjusted = apply_splits(orig_price, splits)
    assert pytest.approx(adjusted, rel=1e-6) == 33.333333333333336
