"""
Currency formatting utilities.
"""

from decimal import Decimal, ROUND_HALF_UP
from typing import Union


def format_currency(value: Union[float, Decimal], currency: str = "USD") -> str:
    """Format number as currency."""
    if isinstance(value, Decimal):
        rounded = value.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    else:
        rounded = round(value, 2)

    if currency == "USD":
        return f"${rounded:,.2f}"
    else:
        return f"{rounded:,.2f} {currency}"