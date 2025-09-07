"""
Symbol parsing and normalization utilities.
"""


def normalize_symbol(symbol: str) -> str:
    """Normalize stock symbol to standard format."""
    if not symbol:
        return ""
    
    # Remove whitespace and convert to uppercase
    normalized = symbol.strip().upper()
    
    # Remove common prefixes/suffixes that might cause issues
    # Remove exchange suffixes like .NASDAQ, .NYSE
    if "." in normalized:
        parts = normalized.split(".")
        if len(parts) == 2 and parts[1] in ["NASDAQ", "NYSE", "AMEX"]:
            normalized = parts[0]
    
    return normalized