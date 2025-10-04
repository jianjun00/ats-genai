# moved from project root
from typing import Dict, Any

def extract_all_signals(tick_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Extract and format all enriched signals from the tick data structure.
    Returns a flat dictionary with all time, calendar, and multi-interval signals.
    Handles unexpected input types gracefully.
    """
    if not isinstance(tick_data, dict):
        return {}
    signals = {}
    # Tick-level signals
    for k in ['symbol', 'bid', 'ask', 'last', 'time', 'volume']:
        if k in tick_data:
            signals[k] = tick_data[k]
    # Time-based signals
    for k in ['hour_of_day', 'day_of_week', 'week_of_month']:
        if k in tick_data:
            signals[k] = tick_data[k]
    # Market calendar signals
    for k in ['lse_last_open', 'lse_last_close']:
        if k in tick_data:
            signals[k] = tick_data[k]
    # Multi-interval signals
    interval_signals = tick_data.get('interval_signals', {})
    if isinstance(interval_signals, dict):
        for interval, vals in interval_signals.items():
            for sig_name, sig_val in vals.items():
                signals[f'{interval}_{sig_name}'] = sig_val
    return signals

# Optionally, you can add further signal processing utilities here, such as:
# - Compute cross-interval features
# - Add custom technical indicators using pandas_ta
# - Filter or select only certain signals for downstream models

# Example usage:
# all_signals = extract_all_signals(tick_data)
# print(all_signals)
