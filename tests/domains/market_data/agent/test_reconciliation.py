from domains.market_data.services.core.agent.reconciliation import ReconciliationEngine
from domains.market_data.services.core.agent.models import EODPrice
from datetime import date

def make_eod(symbol, close, open_=None, high=None, low=None, volume=None, vendor=None):
    return EODPrice(
        instrument_id=symbol,
        date=date(2023, 8, 1),
        open=open_,
        high=high,
        low=low,
        close=close,
        adj_close=close,
        volume=volume,
        vendor=vendor,
        quality_score=None,
        provenance=None
    )

def test_reconcile_majority():
    e = ReconciliationEngine()
    records = [
        make_eod("AAPL", 100, 99, 101, 98, 1000, "polygon"),
        make_eod("AAPL", 100, 99, 101, 98, 1000, "tiingo"),
        make_eod("AAPL", 101, 99, 101, 98, 1000, "iex"),
    ]
    result = e.reconcile_eod_prices(records)
    assert result.value["close"] == 100
    assert result.quality_score == 2/3
    assert "consensus" in result.rationale
    assert set(result.sources) == {"polygon", "tiingo", "iex"}

def test_reconcile_vendor_priority():
    e = ReconciliationEngine(vendor_priority=["iex", "polygon"])
    records = [
        make_eod("AAPL", 101, 99, 101, 98, 1000, "iex"),
        make_eod("AAPL", 102, 99, 101, 98, 1000, "polygon"),
        make_eod("AAPL", 103, 99, 101, 98, 1000, "tiingo"),
    ]
    result = e.reconcile_eod_prices(records)
    assert result.value["close"] == 101
    assert result.rationale.startswith("Used iex")
    assert result.quality_score == 1/3

def test_reconcile_empty():
    e = ReconciliationEngine()
    result = e.reconcile_eod_prices([])
    assert result is None
