from datetime import date
from domains.market_data.services.core.agent.models import InstrumentMetadata, EODPrice

def test_instrument_metadata_schema():
    m = InstrumentMetadata(
        instrument_id="AAPL",
        symbol="AAPL",
        name="Apple Inc.",
        exchange="NASDAQ",
        sector="Technology",
        list_date=date(1980, 12, 12),
        delist_date=None,
        vendor="polygon",
        extra={"foo": "bar"}
    )
    assert m.symbol == "AAPL"
    assert m.vendor == "polygon"

def test_eod_price_schema():
    p = EODPrice(
        instrument_id="AAPL",
        date=date(2023, 8, 1),
        open=190.0,
        high=195.0,
        low=189.0,
        close=192.0,
        adj_close=None,
        volume=1000000,
        vendor="polygon",
        quality_score=0.95,
        provenance={"vendor": "polygon"}
    )
    assert p.instrument_id == "AAPL"
    assert p.close == 192.0
    assert p.vendor == "polygon"
