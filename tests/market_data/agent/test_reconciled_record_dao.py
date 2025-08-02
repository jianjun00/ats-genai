import pytest
import asyncio
import json
from datetime import date
from market_data.agent.models import ReconciledRecord
from market_data.agent.reconciled_record_dao import ReconciledRecordDAO
from db.test_db_manager import unit_test_db

@pytest.mark.asyncio
async def test_insert_and_get_reconciled_record(unit_test_db):
    import asyncpg
    pool = await asyncpg.create_pool(unit_test_db)
    dao = ReconciledRecordDAO(pool)
    record = ReconciledRecord(
        data_type="eod",
        instrument_id="AAPL",
        as_of=date(2023, 8, 1),
        value={"close": 100, "open": 99, "high": 101, "low": 98, "volume": 1000},
        quality_score=0.95,
        sources=["polygon", "tiingo"],
        rationale="Test rationale",
        provenance={
            "raw_records": [
                {"vendor": "polygon", "close": 100},
                {"vendor": "tiingo", "close": 100}
            ],
            "audit_log": [
                {"decision": "consensus_majority", "close_candidates": [100, 100], "close_value": 100, "votes": 2}
            ],
            "scoring": {"close_votes": 2, "total_records": 2, "quality_score": 0.95}
        }
    )
    await dao.insert(record)
    fetched = await dao.get("AAPL", date(2023, 8, 1), "eod")
    assert fetched is not None
    assert fetched.instrument_id == record.instrument_id
    assert fetched.data_type == record.data_type
    assert fetched.as_of == record.as_of
    assert fetched.value == record.value
    assert fetched.quality_score == record.quality_score
    assert set(fetched.sources) == set(record.sources)
    assert fetched.rationale == record.rationale
    assert fetched.provenance["scoring"] == record.provenance["scoring"]
    assert fetched.provenance["audit_log"][0]["decision"] == "consensus_majority"
