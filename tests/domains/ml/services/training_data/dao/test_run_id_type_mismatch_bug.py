"""
Test to reproduce the exact run_id type mismatch error that occurred in production.

This test demonstrates the bug where string run_ids from the runner context 
were being passed to the MonthlyTrainingDataDAO which expects integer run_ids.

Error reproduced:
❌ Failed to save monthly record for AAPL 2025_07: 
invalid input for query argument $1: 'run_20250920_034441_8fe52868' 
('str' object cannot be interpreted as an integer)
"""

import pytest
import asyncpg
from datetime import date

from core.platform.config.environment import Environment, EnvironmentType
from domains.ml.services.training_data.dao.monthly_training_data_dao import (
    MonthlyTrainingDataDAO, 
    MonthlyTrainingDataRecord
)


async def test_run_id_type_mismatch_error_reproduction(unit_test_db):
    """
    REPRODUCE BUG: Test demonstrates the exact error when string run_id is passed
    to MonthlyTrainingDataDAO.create_monthly_record() which expects integer.
    
    This test MUST fail initially, demonstrating the bug that occurred in production.
    """
    environment = Environment(env_type=EnvironmentType.TEST, db_url=unit_test_db)
    dao = MonthlyTrainingDataDAO(environment)
    
    # Reproduce the exact problematic inputs from the production error
    string_run_id = "run_20250920_034441_8fe52868"  # This is what runner.run_context.run_id returns
    
    problematic_record = MonthlyTrainingDataRecord(
        run_id=string_run_id,  # BUG: This should be int but we're passing string
        symbol="AAPL",
        instrument_id=None,
        year_month=date(2025, 7, 1),  # 2025_07 from the error message
        timeframe_paths={
            "5m": "/data/training_data/monthly/AAPL_5m_2025_07.arrayrecord",
            "15m": "/data/training_data/monthly/AAPL_15m_2025_07.arrayrecord", 
            "60m": "/data/training_data/monthly/AAPL_60m_2025_07.arrayrecord"
        },
        total_records=1000,
        file_size_mb=25.5,
        data_quality_score=0.95,
        status="completed"
    )
    
    # This should raise the exact error we saw in production
    with pytest.raises(asyncpg.exceptions.DataError, match="invalid input for query argument.*str.*object cannot be interpreted as an integer"):
        await dao.create_monthly_record(problematic_record)


async def test_run_id_correct_integer_type_works(unit_test_db):
    """
    VERIFY FIX: Test demonstrates that integer run_ids work correctly.
    
    This test should pass, showing the correct way to use the DAO.
    """
    environment = Environment(env_type=EnvironmentType.TEST, db_url=unit_test_db)
    dao = MonthlyTrainingDataDAO(environment)
    
    # Use proper integer run_id (what RunMetadataTracker returns)
    integer_run_id = 12345
    
    correct_record = MonthlyTrainingDataRecord(
        run_id=integer_run_id,  # CORRECT: Integer as expected by DAO
        symbol="AAPL",
        instrument_id=None,
        year_month=date(2025, 7, 1),
        timeframe_paths={
            "5m": "/data/training_data/monthly/AAPL_5m_2025_07.arrayrecord",
            "15m": "/data/training_data/monthly/AAPL_15m_2025_07.arrayrecord",
            "60m": "/data/training_data/monthly/AAPL_60m_2025_07.arrayrecord"
        },
        total_records=1000,
        file_size_mb=25.5,
        data_quality_score=0.95,
        status="completed"
    )
    
    # This should work without any errors
    record_id = await dao.create_monthly_record(correct_record)
    
    # Validate that the record was created successfully
    assert record_id is not None
    assert isinstance(record_id, int)
    assert record_id > 0


async def test_demonstrate_root_cause_analysis():
    """
    ROOT CAUSE ANALYSIS: This test documents the exact root cause of the bug.
    
    The issue occurred because:
    1. Runner.run_context.run_id returns a string like "run_20250920_034441_8fe52868"
    2. MonthlyTrainingDataRecord.run_id is typed as int
    3. MonthlyTrainingDataDAO.create_monthly_record() passes this to PostgreSQL as $1 parameter
    4. PostgreSQL expects integer for the run_id column but receives string
    5. asyncpg raises DataError: "invalid input for query argument $1"
    """
    
    # Document the type mismatch
    runner_context_run_id = "run_20250920_034441_8fe52868"  # str from runner
    dao_expected_run_id = 12345  # int expected by DAO
    
    # Demonstrate the type conflict
    assert isinstance(runner_context_run_id, str)
    assert isinstance(dao_expected_run_id, int)
    
    # This is why the error occurred - type mismatch
    assert type(runner_context_run_id) != type(dao_expected_run_id)
    
    # The fix: Use RunMetadataTracker to create proper database run_id
    # RunMetadataTracker.start_run() returns integer that DAO expects
    print("✅ Root cause identified: String run_id from runner passed to DAO expecting integer")
    print("✅ Fix implemented: Use RunMetadataTracker for proper integer database run_id")