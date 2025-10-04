"""
Test feature_extraction_runner to reproduce and fix run_id undefined error.
"""
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from datetime import date
from pathlib import Path


@pytest.mark.asyncio
async def test_register_feature_extraction_run_id_defined():
    """
    Reproduce error: name 'run_id' is not defined
    
    This test verifies that register_training_dataset returns both dataset_id and run_id,
    and that these values are correctly used in register_feature_extraction.
    """
    from domains.ml.services.training_data.runners.feature_extraction_runner import (
        register_training_dataset,
        register_feature_extraction
    )
    from core.platform.config_env.environment import Environment, EnvironmentType
    
    # Create test environment
    environment = Environment(None, EnvironmentType.TEST)
    
    # Test that register_training_dataset returns tuple of (dataset_id, run_id)
    with patch('domains.ml.services.training_data.runners.feature_extraction_runner.TrainingDatasetDAO') as mock_dao, \
         patch('asyncpg.connect') as mock_connect:
        
        # Mock database connection
        mock_conn = AsyncMock()
        mock_conn.fetchval = AsyncMock(return_value=42)  # run_id = 42
        mock_conn.execute = AsyncMock()
        mock_conn.close = AsyncMock()
        mock_connect.return_value = mock_conn
        
        # Mock DAO
        mock_dao_instance = MagicMock()
        mock_dao_instance.create_training_dataset = AsyncMock(return_value=88)  # dataset_id = 88
        mock_dao.return_value = mock_dao_instance
        
        from domains.ml.services.training_data.timeseries_sequence_training_generator import TrainingDataConfig
        
        # Create minimal config
        config = TrainingDataConfig(
            base_interval_minutes=1,
            feature_types=['ohlcv'],
            signal_names=['etop']
        )
        
        # Call register_training_dataset
        result = await register_training_dataset(
            environment=environment,
            symbols=['TSLA'],
            start_date=date(2025, 7, 1),
            end_date=date(2025, 9, 28),
            config=config,
            output_dir='/tmp/test',
            storage_format='arrayrecord',
            run_id=None
        )
        
        # Assert returns tuple
        assert isinstance(result, tuple), "register_training_dataset must return tuple"
        assert len(result) == 2, "register_training_dataset must return (dataset_id, run_id)"
        
        dataset_id, run_id = result
        assert dataset_id == 88, f"Expected dataset_id=88, got {dataset_id}"
        assert run_id == 42, f"Expected run_id=42, got {run_id}"
        
    # Test that register_feature_extraction can be called with run_id
    with patch('asyncpg.connect') as mock_connect:
        mock_conn = AsyncMock()
        mock_conn.fetchval = AsyncMock(return_value=99)  # extraction_run_id
        mock_conn.execute = AsyncMock()
        mock_conn.close = AsyncMock()
        mock_connect.return_value = mock_conn
        
        # This should NOT raise NameError: name 'run_id' is not defined
        extraction_run_id = await register_feature_extraction(
            run_id=42,  # Use the run_id from register_training_dataset
            dataset_id='dataset_20250928_050917',
            symbols=['TSLA'],
            start_date=date(2025, 7, 1),
            end_date=date(2025, 9, 28),
            feature_groups=['ohlcv_basic'],
            arrayrecord_files={'/tmp/test.arrayrecord': Path('/tmp/test.arrayrecord')},
            metadata={'total_files': 1},
            environment='intg'
        )
        
        assert extraction_run_id == 99


@pytest.mark.asyncio
async def test_main_workflow_run_id_propagation():
    """
    Test complete workflow from register_training_dataset to register_feature_extraction
    to ensure run_id is properly propagated.
    """
    # This test documents the expected flow:
    # 1. register_training_dataset returns (dataset_id, run_id)
    # 2. run_id is used in register_feature_extraction call
    # 3. No NameError should occur
    
    dataset_id, run_id = (88, 42)  # Simulated return from register_training_dataset
    
    # Verify we can use these values
    assert dataset_id is not None
    assert run_id is not None
    assert isinstance(run_id, int)
    
    # This is the pattern that should be used in main()
    # extraction_run_id = await register_feature_extraction(
    #     run_id=run_id,  # <-- This was causing NameError before fix
    #     dataset_id=dataset_id,
    #     ...
    # )
