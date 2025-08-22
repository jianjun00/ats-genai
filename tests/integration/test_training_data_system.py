"""
Comprehensive integration tests for the training data generation system.

This test suite verifies:
1. Database schema and migrations work correctly
2. Training data generation job runner creates proper records
3. DAO operations work correctly
4. Web app integration returns proper data
5. End-to-end training data workflow functions properly
"""

import pytest
import asyncio
import asyncpg
from datetime import date, timedelta
from pathlib import Path
import tempfile
import shutil
import json
import numpy as np

from config.environment import Environment
from dao.training_dataset_dao import TrainingDatasetDAO, TrainingDatasetRecord
from app.training_data_job_runner import (
    TrainingDataJobRunner,
    TrainingDataJobConfig,
    create_sample_job_config,
    run_training_data_job_for_symbol
)

class TestTrainingDataSystem:
    """Integration tests for training data generation system."""
    
    @pytest.fixture
    async def env(self):
        """Test environment fixture."""
        return Environment()
    
    @pytest.fixture
    async def db_connection(self, env):
        """Database connection fixture."""
        conn = await asyncpg.connect(env.get_database_url())
        yield conn
        await conn.close()
    
    @pytest.fixture
    async def training_dao(self, env):
        """Training dataset DAO fixture."""
        return TrainingDatasetDAO(env=env)
    
    @pytest.fixture
    async def temp_output_dir(self):
        """Temporary output directory fixture."""
        temp_dir = Path(tempfile.mkdtemp())
        yield temp_dir
        shutil.rmtree(temp_dir, ignore_errors=True)
    
    @pytest.mark.asyncio
    async def test_database_schema_exists(self, db_connection):
        """Test that training dataset table exists with correct schema."""
        
        # Check table exists
        result = await db_connection.fetchval("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = 'dev_training_dataset'
            )
        """)
        assert result is True, "dev_training_dataset table should exist"
        
        # Check key columns exist
        columns_query = """
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = 'dev_training_dataset'
            ORDER BY ordinal_position
        """
        columns = await db_connection.fetch(columns_query)
        column_names = [col['column_name'] for col in columns]
        
        required_columns = [
            'id', 'dataset_name', 'run_id', 'creation_timestamp',
            'total_sequences', 'sequence_length', 'feature_count',
            'label_count', 'symbols', 'data_quality_score'
        ]
        
        for col in required_columns:
            assert col in column_names, f"Column {col} should exist in training_dataset table"
    
    @pytest.mark.asyncio
    async def test_training_dataset_dao_crud(self, training_dao, db_connection):
        """Test basic CRUD operations for training dataset DAO."""
        
        # Create a test run record first
        run_id = await db_connection.fetchval("""
            INSERT INTO dev_runs (run_type, start_time, status)
            VALUES ('test', NOW(), 'running') RETURNING id
        """)
        
        # Create training dataset record
        dataset_record = TrainingDatasetRecord(
            dataset_name="test_dataset_crud",
            run_id=run_id,
            total_sequences=1000,
            sequence_length=60,
            prediction_horizon=5,
            feature_count=10,
            label_count=2,
            symbols=['AAPL', 'MSFT'],
            date_range_start=date.today() - timedelta(days=30),
            date_range_end=date.today(),
            data_quality_score=0.95,
            feature_completeness=0.98,
            label_completeness=0.96,
            status="created"
        )
        
        # Test CREATE
        dataset_id = await training_dao.create_training_dataset(dataset_record, conn=db_connection)
        assert dataset_id is not None
        assert dataset_id > 0
        
        # Test READ by ID
        retrieved_dataset = await training_dao.get_training_dataset_by_id(dataset_id, conn=db_connection)
        assert retrieved_dataset is not None
        assert retrieved_dataset.dataset_name == "test_dataset_crud"
        assert retrieved_dataset.total_sequences == 1000
        assert retrieved_dataset.symbols == ['AAPL', 'MSFT']
        
        # Test READ by name
        retrieved_by_name = await training_dao.get_training_dataset_by_name("test_dataset_crud", conn=db_connection)
        assert retrieved_by_name is not None
        assert retrieved_by_name.id == dataset_id
        
        # Test LIST
        datasets = await training_dao.list_training_datasets(limit=10, conn=db_connection)
        assert len(datasets) > 0
        dataset_names = [d.dataset_name for d in datasets]
        assert "test_dataset_crud" in dataset_names
        
        # Test UPDATE status
        update_success = await training_dao.update_training_dataset_status(
            dataset_id, "validated", None, {"test": "passed"}, conn=db_connection
        )
        assert update_success is True
        
        # Verify update
        updated_dataset = await training_dao.get_training_dataset_by_id(dataset_id, conn=db_connection)
        assert updated_dataset.status == "validated"
        
        # Test quality metrics update
        quality_update = await training_dao.update_dataset_quality_metrics(
            dataset_id, 0.92, 0.95, 0.90, 0.02, 0.01, conn=db_connection
        )
        assert quality_update is True
        
        # Verify quality update
        updated_quality = await training_dao.get_training_dataset_by_id(dataset_id, conn=db_connection)
        assert updated_quality.data_quality_score == 0.92
        
        # Test DELETE
        delete_success = await training_dao.delete_training_dataset(dataset_id, conn=db_connection)
        assert delete_success is True
        
        # Verify deletion
        deleted_dataset = await training_dao.get_training_dataset_by_id(dataset_id, conn=db_connection)
        assert deleted_dataset is None
    
    @pytest.mark.asyncio
    async def test_training_data_job_config_creation(self):
        """Test training data job configuration creation."""
        
        config = create_sample_job_config(symbols=['AAPL', 'TSLA'], days_back=30)
        
        assert config.symbols == ['AAPL', 'TSLA']
        assert config.sequence_length > 0
        assert config.prediction_horizon > 0
        assert len(config.feature_configs) > 0
        assert len(config.label_configs) > 0
        assert config.start_date < config.end_date
    
    @pytest.mark.asyncio
    async def test_training_data_job_runner_initialization(self, temp_output_dir):
        """Test training data job runner initialization."""
        
        config = create_sample_job_config(symbols=['AAPL'], days_back=30)
        runner = TrainingDataJobRunner(config=config, output_dir=str(temp_output_dir))
        
        assert runner.config.symbols == ['AAPL']
        assert runner.output_dir.exists()
        assert runner.training_dataset_dao is not None
    
    @pytest.mark.asyncio
    async def test_basic_training_data_generation(self, temp_output_dir):
        """Test basic training data generation functionality."""
        
        config = create_sample_job_config(symbols=['AAPL'], days_back=60)
        runner = TrainingDataJobRunner(config=config, output_dir=str(temp_output_dir))
        
        # Test market data loading
        market_data = await runner._load_market_data()
        assert len(market_data) > 0
        assert 'symbol' in market_data.columns
        assert 'AAPL' in market_data['symbol'].values
        
        # Test basic training data creation
        features, labels, metadata = runner._create_basic_training_data(market_data)
        
        assert features.shape[0] > 0  # Should have sequences
        assert features.shape[1] == config.sequence_length
        assert labels.shape[0] == features.shape[0]  # Same number of sequences
        assert len(metadata['feature_names']) > 0
        assert len(metadata['label_names']) > 0
        
        # Test file saving
        dataset_id = "test_dataset_123"
        data_files = runner._save_training_data_files(features, labels, dataset_id)
        
        assert 'features' in data_files
        assert 'labels' in data_files
        assert 'metadata' in data_files
        
        # Verify files exist and can be loaded
        assert Path(data_files['features']).exists()
        assert Path(data_files['labels']).exists()
        assert Path(data_files['metadata']).exists()
        
        # Test loading saved data
        loaded_features = np.load(data_files['features'])
        loaded_labels = np.load(data_files['labels'])
        
        assert np.array_equal(features, loaded_features)
        assert np.array_equal(labels, loaded_labels)
        
        # Test metadata
        with open(data_files['metadata'], 'r') as f:
            saved_metadata = json.load(f)
        assert saved_metadata['dataset_id'] == dataset_id
        assert 'features_shape' in saved_metadata
    
    @pytest.mark.asyncio 
    async def test_end_to_end_training_data_workflow(self, db_connection, temp_output_dir):
        """Test complete end-to-end training data generation workflow."""
        
        # This test will:
        # 1. Generate training data for AAPL
        # 2. Verify run record is created
        # 3. Verify training dataset record is created
        # 4. Verify data files are created
        # 5. Verify database linkage works correctly
        
        config = create_sample_job_config(symbols=['AAPL'], days_back=30)
        runner = TrainingDataJobRunner(config=config, output_dir=str(temp_output_dir))
        
        # Run training data generation
        results = await runner.run_training_data_generation()
        
        # Verify results structure
        assert 'status' in results
        assert 'run_id' in results
        assert 'dataset_ids' in results
        
        if results['status'] == 'success':
            # Verify run record exists
            run_id = results['run_id']
            run_record = await db_connection.fetchrow(
                "SELECT * FROM dev_runs WHERE id = $1", run_id
            )
            assert run_record is not None
            assert run_record['run_type'] == 'training_data_generation'
            assert run_record['status'] == 'completed'
            
            # Verify training dataset records exist
            dataset_ids = results['dataset_ids']
            assert len(dataset_ids) > 0
            
            for dataset_id in dataset_ids:
                dataset_record = await db_connection.fetchrow(
                    "SELECT * FROM dev_training_dataset WHERE id = $1", dataset_id
                )
                assert dataset_record is not None
                assert dataset_record['run_id'] == run_id
                assert len(dataset_record['symbols']) > 0
                assert dataset_record['total_sequences'] > 0
                
                # Verify data files exist
                if dataset_record['features_file_path']:
                    assert Path(dataset_record['features_file_path']).exists()
                if dataset_record['labels_file_path']:
                    assert Path(dataset_record['labels_file_path']).exists()
        else:
            # If generation failed, check error details
            pytest.fail(f"Training data generation failed: {results.get('error', 'Unknown error')}")
    
    @pytest.mark.asyncio
    async def test_training_dataset_summary_view(self, db_connection, training_dao):
        """Test training dataset summary view functionality."""
        
        # Create test run and dataset records
        run_id = await db_connection.fetchval("""
            INSERT INTO dev_runs (run_type, start_time, status)
            VALUES ('training_data_generation', NOW(), 'completed') RETURNING id
        """)
        
        dataset_record = TrainingDatasetRecord(
            dataset_name="test_summary_dataset",
            run_id=run_id,
            total_sequences=500,
            sequence_length=30,
            feature_count=5,
            label_count=1,
            symbols=['MSFT'],
            data_quality_score=0.88
        )
        
        dataset_id = await training_dao.create_training_dataset(dataset_record, conn=db_connection)
        
        # Test summary view
        summaries = await training_dao.list_training_dataset_summaries(limit=10, conn=db_connection)
        
        summary_names = [s.dataset_name for s in summaries]
        assert "test_summary_dataset" in summary_names
        
        # Find our specific summary
        our_summary = next((s for s in summaries if s.dataset_name == "test_summary_dataset"), None)
        assert our_summary is not None
        assert our_summary.run_type == 'training_data_generation'
        assert our_summary.symbol_count == 1
    
    @pytest.mark.asyncio
    async def test_dataset_statistics(self, db_connection, training_dao):
        """Test dataset statistics functionality."""
        
        # Create a few test datasets
        run_id = await db_connection.fetchval("""
            INSERT INTO dev_runs (run_type, start_time, status)
            VALUES ('training_data_generation', NOW(), 'completed') RETURNING id
        """)
        
        for i in range(3):
            dataset_record = TrainingDatasetRecord(
                dataset_name=f"stats_test_dataset_{i}",
                run_id=run_id,
                total_sequences=100 * (i + 1),
                sequence_length=60,
                feature_count=10,
                label_count=2,
                symbols=['TEST'],
                data_quality_score=0.9 + (i * 0.02),
                status="validated"
            )
            await training_dao.create_training_dataset(dataset_record, conn=db_connection)
        
        # Get statistics
        stats = await training_dao.get_dataset_statistics(conn=db_connection)
        
        assert 'total_datasets' in stats
        assert 'validated_count' in stats
        assert 'avg_quality_score' in stats
        assert 'total_sequences_generated' in stats
        
        assert stats['total_datasets'] >= 3
        assert stats['validated_count'] >= 3
        assert stats['total_sequences_generated'] >= 600  # 100 + 200 + 300

class TestTrainingDataWebIntegration:
    """Test web app integration with training data system."""
    
    @pytest.mark.asyncio
    async def test_webapp_training_dataset_endpoint(self):
        """Test that web app can retrieve training datasets."""
        
        from unified_backtest_analytics_webapp import UnifiedAnalyticsEngine
        
        engine = UnifiedAnalyticsEngine()
        await engine.initialize()
        
        try:
            # Test getting training datasets
            datasets = await engine.get_training_datasets(limit=5)
            
            # Should return a list (even if empty)
            assert isinstance(datasets, list)
            
            # If there are datasets, verify structure
            for dataset in datasets:
                assert hasattr(dataset, 'dataset_name')
                assert hasattr(dataset, 'creation_timestamp')
                assert hasattr(dataset, 'total_sequences')
                assert hasattr(dataset, 'symbols')
        
        finally:
            await engine.close()
    
    def test_training_data_generation_config(self):
        """Test training data generation configuration validation."""
        
        # Test valid config
        valid_config = {
            'symbols': ['AAPL', 'GOOGL'],
            'days_back': 90
        }
        
        # Should not raise an exception
        from app.training_data_job_runner import create_sample_job_config
        config = create_sample_job_config(
            symbols=valid_config['symbols'],
            days_back=valid_config['days_back']
        )
        
        assert config.symbols == ['AAPL', 'GOOGL']
        assert (config.end_date - config.start_date).days >= 90

def test_training_dataset_record_validation():
    """Test training dataset record data validation."""
    
    # Test valid record
    record = TrainingDatasetRecord(
        dataset_name="valid_test",
        run_id=1,
        total_sequences=1000,
        sequence_length=60,
        feature_count=10,
        label_count=2,
        symbols=['AAPL'],
        data_quality_score=0.95
    )
    
    # All fields should be properly set
    assert record.dataset_name == "valid_test"
    assert record.total_sequences == 1000
    assert record.data_quality_score == 0.95
    assert record.symbols == ['AAPL']
    assert record.status == "created"  # Default value

if __name__ == "__main__":
    # Run a simple smoke test
    import asyncio
    
    async def smoke_test():
        """Simple smoke test to verify basic functionality."""
        print("Running training data system smoke test...")
        
        try:
            # Test config creation
            config = create_sample_job_config(['AAPL'])
            print(f"✅ Config created for {config.symbols}")
            
            # Test environment setup
            env = Environment()
            print(f"✅ Environment initialized: {env.environment}")
            
            # Test DAO initialization
            dao = TrainingDatasetDAO(env)
            print(f"✅ DAO initialized with table: {dao.table_name}")
            
            print("🎉 Smoke test passed!")
            
        except Exception as e:
            print(f"❌ Smoke test failed: {e}")
            raise
    
    asyncio.run(smoke_test())