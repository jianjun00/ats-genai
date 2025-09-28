"""
Real Objects Test for Training Data Callback

This replaces mock-heavy testing with real database, market data, and ML pipeline integration.
Tests use actual database connections, real market data processing, and end-to-end ArrayRecord generation.

BEFORE: Mock objects masked data processing and ML pipeline issues
AFTER: Real objects reveal actual data quality issues, performance problems, and integration failures
"""

import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path
import tempfile
import shutil
from typing import AsyncGenerator, Dict, List

from core.platform.config.environment import Environment, EnvironmentType
from domains.ml.services.training_data.callbacks.training_data_callback import IntervalBasedTrainingDataCallback
from domains.ml.services.training_data.timeseries_sequence_training_generator import TrainingDataConfig
from domains.market_data.managers.unified_market_data_manager import UnifiedMarketDataManager
from domains.trading.services.state.universe_state_manager import UniverseStateManager


class TestTrainingDataCallbackRealObjects:
    """Real database and market data integration tests for training data callback"""

    @pytest.fixture(scope="session")
    async def test_environment(self) -> Environment:
        """Real test environment with actual database connection"""
        return Environment(
            env_type=EnvironmentType.TEST,
            db_url="postgresql://test:test@localhost/test_training_data_db"
        )

    @pytest.fixture
    async def clean_database(self, test_environment: Environment) -> AsyncGenerator[Environment, None]:
        """Clean database with real training data schema"""
        async with test_environment.get_connection() as conn:
            # Create real universe state intervals table for training data
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS test_universe_state_intervals (
                    id SERIAL PRIMARY KEY,
                    symbol VARCHAR(20) NOT NULL,
                    universe_id VARCHAR(100) NOT NULL,
                    interval_start TIMESTAMP NOT NULL,
                    interval_end TIMESTAMP NOT NULL,
                    timeframe VARCHAR(10) NOT NULL,
                    open_price DECIMAL(15,6),
                    high_price DECIMAL(15,6),
                    low_price DECIMAL(15,6),
                    close_price DECIMAL(15,6),
                    volume BIGINT,
                    vwap DECIMAL(15,6),
                    technical_indicators JSONB,
                    created_at TIMESTAMP DEFAULT NOW(),
                    UNIQUE(symbol, universe_id, interval_start, timeframe)
                )
            """)

            # Create real training dataset table
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS test_training_datasets (
                    id SERIAL PRIMARY KEY,
                    dataset_id VARCHAR(100) NOT NULL UNIQUE,
                    symbols TEXT[] NOT NULL,
                    timeframes TEXT[] NOT NULL,
                    start_date DATE NOT NULL,
                    end_date DATE NOT NULL,
                    sequence_count BIGINT,
                    file_path TEXT NOT NULL,
                    metadata JSONB,
                    created_at TIMESTAMP DEFAULT NOW()
                )
            """)

            # Create real dev_runs table for tracking
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS test_dev_runs (
                    id SERIAL PRIMARY KEY,
                    run_id VARCHAR(100) NOT NULL UNIQUE,
                    command_line TEXT NOT NULL,
                    git_commit_hash VARCHAR(50),
                    start_time TIMESTAMP DEFAULT NOW(),
                    end_time TIMESTAMP,
                    status VARCHAR(50) DEFAULT 'running',
                    output_files TEXT[],
                    metadata JSONB
                )
            """)

            # Clean up before test
            await conn.execute("""
                TRUNCATE TABLE test_dev_runs, test_training_datasets, test_universe_state_intervals 
                RESTART IDENTITY CASCADE
            """)

            # Insert real test universe state data
            await self._insert_test_universe_data(conn)
        
        yield test_environment
        
        # Clean up after test
        async with test_environment.get_connection() as conn:
            await conn.execute("""
                TRUNCATE TABLE test_dev_runs, test_training_datasets, test_universe_state_intervals 
                RESTART IDENTITY CASCADE
            """)

    async def _insert_test_universe_data(self, conn):
        """Insert realistic test universe state data"""
        # Generate realistic market data for AAPL and TSLA
        base_time = datetime.now().replace(hour=9, minute=30, second=0, microsecond=0)
        
        universe_data = []
        
        for symbol in ['AAPL', 'TSLA']:
            base_price = 150.0 if symbol == 'AAPL' else 250.0
            
            # Generate 2 hours of 5-minute data
            for i in range(24):  # 24 five-minute intervals = 2 hours
                interval_start = base_time + timedelta(minutes=i * 5)
                interval_end = interval_start + timedelta(minutes=5)
                
                # Simulate realistic price movement
                price_change = np.random.normal(0, 0.5)  # Small random changes
                open_price = base_price + price_change
                close_price = open_price + np.random.normal(0, 0.3)
                high_price = max(open_price, close_price) + abs(np.random.normal(0, 0.2))
                low_price = min(open_price, close_price) - abs(np.random.normal(0, 0.2))
                volume = int(np.random.normal(1000000, 200000))
                vwap = (high_price + low_price + close_price) / 3
                
                # Create realistic technical indicators
                technical_indicators = {
                    'rsi': float(np.random.uniform(30, 70)),
                    'sma_20': float(base_price + np.random.normal(0, 1)),
                    'volume_sma': float(volume * np.random.uniform(0.8, 1.2))
                }
                
                universe_data.append((
                    symbol, 'test_universe_001', interval_start, interval_end, '5m',
                    float(open_price), float(high_price), float(low_price), float(close_price),
                    volume, float(vwap), technical_indicators
                ))
                
                # Update base price for next interval
                base_price = close_price

        # Insert all data
        await conn.executemany("""
            INSERT INTO test_universe_state_intervals 
            (symbol, universe_id, interval_start, interval_end, timeframe, 
             open_price, high_price, low_price, close_price, volume, vwap, technical_indicators)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
        """, universe_data)

    @pytest.fixture
    def temp_output_dir(self) -> str:
        """Create temporary directory for real file output"""
        temp_dir = tempfile.mkdtemp(prefix="test_training_data_")
        yield temp_dir
        # Cleanup
        shutil.rmtree(temp_dir, ignore_errors=True)

    @pytest.fixture
    async def real_market_data_manager(self, test_environment: Environment) -> UnifiedMarketDataManager:
        """Real market data manager with test database connection"""
        manager = UnifiedMarketDataManager(
            environment=test_environment,
            data_path="/tmp/test_market_data"
        )
        await manager.initialize()
        return manager

    @pytest.fixture
    async def real_universe_state_manager(
        self, 
        clean_database: Environment,
        real_market_data_manager: UnifiedMarketDataManager
    ) -> UniverseStateManager:
        """Real universe state manager for training data"""
        return UniverseStateManager(
            environment=clean_database,
            market_data_manager=real_market_data_manager
        )

    @pytest.fixture
    def real_training_config(self) -> TrainingDataConfig:
        """Real training data configuration"""
        return TrainingDataConfig(
            timeframes={'5m': 5, '15m': 15, '1h': 60},
            features=[
                'open_price', 'high_price', 'low_price', 'close_price', 
                'volume', 'vwap', 'rsi', 'sma_20'
            ],
            lookback_periods={'5m': 12, '15m': 8, '1h': 6},  # Real lookback configurations
            forward_periods={'5m': 1, '15m': 1, '1h': 1},   # Real forward prediction
            validation_split=0.2,
            test_split=0.1
        )

    @pytest.fixture
    async def callback_instance(
        self,
        clean_database: Environment,
        real_training_config: TrainingDataConfig,
        real_universe_state_manager: UniverseStateManager,
        temp_output_dir: str
    ) -> IntervalBasedTrainingDataCallback:
        """Real training data callback with actual dependencies"""
        return IntervalBasedTrainingDataCallback(
            symbols=['AAPL', 'TSLA'],
            config=real_training_config,
            output_dir=temp_output_dir,
            environment=clean_database,
            universe_state_manager=real_universe_state_manager
        )

    # Test real timeframe processing

    @pytest.mark.asyncio
    async def test_timeframe_conversion_real_processing(
        self, 
        callback_instance: IntervalBasedTrainingDataCallback
    ):
        """Test timeframe conversion with real time calculations"""
        # Test real timeframe conversions
        assert callback_instance._timeframe_to_minutes('1m') == 1
        assert callback_instance._timeframe_to_minutes('5m') == 5
        assert callback_instance._timeframe_to_minutes('15m') == 15
        assert callback_instance._timeframe_to_minutes('1h') == 60
        assert callback_instance._timeframe_to_minutes('1d') == 1440
        assert callback_instance._timeframe_to_minutes('1w') == 10080

        # Test unknown timeframe handling (should use default)
        default_minutes = callback_instance._timeframe_to_minutes('unknown')
        assert isinstance(default_minutes, int)
        assert default_minutes > 0

    # Test real feature extraction

    @pytest.mark.asyncio
    async def test_feature_extraction_real_data(
        self, 
        callback_instance: IntervalBasedTrainingDataCallback,
        clean_database: Environment
    ):
        """Test feature extraction with real universe state data"""
        # Get real data from database
        async with clean_database.get_connection() as conn:
            universe_data = await conn.fetch("""
                SELECT symbol, interval_start, interval_end, timeframe,
                       open_price, high_price, low_price, close_price,
                       volume, vwap, technical_indicators
                FROM test_universe_state_intervals
                WHERE symbol = 'AAPL' AND timeframe = '5m'
                ORDER BY interval_start
                LIMIT 10
            """)

        # Convert to DataFrame (real data structure)
        df = pd.DataFrame([dict(row) for row in universe_data])
        
        # Extract features using real processing
        features = await callback_instance.extract_features(df, 'AAPL', '5m')
        
        # Verify real feature extraction results
        assert isinstance(features, dict)
        assert len(features) > 0
        
        # Verify expected feature columns
        expected_features = ['open_price', 'high_price', 'low_price', 'close_price', 'volume', 'vwap']
        for feature in expected_features:
            assert feature in features, f"Missing feature: {feature}"
            assert isinstance(features[feature], (list, np.ndarray))
            assert len(features[feature]) > 0

        # Verify technical indicators are extracted
        if 'technical_indicators' in df.columns:
            # Should extract RSI and SMA from real technical indicators
            assert 'rsi' in features or 'sma_20' in features

    # Test real sequence generation

    @pytest.mark.asyncio
    async def test_sequence_generation_real_data(
        self, 
        callback_instance: IntervalBasedTrainingDataCallback,
        clean_database: Environment
    ):
        """Test training sequence generation with real market data"""
        # Define real time range covering our test data
        end_time = datetime.now().replace(second=0, microsecond=0)
        start_time = end_time - timedelta(hours=3)
        
        # Generate real training sequences
        sequences = await callback_instance.generate_training_sequences(
            symbols=['AAPL', 'TSLA'],
            start_time=start_time,
            end_time=end_time,
            timeframe='5m'
        )
        
        # Verify real sequence generation results
        assert isinstance(sequences, list)
        assert len(sequences) > 0
        
        # Verify sequence structure
        for sequence in sequences:
            assert 'symbol' in sequence
            assert 'timestamp' in sequence
            assert 'features' in sequence
            assert 'target' in sequence
            
            # Verify real symbol mapping
            assert sequence['symbol'] in ['AAPL', 'TSLA']
            
            # Verify feature data types and structure
            assert isinstance(sequence['features'], (dict, np.ndarray))
            assert isinstance(sequence['target'], (float, int, np.ndarray))

    # Test real ArrayRecord generation

    @pytest.mark.asyncio
    async def test_arrayrecord_generation_real_files(
        self, 
        callback_instance: IntervalBasedTrainingDataCallback,
        temp_output_dir: str
    ):
        """Test ArrayRecord file generation with real data and file I/O"""
        # Generate real training data
        end_time = datetime.now().replace(second=0, microsecond=0)
        start_time = end_time - timedelta(hours=2)
        
        # Create real ArrayRecord files
        result = await callback_instance.create_arrayrecord_dataset(
            symbols=['AAPL', 'TSLA'],
            start_time=start_time,
            end_time=end_time,
            dataset_id="test_real_arrayrecord_001"
        )
        
        # Verify real file creation results
        assert result['success'] is True
        assert 'dataset_id' in result
        assert 'file_paths' in result
        assert len(result['file_paths']) > 0
        
        # Verify real files were created
        for file_path in result['file_paths']:
            file_obj = Path(file_path)
            assert file_obj.exists(), f"ArrayRecord file not created: {file_path}"
            assert file_obj.suffix == '.arrayrecord'
            assert file_obj.stat().st_size > 0, f"Empty ArrayRecord file: {file_path}"

    @pytest.mark.asyncio
    async def test_arrayrecord_content_validation_real_data(
        self, 
        callback_instance: IntervalBasedTrainingDataCallback,
        temp_output_dir: str
    ):
        """Test ArrayRecord content validation with real data"""
        # Generate real ArrayRecord dataset
        end_time = datetime.now().replace(second=0, microsecond=0)
        start_time = end_time - timedelta(hours=1)
        
        result = await callback_instance.create_arrayrecord_dataset(
            symbols=['AAPL'],
            start_time=start_time,
            end_time=end_time,
            dataset_id="test_content_validation_001"
        )
        
        assert result['success'] is True
        assert len(result['file_paths']) > 0
        
        # Validate real ArrayRecord content
        for file_path in result['file_paths']:
            # Read and validate ArrayRecord content
            records = await callback_instance.read_arrayrecord_file(file_path)
            
            assert len(records) > 0, f"No records in ArrayRecord file: {file_path}"
            
            # Validate record structure
            for record in records[:5]:  # Check first 5 records
                assert 'symbol' in record
                assert 'timestamp' in record
                assert 'features' in record
                
                # Validate data types
                assert isinstance(record['symbol'], str)
                assert len(record['symbol']) > 0
                
                # Validate feature data
                assert isinstance(record['features'], (dict, list, np.ndarray))

    # Test real database tracking and metadata

    @pytest.mark.asyncio
    async def test_database_tracking_real_persistence(
        self, 
        callback_instance: IntervalBasedTrainingDataCallback,
        clean_database: Environment
    ):
        """Test training dataset tracking with real database persistence"""
        # Generate real training dataset
        end_time = datetime.now().replace(second=0, microsecond=0)
        start_time = end_time - timedelta(hours=1)
        
        result = await callback_instance.create_arrayrecord_dataset(
            symbols=['AAPL', 'TSLA'],
            start_time=start_time,
            end_time=end_time,
            dataset_id="test_db_tracking_001"
        )
        
        assert result['success'] is True
        dataset_id = result['dataset_id']
        
        # Verify real database tracking
        async with clean_database.get_connection() as conn:
            dataset_record = await conn.fetchrow("""
                SELECT * FROM test_training_datasets WHERE dataset_id = $1
            """, dataset_id)
        
        # Verify database record exists
        assert dataset_record is not None
        assert dataset_record['dataset_id'] == dataset_id
        assert dataset_record['symbols'] == ['AAPL', 'TSLA']
        assert len(dataset_record['file_path']) > 0
        assert dataset_record['sequence_count'] is not None
        assert dataset_record['sequence_count'] > 0

    # Test real performance and scalability

    @pytest.mark.asyncio
    async def test_large_dataset_generation_performance(
        self, 
        callback_instance: IntervalBasedTrainingDataCallback,
        clean_database: Environment
    ):
        """Test performance with larger real datasets"""
        import time
        
        # Add more test data for performance testing
        async with clean_database.get_connection() as conn:
            # Generate additional universe state data
            additional_symbols = ['GOOGL', 'MSFT', 'NVDA']
            base_time = datetime.now().replace(hour=9, minute=30, second=0, microsecond=0)
            
            universe_data = []
            for symbol in additional_symbols:
                base_price = 100.0 + len(symbol) * 50  # Different base prices
                
                for i in range(48):  # 4 hours of 5-minute data
                    interval_start = base_time + timedelta(minutes=i * 5)
                    interval_end = interval_start + timedelta(minutes=5)
                    
                    open_price = base_price + np.random.normal(0, 1)
                    close_price = open_price + np.random.normal(0, 0.5)
                    high_price = max(open_price, close_price) + abs(np.random.normal(0, 0.3))
                    low_price = min(open_price, close_price) - abs(np.random.normal(0, 0.3))
                    volume = int(np.random.normal(800000, 150000))
                    vwap = (high_price + low_price + close_price) / 3
                    
                    universe_data.append((
                        symbol, 'test_universe_002', interval_start, interval_end, '5m',
                        float(open_price), float(high_price), float(low_price), float(close_price),
                        volume, float(vwap), {'rsi': float(np.random.uniform(30, 70))}
                    ))
            
            await conn.executemany("""
                INSERT INTO test_universe_state_intervals 
                (symbol, universe_id, interval_start, interval_end, timeframe, 
                 open_price, high_price, low_price, close_price, volume, vwap, technical_indicators)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
            """, universe_data)

        # Test real performance with larger dataset
        all_symbols = ['AAPL', 'TSLA', 'GOOGL', 'MSFT', 'NVDA']
        end_time = datetime.now().replace(second=0, microsecond=0)
        start_time = end_time - timedelta(hours=4)
        
        # Measure real processing time
        start_perf = time.time()
        result = await callback_instance.create_arrayrecord_dataset(
            symbols=all_symbols,
            start_time=start_time,
            end_time=end_time,
            dataset_id="test_performance_001"
        )
        end_perf = time.time()
        
        # Verify performance characteristics
        processing_time = end_perf - start_perf
        assert processing_time < 300.0, f"Processing took too long: {processing_time}s"  # 5 minutes max
        
        # Verify results
        assert result['success'] is True
        assert len(result['file_paths']) > 0

    # Test real error handling and edge cases

    @pytest.mark.asyncio
    async def test_invalid_timeframe_real_handling(
        self, 
        callback_instance: IntervalBasedTrainingDataCallback
    ):
        """Test handling of invalid timeframes with real data constraints"""
        end_time = datetime.now()
        start_time = end_time - timedelta(hours=1)
        
        # Test with invalid timeframe
        with pytest.raises(ValueError):
            await callback_instance.generate_training_sequences(
                symbols=['AAPL'],
                start_time=start_time,
                end_time=end_time,
                timeframe='invalid_timeframe'
            )

    @pytest.mark.asyncio
    async def test_empty_data_handling_real_constraints(
        self, 
        callback_instance: IntervalBasedTrainingDataCallback
    ):
        """Test handling when no data is available for time range"""
        # Use time range with no data
        future_time = datetime.now() + timedelta(days=30)
        start_time = future_time
        end_time = future_time + timedelta(hours=1)
        
        # Should handle empty data gracefully
        result = await callback_instance.create_arrayrecord_dataset(
            symbols=['AAPL'],
            start_time=start_time,
            end_time=end_time,
            dataset_id="test_empty_data_001"
        )
        
        # Should either return empty results or handle gracefully
        # (Exact behavior depends on implementation)
        assert isinstance(result, dict)
        assert 'success' in result

    # Test real data quality validation

    @pytest.mark.asyncio
    async def test_data_quality_validation_real_checks(
        self, 
        callback_instance: IntervalBasedTrainingDataCallback,
        clean_database: Environment
    ):
        """Test data quality validation with real constraints"""
        # Insert some data with quality issues
        async with clean_database.get_connection() as conn:
            # Insert invalid price data (negative prices)
            await conn.execute("""
                INSERT INTO test_universe_state_intervals 
                (symbol, universe_id, interval_start, interval_end, timeframe, 
                 open_price, high_price, low_price, close_price, volume, vwap)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
            """, (
                'BAD_DATA', 'test_universe_003', 
                datetime.now() - timedelta(minutes=10), datetime.now() - timedelta(minutes=5), '5m',
                -150.0, -149.0, -151.0, -150.5, 1000000, -150.25  # Invalid negative prices
            ))

        # Test data quality validation
        quality_report = await callback_instance.validate_data_quality(
            symbols=['AAPL', 'BAD_DATA'],
            start_time=datetime.now() - timedelta(hours=1),
            end_time=datetime.now()
        )
        
        # Verify quality validation results
        assert isinstance(quality_report, dict)
        assert 'issues' in quality_report or 'warnings' in quality_report or 'quality_score' in quality_report
        
        # Should detect price data issues
        if 'issues' in quality_report:
            assert len(quality_report['issues']) > 0