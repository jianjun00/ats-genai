"""
Tests for TFT Data Loading and Preprocessing

Comprehensive test suite for the TFT data loader including sentiment integration,
feature normalization, and sequence preparation.
"""

import pytest
import numpy as np
import pandas as pd
import torch
from datetime import datetime, timedelta
from unittest.mock import Mock, AsyncMock, patch

from models.data_loader import (
    TFTDataConfig,
    FeatureNormalizer,
    TFTDataset,
    TFTDataLoader,
    collate_fn
)


@pytest.fixture
def sample_data_config():
    """Sample TFT data configuration."""
    return TFTDataConfig(
        encoder_length=60,
        prediction_length=15,
        temporal_features=['open', 'high', 'low', 'close', 'volume', 'returns'],
        target_features=['returns'],
        use_sentiment=True,
        normalize_features=True
    )


@pytest.fixture
def sample_price_data():
    """Generate sample price data."""
    dates = pd.date_range('2024-01-01', periods=200, freq='1min')
    symbols = ['AAPL', 'MSFT']
    
    data_list = []
    for symbol in symbols:
        for date in dates:
            price = 100 + np.random.randn() * 5  # Random walk around 100
            data_list.append({
                'symbol': symbol,
                'timestamp': date,
                'open': price + np.random.randn() * 0.5,
                'high': price + abs(np.random.randn()) * 0.8,
                'low': price - abs(np.random.randn()) * 0.8,
                'close': price,
                'volume': np.random.randint(1000, 10000),
                'returns': np.random.randn() * 0.02  # 2% daily volatility
            })
    
    return pd.DataFrame(data_list)


@pytest.fixture
def sample_sentiment_data():
    """Generate sample sentiment data."""
    dates = pd.date_range('2024-01-01', periods=100, freq='5min')
    symbols = ['AAPL', 'MSFT']
    
    data_list = []
    for symbol in symbols:
        for date in dates:
            row = {
                'symbol': symbol,
                'created_at': date,
                'sentiment_score': np.random.randn() * 0.5,
                'confidence': np.random.uniform(0.5, 1.0),
                'momentum': np.random.randn() * 0.3,
                'volume_indicator': np.random.uniform(0, 1),
                'consensus_score': np.random.uniform(0, 1),
                'divergence_score': np.random.uniform(0, 1),
                'risk_score': np.random.uniform(0, 1)
            }
            
            # Add ML features
            for i in range(1, 17):
                row[f'feature_{i}'] = np.random.randn() * 0.1
            
            data_list.append(row)
    
    return pd.DataFrame(data_list)


@pytest.fixture
def mock_connection_pool():
    """Mock database connection pool."""
    pool = Mock()
    conn = Mock()
    
    context_manager = AsyncMock()
    context_manager.__aenter__.return_value = conn
    context_manager.__aexit__.return_value = None
    
    pool.acquire.return_value = context_manager
    return pool, conn


@pytest.fixture
def mock_env():
    """Mock environment configuration."""
    env = Mock()
    env.get_table_name.side_effect = lambda x: f"test_{x}"
    return env


class TestTFTDataConfig:
    """Test TFT data configuration."""
    
    def test_config_creation(self):
        """Test data config creation with defaults."""
        config = TFTDataConfig()
        
        assert config.encoder_length == 120
        assert config.prediction_length == 30
        assert config.use_sentiment is True
        assert config.normalize_features is True
        assert len(config.temporal_features) > 0
        assert len(config.target_features) > 0
    
    def test_config_custom_values(self):
        """Test data config with custom values."""
        config = TFTDataConfig(
            encoder_length=240,
            prediction_length=60,
            temporal_features=['close', 'volume'],
            target_features=['returns', 'volatility'],
            use_sentiment=False
        )
        
        assert config.encoder_length == 240
        assert config.prediction_length == 60
        assert config.temporal_features == ['close', 'volume']
        assert config.target_features == ['returns', 'volatility']
        assert config.use_sentiment is False


class TestFeatureNormalizer:
    """Test feature normalization."""
    
    def test_standard_normalization(self, sample_price_data):
        """Test standard normalization."""
        normalizer = FeatureNormalizer("standard")
        features = ['close', 'volume', 'returns']
        
        # Fit normalizer
        normalizer.fit(sample_price_data, features)
        
        assert normalizer.is_fitted
        assert len(normalizer.feature_stats) == len(features)
        
        for feature in features:
            stats = normalizer.feature_stats[feature]
            assert 'mean' in stats
            assert 'std' in stats
            assert stats['std'] > 0
        
        # Transform data
        normalized_data = normalizer.transform(sample_price_data, features)
        
        for feature in features:
            if feature in normalized_data.columns:
                values = normalized_data[feature].dropna()
                # Should be approximately standardized
                assert abs(values.mean()) < 0.1
                assert abs(values.std() - 1.0) < 0.1
    
    def test_minmax_normalization(self, sample_price_data):
        """Test min-max normalization."""
        normalizer = FeatureNormalizer("minmax")
        features = ['close', 'volume']
        
        normalizer.fit(sample_price_data, features)
        normalized_data = normalizer.transform(sample_price_data, features)
        
        for feature in features:
            if feature in normalized_data.columns:
                values = normalized_data[feature].dropna()
                # Should be in [0, 1] range
                assert values.min() >= -0.1  # Allow small numerical errors
                assert values.max() <= 1.1
    
    def test_robust_normalization(self, sample_price_data):
        """Test robust normalization."""
        normalizer = FeatureNormalizer("robust")
        features = ['close', 'returns']
        
        normalizer.fit(sample_price_data, features)
        normalized_data = normalizer.transform(sample_price_data, features)
        
        assert len(normalizer.feature_stats) == len(features)
        
        for feature in features:
            stats = normalizer.feature_stats[feature]
            assert 'median' in stats
            assert 'iqr' in stats
    
    def test_inverse_transform(self, sample_price_data):
        """Test inverse transformation."""
        normalizer = FeatureNormalizer("standard")
        feature = 'close'
        
        original_values = sample_price_data[feature].values
        normalizer.fit(sample_price_data, [feature])
        
        # Transform and inverse transform
        normalized_data = normalizer.transform(sample_price_data, [feature])
        normalized_values = normalized_data[feature].values
        
        inverse_values = normalizer.inverse_transform(normalized_values, feature)
        
        # Should recover original values
        np.testing.assert_allclose(original_values, inverse_values, rtol=1e-10)
    
    def test_normalization_without_fit(self, sample_price_data):
        """Test error when transforming without fitting."""
        normalizer = FeatureNormalizer("standard")
        
        with pytest.raises(ValueError, match="must be fitted"):
            normalizer.transform(sample_price_data, ['close'])


class TestTFTDataset:
    """Test TFT dataset."""
    
    def test_dataset_creation(self, sample_price_data, sample_data_config, sample_sentiment_data):
        """Test dataset creation."""
        dataset = TFTDataset(
            sample_price_data, 
            sample_data_config,
            sentiment_features=sample_sentiment_data
        )
        
        assert len(dataset) > 0
        assert hasattr(dataset, 'sequences')
        assert len(dataset.sequences) > 0
    
    def test_dataset_without_sentiment(self, sample_price_data, sample_data_config):
        """Test dataset creation without sentiment features."""
        sample_data_config.use_sentiment = False
        
        dataset = TFTDataset(sample_price_data, sample_data_config)
        
        assert len(dataset) > 0
        
        # Check that sequences don't have sentiment features
        sample_sequence = dataset.sequences[0]
        assert 'sentiment_features' not in sample_sequence
    
    def test_dataset_getitem(self, sample_price_data, sample_data_config, sample_sentiment_data):
        """Test dataset __getitem__ method."""
        dataset = TFTDataset(
            sample_price_data, 
            sample_data_config,
            sentiment_features=sample_sentiment_data
        )
        
        item = dataset[0]
        
        # Check required keys
        assert 'encoder_input' in item
        assert 'decoder_input' in item
        assert 'targets' in item
        assert 'encoder_lengths' in item
        assert 'symbol' in item
        assert 'start_time' in item
        assert 'end_time' in item
        
        # Check tensor shapes
        assert item['encoder_input'].shape[0] == sample_data_config.encoder_length
        assert item['decoder_input'].shape[0] == sample_data_config.prediction_length
        assert item['targets'].shape[0] == sample_data_config.prediction_length
        assert item['encoder_input'].shape[1] == len(sample_data_config.temporal_features)
        assert item['targets'].shape[1] == len(sample_data_config.target_features)
        
        # Check sentiment features if enabled
        if sample_data_config.use_sentiment and 'sentiment_features' in item:
            total_length = sample_data_config.encoder_length + sample_data_config.prediction_length
            assert item['sentiment_features'].shape[0] == total_length
            assert item['sentiment_features'].shape[1] == 23  # Sentiment feature count
    
    def test_sequence_preparation_with_missing_data(self, sample_data_config):
        """Test sequence preparation with missing data."""
        # Create data with missing values
        dates = pd.date_range('2024-01-01', periods=100, freq='1min')
        data_list = []
        
        for i, date in enumerate(dates):
            row = {
                'symbol': 'AAPL',
                'timestamp': date,
                'open': 100.0 if i % 10 != 0 else np.nan,  # 10% missing
                'high': 101.0,
                'low': 99.0,
                'close': 100.0,
                'volume': 1000,
                'returns': 0.01
            }
            data_list.append(row)
        
        missing_data = pd.DataFrame(data_list)
        
        # Test with high missing tolerance
        sample_data_config.max_missing_ratio = 0.2
        dataset = TFTDataset(missing_data, sample_data_config)
        assert len(dataset) > 0
        
        # Test with low missing tolerance
        sample_data_config.max_missing_ratio = 0.05
        dataset = TFTDataset(missing_data, sample_data_config)
        assert len(dataset) == 0  # Should reject sequences with too much missing data
    
    def test_sentiment_sequence_interpolation(self, sample_price_data, sample_data_config):
        """Test sentiment sequence interpolation."""
        # Create sentiment data with different length
        sentiment_data = pd.DataFrame([
            {
                'symbol': 'AAPL',
                'created_at': pd.Timestamp('2024-01-01 00:00:00'),
                'sentiment_score': 0.5,
                'confidence': 0.8,
                'momentum': 0.1,
                'volume_indicator': 0.6,
                'consensus_score': 0.7,
                'divergence_score': 0.3,
                'risk_score': 0.4,
                **{f'feature_{i}': 0.1 for i in range(1, 17)}
            }
        ])
        
        dataset = TFTDataset(
            sample_price_data, 
            sample_data_config,
            sentiment_features=sentiment_data
        )
        
        if len(dataset) > 0:
            item = dataset[0]
            if 'sentiment_features' in item:
                expected_length = sample_data_config.encoder_length + sample_data_config.prediction_length
                assert item['sentiment_features'].shape[0] == expected_length


class TestTFTDataLoader:
    """Test TFT data loader."""
    
    @pytest.mark.asyncio
    async def test_data_loader_creation(self, mock_connection_pool, mock_env):
        """Test data loader creation."""
        pool, conn = mock_connection_pool
        
        with patch('models.data_loader.SentimentIntegrator'):
            loader = TFTDataLoader(pool, mock_env)
            
            assert loader.pool == pool
            assert loader.env == mock_env
    
    @pytest.mark.asyncio
    async def test_load_price_data(self, mock_connection_pool, mock_env):
        """Test loading price data."""
        pool, conn = mock_connection_pool
        
        # Mock database response
        mock_rows = [
            {
                'symbol': 'AAPL',
                'timestamp': datetime(2024, 1, 1, 10, 0),
                'open': 100.0,
                'high': 101.0,
                'low': 99.0,
                'close': 100.5,
                'volume': 1000,
                'returns': 0.005
            },
            {
                'symbol': 'AAPL',
                'timestamp': datetime(2024, 1, 1, 10, 1),
                'open': 100.5,
                'high': 101.5,
                'low': 100.0,
                'close': 101.0,
                'volume': 1200,
                'returns': 0.005
            }
        ]
        conn.fetch.return_value = mock_rows
        
        with patch('models.data_loader.SentimentIntegrator'):
            loader = TFTDataLoader(pool, mock_env)
            
            data = await loader.load_price_data(
                ['AAPL'],
                datetime(2024, 1, 1),
                datetime(2024, 1, 2)
            )
            
            assert isinstance(data, pd.DataFrame)
            assert len(data) == 2
            assert 'symbol' in data.columns
            assert 'timestamp' in data.columns
            assert 'returns' in data.columns
    
    @pytest.mark.asyncio
    async def test_technical_indicators_calculation(self, mock_connection_pool, mock_env):
        """Test technical indicators calculation."""
        pool, conn = mock_connection_pool
        
        # Create more extensive mock data for technical indicators
        dates = pd.date_range('2024-01-01', periods=50, freq='1min')
        mock_rows = []
        
        for i, date in enumerate(dates):
            price = 100 + i * 0.1  # Trending price
            mock_rows.append({
                'symbol': 'AAPL',
                'timestamp': date,
                'open': price,
                'high': price + 1,
                'low': price - 1,
                'close': price + 0.5,
                'volume': 1000 + i * 10,
                'returns': 0.001 if i > 0 else None
            })
        
        conn.fetch.return_value = mock_rows
        
        with patch('models.data_loader.SentimentIntegrator'):
            loader = TFTDataLoader(pool, mock_env)
            
            data = await loader.load_price_data(
                ['AAPL'],
                datetime(2024, 1, 1),
                datetime(2024, 1, 2)
            )
            
            # Check that technical indicators were calculated
            expected_indicators = ['rsi', 'macd', 'ema_12', 'ema_26', 'bollinger_upper', 'bollinger_lower', 'atr', 'volatility']
            
            for indicator in expected_indicators:
                assert indicator in data.columns
            
            # Check that some values are not NaN (after warmup period)
            assert not data['rsi'].iloc[-10:].isna().all()
            assert not data['macd'].iloc[-10:].isna().all()
    
    @pytest.mark.asyncio
    async def test_load_sentiment_data(self, mock_connection_pool, mock_env):
        """Test loading sentiment data."""
        pool, conn = mock_connection_pool
        
        # Mock sentiment integrator
        mock_sentiment_integrator = Mock()
        mock_signal = Mock()
        mock_signal.timestamp = datetime(2024, 1, 1, 10, 0)
        mock_signal.overall_sentiment_score = 0.5
        mock_signal.overall_confidence = 0.8
        mock_signal.volume_indicator = 0.6
        mock_signal.consensus_score = 0.7
        mock_signal.divergence_score = 0.3
        mock_signal.risk_score = 0.4
        mock_signal.sentiment_features = {f'feature_{i}': 0.1 for i in range(1, 17)}
        mock_signal.sentiment_features['sentiment_momentum'] = 0.2
        
        with patch('models.data_loader.SentimentIntegrator', return_value=mock_sentiment_integrator), \
             patch('models.data_loader.analyze_unified_sentiment', return_value={'AAPL': mock_signal}):
            
            loader = TFTDataLoader(pool, mock_env)
            
            sentiment_data = await loader.load_sentiment_data(
                ['AAPL'],
                datetime(2024, 1, 1),
                datetime(2024, 1, 2)
            )
            
            assert isinstance(sentiment_data, pd.DataFrame)
            if len(sentiment_data) > 0:
                assert 'symbol' in sentiment_data.columns
                assert 'sentiment_score' in sentiment_data.columns
                assert 'confidence' in sentiment_data.columns
    
    @pytest.mark.asyncio
    async def test_create_datasets(self, mock_connection_pool, mock_env, sample_data_config):
        """Test dataset creation."""
        pool, conn = mock_connection_pool
        
        # Mock price data response
        dates = pd.date_range('2024-01-01', periods=200, freq='1min')
        mock_price_rows = []
        
        for i, date in enumerate(dates):
            mock_price_rows.append({
                'symbol': 'AAPL',
                'timestamp': date,
                'open': 100.0,
                'high': 101.0,
                'low': 99.0,
                'close': 100.0,
                'volume': 1000,
                'returns': 0.001
            })
        
        conn.fetch.return_value = mock_price_rows
        
        with patch('models.data_loader.SentimentIntegrator'), \
             patch('models.data_loader.analyze_unified_sentiment', return_value={}):
            
            loader = TFTDataLoader(pool, mock_env)
            
            train_dataset, val_dataset, test_dataset, normalizer = await loader.create_datasets(
                ['AAPL'],
                datetime(2024, 1, 1),
                datetime(2024, 1, 10),
                sample_data_config
            )
            
            assert isinstance(train_dataset, TFTDataset)
            assert isinstance(val_dataset, TFTDataset)
            assert isinstance(test_dataset, TFTDataset)
            assert isinstance(normalizer, FeatureNormalizer)
            
            # Check that datasets have data
            assert len(train_dataset) > len(val_dataset)
            assert len(val_dataset) > 0
    
    @pytest.mark.asyncio
    async def test_create_data_loaders(self, mock_connection_pool, mock_env, sample_data_config):
        """Test data loader creation."""
        pool, conn = mock_connection_pool
        
        # Mock sufficient data
        dates = pd.date_range('2024-01-01', periods=300, freq='1min')
        mock_rows = []
        
        for i, date in enumerate(dates):
            mock_rows.append({
                'symbol': 'AAPL',
                'timestamp': date,
                'open': 100.0 + i * 0.01,
                'high': 101.0 + i * 0.01,
                'low': 99.0 + i * 0.01,
                'close': 100.0 + i * 0.01,
                'volume': 1000,
                'returns': 0.01 if i > 0 else None
            })
        
        conn.fetch.return_value = mock_rows
        
        with patch('models.data_loader.SentimentIntegrator'), \
             patch('models.data_loader.analyze_unified_sentiment', return_value={}):
            
            loader = TFTDataLoader(pool, mock_env)
            
            train_loader, val_loader, test_loader, normalizer = await loader.create_data_loaders(
                ['AAPL'],
                datetime(2024, 1, 1),
                datetime(2024, 1, 15),
                sample_data_config,
                batch_size=16
            )
            
            assert hasattr(train_loader, '__iter__')
            assert hasattr(val_loader, '__iter__')
            assert hasattr(test_loader, '__iter__')
            assert isinstance(normalizer, FeatureNormalizer)


class TestCollateFn:
    """Test collate function for variable-length sequences."""
    
    def test_collate_fn_basic(self, sample_data_config):
        """Test basic collate function."""
        # Create sample batch items
        batch_items = []
        
        for i in range(3):
            encoder_len = 50 + i * 5  # Variable lengths
            decoder_len = 15
            
            item = {
                'encoder_input': torch.randn(encoder_len, 6),
                'decoder_input': torch.randn(decoder_len, 6),
                'targets': torch.randn(decoder_len, 1),
                'encoder_lengths': torch.tensor([encoder_len]),
                'symbol': f'STOCK{i}',
                'start_time': datetime.now(),
                'end_time': datetime.now() + timedelta(minutes=decoder_len)
            }
            
            batch_items.append(item)
        
        # Apply collate function
        batch = collate_fn(batch_items)
        
        # Check output structure
        assert 'encoder_input' in batch
        assert 'decoder_input' in batch
        assert 'targets' in batch
        assert 'encoder_lengths' in batch
        assert 'symbols' in batch
        
        # Check tensor shapes
        batch_size = len(batch_items)
        max_encoder_len = max(item['encoder_input'].size(0) for item in batch_items)
        max_decoder_len = max(item['decoder_input'].size(0) for item in batch_items)
        
        assert batch['encoder_input'].shape == (batch_size, max_encoder_len, 6)
        assert batch['decoder_input'].shape == (batch_size, max_decoder_len, 6)
        assert batch['targets'].shape == (batch_size, max_decoder_len, 1)
        assert batch['encoder_lengths'].shape == (batch_size,)
        assert len(batch['symbols']) == batch_size
    
    def test_collate_fn_with_sentiment(self):
        """Test collate function with sentiment features."""
        batch_items = []
        
        for i in range(2):
            encoder_len = 60
            decoder_len = 15
            total_len = encoder_len + decoder_len
            
            item = {
                'encoder_input': torch.randn(encoder_len, 6),
                'decoder_input': torch.randn(decoder_len, 6),
                'targets': torch.randn(decoder_len, 1),
                'encoder_lengths': torch.tensor([encoder_len]),
                'sentiment_features': torch.randn(total_len, 23),
                'symbol': f'STOCK{i}',
                'start_time': datetime.now(),
                'end_time': datetime.now() + timedelta(minutes=decoder_len)
            }
            
            batch_items.append(item)
        
        batch = collate_fn(batch_items)
        
        assert 'sentiment_features' in batch
        assert batch['sentiment_features'].shape == (2, 75, 23)  # batch_size, total_len, sentiment_features
    
    def test_collate_fn_empty_batch(self):
        """Test collate function with empty batch."""
        with pytest.raises((IndexError, ValueError)):
            collate_fn([])


class TestIntegrationScenarios:
    """Test integration scenarios."""
    
    @pytest.mark.asyncio
    async def test_end_to_end_data_pipeline(self, mock_connection_pool, mock_env):
        """Test complete data pipeline from database to dataset."""
        pool, conn = mock_connection_pool
        
        # Mock comprehensive data
        dates = pd.date_range('2024-01-01', periods=500, freq='1min')
        mock_rows = []
        
        for i, date in enumerate(dates):
            price = 100 + np.sin(i * 0.1) * 10  # Synthetic price pattern
            mock_rows.append({
                'symbol': 'AAPL',
                'timestamp': date,
                'open': price,
                'high': price + abs(np.random.randn()),
                'low': price - abs(np.random.randn()),
                'close': price + np.random.randn() * 0.5,
                'volume': 1000 + np.random.randint(-100, 100),
                'returns': np.random.randn() * 0.02 if i > 0 else None
            })
        
        conn.fetch.return_value = mock_rows
        
        # Mock sentiment data
        mock_signal = Mock()
        mock_signal.timestamp = datetime(2024, 1, 1, 10, 0)
        mock_signal.overall_sentiment_score = 0.3
        mock_signal.overall_confidence = 0.85
        mock_signal.volume_indicator = 0.65
        mock_signal.consensus_score = 0.75
        mock_signal.divergence_score = 0.25
        mock_signal.risk_score = 0.35
        mock_signal.sentiment_features = {
            **{f'feature_{i}': np.random.randn() * 0.1 for i in range(1, 17)},
            'sentiment_momentum': 0.15
        }
        
        config = TFTDataConfig(
            encoder_length=120,
            prediction_length=30,
            use_sentiment=True,
            normalize_features=True
        )
        
        with patch('models.data_loader.SentimentIntegrator'), \
             patch('models.data_loader.analyze_unified_sentiment', return_value={'AAPL': mock_signal}):
            
            loader = TFTDataLoader(pool, mock_env)
            
            # Create datasets
            train_dataset, val_dataset, test_dataset, normalizer = await loader.create_datasets(
                ['AAPL'],
                datetime(2024, 1, 1),
                datetime(2024, 1, 20),
                config
            )
            
            # Verify datasets
            assert len(train_dataset) > 0
            assert len(val_dataset) > 0
            assert len(test_dataset) >= 0
            
            # Test data access
            train_item = train_dataset[0]
            
            assert train_item['encoder_input'].shape[0] == config.encoder_length
            assert train_item['decoder_input'].shape[0] == config.prediction_length
            assert train_item['targets'].shape[0] == config.prediction_length
            
            if 'sentiment_features' in train_item:
                expected_sentiment_len = config.encoder_length + config.prediction_length
                assert train_item['sentiment_features'].shape[0] == expected_sentiment_len
                assert train_item['sentiment_features'].shape[1] == 23
    
    def test_data_loading_performance(self, sample_price_data, sample_data_config):
        """Test data loading performance with large datasets."""
        # Create larger dataset
        large_data = pd.concat([sample_price_data] * 10, ignore_index=True)
        
        # Measure dataset creation time
        import time
        start_time = time.time()
        
        dataset = TFTDataset(large_data, sample_data_config)
        
        end_time = time.time()
        creation_time = end_time - start_time
        
        # Should create dataset reasonably quickly
        assert creation_time < 10.0  # 10 seconds max for test data
        assert len(dataset) > 0
        
        # Test data access performance
        start_time = time.time()
        
        for i in range(min(100, len(dataset))):
            item = dataset[i]
            assert 'encoder_input' in item
        
        end_time = time.time()
        access_time = end_time - start_time
        
        # Should access data quickly
        assert access_time < 5.0


class TestErrorHandling:
    """Test error handling scenarios."""
    
    @pytest.mark.asyncio
    async def test_no_data_available(self, mock_connection_pool, mock_env, sample_data_config):
        """Test handling when no data is available."""
        pool, conn = mock_connection_pool
        conn.fetch.return_value = []  # No data
        
        with patch('models.data_loader.SentimentIntegrator'):
            loader = TFTDataLoader(pool, mock_env)
            
            with pytest.raises(ValueError, match="No price data"):
                await loader.create_datasets(
                    ['AAPL'],
                    datetime(2024, 1, 1),
                    datetime(2024, 1, 2),
                    sample_data_config
                )
    
    def test_insufficient_data_for_sequences(self, sample_data_config):
        """Test handling when data is insufficient for sequences."""
        # Create minimal data (less than required sequence length)
        minimal_data = pd.DataFrame([
            {
                'symbol': 'AAPL',
                'timestamp': datetime(2024, 1, 1, 10, 0),
                'open': 100.0,
                'high': 101.0,
                'low': 99.0,
                'close': 100.0,
                'volume': 1000,
                'returns': 0.01
            }
        ])
        
        dataset = TFTDataset(minimal_data, sample_data_config)
        
        # Should create empty dataset
        assert len(dataset) == 0
    
    def test_normalization_with_constant_features(self):
        """Test normalization with constant features."""
        # Create data with constant feature
        data = pd.DataFrame({
            'constant_feature': [100.0] * 100,  # Constant values
            'variable_feature': np.random.randn(100)
        })
        
        normalizer = FeatureNormalizer("standard")
        normalizer.fit(data, ['constant_feature', 'variable_feature'])
        
        # Should handle constant feature gracefully
        normalized_data = normalizer.transform(data, ['constant_feature', 'variable_feature'])
        
        # Constant feature should remain constant (or NaN)
        constant_values = normalized_data['constant_feature'].dropna()
        if len(constant_values) > 0:
            assert constant_values.std() < 1e-6  # Essentially constant


if __name__ == "__main__":
    pytest.main([__file__])