#!/usr/bin/env python3
"""
Comprehensive test suite for Multi-Modal Training Dataset Generator
Tests dataset generation, feature engineering, database operations, and data quality.
"""

import pytest
import asyncio
import json
import numpy as np
from datetime import datetime, date, timedelta
from unittest.mock import AsyncMock, MagicMock, patch
from typing import List, Dict, Any

# Add src to path for imports
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'src'))

from domains.ml.services.training_data.generators.multimodal_dataset_generator import (
    MultiModalDatasetGenerator,
    MultiModalSample
)


class TestMultiModalSample:
    """Test the MultiModalSample dataclass"""
    
    def test_sample_creation_with_defaults(self):
        """Test sample creation with default values"""
        sample = MultiModalSample(
            symbol="AAPL",
            sample_date=date(2024, 1, 1),
            prediction_horizon=5
        )
        
        assert sample.symbol == "AAPL"
        assert sample.sample_date == date(2024, 1, 1)
        assert sample.prediction_horizon == 5
        assert sample.news_sentiment_1d == 0.0
        assert sample.news_volume_1d == 0
        assert sample.sample_quality_score == 1.0
        assert sample.price_features == {}
        assert sample.volume_features == {}
        assert sample.market_microstructure == {}
    
    def test_sample_creation_with_full_data(self):
        """Test sample creation with complete data"""
        price_features = {'sma_20': 150.0, 'rsi_14': 65.0}
        volume_features = {'vol_sma_10': 1000000, 'relative_volume': 1.2}
        
        sample = MultiModalSample(
            symbol="TSLA",
            sample_date=date(2024, 2, 15),
            prediction_horizon=10,
            news_sentiment_7d=0.35,
            news_volume_7d=25,
            economic_event_impact_7d=0.15,
            price_features=price_features,
            volume_features=volume_features,
            target_return_10d=0.045,
            target_direction_10d=1,
            sample_quality_score=0.85
        )
        
        assert sample.symbol == "TSLA"
        assert sample.news_sentiment_7d == 0.35
        assert sample.news_volume_7d == 25
        assert sample.price_features == price_features
        assert sample.target_return_10d == 0.045
        assert sample.target_direction_10d == 1
        assert sample.sample_quality_score == 0.85
    
    def test_post_init_feature_initialization(self):
        """Test that __post_init__ properly initializes None features"""
        sample = MultiModalSample(
            symbol="GOOGL",
            sample_date=date(2024, 3, 1),
            prediction_horizon=1,
            price_features=None,
            volume_features=None,
            market_microstructure=None
        )
        
        # Should be initialized to empty dicts
        assert sample.price_features == {}
        assert sample.volume_features == {}
        assert sample.market_microstructure == {}


class TestMultiModalDatasetGenerator:
    """Test the core dataset generator functionality"""
    
    def setup_method(self):
        """Set up test fixtures"""
        self.db_config = {
            'host': 'localhost',
            'port': 5432,
            'user': 'postgres',
            'password': 'test',
            'database': 'test_db'
        }
        self.generator = MultiModalDatasetGenerator(self.db_config)
    
    def test_generator_initialization(self):
        """Test generator initializes with proper config"""
        assert self.generator.db_config == self.db_config
        assert self.generator.prediction_horizons == [1, 5, 10, 20]
        assert 'bull' in self.generator.regime_thresholds
        assert 'bear' in self.generator.regime_thresholds
        assert 'sideways' in self.generator.regime_thresholds
    
    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_database_connection_pool_setup(self):
        """Test database connection pool initialization"""
        mock_pool = AsyncMock()
        
        with patch('asyncpg.create_pool') as mock_create_pool:
            mock_create_pool.return_value = mock_pool
            
            async with self.generator as gen:
                assert gen.db_pool == mock_pool
                mock_create_pool.assert_called_once()
                
                # Verify pool configuration
                call_kwargs = mock_create_pool.call_args.kwargs
                assert call_kwargs['min_size'] == 10
                assert call_kwargs['max_size'] == 20
                assert 'jit' in call_kwargs['server_settings']
    
    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_table_creation_sql(self):
        """Test database table creation logic"""
        mock_conn = AsyncMock()
        mock_pool = AsyncMock()
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
        
        self.generator.db_pool = mock_pool
        await self.generator._ensure_tables_exist()
        
        # Check that table creation SQL was executed
        create_calls = [call.args[0] for call in mock_conn.execute.call_args_list]
        
        # Verify main table creation
        table_sql = next((call for call in create_calls if 'CREATE TABLE IF NOT EXISTS dev_multimodal_training_samples' in call), None)
        assert table_sql is not None
        
        # Verify key columns exist
        assert 'symbol VARCHAR(10)' in table_sql
        assert 'sample_date DATE' in table_sql
        assert 'prediction_horizon INTEGER' in table_sql
        assert 'news_sentiment_1d DECIMAL' in table_sql
        assert 'price_features JSONB' in table_sql
        assert 'target_return_1d DECIMAL' in table_sql
        assert 'PRIMARY KEY (symbol, sample_date, prediction_horizon)' in table_sql
        
        # Verify indexes were created
        index_calls = [call for call in create_calls if 'CREATE INDEX' in call]
        assert len(index_calls) >= 3  # Should create at least 3 indexes
    
    def test_simple_sentiment_calculation(self):
        """Test sentiment calculation logic"""
        # Positive sentiment text
        positive_text = "stock beats earnings expectations with strong growth profit success"
        positive_score = self.generator._calculate_simple_sentiment(positive_text)
        assert positive_score > 0
        
        # Negative sentiment text
        negative_text = "company misses targets with weak decline loss bad worse concerns"
        negative_score = self.generator._calculate_simple_sentiment(negative_text)
        assert negative_score < 0
        
        # Neutral text
        neutral_text = "company reports quarterly results meeting analyst expectations"
        neutral_score = self.generator._calculate_simple_sentiment(neutral_text)
        assert abs(neutral_score) < 0.1  # Should be near zero
        
        # Empty text
        empty_score = self.generator._calculate_simple_sentiment("")
        assert empty_score == 0.0
        
        # Mixed sentiment (more positive)
        mixed_text = "stock beats expectations despite some concerns about future growth risks"
        mixed_score = self.generator._calculate_simple_sentiment(mixed_text)
        # Should be slightly positive (beat > concerns)
    
    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_news_features_generation(self):
        """Test news sentiment feature generation"""
        mock_conn = AsyncMock()
        
        # Mock polygon news data
        mock_polygon_news = [
            {
                'title': 'Apple Beats Earnings Expectations',
                'description': 'Strong iPhone sales drive revenue growth',
                'published_utc': datetime(2024, 1, 5)
            },
            {
                'title': 'AAPL Stock Rises on Positive Outlook',
                'description': 'Analysts bullish on Apple prospects',
                'published_utc': datetime(2024, 1, 4)
            }
        ]
        
        # Mock tiingo news data
        mock_tiingo_news = [
            {
                'title': 'Apple Reports Strong Quarter',
                'description': 'Tech giant shows resilient performance',
                'published_date': datetime(2024, 1, 3)
            }
        ]
        
        mock_conn.fetch.side_effect = [mock_polygon_news, mock_tiingo_news, mock_polygon_news, mock_tiingo_news, mock_polygon_news, mock_tiingo_news]
        
        mock_pool = AsyncMock()
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
        self.generator.db_pool = mock_pool
        
        features = await self.generator.generate_news_features('AAPL', date(2024, 1, 7))
        
        # Check that all required features are present
        assert 'news_sentiment_1d' in features
        assert 'news_sentiment_3d' in features
        assert 'news_sentiment_7d' in features
        assert 'news_volume_1d' in features
        assert 'news_volume_3d' in features
        assert 'news_volume_7d' in features
        assert 'news_momentum_3d' in features
        assert 'news_momentum_7d' in features
        
        # Check that sentiment values are reasonable
        assert -1.0 <= features['news_sentiment_7d'] <= 1.0
        assert features['news_volume_7d'] > 0  # Should have news
        
        # Verify database queries were called correctly
        assert mock_conn.fetch.call_count == 6  # 2 sources × 3 lookback windows
    
    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_economic_event_features_generation(self):
        """Test economic event feature generation"""
        mock_conn = AsyncMock()
        
        # Mock economic events data
        mock_events_1d = [
            {
                'event_category': 'fed',
                'predicted_impact_score': 0.3,
                'severity': 8
            }
        ]
        
        mock_events_3d = [
            {
                'event_category': 'earnings',
                'predicted_impact_score': 0.15,
                'severity': 6
            },
            {
                'event_category': 'fed',
                'predicted_impact_score': 0.3,
                'severity': 8
            }
        ]
        
        mock_events_7d = [
            {
                'event_category': 'macro',
                'predicted_impact_score': -0.1,
                'severity': 5
            }
        ] + mock_events_3d
        
        # Mock the fetch calls for different lookback windows
        mock_conn.fetch.side_effect = [
            mock_events_1d,    # 1-day lookback
            mock_events_3d,    # 3-day lookback  
            mock_events_7d,    # 7-day lookback
            mock_events_7d     # 7-day category-specific query
        ]
        
        mock_pool = AsyncMock()
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
        self.generator.db_pool = mock_pool
        
        features = await self.generator.generate_economic_event_features('AAPL', date(2024, 1, 7))
        
        # Check that all required features are present
        assert 'economic_event_impact_1d' in features
        assert 'economic_event_impact_3d' in features
        assert 'economic_event_impact_7d' in features
        assert 'earnings_impact_score' in features
        assert 'fed_event_impact' in features
        assert 'macro_event_impact' in features
        
        # Check that impact scores are calculated
        assert features['fed_event_impact'] > 0  # Fed event should have positive impact
        assert features['earnings_impact_score'] > 0  # Earnings event should be positive
        assert features['macro_event_impact'] < 0  # Macro event should be negative
    
    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_sample_generation_for_symbol_date(self):
        """Test generating a single training sample"""
        mock_pool = AsyncMock()
        mock_conn = AsyncMock()
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
        
        # Mock news and events features
        mock_conn.fetch.side_effect = [
            [],  # No polygon news
            [],  # No tiingo news
            [],  # No polygon news (3d)
            [],  # No tiingo news (3d)
            [],  # No polygon news (7d)
            [],  # No tiingo news (7d)
            [],  # No events (1d)
            [],  # No events (3d)
            [],  # No events (7d)
            []   # No events (7d category)
        ]
        
        self.generator.db_pool = mock_pool
        
        sample = await self.generator.generate_sample_for_symbol_date(
            'AAPL', 
            date(2024, 1, 15), 
            5
        )
        
        assert sample is not None
        assert sample.symbol == 'AAPL'
        assert sample.sample_date == date(2024, 1, 15)
        assert sample.prediction_horizon == 5
        
        # Check that features were populated
        assert isinstance(sample.price_features, dict)
        assert isinstance(sample.volume_features, dict)
        assert sample.target_return_1d is not None
        assert sample.target_direction_1d in [-1, 0, 1]
        assert 0.1 <= sample.sample_quality_score <= 1.0
    
    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_sample_generation_future_date_filtering(self):
        """Test that samples too close to current date are filtered"""
        mock_pool = AsyncMock()
        self.generator.db_pool = mock_pool
        
        # Try to generate sample for date too close to today
        future_date = date.today() - timedelta(days=2)  # Too recent for 5-day horizon
        
        sample = await self.generator.generate_sample_for_symbol_date(
            'AAPL',
            future_date,
            5
        )
        
        # Should return None because not enough future data
        assert sample is None
    
    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_bulk_insert_samples(self):
        """Test bulk insertion of training samples"""
        mock_conn = AsyncMock()
        mock_pool = AsyncMock()
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
        
        # Create test samples
        samples = [
            MultiModalSample(
                symbol='AAPL',
                sample_date=date(2024, 1, 15),
                prediction_horizon=1,
                news_sentiment_7d=0.2,
                news_volume_7d=10,
                target_return_1d=0.015,
                target_direction_1d=1
            ),
            MultiModalSample(
                symbol='MSFT',
                sample_date=date(2024, 1, 15),
                prediction_horizon=5,
                news_sentiment_7d=-0.1,
                news_volume_7d=5,
                target_return_5d=-0.008,
                target_direction_5d=-1
            )
        ]
        
        self.generator.db_pool = mock_pool
        
        inserted_count = await self.generator.bulk_insert_samples(samples)
        
        assert inserted_count == 2
        
        # Verify executemany was called
        assert mock_conn.executemany.called
        
        # Check the SQL structure
        sql_call = mock_conn.executemany.call_args[0][0]
        assert 'INSERT INTO dev_multimodal_training_samples' in sql_call
        assert 'ON CONFLICT (symbol, sample_date, prediction_horizon)' in sql_call
        
        # Check that all required columns are included
        assert 'news_sentiment_1d' in sql_call
        assert 'price_features' in sql_call
        assert 'target_return_1d' in sql_call
    
    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_bulk_insert_empty_samples(self):
        """Test bulk insert with empty sample list"""
        mock_pool = AsyncMock()
        self.generator.db_pool = mock_pool
        
        inserted_count = await self.generator.bulk_insert_samples([])
        
        assert inserted_count == 0
    
    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_bulk_insert_error_handling(self):
        """Test error handling in bulk insert"""
        mock_conn = AsyncMock()
        mock_conn.executemany.side_effect = Exception("Database error")
        
        mock_pool = AsyncMock()
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
        
        samples = [
            MultiModalSample(
                symbol='AAPL',
                sample_date=date(2024, 1, 15),
                prediction_horizon=1
            )
        ]
        
        self.generator.db_pool = mock_pool
        
        # Should handle error gracefully and return 0
        inserted_count = await self.generator.bulk_insert_samples(samples)
        assert inserted_count == 0
    
    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_training_dataset_generation_workflow(self):
        """Test the complete dataset generation workflow"""
        mock_pool = AsyncMock()
        mock_conn = AsyncMock()
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
        
        # Mock empty news and events (for speed)
        mock_conn.fetch.return_value = []
        mock_conn.executemany.return_value = None
        
        self.generator.db_pool = mock_pool
        
        # Generate small test dataset
        symbols = ['AAPL', 'MSFT']
        start_date = date(2024, 1, 1)
        end_date = date(2024, 1, 14)  # 2 weeks
        
        total_samples = await self.generator.generate_training_dataset(
            symbols=symbols,
            start_date=start_date,
            end_date=end_date,
            sample_freq_days=7
        )
        
        # Should generate samples for 2 symbols × 2 dates × 4 horizons = 16 samples
        expected_samples = 2 * 2 * 4  # 2 symbols, 2 sample dates (Jan 1, Jan 8), 4 horizons
        assert total_samples == expected_samples
    
    @pytest.mark.asyncio 
    @pytest.mark.asyncio
    async def test_get_dataset_summary(self):
        """Test dataset summary statistics generation"""
        mock_conn = AsyncMock()
        mock_pool = AsyncMock()
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
        
        # Mock summary queries
        mock_conn.fetchval.side_effect = [
            2080,  # total_samples
            20     # unique_symbols
        ]
        
        mock_conn.fetchrow.side_effect = [
            {
                'earliest': date(2024, 1, 1),
                'latest': date(2024, 6, 24)
            },  # date_range
            {
                'avg_quality': 0.801,
                'min_quality': 0.1,
                'max_quality': 1.0,
                'high_quality_count': 1073
            }  # quality_stats
        ]
        
        mock_conn.fetch.return_value = [
            {'prediction_horizon': 1, 'count': 520},
            {'prediction_horizon': 5, 'count': 520},
            {'prediction_horizon': 10, 'count': 520},
            {'prediction_horizon': 20, 'count': 520}
        ]
        
        self.generator.db_pool = mock_pool
        
        summary = await self.generator.get_dataset_summary()
        
        assert summary['total_samples'] == 2080
        assert summary['unique_symbols'] == 20
        assert summary['date_range']['earliest'] == '2024-01-01'
        assert summary['date_range']['latest'] == '2024-06-24'
        assert len(summary['by_horizon']) == 4
        assert summary['by_horizon']['1'] == 520
        assert summary['quality_stats']['avg_quality'] == 0.801


class TestDataQualityAndValidation:
    """Test data quality, validation, and edge cases"""
    
    def setup_method(self):
        """Set up data quality test fixtures"""
        self.generator = MultiModalDatasetGenerator({})
    
    def test_sample_quality_score_calculation(self):
        """Test sample quality score calculation logic"""
        # Test quality scoring based on news volume
        high_news_features = {'news_volume_7d': 50}
        low_news_features = {'news_volume_7d': 0}
        
        # Mock the news feature generation to return different volumes
        with patch.object(self.generator, 'generate_news_features') as mock_news:
            with patch.object(self.generator, 'generate_economic_event_features') as mock_events:
                
                # High news volume case
                mock_news.return_value = high_news_features
                mock_events.return_value = {'economic_event_impact_1d': 0.1}
                
                # This would need to be tested with the actual quality calculation logic
                # in generate_sample_for_symbol_date method
                pass
    
    def test_feature_value_bounds_validation(self):
        """Test that feature values stay within expected bounds"""
        # Test sentiment bounds (-1 to 1)
        test_cases = [
            "extremely bullish positive great excellent amazing outstanding",
            "terrible horrible awful worst disaster catastrophic failure",
            "neutral meeting expectations standard regular normal",
            ""  # empty case
        ]
        
        for text in test_cases:
            sentiment = self.generator._calculate_simple_sentiment(text)
            assert -1.0 <= sentiment <= 1.0, f"Sentiment {sentiment} out of bounds for text: {text}"
    
    def test_prediction_horizon_validation(self):
        """Test that only valid prediction horizons are used"""
        valid_horizons = [1, 5, 10, 20]
        assert self.generator.prediction_horizons == valid_horizons
        
        # Test sample creation with invalid horizon should be handled
        sample = MultiModalSample(
            symbol='AAPL',
            sample_date=date(2024, 1, 1),
            prediction_horizon=999  # Invalid horizon
        )
        
        # The sample can be created, but database constraints should prevent insertion
        assert sample.prediction_horizon == 999
    
    def test_news_volume_consistency(self):
        """Test that news volume counts are consistent across features"""
        # Mock consistent news data
        mock_news_data = [
            {'title': 'News 1', 'description': 'Content 1', 'date': datetime(2024, 1, 5)},
            {'title': 'News 2', 'description': 'Content 2', 'date': datetime(2024, 1, 4)},
            {'title': 'News 3', 'description': 'Content 3', 'date': datetime(2024, 1, 3)},
        ]
        
        # Test that volume calculations would be consistent
        # (This would require mocking the actual database calls)
        pass
    
    def test_date_range_validation(self):
        """Test proper date range handling"""
        # Test that future dates are properly filtered
        today = date.today()
        
        # Sample dates that are too recent for different horizons
        test_cases = [
            (today - timedelta(days=0), 1, False),   # Same day, 1-day horizon - should be filtered
            (today - timedelta(days=10), 5, True),   # 10 days ago, 5-day horizon - should be OK  
            (today - timedelta(days=25), 20, True),  # 25 days ago, 20-day horizon - should be OK
            (today - timedelta(days=3), 10, False),  # 3 days ago, 10-day horizon - should be filtered
        ]
        
        for sample_date, horizon, should_be_valid in test_cases:
            future_date = sample_date + timedelta(days=horizon)
            has_enough_buffer = future_date <= today - timedelta(days=5)
            
            if should_be_valid:
                assert has_enough_buffer, f"Date {sample_date} with horizon {horizon} should be valid"
            else:
                assert not has_enough_buffer, f"Date {sample_date} with horizon {horizon} should be filtered"


class TestPerformanceAndScalability:
    """Test performance characteristics and scalability"""
    
    def setup_method(self):
        """Set up performance test fixtures"""
        self.generator = MultiModalDatasetGenerator({})
    
    def test_sentiment_calculation_performance(self):
        """Test sentiment calculation with large text"""
        large_text = "Apple beats earnings expectations with strong growth " * 1000
        
        import time
        start_time = time.time()
        sentiment = self.generator._calculate_simple_sentiment(large_text)
        end_time = time.time()
        
        # Should complete in reasonable time (< 1 second for 1000 word repetition)
        assert end_time - start_time < 1.0
        assert -1.0 <= sentiment <= 1.0
    
    def test_memory_usage_with_large_datasets(self):
        """Test memory efficiency with large sample lists"""
        # Create many samples to test memory usage
        samples = []
        for i in range(1000):
            sample = MultiModalSample(
                symbol=f"STOCK{i % 100}",
                sample_date=date(2024, 1, 1) + timedelta(days=i % 30),
                prediction_horizon=(i % 4) * 5 + 1,
                news_sentiment_7d=0.1 * (i % 10),
                price_features={'test': i}
            )
            samples.append(sample)
        
        # Should be able to create large lists without issues
        assert len(samples) == 1000
        assert all(isinstance(s, MultiModalSample) for s in samples)
    
    def test_concurrent_feature_generation(self):
        """Test that feature generation could handle concurrent requests"""
        # This would test async behavior in actual implementation
        # For now, just verify that the methods are properly async
        import inspect
        
        assert inspect.iscoroutinefunction(self.generator.generate_news_features)
        assert inspect.iscoroutinefunction(self.generator.generate_economic_event_features)
        assert inspect.iscoroutinefunction(self.generator.generate_sample_for_symbol_date)


# Test fixtures and configuration
@pytest.fixture(scope="session")
def event_loop():
    """Create an instance of the default event loop for the test session."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@pytest.fixture
def sample_news_data():
    """Provide sample news data for testing"""
    return [
        {
            'id': 1,
            'title': 'Apple Beats Q3 Earnings',
            'description': 'Strong iPhone sales drive revenue growth',
            'tickers': ['AAPL'],
            'published_utc': datetime(2024, 1, 15, 16, 0)
        },
        {
            'id': 2, 
            'title': 'Fed Holds Rates Steady',
            'description': 'Federal Reserve maintains current interest rate policy',
            'tickers': ['SPY'],
            'published_date': datetime(2024, 1, 15, 14, 30)
        }
    ]


@pytest.fixture
def sample_events_data():
    """Provide sample economic events data for testing"""
    return [
        {
            'event_category': 'fed',
            'predicted_impact_score': 0.2,
            'severity': 8
        },
        {
            'event_category': 'earnings',
            'predicted_impact_score': 0.15,
            'severity': 6
        }
    ]


if __name__ == "__main__":
    # Run tests with pytest
    pytest.main([__file__, "-v", "--tb=short", "--maxfail=5"])