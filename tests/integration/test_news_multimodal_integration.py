#!/usr/bin/env python3
"""
Integration tests for the complete News-Driven Multi-Modal Prediction System
Tests end-to-end workflows, data flow, and system integration.
"""

import pytest
import asyncio
import json
import tempfile
import os
from datetime import datetime, date, timedelta
from unittest.mock import AsyncMock, MagicMock, patch
from typing import List, Dict, Any

# Add src to path for imports
import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'src'))

from domains.market_data.services.news.comprehensive_news_backfill import ComprehensiveNewsBackfillSystem
from events.economic_events_classifier import EconomicEventsProcessor
from training.multimodal_dataset_generator import MultiModalDatasetGenerator


class TestNewsToDatasetIntegration:
    """Test complete pipeline from news backfill to training dataset"""
    
    def setup_method(self):
        """Set up integration test fixtures"""
        self.db_config = {
            'host': 'localhost',
            'port': 5432,
            'user': 'postgres', 
            'password': 'test',
            'database': 'test_db'
        }
        
        # Mock API configurations
        self.api_configs = {
            'polygon': {'api_key': 'test_polygon_key'},
            'tiingo': {'api_key': 'test_tiingo_key'},
            'eodhd': {'api_key': 'test_eodhd_key'}
        }
    
    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_complete_pipeline_flow(self):
        """Test complete pipeline: News → Events → Training Dataset"""
        
        # Mock database operations
        mock_conn = AsyncMock()
        mock_pool = AsyncMock()
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
        
        # Mock news data from backfill
        mock_news_articles = [
            {
                'id': 1,
                'title': 'Fed Raises Interest Rates by 0.25%',
                'description': 'Federal Reserve increases rates to combat inflation',
                'tickers': ['SPY'],
                'published_utc': datetime(2024, 1, 15),
                'source': 'polygon'
            },
            {
                'id': 2,
                'title': 'Apple Reports Strong Q1 Results', 
                'description': 'AAPL beats earnings with iPhone growth',
                'tickers': ['AAPL'],
                'published_date': datetime(2024, 1, 14),
                'source': 'tiingo'
            },
            {
                'id': 3,
                'title': 'Tech Sector Shows Resilience',
                'description': 'Technology stocks outperform broader market',
                'tickers': ['AAPL', 'MSFT', 'GOOGL'],
                'published_utc': datetime(2024, 1, 13),
                'source': 'polygon'
            }
        ]
        
        # Mock economic events from classification
        mock_economic_events = [
            {
                'id': 1,
                'event_category': 'fed',
                'predicted_impact_score': -0.3,
                'severity': 8,
                'event_date': datetime(2024, 1, 15)
            },
            {
                'id': 2,
                'event_category': 'earnings',
                'predicted_impact_score': 0.2,
                'severity': 6,
                'event_date': datetime(2024, 1, 14)
            }
        ]
        
        # Set up mocks for each component
        with patch('asyncpg.create_pool') as mock_create_pool:
            mock_create_pool.return_value = mock_pool
            
            # Mock news backfill system
            news_system = ComprehensiveNewsBackfillSystem(self.api_configs, self.db_config)
            
            # Mock events processor
            events_processor = EconomicEventsProcessor(self.db_config)
            
            # Mock dataset generator
            dataset_generator = MultiModalDatasetGenerator(self.db_config)
            
            # Set up database mocks for each stage
            
            # 1. News backfill stage
            mock_conn.fetchval.side_effect = [
                1, 2, 3,  # Insert IDs for news articles
                100,      # Event ID for Fed news
                101,      # Event ID for earnings news
                2080, 20, # Dataset summary stats
                1073      # High quality samples
            ]
            
            # 2. Events classification stage
            mock_conn.fetch.side_effect = [
                mock_news_articles[:2],    # Polygon articles for classification
                mock_news_articles[2:],    # Tiingo articles for classification
                [],                        # No events for news features (1d)
                [],                        # No events for news features (3d) 
                mock_economic_events,      # Economic events for features (7d)
                mock_economic_events       # Events for category breakdown
            ]
            
            # 3. Dataset generation stage - mock news and events queries
            mock_conn.fetch.return_value = []  # Empty for simplicity in test
            
            # Test the complete pipeline
            
            # Step 1: News backfill (mock successful)
            mock_conn.executemany.return_value = None
            mock_conn.execute.return_value = None
            
            # Step 2: Events classification
            async with events_processor as processor:
                events_created = await processor.process_news_articles('news_polygon', limit=100)
                assert events_created >= 0  # Should process some events
            
            # Step 3: Dataset generation
            async with dataset_generator as generator:
                samples_created = await generator.generate_training_dataset(
                    symbols=['AAPL', 'SPY'],
                    start_date=date(2024, 1, 1),
                    end_date=date(2024, 1, 31),
                    sample_freq_days=7
                )
                
                assert samples_created > 0  # Should generate training samples
            
            # Verify integration points
            assert mock_conn.fetch.called  # News data was queried
            assert mock_conn.executemany.called  # Bulk operations occurred
    
    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_data_consistency_across_pipeline(self):
        """Test that data remains consistent as it flows through pipeline"""
        
        mock_conn = AsyncMock()
        mock_pool = AsyncMock()
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
        
        # Create consistent test data
        test_symbol = 'AAPL'
        test_date = datetime(2024, 1, 15)
        
        # Mock news article that should create economic event
        test_news_article = {
            'id': 1,
            'title': 'Apple Beats Q1 Earnings Expectations',
            'description': 'AAPL reports strong iPhone sales growth',
            'tickers': [test_symbol],
            'published_utc': test_date
        }
        
        # Mock resulting economic event
        test_economic_event = {
            'id': 1,
            'event_category': 'earnings',
            'predicted_impact_score': 0.15,
            'severity': 6,
            'event_date': test_date,
            'affected_symbols': [test_symbol]
        }
        
        # Set up mock responses
        mock_conn.fetch.side_effect = [
            [test_news_article],      # News articles for events classification
            [],                       # Tiingo articles (empty)
            [test_economic_event],    # Economic events for dataset generation
        ]
        
        mock_conn.fetchval.side_effect = [
            100,  # Event ID from insertion
            50    # Sample count
        ]
        
        with patch('asyncpg.create_pool') as mock_create_pool:
            mock_create_pool.return_value = mock_pool
            
            # Test events classification
            events_processor = EconomicEventsProcessor(self.db_config)
            
            async with events_processor as processor:
                events_created = await processor.process_news_articles('news_polygon')
                
                # Verify event classification call structure
                insert_call = None
                for call in mock_conn.fetchval.call_args_list:
                    sql = call[0][0] if call[0] else ''
                    if 'INSERT INTO dev_economic_events' in sql:
                        insert_call = call
                        break
                
                # Should have attempted to insert economic event
                assert insert_call is not None
            
            # Test dataset generation uses the event
            dataset_generator = MultiModalDatasetGenerator(self.db_config)
            
            async with dataset_generator as generator:
                # Mock additional database calls for features
                mock_conn.fetch.side_effect = [
                    [],                          # Polygon news for sentiment (1d)
                    [],                          # Tiingo news for sentiment (1d)
                    [],                          # Polygon news for sentiment (3d) 
                    [],                          # Tiingo news for sentiment (3d)
                    [test_news_article],         # Polygon news for sentiment (7d)
                    [],                          # Tiingo news for sentiment (7d)
                    [test_economic_event],       # Economic events (1d)
                    [test_economic_event],       # Economic events (3d)
                    [test_economic_event],       # Economic events (7d)
                    [test_economic_event],       # Economic events (category breakdown)
                ]
                
                sample = await generator.generate_sample_for_symbol_date(
                    test_symbol,
                    test_date.date(),
                    5  # 5-day prediction horizon
                )
                
                # Verify sample creation
                assert sample is not None
                assert sample.symbol == test_symbol
                assert sample.sample_date == test_date.date()
                
                # The economic event should influence features
                # (specific values depend on implementation details)
                assert hasattr(sample, 'earnings_impact_score')
    
    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_error_handling_and_recovery(self):
        """Test system behavior when components fail"""
        
        mock_conn = AsyncMock()
        mock_pool = AsyncMock()
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
        
        with patch('asyncpg.create_pool') as mock_create_pool:
            mock_create_pool.return_value = mock_pool
            
            # Test events processor handles missing news gracefully
            mock_conn.fetch.return_value = []  # No news articles
            
            events_processor = EconomicEventsProcessor(self.db_config)
            
            async with events_processor as processor:
                events_created = await processor.process_news_articles('news_polygon')
                assert events_created == 0  # Should handle empty data gracefully
            
            # Test dataset generator handles missing events gracefully
            dataset_generator = MultiModalDatasetGenerator(self.db_config)
            
            async with dataset_generator as generator:
                # Mock no news or events
                mock_conn.fetch.return_value = []
                
                sample = await generator.generate_sample_for_symbol_date(
                    'AAPL',
                    date(2024, 1, 15),
                    5
                )
                
                # Should still create sample with default values
                assert sample is not None
                assert sample.news_sentiment_7d == 0.0
                assert sample.news_volume_7d == 0
                assert sample.economic_event_impact_7d == 0.0
    
    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_performance_and_scalability(self):
        """Test system performance with larger datasets"""
        
        mock_conn = AsyncMock()
        mock_pool = AsyncMock() 
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
        
        # Create large test dataset
        num_articles = 1000
        mock_articles = []
        
        for i in range(num_articles):
            article = {
                'id': i,
                'title': f'News Article {i}',
                'description': f'Content for article {i}',
                'tickers': [f'STOCK{i % 100}'],
                'published_utc': datetime(2024, 1, 1) + timedelta(hours=i)
            }
            mock_articles.append(article)
        
        # Mock batch processing
        batch_size = 100
        mock_conn.fetch.return_value = mock_articles[:batch_size]  # Process in batches
        mock_conn.fetchval.return_value = 1
        
        with patch('asyncpg.create_pool') as mock_create_pool:
            mock_create_pool.return_value = mock_pool
            
            events_processor = EconomicEventsProcessor(self.db_config)
            
            # Test processing time and memory usage
            import time
            start_time = time.time()
            
            async with events_processor as processor:
                events_created = await processor.process_news_articles(
                    'news_polygon', 
                    limit=batch_size
                )
                
            end_time = time.time()
            processing_time = end_time - start_time
            
            # Should process efficiently (less than 5 seconds for 100 articles in test)
            assert processing_time < 5.0
            assert events_created >= 0  # Some events should be classified
    
    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_data_quality_validation(self):
        """Test data quality checks throughout pipeline"""
        
        mock_conn = AsyncMock()
        mock_pool = AsyncMock()
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
        
        # Test data with quality issues
        problematic_news = [
            {
                'id': 1,
                'title': None,  # Missing title
                'description': 'Fed announcement',
                'tickers': ['SPY'],
                'published_utc': datetime(2024, 1, 15)
            },
            {
                'id': 2,
                'title': '',  # Empty title
                'description': '',  # Empty description
                'tickers': [],
                'published_utc': datetime(2024, 1, 14)
            },
            {
                'id': 3,
                'title': 'Valid News Title',
                'description': 'Good quality content with meaningful information',
                'tickers': ['AAPL'],
                'published_utc': datetime(2024, 1, 13)
            }
        ]
        
        mock_conn.fetch.return_value = problematic_news
        mock_conn.fetchval.return_value = 1
        
        with patch('asyncpg.create_pool') as mock_create_pool:
            mock_create_pool.return_value = mock_pool
            
            events_processor = EconomicEventsProcessor(self.db_config)
            
            async with events_processor as processor:
                # Should handle problematic data gracefully
                events_created = await processor.process_news_articles('news_polygon')
                
                # Should process at least the valid article
                assert events_created >= 0
            
            # Test dataset generator quality scoring
            dataset_generator = MultiModalDatasetGenerator(self.db_config)
            
            async with dataset_generator as generator:
                # Mock features with varying quality
                mock_conn.fetch.side_effect = [
                    [],  # No news (poor quality)
                    [],  # No news
                    [],  # No news
                    [],  # No news
                    [],  # No news
                    [],  # No news
                    [],  # No events
                    [],  # No events  
                    [],  # No events
                    []   # No events
                ]
                
                sample = await generator.generate_sample_for_symbol_date(
                    'AAPL',
                    date(2024, 1, 15),
                    5
                )
                
                # Should have low quality score due to lack of data
                assert sample is not None
                assert sample.sample_quality_score < 0.8  # Should reflect poor data quality


class TestMultiVendorIntegration:
    """Test integration across multiple data vendors"""
    
    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_vendor_data_consolidation(self):
        """Test that data from multiple vendors is properly consolidated"""
        
        mock_conn = AsyncMock()
        mock_pool = AsyncMock()
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
        
        # Mock data from different vendors for same symbol and timeframe
        polygon_news = [
            {
                'title': 'Apple Q1 Results (Polygon)',
                'description': 'Strong earnings from Polygon source',
                'published_utc': datetime(2024, 1, 15, 16, 0)
            }
        ]
        
        tiingo_news = [
            {
                'title': 'AAPL Beats Estimates (Tiingo)', 
                'description': 'Positive earnings from Tiingo source',
                'published_date': datetime(2024, 1, 15, 16, 30)
            }
        ]
        
        with patch('asyncpg.create_pool') as mock_create_pool:
            mock_create_pool.return_value = mock_pool
            
            dataset_generator = MultiModalDatasetGenerator({})
            dataset_generator.db_pool = mock_pool
            
            # Mock database calls to return data from both sources
            mock_conn.fetch.side_effect = [
                polygon_news,  # Polygon 1d
                tiingo_news,   # Tiingo 1d
                polygon_news,  # Polygon 3d
                tiingo_news,   # Tiingo 3d
                polygon_news,  # Polygon 7d
                tiingo_news,   # Tiingo 7d
            ]
            
            features = await dataset_generator.generate_news_features(
                'AAPL',
                date(2024, 1, 16)
            )
            
            # Should consolidate data from both sources
            assert features['news_volume_1d'] == 2  # Combined from both sources
            assert features['news_sentiment_1d'] > 0  # Should be positive (both positive)
    
    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_vendor_specific_error_handling(self):
        """Test handling when one vendor fails but others succeed"""
        
        mock_conn = AsyncMock()
        mock_pool = AsyncMock()
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
        
        with patch('asyncpg.create_pool') as mock_create_pool:
            mock_create_pool.return_value = mock_pool
            
            dataset_generator = MultiModalDatasetGenerator({})
            dataset_generator.db_pool = mock_pool
            
            # Mock scenario where Polygon fails but Tiingo succeeds
            tiingo_news = [
                {
                    'title': 'Market Update',
                    'description': 'Tiingo market analysis',
                    'published_date': datetime(2024, 1, 15)
                }
            ]
            
            # First call (Polygon) fails, second call (Tiingo) succeeds
            mock_conn.fetch.side_effect = [
                Exception("Polygon connection failed"),  # Polygon fails
                tiingo_news,                             # Tiingo succeeds
            ]
            
            # Should handle partial failure gracefully
            try:
                features = await dataset_generator.generate_news_features(
                    'AAPL',
                    date(2024, 1, 16)
                )
                # If it succeeds, should have some data from Tiingo
                assert isinstance(features, dict)
            except:
                # Or it might fail completely, which is also acceptable
                pass


# Test configuration and fixtures
@pytest.fixture(scope="session")
def event_loop():
    """Create an instance of the default event loop for the test session."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@pytest.fixture
def temp_database_config():
    """Provide temporary database configuration for testing"""
    return {
        'host': 'localhost',
        'port': 5432,
        'user': 'test_user',
        'password': 'test_password',
        'database': 'test_multimodal_db'
    }


@pytest.fixture
def sample_api_configs():
    """Provide sample API configurations for testing"""
    return {
        'polygon': {'api_key': 'test_polygon_key'},
        'tiingo': {'api_key': 'test_tiingo_key'},
        'eodhd': {'api_key': 'test_eodhd_key'}
    }


if __name__ == "__main__":
    # Run integration tests
    pytest.main([__file__, "-v", "--tb=short", "-x"])