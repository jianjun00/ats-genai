#!/usr/bin/env python3
"""
Comprehensive test suite for Economic Events Classification System
Tests pattern matching, classification accuracy, database operations, and edge cases.
"""

import pytest
import asyncio
import json
from datetime import datetime, date, timedelta
from unittest.mock import AsyncMock, MagicMock, patch
from typing import List, Dict, Any

# Add src to path for imports
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'src'))

from events.economic_events_classifier import (
    EconomicEventsClassifier,
    EconomicEventsProcessor,
    EconomicEvent,
    EventCategory,
    EventSeverity
)


class TestEconomicEventsClassifier:
    """Test the core classification logic"""

    def setup_method(self):
        """Set up test fixtures"""
        self.classifier = EconomicEventsClassifier()

    def test_classifier_initialization(self):
        """Test classifier initializes properly"""
        assert self.classifier is not None
        assert hasattr(self.classifier, '_compiled_patterns')
        assert 'fed' in self.classifier._compiled_patterns
        assert 'employment' in self.classifier._compiled_patterns
        assert 'inflation' in self.classifier._compiled_patterns

    def test_fed_rate_decision_classification(self):
        """Test Federal Reserve rate decision classification"""
        title = "Fed Raises Interest Rates by 0.75%"
        description = "The Federal Reserve announced a rate hike at today's FOMC meeting"

        event = self.classifier.classify_news_article(
            title=title,
            description=description,
            symbols=['SPY'],
            published_date=datetime.now()
        )

        assert event is not None
        assert event.event_category == EventCategory.FEDERAL_RESERVE
        assert event.event_type in ['rate_hike', 'rate_decision']
        assert event.confidence_score >= 0.7
        assert event.severity.value >= 7  # Fed events are high severity
        assert event.predicted_impact_score < 0  # Rate hikes generally negative

    def test_earnings_beat_classification(self):
        """Test earnings announcement classification"""
        title = "Apple Reports Q3 Earnings Beat"
        description = "AAPL beats EPS estimates with strong iPhone sales driving revenue growth"

        event = self.classifier.classify_news_article(
            title=title,
            description=description,
            symbols=['AAPL'],
            published_date=datetime.now()
        )

        assert event is not None
        assert event.event_category == EventCategory.CORPORATE
        assert event.event_type == 'earnings_announcement'
        assert 'AAPL' in event.affected_symbols
        assert event.predicted_impact_score > 0  # Earnings beat is positive

    def test_unemployment_data_classification(self):
        """Test employment data classification"""
        title = "U.S. Unemployment Rate Falls to 3.5%"
        description = "Latest jobs report shows strong labor market with nonfarm payrolls up"

        event = self.classifier.classify_news_article(
            title=title,
            description=description,
            symbols=[],
            published_date=datetime.now()
        )

        assert event is not None
        assert event.event_category == EventCategory.EMPLOYMENT
        assert event.event_type in ['jobs_report', 'unemployment']
        assert event.confidence_score >= 0.6
        assert 'market' in event.affected_sectors

    def test_inflation_cpi_classification(self):
        """Test inflation data classification"""
        title = "CPI Data Shows Core Inflation Rising"
        description = "Consumer price index increases 0.3% month-over-month, core inflation up"

        event = self.classifier.classify_news_article(
            title=title,
            description=description,
            symbols=[],
            published_date=datetime.now()
        )

        assert event is not None
        assert event.event_category == EventCategory.INFLATION
        assert event.event_type == 'cpi_release'
        assert event.predicted_impact_score < 0  # Inflation rise generally negative

    def test_gdp_growth_classification(self):
        """Test GDP data classification"""
        title = "Q2 GDP Growth Exceeds Expectations"
        description = "Quarterly GDP data shows economic expansion at 2.8% annualized rate"

        event = self.classifier.classify_news_article(
            title=title,
            description=description,
            symbols=[],
            published_date=datetime.now()
        )

        assert event is not None
        assert event.event_category == EventCategory.GDP_GROWTH
        assert event.event_type == 'gdp_release'
        assert event.predicted_impact_score > 0  # GDP growth is positive

    def test_no_classification_for_irrelevant_news(self):
        """Test that irrelevant news is not classified"""
        title = "Celebrity News: Actor Wins Award"
        description = "Entertainment industry celebrates achievements at annual ceremony"

        event = self.classifier.classify_news_article(
            title=title,
            description=description,
            symbols=[],
            published_date=datetime.now()
        )

        assert event is None

    def test_severity_calculation_with_keywords(self):
        """Test severity calculation with impact keywords"""
        title = "SURPRISE Fed Rate Cut Shocks Markets"
        description = "Unexpected dramatic monetary policy change catches investors off guard"

        event = self.classifier.classify_news_article(
            title=title,
            description=description,
            symbols=['SPY', 'QQQ', 'IWM'],  # Multiple symbols for market impact
            published_date=datetime.now()
        )

        assert event is not None
        assert event.severity.value >= 8  # Should be high due to surprise keywords

    def test_sector_identification(self):
        """Test affected sector identification"""
        title = "Bank Earnings Show Strong Quarter"
        description = "Financial sector reports beat expectations with credit growth"

        event = self.classifier.classify_news_article(
            title=title,
            description=description,
            symbols=['JPM', 'BAC'],
            published_date=datetime.now()
        )

        assert event is not None
        assert 'financial' in event.affected_sectors
        assert 'market' in event.affected_sectors

    def test_confidence_score_title_vs_description(self):
        """Test confidence scores higher for title matches"""
        # Title match case
        title1 = "Fed Cuts Interest Rates"
        description1 = "Other financial news here"

        event1 = self.classifier.classify_news_article(
            title=title1,
            description=description1,
            symbols=[],
            published_date=datetime.now()
        )

        # Description only match case
        title2 = "Financial News Update"
        description2 = "The Federal Reserve decided to cut interest rates today"

        event2 = self.classifier.classify_news_article(
            title=title2,
            description=description2,
            symbols=[],
            published_date=datetime.now()
        )

        assert event1 is not None
        assert event2 is not None
        assert event1.confidence_score > event2.confidence_score

    def test_impact_score_bounds(self):
        """Test that impact scores stay within -1 to 1 bounds"""
        title = "MASSIVE Fed Rate Hike Surprise Shocks Global Markets"
        description = "Dramatic unexpected significant major monetary policy tightening"

        event = self.classifier.classify_news_article(
            title=title,
            description=description,
            symbols=['SPY'] * 50,  # Many symbols to test bounds
            published_date=datetime.now()
        )

        assert event is not None
        assert -1.0 <= event.predicted_impact_score <= 1.0

    def test_edge_case_empty_strings(self):
        """Test handling of empty title/description"""
        event = self.classifier.classify_news_article(
            title="",
            description="",
            symbols=[],
            published_date=datetime.now()
        )

        assert event is None

    def test_edge_case_none_values(self):
        """Test handling of None values"""
        event = self.classifier.classify_news_article(
            title="Fed Rate Decision",
            description=None,
            symbols=None,
            published_date=None
        )

        assert event is not None  # Should handle None gracefully
        assert isinstance(event.affected_symbols, list)
        assert isinstance(event.event_date, datetime)


class TestEconomicEventsProcessor:
    """Test the database processing logic"""

    def setup_method(self):
        """Set up test fixtures"""
        self.db_config = {
            'host': 'localhost',
            'port': 5432,
            'user': 'postgres',
            'password': 'test',
            'database': 'test_db'
        }
        self.processor = EconomicEventsProcessor(self.db_config)

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_processor_initialization(self):
        """Test processor initializes with classifier"""
        assert self.processor.classifier is not None
        assert self.processor.db_config == self.db_config

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_table_creation_logic(self):
        """Test database table creation SQL"""
        mock_conn = AsyncMock()

        with patch.object(self.processor, 'db_pool') as mock_pool:
            mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
            await self.processor._ensure_tables_exist()

            # Verify economic events table creation
            create_calls = [call.args[0] for call in mock_conn.execute.call_args_list]

            # Check that CREATE TABLE statements were called
            assert any('CREATE TABLE IF NOT EXISTS dev_economic_events' in call for call in create_calls)
            assert any('CREATE TABLE IF NOT EXISTS dev_news_economic_events' in call for call in create_calls)

            # Check indexes were created
            assert any('CREATE INDEX' in call for call in create_calls)

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_news_article_processing_flow(self):
        """Test end-to-end news processing workflow"""
        mock_conn = AsyncMock()

        # Mock news articles data
        mock_articles = [
            {
                'id': 1,
                'title': 'Fed Raises Rates',
                'description': 'Federal Reserve increases interest rates by 0.25%',
                'tickers': ['SPY'],
                'published_date': datetime.now()
            },
            {
                'id': 2,
                'title': 'Apple Beats Earnings',
                'description': 'AAPL reports strong quarterly results',
                'tickers': ['AAPL'],
                'published_date': datetime.now()
            },
            {
                'id': 3,
                'title': 'Weather Update',
                'description': 'Sunny skies expected tomorrow',
                'tickers': [],
                'published_date': datetime.now()
            }
        ]

        mock_conn.fetch.return_value = mock_articles
        mock_conn.fetchval.return_value = 100  # Mock event ID

        with patch.object(self.processor, 'db_pool') as mock_pool:
            mock_pool.acquire.return_value.__aenter__.return_value = mock_conn

            events_created = await self.processor.process_news_articles('news_polygon', limit=10)

            # Should have processed 2 relevant events (Fed + Apple), skipped weather
            assert events_created == 2

            # Verify database operations
            assert mock_conn.fetch.called
            assert mock_conn.fetchval.call_count == 2  # Two events inserted
            assert mock_conn.execute.call_count == 2   # Two news-event mappings

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_confidence_threshold_filtering(self):
        """Test that low confidence events are filtered out"""
        mock_conn = AsyncMock()

        # Create classifier that returns low confidence
        with patch.object(self.processor.classifier, 'classify_news_article') as mock_classify:
            # First article: high confidence (should be included)
            event1 = MagicMock()
            event1.confidence_score = 0.8
            event1.event_type = 'rate_decision'
            event1.event_subtype = 'rate_decision'
            event1.event_category.value = 'fed'
            event1.severity.value = 8
            event1.affected_symbols = ['SPY']
            event1.affected_sectors = ['market']
            event1.affected_regions = ['US']
            event1.event_date = datetime.now()
            event1.announcement_date = datetime.now()
            event1.predicted_impact_score = -0.2
            event1.title = 'Fed Decision'
            event1.description = 'Rate decision made'
            event1.raw_data = {}
            event1.data_vendor = 'news_classifier'

            # Second article: low confidence (should be filtered)
            event2 = MagicMock()
            event2.confidence_score = 0.3

            mock_classify.side_effect = [event1, event2]

            mock_articles = [
                {'id': 1, 'title': 'Fed News', 'description': 'Fed decision', 'tickers': [], 'published_date': datetime.now()},
                {'id': 2, 'title': 'Low Confidence', 'description': 'Maybe economic', 'tickers': [], 'published_date': datetime.now()}
            ]

            mock_conn.fetch.return_value = mock_articles
            mock_conn.fetchval.return_value = 100

            with patch.object(self.processor, 'db_pool') as mock_pool:
                mock_pool.acquire.return_value.__aenter__.return_value = mock_conn

                events_created = await self.processor.process_news_articles('news_test')

                # Only high confidence event should be processed
                assert events_created == 1
                assert mock_conn.fetchval.call_count == 1

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_database_error_handling(self):
        """Test graceful handling of database errors"""
        mock_conn = AsyncMock()
        mock_conn.fetch.side_effect = Exception("Database connection error")

        with patch.object(self.processor, 'db_pool') as mock_pool:
            mock_pool.acquire.return_value.__aenter__.return_value = mock_conn

            # Should not raise exception, should handle gracefully
            with pytest.raises(Exception):
                await self.processor.process_news_articles('news_test')

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_duplicate_event_handling(self):
        """Test handling of duplicate news-event mappings"""
        mock_conn = AsyncMock()
        mock_conn.execute.side_effect = [None]  # ON CONFLICT DO NOTHING

        mock_articles = [
            {
                'id': 1,
                'title': 'Fed Rate Decision',
                'description': 'FOMC meeting results',
                'tickers': ['SPY'],
                'published_date': datetime.now()
            }
        ]

        mock_conn.fetch.return_value = mock_articles
        mock_conn.fetchval.return_value = 100

        with patch.object(self.processor, 'db_pool') as mock_pool:
            mock_pool.acquire.return_value.__aenter__.return_value = mock_conn

            events_created = await self.processor.process_news_articles('news_polygon')

            assert events_created == 1

            # Verify ON CONFLICT handling in SQL
            execute_calls = [call.args[0] for call in mock_conn.execute.call_args_list]
            assert any('ON CONFLICT' in call for call in execute_calls)


class TestEconomicEventsIntegration:
    """Integration tests with real-like data scenarios"""

    def setup_method(self):
        """Set up integration test fixtures"""
        self.classifier = EconomicEventsClassifier()

    def test_fed_meeting_comprehensive_analysis(self):
        """Test comprehensive Fed meeting classification"""
        scenarios = [
            {
                'title': 'Fed Holds Rates Steady at 5.25%-5.50%',
                'description': 'Federal Open Market Committee maintains target range, signals potential future cuts',
                'expected_type': 'rate_decision',
                'expected_impact_range': (-0.1, 0.1)  # Neutral decision
            },
            {
                'title': 'Fed Cuts Rates by 50 Basis Points in Emergency Move',
                'description': 'Surprise monetary policy easing amid economic concerns',
                'expected_type': 'rate_cut',
                'expected_impact_range': (0.3, 0.8)  # Very positive for stocks
            },
            {
                'title': 'Fed Announces New QE Program',
                'description': 'Central bank to purchase $500B in government bonds monthly',
                'expected_type': 'qe_announcement',
                'expected_impact_range': (0.4, 0.8)  # Very positive
            }
        ]

        for scenario in scenarios:
            event = self.classifier.classify_news_article(
                title=scenario['title'],
                description=scenario['description'],
                symbols=['SPY', 'QQQ'],
                published_date=datetime.now()
            )

            assert event is not None
            assert event.event_category == EventCategory.FEDERAL_RESERVE
            assert event.event_type == scenario['expected_type']
            assert scenario['expected_impact_range'][0] <= event.predicted_impact_score <= scenario['expected_impact_range'][1]
            assert event.severity.value >= 7  # Fed events are always high severity

    def test_earnings_season_patterns(self):
        """Test various earnings announcement patterns"""
        earnings_scenarios = [
            {
                'title': 'Microsoft Beats Q3 Estimates on Cloud Growth',
                'description': 'MSFT reports EPS of $2.45 vs $2.30 expected, revenue up 12%',
                'symbol': 'MSFT',
                'expected_positive': True
            },
            {
                'title': 'Tesla Misses Revenue Targets Despite EPS Beat',
                'description': 'TSLA earnings mixed with production concerns weighing on outlook',
                'symbol': 'TSLA',
                'expected_positive': False  # Revenue miss more important
            },
            {
                'title': 'Apple Provides Strong Forward Guidance',
                'description': 'AAPL raises full-year outlook on iPhone demand strength',
                'symbol': 'AAPL',
                'expected_positive': True
            }
        ]

        for scenario in earnings_scenarios:
            event = self.classifier.classify_news_article(
                title=scenario['title'],
                description=scenario['description'],
                symbols=[scenario['symbol']],
                published_date=datetime.now()
            )

            assert event is not None
            assert event.event_category == EventCategory.CORPORATE
            assert scenario['symbol'] in event.affected_symbols

            if scenario['expected_positive']:
                assert event.predicted_impact_score > 0
            else:
                assert event.predicted_impact_score <= 0

    def test_macro_economic_indicators(self):
        """Test classification of major economic indicators"""
        macro_scenarios = [
            {
                'title': 'U.S. GDP Growth Slows to 1.2% in Q3',
                'description': 'Economic expansion decelerates amid consumer spending weakness',
                'category': EventCategory.GDP_GROWTH,
                'severity_min': 6
            },
            {
                'title': 'Core CPI Rises 0.4% Monthly, Above Expectations',
                'description': 'Inflation pressures persist with shelter costs driving increase',
                'category': EventCategory.INFLATION,
                'severity_min': 5
            },
            {
                'title': 'Unemployment Jumps to 4.1% as Job Market Cools',
                'description': 'Labor market shows signs of weakening with layoffs increasing',
                'category': EventCategory.EMPLOYMENT,
                'severity_min': 6
            }
        ]

        for scenario in macro_scenarios:
            event = self.classifier.classify_news_article(
                title=scenario['title'],
                description=scenario['description'],
                symbols=[],
                published_date=datetime.now()
            )

            assert event is not None
            assert event.event_category == scenario['category']
            assert event.severity.value >= scenario['severity_min']
            assert event.confidence_score >= 0.5


class TestEconomicEventsEdgeCases:
    """Test edge cases and error conditions"""

    def setup_method(self):
        """Set up edge case test fixtures"""
        self.classifier = EconomicEventsClassifier()

    def test_malformed_input_handling(self):
        """Test handling of malformed inputs"""
        edge_cases = [
            {'title': None, 'description': 'Fed news', 'symbols': ['SPY']},
            {'title': 'Fed news', 'description': None, 'symbols': ['SPY']},
            {'title': 'Fed news', 'description': 'Rate decision', 'symbols': None},
            {'title': '', 'description': '', 'symbols': []},
            {'title': 'A' * 10000, 'description': 'Fed decision', 'symbols': ['SPY']},  # Very long title
        ]

        for case in edge_cases:
            # Should not raise exceptions
            event = self.classifier.classify_news_article(
                title=case['title'],
                description=case['description'],
                symbols=case['symbols'],
                published_date=datetime.now()
            )

            # Some may return None, but shouldn't crash
            if event:
                assert isinstance(event.affected_symbols, list)
                assert isinstance(event.event_date, datetime)

    def test_ambiguous_classification_handling(self):
        """Test handling of articles that could match multiple categories"""
        ambiguous_title = "Fed Employment Report Shows Rate Decision Impact"
        ambiguous_description = "Federal Reserve labor market analysis influences monetary policy"

        event = self.classifier.classify_news_article(
            title=ambiguous_title,
            description=ambiguous_description,
            symbols=[],
            published_date=datetime.now()
        )

        # Should classify as Fed (highest priority) even with employment keywords
        assert event is not None
        assert event.event_category == EventCategory.FEDERAL_RESERVE

    def test_performance_with_large_symbol_lists(self):
        """Test performance with large lists of affected symbols"""
        large_symbol_list = [f"STOCK{i}" for i in range(1000)]

        event = self.classifier.classify_news_article(
            title="Fed Cuts Rates",
            description="Broad market impact expected",
            symbols=large_symbol_list,
            published_date=datetime.now()
        )

        assert event is not None
        assert len(event.affected_symbols) == 1000
        assert event.severity.value >= 8  # High severity due to broad impact


# Pytest configuration and test discovery
@pytest.fixture(scope="session")
def event_loop():
    """Create an instance of the default event loop for the test session."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


if __name__ == "__main__":
    # Run tests with pytest
    pytest.main([__file__, "-v", "--tb=short"])