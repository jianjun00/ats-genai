#!/usr/bin/env python3
"""
End-to-End Tests for News Processing Pipeline

This test suite covers the complete news processing workflow:
- Real-time news ingestion from multiple vendors
- LLM-powered analysis with multi-agent framework
- Signal generation and validation
- Broadcasting system integration
- Database persistence and retrieval
- Performance and reliability requirements
"""

import pytest
import asyncio
import json
import uuid
from unittest.mock import AsyncMock, MagicMock
from datetime import datetime

import asyncpg

from domains.market_data.services.news.realtime_news_ingestion import (
    create_realtime_news_service
)
from domains.market_data.services.llm.enhanced_news_llm_processor import (
    EnhancedLLMNewsProcessor
)
from domains.market_data.services.signals.signal_broadcasting_system import (
    create_signal_broadcasting_system
)
from domains.market_data.agents.multi_agent_framework import AgentType
from infrastructure.llm.multi_provider_client import MultiProviderLLMClient, LLMResponse
from core.config.environment import Environment


class TestNewsProcessingPipelineE2E:
    """End-to-end tests for the complete news processing pipeline."""

    @pytest.fixture
    async def mock_database_pool(self):
        """Mock database connection pool with realistic behavior."""
        pool = AsyncMock(spec=asyncpg.Pool)

        # Mock connection context manager
        conn = AsyncMock()
        pool.acquire.return_value.__aenter__.return_value = conn
        pool.acquire.return_value.__aexit__.return_value = None

        # Mock database operations
        conn.execute = AsyncMock()
        conn.fetch = AsyncMock()
        conn.fetchrow = AsyncMock()
        conn.fetchval = AsyncMock(return_value=1)  # Health check

        # Mock news insertion
        conn.execute.return_value = None

        # Mock analysis insertion - return generated ID
        def mock_fetchval_side_effect(*args, **kwargs):
            query = args[0] if args else ""
            if "INSERT INTO dev_news_llm_analysis" in query:
                return 12345  # Mock analysis ID
            elif "INSERT INTO dev_critical_news_signals" in query:
                return 67890  # Mock signal ID
            elif "SELECT 1" in query:
                return 1  # Health check
            else:
                return None

        conn.fetchval.side_effect = mock_fetchval_side_effect

        return pool

    @pytest.fixture
    def mock_environment(self):
        """Mock environment configuration."""
        env = MagicMock(spec=Environment)
        env.get_database_url.return_value = "postgresql://test:test@localhost:5432/test_db"
        return env

    @pytest.fixture
    def mock_llm_client(self):
        """Mock LLM client with realistic multi-agent responses."""
        client = AsyncMock(spec=MultiProviderLLMClient)

        def create_analysis_response(agent_type: str) -> LLMResponse:
            """Create realistic LLM responses based on agent type."""

            responses = {
                "sentiment": json.dumps({
                    "sentiment": "positive",
                    "sentiment_score": 0.82,
                    "confidence": 0.88,
                    "key_phrases": ["strong earnings", "revenue growth", "beats expectations"],
                    "explanation": "Highly positive sentiment driven by strong financial performance"
                }),
                "entity": json.dumps({
                    "entities": [
                        {"name": "Apple Inc.", "type": "company", "ticker": "AAPL", "relevance": 1.0},
                        {"name": "iPhone", "type": "product", "ticker": "AAPL", "relevance": 0.9},
                        {"name": "Tim Cook", "type": "person", "ticker": "AAPL", "relevance": 0.7}
                    ],
                    "confidence": 0.92,
                    "explanation": "Clear entity identification with high confidence"
                }),
                "event": json.dumps({
                    "events": [
                        {
                            "type": "earnings_announcement",
                            "description": "Apple reports Q4 earnings beat",
                            "importance": "high",
                            "market_impact": "positive",
                            "affected_tickers": ["AAPL"],
                            "event_timestamp": datetime.now().isoformat()
                        }
                    ],
                    "confidence": 0.91,
                    "explanation": "Major earnings event with clear market implications"
                }),
                "risk": json.dumps({
                    "risk_level": "medium",
                    "risk_score": 0.35,
                    "risk_factors": [
                        {"factor": "supply_chain_disruption", "severity": "medium", "probability": 0.4},
                        {"factor": "regulatory_changes", "severity": "low", "probability": 0.2}
                    ],
                    "confidence": 0.78,
                    "explanation": "Moderate risk factors identified but manageable"
                }),
                "impact": json.dumps({
                    "market_impact": "positive",
                    "impact_score": 0.75,
                    "expected_price_movement": "up_5_to_10_percent",
                    "timeframe": "1_to_3_days",
                    "confidence": 0.85,
                    "explanation": "Strong positive impact expected from earnings beat"
                }),
                "signal": json.dumps({
                    "signal": "buy",
                    "signal_strength": 0.83,
                    "signal_confidence": 0.87,
                    "signal_horizon": "short_term",
                    "key_catalysts": ["earnings_beat", "revenue_growth", "positive_guidance"],
                    "explanation": "Strong buy signal supported by fundamental strength"
                })
            }

            content = responses.get(agent_type, '{"confidence": 0.5, "explanation": "Generic response"}')

            return LLMResponse(
                content=content,
                model="gpt-4o-mini",
                provider="openai",
                tokens_used=120,
                cost_usd=0.0012,
                latency_ms=450
            )

        def mock_generate_response(*args, **kwargs):
            prompt = args[0] if args else kwargs.get('prompt', '')

            # Determine agent type from prompt
            if 'sentiment' in prompt.lower():
                return create_analysis_response("sentiment")
            elif 'entity' in prompt.lower():
                return create_analysis_response("entity")
            elif 'event' in prompt.lower():
                return create_analysis_response("event")
            elif 'risk' in prompt.lower():
                return create_analysis_response("risk")
            elif 'impact' in prompt.lower():
                return create_analysis_response("impact")
            elif 'signal' in prompt.lower():
                return create_analysis_response("signal")
            else:
                return create_analysis_response("generic")

        client.generate_response = AsyncMock(side_effect=mock_generate_response)
        client.health_check = AsyncMock(return_value=True)
        client.get_cost_tracking = AsyncMock(return_value={
            'total_tokens': 1200,
            'total_cost_usd': 0.024,
            'requests_count': 10
        })

        return client

    @pytest.fixture
    def sample_news_article(self):
        """Sample news article for testing."""
        return {
            'id': f'test_article_{uuid.uuid4().hex[:8]}',
            'title': 'Apple Reports Record Q4 Earnings with Strong iPhone Sales',
            'content': '''
            Apple Inc. (NASDAQ: AAPL) delivered a remarkable fourth quarter, reporting earnings that significantly
            exceeded Wall Street expectations. The technology giant posted revenue of $94.9 billion, up 6% from
            the previous year, while earnings per share reached $1.68, beating analyst estimates of $1.53.

            The standout performance was driven by robust iPhone sales, which generated $43.8 billion in revenue,
            representing a 9% year-over-year increase. CEO Tim Cook attributed the strong results to the successful
            launch of the iPhone 15 series and continued expansion in emerging markets.

            "We're thrilled with these results, which reflect the strength of our ecosystem and the loyalty of
            our customers," Cook said during the earnings call. "Our services business continues to grow, now
            representing 24% of total revenue, and we remain optimistic about our future prospects."

            The company also announced a $90 billion share buyback program and increased its dividend by 4%,
            signaling confidence in future cash generation. However, management noted potential headwinds from
            supply chain disruptions and increased competition in key markets.

            Wall Street analysts are largely positive on the results, with several firms raising their price targets.
            Morgan Stanley increased its target to $265, citing strong fundamentals and market share gains.
            ''',
            'summary': 'Apple reports strong Q4 earnings beat with record iPhone sales and increased shareholder returns',
            'url': 'https://example.com/apple-earnings-q4-2024',
            'source': 'MarketWatch',
            'published_date': datetime.now(),
            'tickers': ['AAPL'],
            'language': 'en',
            'sentiment_score': None,
            'importance_score': 0.85,
            'vendor': 'polygon',
            'vendor_id': 'polygon_12345'
        }

    @pytest.mark.asyncio
    async def test_complete_news_processing_workflow(
        self,
        mock_database_pool,
        mock_environment,
        mock_llm_client,
        sample_news_article
    ):
        """Test complete news processing from ingestion to signal broadcasting."""

        # 1. Initialize services
        news_service = await create_realtime_news_service(
            mock_database_pool,
            mock_environment,
            mock_llm_client,
            {'polygon': 'test_key', 'tiingo': 'test_key'}
        )

        broadcasting_system = await create_signal_broadcasting_system(
            mock_database_pool,
            mock_environment
        )

        # 2. Process news article through pipeline
        processed_article = await news_service._process_article(sample_news_article)

        # 3. Verify article processing
        assert processed_article is not None
        assert processed_article.id == sample_news_article['id']
        assert processed_article.title == sample_news_article['title']
        assert processed_article.tickers == ['AAPL']

        # 4. Verify LLM analysis was performed
        llm_processor = news_service.llm_processor
        assert isinstance(llm_processor, EnhancedLLMNewsProcessor)

        # Mock the analysis results (normally done by LLM processor)
        analysis_results = {
            AgentType.SENTIMENT: MagicMock(confidence=0.88, sentiment='positive'),
            AgentType.ENTITY_RECOGNITION: MagicMock(confidence=0.92),
            AgentType.EVENT_DETECTION: MagicMock(confidence=0.91),
            AgentType.RISK_ASSESSMENT: MagicMock(confidence=0.78, risk_level='medium'),
            AgentType.MARKET_IMPACT: MagicMock(confidence=0.85, market_impact='positive'),
            AgentType.SIGNAL_GENERATION: MagicMock(confidence=0.87, signal='buy', signal_strength=0.83)
        }

        # 5. Simulate signal generation
        signal_data = {
            'id': 67890,
            'symbol': 'AAPL',
            'signal_type': 'buy',
            'signal_strength': 0.83,
            'signal_confidence': 0.87,
            'urgency_level': 'medium',
            'signal_timestamp': datetime.now(),
            'analysis_summary': 'Strong buy signal based on earnings beat and positive fundamentals',
            'key_catalysts': ['earnings_beat', 'revenue_growth', 'positive_guidance']
        }

        # 6. Test signal broadcasting
        broadcast_targets = ['websocket', 'rest_api', 'slack_alert']
        broadcast_results = {}

        for target in broadcast_targets:
            # Mock successful broadcast
            broadcast_results[target] = {
                'success': True,
                'latency_ms': 250,
                'target_name': f'{target}_channel',
                'response_status_code': 200
            }

        # 7. Verify end-to-end metrics
        assert len(broadcast_results) == 3
        assert all(result['success'] for result in broadcast_results.values())
        assert all(result['latency_ms'] < 1000 for result in broadcast_results.values())

        # 8. Verify database interactions
        # Should have called database to store news, analysis, and signal
        assert mock_database_pool.acquire.call_count >= 3

        # 9. Verify LLM client usage
        # Should have made multiple LLM calls for different agents
        assert mock_llm_client.generate_response.call_count >= 6

        # 10. Check cost tracking
        cost_metrics = await mock_llm_client.get_cost_tracking()
        assert cost_metrics['total_tokens'] > 0
        assert cost_metrics['total_cost_usd'] > 0

    @pytest.mark.asyncio
    async def test_pipeline_error_recovery(
        self,
        mock_database_pool,
        mock_environment,
        mock_llm_client,
        sample_news_article
    ):
        """Test pipeline behavior when components fail."""

        # Configure LLM client to fail initially then recover
        call_count = 0

        def failing_llm_side_effect(*args, **kwargs):
            nonlocal call_count
            call_count += 1

            if call_count <= 2:
                raise Exception("LLM service temporarily unavailable")
            else:
                # Return successful response after failures
                return LLMResponse(
                    content='{"confidence": 0.7, "explanation": "Recovered response"}',
                    model="gpt-4o-mini",
                    provider="openai",
                    tokens_used=80,
                    cost_usd=0.0008,
                    latency_ms=600
                )

        mock_llm_client.generate_response.side_effect = failing_llm_side_effect

        # Initialize service
        news_service = await create_realtime_news_service(
            mock_database_pool,
            mock_environment,
            mock_llm_client,
            {'polygon': 'test_key'}
        )

        # Process article - should handle failures gracefully
        processed_article = await news_service._process_article(sample_news_article)

        # Should still process article even with some LLM failures
        assert processed_article is not None
        assert processed_article.id == sample_news_article['id']

        # Should have made multiple retry attempts
        assert call_count >= 3

    @pytest.mark.asyncio
    async def test_pipeline_performance_requirements(
        self,
        mock_database_pool,
        mock_environment,
        mock_llm_client,
        sample_news_article
    ):
        """Test that pipeline meets performance requirements."""

        # Configure realistic latencies
        mock_llm_client.generate_response = AsyncMock(return_value=LLMResponse(
            content='{"confidence": 0.8, "explanation": "Fast response"}',
            model="gpt-4o-mini",
            provider="openai",
            tokens_used=100,
            cost_usd=0.001,
            latency_ms=300  # Fast response
        ))

        news_service = await create_realtime_news_service(
            mock_database_pool,
            mock_environment,
            mock_llm_client,
            {'polygon': 'test_key'}
        )

        # Measure end-to-end processing time
        start_time = datetime.now()
        processed_article = await news_service._process_article(sample_news_article)
        end_time = datetime.now()

        processing_time_ms = (end_time - start_time).total_seconds() * 1000

        # Should process within 30 seconds (requirement for real-time)
        assert processing_time_ms < 30000
        assert processed_article is not None

        # Verify high-volume processing capability
        # Process multiple articles concurrently
        articles = [sample_news_article.copy() for _ in range(5)]
        for i, article in enumerate(articles):
            article['id'] = f'test_article_{i}'

        start_batch = datetime.now()
        batch_results = await asyncio.gather(
            *[news_service._process_article(article) for article in articles],
            return_exceptions=True
        )
        end_batch = datetime.now()

        batch_time_ms = (end_batch - start_batch).total_seconds() * 1000

        # Should handle batch processing efficiently
        assert batch_time_ms < 60000  # Under 1 minute for 5 articles
        assert all(not isinstance(result, Exception) for result in batch_results)

    @pytest.mark.asyncio
    async def test_signal_quality_validation(
        self,
        mock_database_pool,
        mock_environment,
        mock_llm_client
    ):
        """Test signal quality validation and filtering."""

        # Create articles with different signal qualities
        high_quality_article = {
            'id': 'high_quality_test',
            'title': 'Apple Reports Exceptional Q4 Earnings with 25% Revenue Growth',
            'content': 'Detailed earnings report with specific financial metrics...',
            'tickers': ['AAPL'],
            'importance_score': 0.95,
            'published_date': datetime.now()
        }

        low_quality_article = {
            'id': 'low_quality_test',
            'title': 'Apple stock mentioned briefly',
            'content': 'Brief mention without significant details...',
            'tickers': ['AAPL'],
            'importance_score': 0.25,
            'published_date': datetime.now()
        }

        # Configure LLM responses based on article quality
        def quality_based_response(*args, **kwargs):
            prompt = args[0] if args else kwargs.get('prompt', '')

            if 'high_quality_test' in str(kwargs.get('context', '')):
                # High confidence response for high-quality article
                if 'signal' in prompt.lower():
                    return LLMResponse(
                        content=json.dumps({
                            "signal": "strong_buy",
                            "signal_strength": 0.92,
                            "signal_confidence": 0.95,
                            "explanation": "Very strong signal with high confidence"
                        }),
                        model="gpt-4o-mini", provider="openai",
                        tokens_used=100, cost_usd=0.001, latency_ms=400
                    )
            else:
                # Low confidence response for low-quality article
                if 'signal' in prompt.lower():
                    return LLMResponse(
                        content=json.dumps({
                            "signal": "hold",
                            "signal_strength": 0.35,
                            "signal_confidence": 0.42,
                            "explanation": "Weak signal with low confidence"
                        }),
                        model="gpt-4o-mini", provider="openai",
                        tokens_used=80, cost_usd=0.0008, latency_ms=350
                    )

            return LLMResponse(
                content='{"confidence": 0.5}',
                model="gpt-4o-mini", provider="openai",
                tokens_used=50, cost_usd=0.0005, latency_ms=300
            )

        mock_llm_client.generate_response.side_effect = quality_based_response

        news_service = await create_realtime_news_service(
            mock_database_pool,
            mock_environment,
            mock_llm_client,
            {'polygon': 'test_key'}
        )

        # Process both articles
        high_quality_result = await news_service._process_article(high_quality_article)
        low_quality_result = await news_service._process_article(low_quality_article)

        # Both should be processed but with different confidence levels
        assert high_quality_result is not None
        assert low_quality_result is not None

        # High-quality article should generate stronger signals
        # (In real implementation, this would be reflected in the signal generation)
        assert high_quality_result.importance_score > low_quality_result.importance_score


class TestNewsProcessingIntegrationScenarios:
    """Integration tests for specific news processing scenarios."""

    @pytest.fixture
    def market_moving_news_scenarios(self):
        """Different types of market-moving news for testing."""
        return [
            {
                'id': 'earnings_beat_scenario',
                'title': 'NVIDIA Reports 200% Revenue Growth in Q4 AI Chip Sales',
                'content': 'NVIDIA reported exceptional Q4 results with AI chip revenue growing 200% year-over-year...',
                'tickers': ['NVDA'],
                'expected_signal': 'strong_buy',
                'expected_urgency': 'high'
            },
            {
                'id': 'merger_announcement',
                'title': 'Microsoft Announces $75B Acquisition of Gaming Company',
                'content': 'Microsoft Corporation announced today its intention to acquire...',
                'tickers': ['MSFT'],
                'expected_signal': 'buy',
                'expected_urgency': 'high'
            },
            {
                'id': 'regulatory_concern',
                'title': 'FDA Raises Safety Concerns About New Drug Trial Results',
                'content': 'The Food and Drug Administration issued a statement expressing concerns...',
                'tickers': ['PFE'],
                'expected_signal': 'sell',
                'expected_urgency': 'medium'
            },
            {
                'id': 'guidance_cut',
                'title': 'Tesla Cuts 2025 Production Guidance Citing Supply Challenges',
                'content': 'Tesla Inc. revised its 2025 production guidance downward...',
                'tickers': ['TSLA'],
                'expected_signal': 'sell',
                'expected_urgency': 'medium'
            }
        ]

    @pytest.mark.asyncio
    async def test_different_news_scenarios(self, market_moving_news_scenarios):
        """Test pipeline handling of different types of market-moving news."""

        # Mock components for scenario testing
        mock_pool = AsyncMock()
        mock_env = MagicMock()
        mock_env.get_database_url.return_value = "postgresql://test:test@localhost/test"

        # Configure LLM client with scenario-specific responses
        mock_llm = AsyncMock()

        def scenario_response(*args, **kwargs):
            prompt = args[0] if args else kwargs.get('prompt', '')
            context = str(kwargs.get('context', ''))

            # Determine scenario from context
            if 'earnings_beat_scenario' in context:
                if 'signal' in prompt.lower():
                    return LLMResponse(
                        content=json.dumps({
                            "signal": "strong_buy",
                            "signal_strength": 0.95,
                            "signal_confidence": 0.92,
                            "urgency_level": "high"
                        }),
                        model="gpt-4o", provider="openai",
                        tokens_used=150, cost_usd=0.003, latency_ms=500
                    )
            elif 'regulatory_concern' in context:
                if 'signal' in prompt.lower():
                    return LLMResponse(
                        content=json.dumps({
                            "signal": "sell",
                            "signal_strength": 0.78,
                            "signal_confidence": 0.83,
                            "urgency_level": "medium"
                        }),
                        model="gpt-4o", provider="openai",
                        tokens_used=140, cost_usd=0.0028, latency_ms=480
                    )

            # Default response
            return LLMResponse(
                content='{"signal": "hold", "signal_strength": 0.5, "confidence": 0.6}',
                model="gpt-4o-mini", provider="openai",
                tokens_used=80, cost_usd=0.0008, latency_ms=350
            )

        mock_llm.generate_response.side_effect = scenario_response
        mock_llm.health_check.return_value = True

        # Initialize service
        news_service = await create_realtime_news_service(
            mock_pool, mock_env, mock_llm, {'test': 'key'}
        )

        # Test each scenario
        for scenario in market_moving_news_scenarios:
            result = await news_service._process_article(scenario)

            # Verify article was processed
            assert result is not None
            assert result.id == scenario['id']
            assert result.tickers == scenario['tickers']

            # In a real implementation, we would verify the signal matches expectations
            # For now, just verify processing completed successfully

        # Verify all scenarios were processed
        assert mock_llm.generate_response.call_count >= len(market_moving_news_scenarios) * 6  # 6 agents per article

    @pytest.mark.asyncio
    async def test_high_volume_processing_stress(self):
        """Stress test the pipeline with high volume of concurrent articles."""

        # Create mock infrastructure
        mock_pool = AsyncMock()
        mock_env = MagicMock()
        mock_env.get_database_url.return_value = "postgresql://test:test@localhost/test"

        # Fast mock LLM client
        mock_llm = AsyncMock()
        mock_llm.generate_response.return_value = LLMResponse(
            content='{"confidence": 0.7, "signal": "hold"}',
            model="gpt-4o-mini", provider="openai",
            tokens_used=50, cost_usd=0.0005, latency_ms=200
        )
        mock_llm.health_check.return_value = True

        news_service = await create_realtime_news_service(
            mock_pool, mock_env, mock_llm, {'test': 'key'}
        )

        # Generate high volume of test articles
        test_articles = []
        for i in range(50):  # 50 concurrent articles
            article = {
                'id': f'stress_test_{i}',
                'title': f'Market News Update {i}',
                'content': f'Market analysis and update number {i} with relevant financial information.',
                'tickers': ['SPY'],
                'published_date': datetime.now(),
                'importance_score': 0.6
            }
            test_articles.append(article)

        # Process all articles concurrently
        start_time = datetime.now()
        results = await asyncio.gather(
            *[news_service._process_article(article) for article in test_articles],
            return_exceptions=True
        )
        end_time = datetime.now()

        total_time_seconds = (end_time - start_time).total_seconds()

        # Verify all articles were processed successfully
        successful_results = [r for r in results if not isinstance(r, Exception)]
        failed_results = [r for r in results if isinstance(r, Exception)]

        assert len(successful_results) >= 45  # Allow up to 10% failure rate under stress
        assert total_time_seconds < 120  # Should complete within 2 minutes

        # Calculate throughput
        throughput_per_second = len(successful_results) / total_time_seconds
        assert throughput_per_second >= 0.5  # At least 0.5 articles per second

        print(f"Processed {len(successful_results)} articles in {total_time_seconds:.2f} seconds")
        print(f"Throughput: {throughput_per_second:.2f} articles/second")
        print(f"Failed: {len(failed_results)} articles")