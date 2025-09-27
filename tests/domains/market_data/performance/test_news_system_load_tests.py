#!/usr/bin/env python3
"""
Performance and Load Tests for News Processing System

This test suite focuses on:
- Load testing with high volume of concurrent news articles
- Performance benchmarking of LLM analysis pipeline
- Memory usage and resource consumption monitoring
- Latency and throughput measurements
- Scalability testing under different loads
- Circuit breaker and rate limiting behavior under load
"""

import pytest
import asyncio
import time
import psutil
import statistics
from unittest.mock import AsyncMock, MagicMock
from datetime import datetime
from typing import List, Dict, Any
import json
import uuid

from domains.market_data.services.vendor_adapters.news.realtime_news_ingestion import (
    create_realtime_news_service
)
from infrastructure.llm.multi_provider_client import MultiProviderLLMClient, LLMResponse


class PerformanceMetrics:
    """Helper class to track performance metrics during tests."""

    def __init__(self):
        self.start_time = None
        self.end_time = None
        self.memory_samples = []
        self.cpu_samples = []
        self.processing_times = []
        self.llm_call_times = []
        self.database_call_times = []

    def start_monitoring(self):
        """Start performance monitoring."""
        self.start_time = time.time()
        self.memory_samples = []
        self.cpu_samples = []

    def sample_system_metrics(self):
        """Sample current system metrics."""
        process = psutil.Process()
        self.memory_samples.append(process.memory_info().rss / 1024 / 1024)  # MB
        self.cpu_samples.append(process.cpu_percent())

    def stop_monitoring(self):
        """Stop monitoring and calculate final metrics."""
        self.end_time = time.time()

    def get_summary(self) -> Dict[str, Any]:
        """Get performance summary."""
        total_time = self.end_time - self.start_time if self.end_time and self.start_time else 0

        return {
            'total_time_seconds': total_time,
            'avg_memory_mb': statistics.mean(self.memory_samples) if self.memory_samples else 0,
            'max_memory_mb': max(self.memory_samples) if self.memory_samples else 0,
            'avg_cpu_percent': statistics.mean(self.cpu_samples) if self.cpu_samples else 0,
            'max_cpu_percent': max(self.cpu_samples) if self.cpu_samples else 0,
            'avg_processing_time_ms': statistics.mean(self.processing_times) if self.processing_times else 0,
            'p95_processing_time_ms': self._percentile(self.processing_times, 0.95) if self.processing_times else 0,
            'throughput_per_second': len(self.processing_times) / total_time if total_time > 0 else 0
        }

    def _percentile(self, data: List[float], percentile: float) -> float:
        """Calculate percentile of data."""
        if not data:
            return 0
        sorted_data = sorted(data)
        index = int(percentile * len(sorted_data))
        return sorted_data[min(index, len(sorted_data) - 1)]


class TestNewsSystemLoadTests:
    """Load tests for the news processing system."""

    @pytest.fixture
    def performance_metrics(self):
        """Performance metrics tracker."""
        return PerformanceMetrics()

    @pytest.fixture
    def mock_database_pool_optimized(self):
        """Optimized mock database pool for load testing."""
        pool = AsyncMock()

        # Fast database responses
        conn = AsyncMock()
        pool.acquire.return_value.__aenter__.return_value = conn
        pool.acquire.return_value.__aexit__.return_value = None

        # Simulate realistic database latencies
        async def mock_execute(*args, **kwargs):
            await asyncio.sleep(0.01)  # 10ms database latency
            return None

        async def mock_fetchval(*args, **kwargs):
            await asyncio.sleep(0.005)  # 5ms for simple queries
            query = args[0] if args else ""
            if "INSERT" in query:
                return 12345  # Mock ID
            return 1  # Health check

        conn.execute = mock_execute
        conn.fetchval = mock_fetchval
        conn.fetch = AsyncMock(return_value=[])
        conn.fetchrow = AsyncMock(return_value=None)

        return pool

    @pytest.fixture
    def mock_llm_client_performance(self):
        """Mock LLM client optimized for performance testing."""
        client = AsyncMock(spec=MultiProviderLLMClient)

        async def fast_generate_response(*args, **kwargs):
            # Simulate variable LLM latencies (200-800ms)
            latency = 200 + (hash(str(args)) % 600)
            await asyncio.sleep(latency / 1000)

            prompt = args[0] if args else kwargs.get('prompt', '')

            # Return appropriate response based on agent type
            if 'sentiment' in prompt.lower():
                content = json.dumps({
                    "sentiment": "positive",
                    "sentiment_score": 0.8,
                    "confidence": 0.85,
                    "key_phrases": ["growth", "strong", "beat"]
                })
            elif 'entity' in prompt.lower():
                content = json.dumps({
                    "entities": [{"name": "TestCorp", "ticker": "TEST", "relevance": 0.9}],
                    "confidence": 0.9
                })
            elif 'signal' in prompt.lower():
                content = json.dumps({
                    "signal": "buy",
                    "signal_strength": 0.8,
                    "signal_confidence": 0.85
                })
            else:
                content = '{"confidence": 0.8}'

            return LLMResponse(
                content=content,
                model="gpt-4o-mini",
                provider="openai",
                tokens_used=100,
                cost_usd=0.001,
                latency_ms=latency
            )

        client.generate_response = fast_generate_response
        client.health_check = AsyncMock(return_value=True)

        return client

    @pytest.fixture
    def test_articles_generator(self):
        """Generate test articles for load testing."""
        def generate_articles(count: int) -> List[Dict[str, Any]]:
            articles = []
            companies = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA', 'META', 'NVDA', 'NFLX']

            for i in range(count):
                ticker = companies[i % len(companies)]
                articles.append({
                    'id': f'load_test_{uuid.uuid4().hex[:8]}',
                    'title': f'{ticker} Market Update - Performance Analysis {i}',
                    'content': f'''
                    {ticker} Corporation reported strong quarterly results with revenue growth
                    exceeding analyst expectations. The company demonstrated solid fundamentals
                    across key business segments, with particular strength in core operations.
                    Management provided positive guidance for the upcoming quarter, citing
                    favorable market conditions and operational efficiency improvements.
                    Key financial metrics showed improvement year-over-year, including
                    margin expansion and strong cash flow generation.
                    ''',
                    'summary': f'{ticker} reports strong quarterly results with positive outlook',
                    'url': f'https://example.com/news/{ticker.lower()}-{i}',
                    'source': 'LoadTestSource',
                    'published_date': datetime.now(),
                    'tickers': [ticker],
                    'language': 'en',
                    'importance_score': 0.7,
                    'vendor': 'test_vendor',
                    'vendor_id': f'test_{i}'
                })

            return articles

        return generate_articles

    @pytest.mark.asyncio
    async def test_low_volume_baseline_performance(
        self,
        mock_database_pool_optimized,
        mock_llm_client_performance,
        test_articles_generator,
        performance_metrics
    ):
        """Establish baseline performance with low volume."""

        # Setup
        env = MagicMock()
        env.get_database_url.return_value = "postgresql://test:test@localhost/test"

        news_service = await create_realtime_news_service(
            mock_database_pool_optimized,
            env,
            mock_llm_client_performance,
            {'test': 'key'}
        )

        # Generate small batch of articles
        articles = test_articles_generator(10)

        # Start monitoring
        performance_metrics.start_monitoring()

        # Process articles sequentially to establish baseline
        results = []
        for article in articles:
            start_time = time.time()
            result = await news_service._process_article(article)
            end_time = time.time()

            processing_time_ms = (end_time - start_time) * 1000
            performance_metrics.processing_times.append(processing_time_ms)
            performance_metrics.sample_system_metrics()

            results.append(result)

        performance_metrics.stop_monitoring()

        # Verify results
        assert len(results) == 10
        assert all(r is not None for r in results)

        # Performance assertions for baseline
        metrics = performance_metrics.get_summary()
        assert metrics['avg_processing_time_ms'] < 10000  # Under 10 seconds per article
        assert metrics['p95_processing_time_ms'] < 15000  # 95th percentile under 15 seconds
        assert metrics['max_memory_mb'] < 1000  # Under 1GB memory usage

        print(f"Baseline Performance Metrics: {metrics}")

    @pytest.mark.asyncio
    async def test_medium_volume_concurrent_processing(
        self,
        mock_database_pool_optimized,
        mock_llm_client_performance,
        test_articles_generator,
        performance_metrics
    ):
        """Test medium volume with concurrent processing."""

        env = MagicMock()
        env.get_database_url.return_value = "postgresql://test:test@localhost/test"

        news_service = await create_realtime_news_service(
            mock_database_pool_optimized,
            env,
            mock_llm_client_performance,
            {'test': 'key'}
        )

        # Generate medium batch
        articles = test_articles_generator(50)

        performance_metrics.start_monitoring()

        # Process articles concurrently
        async def process_with_timing(article):
            start_time = time.time()
            result = await news_service._process_article(article)
            end_time = time.time()

            processing_time_ms = (end_time - start_time) * 1000
            performance_metrics.processing_times.append(processing_time_ms)
            return result

        # Monitor system during processing
        async def system_monitor():
            while performance_metrics.start_time and not performance_metrics.end_time:
                performance_metrics.sample_system_metrics()
                await asyncio.sleep(1)

        monitor_task = asyncio.create_task(system_monitor())

        # Process all articles concurrently
        results = await asyncio.gather(
            *[process_with_timing(article) for article in articles],
            return_exceptions=True
        )

        performance_metrics.stop_monitoring()
        monitor_task.cancel()

        # Analyze results
        successful_results = [r for r in results if not isinstance(r, Exception)]
        failed_results = [r for r in results if isinstance(r, Exception)]

        assert len(successful_results) >= 45  # Allow 10% failure rate

        metrics = performance_metrics.get_summary()

        # Performance requirements for medium load
        assert metrics['throughput_per_second'] >= 1.0  # At least 1 article/second
        assert metrics['avg_processing_time_ms'] < 8000  # Average under 8 seconds
        assert metrics['p95_processing_time_ms'] < 20000  # 95th percentile under 20 seconds
        assert metrics['max_memory_mb'] < 2000  # Under 2GB memory

        print(f"Medium Volume Metrics: {metrics}")
        print(f"Success rate: {len(successful_results)}/{len(articles)} ({len(successful_results)/len(articles)*100:.1f}%)")

    @pytest.mark.asyncio
    async def test_high_volume_stress_testing(
        self,
        mock_database_pool_optimized,
        mock_llm_client_performance,
        test_articles_generator,
        performance_metrics
    ):
        """Stress test with high volume of articles."""

        env = MagicMock()
        env.get_database_url.return_value = "postgresql://test:test@localhost/test"

        news_service = await create_realtime_news_service(
            mock_database_pool_optimized,
            env,
            mock_llm_client_performance,
            {'test': 'key'}
        )

        # Generate high volume batch
        articles = test_articles_generator(100)

        performance_metrics.start_monitoring()

        # Process in batches to manage concurrency
        batch_size = 20
        all_results = []

        for i in range(0, len(articles), batch_size):
            batch = articles[i:i + batch_size]

            # Process batch
            batch_results = await asyncio.gather(
                *[news_service._process_article(article) for article in batch],
                return_exceptions=True
            )

            all_results.extend(batch_results)

            # Sample metrics after each batch
            performance_metrics.sample_system_metrics()

            # Brief pause between batches
            await asyncio.sleep(0.1)

        performance_metrics.stop_monitoring()

        # Analyze stress test results
        successful_results = [r for r in all_results if not isinstance(r, Exception)]
        failed_results = [r for r in all_results if isinstance(r, Exception)]

        success_rate = len(successful_results) / len(articles)

        # Under stress, allow higher failure rate but expect decent throughput
        assert success_rate >= 0.8  # 80% success rate minimum under stress

        metrics = performance_metrics.get_summary()

        # Stress test requirements (more relaxed)
        assert metrics['throughput_per_second'] >= 0.8  # At least 0.8 articles/second under stress
        assert metrics['max_memory_mb'] < 4000  # Memory should not exceed 4GB
        assert metrics['max_cpu_percent'] < 95  # Should not max out CPU completely

        print(f"High Volume Stress Test Metrics: {metrics}")
        print(f"Success rate under stress: {success_rate*100:.1f}%")
        print(f"Failed articles: {len(failed_results)}")

    @pytest.mark.asyncio
    async def test_sustained_load_endurance(
        self,
        mock_database_pool_optimized,
        mock_llm_client_performance,
        test_articles_generator,
        performance_metrics
    ):
        """Test sustained load over time (endurance test)."""

        env = MagicMock()
        env.get_database_url.return_value = "postgresql://test:test@localhost/test"

        news_service = await create_realtime_news_service(
            mock_database_pool_optimized,
            env,
            mock_llm_client_performance,
            {'test': 'key'}
        )

        performance_metrics.start_monitoring()

        # Run sustained load for 2 minutes
        test_duration_seconds = 120
        end_time = time.time() + test_duration_seconds

        processed_count = 0
        error_count = 0

        while time.time() < end_time:
            # Generate and process articles continuously
            articles = test_articles_generator(5)  # Small batches

            batch_results = await asyncio.gather(
                *[news_service._process_article(article) for article in articles],
                return_exceptions=True
            )

            successful = len([r for r in batch_results if not isinstance(r, Exception)])
            failed = len(batch_results) - successful

            processed_count += successful
            error_count += failed

            # Sample metrics periodically
            if processed_count % 10 == 0:
                performance_metrics.sample_system_metrics()

            await asyncio.sleep(2)

        performance_metrics.stop_monitoring()

        metrics = performance_metrics.get_summary()

        # Endurance test requirements
        total_processed = processed_count + error_count
        success_rate = processed_count / total_processed if total_processed > 0 else 0

        assert success_rate >= 0.85  # 85% success rate for sustained load
        assert processed_count >= 20  # Should process at least 20 articles in 2 minutes
        assert metrics['avg_memory_mb'] < 3000  # Memory should remain stable

        print(f"Sustained Load Metrics: {metrics}")
        print(f"Total processed: {processed_count}, Errors: {error_count}")
        print(f"Sustained success rate: {success_rate*100:.1f}%")

    @pytest.mark.asyncio
    async def test_memory_leak_detection(
        self,
        mock_database_pool_optimized,
        mock_llm_client_performance,
        test_articles_generator
    ):
        """Test for memory leaks during extended processing."""

        env = MagicMock()
        env.get_database_url.return_value = "postgresql://test:test@localhost/test"

        news_service = await create_realtime_news_service(
            mock_database_pool_optimized,
            env,
            mock_llm_client_performance,
            {'test': 'key'}
        )

        # Collect memory samples over multiple processing cycles
        memory_samples = []
        process = psutil.Process()

        # Initial memory sample
        initial_memory = process.memory_info().rss / 1024 / 1024
        memory_samples.append(initial_memory)

        # Process articles in multiple cycles
        for cycle in range(10):
            articles = test_articles_generator(5)

            # Process articles
            await asyncio.gather(
                *[news_service._process_article(article) for article in articles],
                return_exceptions=True
            )

            # Force garbage collection
            import gc
            gc.collect()

            # Sample memory after each cycle
            current_memory = process.memory_info().rss / 1024 / 1024
            memory_samples.append(current_memory)

            await asyncio.sleep(1)  # Brief pause between cycles

        # Analyze memory trend
        final_memory = memory_samples[-1]
        memory_growth = final_memory - initial_memory

        # Calculate memory growth rate
        memory_growth_rate = memory_growth / len(memory_samples)

        print(f"Memory samples: {memory_samples}")
        print(f"Initial memory: {initial_memory:.1f} MB")
        print(f"Final memory: {final_memory:.1f} MB")
        print(f"Total growth: {memory_growth:.1f} MB")
        print(f"Growth rate: {memory_growth_rate:.2f} MB/cycle")

        # Memory leak detection
        assert memory_growth < 500  # Should not grow by more than 500MB
        assert memory_growth_rate < 50  # Should not grow by more than 50MB per cycle

        # Check for stable memory usage (no continuous growth)
        recent_samples = memory_samples[-5:]  # Last 5 samples
        if len(recent_samples) >= 5:
            memory_trend = statistics.mean(recent_samples[-3:]) - statistics.mean(recent_samples[:2])
            assert memory_trend < 100  # Recent trend should be stable


class TestLLMProviderLoadBalance:
    """Test load balancing and failover under high load."""

    @pytest.fixture
    def multi_provider_llm_client(self):
        """Mock multi-provider LLM client for load testing."""
        client = AsyncMock()

        # Track provider usage
        provider_calls = {'openai': 0, 'anthropic': 0, 'google': 0}

        async def load_balanced_response(*args, **kwargs):
            # Simulate load balancing by rotating providers
            total_calls = sum(provider_calls.values())
            provider = ['openai', 'anthropic', 'google'][total_calls % 3]
            provider_calls[provider] += 1

            # Simulate different latencies for providers
            latencies = {'openai': 400, 'anthropic': 600, 'google': 500}
            latency = latencies[provider]
            await asyncio.sleep(latency / 1000)

            return LLMResponse(
                content='{"confidence": 0.8, "signal": "hold"}',
                model=f"{provider}-model",
                provider=provider,
                tokens_used=100,
                cost_usd=0.001,
                latency_ms=latency
            )

        client.generate_response = load_balanced_response
        client.health_check = AsyncMock(return_value=True)
        client.get_provider_usage = lambda: provider_calls

        return client

    @pytest.mark.asyncio
    async def test_provider_load_balancing(
        self,
        multi_provider_llm_client,
        test_articles_generator
    ):
        """Test that load is balanced across LLM providers."""

        # Mock other components
        mock_pool = AsyncMock()
        env = MagicMock()
        env.get_database_url.return_value = "postgresql://test:test@localhost/test"

        # Initialize service
        news_service = await create_realtime_news_service(
            mock_pool,
            env,
            multi_provider_llm_client,
            {'test': 'key'}
        )

        # Generate articles for processing
        articles = test_articles_generator(30)  # 30 articles = 180 LLM calls (6 agents each)

        # Process articles
        await asyncio.gather(
            *[news_service._process_article(article) for article in articles],
            return_exceptions=True
        )

        # Check provider usage distribution
        provider_usage = multi_provider_llm_client.get_provider_usage()
        total_calls = sum(provider_usage.values())

        print(f"Provider usage: {provider_usage}")
        print(f"Total LLM calls: {total_calls}")

        # Verify load balancing
        assert total_calls >= 150  # Should have made many LLM calls

        # Each provider should handle roughly equal load (within 20% variance)
        expected_per_provider = total_calls / 3
        for provider, calls in provider_usage.items():
            variance = abs(calls - expected_per_provider) / expected_per_provider
            assert variance < 0.3  # Within 30% of expected load

        assert min(provider_usage.values()) > 0  # All providers should be used