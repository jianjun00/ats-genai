#!/usr/bin/env python3
"""
Performance and load tests for the Multi-Modal News Prediction System
Tests throughput, latency, memory usage, and scalability characteristics.
"""

import pytest
import asyncio
import time
import psutil
import gc
from datetime import datetime, date, timedelta
from unittest.mock import AsyncMock, patch
from typing import List, Dict, Any
from concurrent.futures import ThreadPoolExecutor
import threading

# Add src to path for imports
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'src'))

from events.economic_events_classifier import EconomicEventsClassifier, EconomicEventsProcessor
from domains.ml.services.training_data.generators.multimodal_dataset_generator import MultiModalDatasetGenerator, MultiModalSample


class TestPerformanceBenchmarks:
    """Benchmark core system performance"""

    def setup_method(self):
        """Set up performance test fixtures"""
        self.classifier = EconomicEventsClassifier()
        self.db_config = {
            'host': 'localhost',
            'port': 5432,
            'user': 'test',
            'password': 'test',
            'database': 'test_db'
        }

    def test_sentiment_calculation_throughput(self):
        """Test sentiment calculation performance at scale"""
        # Generate test articles with varying lengths
        test_articles = []
        for i in range(1000):
            length = 50 + (i % 200)  # 50-250 words
            text = f"Apple reports strong earnings with growth profit success beat expectations " * (length // 10)
            test_articles.append(text)

        # Measure throughput
        start_time = time.time()

        generator = MultiModalDatasetGenerator(self.db_config)

        for article in test_articles:
            sentiment = generator._calculate_simple_sentiment(article)
            assert -1.0 <= sentiment <= 1.0

        end_time = time.time()
        total_time = end_time - start_time
        throughput = len(test_articles) / total_time

        # Should process at least 100 articles per second
        assert throughput > 100, f"Sentiment calculation too slow: {throughput:.1f} articles/sec"

        # Should complete within reasonable time
        assert total_time < 20.0, f"Total time too slow: {total_time:.2f}s for {len(test_articles)} articles"

    def test_event_classification_performance(self):
        """Test economic event classification performance"""
        # Generate test news articles
        test_cases = [
            ("Fed Raises Interest Rates", "Federal Reserve increases rates by 0.25%"),
            ("Apple Beats Earnings", "AAPL reports strong quarterly results"),
            ("Unemployment Data Released", "Jobs report shows labor market strength"),
            ("CPI Inflation Report", "Consumer prices rise 0.3% monthly"),
            ("GDP Growth Announced", "Economy expands at 2.1% rate"),
        ] * 200  # 1000 total classifications

        start_time = time.time()
        successful_classifications = 0

        for title, description in test_cases:
            event = self.classifier.classify_news_article(
                title=title,
                description=description,
                symbols=['SPY'],
                published_date=datetime.now()
            )
            if event:
                successful_classifications += 1

        end_time = time.time()
        total_time = end_time - start_time
        throughput = len(test_cases) / total_time

        # Should classify at least 500 articles per second
        assert throughput > 500, f"Classification too slow: {throughput:.1f} articles/sec"

        # Should classify most relevant articles
        classification_rate = successful_classifications / len(test_cases)
        assert classification_rate > 0.8, f"Classification rate too low: {classification_rate:.1%}"

    def test_memory_usage_under_load(self):
        """Test memory usage with large datasets"""
        process = psutil.Process()
        initial_memory = process.memory_info().rss / 1024 / 1024  # MB

        # Create large dataset
        samples = []
        for i in range(10000):
            sample = MultiModalSample(
                symbol=f"STOCK{i % 100}",
                sample_date=date(2024, 1, 1) + timedelta(days=i % 365),
                prediction_horizon=(i % 4) * 5 + 1,
                news_sentiment_7d=0.1 * (i % 20 - 10),
                news_volume_7d=i % 50,
                price_features={'sma_20': 100.0 + i % 50, 'rsi_14': 30 + i % 40},
                volume_features={'vol_sma_20': 1000000 + i % 500000},
                target_return_1d=0.001 * (i % 40 - 20),
                target_direction_1d=(i % 3) - 1
            )
            samples.append(sample)

        peak_memory = process.memory_info().rss / 1024 / 1024  # MB
        memory_increase = peak_memory - initial_memory

        # Should not use excessive memory (less than 500MB for 10k samples)
        assert memory_increase < 500, f"Memory usage too high: {memory_increase:.1f}MB"

        # Clean up
        del samples
        gc.collect()

        final_memory = process.memory_info().rss / 1024 / 1024  # MB
        memory_freed = peak_memory - final_memory

        # Should free most of the memory
        assert memory_freed > memory_increase * 0.5, f"Memory not freed properly: {memory_freed:.1f}MB freed"

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_concurrent_sample_generation(self):
        """Test concurrent sample generation performance"""
        generator = MultiModalDatasetGenerator(self.db_config)

        mock_pool = AsyncMock()
        mock_conn = AsyncMock()
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
        mock_conn.fetch.return_value = []  # Empty results for speed

        generator.db_pool = mock_pool

        symbols = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA'] * 20  # 100 symbols
        test_date = date(2024, 1, 15)

        start_time = time.time()

        # Generate samples concurrently
        tasks = []
        for symbol in symbols:
            task = generator.generate_sample_for_symbol_date(symbol, test_date, 5)
            tasks.append(task)

        results = await asyncio.gather(*tasks, return_exceptions=True)

        end_time = time.time()
        total_time = end_time - start_time
        throughput = len(symbols) / total_time

        # Should handle concurrent generation efficiently
        assert throughput > 50, f"Concurrent generation too slow: {throughput:.1f} samples/sec"

        # Most results should be successful (not exceptions)
        successful_results = sum(1 for r in results if not isinstance(r, Exception))
        success_rate = successful_results / len(results)
        assert success_rate > 0.95, f"Too many failures: {success_rate:.1%} success rate"


class TestScalabilityLimits:
    """Test system behavior at scale limits"""

    def setup_method(self):
        """Set up scalability test fixtures"""
        self.db_config = {
            'host': 'localhost',
            'port': 5432,
            'user': 'test',
            'password': 'test',
            'database': 'test_db'
        }

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_large_symbol_universe_processing(self):
        """Test processing large numbers of symbols"""
        generator = MultiModalDatasetGenerator(self.db_config)

        # Mock database operations
        mock_pool = AsyncMock()
        mock_conn = AsyncMock()
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
        mock_conn.fetch.return_value = []
        mock_conn.executemany.return_value = None

        generator.db_pool = mock_pool

        # Test with 500 symbols (approaching Russell 500)
        large_symbol_list = [f"STOCK{i:03d}" for i in range(500)]

        start_time = time.time()

        total_samples = await generator.generate_training_dataset(
            symbols=large_symbol_list,
            start_date=date(2024, 1, 1),
            end_date=date(2024, 1, 7),  # 1 week to keep test reasonable
            sample_freq_days=7
        )

        end_time = time.time()
        total_time = end_time - start_time

        # Should complete large symbol universe in reasonable time
        assert total_time < 300, f"Large universe processing too slow: {total_time:.1f}s"

        # Should generate expected number of samples
        expected_samples = 500 * 1 * 4  # 500 symbols × 1 date × 4 horizons
        assert total_samples == expected_samples

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_extended_date_range_processing(self):
        """Test processing extended historical periods"""
        generator = MultiModalDatasetGenerator(self.db_config)

        mock_pool = AsyncMock()
        mock_conn = AsyncMock()
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
        mock_conn.fetch.return_value = []
        mock_conn.executemany.return_value = None

        generator.db_pool = mock_pool

        # Test with 2 years of data
        start_date = date(2022, 1, 1)
        end_date = date(2023, 12, 31)
        symbols = ['AAPL', 'MSFT', 'GOOGL']  # Small symbol set for extended period

        start_time = time.time()

        total_samples = await generator.generate_training_dataset(
            symbols=symbols,
            start_date=start_date,
            end_date=end_date,
            sample_freq_days=7  # Weekly sampling
        )

        end_time = time.time()
        total_time = end_time - start_time

        # Should handle extended periods efficiently
        assert total_time < 180, f"Extended period processing too slow: {total_time:.1f}s"

        # Verify reasonable sample count (approx 104 weeks × 3 symbols × 4 horizons)
        assert total_samples > 1000

    def test_high_frequency_sentiment_analysis(self):
        """Test sentiment analysis at high frequency"""
        generator = MultiModalDatasetGenerator(self.db_config)

        # Create high-frequency sentiment calculation scenario
        text_variations = [
            "Apple beats earnings expectations with strong growth",
            "Microsoft reports quarterly revenue miss amid cloud concerns",
            "Google parent Alphabet shows resilient advertising performance",
            "Amazon warehouse automation drives efficiency gains",
            "Tesla production ramp shows promising early results",
        ]

        # Multiply to create high-frequency scenario
        test_texts = text_variations * 2000  # 10,000 sentiment calculations

        start_time = time.time()

        sentiment_scores = []
        for text in test_texts:
            score = generator._calculate_simple_sentiment(text)
            sentiment_scores.append(score)

        end_time = time.time()
        total_time = end_time - start_time
        throughput = len(test_texts) / total_time

        # Should maintain high throughput
        assert throughput > 1000, f"High-frequency sentiment too slow: {throughput:.1f} texts/sec"

        # All scores should be valid
        assert all(-1.0 <= score <= 1.0 for score in sentiment_scores)
        assert len(sentiment_scores) == len(test_texts)


class TestConcurrencyAndThreadSafety:
    """Test concurrent operations and thread safety"""

    def setup_method(self):
        """Set up concurrency test fixtures"""
        self.classifier = EconomicEventsClassifier()

    def test_classifier_thread_safety(self):
        """Test that classifier works safely across multiple threads"""

        def classify_articles(thread_id: int, num_articles: int) -> List[bool]:
            """Classify articles in a separate thread"""
            results = []

            for i in range(num_articles):
                title = f"Fed Rate Decision Thread {thread_id} Article {i}"
                description = "Federal Reserve monetary policy announcement"

                event = self.classifier.classify_news_article(
                    title=title,
                    description=description,
                    symbols=['SPY'],
                    published_date=datetime.now()
                )

                results.append(event is not None)

            return results

        # Run classification in multiple threads
        num_threads = 5
        articles_per_thread = 100

        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            start_time = time.time()

            futures = []
            for thread_id in range(num_threads):
                future = executor.submit(classify_articles, thread_id, articles_per_thread)
                futures.append(future)

            # Collect results
            all_results = []
            for future in futures:
                thread_results = future.result()
                all_results.extend(thread_results)

            end_time = time.time()
            total_time = end_time - start_time

        # Should complete successfully
        total_articles = num_threads * articles_per_thread
        success_rate = sum(all_results) / len(all_results)

        assert success_rate > 0.95, f"Thread safety issues: {success_rate:.1%} success rate"
        assert len(all_results) == total_articles

        # Should maintain reasonable performance under concurrency
        throughput = total_articles / total_time
        assert throughput > 100, f"Concurrent performance degraded: {throughput:.1f} articles/sec"

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_async_database_pool_usage(self):
        """Test efficient database connection pool usage"""
        processor = EconomicEventsProcessor(self.db_config)

        # Mock database pool
        mock_pool = AsyncMock()
        mock_conn = AsyncMock()
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
        mock_conn.fetch.return_value = []

        processor.db_pool = mock_pool

        # Simulate concurrent database operations
        num_concurrent_ops = 20

        async def process_batch(batch_id: int) -> int:
            """Process a batch of articles concurrently"""
            return await processor.process_news_articles(f'news_test_{batch_id}', limit=50)

        start_time = time.time()

        # Run concurrent operations
        tasks = [process_batch(i) for i in range(num_concurrent_ops)]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        end_time = time.time()
        total_time = end_time - start_time

        # Should handle concurrent operations efficiently
        successful_ops = sum(1 for r in results if not isinstance(r, Exception))
        success_rate = successful_ops / len(results)

        assert success_rate > 0.9, f"Concurrent DB operations failed: {success_rate:.1%} success rate"
        assert total_time < 30, f"Concurrent operations too slow: {total_time:.1f}s"

    def test_memory_consistency_under_concurrency(self):
        """Test memory consistency during concurrent operations"""

        def memory_intensive_task(task_id: int) -> Dict[str, Any]:
            """Perform memory-intensive operations"""
            classifier = EconomicEventsClassifier()
            results = []

            # Generate many classifications
            for i in range(1000):
                event = classifier.classify_news_article(
                    title=f"Task {task_id} News {i}",
                    description="Economic event description with detailed analysis",
                    symbols=[f"STOCK{i % 100}"],
                    published_date=datetime.now()
                )
                if event:
                    results.append({
                        'event_type': event.event_type,
                        'confidence': event.confidence_score,
                        'impact': event.predicted_impact_score
                    })

            return {
                'task_id': task_id,
                'total_events': len(results),
                'avg_confidence': sum(r['confidence'] for r in results) / len(results) if results else 0
            }

        # Monitor memory during concurrent execution
        process = psutil.Process()
        initial_memory = process.memory_info().rss / 1024 / 1024  # MB

        num_concurrent_tasks = 8

        with ThreadPoolExecutor(max_workers=num_concurrent_tasks) as executor:
            start_time = time.time()

            futures = [executor.submit(memory_intensive_task, i) for i in range(num_concurrent_tasks)]
            results = [future.result() for future in futures]

            end_time = time.time()

        peak_memory = process.memory_info().rss / 1024 / 1024  # MB
        memory_increase = peak_memory - initial_memory

        # Should not use excessive memory under concurrency
        assert memory_increase < 1000, f"Memory usage too high under concurrency: {memory_increase:.1f}MB"

        # All tasks should complete successfully
        assert len(results) == num_concurrent_tasks
        assert all(isinstance(r['total_events'], int) for r in results)


class TestResourceUtilization:
    """Test CPU, memory, and I/O resource utilization"""

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_cpu_utilization_efficiency(self):
        """Test CPU utilization during intensive operations"""
        generator = MultiModalDatasetGenerator({})

        # CPU-intensive sentiment analysis
        large_texts = [
            "Apple reports exceptional quarterly earnings with record iPhone sales driving strong revenue growth across all geographic regions while maintaining healthy profit margins despite supply chain challenges and increased component costs which the company successfully managed through operational efficiency improvements and strategic supplier partnerships that enabled consistent production volumes throughout the quarter resulting in better than expected financial performance that exceeded analyst expectations and provided positive guidance for future periods" * 10
        ] * 1000  # Very large texts

        # Monitor CPU usage
        process = psutil.Process()
        cpu_percent_start = process.cpu_percent()

        start_time = time.time()

        # Perform CPU-intensive operations
        sentiment_scores = []
        for text in large_texts:
            score = generator._calculate_simple_sentiment(text)
            sentiment_scores.append(score)

        end_time = time.time()
        cpu_percent_end = process.cpu_percent()

        total_time = end_time - start_time
        throughput = len(large_texts) / total_time

        # Should efficiently utilize CPU
        assert throughput > 10, f"CPU utilization inefficient: {throughput:.1f} large texts/sec"
        assert len(sentiment_scores) == len(large_texts)

        # CPU usage should be reasonable (not stuck in infinite loops)
        assert cpu_percent_end < 100, f"CPU usage too high: {cpu_percent_end}%"

    def test_regex_compilation_caching(self):
        """Test that regex patterns are efficiently cached"""
        # Create multiple classifier instances
        classifiers = [EconomicEventsClassifier() for _ in range(10)]

        # Test articles
        test_articles = [
            ("Fed Rate Decision", "Federal Reserve announces monetary policy"),
            ("Earnings Beat", "Company reports quarterly results"),
            ("Jobs Data", "Employment statistics released")
        ] * 100  # 300 classifications per classifier

        start_time = time.time()

        # Run classifications on all instances
        total_classifications = 0
        for classifier in classifiers:
            for title, description in test_articles:
                event = classifier.classify_news_article(title, description)
                if event:
                    total_classifications += 1

        end_time = time.time()
        total_time = end_time - start_time

        total_articles = len(classifiers) * len(test_articles)
        throughput = total_articles / total_time

        # Should benefit from regex caching and maintain high throughput
        assert throughput > 500, f"Regex caching inefficient: {throughput:.1f} articles/sec"
        assert total_classifications > 0  # Should classify some articles


# Test configuration
@pytest.fixture(scope="session")
def event_loop():
    """Create an instance of the default event loop for the test session."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


# Performance test markers
pytestmark = pytest.mark.performance


if __name__ == "__main__":
    # Run performance tests
    pytest.main([__file__, "-v", "--tb=short", "-m", "not slow"])