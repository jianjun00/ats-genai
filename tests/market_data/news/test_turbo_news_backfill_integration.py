import pytest
import asyncio
import json
from datetime import datetime, date
from db.test_db_manager import unit_test_db
from shared.utils.environment import Environment, EnvironmentType

from domains.market_data.services.news.turbo_news_backfill import (
    TurboPolygonNewsFetcher,
    TurboTiingoNewsFetcher,
    TurboNewsDatabaseInserter
)


@pytest.mark.asyncio
@pytest.mark.asyncio
async def test_end_to_end_polygon_news_backfill(unit_test_db):
    """Test end-to-end Polygon news backfill with real database."""
    # Setup test environment
    env = Environment(env_type=EnvironmentType.TEST, db_url=unit_test_db)

    # Sample news data (simulating API response processing)
    sample_news_data = [
        {
            'polygon_id': 'test-polygon-news-1',
            'title': 'Test Integration News Article',
            'description': 'This is a test news article for integration testing',
            'author': 'Test Author',
            'published_utc': datetime(2024, 8, 1, 10, 0, 0),
            'article_url': 'https://example.com/test-article',
            'image_url': 'https://example.com/test-image.jpg',
            'publisher_name': 'Test Publisher',
            'publisher_homepage_url': 'https://testpublisher.com',
            'publisher_logo_url': 'https://testpublisher.com/logo.png',
            'publisher_favicon_url': 'https://testpublisher.com/favicon.ico',
            'keywords': ['test', 'integration', 'news'],
            'tickers': ['AAPL', 'MSFT'],
            'insights': [
                {
                    'ticker': 'AAPL',
                    'sentiment': 'positive',
                    'sentiment_reasoning': 'Positive test sentiment'
                }
            ],
            'data': {
                'original_api_response': 'test_data',
                'additional_fields': ['field1', 'field2']
            }
        },
        {
            'polygon_id': 'test-polygon-news-2',
            'title': 'Another Test News Article',
            'description': 'Second test article for batch testing',
            'author': 'Another Author',
            'published_utc': datetime(2024, 8, 2, 15, 30, 0),
            'article_url': 'https://example.com/test-article-2',
            'image_url': None,  # Test null handling
            'publisher_name': 'Another Publisher',
            'publisher_homepage_url': 'https://anotherpublisher.com',
            'publisher_logo_url': None,
            'publisher_favicon_url': None,
            'keywords': ['finance', 'stocks'],
            'tickers': ['GOOGL'],
            'insights': None,  # Test null insights
            'data': {
                'different_structure': True,
                'test_field': 123
            }
        }
    ]

    # Test database insertion
    db_config = {
        'host': env.get_database_config()['host'],
        'port': env.get_database_config()['port'],
        'user': env.get_database_config()['user'],
        'password': env.get_database_config()['password'],
        'database': env.get_database_config()['database']
    }

    async with TurboNewsDatabaseInserter(db_config, pool_size=1) as db_inserter:
        # Insert sample data
        inserted_count = await db_inserter.bulk_insert_polygon_news(sample_news_data)
        assert inserted_count == 2

        # Verify data was inserted correctly
        import asyncpg

        pool = await asyncpg.create_pool(
            host=db_config['host'],
            port=db_config['port'],
            user=db_config['user'],
            password=db_config['password'],
            database=db_config['database']
        )

        try:
            async with pool.acquire() as conn:
                # Get inserted news articles
                news_articles = await conn.fetch(
                    "SELECT * FROM test_news_polygon ORDER BY published_utc"
                )

                assert len(news_articles) == 2

                # Verify first article
                first_article = news_articles[0]
                assert first_article['polygon_id'] == 'test-polygon-news-1'
                assert first_article['title'] == 'Test Integration News Article'
                assert first_article['author'] == 'Test Author'
                assert first_article['publisher_name'] == 'Test Publisher'
                assert first_article['keywords'] == ['test', 'integration', 'news']
                assert first_article['tickers'] == ['AAPL', 'MSFT']

                # Verify insights JSON structure
                insights_data = json.loads(first_article['insights'])
                assert len(insights_data) == 1
                assert insights_data[0]['sentiment'] == 'positive'

                # Verify original data JSON structure
                original_data = json.loads(first_article['data'])
                assert original_data['original_api_response'] == 'test_data'
                assert original_data['additional_fields'] == ['field1', 'field2']

                # Verify second article (with null handling)
                second_article = news_articles[1]
                assert second_article['polygon_id'] == 'test-polygon-news-2'
                assert second_article['image_url'] is None
                assert second_article['publisher_logo_url'] is None
                assert second_article['insights'] is None

        finally:
            await pool.close()


@pytest.mark.asyncio
@pytest.mark.asyncio
async def test_end_to_end_tiingo_news_backfill(unit_test_db):
    """Test end-to-end Tiingo news backfill with real database."""
    # Setup test environment
    env = Environment(env_type=EnvironmentType.TEST, db_url=unit_test_db)

    # Sample Tiingo news data
    sample_tiingo_data = [
        {
            'tiingo_id': 12345,
            'title': 'Test Tiingo Integration News',
            'description': 'This is a test Tiingo news article',
            'published_date': datetime(2024, 8, 1, 14, 0, 0),
            'crawl_date': datetime(2024, 8, 1, 15, 0, 0),
            'url': 'https://example.com/tiingo-test',
            'source': 'test-source.com',
            'tags': ['finance', 'technology', 'integration'],
            'tickers': ['aapl', 'msft'],
            'data': {
                'source_metadata': 'test_metadata',
                'crawl_info': {'version': '1.0'}
            }
        },
        {
            'tiingo_id': 67890,
            'title': 'Another Tiingo Test Article',
            'description': 'Second test article for Tiingo',
            'published_date': datetime(2024, 8, 2, 16, 30, 0),
            'crawl_date': datetime(2024, 8, 2, 17, 0, 0),
            'url': 'https://example.com/tiingo-test-2',
            'source': 'another-source.com',
            'tags': ['market', 'analysis'],
            'tickers': ['googl', 'amzn'],
            'data': {
                'different_metadata': True,
                'article_length': 1500
            }
        }
    ]

    # Test database insertion
    db_config = {
        'host': env.get_database_config()['host'],
        'port': env.get_database_config()['port'],
        'user': env.get_database_config()['user'],
        'password': env.get_database_config()['password'],
        'database': env.get_database_config()['database']
    }

    async with TurboNewsDatabaseInserter(db_config, pool_size=1) as db_inserter:
        # Insert sample data
        inserted_count = await db_inserter.bulk_insert_tiingo_news(sample_tiingo_data)
        assert inserted_count == 2

        # Verify data was inserted correctly
        import asyncpg

        pool = await asyncpg.create_pool(
            host=db_config['host'],
            port=db_config['port'],
            user=db_config['user'],
            password=db_config['password'],
            database=db_config['database']
        )

        try:
            async with pool.acquire() as conn:
                # Get inserted news articles
                news_articles = await conn.fetch(
                    "SELECT * FROM test_news_tiingo ORDER BY published_date"
                )

                assert len(news_articles) == 2

                # Verify first article
                first_article = news_articles[0]
                assert first_article['tiingo_id'] == 12345
                assert first_article['title'] == 'Test Tiingo Integration News'
                assert first_article['source'] == 'test-source.com'
                assert first_article['tags'] == ['finance', 'technology', 'integration']
                assert first_article['tickers'] == ['aapl', 'msft']

                # Verify data JSON structure
                original_data = json.loads(first_article['data'])
                assert original_data['source_metadata'] == 'test_metadata'
                assert original_data['crawl_info']['version'] == '1.0'

                # Verify second article
                second_article = news_articles[1]
                assert second_article['tiingo_id'] == 67890
                assert second_article['url'] == 'https://example.com/tiingo-test-2'

        finally:
            await pool.close()


@pytest.mark.asyncio
@pytest.mark.asyncio
async def test_duplicate_news_handling(unit_test_db):
    """Test that duplicate news articles are handled correctly."""
    # Setup test environment
    env = Environment(env_type=EnvironmentType.TEST, db_url=unit_test_db)

    # Sample data with potential duplicate
    sample_data = [
        {
            'polygon_id': 'test-duplicate-news',
            'title': 'Test Duplicate News',
            'description': 'This article will be inserted twice',
            'author': 'Test Author',
            'published_utc': datetime(2024, 8, 1, 10, 0, 0),
            'article_url': 'https://example.com/duplicate-test',
            'image_url': None,
            'publisher_name': 'Test Publisher',
            'publisher_homepage_url': None,
            'publisher_logo_url': None,
            'publisher_favicon_url': None,
            'keywords': ['test', 'duplicate'],
            'tickers': ['AAPL'],
            'insights': None,
            'data': {'test': 'data'}
        }
    ]

    # Database config
    db_config = {
        'host': env.get_database_config()['host'],
        'port': env.get_database_config()['port'],
        'user': env.get_database_config()['user'],
        'password': env.get_database_config()['password'],
        'database': env.get_database_config()['database']
    }

    async with TurboNewsDatabaseInserter(db_config, pool_size=1) as db_inserter:
        # Insert data first time
        first_insert = await db_inserter.bulk_insert_polygon_news(sample_data)
        assert first_insert == 1

        # Insert same data again (should handle duplicates)
        second_insert = await db_inserter.bulk_insert_polygon_news(sample_data)
        assert second_insert == 1  # Reports attempted insert

        # Verify only one record exists
        import asyncpg

        pool = await asyncpg.create_pool(
            host=db_config['host'],
            port=db_config['port'],
            user=db_config['user'],
            password=db_config['password'],
            database=db_config['database']
        )

        try:
            async with pool.acquire() as conn:
                news_articles = await conn.fetch(
                    "SELECT * FROM test_news_polygon WHERE polygon_id = 'test-duplicate-news'"
                )

                assert len(news_articles) == 1  # Should still be only 1 record

        finally:
            await pool.close()


@pytest.mark.asyncio
@pytest.mark.asyncio
async def test_concurrent_news_insertions(unit_test_db):
    """Test concurrent news insertions work correctly."""
    # Setup test environment
    env = Environment(env_type=EnvironmentType.TEST, db_url=unit_test_db)

    # Create sample data for concurrent insertion
    batch_1 = [
        {
            'polygon_id': f'test-concurrent-1-{i}',
            'title': f'Concurrent Test Article 1-{i}',
            'description': f'Description for article 1-{i}',
            'author': f'Author {i}',
            'published_utc': datetime(2024, 8, 1, 10, i, 0),
            'article_url': f'https://example.com/concurrent-1-{i}',
            'image_url': None,
            'publisher_name': 'Concurrent Publisher 1',
            'publisher_homepage_url': None,
            'publisher_logo_url': None,
            'publisher_favicon_url': None,
            'keywords': ['concurrent', 'test', 'batch1'],
            'tickers': ['AAPL'],
            'insights': None,
            'data': {'batch': 1, 'index': i}
        }
        for i in range(5)
    ]

    batch_2 = [
        {
            'polygon_id': f'test-concurrent-2-{i}',
            'title': f'Concurrent Test Article 2-{i}',
            'description': f'Description for article 2-{i}',
            'author': f'Author {i}',
            'published_utc': datetime(2024, 8, 2, 10, i, 0),
            'article_url': f'https://example.com/concurrent-2-{i}',
            'image_url': None,
            'publisher_name': 'Concurrent Publisher 2',
            'publisher_homepage_url': None,
            'publisher_logo_url': None,
            'publisher_favicon_url': None,
            'keywords': ['concurrent', 'test', 'batch2'],
            'tickers': ['MSFT'],
            'insights': None,
            'data': {'batch': 2, 'index': i}
        }
        for i in range(5)
    ]

    # Database config
    db_config = {
        'host': env.get_database_config()['host'],
        'port': env.get_database_config()['port'],
        'user': env.get_database_config()['user'],
        'password': env.get_database_config()['password'],
        'database': env.get_database_config()['database']
    }

    async with TurboNewsDatabaseInserter(db_config, pool_size=3) as db_inserter:
        # Execute concurrent insertions
        tasks = [
            db_inserter.bulk_insert_polygon_news(batch_1),
            db_inserter.bulk_insert_polygon_news(batch_2)
        ]

        results = await asyncio.gather(*tasks)

        # Verify all insertions succeeded
        assert results[0] == 5  # batch_1
        assert results[1] == 5  # batch_2

        # Verify data was inserted correctly
        import asyncpg

        pool = await asyncpg.create_pool(
            host=db_config['host'],
            port=db_config['port'],
            user=db_config['user'],
            password=db_config['password'],
            database=db_config['database']
        )

        try:
            async with pool.acquire() as conn:
                # Check total count
                total_count = await conn.fetchval(
                    "SELECT COUNT(*) FROM test_news_polygon WHERE polygon_id LIKE 'test-concurrent-%'"
                )
                assert total_count == 10

                # Check batch 1 articles
                batch1_articles = await conn.fetch(
                    "SELECT * FROM test_news_polygon WHERE polygon_id LIKE 'test-concurrent-1-%' ORDER BY polygon_id"
                )
                assert len(batch1_articles) == 5
                assert all('batch1' in article['keywords'] for article in batch1_articles)

                # Check batch 2 articles
                batch2_articles = await conn.fetch(
                    "SELECT * FROM test_news_polygon WHERE polygon_id LIKE 'test-concurrent-2-%' ORDER BY polygon_id"
                )
                assert len(batch2_articles) == 5
                assert all('batch2' in article['keywords'] for article in batch2_articles)

        finally:
            await pool.close()


@pytest.mark.asyncio
@pytest.mark.asyncio
async def test_large_news_batch_processing(unit_test_db):
    """Test processing of large batches of news data."""
    # Setup test environment
    env = Environment(env_type=EnvironmentType.TEST, db_url=unit_test_db)

    # Create large batch of sample news data (50 articles)
    large_batch = []

    for i in range(50):
        article = {
            'tiingo_id': 10000 + i,
            'title': f'Large Batch Test Article {i}',
            'description': f'This is test article number {i} in a large batch processing test',
            'published_date': datetime(2024, 8, 1, 10, i % 60, 0),  # Spread across hours
            'crawl_date': datetime(2024, 8, 1, 11, i % 60, 0),
            'url': f'https://example.com/large-batch-{i}',
            'source': f'source-{i % 5}.com',  # 5 different sources
            'tags': ['large', 'batch', 'test', f'tag-{i % 10}'],  # Varying tags
            'tickers': [['aapl', 'msft', 'googl', 'amzn', 'tsla'][i % 5]],  # Rotate through tickers
            'data': {
                'article_index': i,
                'batch_info': 'large_batch_test',
                'metadata': {'word_count': 500 + i * 10}
            }
        }
        large_batch.append(article)

    # Database config
    db_config = {
        'host': env.get_database_config()['host'],
        'port': env.get_database_config()['port'],
        'user': env.get_database_config()['user'],
        'password': env.get_database_config()['password'],
        'database': env.get_database_config()['database']
    }

    async with TurboNewsDatabaseInserter(db_config, pool_size=1) as db_inserter:
        # Insert large batch
        inserted_count = await db_inserter.bulk_insert_tiingo_news(large_batch)
        assert inserted_count == 50

        # Verify all data was inserted
        import asyncpg

        pool = await asyncpg.create_pool(
            host=db_config['host'],
            port=db_config['port'],
            user=db_config['user'],
            password=db_config['password'],
            database=db_config['database']
        )

        try:
            async with pool.acquire() as conn:
                # Check total count
                total_count = await conn.fetchval(
                    "SELECT COUNT(*) FROM test_news_tiingo WHERE tiingo_id BETWEEN 10000 AND 10049"
                )
                assert total_count == 50

                # Verify data integrity (check first and last records)
                first_article = await conn.fetchrow(
                    "SELECT * FROM test_news_tiingo WHERE tiingo_id = 10000"
                )
                assert first_article['title'] == 'Large Batch Test Article 0'
                assert first_article['source'] == 'source-0.com'

                last_article = await conn.fetchrow(
                    "SELECT * FROM test_news_tiingo WHERE tiingo_id = 10049"
                )
                assert last_article['title'] == 'Large Batch Test Article 49'
                assert last_article['source'] == 'source-4.com'

                # Verify JSON data structure
                first_data = json.loads(first_article['data'])
                assert first_data['article_index'] == 0
                assert first_data['batch_info'] == 'large_batch_test'

                last_data = json.loads(last_article['data'])
                assert last_data['article_index'] == 49
                assert last_data['metadata']['word_count'] == 500 + 49 * 10

        finally:
            await pool.close()


@pytest.mark.asyncio
@pytest.mark.asyncio
async def test_json_data_integrity(unit_test_db):
    """Test that complex JSON data structures maintain integrity through the database."""
    # Setup test environment
    env = Environment(env_type=EnvironmentType.TEST, db_url=unit_test_db)

    # Complex data structure to test JSON handling
    complex_insights = [
        {
            'ticker': 'AAPL',
            'sentiment': 'positive',
            'sentiment_reasoning': 'Strong product launch mentioned',
            'confidence_score': 0.85,
            'keywords_found': ['iPhone', 'revenue', 'growth'],
            'entities': {
                'companies': ['Apple Inc.', 'Samsung'],
                'products': ['iPhone 15', 'MacBook'],
                'metrics': {'revenue': '$100B', 'growth': '15%'}
            }
        },
        {
            'ticker': 'MSFT',
            'sentiment': 'neutral',
            'sentiment_reasoning': 'Mixed signals about cloud business',
            'confidence_score': 0.65,
            'related_articles': [
                {'id': 'article-1', 'sentiment': 'positive'},
                {'id': 'article-2', 'sentiment': 'negative'}
            ]
        }
    ]

    complex_data = {
        'api_metadata': {
            'version': '2.1',
            'timestamp': '2024-08-01T10:00:00Z',
            'source_reliability': 0.95
        },
        'content_analysis': {
            'word_count': 1500,
            'reading_time_minutes': 6,
            'topics': ['technology', 'finance', 'innovation'],
            'named_entities': {
                'persons': ['Tim Cook', 'Satya Nadella'],
                'organizations': ['Apple', 'Microsoft', 'SEC'],
                'locations': ['Cupertino', 'Redmond']
            }
        },
        'social_metrics': {
            'shares': 1250,
            'likes': 850,
            'comments': 75,
            'engagement_rate': 0.045
        }
    }

    sample_data = [
        {
            'polygon_id': 'test-json-integrity',
            'title': 'Complex JSON Test Article',
            'description': 'Testing complex JSON data structures',
            'author': 'JSON Test Author',
            'published_utc': datetime(2024, 8, 1, 10, 0, 0),
            'article_url': 'https://example.com/json-test',
            'image_url': None,
            'publisher_name': 'JSON Test Publisher',
            'publisher_homepage_url': None,
            'publisher_logo_url': None,
            'publisher_favicon_url': None,
            'keywords': ['json', 'test', 'complex'],
            'tickers': ['AAPL', 'MSFT'],
            'insights': complex_insights,
            'data': complex_data
        }
    ]

    # Database config
    db_config = {
        'host': env.get_database_config()['host'],
        'port': env.get_database_config()['port'],
        'user': env.get_database_config()['user'],
        'password': env.get_database_config()['password'],
        'database': env.get_database_config()['database']
    }

    async with TurboNewsDatabaseInserter(db_config, pool_size=1) as db_inserter:
        # Insert data
        inserted_count = await db_inserter.bulk_insert_polygon_news(sample_data)
        assert inserted_count == 1

        # Verify data integrity
        import asyncpg

        pool = await asyncpg.create_pool(
            host=db_config['host'],
            port=db_config['port'],
            user=db_config['user'],
            password=db_config['password'],
            database=db_config['database']
        )

        try:
            async with pool.acquire() as conn:
                article = await conn.fetchrow(
                    "SELECT * FROM test_news_polygon WHERE polygon_id = 'test-json-integrity'"
                )

                # Parse and verify insights JSON
                retrieved_insights = json.loads(article['insights'])
                assert len(retrieved_insights) == 2
                assert retrieved_insights[0]['ticker'] == 'AAPL'
                assert retrieved_insights[0]['confidence_score'] == 0.85
                assert retrieved_insights[0]['entities']['companies'] == ['Apple Inc.', 'Samsung']
                assert retrieved_insights[1]['related_articles'][0]['id'] == 'article-1'

                # Parse and verify data JSON
                retrieved_data = json.loads(article['data'])
                assert retrieved_data['api_metadata']['version'] == '2.1'
                assert retrieved_data['content_analysis']['word_count'] == 1500
                assert retrieved_data['content_analysis']['named_entities']['persons'] == ['Tim Cook', 'Satya Nadella']
                assert retrieved_data['social_metrics']['engagement_rate'] == 0.045

                # Verify the original complex structure is preserved
                assert retrieved_insights == complex_insights
                assert retrieved_data == complex_data

        finally:
            await pool.close()