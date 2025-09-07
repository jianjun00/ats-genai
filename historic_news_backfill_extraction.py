#!/usr/bin/env python3
"""
Historic News Signal Extraction - Full Backfill

This script processes ALL historic news records (104K+) and extracts structured
trading signals from the existing insights data. This is a scaled-up version
of the working simple_news_signal_test.py.
"""

import asyncio
import json
import logging
import os
import sys
from datetime import datetime
from typing import Dict, List, Any, Optional

# Set environment variables BEFORE importing anything
os.environ['ENVIRONMENT'] = 'dev'
os.environ['DB_HOST'] = 'localhost'
os.environ['DB_PORT'] = '3432'
os.environ['DB_USER'] = 'postgres'
os.environ['DB_PASSWORD'] = 'dev_password'
os.environ['DB_NAME'] = 'dev_db'

sys.path.insert(0, 'src')

import asyncpg

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class SimpleTradingSignal:
    """Simplified trading signal from news."""
    def __init__(self, news_id: str, ticker: str, signal_type: str,
                 confidence: float, sentiment: str, sentiment_score: float,
                 published_utc: datetime, reasoning: str = ""):
        self.news_id = news_id
        self.ticker = ticker
        self.signal_type = signal_type  # BUY, SELL, HOLD, WATCH
        self.confidence = confidence
        self.sentiment = sentiment
        self.sentiment_score = sentiment_score
        self.published_utc = published_utc
        self.reasoning = reasoning

async def extract_signals_from_news_record(news_record: Dict) -> List[SimpleTradingSignal]:
    """Extract trading signals from a single news record."""

    signals = []
    news_id = news_record.get('vendor_id', str(news_record.get('id', '')))
    published_utc = news_record.get('published_utc')
    insights = news_record.get('insights', [])

    # Parse insights if it's a JSON string (PostgreSQL JSONB returns as string)
    if isinstance(insights, str):
        try:
            insights = json.loads(insights)
        except json.JSONDecodeError:
            logger.error(f"Failed to parse insights JSON for {news_id}: {insights[:100]}...")
            return signals

    if not isinstance(insights, list):
        return signals

    for insight in insights:
        if not isinstance(insight, dict):
            continue

        ticker = insight.get('ticker')
        sentiment = insight.get('sentiment', 'neutral')
        reasoning = insight.get('sentiment_reasoning', '')

        if not ticker:
            continue

        # Convert sentiment to signal and confidence
        if sentiment == 'positive':
            signal_type = 'BUY'
            confidence = 0.75
            sentiment_score = 0.7
        elif sentiment == 'negative':
            signal_type = 'SELL'
            confidence = 0.75
            sentiment_score = -0.7
        else:
            signal_type = 'HOLD'
            confidence = 0.5
            sentiment_score = 0.0

        signal = SimpleTradingSignal(
            news_id=news_id,
            ticker=ticker,
            signal_type=signal_type,
            confidence=confidence,
            sentiment=sentiment,
            sentiment_score=sentiment_score,
            published_utc=published_utc,
            reasoning=reasoning[:500]  # Limit reasoning length
        )

        signals.append(signal)

    return signals

async def store_trading_signals(pool: asyncpg.Pool, signals: List[SimpleTradingSignal]):
    """Store trading signals in database."""

    if not signals:
        return

    async with pool.acquire() as conn:
        for signal in signals:
            try:
                await conn.execute("""
                    INSERT INTO dev_trading_signals
                    (news_id, ticker, signal_type, confidence, sentiment, sentiment_score,
                     impact_timeframe, key_factors, risk_level, published_utc,
                     processed_at, model_version)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
                    ON CONFLICT (news_id, ticker) DO UPDATE SET
                        signal_type = EXCLUDED.signal_type,
                        confidence = EXCLUDED.confidence,
                        sentiment = EXCLUDED.sentiment,
                        sentiment_score = EXCLUDED.sentiment_score,
                        processed_at = NOW()
                """,
                signal.news_id, signal.ticker, signal.signal_type, signal.confidence,
                signal.sentiment, signal.sentiment_score, 'medium_term',
                json.dumps([signal.reasoning]), 'medium', signal.published_utc,
                datetime.now(), 'simple_extractor_v1.0')
            except Exception as e:
                logger.error(f"Failed to store signal for {signal.ticker}: {e}")

async def process_historic_news_backfill():
    """Process ALL historic news records and extract signals."""

    # Direct database connection for simplicity
    database_url = "postgresql://postgres:dev_password@localhost:3432/dev_db"

    logger.info("🚀 Starting Historic News Signal Extraction - FULL BACKFILL")
    logger.info("=" * 80)

    # Connect to database
    pool = await asyncpg.create_pool(database_url, min_size=2, max_size=10)

    try:
        # First, get total count of news records with insights
        total_records = await pool.fetchval("""
            SELECT COUNT(*)
            FROM dev_news_polygon
            WHERE insights IS NOT NULL
            AND jsonb_array_length(insights) > 0
        """)

        logger.info(f"📊 Total news records to process: {total_records}")

        if total_records == 0:
            logger.warning("❌ No news records with insights found!")
            return

        # Process in batches of 1000 to avoid memory issues
        batch_size = 1000
        total_signals = 0
        processed_records = 0
        start_time = datetime.now()

        for offset in range(0, total_records, batch_size):
            batch_start = datetime.now()

            # Get batch of records
            records = await pool.fetch("""
                SELECT vendor_id, published_utc, title, insights, tickers
                FROM dev_news_polygon
                WHERE insights IS NOT NULL
                AND jsonb_array_length(insights) > 0
                ORDER BY published_utc DESC
                LIMIT $1 OFFSET $2
            """, batch_size, offset)

            batch_signals = 0

            # Process each record in the batch
            for record in records:
                news_dict = {
                    'vendor_id': record['vendor_id'],
                    'published_utc': record['published_utc'],
                    'title': record['title'],
                    'insights': record['insights'],
                    'tickers': record['tickers']
                }

                # Extract signals from this record
                signals = await extract_signals_from_news_record(news_dict)

                if signals:
                    # Store signals in database
                    await store_trading_signals(pool, signals)
                    batch_signals += len(signals)

            processed_records += len(records)
            total_signals += batch_signals

            # Progress reporting
            batch_time = (datetime.now() - batch_start).total_seconds()
            elapsed_time = (datetime.now() - start_time).total_seconds()
            progress_pct = (processed_records / total_records) * 100
            estimated_total_time = (elapsed_time / processed_records) * total_records if processed_records > 0 else 0
            remaining_time = estimated_total_time - elapsed_time if estimated_total_time > elapsed_time else 0

            logger.info(f"✅ Batch {offset//batch_size + 1}: Processed {len(records)} records, "
                       f"extracted {batch_signals} signals in {batch_time:.1f}s")
            logger.info(f"📈 Progress: {processed_records}/{total_records} records ({progress_pct:.1f}%), "
                       f"{total_signals} total signals extracted")
            logger.info(f"⏱️  Elapsed: {elapsed_time/60:.1f}min, Estimated remaining: {remaining_time/60:.1f}min")
            logger.info("-" * 60)

        # Final verification and statistics
        total_signals_db = await pool.fetchval("SELECT COUNT(*) FROM dev_trading_signals")
        unique_tickers = await pool.fetchval("SELECT COUNT(DISTINCT ticker) FROM dev_trading_signals")
        signal_types = await pool.fetch("""
            SELECT signal_type, COUNT(*) as count
            FROM dev_trading_signals
            GROUP BY signal_type
            ORDER BY count DESC
        """)

        # Get date range of signals
        date_range = await pool.fetchrow("""
            SELECT MIN(published_utc) as earliest, MAX(published_utc) as latest
            FROM dev_trading_signals
        """)

        logger.info("\n" + "=" * 80)
        logger.info("🎉 HISTORIC NEWS SIGNAL EXTRACTION COMPLETED!")
        logger.info("=" * 80)
        logger.info(f"📊 Statistics:")
        logger.info(f"   • Total news records processed: {processed_records}")
        logger.info(f"   • Total signals extracted: {total_signals}")
        logger.info(f"   • Total signals in database: {total_signals_db}")
        logger.info(f"   • Unique tickers covered: {unique_tickers}")
        logger.info(f"   • Processing time: {(datetime.now() - start_time).total_seconds()/60:.1f} minutes")

        logger.info(f"\n📈 Signal Distribution:")
        for signal_type in signal_types:
            logger.info(f"   • {signal_type['signal_type']}: {signal_type['count']}")

        logger.info(f"\n📅 Date Range:")
        logger.info(f"   • Earliest signal: {date_range['earliest']}")
        logger.info(f"   • Latest signal: {date_range['latest']}")

        # Sample recent signals
        sample_signals = await pool.fetch("""
            SELECT ticker, signal_type, confidence, sentiment, published_utc,
                   LEFT(key_factors::text, 100) as factors_sample
            FROM dev_trading_signals
            ORDER BY processed_at DESC
            LIMIT 20
        """)

        logger.info(f"\n📋 Sample recent signals:")
        for signal in sample_signals[:10]:
            logger.info(f"   • {signal['ticker']}: {signal['signal_type']} "
                       f"({signal['confidence']:.3f} confidence, {signal['sentiment']}) "
                       f"on {signal['published_utc'].strftime('%Y-%m-%d')}")

    except Exception as e:
        logger.error(f"❌ Processing failed: {e}")
        import traceback
        traceback.print_exc()
    finally:
        await pool.close()
        logger.info("\n✅ Historic news signal extraction backfill completed")

if __name__ == "__main__":
    asyncio.run(process_historic_news_backfill())