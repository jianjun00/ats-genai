#!/usr/bin/env python3
"""
Simple News Signal Extraction Test

This script demonstrates how to enhance existing news insights into structured
trading signals using the existing 104K+ news records with insights.
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
    level=logging.DEBUG,
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

    # Parse insights if it's a JSON string
    if isinstance(insights, str):
        try:
            insights = json.loads(insights)
        except json.JSONDecodeError:
            logger.error(f"Failed to parse insights JSON: {insights}")
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

async def process_news_batch():
    """Process a batch of news records and extract signals."""

    # Direct database connection for simplicity
    database_url = "postgresql://postgres:dev_password@localhost:3432/dev_db"

    logger.info("🚀 Starting Simple News Signal Extraction")
    logger.info("=" * 60)

    # Connect to database
    pool = await asyncpg.create_pool(database_url, min_size=1, max_size=3)

    try:
        # Get news records that actually have insights data (limit to 50 for testing)
        records = await pool.fetch("""
            SELECT vendor_id, published_utc, title, insights, tickers
            FROM dev_news_polygon
            WHERE insights IS NOT NULL
            AND jsonb_array_length(insights) > 0
            ORDER BY published_utc DESC
            LIMIT 50
        """)

        logger.info(f"📊 Found {len(records)} news records to process")

        total_signals = 0

        for i, record in enumerate(records):
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
                total_signals += len(signals)

                logger.info(f"✅ Processed record {i+1}: {len(signals)} signals extracted from '{record['title'][:80]}...'")
            else:
                logger.info(f"ℹ️  Record {i+1}: No signals extracted from '{record['title'][:80]}...'")

        # Check results
        signal_count = await pool.fetchval("SELECT COUNT(*) FROM dev_trading_signals")
        analysis_count = await pool.fetchval("SELECT COUNT(*) FROM dev_enhanced_news_analysis")

        logger.info(f"\n🎉 Processing completed!")
        logger.info(f"   📈 Total signals extracted: {total_signals}")
        logger.info(f"   📊 Total signals in database: {signal_count}")
        logger.info(f"   📋 Enhanced analysis records: {analysis_count}")

        # Show sample results
        sample_signals = await pool.fetch("""
            SELECT ticker, signal_type, confidence, sentiment,
                   LEFT(key_factors::text, 100) as factors_sample
            FROM dev_trading_signals
            ORDER BY processed_at DESC
            LIMIT 10
        """)

        if sample_signals:
            logger.info(f"\n📋 Sample signals generated:")
            for signal in sample_signals:
                logger.info(f"   {signal['ticker']}: {signal['signal_type']} "
                          f"(confidence: {signal['confidence']}, sentiment: {signal['sentiment']})")

    except Exception as e:
        logger.error(f"❌ Processing failed: {e}")
        import traceback
        traceback.print_exc()
    finally:
        await pool.close()
        logger.info("\n✅ Simple signal extraction test completed")

if __name__ == "__main__":
    asyncio.run(process_news_batch())