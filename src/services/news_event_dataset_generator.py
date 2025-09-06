#!/usr/bin/env python3
"""
News Event Training Dataset Generator

Generates ML training datasets centered around news events with OHLC data
±10 days and ±10 hours surrounding each news event. Stores datasets in 
structured format for ML model training and backtesting analysis.
"""

import asyncio
import logging
import os
import json
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, asdict
from pathlib import Path

import asyncpg
import pandas as pd
import numpy as np

from core.config.environment import Environment
from core.config.database import Database
from storage.file_based_minute_manager import FileBasedMinuteManager

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@dataclass
class NewsEventData:
    """News event with signal information"""
    news_id: str
    ticker: str
    published_utc: datetime
    signal_type: str
    confidence: float
    sentiment: str
    sentiment_score: float
    title: str
    reasoning: str


@dataclass
class TrainingDatasetMetadata:
    """Training dataset metadata"""
    news_id: str
    ticker: str
    dataset_path: str
    news_date: datetime
    start_date: datetime
    end_date: datetime
    daily_records: int
    hourly_records: int
    dataset_size_mb: float
    signal_type: str
    confidence: float
    sentiment: str


class NewsEventDatasetGenerator:
    """Generate ML training datasets from news events"""
    
    def __init__(self):
        self.db = Database()
        self.pool: Optional[asyncpg.Pool] = None
        self.minute_manager = FileBasedMinuteManager()
        
        # Dataset storage configuration
        self.base_path = Path("/mnt/d/ats-data/news/training_data")
        self.base_path.mkdir(parents=True, exist_ok=True)
        
    async def initialize(self):
        """Initialize database connection and components"""
        try:
            self.pool = await asyncpg.create_pool(
                host=self.db.host,
                port=self.db.port,
                user=self.db.user,
                password=self.db.password,
                database=self.db.database,
                min_size=2,
                max_size=10
            )
            logger.info("✅ News Event Dataset Generator initialized")
        except Exception as e:
            logger.error(f"❌ Failed to initialize generator: {e}")
            raise
    
    async def close(self):
        """Close database connections"""
        if self.pool:
            await self.pool.close()
    
    async def get_news_events_for_training(
        self,
        limit: Optional[int] = None,
        ticker_filter: Optional[str] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None
    ) -> List[NewsEventData]:
        """Get news events that need training datasets generated"""
        if not self.pool:
            return []
            
        try:
            # Build query conditions
            conditions = []
            params = []
            param_count = 0
            
            if ticker_filter:
                param_count += 1
                conditions.append(f"ts.ticker = ${param_count}")
                params.append(ticker_filter)
            
            if start_date:
                param_count += 1
                conditions.append(f"ts.published_utc >= ${param_count}")
                params.append(start_date)
                
            if end_date:
                param_count += 1
                conditions.append(f"ts.published_utc <= ${param_count}")
                params.append(end_date)
            
            # Exclude events that already have datasets
            conditions.append("""
                NOT EXISTS (
                    SELECT 1 FROM dev_news_training_datasets ntd 
                    WHERE ntd.news_id = ts.news_id AND ntd.ticker = ts.ticker
                )
            """)
            
            where_clause = " AND " + " AND ".join(conditions) if conditions else ""
            limit_clause = f" LIMIT {limit}" if limit else ""
            
            async with self.pool.acquire() as conn:
                query = f"""
                    SELECT 
                        ts.news_id,
                        ts.ticker,
                        ts.published_utc,
                        ts.signal_type,
                        ts.confidence,
                        ts.sentiment,
                        ts.sentiment_score,
                        ts.key_factors,
                        np.title
                    FROM dev_trading_signals ts
                    JOIN dev_news_polygon np ON ts.news_id = np.vendor_id
                    WHERE 1=1 {where_clause}
                    ORDER BY ts.published_utc DESC
                    {limit_clause}
                """
                
                rows = await conn.fetch(query, *params)
                
                events = []
                for row in rows:
                    # Extract reasoning from key_factors JSON
                    reasoning = ""
                    if row['key_factors']:
                        try:
                            factors = json.loads(row['key_factors']) if isinstance(row['key_factors'], str) else row['key_factors']
                            if isinstance(factors, list) and factors:
                                reasoning = factors[0][:200]  # First 200 chars
                        except:
                            reasoning = ""
                    
                    events.append(NewsEventData(
                        news_id=row['news_id'],
                        ticker=row['ticker'],
                        published_utc=row['published_utc'],
                        signal_type=row['signal_type'],
                        confidence=float(row['confidence']),
                        sentiment=row['sentiment'],
                        sentiment_score=float(row['sentiment_score']),
                        title=row['title'] or "",
                        reasoning=reasoning
                    ))
                
                return events
                
        except Exception as e:
            logger.error(f"❌ Error retrieving news events: {e}")
            return []
    
    async def generate_training_dataset(
        self,
        news_event: NewsEventData
    ) -> Optional[TrainingDatasetMetadata]:
        """Generate training dataset for a single news event"""
        try:
            ticker = news_event.ticker
            news_date = news_event.published_utc
            
            logger.info(f"📊 Generating training dataset for {ticker} news on {news_date}")
            
            # Define date ranges: ±10 days and ±10 hours
            daily_start = news_date - timedelta(days=10)
            daily_end = news_date + timedelta(days=10)
            
            hourly_start = news_date - timedelta(hours=10)
            hourly_end = news_date + timedelta(hours=10)
            
            # Create dataset directory structure
            dataset_dir = self.base_path / ticker / news_date.strftime("%Y/%m")
            dataset_dir.mkdir(parents=True, exist_ok=True)
            
            # Generate unique dataset filename
            dataset_filename = f"{ticker}_{news_date.strftime('%Y%m%d_%H%M%S')}_{news_event.news_id[:8]}"
            
            # Generate daily dataset (±10 days)
            daily_data = await self._get_aggregated_ohlc_data(
                ticker, daily_start, daily_end, "1D"
            )
            
            # Generate hourly dataset (±10 hours)  
            hourly_data = await self._get_aggregated_ohlc_data(
                ticker, hourly_start, hourly_end, "1H"
            )
            
            if not daily_data and not hourly_data:
                logger.warning(f"⚠️ No price data found for {ticker} around {news_date}")
                return None
            
            # Create comprehensive dataset
            dataset = {
                "metadata": {
                    "news_id": news_event.news_id,
                    "ticker": ticker,
                    "news_date": news_date.isoformat(),
                    "signal_type": news_event.signal_type,
                    "confidence": news_event.confidence,
                    "sentiment": news_event.sentiment,
                    "sentiment_score": news_event.sentiment_score,
                    "title": news_event.title,
                    "reasoning": news_event.reasoning,
                    "generated_at": datetime.now().isoformat()
                },
                "daily_range": {
                    "start_date": daily_start.isoformat(),
                    "end_date": daily_end.isoformat(),
                    "data_points": len(daily_data)
                },
                "hourly_range": {
                    "start_date": hourly_start.isoformat(), 
                    "end_date": hourly_end.isoformat(),
                    "data_points": len(hourly_data)
                },
                "daily_ohlc": [
                    {
                        "timestamp": point["timestamp"],
                        "open": point["open"],
                        "high": point["high"], 
                        "low": point["low"],
                        "close": point["close"],
                        "volume": point["volume"]
                    }
                    for point in daily_data
                ],
                "hourly_ohlc": [
                    {
                        "timestamp": point["timestamp"],
                        "open": point["open"],
                        "high": point["high"],
                        "low": point["low"], 
                        "close": point["close"],
                        "volume": point["volume"]
                    }
                    for point in hourly_data
                ]
            }
            
            # Save dataset as JSON
            json_path = dataset_dir / f"{dataset_filename}.json"
            with open(json_path, 'w') as f:
                json.dump(dataset, f, indent=2, default=str)
            
            # Save as Parquet for efficient processing
            parquet_path = dataset_dir / f"{dataset_filename}.parquet"
            
            # Combine daily and hourly data with labels
            combined_data = []
            
            # Add daily data points
            for point in daily_data:
                combined_data.append({
                    **point,
                    "timeframe": "daily",
                    "hours_to_news": (news_date - pd.to_datetime(point["timestamp"])).total_seconds() / 3600
                })
            
            # Add hourly data points
            for point in hourly_data:
                combined_data.append({
                    **point,
                    "timeframe": "hourly", 
                    "hours_to_news": (news_date - pd.to_datetime(point["timestamp"])).total_seconds() / 3600
                })
            
            if combined_data:
                df = pd.DataFrame(combined_data)
                df.to_parquet(parquet_path, index=False)
            
            # Calculate file size
            json_size = json_path.stat().st_size
            parquet_size = parquet_path.stat().st_size if parquet_path.exists() else 0
            total_size_mb = (json_size + parquet_size) / (1024 * 1024)
            
            # Create metadata record
            metadata = TrainingDatasetMetadata(
                news_id=news_event.news_id,
                ticker=ticker,
                dataset_path=str(json_path.relative_to(self.base_path)),
                news_date=news_date,
                start_date=daily_start,
                end_date=daily_end,
                daily_records=len(daily_data),
                hourly_records=len(hourly_data),
                dataset_size_mb=total_size_mb,
                signal_type=news_event.signal_type,
                confidence=news_event.confidence,
                sentiment=news_event.sentiment
            )
            
            # Save metadata to database
            await self._save_dataset_metadata(metadata)
            
            logger.info(f"✅ Generated dataset: {dataset_filename} "
                       f"({len(daily_data)} daily, {len(hourly_data)} hourly points, "
                       f"{total_size_mb:.2f}MB)")
            
            return metadata
            
        except Exception as e:
            logger.error(f"❌ Error generating dataset for {news_event.ticker}: {e}")
            return None
    
    async def _get_aggregated_ohlc_data(
        self,
        ticker: str,
        start_date: datetime,
        end_date: datetime,
        timeframe: str
    ) -> List[Dict[str, Any]]:
        """Get aggregated OHLC data from minute bars"""
        try:
            # Get minute data from file-based storage
            minute_data = await self.minute_manager.get_minute_data(
                ticker, start_date, end_date
            )
            
            if not minute_data:
                return []
            
            # Convert to DataFrame for aggregation
            df = pd.DataFrame([
                {
                    "timestamp": bar.timestamp,
                    "open": bar.open_price,
                    "high": bar.high_price,
                    "low": bar.low_price,
                    "close": bar.close_price,
                    "volume": bar.volume
                }
                for bar in minute_data
            ])
            
            if df.empty:
                return []
            
            df["timestamp"] = pd.to_datetime(df["timestamp"])
            df = df.set_index("timestamp")
            
            # Aggregate based on timeframe
            aggregated = df.resample(timeframe).agg({
                "open": "first",
                "high": "max", 
                "low": "min",
                "close": "last",
                "volume": "sum"
            }).dropna()
            
            # Convert back to list of dictionaries
            result = []
            for timestamp, row in aggregated.iterrows():
                result.append({
                    "timestamp": timestamp.isoformat(),
                    "open": float(row["open"]),
                    "high": float(row["high"]),
                    "low": float(row["low"]),
                    "close": float(row["close"]),
                    "volume": int(row["volume"])
                })
            
            return result
            
        except Exception as e:
            logger.error(f"❌ Error aggregating OHLC data: {e}")
            return []
    
    async def _save_dataset_metadata(self, metadata: TrainingDatasetMetadata):
        """Save dataset metadata to database"""
        if not self.pool:
            return
            
        try:
            async with self.pool.acquire() as conn:
                await conn.execute("""
                    INSERT INTO dev_news_training_datasets 
                    (news_id, ticker, dataset_path, news_date, start_date, end_date,
                     daily_records, hourly_records, dataset_size_mb, signal_type,
                     confidence, sentiment)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
                    ON CONFLICT (news_id, ticker) DO NOTHING
                """,
                metadata.news_id, metadata.ticker, metadata.dataset_path,
                metadata.news_date, metadata.start_date, metadata.end_date,
                metadata.daily_records, metadata.hourly_records, metadata.dataset_size_mb,
                metadata.signal_type, metadata.confidence, metadata.sentiment)
                
        except Exception as e:
            logger.error(f"❌ Error saving dataset metadata: {e}")
    
    async def process_news_events_backfill(
        self,
        limit: Optional[int] = None,
        ticker_filter: Optional[str] = None
    ) -> Dict[str, int]:
        """Process all news events to generate training datasets"""
        logger.info("🚀 Starting news events training dataset backfill")
        
        # Get news events that need datasets
        events = await self.get_news_events_for_training(
            limit=limit,
            ticker_filter=ticker_filter
        )
        
        if not events:
            logger.info("ℹ️ No news events found that need training datasets")
            return {"processed": 0, "successful": 0, "failed": 0}
        
        logger.info(f"📊 Processing {len(events)} news events for training datasets")
        
        processed = 0
        successful = 0
        failed = 0
        
        for i, event in enumerate(events, 1):
            try:
                logger.info(f"📈 Processing {i}/{len(events)}: "
                          f"{event.ticker} on {event.published_utc.strftime('%Y-%m-%d %H:%M')}")
                
                metadata = await self.generate_training_dataset(event)
                
                if metadata:
                    successful += 1
                    logger.info(f"✅ Generated dataset: {metadata.daily_records} daily, "
                              f"{metadata.hourly_records} hourly records")
                else:
                    failed += 1
                    logger.warning(f"❌ Failed to generate dataset for {event.ticker}")
                
                processed += 1
                
                # Progress reporting
                if processed % 10 == 0:
                    logger.info(f"📈 Progress: {processed}/{len(events)} events processed "
                              f"({successful} successful, {failed} failed)")
                
            except Exception as e:
                failed += 1
                logger.error(f"❌ Error processing event {event.ticker}: {e}")
        
        results = {
            "processed": processed,
            "successful": successful, 
            "failed": failed
        }
        
        logger.info(f"🎉 Backfill completed: {results}")
        return results


async def main():
    """Main execution function for standalone running"""
    generator = NewsEventDatasetGenerator()
    
    try:
        await generator.initialize()
        
        # Process news events backfill
        results = await generator.process_news_events_backfill(limit=10)  # Test with 10
        
        logger.info(f"✅ Processing complete: {results}")
        
    finally:
        await generator.close()


if __name__ == "__main__":
    asyncio.run(main())