#!/usr/bin/env python3
"""
Ray-powered Data Quality Agent
Leverages Ray for distributed processing of large-scale data quality analysis
"""

import ray
import asyncio
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
import asyncpg
from dataclasses import dataclass

logger = logging.getLogger(__name__)

@dataclass
class QualityIssue:
    """Data quality issue representation"""
    id: str
    symbol: str
    issue_type: str
    severity: str
    description: str
    detected_at: str
    affected_date: str
    field: str
    expected_value: str
    actual_value: str
    vendor_source: str
    status: str

@ray.remote
class DataQualityWorker:
    """Ray remote worker for distributed data quality checks"""
    
    def __init__(self, db_config: Dict[str, Any]):
        self.db_config = db_config
        self.conn_pool = None
        
    async def initialize(self):
        """Initialize database connection pool"""
        try:
            self.conn_pool = await asyncpg.create_pool(**self.db_config, max_size=5)
            logger.info("✅ Ray worker database pool initialized")
        except Exception as e:
            logger.error(f"❌ Failed to initialize Ray worker database pool: {e}")
            raise
    
    async def scan_stale_data_batch(self, symbols_batch: List[str], days_threshold: int = 3) -> List[Dict[str, Any]]:
        """Scan batch of symbols for stale data"""
        if not self.conn_pool:
            await self.initialize()
        
        issues = []
        
        try:
            async with self.conn_pool.acquire() as conn:
                # Check stale data across all vendor tables for this batch
                stale_query = """
                WITH vendor_freshness AS (
                    SELECT symbol, MAX(date) as latest_date, 'polygon' as vendor
                    FROM intg_daily_price_polygon
                    WHERE symbol = ANY($1)
                    GROUP BY symbol
                    UNION ALL
                    SELECT symbol, MAX(date) as latest_date, 'tiingo' as vendor
                    FROM intg_daily_price_tiingo
                    WHERE symbol = ANY($1)
                    GROUP BY symbol
                    UNION ALL
                    SELECT symbol, MAX(date) as latest_date, 'eodhd' as vendor
                    FROM intg_daily_price_eodhd
                    WHERE symbol = ANY($1)
                    GROUP BY symbol
                ),
                symbol_freshness AS (
                    SELECT symbol, MAX(latest_date) as latest_date, 
                           string_agg(vendor, ',' ORDER BY latest_date DESC) as vendors
                    FROM vendor_freshness
                    GROUP BY symbol
                )
                SELECT symbol, latest_date, vendors
                FROM symbol_freshness
                WHERE latest_date < CURRENT_DATE - INTERVAL '%s days'
                ORDER BY latest_date DESC;
                """ % days_threshold
                
                stale_data = await conn.fetch(stale_query, symbols_batch)
                
                for row in stale_data:
                    days_stale = (datetime.now().date() - row['latest_date']).days
                    issues.append({
                        "id": f"stale_data_{row['symbol']}",
                        "symbol": row['symbol'],
                        "issue_type": "stale_data",
                        "severity": "medium" if days_stale < 7 else "high",
                        "description": f"Data is {days_stale} days old (last: {row['latest_date']}) - vendors: {row['vendors']}",
                        "detected_at": datetime.now().isoformat(),
                        "affected_date": row['latest_date'].isoformat(),
                        "field": "date",
                        "expected_value": f"< {days_threshold} days old",
                        "actual_value": f"{days_stale} days old",
                        "vendor_source": row['vendors'],
                        "status": "open"
                    })
                    
        except Exception as e:
            logger.error(f"❌ Error scanning stale data batch {symbols_batch[:3]}...: {e}")
            
        return issues
    
    async def scan_duplicates_batch(self, date_range_days: int = 7) -> List[Dict[str, Any]]:
        """Scan for duplicate records within date range"""
        if not self.conn_pool:
            await self.initialize()
            
        issues = []
        
        try:
            async with self.conn_pool.acquire() as conn:
                # Check for duplicates in each vendor table
                duplicate_query = """
                SELECT symbol, date as price_date, COUNT(*) as count, 'polygon' as vendor
                FROM intg_daily_price_polygon 
                WHERE date >= CURRENT_DATE - INTERVAL '%s days'
                GROUP BY symbol, date
                HAVING COUNT(*) > 1
                UNION ALL
                SELECT symbol, date as price_date, COUNT(*) as count, 'tiingo' as vendor
                FROM intg_daily_price_tiingo 
                WHERE date >= CURRENT_DATE - INTERVAL '%s days'
                GROUP BY symbol, date
                HAVING COUNT(*) > 1
                UNION ALL
                SELECT symbol, date as price_date, COUNT(*) as count, 'eodhd' as vendor
                FROM intg_daily_price_eodhd 
                WHERE date >= CURRENT_DATE - INTERVAL '%s days'
                GROUP BY symbol, date
                HAVING COUNT(*) > 1
                ORDER BY count DESC
                LIMIT 1000;
                """ % (date_range_days, date_range_days, date_range_days)
                
                duplicates = await conn.fetch(duplicate_query)
                
                for row in duplicates:
                    issues.append({
                        "id": f"duplicate_{row['symbol']}_{row['price_date']}_{row['vendor']}",
                        "symbol": row['symbol'],
                        "issue_type": "duplicate_records",
                        "severity": "critical",
                        "description": f"Duplicate records: {row['count']} entries for same date ({row['vendor']})",
                        "detected_at": datetime.now().isoformat(),
                        "affected_date": row['price_date'].isoformat(),
                        "field": "all_fields",
                        "expected_value": "1 record per day",
                        "actual_value": f"{row['count']} records",
                        "vendor_source": row['vendor'],
                        "status": "open"
                    })
                    
        except Exception as e:
            logger.error(f"❌ Error scanning duplicates batch: {e}")
            
        return issues

class RayDataQualityAgent:
    """Ray-powered distributed data quality agent"""
    
    def __init__(self, db_config: Dict[str, Any], num_workers: int = 4):
        self.db_config = db_config
        self.num_workers = num_workers
        self.workers = []
        self.initialized = False
        
    async def initialize(self):
        """Initialize Ray cluster and workers"""
        try:
            if not ray.is_initialized():
                ray.init(ignore_reinit_error=True)
                logger.info("🚀 Ray cluster initialized")
            
            # Create distributed workers
            self.workers = [
                DataQualityWorker.remote(self.db_config) 
                for _ in range(self.num_workers)
            ]
            
            # Initialize all workers
            await asyncio.gather(*[
                worker.initialize.remote() for worker in self.workers
            ])
            
            self.initialized = True
            logger.info(f"✅ Ray Data Quality Agent initialized with {self.num_workers} workers")
            
        except Exception as e:
            logger.error(f"❌ Failed to initialize Ray Data Quality Agent: {e}")
            raise
    
    async def get_all_symbols(self) -> List[str]:
        """Get all unique symbols from database"""
        try:
            conn = await asyncpg.connect(**self.db_config)
            
            symbols_query = """
            SELECT DISTINCT symbol FROM (
                SELECT symbol FROM intg_daily_price_polygon
                UNION
                SELECT symbol FROM intg_daily_price_tiingo  
                UNION
                SELECT symbol FROM intg_daily_price_eodhd
            ) AS all_symbols
            ORDER BY symbol;
            """
            
            symbols = await conn.fetch(symbols_query)
            await conn.close()
            
            return [row['symbol'] for row in symbols]
            
        except Exception as e:
            logger.error(f"❌ Failed to get symbols: {e}")
            return []
    
    async def scan_all_issues_distributed(self, page_size: int = 1000) -> List[Dict[str, Any]]:
        """Distribute data quality scanning across Ray workers with pagination"""
        if not self.initialized:
            await self.initialize()
        
        logger.info("🔄 Starting distributed data quality scan...")
        
        try:
            # Get all symbols and distribute across workers
            all_symbols = await self.get_all_symbols()
            logger.info(f"📊 Scanning {len(all_symbols)} symbols across {self.num_workers} Ray workers")
            
            # Batch symbols for parallel processing
            batch_size = max(1, len(all_symbols) // self.num_workers)
            symbol_batches = [
                all_symbols[i:i + batch_size] 
                for i in range(0, len(all_symbols), batch_size)
            ]
            
            # Distribute stale data scanning
            stale_data_tasks = [
                self.workers[i % self.num_workers].scan_stale_data_batch.remote(
                    batch, days_threshold=3
                ) for i, batch in enumerate(symbol_batches)
            ]
            
            # Distribute duplicate scanning (fewer workers needed)
            duplicate_tasks = [
                self.workers[i].scan_duplicates_batch.remote(date_range_days=7)
                for i in range(min(2, self.num_workers))  # Use only 2 workers for duplicates
            ]
            
            # Execute all tasks in parallel
            logger.info("⚡ Executing parallel data quality checks...")
            stale_results = await asyncio.gather(*stale_data_tasks)
            duplicate_results = await asyncio.gather(*duplicate_tasks)
            
            # Combine all results
            all_issues = []
            for batch_issues in stale_results:
                all_issues.extend(batch_issues)
            for duplicate_issues in duplicate_results:
                all_issues.extend(duplicate_issues)
            
            logger.info(f"✅ Ray scan complete: {len(all_issues)} issues detected")
            
            return all_issues
            
        except Exception as e:
            logger.error(f"❌ Distributed scan failed: {e}")
            return []
    
    async def get_issues_page(self, page: int = 1, page_size: int = 50, 
                             severity_filter: Optional[str] = None) -> Dict[str, Any]:
        """Get paginated issues with filtering"""
        
        all_issues = await self.scan_all_issues_distributed()
        
        # Apply severity filter
        if severity_filter:
            all_issues = [issue for issue in all_issues if issue['severity'] == severity_filter]
        
        # Sort by severity priority and date
        severity_priority = {'critical': 0, 'high': 1, 'medium': 2, 'low': 3}
        all_issues.sort(key=lambda x: (
            severity_priority.get(x['severity'], 4),
            x['affected_date']
        ), reverse=True)
        
        # Paginate
        total_issues = len(all_issues)
        total_pages = (total_issues + page_size - 1) // page_size
        start_idx = (page - 1) * page_size
        end_idx = start_idx + page_size
        
        page_issues = all_issues[start_idx:end_idx]
        
        return {
            "issues": page_issues,
            "pagination": {
                "current_page": page,
                "page_size": page_size,
                "total_issues": total_issues,
                "total_pages": total_pages,
                "has_next": page < total_pages,
                "has_prev": page > 1
            },
            "summary": {
                "total_issues": total_issues,
                "critical": len([i for i in all_issues if i['severity'] == 'critical']),
                "high": len([i for i in all_issues if i['severity'] == 'high']),
                "medium": len([i for i in all_issues if i['severity'] == 'medium']),
                "low": len([i for i in all_issues if i['severity'] == 'low'])
            }
        }
    
    async def shutdown(self):
        """Shutdown Ray workers and cluster"""
        try:
            if self.workers:
                # Shutdown workers gracefully
                for worker in self.workers:
                    ray.kill(worker)
                
            if ray.is_initialized():
                ray.shutdown()
                
            logger.info("🔄 Ray Data Quality Agent shutdown complete")
            
        except Exception as e:
            logger.error(f"❌ Error during Ray shutdown: {e}")

# Test script for development use only
async def test_ray_agent():
    """Test Ray-powered data quality agent with real data"""
    
    print("🚀 Ray Data Quality Agent Test")
    print("=" * 50)
    
    db_config = {
        'host': 'ats-intg-postgres',
        'port': 5432,
        'user': 'postgres',
        'password': 'intg_password',
        'database': 'intg_db'
    }
    
    agent = RayDataQualityAgent(db_config, num_workers=4)
    
    try:
        await agent.initialize()
        
        # Test paginated results with real data
        result = await agent.get_issues_page(page=1, page_size=10, severity_filter='high')
        
        print(f"📊 Results Summary:")
        print(f"   Total Issues: {result['summary']['total_issues']}")
        print(f"   High Issues: {result['summary']['high']}")
        print(f"   Page 1/{result['pagination']['total_pages']}")
        print(f"   Issues on this page: {len(result['issues'])}")
        
        print(f"\n📋 Sample Issues:")
        for i, issue in enumerate(result['issues'][:3], 1):
            print(f"   {i}. {issue['symbol']}: {issue['description'][:50]}...")
        
    finally:
        await agent.shutdown()

if __name__ == "__main__":
    asyncio.run(test_ray_agent())