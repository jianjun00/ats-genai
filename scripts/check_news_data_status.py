#!/usr/bin/env python3
"""
Check News Data Status Across All Vendors

Analyzes news data coverage from Polygon, Tiingo, and EODHD to understand:
- Total news records per vendor
- Symbol coverage
- Date range coverage (30 years)
- Data quality and gaps
"""

import sys
sys.path.append('/workspace/src')

import asyncio
import asyncpg
import os
import logging
from datetime import datetime, timedelta, date
from typing import Dict, List, Any

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class NewsDataAnalyzer:
    """Analyze news data coverage across all vendors."""
    
    def __init__(self):
        self.vendors = ['polygon', 'tiingo', 'eodhd']
        logger.info("📰 News Data Coverage Analyzer initialized")

    async def get_database_connection(self):
        """Get database connection."""
        db_host = os.getenv('DB_HOST', 'postgres')
        db_port = int(os.getenv('DB_PORT', '5432'))
        db_user = os.getenv('DB_USER', 'postgres')
        db_password = os.getenv('DB_PASSWORD', 'dev_password')
        db_name = os.getenv('DB_NAME', 'dev_db')
        
        return await asyncpg.connect(
            host=db_host,
            port=db_port,
            user=db_user,
            password=db_password,
            database=db_name
        )

    async def check_table_exists(self, conn, table_name: str) -> bool:
        """Check if a news table exists."""
        try:
            result = await conn.fetchval("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_name = $1
                )
            """, table_name)
            return result
        except Exception as e:
            logger.warning(f"Error checking table {table_name}: {e}")
            return False

    async def analyze_polygon_news(self, conn) -> Dict[str, Any]:
        """Analyze Polygon news data."""
        table_name = "dev_news_polygon"
        
        if not await self.check_table_exists(conn, table_name):
            return {
                'vendor': 'Polygon',
                'table_exists': False,
                'total_records': 0,
                'unique_symbols': 0,
                'date_range': 'No data',
                'coverage_days': 0
            }
        
        try:
            # Basic stats - Polygon uses 'tickers' array column
            stats = await conn.fetchrow(f"""
                WITH ticker_counts AS (
                    SELECT UNNEST(tickers) as ticker
                    FROM {table_name}
                    WHERE published_utc IS NOT NULL AND tickers IS NOT NULL
                )
                SELECT 
                    (SELECT COUNT(*) FROM {table_name} WHERE published_utc IS NOT NULL) as total_records,
                    (SELECT COUNT(DISTINCT ticker) FROM ticker_counts) as unique_symbols,
                    (SELECT MIN(published_utc::date) FROM {table_name} WHERE published_utc IS NOT NULL) as earliest_date,
                    (SELECT MAX(published_utc::date) FROM {table_name} WHERE published_utc IS NOT NULL) as latest_date,
                    (SELECT ROUND(AVG(EXTRACT(days FROM NOW() - published_utc::date))) FROM {table_name} WHERE published_utc IS NOT NULL) as avg_days_old
            """)
            
            # Sample data
            sample = await conn.fetchrow(f"""
                SELECT tickers, title, published_utc, author
                FROM {table_name}
                WHERE published_utc IS NOT NULL
                ORDER BY published_utc DESC
                LIMIT 1
            """)
            
            coverage_days = 0
            if stats['earliest_date'] and stats['latest_date']:
                coverage_days = (stats['latest_date'] - stats['earliest_date']).days
            
            return {
                'vendor': 'Polygon',
                'table_exists': True,
                'total_records': stats['total_records'] or 0,
                'unique_symbols': stats['unique_symbols'] or 0,
                'earliest_date': stats['earliest_date'],
                'latest_date': stats['latest_date'],
                'avg_days_old': stats['avg_days_old'],
                'coverage_days': coverage_days,
                'sample_record': dict(sample) if sample else None
            }
            
        except Exception as e:
            logger.error(f"Error analyzing Polygon news: {e}")
            return {
                'vendor': 'Polygon',
                'table_exists': True,
                'error': str(e)
            }

    async def analyze_tiingo_news(self, conn) -> Dict[str, Any]:
        """Analyze Tiingo news data."""
        table_name = "dev_news_tiingo"
        
        if not await self.check_table_exists(conn, table_name):
            return {
                'vendor': 'Tiingo',
                'table_exists': False,
                'total_records': 0,
                'unique_symbols': 0,
                'date_range': 'No data',
                'coverage_days': 0
            }
        
        try:
            # Basic stats - Tiingo uses 'tickers' array column similar to Polygon  
            stats = await conn.fetchrow(f"""
                WITH ticker_counts AS (
                    SELECT UNNEST(tickers) as ticker
                    FROM {table_name}
                    WHERE published_date IS NOT NULL AND tickers IS NOT NULL
                )
                SELECT 
                    (SELECT COUNT(*) FROM {table_name} WHERE published_date IS NOT NULL) as total_records,
                    (SELECT COALESCE(COUNT(DISTINCT ticker), 0) FROM ticker_counts) as unique_symbols,
                    (SELECT MIN(published_date::date) FROM {table_name} WHERE published_date IS NOT NULL) as earliest_date,
                    (SELECT MAX(published_date::date) FROM {table_name} WHERE published_date IS NOT NULL) as latest_date,
                    (SELECT COALESCE(ROUND(AVG(EXTRACT(days FROM NOW() - published_date::date))), 0) FROM {table_name} WHERE published_date IS NOT NULL) as avg_days_old
            """)
            
            # Sample data
            sample = await conn.fetchrow(f"""
                SELECT tickers, title, published_date, source
                FROM {table_name}
                WHERE published_date IS NOT NULL
                ORDER BY published_date DESC
                LIMIT 1
            """)
            
            coverage_days = 0
            if stats['earliest_date'] and stats['latest_date']:
                coverage_days = (stats['latest_date'] - stats['earliest_date']).days
            
            return {
                'vendor': 'Tiingo',
                'table_exists': True,
                'total_records': stats['total_records'] or 0,
                'unique_symbols': stats['unique_symbols'] or 0,
                'earliest_date': stats['earliest_date'],
                'latest_date': stats['latest_date'],
                'avg_days_old': stats['avg_days_old'],
                'coverage_days': coverage_days,
                'sample_record': dict(sample) if sample else None
            }
            
        except Exception as e:
            logger.error(f"Error analyzing Tiingo news: {e}")
            return {
                'vendor': 'Tiingo',
                'table_exists': True,
                'error': str(e)
            }

    async def analyze_eodhd_news(self, conn) -> Dict[str, Any]:
        """Analyze EODHD news data."""
        table_name = "dev_news_eodhd"
        
        if not await self.check_table_exists(conn, table_name):
            return {
                'vendor': 'EODHD',
                'table_exists': False,
                'total_records': 0,
                'unique_symbols': 0,
                'date_range': 'No data',
                'coverage_days': 0
            }
        
        try:
            # Basic stats
            stats = await conn.fetchrow(f"""
                SELECT 
                    COUNT(*) as total_records,
                    COUNT(DISTINCT symbol) as unique_symbols,
                    MIN(date::date) as earliest_date,
                    MAX(date::date) as latest_date,
                    ROUND(AVG(EXTRACT(days FROM NOW() - date::date))) as avg_days_old
                FROM {table_name}
                WHERE date IS NOT NULL
            """)
            
            # Sample data
            sample = await conn.fetchrow(f"""
                SELECT symbol, title, date, link
                FROM {table_name}
                WHERE date IS NOT NULL
                ORDER BY date DESC
                LIMIT 1
            """)
            
            coverage_days = 0
            if stats['earliest_date'] and stats['latest_date']:
                coverage_days = (stats['latest_date'] - stats['earliest_date']).days
            
            return {
                'vendor': 'EODHD',
                'table_exists': True,
                'total_records': stats['total_records'] or 0,
                'unique_symbols': stats['unique_symbols'] or 0,
                'earliest_date': stats['earliest_date'],
                'latest_date': stats['latest_date'],
                'avg_days_old': stats['avg_days_old'],
                'coverage_days': coverage_days,
                'sample_record': dict(sample) if sample else None
            }
            
        except Exception as e:
            logger.error(f"Error analyzing EODHD news: {e}")
            return {
                'vendor': 'EODHD',
                'table_exists': True,
                'error': str(e)
            }

    async def get_total_instruments(self, conn) -> int:
        """Get total number of active instruments."""
        try:
            count = await conn.fetchval("""
                SELECT COUNT(*) FROM dev_instruments WHERE active = true
            """)
            return count or 0
        except Exception as e:
            logger.warning(f"Could not get instruments count: {e}")
            return 0

    async def analyze_news_coverage(self):
        """Analyze comprehensive news data coverage."""
        logger.info("🚀 Starting comprehensive news data analysis...")
        
        conn = await self.get_database_connection()
        
        try:
            # Get total instruments for context
            total_instruments = await self.get_total_instruments(conn)
            
            # Analyze each vendor
            polygon_analysis = await self.analyze_polygon_news(conn)
            tiingo_analysis = await self.analyze_tiingo_news(conn)
            eodhd_analysis = await self.analyze_eodhd_news(conn)
            
            analyses = [polygon_analysis, tiingo_analysis, eodhd_analysis]
            
            # Log comprehensive results
            self.log_news_analysis_results(analyses, total_instruments)
            
            return analyses
            
        finally:
            await conn.close()

    def log_news_analysis_results(self, analyses: List[Dict[str, Any]], total_instruments: int):
        """Log comprehensive news analysis results."""
        
        logger.info("=" * 80)
        logger.info("📰 COMPREHENSIVE NEWS DATA COVERAGE ANALYSIS")
        logger.info("=" * 80)
        logger.info(f"📊 Total Active Instruments: {total_instruments:,}")
        logger.info("")
        
        # Target: 30 years of data
        target_days = 30 * 365
        target_date = datetime.now().date() - timedelta(days=target_days)
        
        for analysis in analyses:
            vendor = analysis.get('vendor', 'Unknown')
            logger.info(f"🔍 {vendor.upper()} NEWS ANALYSIS:")
            
            if analysis.get('error'):
                logger.info(f"  ❌ Error: {analysis['error']}")
                continue
                
            if not analysis.get('table_exists'):
                logger.info(f"  ❌ No news table found")
                continue
            
            total_records = analysis.get('total_records', 0)
            unique_symbols = analysis.get('unique_symbols', 0)
            earliest_date = analysis.get('earliest_date')
            latest_date = analysis.get('latest_date')
            coverage_days = analysis.get('coverage_days', 0)
            
            logger.info(f"  📊 Total Records: {total_records:,}")
            logger.info(f"  📈 Unique Symbols: {unique_symbols:,}")
            
            if total_instruments > 0:
                symbol_coverage = (unique_symbols / total_instruments) * 100
                logger.info(f"  📊 Symbol Coverage: {symbol_coverage:.1f}%")
            
            if earliest_date and latest_date:
                logger.info(f"  📅 Date Range: {earliest_date} to {latest_date}")
                logger.info(f"  📊 Coverage Days: {coverage_days:,} days")
                
                # 30-year target analysis
                years_covered = coverage_days / 365
                logger.info(f"  📊 Years Covered: {years_covered:.1f} years")
                
                if earliest_date <= target_date:
                    logger.info(f"  ✅ 30-year coverage: YES")
                else:
                    days_short = (earliest_date - target_date).days
                    logger.info(f"  ❌ 30-year coverage: NO (short by {days_short} days)")
            else:
                logger.info(f"  ❌ No valid date data")
            
            # Sample record
            sample = analysis.get('sample_record')
            if sample:
                logger.info(f"  📋 Latest Record Sample:")
                for key, value in sample.items():
                    if key in ['title', 'ticker', 'symbol']:
                        logger.info(f"    {key}: {str(value)[:100]}...")
                    else:
                        logger.info(f"    {key}: {value}")
            
            logger.info("")
        
        # Overall summary
        total_news_records = sum(a.get('total_records', 0) for a in analyses)
        total_unique_symbols = len(set().union(*[
            set() for a in analyses 
            if a.get('table_exists') and not a.get('error')
        ]))
        
        logger.info("📊 OVERALL NEWS SUMMARY:")
        logger.info(f"  📰 Total News Records: {total_news_records:,}")
        logger.info(f"  📈 Combined Symbol Coverage: {total_unique_symbols:,}")
        
        # Coverage assessment
        vendors_with_data = sum(1 for a in analyses if a.get('total_records', 0) > 0)
        vendors_with_30y = sum(1 for a in analyses 
                              if a.get('earliest_date') and 
                              a.get('earliest_date') <= target_date)
        
        logger.info(f"  📊 Vendors with News Data: {vendors_with_data}/3")
        logger.info(f"  📅 Vendors with 30-year Coverage: {vendors_with_30y}/3")
        
        if vendors_with_data == 0:
            logger.warning("  ⚠️ NO NEWS DATA FOUND - Need to implement news collection")
        elif vendors_with_30y == 0:
            logger.warning("  ⚠️ NO 30-YEAR COVERAGE - Need historical news backfill")
        else:
            logger.info("  ✅ News data infrastructure is operational")
        
        logger.info("=" * 80)

async def main():
    """Main execution function."""
    
    try:
        analyzer = NewsDataAnalyzer()
        analyses = await analyzer.analyze_news_coverage()
        
        logger.info("✅ News data analysis complete")
        
    except Exception as e:
        logger.error(f"❌ Failed to analyze news data: {e}")
        import traceback
        logger.error(traceback.format_exc())
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())