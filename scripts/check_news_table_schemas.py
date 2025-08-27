#!/usr/bin/env python3
"""
Check News Table Schemas

Inspect the actual structure of news tables to understand column names and data structure.
"""

import sys
sys.path.append('/workspace/src')

import asyncio
import asyncpg
import os
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

async def get_database_connection():
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

async def check_table_schema(conn, table_name):
    """Check the schema of a table."""
    try:
        # Check if table exists
        exists = await conn.fetchval("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = $1
            )
        """, table_name)
        
        if not exists:
            logger.info(f"❌ Table {table_name} does not exist")
            return None
        
        # Get table schema
        columns = await conn.fetch("""
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_schema = 'public' 
            AND table_name = $1
            ORDER BY ordinal_position
        """, table_name)
        
        # Get row count
        row_count = await conn.fetchval(f"SELECT COUNT(*) FROM {table_name}")
        
        # Get sample data
        sample = await conn.fetchrow(f"SELECT * FROM {table_name} LIMIT 1")
        
        logger.info(f"✅ {table_name.upper()}:")
        logger.info(f"  📊 Row Count: {row_count:,}")
        logger.info(f"  📋 Columns ({len(columns)}):")
        for col in columns:
            logger.info(f"    {col['column_name']}: {col['data_type']} ({'nullable' if col['is_nullable'] == 'YES' else 'not null'})")
        
        if sample:
            logger.info(f"  📝 Sample Data:")
            for key, value in sample.items():
                if isinstance(value, str) and len(value) > 100:
                    logger.info(f"    {key}: {str(value)[:100]}...")
                else:
                    logger.info(f"    {key}: {value}")
        
        logger.info("")
        
        return {
            'table_name': table_name,
            'exists': True,
            'row_count': row_count,
            'columns': [dict(col) for col in columns],
            'sample': dict(sample) if sample else None
        }
        
    except Exception as e:
        logger.error(f"❌ Error checking {table_name}: {e}")
        return None

async def main():
    """Main execution function."""
    
    logger.info("🔍 Checking news table schemas...")
    
    conn = await get_database_connection()
    
    try:
        # Check all potential news tables
        news_tables = [
            'dev_news_polygon',
            'dev_news_tiingo', 
            'dev_news_eodhd',
            'news',
            'polygon_news',
            'tiingo_news',
            'eodhd_news'
        ]
        
        for table in news_tables:
            await check_table_schema(conn, table)
            
        # Also check what tables exist with 'news' in the name
        logger.info("🔍 Searching for any tables containing 'news'...")
        news_like_tables = await conn.fetch("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_name LIKE '%news%'
            ORDER BY table_name
        """)
        
        if news_like_tables:
            logger.info(f"📋 Found {len(news_like_tables)} tables containing 'news':")
            for table in news_like_tables:
                logger.info(f"  - {table['table_name']}")
        else:
            logger.info("❌ No tables found containing 'news'")
            
    finally:
        await conn.close()

if __name__ == "__main__":
    asyncio.run(main())