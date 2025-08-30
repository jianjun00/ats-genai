#!/usr/bin/env python3
"""
Daily News Refresh Job for ATS-INTG Environment

Refreshes news data from all configured vendors for the integration environment.
Designed to run as a scheduled daily job.
"""

import sys
import os
import subprocess
import time
from datetime import datetime, timedelta
import json
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
import hashlib

# Add ATS source path
sys.path.append('/workspace/src')

# Configuration
API_KEYS = {
    'POLYGON_API_KEY': 'wfrcZNX3ZJJt55Or_CmBXda8G8e8tABD',
    'FMP_API_KEY': 'Qf5MGG5HrOnEaWTumhVJzx3Onb3kw7Rr',
    'ALPHA_VANTAGE_API_KEY': '9GI0NZ3V4VNFX271'
}

VENDORS = ['polygon', 'fmp', 'alpha_vantage']
MAX_WORKERS = 2  # Conservative for API rate limits
RATE_LIMIT_DELAY = 1.5  # seconds between requests for news

# Threading for progress tracking
stats = {
    'total_symbols': 0,
    'processed_symbols': 0,
    'successful_vendors': 0,
    'failed_vendors': 0,
    'total_news_items': 0
}
stats_lock = threading.Lock()

def log_info(message: str):
    """Enhanced logging with timestamp."""
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"{timestamp} - INFO - {message}")

def run_intg_query(query: str) -> str:
    """Execute database query using run_intg infrastructure."""
    try:
        result = subprocess.run(
            ['python3', 'scripts/run_intg.py', 'query', '--query', query],
            capture_output=True,
            text=True,
            cwd='/workspace'
        )
        if result.returncode == 0:
            return result.stdout.strip()
        else:
            log_info(f"❌ Database query failed: {result.stderr}")
            return ""
    except Exception as e:
        log_info(f"❌ Error executing database query: {e}")
        return ""

def get_high_priority_symbols(limit: int = 100) -> list:
    """Get list of high priority symbols for news refresh."""
    log_info(f"📋 Fetching high priority symbols for news (limit: {limit})")
    
    # Focus on major stocks and recently active symbols
    query = f"""
    SELECT DISTINCT i.symbol 
    FROM intg_instruments i
    LEFT JOIN intg_daily_prices dp ON i.symbol = dp.symbol 
        AND dp.date >= CURRENT_DATE - INTERVAL '7 days'
    WHERE i.active = true 
    AND i.symbol ~ '^[A-Z]{{1,5}}$'
    AND (
        i.symbol IN ('AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA', 'META', 'NVDA', 'NFLX', 'JPM', 'V')
        OR dp.volume > 1000000
        OR EXISTS (
            SELECT 1 FROM intg_daily_prices dp2 
            WHERE dp2.symbol = i.symbol 
            AND dp2.date >= CURRENT_DATE - INTERVAL '1 day'
        )
    )
    ORDER BY 
        CASE WHEN i.symbol IN ('AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA') THEN 1 ELSE 2 END,
        i.symbol 
    LIMIT {limit}
    """
    
    result = run_intg_query(query)
    symbols = []
    
    for line in result.split('\n'):
        line = line.strip()
        if line and line not in ['symbol', '--------', '(', 'rows)'] and 'row' not in line:
            symbols.append(line)
    
    log_info(f"📊 Found {len(symbols)} high priority symbols for news refresh")
    return symbols

def create_news_checkpoint_table():
    """Create checkpoint table for tracking news refresh progress."""
    log_info("🔧 Setting up news checkpoint tracking...")
    
    # Create news table if it doesn't exist
    news_table_query = """
    CREATE TABLE IF NOT EXISTS intg_news (
        id SERIAL PRIMARY KEY,
        symbol VARCHAR(20) NOT NULL,
        vendor VARCHAR(50) NOT NULL,
        news_id VARCHAR(255) NOT NULL,
        title TEXT NOT NULL,
        summary TEXT,
        content TEXT,
        published_at TIMESTAMP NOT NULL,
        url TEXT,
        sentiment_score DECIMAL(5,3),
        author VARCHAR(255),
        source VARCHAR(255),
        keywords TEXT[],
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(symbol, vendor, news_id)
    );
    
    CREATE INDEX IF NOT EXISTS idx_intg_news_symbol_date ON intg_news(symbol, published_at DESC);
    CREATE INDEX IF NOT EXISTS idx_intg_news_vendor_date ON intg_news(vendor, published_at DESC);
    """
    
    run_intg_query(news_table_query)
    
    # Create checkpoint table
    checkpoint_query = """
    CREATE TABLE IF NOT EXISTS intg_news_checkpoint (
        id SERIAL PRIMARY KEY,
        job_date DATE NOT NULL,
        vendor VARCHAR(50) NOT NULL,
        symbols_processed INTEGER DEFAULT 0,
        news_items_inserted INTEGER DEFAULT 0,
        last_symbol VARCHAR(20),
        status VARCHAR(20) DEFAULT 'running',
        started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        completed_at TIMESTAMP,
        error_message TEXT,
        UNIQUE(job_date, vendor)
    );
    """
    
    run_intg_query(checkpoint_query)
    
    # Initialize today's checkpoints
    today = datetime.now().date()
    for vendor in VENDORS:
        init_query = f"""
        INSERT INTO intg_news_checkpoint (job_date, vendor, status)
        VALUES ('{today}', '{vendor}', 'running')
        ON CONFLICT (job_date, vendor) DO NOTHING
        """
        run_intg_query(init_query)

def update_news_checkpoint(vendor: str, symbols_processed: int, news_items_inserted: int, 
                          last_symbol: str = None, status: str = 'running', error: str = None):
    """Update checkpoint for a vendor."""
    today = datetime.now().date()
    
    query = f"""
    UPDATE intg_news_checkpoint 
    SET symbols_processed = {symbols_processed},
        news_items_inserted = {news_items_inserted},
        last_symbol = {f"'{last_symbol}'" if last_symbol else 'NULL'},
        status = '{status}',
        {"completed_at = CURRENT_TIMESTAMP," if status == 'completed' else ""}
        error_message = {f"'{error}'" if error else 'NULL'}
    WHERE job_date = '{today}' AND vendor = '{vendor}'
    """
    
    run_intg_query(query)

def fetch_polygon_news(symbol: str, days_back: int = 1) -> list:
    """Fetch news from Polygon API."""
    import requests
    
    try:
        # Get news for the past few days
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days_back)
        
        url = f"https://api.polygon.io/v2/reference/news"
        params = {
            "ticker": symbol,
            "published_at.gte": start_date.strftime('%Y-%m-%d'),
            "published_at.lte": end_date.strftime('%Y-%m-%d'),
            "limit": 10,
            "apikey": API_KEYS['POLYGON_API_KEY']
        }
        
        response = requests.get(url, params=params, timeout=30)
        if response.status_code == 200:
            data = response.json()
            results = data.get("results", [])
            
            news_items = []
            for item in results:
                # Create unique news ID
                news_id = hashlib.md5(f"{symbol}_{item.get('id', '')}_{item.get('title', '')}".encode()).hexdigest()
                
                news_items.append({
                    'symbol': symbol,
                    'vendor': 'polygon',
                    'news_id': news_id,
                    'title': item.get('title', ''),
                    'summary': item.get('description', ''),
                    'published_at': item.get('published_utc'),
                    'url': item.get('article_url'),
                    'author': item.get('author'),
                    'source': item.get('publisher', {}).get('name') if item.get('publisher') else None,
                    'keywords': item.get('keywords', [])
                })
            
            return news_items
        
        return []
        
    except Exception as e:
        log_info(f"⚠️ Polygon news API error for {symbol}: {e}")
        return []

def fetch_fmp_news(symbol: str, days_back: int = 1) -> list:
    """Fetch news from FMP API."""
    import requests
    
    try:
        url = f"https://financialmodelingprep.com/api/v3/stock_news"
        params = {
            "tickers": symbol,
            "limit": 10,
            "apikey": API_KEYS['FMP_API_KEY']
        }
        
        response = requests.get(url, params=params, timeout=30)
        if response.status_code == 200:
            data = response.json()
            
            news_items = []
            cutoff_date = datetime.now() - timedelta(days=days_back)
            
            for item in data[:10]:  # Limit to recent items
                published_date = datetime.strptime(item.get('publishedDate', ''), '%Y-%m-%d %H:%M:%S')
                if published_date >= cutoff_date:
                    # Create unique news ID
                    news_id = hashlib.md5(f"{symbol}_{item.get('title', '')}_{item.get('publishedDate', '')}".encode()).hexdigest()
                    
                    news_items.append({
                        'symbol': symbol,
                        'vendor': 'fmp',
                        'news_id': news_id,
                        'title': item.get('title', ''),
                        'summary': item.get('text', ''),
                        'published_at': item.get('publishedDate'),
                        'url': item.get('url'),
                        'source': item.get('site')
                    })
            
            return news_items
        
        return []
        
    except Exception as e:
        log_info(f"⚠️ FMP news API error for {symbol}: {e}")
        return []

def fetch_alpha_vantage_news(symbol: str, days_back: int = 1) -> list:
    """Fetch news from Alpha Vantage API."""
    import requests
    
    try:
        url = f"https://www.alphavantage.co/query"
        params = {
            "function": "NEWS_SENTIMENT",
            "tickers": symbol,
            "limit": 10,
            "apikey": API_KEYS['ALPHA_VANTAGE_API_KEY']
        }
        
        response = requests.get(url, params=params, timeout=30)
        if response.status_code == 200:
            data = response.json()
            feed = data.get("feed", [])
            
            news_items = []
            cutoff_date = datetime.now() - timedelta(days=days_back)
            
            for item in feed[:10]:  # Limit to recent items
                time_published = item.get('time_published', '')
                if time_published:
                    try:
                        # Parse Alpha Vantage time format
                        published_date = datetime.strptime(time_published, '%Y%m%dT%H%M%S')
                        if published_date >= cutoff_date:
                            # Create unique news ID
                            news_id = hashlib.md5(f"{symbol}_{item.get('title', '')}_{time_published}".encode()).hexdigest()
                            
                            # Extract sentiment score for the specific ticker
                            sentiment_score = None
                            ticker_sentiments = item.get('ticker_sentiment', [])
                            for ticker_info in ticker_sentiments:
                                if ticker_info.get('ticker') == symbol:
                                    sentiment_score = float(ticker_info.get('relevance_score', 0))
                                    break
                            
                            news_items.append({
                                'symbol': symbol,
                                'vendor': 'alpha_vantage',
                                'news_id': news_id,
                                'title': item.get('title', ''),
                                'summary': item.get('summary', ''),
                                'published_at': published_date.strftime('%Y-%m-%d %H:%M:%S'),
                                'url': item.get('url'),
                                'sentiment_score': sentiment_score,
                                'source': item.get('source')
                            })
                    except ValueError:
                        continue
            
            return news_items
        
        return []
        
    except Exception as e:
        log_info(f"⚠️ Alpha Vantage news API error for {symbol}: {e}")
        return []

def insert_news_data(news_items: list) -> int:
    """Insert news data into intg database."""
    if not news_items:
        return 0
    
    records_inserted = 0
    
    for news_item in news_items:
        if not news_item.get('title'):
            continue
        
        # Escape single quotes in text fields
        title = news_item.get('title', '').replace("'", "''")
        summary = news_item.get('summary', '').replace("'", "''") if news_item.get('summary') else None
        url = news_item.get('url', '').replace("'", "''") if news_item.get('url') else None
        author = news_item.get('author', '').replace("'", "''") if news_item.get('author') else None
        source = news_item.get('source', '').replace("'", "''") if news_item.get('source') else None
        
        query = f"""
        INSERT INTO intg_news 
        (symbol, vendor, news_id, title, summary, published_at, url, 
         sentiment_score, author, source, keywords)
        VALUES (
            '{news_item['symbol']}',
            '{news_item['vendor']}',
            '{news_item['news_id']}',
            '{title}',
            {f"'{summary}'" if summary else 'NULL'},
            '{news_item['published_at']}',
            {f"'{url}'" if url else 'NULL'},
            {news_item.get('sentiment_score') or 'NULL'},
            {f"'{author}'" if author else 'NULL'},
            {f"'{source}'" if source else 'NULL'},
            {f"ARRAY{news_item.get('keywords', [])}" if news_item.get('keywords') else 'NULL'}
        )
        ON CONFLICT (symbol, vendor, news_id) 
        DO UPDATE SET
            title = EXCLUDED.title,
            summary = EXCLUDED.summary,
            published_at = EXCLUDED.published_at,
            sentiment_score = EXCLUDED.sentiment_score,
            updated_at = CURRENT_TIMESTAMP
        """
        
        result = run_intg_query(query)
        if 'INSERT' in result or 'UPDATE' in result:
            records_inserted += 1
    
    return records_inserted

def process_symbol_vendor_news(symbol: str, vendor: str) -> int:
    """Process a single symbol for a specific vendor."""
    try:
        # Fetch data based on vendor
        if vendor == 'polygon':
            news_data = fetch_polygon_news(symbol)
        elif vendor == 'fmp':
            news_data = fetch_fmp_news(symbol)
        elif vendor == 'alpha_vantage':
            news_data = fetch_alpha_vantage_news(symbol)
        else:
            return 0
        
        # Insert data if available
        if news_data:
            news_count = insert_news_data(news_data)
            if news_count > 0:
                with stats_lock:
                    stats['total_news_items'] += news_count
                return news_count
        
        return 0
        
    except Exception as e:
        log_info(f"❌ Error processing {symbol} with {vendor}: {e}")
        return 0
    finally:
        # Rate limiting for news
        time.sleep(RATE_LIMIT_DELAY)

def process_vendor_news_batch(vendor: str, symbols: list) -> dict:
    """Process a batch of symbols for a specific vendor."""
    log_info(f"🚀 Starting {vendor.upper()} news batch: {len(symbols)} symbols")
    
    vendor_stats = {
        'processed': 0,
        'successful': 0,
        'news_items': 0
    }
    
    with ThreadPoolExecutor(max_workers=1) as executor:  # Single thread per vendor for rate limiting
        futures = {
            executor.submit(process_symbol_vendor_news, symbol, vendor): symbol 
            for symbol in symbols
        }
        
        for future in as_completed(futures):
            symbol = futures[future]
            try:
                news_count = future.result()
                vendor_stats['processed'] += 1
                
                if news_count > 0:
                    vendor_stats['successful'] += 1
                    vendor_stats['news_items'] += news_count
                    log_info(f"📰 {symbol}: {news_count} news items from {vendor}")
                
                # Update checkpoint periodically
                if vendor_stats['processed'] % 20 == 0:
                    update_news_checkpoint(
                        vendor, 
                        vendor_stats['processed'], 
                        vendor_stats['news_items'],
                        symbol
                    )
                    log_info(f"📊 {vendor.upper()}: {vendor_stats['processed']}/{len(symbols)} symbols, {vendor_stats['news_items']} news items")
                
            except Exception as e:
                log_info(f"❌ Future error for {symbol}: {e}")
                vendor_stats['processed'] += 1
    
    # Final checkpoint update
    status = 'completed' if vendor_stats['processed'] == len(symbols) else 'partial'
    update_news_checkpoint(vendor, vendor_stats['processed'], vendor_stats['news_items'], status=status)
    
    log_info(f"✅ {vendor.upper()} completed: {vendor_stats['successful']}/{len(symbols)} successful, {vendor_stats['news_items']} news items")
    return vendor_stats

def main():
    """Main daily news refresh job."""
    log_info("🚀 Starting Daily News Refresh Job for ATS-INTG")
    
    # Setup checkpoint tracking
    create_news_checkpoint_table()
    
    # Get high priority symbols
    symbols = get_high_priority_symbols(limit=50)  # Conservative limit for news refresh
    if not symbols:
        log_info("✅ No high priority symbols found for news refresh")
        return True
    
    with stats_lock:
        stats['total_symbols'] = len(symbols)
    
    log_info(f"📋 Processing {len(symbols)} symbols across {len(VENDORS)} vendors")
    
    # Process each vendor sequentially to avoid rate limiting conflicts
    vendor_results = {}
    
    for vendor in VENDORS:
        log_info(f"🔧 Starting vendor: {vendor.upper()}")
        
        try:
            vendor_stats = process_vendor_news_batch(vendor, symbols)
            vendor_results[vendor] = vendor_stats
            
            with stats_lock:
                if vendor_stats['successful'] > 0:
                    stats['successful_vendors'] += 1
                else:
                    stats['failed_vendors'] += 1
            
            # Longer pause between vendors for news
            time.sleep(10)
            
        except Exception as e:
            log_info(f"❌ Vendor {vendor} failed: {e}")
            update_news_checkpoint(vendor, 0, 0, status='failed', error=str(e))
            with stats_lock:
                stats['failed_vendors'] += 1
    
    # Final summary
    log_info("🎉 Daily News Refresh Job Complete!")
    log_info("=" * 60)
    log_info(f"📊 Total symbols: {stats['total_symbols']}")
    log_info(f"✅ Successful vendors: {stats['successful_vendors']}")
    log_info(f"❌ Failed vendors: {stats['failed_vendors']}")
    log_info(f"📰 Total news items inserted: {stats['total_news_items']}")
    
    # Vendor breakdown
    for vendor, result in vendor_results.items():
        log_info(f"   {vendor.upper()}: {result['successful']}/{result['processed']} symbols, {result['news_items']} news items")
    
    # Check final database status
    today = datetime.now().date()
    count_query = f"SELECT COUNT(*) as total FROM intg_news WHERE DATE(published_at) >= '{today - timedelta(days=1)}'"
    result = run_intg_query(count_query)
    log_info(f"🗄️ Recent news in database: {result}")
    
    log_info("=" * 60)
    return stats['successful_vendors'] > 0

if __name__ == "__main__":
    success = main()
    if not success:
        sys.exit(1)