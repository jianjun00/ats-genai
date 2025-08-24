#!/usr/bin/env python3

#!/usr/bin/env python3
"""
Enhanced FMP Minute Backfill with Exponential Backoff

MASSIVE SCALE: Expected 19.66 billion minute records over 20 years
- 10,000 instruments × 98,280 records/year/instrument × 20 years
- Critical gap: FMP has 0 historical minute data vs Polygon (190M) and Tiingo (160M)
"""

import asyncio
import asyncpg
import aiohttp
import logging
import os
import json
import argparse
import random
from datetime import date, datetime, timedelta
from typing import List, Dict, Optional, Tuple, Any
from dataclasses import dataclass, asdict
import sys
from pathlib import Path

@dataclass
class RetryConfig:
"""Retry configuration for minute data API calls"""
max_retries: int = 3
base_delay: float = 2.0
max_delay: float = 180.0
exponential_base: float = 2.0
jitter: bool = True
backoff_statuses: List[int] = None

def __post_init__(self):
if self.backoff_statuses is None:
self.backoff_statuses = [403, 429, 500, 502, 503, 504]

@dataclass
class CircuitBreakerConfig:
"""Circuit breaker for minute data API protection"""
failure_threshold: int = 15
success_threshold: int = 5
timeout_seconds: int = 600

@dataclass
class FMPMinuteBackfillConfig:
"""Configuration for FMP minute backfill - massive scale optimization"""
start_date: date = date(2005, 1, 1)
end_date: date = date(2025, 8, 23)

# Conservative settings for massive volume
batch_size: int = 3               # Very small batches
max_concurrent_requests: int = 2   # Ultra conservative
save_progress_interval: int = 25   # Frequent saves
checkpoint_file: str = "/tmp/fmp_minute_backfill_checkpoint.json"

# Temporal chunking optimized for API limits
days_per_chunk: int = 7           # Weekly chunks

# FMP minute API limits - very conservative
calls_per_minute: int = 50        # Conservative for massive volume
calls_per_day: int = 3000         # Lower limit for minute endpoints  
base_delay: float = 1.2           # 50 calls/min = 1.2s between calls

retry_config: RetryConfig = None
circuit_breaker_config: CircuitBreakerConfig = None

def __post_init__(self):
if self.retry_config is None:
self.retry_config = RetryConfig()
if self.circuit_breaker_config is None:
self.circuit_breaker_config = CircuitBreakerConfig()

@dataclass
class MinuteBackfillProgress:
"""Progress tracking for massive minute data backfill"""
total_symbols: int = 0
completed_symbols: int = 0
total_chunks: int = 0
completed_chunks: int = 0
failed_symbols: List[str] = None
skipped_symbols: List[str] = None
failed_chunks: List[str] = None
total_records_inserted: int = 0
start_time: Optional[datetime] = None
last_checkpoint: Optional[datetime] = None
retry_stats: Dict[str, int] = None
circuit_breaker_trips: int = 0
api_call_stats: Dict[str, int] = None

def __post_init__(self):
if self.failed_symbols is None:
self.failed_symbols = []
if self.skipped_symbols is None:
self.skipped_symbols = []
if self.failed_chunks is None:
self.failed_chunks = []
if self.retry_stats is None:
self.retry_stats = {
'total_retries': 0,
'http_403_retries': 0,
'http_429_retries': 0,
'http_5xx_retries': 0,
'successful_retries': 0,
'timeout_retries': 0
}
if self.api_call_stats is None:
self.api_call_stats = {
'total_calls': 0,
'successful_calls': 0,
'failed_calls': 0,
'empty_responses': 0
}

class CircuitBreaker:
"""Circuit breaker optimized for minute data API calls"""

def __init__(self, config: CircuitBreakerConfig):
self.config = config
self.failure_count = 0
self.success_count = 0
self.state = 'CLOSED'
self.last_failure_time = None

def can_execute(self) -> bool:
"""Check if minute data requests can be executed"""
if self.state == 'CLOSED':
return True
elif self.state == 'OPEN':
if self.last_failure_time and \
(datetime.now() - self.last_failure_time).seconds >= self.config.timeout_seconds:
self.state = 'HALF_OPEN'
self.success_count = 0
return True
return False
elif self.state == 'HALF_OPEN':
return True
return False

def record_success(self):
"""Record successful minute data request"""
if self.state == 'HALF_OPEN':
self.success_count += 1
if self.success_count >= self.config.success_threshold:
self.state = 'CLOSED'
self.failure_count = 0
elif self.state == 'CLOSED':
self.failure_count = max(0, self.failure_count - 1)

def record_failure(self):
"""Record failed minute data request"""
self.failure_count += 1
self.last_failure_time = datetime.now()

if self.state == 'HALF_OPEN':
self.state = 'OPEN'
elif self.state == 'CLOSED' and self.failure_count >= self.config.failure_threshold:
self.state = 'OPEN'

class EnhancedFMPMinuteBackfiller:
"""Enhanced FMP minute backfill for 20-year historical coverage"""

def __init__(self, config: FMPMinuteBackfillConfig):
self.config = config
self.db_url = f"postgresql://postgres:{os.getenv('DB_PASSWORD', 'dev_password')}@{os.getenv('DB_HOST', 'postgres-simple')}:5432/dev_db"
self.logger = logging.getLogger(__name__)

# FMP minute API configuration
self.api_key = os.getenv('FMP_API_KEY')
if not self.api_key:
raise ValueError("FMP_API_KEY environment variable not set")

self.base_url = 'https://financialmodelingprep.com'
self.table_name = 'dev_minute_prices_fmp'

# Initialize enhanced tracking
self.circuit_breaker = CircuitBreaker(config.circuit_breaker_config)
self.progress = MinuteBackfillProgress()

# Daily API call tracking for minute data
self.daily_calls_made = 0
self.last_call_reset = datetime.now().date()

async def exponential_backoff_delay(self, attempt: int, status_code: int) -> float:
"""Enhanced backoff calculation for minute data volume"""
retry_config = self.config.retry_config

delay = retry_config.base_delay * (retry_config.exponential_base ** attempt)
delay = min(delay, retry_config.max_delay)

if retry_config.jitter:
jitter_range = delay * 0.15
delay += random.uniform(-jitter_range, jitter_range)

# Enhanced handling for minute data API limits
if status_code == 429:  # Rate limited - critical for minute data
delay = max(delay, 120.0)
elif status_code == 403:  # Quota exceeded - common with minute data
delay = max(delay, 60.0)
elif status_code >= 500:
delay = max(delay, 10.0)

return max(1.0, delay)

def generate_date_chunks(self, start_date: date, end_date: date) -> List[Tuple[date, date]]:
"""Generate weekly chunks for minute data API calls"""
chunks = []
current_date = start_date

while current_date < end_date:
chunk_end = min(current_date + timedelta(days=self.config.days_per_chunk - 1), end_date)
chunks.append((current_date, chunk_end))
current_date = chunk_end + timedelta(days=1)

return chunks

async def fetch_minute_data_with_retry(self, session: aiohttp.ClientSession, symbol: str, 
start_date: date, end_date: date) -> Optional[List[Dict]]:
"""Fetch minute data with comprehensive retry logic"""

if not self.circuit_breaker.can_execute():
self.logger.warning(f"Circuit breaker OPEN - skipping {symbol}")
self.progress.skipped_symbols.append(f"{symbol}_{start_date}_{end_date}")
return None

# Check daily limits for minute data
if self.last_call_reset < datetime.now().date():
self.daily_calls_made = 0
self.last_call_reset = datetime.now().date()

if self.daily_calls_made >= self.config.calls_per_day:
self.logger.error(f"Daily API limit reached ({self.config.calls_per_day}), stopping minute backfill")
return None

# Build FMP minute data URL - different endpoint than daily
url = f"{self.base_url}/api/v3/historical-chart/1min/{symbol}"
params = {
'from': start_date.strftime('%Y-%m-%d'),
'to': end_date.strftime('%Y-%m-%d'),
'apikey': self.api_key
}

retry_config = self.config.retry_config

for attempt in range(retry_config.max_retries + 1):
try:
# Rate limiting with minute data delays
await asyncio.sleep(self.config.base_delay)

async with session.get(url, params=params) as response:
self.daily_calls_made += 1
self.progress.api_call_stats['total_calls'] += 1

# Success case
if response.status == 200:
data = await response.json()
self.circuit_breaker.record_success()

if attempt > 0:
self.progress.retry_stats['successful_retries'] += 1
self.logger.info(f"✅ Retry successful for {symbol} ({start_date}-{end_date}) after {attempt} attempts")

parsed_data = self.parse_fmp_minute_response(data)

if parsed_data:
self.progress.api_call_stats['successful_calls'] += 1
else:
self.progress.api_call_stats['empty_responses'] += 1

return parsed_data

# Handle retryable errors
elif response.status in retry_config.backoff_statuses:
if attempt < retry_config.max_retries:
delay = await self.exponential_backoff_delay(attempt, response.status)

# Update retry statistics
self.progress.retry_stats['total_retries'] += 1
if response.status == 403:
self.progress.retry_stats['http_403_retries'] += 1
elif response.status == 429:
self.progress.retry_stats['http_429_retries'] += 1
elif response.status >= 500:
self.progress.retry_stats['http_5xx_retries'] += 1

self.logger.warning(
f"🔄 HTTP {response.status} for {symbol} ({start_date}-{end_date}) - "
f"retry {attempt + 1}/{retry_config.max_retries} in {delay:.1f}s"
)

await asyncio.sleep(delay)
continue
else:
self.logger.error(f"❌ Max retries exceeded for {symbol} ({start_date}-{end_date}) - HTTP {response.status}")
self.circuit_breaker.record_failure()
self.progress.api_call_stats['failed_calls'] += 1
return None

# Non-retryable errors
else:
error_text = await response.text()
self.logger.error(f"❌ Non-retryable error for {symbol} ({start_date}-{end_date}) - HTTP {response.status}: {error_text[:100]}")
self.progress.api_call_stats['failed_calls'] += 1
return None

except asyncio.TimeoutError:
if attempt < retry_config.max_retries:
delay = await self.exponential_backoff_delay(attempt, 408)
self.progress.retry_stats['timeout_retries'] += 1
self.logger.warning(f"⏱️  Timeout for {symbol} ({start_date}-{end_date}) - retry {attempt + 1} in {delay:.1f}s")
await asyncio.sleep(delay)
continue
else:
self.logger.error(f"❌ Max retries exceeded for {symbol} ({start_date}-{end_date}) - timeout")
self.circuit_breaker.record_failure()
self.progress.api_call_stats['failed_calls'] += 1
return None

except Exception as e:
if attempt < retry_config.max_retries:
delay = await self.exponential_backoff_delay(attempt, 500)
self.logger.warning(f"🔄 Exception for {symbol} ({start_date}-{end_date}): {e} - retry {attempt + 1} in {delay:.1f}s")
await asyncio.sleep(delay)
continue
else:
self.logger.error(f"❌ Max retries exceeded for {symbol} ({start_date}-{end_date}) - {e}")
self.circuit_breaker.record_failure()
self.progress.api_call_stats['failed_calls'] += 1
break

if self.circuit_breaker.state == 'OPEN':
self.progress.circuit_breaker_trips += 1

return None

def parse_fmp_minute_response(self, data: List[Dict]) -> List[Dict]:
"""Parse FMP minute data response"""
parsed_data = []

try:
# FMP minute data comes as array directly
if isinstance(data, list):
for item in data:
# Parse FMP minute data format
parsed_data.append({
'timestamp': datetime.strptime(item['date'], '%Y-%m-%d %H:%M:%S'),
'open_price': float(item.get('open', 0)),
'high_price': float(item.get('high', 0)),
'low_price': float(item.get('low', 0)),
'close_price': float(item.get('close', 0)),
'volume': int(item.get('volume', 0))
})
except Exception as e:
self.logger.error(f"Error parsing FMP minute response: {e}")

return parsed_data

async def save_fmp_minute_data(self, symbol: str, data: List[Dict]) -> int:
"""Save FMP minute data to database"""
if not data:
return 0

pool = await asyncpg.create_pool(self.db_url, min_size=2, max_size=5)

try:
async with pool.acquire() as conn:
# Get instrument ID
instrument_id = await conn.fetchval(
"SELECT id FROM dev_instruments WHERE symbol = $1", symbol
)

if not instrument_id:
self.logger.warning(f"Instrument not found for {symbol}")
return 0

# Prepare minute data inserts
insert_data = []
for record in data:
insert_data.append((
instrument_id,
record['timestamp'],
record.get('open_price'),
record.get('high_price'),
record.get('low_price'),
record.get('close_price'),
record.get('volume')
))

# Create table if not exists (minute data specific)
await conn.execute(f"""
CREATE TABLE IF NOT EXISTS {self.table_name} (
id SERIAL PRIMARY KEY,
instrument_id INTEGER NOT NULL REFERENCES dev_instruments(id),
timestamp TIMESTAMP WITHOUT TIME ZONE NOT NULL,
open_price NUMERIC(10, 4),
high_price NUMERIC(10, 4),
low_price NUMERIC(10, 4),
close_price NUMERIC(10, 4),
volume BIGINT,
created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW(),
updated_at TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW(),
UNIQUE(instrument_id, timestamp)
)
""")

sql = f"""
INSERT INTO {self.table_name}
(instrument_id, timestamp, open_price, high_price, low_price, close_price, volume)
VALUES ($1, $2, $3, $4, $5, $6, $7)
ON CONFLICT (instrument_id, timestamp) DO UPDATE SET
open_price = EXCLUDED.open_price,
high_price = EXCLUDED.high_price,
low_price = EXCLUDED.low_price,
close_price = EXCLUDED.close_price,
volume = EXCLUDED.volume,
updated_at = NOW()
"""

await conn.executemany(sql, insert_data)
return len(insert_data)

except Exception as e:
self.logger.error(f"Database error saving minute data for {symbol}: {e}")
return 0

finally:
await pool.close()

async def get_target_symbols(self) -> List[str]:
"""Get sample of instruments for initial FMP minute backfill test"""
pool = await asyncpg.create_pool(self.db_url, min_size=1, max_size=2)

try:
async with pool.acquire() as conn:
# Start with top 100 symbols for testing massive minute backfill
rows = await conn.fetch("""
SELECT DISTINCT symbol
FROM dev_instruments
WHERE symbol IS NOT NULL
ORDER BY symbol
LIMIT 100
""")

symbols = [row['symbol'] for row in rows]
self.logger.info(f"Found {len(symbols)} instruments for FMP minute backfill (TEST MODE)")

return symbols

finally:
await pool.close()

def save_checkpoint(self):
"""Save minute backfill progress checkpoint"""
checkpoint_data = {
'config': asdict(self.config),
'progress': asdict(self.progress),
'circuit_breaker_state': {
'state': self.circuit_breaker.state,
'failure_count': self.circuit_breaker.failure_count,
'success_count': self.circuit_breaker.success_count
},
'daily_calls_made': self.daily_calls_made,
'last_call_reset': self.last_call_reset.isoformat(),
'timestamp': datetime.now().isoformat()
}

with open(self.config.checkpoint_file, 'w') as f:
json.dump(checkpoint_data, f, indent=2, default=str)

self.progress.last_checkpoint = datetime.now()
self.logger.info(f"💾 Checkpoint saved: {self.progress.completed_chunks}/{self.progress.total_chunks} chunks, "
f"{self.progress.total_records_inserted:,} minute records")

async def run_fmp_minute_backfill(self) -> MinuteBackfillProgress:
"""Run comprehensive FMP minute backfill for 20 years"""

self.progress.start_time = datetime.now()

# Get target symbols (limited for initial test)
all_symbols = await self.get_target_symbols()
self.progress.total_symbols = len(all_symbols)

# Calculate total chunks for minute data
date_chunks = self.generate_date_chunks(self.config.start_date, self.config.end_date)
self.progress.total_chunks = len(all_symbols) * len(date_chunks)

self.logger.info(f"🚀 Starting Enhanced FMP Minute Backfill (TEST MODE)")
self.logger.info(f"📅 Period: {self.config.start_date} to {self.config.end_date}")
self.logger.info(f"🎯 Symbols: {len(all_symbols):,} (TEST: top 100)")
self.logger.info(f"📊 Date chunks: {len(date_chunks):,} ({self.config.days_per_chunk}-day chunks)")
self.logger.info(f"🔢 Total chunks: {self.progress.total_chunks:,}")
self.logger.info(f"📈 Expected records: {len(all_symbols) * 98280 * 20:,} (~{len(all_symbols) * 98280 * 20 / 1000000:.0f}M records)")

# Create session for minute data
timeout = aiohttp.ClientTimeout(total=120)

async with aiohttp.ClientSession(timeout=timeout) as session:

# Process symbols in batches
for symbol_batch_start in range(0, len(all_symbols), self.config.batch_size):
batch_symbols = all_symbols[symbol_batch_start:symbol_batch_start + self.config.batch_size]

batch_num = symbol_batch_start // self.config.batch_size + 1
total_batches = (len(all_symbols) + self.config.batch_size - 1) // self.config.batch_size

self.logger.info(f"📦 Processing symbol batch {batch_num}/{total_batches}: {batch_symbols}")

# Process each symbol across all date chunks
semaphore = asyncio.Semaphore(self.config.max_concurrent_requests)

async def process_symbol_chunks(symbol: str):
"""Process all date chunks for a symbol"""
symbol_records = 0
symbol_chunks = 0

for chunk_start, chunk_end in date_chunks:
async with semaphore:
try:
chunk_id = f"{symbol}_{chunk_start}_{chunk_end}"

data = await self.fetch_minute_data_with_retry(
session, symbol, chunk_start, chunk_end
)

if data:
records_saved = await self.save_fmp_minute_data(symbol, data)
symbol_records += records_saved
self.progress.total_records_inserted += records_saved

if records_saved > 0:
self.logger.info(f"✅ {chunk_id}: {records_saved:,} minute records")
else:
self.progress.failed_chunks.append(chunk_id)

except Exception as e:
self.logger.error(f"❌ Error processing {chunk_id}: {e}")
self.progress.failed_chunks.append(chunk_id)

finally:
self.progress.completed_chunks += 1
symbol_chunks += 1

self.logger.info(f"🎯 {symbol} complete: {symbol_records:,} minute records, {symbol_chunks} chunks")
self.progress.completed_symbols += 1

# Execute symbol batch
tasks = [process_symbol_chunks(symbol) for symbol in batch_symbols]
await asyncio.gather(*tasks, return_exceptions=True)

# Save checkpoint after each batch
self.save_checkpoint()

# Progress report
completion_pct = (self.progress.completed_chunks / self.progress.total_chunks) * 100
elapsed = datetime.now() - self.progress.start_time

if elapsed.total_seconds() > 0:
rate = self.progress.completed_chunks / elapsed.total_seconds() * 3600  # chunks/hour

self.logger.info(f"📊 Progress: {completion_pct:.2f}% complete")
self.logger.info(f"📈 Rate: {rate:.1f} chunks/hour, {self.progress.total_records_inserted:,} minute records")
self.logger.info(f"🔄 API Stats: {self.progress.api_call_stats['successful_calls']}/{self.progress.api_call_stats['total_calls']} successful")
self.logger.info(f"⚡ Circuit breaker: {self.circuit_breaker.state}")

# Final checkpoint and summary
self.save_checkpoint()

elapsed = datetime.now() - self.progress.start_time

self.logger.info(f"🎉 FMP Minute Backfill Complete!")
self.logger.info(f"⏱️  Total time: {elapsed}")
self.logger.info(f"✅ Total minute records: {self.progress.total_records_inserted:,}")
self.logger.info(f"📊 Chunks completed: {self.progress.completed_chunks:,}/{self.progress.total_chunks:,}")
self.logger.info(f"❌ Failed chunks: {len(self.progress.failed_chunks):,}")
self.logger.info(f"⏭️  Skipped symbols: {len(self.progress.skipped_symbols):,}")
self.logger.info(f"🔄 Circuit breaker trips: {self.progress.circuit_breaker_trips}")

return self.progress

def main():
"""Main execution for FMP minute backfill"""
# Configure logging for minute data volume
logging.basicConfig(
level=logging.INFO,
format='%(asctime)s - %(levelname)s - %(message)s',
handlers=[
logging.StreamHandler(),
logging.FileHandler('/tmp/fmp_minute_backfill.log')
]
)

logger = logging.getLogger(__name__)

try:
config = FMPMinuteBackfillConfig(
start_date=date(2005, 1, 1),
end_date=date(2025, 8, 23),
batch_size=3,
days_per_chunk=7
)

backfiller = EnhancedFMPMinuteBackfiller(config)
asyncio.run(backfiller.run_fmp_minute_backfill())

except KeyboardInterrupt:
logger.info("🛑 FMP minute backfill interrupted by user")
except Exception as e:
logger.error(f"💥 FMP minute backfill failed: {e}")
raise

if __name__ == "__main__":
main()
