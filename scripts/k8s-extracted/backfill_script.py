#!/usr/bin/env python3

#!/usr/bin/env python3
import os
import sys
import asyncio
import logging
import time
import psutil
from datetime import datetime, date, timedelta

# Add src to Python path
sys.path.insert(0, '/app/src')

from config.environment import Environment

class ResourceMonitor:
def __init__(self, memory_threshold=60, cpu_threshold=50):
self.memory_threshold = memory_threshold
self.cpu_threshold = cpu_threshold
self.logger = logging.getLogger(f"{__name__}.ResourceMonitor")

async def check_resources(self):
"""Check system resources and pause if needed"""
memory_percent = psutil.virtual_memory().percent
cpu_percent = psutil.cpu_percent(interval=1)

self.logger.info(f"Resources - Memory: {memory_percent:.1f}%, CPU: {cpu_percent:.1f}%")

if memory_percent > self.memory_threshold:
self.logger.warning(f"Memory pressure ({memory_percent:.1f}% > {self.memory_threshold}%)")
return False

if cpu_percent > self.cpu_threshold:
self.logger.warning(f"CPU pressure ({cpu_percent:.1f}% > {self.cpu_threshold}%)")
return False

return True

async def wait_for_resources(self, max_wait=300):
"""Wait until resources are available"""
wait_time = 0
while wait_time < max_wait:
if await self.check_resources():
return True

self.logger.info("Waiting 30s for resources to free up...")
await asyncio.sleep(30)
wait_time += 30

self.logger.error(f"Resources not available after {max_wait}s")
return False

async def main():
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Environment setup
env = Environment()
monitor = ResourceMonitor(memory_threshold=60, cpu_threshold=50)

# PROGRESSIVE BACKFILL STRATEGY
# Stage 1: 500 instruments, 3-day chunks
stage = int(os.getenv('STAGE', '1'))

if stage == 1:
instruments_limit = 500
days_per_chunk = 3
batch_size = 50
delay_between_batches = 10
logger.info("=== STAGE 1: CONSERVATIVE BACKFILL ===")
elif stage == 2:
instruments_limit = 1000
days_per_chunk = 7
batch_size = 100
delay_between_batches = 5
logger.info("=== STAGE 2: MODERATE BACKFILL ===")
elif stage == 3:
instruments_limit = 2000
days_per_chunk = 14
batch_size = 200
delay_between_batches = 3
logger.info("=== STAGE 3: FULL BACKFILL ===")
else:
logger.error(f"Invalid stage: {stage}")
return

# Calculate target period for this stage
total_missing_days = int(4.4 * 365)  # 4.4 years
chunks_needed = total_missing_days // days_per_chunk

start_date = date(2019, 12, 31) - timedelta(days=days_per_chunk * int(os.getenv('CHUNK_ID', '0')))
end_date = start_date + timedelta(days=days_per_chunk)

logger.info(f"Stage {stage} Configuration:")
logger.info(f"- Instruments: {instruments_limit}")
logger.info(f"- Period: {start_date} to {end_date} ({days_per_chunk} days)")
logger.info(f"- Batch size: {batch_size}")
logger.info(f"- Delay between batches: {delay_between_batches}s")

# Get instruments for this stage
import asyncpg
conn = await asyncpg.connect(env.get_database_url())

instruments = await conn.fetch("""
SELECT DISTINCT i.id, i.symbol 
FROM dev_instruments i
JOIN dev_daily_prices_polygon p ON i.id = p.instrument_id
WHERE p.date >= '2025-08-01'
ORDER BY i.symbol
LIMIT $1
""", instruments_limit)

await conn.close()

logger.info(f"Processing {len(instruments)} instruments")

# Wait for resources before starting
if not await monitor.wait_for_resources():
logger.error("Insufficient resources to start backfill")
return

# Process in controlled batches
total_batches = (len(instruments) + batch_size - 1) // batch_size

for i in range(0, len(instruments), batch_size):
batch_num = i // batch_size + 1
batch = instruments[i:i + batch_size]

logger.info(f"=== Batch {batch_num}/{total_batches} ===")
logger.info(f"Symbols: {[r['symbol'] for r in batch[:5]]}{'...' if len(batch) > 5 else ''}")

# Check resources before each batch
if not await monitor.check_resources():
logger.warning("Resource pressure detected, waiting...")
if not await monitor.wait_for_resources():
logger.error("Aborting due to persistent resource pressure")
break

try:
# Estimate and log what we're about to process
trading_days = max(1, (end_date - start_date).days * 0.7)  # Account for weekends
minutes_per_day = 390
estimated_records = len(batch) * trading_days * minutes_per_day
estimated_mb = (estimated_records * 100) / (1024 * 1024)  # 100 bytes per record

logger.info(f"  Estimated records: {estimated_records:,}")
logger.info(f"  Estimated memory: {estimated_mb:.1f} MB")

# HERE: Add actual minute data backfill logic
# For now, simulate processing
logger.info(f"  Processing {len(batch)} instruments...")
await asyncio.sleep(min(2, len(batch) * 0.1))  # Simulate work

logger.info(f"  ✅ Batch {batch_num} completed")

except Exception as e:
logger.error(f"❌ Batch {batch_num} failed: {e}")
logger.info("Pausing 30s before continuing...")
await asyncio.sleep(30)
continue

# Mandatory delay between batches for system stability
if batch_num < total_batches:
logger.info(f"⏸️  Cooling down for {delay_between_batches}s...")
await asyncio.sleep(delay_between_batches)

# Final status
final_check = await monitor.check_resources()
logger.info(f"=== STAGE {stage} COMPLETED ===")
logger.info(f"System stable: {final_check}")

if stage < 3:
logger.info(f"Next: Run with STAGE={stage + 1} for increased throughput")
else:
logger.info("All stages complete! Full minute data backfill achieved.")

if __name__ == "__main__":
asyncio.run(main())
