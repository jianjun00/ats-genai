#!/usr/bin/env python3
"""
Minute Data Backfill Monitor

Monitors system resources and minute data backfill progress to prevent crashes.
"""

import asyncio
import asyncpg
import psutil
import time
import subprocess
from datetime import datetime, timedelta
import logging
import os
import sys

# Add src to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'src'))

from config.environment import Environment

class MinuteBackfillMonitor:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.env = Environment()
        
        # Thresholds for emergency intervention
        self.memory_emergency = 85  # %
        self.cpu_emergency = 80     # %
        self.memory_warning = 70    # %
        self.cpu_warning = 60       # %
        
    async def get_system_resources(self):
        """Get current system resource usage"""
        memory = psutil.virtual_memory()
        cpu_percent = psutil.cpu_percent(interval=1)
        disk = psutil.disk_usage('/')
        
        return {
            'memory_percent': memory.percent,
            'memory_available_gb': memory.available / (1024**3),
            'cpu_percent': cpu_percent,
            'disk_percent': disk.percent,
            'disk_free_gb': disk.free / (1024**3)
        }
    
    async def get_minute_data_stats(self):
        """Get current minute data statistics"""
        try:
            conn = await asyncpg.connect(self.env.get_database_url())
            
            # Count current minute data records
            polygon_count = await conn.fetchval("SELECT COUNT(*) FROM dev_minute_prices_polygon")
            tiingo_count = await conn.fetchval("SELECT COUNT(*) FROM dev_minute_prices_tiingo")
            
            # Get date ranges
            polygon_range = await conn.fetchrow("""
                SELECT MIN(timestamp) as min_ts, MAX(timestamp) as max_ts 
                FROM dev_minute_prices_polygon
            """)
            
            tiingo_range = await conn.fetchrow("""
                SELECT MIN(timestamp) as min_ts, MAX(timestamp) as max_ts 
                FROM dev_minute_prices_tiingo
            """)
            
            await conn.close()
            
            return {
                'polygon_records': polygon_count,
                'tiingo_records': tiingo_count,
                'total_records': polygon_count + tiingo_count,
                'polygon_range': polygon_range,
                'tiingo_range': tiingo_range
            }
            
        except Exception as e:
            self.logger.error(f"Database query failed: {e}")
            return None
    
    def get_k8s_jobs_status(self):
        """Get status of minute backfill jobs"""
        try:
            result = subprocess.run([
                'kubectl', 'get', 'jobs', '-n', 'ats-dev',
                '--selector=app=minute-backfill',
                '-o', 'json'
            ], capture_output=True, text=True)
            
            if result.returncode == 0:
                import json
                jobs_data = json.loads(result.stdout)
                return jobs_data.get('items', [])
            else:
                self.logger.warning(f"kubectl command failed: {result.stderr}")
                return []
                
        except Exception as e:
            self.logger.error(f"Failed to get k8s jobs: {e}")
            return []
    
    def emergency_stop_jobs(self):
        """Emergency stop all minute backfill jobs"""
        self.logger.critical("🚨 EMERGENCY STOP - Killing all minute backfill jobs")
        
        try:
            # Delete running minute backfill jobs
            subprocess.run([
                'kubectl', 'delete', 'jobs', '-n', 'ats-dev',
                '--selector=app=minute-backfill'
            ], check=False)
            
            # Kill related pods
            subprocess.run([
                'kubectl', 'delete', 'pods', '-n', 'ats-dev',
                '--selector=job-name=minute-backfill'
            ], check=False)
            
            self.logger.critical("Emergency stop completed")
            return True
            
        except Exception as e:
            self.logger.error(f"Emergency stop failed: {e}")
            return False
    
    async def check_and_alert(self):
        """Check system status and take action if needed"""
        resources = await self.get_system_resources()
        minute_stats = await self.get_minute_data_stats()
        
        status = "🟢 HEALTHY"
        actions_taken = []
        
        # Check for emergency conditions
        if (resources['memory_percent'] > self.memory_emergency or 
            resources['cpu_percent'] > self.cpu_emergency):
            
            status = "🔴 EMERGENCY"
            self.logger.critical(f"EMERGENCY: Memory {resources['memory_percent']:.1f}%, CPU {resources['cpu_percent']:.1f}%")
            
            if self.emergency_stop_jobs():
                actions_taken.append("Stopped all minute backfill jobs")
        
        # Check for warning conditions
        elif (resources['memory_percent'] > self.memory_warning or 
              resources['cpu_percent'] > self.cpu_warning):
            
            status = "🟡 WARNING"
            self.logger.warning(f"WARNING: Memory {resources['memory_percent']:.1f}%, CPU {resources['cpu_percent']:.1f}%")
        
        # Log status
        self.logger.info(f"""
=== MINUTE BACKFILL MONITOR STATUS ===
Status: {status}
Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

System Resources:
- Memory: {resources['memory_percent']:.1f}% ({resources['memory_available_gb']:.1f}GB available)
- CPU: {resources['cpu_percent']:.1f}%
- Disk: {resources['disk_percent']:.1f}% ({resources['disk_free_gb']:.1f}GB free)

Minute Data Stats:
- Polygon records: {minute_stats['polygon_records']:,} if minute_stats else 'N/A'
- Tiingo records: {minute_stats['tiingo_records']:,} if minute_stats else 'N/A'
- Total records: {minute_stats['total_records']:,} if minute_stats else 'N/A'

Actions taken: {actions_taken if actions_taken else 'None'}
        """)
        
        return status, resources, minute_stats
    
    async def run_monitoring(self, interval=60):
        """Run continuous monitoring"""
        self.logger.info("Starting minute backfill monitoring...")
        
        while True:
            try:
                status, resources, minute_stats = await self.check_and_alert()
                
                # If in emergency state, check more frequently
                if status == "🔴 EMERGENCY":
                    await asyncio.sleep(10)
                elif status == "🟡 WARNING":
                    await asyncio.sleep(30)
                else:
                    await asyncio.sleep(interval)
                    
            except KeyboardInterrupt:
                self.logger.info("Monitoring stopped by user")
                break
            except Exception as e:
                self.logger.error(f"Monitoring error: {e}")
                await asyncio.sleep(30)

async def main():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    monitor = MinuteBackfillMonitor()
    
    # Run one-time check or continuous monitoring
    mode = os.getenv('MODE', 'continuous')
    
    if mode == 'check':
        status, resources, minute_stats = await monitor.check_and_alert()
        print(f"Status: {status}")
    else:
        await monitor.run_monitoring()

if __name__ == "__main__":
    asyncio.run(main())