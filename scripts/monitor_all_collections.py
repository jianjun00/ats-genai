#!/usr/bin/env python3
"""
ATS Multi-Vendor Collection Monitor

Comprehensive monitoring dashboard for all active collection jobs:
- Daily price backfills (Polygon, Tiingo, EODHD)  
- Financial events collection (Polygon, EODHD, Tiingo)
- News collection
- Real-time progress tracking
- Performance metrics and ETA calculations

Usage:
    python monitor_all_collections.py --refresh 10
    python monitor_all_collections.py --summary-only
"""

import sys
sys.path.append('/workspace/src')

import os
import asyncio
import asyncpg
import time
import argparse
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import subprocess
import json

class ATSCollectionMonitor:
    """
    Comprehensive monitor for all ATS data collection jobs.
    
    Tracks:
    - Multi-vendor price backfills (30-year historical)
    - Financial events collection (earnings, corporate actions, news)
    - Collection rates, progress, and ETAs
    - Error rates and performance metrics
    """
    
    def __init__(self):
        self.collection_jobs = {
            'price_backfills': [
                {'name': 'Polygon 30Y Daily', 'log': '/tmp/polygon_30year_daily_backfill.log', 'table': 'dev_daily_prices_polygon_30year'},
                {'name': 'Tiingo 30Y Daily', 'log': '/tmp/tiingo_30year_backfill.log', 'table': 'dev_daily_prices_tiingo_30year'},
                {'name': 'EODHD 30Y Daily', 'log': '/tmp/eodhd_30year_backfill.log', 'table': 'dev_daily_prices_eodhd_30year'},
            ],
            'events_collection': [
                {'name': 'Polygon Earnings', 'log': '/tmp/polygon_earnings_fixed.log', 'table': 'dev_financial_events', 'vendor': 'polygon'},
                {'name': 'EODHD Events', 'log': '/tmp/eodhd_events.log', 'table': 'dev_financial_events', 'vendor': 'eodhd'},
                {'name': 'Tiingo Events', 'log': '/tmp/tiingo_events.log', 'table': 'dev_financial_events', 'vendor': 'tiingo'},
            ],
            'minute_data': [
                {'name': 'Polygon Minutes', 'log': '/tmp/polygon_minute_backfill.log', 'table': 'dev_minute_prices_polygon'},
            ]
        }
        
        self.target_symbols = 15000  # Approximate number of instruments
        self.start_time = datetime.now()

    async def get_database_connection(self):
        """Get database connection."""
        return await asyncpg.connect(
            host='postgres',
            port=5432,
            user='postgres',
            password='dev_password',
            database='dev_db'
        )

    def get_process_status(self, log_path: str) -> Dict:
        """Get process status from log file."""
        status = {
            'running': False,
            'last_activity': None,
            'current_symbol': None,
            'progress_pct': 0.0,
            'events_collected': 0,
            'errors': 0,
            'rate_per_hour': 0.0,
            'eta_hours': None
        }
        
        try:
            if not os.path.exists(log_path):
                return status
            
            # Check if process is active (log modified recently)
            log_mtime = os.path.getmtime(log_path)
            last_activity = datetime.fromtimestamp(log_mtime)
            status['last_activity'] = last_activity
            status['running'] = (datetime.now() - last_activity).total_seconds() < 300  # 5 minutes
            
            # Parse last 50 lines for progress info
            try:
                result = subprocess.run(['tail', '-50', log_path], 
                                      capture_output=True, text=True, timeout=5)
                log_lines = result.stdout.split('\n')
                
                for line in reversed(log_lines):
                    if not line.strip():
                        continue
                    
                    # Extract current symbol
                    if 'Processing' in line or 'Collecting' in line:
                        parts = line.split()
                        for i, part in enumerate(parts):
                            if part.endswith('...') or part.endswith(':'):
                                symbol = part.replace('...', '').replace(':', '')
                                if len(symbol) <= 6 and symbol.isalnum():
                                    status['current_symbol'] = symbol
                                    break
                    
                    # Extract progress percentage
                    if 'Progress:' in line and '%' in line:
                        try:
                            pct_start = line.find('(') + 1
                            pct_end = line.find('%)')
                            if pct_start > 0 and pct_end > pct_start:
                                status['progress_pct'] = float(line[pct_start:pct_end])
                        except:
                            pass
                    
                    # Extract events/records collected
                    if 'events collected' in line or 'records' in line:
                        parts = line.split()
                        for part in parts:
                            if part.replace(',', '').isdigit():
                                status['events_collected'] = int(part.replace(',', ''))
                                break
                    
                    # Count errors
                    if 'ERROR' in line or '❌' in line:
                        status['errors'] += 1
                        
            except subprocess.TimeoutExpired:
                pass
                
        except Exception as e:
            print(f"Error parsing log {log_path}: {e}")
        
        return status

    async def get_database_stats(self, table_name: str, vendor: str = None) -> Dict:
        """Get database statistics for a table."""
        conn = await self.get_database_connection()
        try:
            if vendor:
                # For events table, filter by vendor
                count_query = f"SELECT COUNT(*) FROM {table_name} WHERE vendor = $1"
                count = await conn.fetchval(count_query, vendor)
                
                latest_query = f"SELECT MAX(created_at) FROM {table_name} WHERE vendor = $1"
                latest = await conn.fetchval(latest_query, vendor)
                
                symbols_query = f"SELECT COUNT(DISTINCT symbol) FROM {table_name} WHERE vendor = $1"
                unique_symbols = await conn.fetchval(symbols_query, vendor)
            else:
                # For price tables
                count = await conn.fetchval(f"SELECT COUNT(*) FROM {table_name}")
                latest = await conn.fetchval(f"SELECT MAX(created_at) FROM {table_name}")
                unique_symbols = await conn.fetchval(f"SELECT COUNT(DISTINCT symbol) FROM {table_name}")
            
            return {
                'total_records': count or 0,
                'unique_symbols': unique_symbols or 0,
                'latest_record': latest
            }
        except Exception as e:
            return {
                'total_records': 0,
                'unique_symbols': 0,
                'latest_record': None,
                'error': str(e)
            }
        finally:
            await conn.close()

    def calculate_eta(self, progress_pct: float, elapsed_minutes: float) -> Optional[float]:
        """Calculate estimated time to completion."""
        if progress_pct <= 0 or elapsed_minutes <= 0:
            return None
        
        total_estimated_minutes = (elapsed_minutes / progress_pct) * 100
        remaining_minutes = total_estimated_minutes - elapsed_minutes
        return remaining_minutes / 60  # Return hours

    def format_duration(self, hours: float) -> str:
        """Format duration in human readable format."""
        if hours is None:
            return "Unknown"
        
        if hours < 1:
            return f"{int(hours * 60)}m"
        elif hours < 24:
            return f"{hours:.1f}h"
        else:
            days = int(hours // 24)
            remaining_hours = hours % 24
            return f"{days}d {remaining_hours:.1f}h"

    async def get_comprehensive_status(self) -> Dict:
        """Get comprehensive status of all collections."""
        status = {
            'timestamp': datetime.now().isoformat(),
            'monitoring_duration': (datetime.now() - self.start_time).total_seconds() / 60,
            'categories': {}
        }
        
        for category, jobs in self.collection_jobs.items():
            category_status = {
                'jobs': [],
                'total_records': 0,
                'total_symbols': 0,
                'active_jobs': 0
            }
            
            for job in jobs:
                # Get process status from logs
                process_status = self.get_process_status(job['log'])
                
                # Get database statistics
                db_stats = await self.get_database_stats(
                    job['table'], 
                    job.get('vendor')
                )
                
                # Calculate performance metrics
                elapsed_minutes = (datetime.now() - self.start_time).total_seconds() / 60
                eta_hours = self.calculate_eta(process_status['progress_pct'], elapsed_minutes)
                
                job_status = {
                    'name': job['name'],
                    'running': process_status['running'],
                    'last_activity': process_status['last_activity'].isoformat() if process_status['last_activity'] else None,
                    'current_symbol': process_status['current_symbol'],
                    'progress_pct': process_status['progress_pct'],
                    'database_records': db_stats['total_records'],
                    'unique_symbols': db_stats['unique_symbols'],
                    'errors': process_status['errors'],
                    'eta_hours': eta_hours,
                    'latest_record': db_stats['latest_record'].isoformat() if db_stats['latest_record'] else None
                }
                
                category_status['jobs'].append(job_status)
                category_status['total_records'] += db_stats['total_records']
                category_status['total_symbols'] += db_stats['unique_symbols']
                if process_status['running']:
                    category_status['active_jobs'] += 1
            
            status['categories'][category] = category_status
        
        return status

    def print_status_dashboard(self, status: Dict):
        """Print comprehensive status dashboard."""
        print("\n" + "="*100)
        print("🔍 ATS MULTI-VENDOR COLLECTION MONITOR")
        print("="*100)
        print(f"📊 Monitor Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"⏱️  Running Since: {self.format_duration(status['monitoring_duration']/60)}")
        print()
        
        # Overall Summary
        total_active = sum(cat['active_jobs'] for cat in status['categories'].values())
        total_records = sum(cat['total_records'] for cat in status['categories'].values())
        total_symbols = sum(cat['total_symbols'] for cat in status['categories'].values())
        
        print(f"🚀 OVERALL STATUS: {total_active} Active Jobs | {total_records:,} Total Records | {total_symbols:,} Unique Symbols")
        print()
        
        # Category Details
        for category, cat_data in status['categories'].items():
            print(f"📋 {category.replace('_', ' ').upper()}")
            print("-" * 80)
            
            for job in cat_data['jobs']:
                status_icon = "🟢" if job['running'] else "🔴"
                progress_bar = self.create_progress_bar(job['progress_pct'])
                
                print(f"{status_icon} {job['name']:<20} | {progress_bar} | {job['progress_pct']:.1f}%")
                
                if job['current_symbol']:
                    print(f"   📈 Current: {job['current_symbol']:<8} | Records: {job['database_records']:,} | Symbols: {job['unique_symbols']:,}")
                else:
                    print(f"   📊 Records: {job['database_records']:,} | Symbols: {job['unique_symbols']:,}")
                
                if job['eta_hours']:
                    eta_str = self.format_duration(job['eta_hours'])
                    print(f"   ⏰ ETA: {eta_str}")
                
                if job['errors'] > 0:
                    print(f"   ⚠️  Errors: {job['errors']}")
                
                if job['last_activity']:
                    last_activity = datetime.fromisoformat(job['last_activity'])
                    minutes_ago = (datetime.now() - last_activity).total_seconds() / 60
                    print(f"   🕐 Last Activity: {minutes_ago:.1f} minutes ago")
                
                print()
        
        # Performance Summary
        print("📊 PERFORMANCE SUMMARY")
        print("-" * 80)
        
        for category, cat_data in status['categories'].items():
            active_jobs = cat_data['active_jobs']
            total_jobs = len(cat_data['jobs'])
            completion_rate = (total_jobs - active_jobs) / total_jobs * 100 if total_jobs > 0 else 0
            
            print(f"{category.replace('_', ' ').title():<20} | Active: {active_jobs}/{total_jobs} | Records: {cat_data['total_records']:,}")
        
        print("\n" + "="*100)

    def create_progress_bar(self, percentage: float, width: int = 20) -> str:
        """Create a visual progress bar."""
        filled = int(width * percentage / 100)
        bar = "█" * filled + "░" * (width - filled)
        return f"[{bar}]"

    async def run_continuous_monitor(self, refresh_seconds: int = 30):
        """Run continuous monitoring with periodic refresh."""
        try:
            while True:
                # Clear screen (works on most terminals)
                os.system('clear' if os.name == 'posix' else 'cls')
                
                # Get and display status
                status = await self.get_comprehensive_status()
                self.print_status_dashboard(status)
                
                print(f"\n🔄 Refreshing in {refresh_seconds} seconds... (Press Ctrl+C to exit)")
                
                # Wait for next refresh
                await asyncio.sleep(refresh_seconds)
                
        except KeyboardInterrupt:
            print("\n👋 Monitor stopped by user")
        except Exception as e:
            print(f"❌ Monitor error: {e}")

    async def print_summary_once(self):
        """Print one-time summary and exit."""
        status = await self.get_comprehensive_status()
        self.print_status_dashboard(status)

async def main():
    parser = argparse.ArgumentParser(description="ATS Multi-Vendor Collection Monitor")
    parser.add_argument('--refresh', type=int, default=30, 
                       help='Refresh interval in seconds (default: 30)')
    parser.add_argument('--summary-only', action='store_true', 
                       help='Print summary once and exit')
    
    args = parser.parse_args()
    
    monitor = ATSCollectionMonitor()
    
    if args.summary_only:
        await monitor.print_summary_once()
    else:
        print("🚀 Starting ATS Multi-Vendor Collection Monitor...")
        print(f"📊 Refresh interval: {args.refresh} seconds")
        await monitor.run_continuous_monitor(args.refresh)

if __name__ == "__main__":
    asyncio.run(main())