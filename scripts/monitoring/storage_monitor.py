#!/usr/bin/env python3
"""
Time-Series Storage System Monitor

Monitors the file-based storage system for:
- Storage capacity and usage
- File integrity and health
- Performance metrics
- Error rates and alerts

Provides Prometheus metrics and health checks for observability.
"""

import asyncio
import os
import time
import psutil
import logging
import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional
import aiofiles

try:
    from prometheus_client import start_http_server, Gauge, Counter, Histogram
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False

class StorageMonitor:
    """Monitor for time-series file storage system"""
    
    def __init__(self, storage_path: str = "/data/monthly/interval"):
        self.storage_path = Path(storage_path)
        self.logger = logging.getLogger(__name__)
        self.monitoring_interval = int(os.getenv("MONITORING_INTERVAL", "30"))
        
        # Prometheus metrics
        if PROMETHEUS_AVAILABLE:
            self.setup_prometheus_metrics()
        
        # Internal stats
        self.stats = {
            'files_monitored': 0,
            'total_size_bytes': 0,
            'total_records': 0,
            'files_with_errors': 0,
            'last_check_time': None,
            'uptime_start': datetime.now()
        }
    
    def setup_prometheus_metrics(self):
        """Setup Prometheus metrics"""
        self.storage_size_bytes = Gauge('timeseries_storage_size_bytes', 'Total storage size in bytes')
        self.file_count = Gauge('timeseries_file_count', 'Number of time-series files')
        self.record_count = Gauge('timeseries_record_count', 'Total number of records')
        self.file_errors = Counter('timeseries_file_errors_total', 'Number of file errors detected')
        self.monitoring_duration = Histogram('timeseries_monitoring_duration_seconds', 'Time spent monitoring')
        self.disk_usage_percent = Gauge('timeseries_disk_usage_percent', 'Disk usage percentage')
        self.compression_ratio = Gauge('timeseries_compression_ratio', 'Average compression ratio')
        self.files_by_shard = Gauge('timeseries_files_by_shard', 'Files per shard', ['shard'])
        
    async def start_monitoring(self):
        """Start the monitoring loop"""
        self.logger.info("🔍 Starting time-series storage monitoring")
        
        # Start Prometheus server if available
        if PROMETHEUS_AVAILABLE and os.getenv("ENABLE_PROMETHEUS", "false").lower() == "true":
            start_http_server(8000)
            self.logger.info("📊 Prometheus metrics server started on port 8000")
        
        # Main monitoring loop
        while True:
            try:
                start_time = time.time()
                await self.check_storage_health()
                duration = time.time() - start_time
                
                if PROMETHEUS_AVAILABLE:
                    self.monitoring_duration.observe(duration)
                
                self.logger.info(f"✅ Storage health check completed in {duration:.2f}s")
                await asyncio.sleep(self.monitoring_interval)
                
            except Exception as e:
                self.logger.error(f"❌ Monitoring error: {e}")
                await asyncio.sleep(10)  # Short retry interval on error
    
    async def check_storage_health(self):
        """Perform comprehensive storage health check"""
        # Reset stats
        self.stats['files_monitored'] = 0
        self.stats['total_size_bytes'] = 0
        self.stats['total_records'] = 0
        self.stats['files_with_errors'] = 0
        self.stats['last_check_time'] = datetime.now()
        
        # Check if storage directory exists
        if not self.storage_path.exists():
            self.logger.warning(f"⚠️  Storage path does not exist: {self.storage_path}")
            self.storage_path.mkdir(parents=True, exist_ok=True)
            return
        
        # Check disk usage
        await self.check_disk_usage()
        
        # Scan storage structure
        await self.scan_storage_structure()
        
        # Update metrics
        if PROMETHEUS_AVAILABLE:
            self.update_prometheus_metrics()
        
        # Log summary
        self.log_health_summary()
    
    async def check_disk_usage(self):
        """Check disk usage for storage volume"""
        try:
            usage = psutil.disk_usage(str(self.storage_path))
            usage_percent = (usage.used / usage.total) * 100
            
            if PROMETHEUS_AVAILABLE:
                self.disk_usage_percent.set(usage_percent)
            
            if usage_percent > 90:
                self.logger.error(f"🚨 HIGH DISK USAGE: {usage_percent:.1f}% used")
            elif usage_percent > 80:
                self.logger.warning(f"⚠️  Disk usage: {usage_percent:.1f}%")
            else:
                self.logger.debug(f"💾 Disk usage: {usage_percent:.1f}%")
                
        except Exception as e:
            self.logger.error(f"❌ Disk usage check failed: {e}")
    
    async def scan_storage_structure(self):
        """Scan the storage directory structure and validate files"""
        shard_stats = {}
        compression_ratios = []
        
        try:
            # Walk through year directories
            for year_dir in self.storage_path.iterdir():
                if not year_dir.is_dir() or not year_dir.name.isdigit():
                    continue
                    
                # Walk through month directories
                for month_dir in year_dir.iterdir():
                    if not month_dir.is_dir() or not month_dir.name.isdigit():
                        continue
                    
                    # Walk through shard directories
                    for shard_dir in month_dir.iterdir():
                        if not shard_dir.is_dir() or not shard_dir.name.isdigit():
                            continue
                        
                        shard_num = shard_dir.name
                        if shard_num not in shard_stats:
                            shard_stats[shard_num] = 0
                        
                        # Check files in shard
                        await self.scan_shard_directory(shard_dir, compression_ratios)
                        shard_stats[shard_num] += 1
        
        except Exception as e:
            self.logger.error(f"❌ Storage scan failed: {e}")
        
        # Update shard metrics
        if PROMETHEUS_AVAILABLE:
            for shard, count in shard_stats.items():
                self.files_by_shard.labels(shard=shard).set(count)
            
            if compression_ratios:
                avg_compression = sum(compression_ratios) / len(compression_ratios)
                self.compression_ratio.set(avg_compression)
    
    async def scan_shard_directory(self, shard_dir: Path, compression_ratios: List[float]):
        """Scan files in a shard directory"""
        try:
            for file_path in shard_dir.iterdir():
                if not file_path.is_file() or not file_path.name.endswith('.record.gz'):
                    continue
                
                await self.validate_file(file_path, compression_ratios)
                
        except Exception as e:
            self.logger.error(f"❌ Shard scan failed for {shard_dir}: {e}")
    
    async def validate_file(self, file_path: Path, compression_ratios: List[float]):
        """Validate a single time-series file"""
        try:
            self.stats['files_monitored'] += 1
            
            # Get file size
            file_size = file_path.stat().st_size
            self.stats['total_size_bytes'] += file_size
            
            # Basic file validation - check if it can be read
            async with aiofiles.open(file_path, 'rb') as f:
                # Read metadata header (48 bytes)
                metadata_bytes = await f.read(48)
                if len(metadata_bytes) != 48:
                    self.logger.warning(f"⚠️  Invalid metadata size in {file_path.name}")
                    self.stats['files_with_errors'] += 1
                    if PROMETHEUS_AVAILABLE:
                        self.file_errors.inc()
                    return
                
                # Parse metadata (simplified)
                import struct
                try:
                    instrument_id, year, month, record_count = struct.unpack('<4I', metadata_bytes[:16])
                    self.stats['total_records'] += record_count
                    
                    # Estimate compression ratio if we have the data
                    if record_count > 0:
                        uncompressed_size = record_count * 32 + 48  # 32 bytes per record + metadata
                        if uncompressed_size > 0:
                            compression_ratio = file_size / uncompressed_size
                            compression_ratios.append(compression_ratio)
                    
                except struct.error as e:
                    self.logger.warning(f"⚠️  Metadata parsing error in {file_path.name}: {e}")
                    self.stats['files_with_errors'] += 1
                    if PROMETHEUS_AVAILABLE:
                        self.file_errors.inc()
                
        except Exception as e:
            self.logger.error(f"❌ File validation failed for {file_path.name}: {e}")
            self.stats['files_with_errors'] += 1
            if PROMETHEUS_AVAILABLE:
                self.file_errors.inc()
    
    def update_prometheus_metrics(self):
        """Update all Prometheus metrics"""
        if not PROMETHEUS_AVAILABLE:
            return
        
        self.storage_size_bytes.set(self.stats['total_size_bytes'])
        self.file_count.set(self.stats['files_monitored'])
        self.record_count.set(self.stats['total_records'])
    
    def log_health_summary(self):
        """Log a summary of storage health"""
        uptime = datetime.now() - self.stats['uptime_start']
        
        summary = {
            'files_monitored': self.stats['files_monitored'],
            'total_size_gb': round(self.stats['total_size_bytes'] / (1024**3), 2),
            'total_records': self.stats['total_records'],
            'files_with_errors': self.stats['files_with_errors'],
            'error_rate_percent': round((self.stats['files_with_errors'] / max(self.stats['files_monitored'], 1)) * 100, 2),
            'uptime_hours': round(uptime.total_seconds() / 3600, 1),
            'last_check': self.stats['last_check_time'].isoformat() if self.stats['last_check_time'] else None
        }
        
        self.logger.info(f"📊 Storage Health Summary: {json.dumps(summary, indent=2)}")
        
        # Alert on errors
        if self.stats['files_with_errors'] > 0:
            self.logger.warning(f"⚠️  {self.stats['files_with_errors']} files have errors!")

async def health_check_endpoint():
    """Simple health check for readiness/liveness probes"""
    from http.server import HTTPServer, BaseHTTPRequestHandler
    import threading
    
    class HealthHandler(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path == '/health':
                self.send_response(200)
                self.send_header('Content-type', 'application/json')
                self.end_headers()
                self.wfile.write(b'{"status": "healthy", "timestamp": "' + 
                               datetime.now().isoformat().encode() + b'"}')
            elif self.path == '/metrics' and not PROMETHEUS_AVAILABLE:
                self.send_response(200)
                self.send_header('Content-type', 'text/plain')
                self.end_headers()
                self.wfile.write(b'# Prometheus not available\n')
            else:
                self.send_response(404)
                self.end_headers()
        
        def log_message(self, format, *args):
            pass  # Suppress default logging
    
    def run_server():
        server = HTTPServer(('', 8000), HealthHandler)
        server.serve_forever()
    
    if not PROMETHEUS_AVAILABLE:
        thread = threading.Thread(target=run_server, daemon=True)
        thread.start()

async def main():
    """Main monitoring function"""
    # Setup logging
    log_level = os.getenv("LOG_LEVEL", "INFO").upper()
    logging.basicConfig(
        level=getattr(logging, log_level),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    logger = logging.getLogger(__name__)
    logger.info("🚀 Starting Time-Series Storage Monitor")
    
    # Start health check endpoint if Prometheus not available
    await health_check_endpoint()
    
    # Create and start monitor
    storage_path = os.getenv("STORAGE_BASE_PATH", "/data/monthly/interval")
    monitor = StorageMonitor(storage_path)
    
    try:
        await monitor.start_monitoring()
    except KeyboardInterrupt:
        logger.info("🛑 Monitor stopped by user")
    except Exception as e:
        logger.error(f"❌ Monitor failed: {e}")
        raise

if __name__ == "__main__":
    asyncio.run(main())