# File-Based Time-Series Storage Architecture Guide

## 🎯 Overview

This guide covers the complete migration from database storage to an optimized file-based storage system for massive-scale time-series data (29.5+ billion minute records). The new architecture provides:

- **10x storage cost reduction** (object storage vs database)
- **Predictable performance** (file I/O vs complex DB joins)  
- **Horizontal scaling** (distribute files across nodes)
- **Simple backup/recovery** (file copy vs database dumps)
- **Parallel processing** (multiple files simultaneously)

## 📁 Architecture Overview

### Directory Structure
```
/data/monthly/interval/
├── 2024/
│   ├── 01/
│   │   ├── 00/          # Shard 0 (instrument_id % 100 == 0)
│   │   │   ├── 100_2024_01.record.gz
│   │   │   ├── 200_2024_01.record.gz
│   │   │   └── ...
│   │   ├── 01/          # Shard 1 (instrument_id % 100 == 1)
│   │   │   ├── 101_2024_01.record.gz
│   │   │   └── ...
│   │   └── ...
│   ├── 02/
│   └── ...
├── 2025/
└── ...
```

### File Format Specification
Each `.record.gz` file contains:
- **Header (64 bytes)**: File metadata (instrument_id, year, month, record_count, timestamps, version)
- **Records (32 bytes each)**: Binary OHLCV minute data with timestamps
- **Compression**: gzip level 6 (typically 60-70% size reduction)

### Binary Record Format
```
Timestamp    | Open  | High  | Low   | Close | Volume
8 bytes      | 4 B   | 4 B   | 4 B   | 4 B   | 8 bytes
(uint64)     | (f32) | (f32) | (f32) | (f32) | (uint64)
```

## 🚀 Quick Start

### 1. Basic Usage

```python
from storage.time_series_file_manager import TimeSeriesFileManager, MinuteRecord
from storage.dual_write_manager import DualWriteTimeSeriesManager, DualWriteConfig
from datetime import datetime

# Initialize file manager
file_manager = TimeSeriesFileManager("/data/monthly/interval")

# Write minute data for January 2024
instrument_id = 12345
records = [
    MinuteRecord(
        timestamp=datetime(2024, 1, 1, 9, 30),
        open_price=100.0,
        high_price=101.0,
        low_price=99.0,
        close_price=100.5,
        volume=1000
    ),
    # ... more records
]

# Write monthly file
success = await file_manager.write_monthly_file(instrument_id, 2024, 1, records)

# Read monthly file
read_records = await file_manager.read_monthly_file(instrument_id, 2024, 1)
```

### 2. Query Engine Usage

```python
from storage.time_series_file_manager import TimeSeriesQueryEngine

query_engine = TimeSeriesQueryEngine(file_manager)

# Query multiple instruments across date range
instruments = [12345, 12346, 12347]
start_time = datetime(2024, 1, 1)
end_time = datetime(2024, 1, 31, 23, 59, 59)

data = await query_engine.query_range(instruments, start_time, end_time)

# Get daily OHLC aggregation
daily_ohlc = await query_engine.get_daily_ohlc(12345, date(2024, 1, 1), date(2024, 1, 31))
```

## 🔄 Migration Process

### Phase 1: Database-to-File Migration

```bash
# Analyze existing data
python scripts/migration/database_to_file_migration.py --analyze-only

# Run migration for specific date range
python scripts/migration/database_to_file_migration.py \
    --start-date 2024-01-01 \
    --end-date 2024-12-31 \
    --output-path /data/monthly/interval \
    --batch-size 10000 \
    --max-concurrent 20

# Resume from checkpoint
python scripts/migration/database_to_file_migration.py \
    --resume \
    --checkpoint-file /tmp/migration_checkpoint.json
```

### Phase 2: Dual-Write Transition

```python
from storage.dual_write_manager import (
    DualWriteTimeSeriesManager, 
    DualWriteConfig, 
    WriteMode, 
    ReadMode
)

# Configure dual-write system
config = DualWriteConfig(
    write_mode=WriteMode.DUAL_WRITE,           # Write to both DB and files
    read_mode=ReadMode.FILES_WITH_DB_FALLBACK, # Read from files, fallback to DB
    fail_on_file_error=True,                   # Prioritize file writes
    fail_on_db_error=False                     # Allow DB failures
)

manager = DualWriteTimeSeriesManager(config)

# Write data (goes to both systems)
result = await manager.write_minute_data(instrument_id, records, 'fmp')

# Read data (files first, DB fallback)
data = await manager.read_minute_data([instrument_id], start_time, end_time)
```

### Phase 3: Validation

```bash
# Comprehensive validation
python scripts/validation/data_integrity_validator.py \
    --full-validation \
    --start-date 2024-01-01 \
    --end-date 2024-12-31 \
    --sample-size 1000

# Performance comparison
python scripts/validation/data_integrity_validator.py \
    --performance-test \
    --instruments 50

# Specific date range validation
python scripts/validation/data_integrity_validator.py \
    --start-date 2024-06-01 \
    --end-date 2024-06-30
```

### Phase 4: Production Cutover

```python
# Final configuration - files only
config = DualWriteConfig(
    write_mode=WriteMode.FILES_ONLY,
    read_mode=ReadMode.FILES_ONLY
)
```

## 📊 Performance Characteristics

### Storage Efficiency
- **Raw data**: ~32 bytes per minute record
- **Compressed**: ~10-12 bytes per minute record (60-70% compression)
- **Metadata overhead**: 64 bytes per monthly file
- **Expected size**: 5-10TB for 29.5B records (vs 14TB in database)

### Query Performance
- **Single instrument, single month**: ~1-5ms
- **Single instrument, full year**: ~50-100ms
- **100 instruments, single month**: ~100-500ms
- **Aggregation queries**: 2-10x faster than database equivalent

### Scalability Limits
- **Files per directory**: ~1,000 (managed by sharding)
- **Records per file**: ~50,000 (monthly trading minutes)
- **Concurrent readers**: Limited by filesystem and storage throughput
- **Concurrent writers**: Semaphore-controlled (default: 20)

## 🛠️ Configuration Options

### TimeSeriesFileManager Configuration

```python
manager = TimeSeriesFileManager(
    base_path="/data/monthly/interval",
    compression_level=6,    # gzip compression (1-9)
    metadata_size=64,       # bytes
    record_size=32          # bytes
)
```

### Dual-Write Configuration Modes

```python
# Migration phases
WriteMode.DATABASE_ONLY              # Phase 0: Current state
WriteMode.DUAL_WRITE                 # Phase 1: Transition
WriteMode.DUAL_WRITE_FILES_PRIMARY   # Phase 2: Files priority
WriteMode.FILES_ONLY                 # Phase 3: Final state

ReadMode.DATABASE_ONLY               # Read from DB only
ReadMode.FILES_WITH_DB_FALLBACK      # Try files first
ReadMode.DATABASE_WITH_FILES_FALLBACK # Try DB first  
ReadMode.FILES_ONLY                  # Files only
```

## 🔧 Operations Guide

### Monitoring Storage Health

```python
# Get storage statistics
stats = await file_manager.get_storage_stats()
print(f"Total files: {stats['total_files']:,}")
print(f"Total size: {stats['total_size_bytes'] / (1024**3):.2f} GB")
print(f"Compression ratio: {stats['compression_ratio']:.1%}")
print(f"Years covered: {stats['years_covered']}")
print(f"Instruments: {stats['instruments_count']}")
```

### Backup and Recovery

```bash
# Backup files (incremental)
rsync -av --progress /data/monthly/interval/ /backup/monthly/interval/

# Restore specific month
rsync -av /backup/monthly/interval/2024/01/ /data/monthly/interval/2024/01/

# Verify integrity after restore
python scripts/validation/data_integrity_validator.py --start-date 2024-01-01 --end-date 2024-01-31
```

### Performance Tuning

```python
# Increase concurrent file operations
config = DualWriteConfig(max_concurrent_files=50)

# Batch size optimization
config.batch_size = 5000  # Smaller batches for memory efficiency

# Timeout configuration
config.write_timeout = 60.0  # Seconds
```

## 🚨 Troubleshooting

### Common Issues

#### 1. File Write Failures
```python
# Check disk space
import shutil
free_space = shutil.disk_usage("/data/monthly/interval").free
print(f"Free space: {free_space / (1024**3):.2f} GB")

# Check permissions
path = Path("/data/monthly/interval")
print(f"Writable: {path.exists() and os.access(path, os.W_OK)}")
```

#### 2. Validation Errors
```bash
# Check specific instrument
python scripts/validation/data_integrity_validator.py --instruments 1

# Analyze failed files
grep "❌" data_validation.log | head -10
```

#### 3. Performance Issues
```python
# Profile query performance
import time
start_time = time.time()
data = await query_engine.query_range([instrument_id], start_time, end_time)
elapsed = time.time() - start_time
print(f"Query took {elapsed:.2f}s for {sum(len(records) for records in data.values())} records")
```

### Recovery Procedures

#### Corrupted File Recovery
```python
# Identify corrupted files
try:
    records = await file_manager.read_monthly_file(instrument_id, year, month)
except Exception as e:
    print(f"Corrupted file: {instrument_id}_{year}_{month:02d}.record.gz")
    
    # Re-migrate from database
    # python scripts/migration/database_to_file_migration.py --start-date {year}-{month:02d}-01 --end-date {year}-{month:02d}-31
```

#### Missing Files Recovery
```bash
# Find missing months
python -c "
import asyncio
from storage.time_series_file_manager import TimeSeriesFileManager

async def check_coverage():
    manager = TimeSeriesFileManager('/data/monthly/interval')
    # Check specific instrument coverage
    available = await manager.list_available_data(12345)
    print(f'Available months: {available}')

asyncio.run(check_coverage())
"
```

## 📈 Scaling Considerations

### Horizontal Scaling
- **Shard distribution**: Use `instrument_id % 100` for load balancing
- **Storage nodes**: Distribute shards across multiple storage systems
- **Read replicas**: Create read-only copies for query load distribution

### Vertical Scaling
- **Storage type**: NVMe SSD for best performance, HDD for cost efficiency
- **Memory**: 16-32GB RAM for efficient file caching
- **CPU**: Multi-core for parallel file processing

### Cloud Deployment
```yaml
# Kubernetes persistent volume
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: timeseries-storage
spec:
  accessModes:
    - ReadWriteMany  # Multiple pods can read/write
  resources:
    requests:
      storage: 20Ti  # 20TB for full dataset
  storageClassName: fast-ssd
```

## 🔐 Security Considerations

### File Permissions
```bash
# Secure file permissions
chmod -R 640 /data/monthly/interval/
chown -R app:app /data/monthly/interval/
```

### Data Encryption
```python
# Enable encryption at rest (filesystem level)
# Use LUKS, dm-crypt, or cloud provider encryption

# For application-level encryption, modify MinuteRecord.to_binary()
def to_binary_encrypted(self, key):
    raw_data = self.to_binary()
    from cryptography.fernet import Fernet
    f = Fernet(key)
    return f.encrypt(raw_data)
```

## 📚 API Reference

### TimeSeriesFileManager

```python
class TimeSeriesFileManager:
    async def write_monthly_file(instrument_id: int, year: int, month: int, records: List[MinuteRecord]) -> bool
    async def read_monthly_file(instrument_id: int, year: int, month: int, start_time: datetime = None, end_time: datetime = None) -> List[MinuteRecord]
    async def get_file_metadata(instrument_id: int, year: int, month: int) -> Optional[FileMetadata]
    async def list_available_data(instrument_id: int, start_year: int = 2005, end_year: int = 2025) -> List[Tuple[int, int]]
    async def get_storage_stats() -> Dict[str, any]
```

### TimeSeriesQueryEngine

```python
class TimeSeriesQueryEngine:
    async def query_range(instrument_ids: List[int], start_time: datetime, end_time: datetime) -> Dict[int, List[MinuteRecord]]
    async def get_daily_ohlc(instrument_id: int, start_date: date, end_date: date) -> List[Dict]
```

### DualWriteTimeSeriesManager

```python
class DualWriteTimeSeriesManager:
    async def write_minute_data(instrument_id: int, records: List[MinuteRecord], vendor: str = 'fmp') -> WriteResult
    async def read_minute_data(instrument_ids: List[int], start_time: datetime, end_time: datetime) -> Dict[int, List[MinuteRecord]]
    def get_metrics_summary() -> Dict[str, Any]
```

## 🎯 Migration Checklist

### Pre-Migration
- [ ] **Analyze source data** - Run `database_to_file_migration.py --analyze-only`
- [ ] **Verify storage capacity** - Ensure sufficient disk space (5-10TB)
- [ ] **Test file permissions** - Verify write access to target directory
- [ ] **Backup database** - Create full backup of existing data
- [ ] **Configure logging** - Set up monitoring and alerting

### Migration Phase
- [ ] **Start dual-write mode** - Begin writing to both systems
- [ ] **Monitor write performance** - Check dual-write metrics
- [ ] **Run incremental migration** - Migrate historical data in chunks
- [ ] **Validate data integrity** - Run comprehensive validation suite
- [ ] **Test query performance** - Benchmark file-based queries

### Post-Migration
- [ ] **Switch to files-primary** - Configure files as primary read source
- [ ] **Validate production workloads** - Test with real application traffic
- [ ] **Monitor performance metrics** - Track query latency and throughput
- [ ] **Clean up database** - Remove redundant database tables
- [ ] **Update backup procedures** - Implement file-based backup strategy

## 💡 Best Practices

1. **Always validate after migration** - Run integrity checks after each migration batch
2. **Use checkpoints for resumability** - Enable checkpoint saves for large migrations
3. **Monitor storage growth** - Track file size and compression ratios
4. **Test disaster recovery** - Regularly test backup and restore procedures
5. **Optimize for access patterns** - Align file organization with query patterns
6. **Use appropriate compression** - Balance CPU cost vs storage savings
7. **Implement proper error handling** - Handle corrupted files gracefully
8. **Monitor system resources** - Track disk I/O, memory, and CPU usage

## 📞 Support and Troubleshooting

For issues and support:
1. **Check logs** - Review application and migration logs
2. **Run validation** - Use data integrity validator to identify issues
3. **Monitor metrics** - Check dual-write performance statistics
4. **Verify configuration** - Ensure proper setup of write/read modes
5. **Test recovery** - Validate backup and recovery procedures

The file-based architecture provides a robust, scalable foundation for massive time-series workloads while significantly reducing operational costs and complexity.