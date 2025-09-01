# Design Requirements Document (DRD)
# ATS-INTG Daily 1-Minute Bar Backfill System

**Document Version:** 1.0  
**Last Updated:** 2025-01-18  
**Owner:** Platform Engineering Team  
**Status:** Active  
**Related PRD:** PRD_Daily_Minute_Bars_Backfill.md

---

## 1. Design Overview

### 1.1 System Architecture
The ATS-INTG Daily 1-Minute Bar Backfill System implements a distributed, container-based architecture that processes market data through a multi-stage pipeline optimized for high throughput, reliability, and monitoring.

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   FirstRate     │    │  ATS-INTG        │    │   File System   │
│   API Service   │───▶│  Backfill        │───▶│   Organized     │
│                 │    │  Container       │    │   Parquet Files │
└─────────────────┘    └──────────────────┘    └─────────────────┘
         │                       │                       │
         │              ┌─────────────────┐              │
         │              │  Prometheus     │              │
         └──────────────│  Metrics        │──────────────┘
                        │  Server         │
                        └─────────────────┘
                                 │
                        ┌─────────────────┐
                        │  Slack          │
                        │  Notifications  │
                        └─────────────────┘
```

### 1.2 Core Components

#### 1.2.1 Data Processing Engine
- **Primary Service**: `daily_minute_bars_backfill.py`
- **Execution Model**: Async/await with batch processing
- **Error Handling**: Comprehensive retry logic with exponential backoff
- **Memory Management**: Streaming processing with configurable batch sizes

#### 1.2.2 File Organization System
- **Structure Setup**: `setup_daily_minute_bars_structure.py`
- **Hierarchy**: 4-level directory structure (year/month/day/letter)
- **File Format**: Parquet with gzip compression
- **Naming Convention**: `{symbol}_{YYYYMMDD}.parquet`

#### 1.2.3 Monitoring & Alerting
- **Metrics Collection**: Enhanced Prometheus metrics server
- **Notification Service**: Slack integration with rich formatting
- **Health Monitoring**: Multi-level health checks and recovery

#### 1.2.4 Container Orchestration
- **Scheduler Container**: Cron-based job execution
- **Metrics Container**: Prometheus HTTP server
- **Notification Container**: Slack notification service

---

## 2. Detailed Component Design

### 2.1 Daily Minute Bars Backfill Engine

#### 2.1.1 Class Structure
```python
class DailyMinuteBarBackfill:
    """Main processing engine for 1-minute bar backfill operations."""
    
    # Core Components
    - firstrate_adapter: FirstRateAdapter      # API integration
    - db_pool: asyncpg.Pool                   # Database connection pool
    - stats: Dict                             # Processing statistics
    
    # Configuration
    - base_data_path: Path                    # Data storage root
    - daily_output_path: Path                 # Organized file output
    - lookback_days: int                      # Processing window
    - critical_etfs: Set[str]                 # Priority instruments
    
    # External Integrations
    - slack_webhook: str                      # Notification endpoint
    - prometheus_gateway: str                 # Metrics endpoint
```

#### 2.1.2 Processing Workflow
```python
async def run_daily_backfill():
    """Main processing workflow with error handling and monitoring."""
    
    1. Initialize system components
    2. Get processing dates (last 7 days, exclude weekends/holidays)
    3. Retrieve active instruments from database with classification
    4. Apply filters (instrument types, test limits, etc.)
    5. Process instruments in batches of 50 concurrently
    6. For each instrument:
       a. Download 1-minute data from FirstRate API
       b. Validate data quality (>10 bars, logical OHLCV)
       c. Convert to DataFrame with proper indexing
       d. Save to organized Parquet file structure
       e. Update processing statistics
    7. Send metrics to Prometheus
    8. Send summary to Slack
    9. Log final statistics and cleanup
```

#### 2.1.3 Error Handling Strategy
```python
# Multi-level error handling with specific recovery strategies
try:
    # API-level errors
    minute_data = await self.firstrate.get_minute_bars(symbol, date)
except RateLimitError:
    await asyncio.sleep(exponential_backoff_delay)
    retry_with_backoff()
except APITimeoutError:
    log_error_and_continue()
except DataQualityError:
    mark_symbol_for_manual_review()
except FileSystemError:
    retry_with_different_path()
```

### 2.2 File Organization System

#### 2.2.1 Directory Structure Design
```
/mnt/d/ats-data/firstrate-data/daily/
├── 2025/
│   ├── 01/
│   │   ├── 18/
│   │   │   ├── A/
│   │   │   │   ├── AAPL_20250118.parquet
│   │   │   │   ├── AMZN_20250118.parquet
│   │   │   │   └── ...
│   │   │   ├── B/
│   │   │   ├── C/
│   │   │   └── ... (through Z)
│   │   ├── 19/
│   │   └── ...
│   ├── 02/
│   └── ...
└── 2024/
    └── ...
```

#### 2.2.2 File Path Generation Algorithm
```python
def get_output_path(symbol: str, processing_date: date) -> Path:
    """Generate organized file path for symbol and date."""
    
    # Extract components
    first_letter = symbol[0].upper()
    date_str = processing_date.strftime('%Y%m%d')
    
    # Build hierarchical path
    output_dir = (
        self.daily_output_path / 
        processing_date.strftime('%Y') /    # Year
        processing_date.strftime('%m') /    # Month (zero-padded)
        processing_date.strftime('%d') /    # Day (zero-padded)
        first_letter                        # First letter grouping
    )
    
    # Ensure directory exists
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Return full file path
    return output_dir / f"{symbol}_{date_str}.parquet"
```

#### 2.2.3 Directory Structure Setup
```python
class DailyMinuteBarsStructureSetup:
    """Creates and validates directory structure for minute bar files."""
    
    def create_year_structure(year: int) -> bool:
        """Create complete directory structure for a given year."""
        
        # Create year/month/day hierarchy
        for month in range(1, 13):
            for day in range(1, calendar.monthrange(year, month)[1] + 1):
                day_path = base_path / str(year) / f"{month:02d}" / f"{day:02d}"
                
                # Create A-Z letter directories
                for letter in 'ABCDEFGHIJKLMNOPQRSTUVWXYZ':
                    letter_path = day_path / letter
                    letter_path.mkdir(parents=True, exist_ok=True)
```

### 2.3 Prometheus Metrics Integration

#### 2.3.1 Enhanced Metrics Server Design
```python
class PrometheusMetricsServer:
    """Enhanced metrics server with minute bars support."""
    
    # Core Metrics
    - ats_daily_minute_backfill_instruments_processed
    - ats_daily_minute_backfill_total_minute_bars  
    - ats_daily_minute_backfill_files_created
    - ats_daily_minute_backfill_files_updated
    
    # Classification Metrics
    - ats_daily_minute_backfill_symbols_by_type{type="stock|critical_etf|other_etf"}
    - ats_daily_minute_backfill_bars_by_type{type="stock|critical_etf|other_etf"}
    - ats_daily_minute_backfill_symbols_by_letter{letter="A-Z"}
    
    # Performance Metrics
    - ats_daily_minute_backfill_processing_errors
    - ats_daily_minute_backfill_total_data_size_mb
```

#### 2.3.2 Metrics Collection Strategy
```python
async def send_prometheus_metrics():
    """Send comprehensive metrics to Prometheus pushgateway."""
    
    metrics = []
    timestamp = int(datetime.now().timestamp())
    
    # Total processing metrics
    metrics.extend([
        f"ats_daily_minute_backfill_instruments_processed {self.stats['instruments_processed']} {timestamp}",
        f"ats_daily_minute_backfill_total_minute_bars {self.stats['total_minute_bars']} {timestamp}",
    ])
    
    # Classification-based metrics
    for inst_type, count in self.stats['instrument_types'].items():
        metrics.append(f'ats_daily_minute_backfill_symbols_by_type{{type="{inst_type}"}} {count} {timestamp}')
    
    # Send to pushgateway
    async with aiohttp.ClientSession() as session:
        await session.post(
            f"{self.prometheus_gateway}/metrics/job/ats_daily_minute_backfill",
            data='\n'.join(metrics) + '\n'
        )
```

### 2.4 Slack Notification System

#### 2.4.1 Notification Service Design
```python
class SlackMinuteBarsNotifier:
    """Comprehensive Slack notification service."""
    
    def create_daily_summary_message() -> Dict:
        """Create rich daily summary with interactive elements."""
        
        return {
            "blocks": [
                {"type": "header", "text": "Daily 1-Minute Bar Summary"},
                {"type": "section", "fields": [
                    {"type": "mrkdwn", "text": f"*Files:* {files_processed:,}"},
                    {"type": "mrkdwn", "text": f"*Symbols:* {unique_symbols:,}"}
                ]},
                {"type": "actions", "elements": [
                    {"type": "button", "text": "View Metrics", "url": prometheus_url},
                    {"type": "button", "text": "Manual Backfill"}
                ]}
            ]
        }
```

#### 2.4.2 Message Templates
```json
{
  "daily_summary": {
    "frequency": "8:00 AM EST daily",
    "content": [
      "files_processed_today",
      "unique_symbols_count",
      "storage_usage",
      "processing_errors",
      "trend_indicators"
    ]
  },
  "weekly_summary": {
    "frequency": "Monday 9:00 AM EST", 
    "content": [
      "comprehensive_statistics",
      "daily_breakdown",
      "storage_trends",
      "instrument_type_analysis",
      "performance_metrics"
    ]
  }
}
```

### 2.5 Container Orchestration Design

#### 2.5.1 Docker Compose Architecture
```yaml
# docker-compose.minute-bars-jobs.yml
services:
  ats-intg-minute-bars-scheduler:
    # Primary processing container with cron scheduling
    command: |
      - Setup directory structure
      - Install and configure cron
      - Create processing jobs at 4:00 AM EST
      - Create priority jobs at 4:30 AM EST
      - Create weekend catch-up jobs
      - Monitor system health
  
  ats-intg-prometheus-metrics:
    # Metrics collection and HTTP endpoint
    ports: ["4080:8080"]
    command: python3 scripts/prometheus_metrics_server.py
    
  ats-intg-slack-notifier:
    # Notification service with cron scheduling
    command: |
      - Setup Slack notification cron jobs
      - Daily summary at 8:00 AM EST
      - Weekly summary on Mondays at 9:00 AM EST
```

#### 2.5.2 Container Dependencies
```yaml
dependency_graph:
  ats-intg-postgres: # Database (external)
    required_by: [scheduler, metrics, notifier]
    health_check: "pg_isready"
    
  ats-intg-minute-bars-scheduler: # Main processor
    depends_on: [ats-intg-postgres]
    health_check: "pgrep cron && API connectivity"
    
  ats-intg-prometheus-metrics: # Metrics server
    depends_on: [ats-intg-postgres]
    health_check: "curl -f localhost:8080/health"
    
  ats-intg-slack-notifier: # Notifications
    depends_on: [ats-intg-postgres, ats-intg-prometheus-metrics]
    health_check: "pgrep cron"
```

---

## 3. Data Flow Design

### 3.1 Processing Pipeline Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    Data Processing Pipeline                      │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌─────────────────┐    ┌─────────────────┐    ┌──────────────┐ │
│  │   Database      │    │   FirstRate     │    │   Output     │ │
│  │   Query         │───▶│   API Calls     │───▶│   Parquet    │ │
│  │                 │    │                 │    │   Files      │ │
│  └─────────────────┘    └─────────────────┘    └──────────────┘ │
│           │                       │                     │       │
│           ▼                       ▼                     ▼       │
│  ┌─────────────────┐    ┌─────────────────┐    ┌──────────────┐ │
│  │   Instrument    │    │   Data          │    │   File       │ │
│  │   Classification│    │   Validation    │    │   Organization│ │
│  │   & Filtering   │    │   & Quality     │    │   & Indexing │ │
│  └─────────────────┘    └─────────────────┘    └──────────────┘ │
│                                                                 │
├─────────────────────────────────────────────────────────────────┤
│                      Monitoring & Alerting                     │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌─────────────────┐    ┌─────────────────┐    ┌──────────────┐ │
│  │   Statistics    │    │   Prometheus    │    │   Slack      │ │
│  │   Collection    │───▶│   Metrics       │───▶│   Notifications│ │
│  │                 │    │   Export        │    │              │ │
│  └─────────────────┘    └─────────────────┘    └──────────────┘ │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### 3.2 Data Transformation Stages

#### Stage 1: Instrument Discovery & Classification
```sql
-- Database query for active instruments with classification
SELECT DISTINCT 
    symbol,
    CASE 
        WHEN symbol IN ('SPY', 'QQQ', 'VTI', ...) THEN 'critical_etf'
        WHEN symbol LIKE '%--%' OR symbol LIKE '%-%' THEN 'other_etf'
        ELSE 'stock'
    END as instrument_type,
    COALESCE(exchange, 'UNKNOWN') as exchange
FROM intg_instruments 
WHERE active = true 
ORDER BY instrument_type, symbol
```

#### Stage 2: FirstRate API Data Retrieval
```python
# API call structure for 1-minute bars
async def get_minute_bars(symbol: str, date: date) -> List[Tick]:
    start_time = datetime.combine(date, datetime.min.time())
    end_time = start_time + timedelta(hours=23, minutes=59, seconds=59)
    
    # Call FirstRate API with rate limiting
    async with rate_limiter:
        response = await firstrate_client.get_minute_data(
            symbol=symbol,
            start=start_time,
            end=end_time,
            timeframe='1min'
        )
    
    return [Tick(timestamp=t, open=o, high=h, low=l, close=c, volume=v) 
            for t, o, h, l, c, v in response.data]
```

#### Stage 3: Data Validation & Quality Checks
```python
def validate_minute_data(df: pd.DataFrame) -> ValidationResult:
    """Comprehensive data quality validation."""
    
    checks = []
    
    # Minimum data requirement
    if len(df) < 10:
        checks.append("Insufficient data: <10 minute bars")
    
    # OHLC logical consistency
    invalid_ohlc = df[(df['high'] < df['low']) | 
                      (df['close'] > df['high']) | 
                      (df['close'] < df['low'])]
    if not invalid_ohlc.empty:
        checks.append(f"Invalid OHLC data: {len(invalid_ohlc)} bars")
    
    # Volume validation
    if df['volume'].isna().sum() > len(df) * 0.1:
        checks.append("Excessive missing volume data")
    
    return ValidationResult(
        is_valid=(len(checks) == 0),
        errors=checks,
        quality_score=calculate_quality_score(df)
    )
```

#### Stage 4: Parquet File Generation
```python
def save_minute_data(df: pd.DataFrame, output_path: Path) -> None:
    """Save minute bar data to optimized Parquet format."""
    
    # Ensure proper timestamp indexing
    df = df.set_index('timestamp').sort_index()
    
    # Optimize data types for storage efficiency
    df['open'] = df['open'].astype('float32')
    df['high'] = df['high'].astype('float32')
    df['low'] = df['low'].astype('float32')
    df['close'] = df['close'].astype('float32')
    df['volume'] = df['volume'].astype('int64')
    
    # Save with compression and metadata
    df.to_parquet(
        output_path,
        compression='gzip',
        index=True,
        metadata={'source': 'FirstRate', 'processed_at': datetime.now().isoformat()}
    )
```

### 3.3 Batch Processing Design

#### 3.3.1 Concurrency Model
```python
async def process_instrument_batch(batch: List[Tuple[str, str, str]]) -> List[Dict]:
    """Process batch of instruments concurrently with controlled parallelism."""
    
    batch_tasks = []
    semaphore = asyncio.Semaphore(10)  # Limit concurrent API calls
    
    for symbol, instrument_type, exchange in batch:
        async def process_with_semaphore():
            async with semaphore:
                return await process_symbol(symbol, instrument_type, processing_dates)
        
        batch_tasks.append(process_with_semaphore())
    
    # Execute batch concurrently
    return await asyncio.gather(*batch_tasks, return_exceptions=True)
```

#### 3.3.2 Memory Management Strategy
```python
class MemoryAwareProcessor:
    """Processor with memory management and garbage collection."""
    
    def __init__(self, max_memory_mb: int = 8192):
        self.max_memory_mb = max_memory_mb
        self.current_memory_usage = 0
        
    async def process_with_memory_check(self, batch_size: int = 50):
        """Process batches with memory monitoring."""
        
        for batch in chunks(instruments, batch_size):
            # Check memory usage before processing
            current_usage = get_memory_usage_mb()
            
            if current_usage > self.max_memory_mb * 0.8:
                # Trigger garbage collection
                gc.collect()
                await asyncio.sleep(1)
            
            # Process batch
            results = await process_instrument_batch(batch)
            
            # Brief pause to allow memory cleanup
            await asyncio.sleep(0.1)
```

---

## 4. Performance Optimization Design

### 4.1 I/O Optimization

#### 4.1.1 Async File Operations
```python
import aiofiles
import asyncio

async def save_file_async(data: pd.DataFrame, path: Path) -> None:
    """Asynchronous file writing with buffer management."""
    
    # Convert to bytes in memory
    buffer = io.BytesIO()
    data.to_parquet(buffer, compression='gzip')
    buffer.seek(0)
    
    # Write asynchronously
    async with aiofiles.open(path, 'wb') as f:
        await f.write(buffer.getvalue())
    
    buffer.close()
```

#### 4.1.2 Parallel Directory Creation
```python
async def create_directories_parallel(date_paths: List[Path]) -> None:
    """Create directory structure in parallel."""
    
    async def create_single_directory(path: Path):
        await asyncio.to_thread(path.mkdir, parents=True, exist_ok=True)
    
    # Create directories concurrently
    tasks = [create_single_directory(path) for path in date_paths]
    await asyncio.gather(*tasks)
```

### 4.2 API Rate Limiting Optimization

#### 4.2.1 Intelligent Rate Limiter
```python
class AdaptiveRateLimiter:
    """Rate limiter that adapts to API response patterns."""
    
    def __init__(self, initial_rate: float = 5.0):  # 5 calls per minute
        self.rate = initial_rate
        self.last_call_time = 0
        self.consecutive_errors = 0
        
    async def acquire(self):
        """Acquire rate limit permission with adaptive backoff."""
        
        # Calculate required delay
        time_since_last = time.time() - self.last_call_time
        required_interval = 60.0 / self.rate  # seconds per call
        
        if time_since_last < required_interval:
            delay = required_interval - time_since_last
            await asyncio.sleep(delay)
        
        self.last_call_time = time.time()
    
    def adjust_rate(self, success: bool):
        """Adjust rate based on API response."""
        if success:
            self.consecutive_errors = 0
            # Gradually increase rate
            self.rate = min(self.rate * 1.05, 10.0)
        else:
            self.consecutive_errors += 1
            # Decrease rate on errors
            self.rate = max(self.rate * 0.8, 1.0)
```

#### 4.2.2 Retry Strategy with Exponential Backoff
```python
async def api_call_with_retry(
    func: Callable,
    max_retries: int = 3,
    base_delay: float = 1.0,
    max_delay: float = 60.0
) -> Any:
    """Retry API calls with exponential backoff."""
    
    for attempt in range(max_retries + 1):
        try:
            return await func()
            
        except (APITimeoutError, RateLimitError) as e:
            if attempt == max_retries:
                raise
            
            # Exponential backoff with jitter
            delay = min(base_delay * (2 ** attempt), max_delay)
            jitter = random.uniform(0, delay * 0.1)
            
            await asyncio.sleep(delay + jitter)
            logger.warning(f"API call failed (attempt {attempt + 1}), retrying in {delay:.1f}s")
```

### 4.3 Memory Optimization

#### 4.3.1 Streaming Data Processing
```python
async def process_large_dataset_streaming(instruments: List[str]) -> None:
    """Process large datasets using streaming to minimize memory usage."""
    
    async def process_instrument_stream():
        for instrument in instruments:
            # Process one instrument at a time
            data = await download_minute_data(instrument)
            
            if data:
                # Process and save immediately
                await save_minute_data(data, get_output_path(instrument))
                
                # Clear from memory
                del data
                
                # Yield control to allow garbage collection
                await asyncio.sleep(0.01)
    
    await process_instrument_stream()
```

#### 4.3.2 Memory Pool Management
```python
class DataFramePool:
    """Pool of reusable DataFrame objects to reduce allocation overhead."""
    
    def __init__(self, pool_size: int = 100):
        self.pool = asyncio.Queue(maxsize=pool_size)
        self.pool_size = pool_size
        
    async def get_dataframe(self) -> pd.DataFrame:
        """Get DataFrame from pool or create new one."""
        try:
            df = await asyncio.wait_for(self.pool.get(), timeout=0.1)
            return df.iloc[0:0]  # Return empty DataFrame with same structure
        except asyncio.TimeoutError:
            return pd.DataFrame()
    
    async def return_dataframe(self, df: pd.DataFrame):
        """Return DataFrame to pool for reuse."""
        try:
            df.drop(df.index, inplace=True)  # Clear data but keep structure
            await asyncio.wait_for(self.pool.put(df), timeout=0.1)
        except asyncio.TimeoutError:
            pass  # Pool is full, let it be garbage collected
```

---

## 5. Error Handling & Recovery Design

### 5.1 Error Classification System

```python
class ProcessingError(Exception):
    """Base class for processing errors with classification."""
    
    ERROR_TYPES = {
        'API_RATE_LIMIT': {'severity': 'warning', 'retry': True, 'delay': 60},
        'API_TIMEOUT': {'severity': 'warning', 'retry': True, 'delay': 5},
        'DATA_QUALITY': {'severity': 'error', 'retry': False, 'delay': 0},
        'FILESYSTEM': {'severity': 'error', 'retry': True, 'delay': 1},
        'NETWORK': {'severity': 'warning', 'retry': True, 'delay': 10},
        'DATABASE': {'severity': 'critical', 'retry': True, 'delay': 30}
    }
    
    def __init__(self, error_type: str, message: str, context: Dict = None):
        self.error_type = error_type
        self.message = message
        self.context = context or {}
        super().__init__(f"{error_type}: {message}")
```

### 5.2 Recovery Strategies

#### 5.2.1 Automatic Recovery Actions
```python
async def handle_processing_error(
    error: ProcessingError,
    symbol: str,
    date: date,
    attempt: int = 1
) -> bool:
    """Handle processing errors with appropriate recovery actions."""
    
    error_config = ProcessingError.ERROR_TYPES.get(error.error_type, {})
    
    # Log error with context
    logger.error(f"Processing error for {symbol} on {date}", extra={
        'error_type': error.error_type,
        'attempt': attempt,
        'context': error.context
    })
    
    # Check if retry is appropriate
    if error_config.get('retry', False) and attempt <= 3:
        delay = error_config.get('delay', 1)
        logger.info(f"Retrying {symbol} in {delay} seconds (attempt {attempt + 1})")
        
        await asyncio.sleep(delay)
        return True  # Indicate retry should happen
    
    # Log failure for manual review
    failure_log = {
        'symbol': symbol,
        'date': date.isoformat(),
        'error_type': error.error_type,
        'message': error.message,
        'context': error.context,
        'timestamp': datetime.now().isoformat()
    }
    
    # Store in processing statistics for reporting
    stats['processing_errors'].append(failure_log)
    
    return False  # No more retries
```

#### 5.2.2 Circuit Breaker Pattern
```python
class CircuitBreaker:
    """Circuit breaker to prevent cascade failures."""
    
    def __init__(self, failure_threshold: int = 10, timeout: int = 300):
        self.failure_threshold = failure_threshold
        self.timeout = timeout
        self.failure_count = 0
        self.last_failure_time = 0
        self.state = 'CLOSED'  # CLOSED, OPEN, HALF_OPEN
        
    async def call(self, func: Callable, *args, **kwargs):
        """Execute function through circuit breaker."""
        
        # Check circuit state
        if self.state == 'OPEN':
            if time.time() - self.last_failure_time < self.timeout:
                raise CircuitBreakerOpenError("Circuit breaker is OPEN")
            else:
                self.state = 'HALF_OPEN'
        
        try:
            result = await func(*args, **kwargs)
            
            # Success - reset failure count
            if self.state == 'HALF_OPEN':
                self.state = 'CLOSED'
            self.failure_count = 0
            
            return result
            
        except Exception as e:
            self.failure_count += 1
            self.last_failure_time = time.time()
            
            # Open circuit if threshold exceeded
            if self.failure_count >= self.failure_threshold:
                self.state = 'OPEN'
                logger.error(f"Circuit breaker OPENED after {self.failure_count} failures")
            
            raise
```

### 5.3 Data Consistency Guarantees

#### 5.3.1 Atomic File Operations
```python
async def save_file_atomically(data: pd.DataFrame, final_path: Path) -> None:
    """Save file with atomic operations to prevent corruption."""
    
    # Create temporary file with unique name
    temp_path = final_path.with_suffix(f".tmp.{uuid.uuid4().hex[:8]}")
    
    try:
        # Write to temporary file
        await save_file_async(data, temp_path)
        
        # Verify file integrity
        await verify_file_integrity(temp_path)
        
        # Atomic move to final location
        temp_path.replace(final_path)
        
    except Exception as e:
        # Clean up temporary file on error
        if temp_path.exists():
            temp_path.unlink()
        raise ProcessingError('FILESYSTEM', f"Atomic save failed: {e}")
```

#### 5.3.2 Checkpointing System
```python
class ProcessingCheckpoint:
    """Checkpoint system for resumable processing."""
    
    def __init__(self, checkpoint_file: Path = Path("/logs/processing_checkpoint.json")):
        self.checkpoint_file = checkpoint_file
        
    async def save_progress(self, processed_symbols: List[str], current_date: date):
        """Save current processing progress."""
        
        checkpoint = {
            'timestamp': datetime.now().isoformat(),
            'processed_symbols': processed_symbols,
            'current_date': current_date.isoformat(),
            'total_processed': len(processed_symbols)
        }
        
        # Atomic checkpoint save
        temp_file = self.checkpoint_file.with_suffix('.tmp')
        async with aiofiles.open(temp_file, 'w') as f:
            await f.write(json.dumps(checkpoint, indent=2))
        
        temp_file.replace(self.checkpoint_file)
    
    async def load_progress(self) -> Optional[Dict]:
        """Load previous processing progress."""
        
        if not self.checkpoint_file.exists():
            return None
            
        try:
            async with aiofiles.open(self.checkpoint_file, 'r') as f:
                return json.loads(await f.read())
        except Exception as e:
            logger.warning(f"Failed to load checkpoint: {e}")
            return None
```

---

## 6. Security & Compliance Design

### 6.1 API Security

#### 6.1.1 Credential Management
```python
class SecureCredentialManager:
    """Secure management of API credentials with rotation support."""
    
    def __init__(self):
        self.credentials = {}
        self.last_rotation = {}
        
    def get_api_key(self, service: str) -> str:
        """Get API key with automatic rotation check."""
        
        # Check if key needs rotation (every 30 days)
        if self._needs_rotation(service):
            self._rotate_key(service)
        
        return os.getenv(f"{service.upper()}_API_KEY")
    
    def _needs_rotation(self, service: str) -> bool:
        """Check if API key needs rotation."""
        last_rotation = self.last_rotation.get(service, 0)
        return (time.time() - last_rotation) > (30 * 24 * 3600)  # 30 days
    
    def _rotate_key(self, service: str):
        """Rotate API key (placeholder for actual rotation logic)."""
        logger.info(f"API key rotation needed for {service}")
        self.last_rotation[service] = time.time()
```

#### 6.1.2 Request Authentication
```python
async def authenticated_api_request(
    url: str,
    headers: Dict[str, str] = None,
    timeout: int = 30
) -> Dict:
    """Make authenticated API request with security headers."""
    
    # Prepare secure headers
    secure_headers = {
        'User-Agent': 'ATS-INTG/1.0',
        'Accept': 'application/json',
        'Authorization': f"Bearer {credential_manager.get_api_key('firstrate')}",
        'X-Request-ID': str(uuid.uuid4()),
        **(headers or {})
    }
    
    # Make request with timeout and SSL verification
    async with aiohttp.ClientSession(
        timeout=aiohttp.ClientTimeout(total=timeout),
        connector=aiohttp.TCPConnector(ssl=True)
    ) as session:
        async with session.get(url, headers=secure_headers) as response:
            response.raise_for_status()
            return await response.json()
```

### 6.2 Data Privacy & Compliance

#### 6.2.1 Data Sanitization
```python
def sanitize_financial_data(df: pd.DataFrame) -> pd.DataFrame:
    """Sanitize financial data removing any potential PII."""
    
    # Remove any columns that might contain PII
    pii_columns = ['user_id', 'account_number', 'customer_id']
    df = df.drop(columns=[col for col in pii_columns if col in df.columns])
    
    # Validate that only expected financial data remains
    expected_columns = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
    unexpected_columns = [col for col in df.columns if col not in expected_columns]
    
    if unexpected_columns:
        logger.warning(f"Unexpected columns found: {unexpected_columns}")
        df = df[expected_columns]
    
    return df
```

#### 6.2.2 Audit Logging
```python
class AuditLogger:
    """Comprehensive audit logging for compliance."""
    
    def __init__(self, audit_log_path: Path = Path("/logs/audit.jsonl")):
        self.audit_log_path = audit_log_path
        
    async def log_data_access(self, symbol: str, date: date, operation: str):
        """Log data access operations for audit trail."""
        
        audit_entry = {
            'timestamp': datetime.now().isoformat(),
            'operation': operation,
            'symbol': symbol,
            'date': date.isoformat(),
            'user': 'ats-intg-system',
            'session_id': os.getenv('CONTAINER_ID', 'unknown'),
            'ip_address': 'container-internal'
        }
        
        # Append to audit log
        async with aiofiles.open(self.audit_log_path, 'a') as f:
            await f.write(json.dumps(audit_entry) + '\n')
```

---

## 7. Monitoring & Observability Design

### 7.1 Health Check System

#### 7.1.1 Multi-level Health Checks
```python
class SystemHealthChecker:
    """Comprehensive system health monitoring."""
    
    async def check_system_health(self) -> Dict[str, bool]:
        """Perform comprehensive system health check."""
        
        health_status = {}
        
        # Database connectivity
        health_status['database'] = await self._check_database()
        
        # API connectivity
        health_status['firstrate_api'] = await self._check_firstrate_api()
        
        # File system
        health_status['filesystem'] = await self._check_filesystem()
        
        # Memory usage
        health_status['memory'] = await self._check_memory_usage()
        
        # Disk space
        health_status['disk_space'] = await self._check_disk_space()
        
        return health_status
    
    async def _check_database(self) -> bool:
        """Check database connectivity and basic query."""
        try:
            async with self.db_pool.acquire() as conn:
                result = await conn.fetchval("SELECT 1")
                return result == 1
        except Exception:
            return False
    
    async def _check_firstrate_api(self) -> bool:
        """Check FirstRate API connectivity."""
        try:
            # Simple API health check (implementation dependent)
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    "https://api.firstrate.com/health",
                    timeout=aiohttp.ClientTimeout(total=10)
                ) as response:
                    return response.status == 200
        except Exception:
            return False
    
    async def _check_filesystem(self) -> bool:
        """Check filesystem write permissions."""
        try:
            test_file = self.daily_output_path / "health_check.tmp"
            test_file.touch()
            test_file.unlink()
            return True
        except Exception:
            return False
```

#### 7.1.2 Performance Monitoring
```python
class PerformanceMonitor:
    """Performance monitoring and alerting system."""
    
    def __init__(self):
        self.metrics = defaultdict(list)
        self.alert_thresholds = {
            'processing_time_per_symbol': 5.0,  # seconds
            'memory_usage_mb': 6000,            # MB
            'disk_usage_percent': 85,           # percentage
            'api_response_time': 2.0            # seconds
        }
    
    def record_metric(self, metric_name: str, value: float, timestamp: Optional[datetime] = None):
        """Record performance metric."""
        
        entry = {
            'value': value,
            'timestamp': timestamp or datetime.now(),
        }
        
        self.metrics[metric_name].append(entry)
        
        # Check for alert conditions
        if value > self.alert_thresholds.get(metric_name, float('inf')):
            self._trigger_alert(metric_name, value)
    
    def _trigger_alert(self, metric_name: str, value: float):
        """Trigger alert for performance threshold breach."""
        
        alert = {
            'metric': metric_name,
            'value': value,
            'threshold': self.alert_thresholds[metric_name],
            'timestamp': datetime.now().isoformat(),
            'severity': 'warning' if value < self.alert_thresholds[metric_name] * 1.2 else 'critical'
        }
        
        logger.warning(f"Performance alert: {alert}")
        
        # Store alert for reporting
        self.metrics['alerts'].append(alert)
```

### 7.2 Logging Strategy

#### 7.2.1 Structured Logging
```python
import structlog

# Configure structured logging
structlog.configure(
    processors=[
        structlog.stdlib.filter_by_level,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.processors.TimeStamper(fmt="ISO"),
        structlog.processors.JSONRenderer()
    ],
    context_class=dict,
    logger_factory=structlog.stdlib.LoggerFactory(),
    wrapper_class=structlog.stdlib.BoundLogger,
    cache_logger_on_first_use=True,
)

logger = structlog.get_logger()

# Usage examples
logger.info("Processing started",
    symbol="AAPL",
    date="2025-01-18",
    batch_size=50,
    processing_id="batch_001"
)

logger.error("API call failed",
    symbol="TSLA",
    error_type="RATE_LIMIT",
    retry_attempt=2,
    api_response_code=429
)
```

#### 7.2.2 Log Aggregation & Analysis
```python
class LogAnalyzer:
    """Automated log analysis and pattern detection."""
    
    def __init__(self, log_file_path: Path):
        self.log_file_path = log_file_path
        
    async def analyze_error_patterns(self) -> Dict[str, int]:
        """Analyze error patterns in logs."""
        
        error_patterns = defaultdict(int)
        
        async with aiofiles.open(self.log_file_path, 'r') as f:
            async for line in f:
                try:
                    log_entry = json.loads(line)
                    
                    if log_entry.get('level') == 'error':
                        error_type = log_entry.get('error_type', 'unknown')
                        error_patterns[error_type] += 1
                        
                except json.JSONDecodeError:
                    continue
        
        return dict(error_patterns)
    
    async def detect_anomalies(self) -> List[Dict]:
        """Detect anomalous patterns in processing logs."""
        
        anomalies = []
        
        # Example: Detect unusually high processing times
        processing_times = []
        
        async with aiofiles.open(self.log_file_path, 'r') as f:
            async for line in f:
                try:
                    log_entry = json.loads(line)
                    
                    if 'processing_time' in log_entry:
                        processing_times.append(log_entry['processing_time'])
                        
                except json.JSONDecodeError:
                    continue
        
        if processing_times:
            mean_time = statistics.mean(processing_times)
            std_time = statistics.stdev(processing_times)
            
            for time in processing_times:
                if abs(time - mean_time) > 3 * std_time:
                    anomalies.append({
                        'type': 'high_processing_time',
                        'value': time,
                        'threshold': mean_time + 3 * std_time
                    })
        
        return anomalies
```

---

## 8. Testing Strategy Design

### 8.1 Unit Testing Framework

#### 8.1.1 Component Testing
```python
import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock

class TestDailyMinuteBarBackfill:
    """Unit tests for daily minute bar backfill system."""
    
    @pytest.fixture
    async def backfill_system(self):
        """Create backfill system for testing."""
        system = DailyMinuteBarBackfill(
            base_data_path="/tmp/test_data",
            daily_output_path="/tmp/test_output",
            lookback_days=1
        )
        
        # Mock external dependencies
        system.db_pool = AsyncMock()
        system.firstrate = AsyncMock()
        
        return system
    
    @pytest.mark.asyncio
    async def test_instrument_classification(self, backfill_system):
        """Test instrument classification logic."""
        
        # Mock database response
        backfill_system.db_pool.acquire().__aenter__.return_value.fetch.return_value = [
            {'symbol': 'AAPL', 'active': True},
            {'symbol': 'SPY', 'active': True},
            {'symbol': 'INVALID-SYMBOL', 'active': True}
        ]
        
        instruments = await backfill_system.get_active_instruments()
        
        # Verify classification
        assert len(instruments) == 3
        assert ('SPY', 'critical_etf', 'UNKNOWN') in instruments
        assert ('AAPL', 'stock', 'UNKNOWN') in instruments
    
    @pytest.mark.asyncio
    async def test_file_path_generation(self, backfill_system):
        """Test file path generation logic."""
        
        symbol = "AAPL"
        test_date = date(2025, 1, 18)
        
        path = backfill_system.get_output_path(symbol, test_date)
        
        expected_path = Path("/tmp/test_output/2025/01/18/A/AAPL_20250118.parquet")
        assert path == expected_path
    
    @pytest.mark.asyncio
    async def test_data_validation(self, backfill_system):
        """Test data validation logic."""
        
        # Create test DataFrame with good data
        good_data = pd.DataFrame({
            'timestamp': pd.date_range('2025-01-18 09:30', periods=100, freq='1min'),
            'open': [100 + i * 0.1 for i in range(100)],
            'high': [100.5 + i * 0.1 for i in range(100)],
            'low': [99.5 + i * 0.1 for i in range(100)],
            'close': [100.2 + i * 0.1 for i in range(100)],
            'volume': [1000 + i * 10 for i in range(100)]
        })
        
        # Test validation passes
        result = backfill_system.validate_data(good_data)
        assert result.is_valid
        
        # Create test DataFrame with bad data
        bad_data = good_data.copy()
        bad_data.loc[0, 'high'] = bad_data.loc[0, 'low'] - 1  # Invalid OHLC
        
        result = backfill_system.validate_data(bad_data)
        assert not result.is_valid
```

#### 8.1.2 Integration Testing
```python
class TestIntegrationScenarios:
    """Integration tests for complete workflows."""
    
    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_end_to_end_processing(self):
        """Test complete end-to-end processing workflow."""
        
        # Setup test environment
        test_output_dir = Path("/tmp/integration_test")
        test_output_dir.mkdir(parents=True, exist_ok=True)
        
        # Create backfill system with real configuration
        backfill = DailyMinuteBarBackfill(
            daily_output_path=test_output_dir,
            lookback_days=1
        )
        
        # Mock external services but use real file operations
        backfill.db_pool = await create_test_db_pool()
        backfill.firstrate = create_mock_firstrate_client()
        
        # Run processing for small test set
        await backfill.run_daily_backfill(
            instrument_filter=['AAPL', 'SPY'],
            test_limit=2
        )
        
        # Verify files were created
        expected_files = [
            test_output_dir / "2025/01/18/A/AAPL_20250118.parquet",
            test_output_dir / "2025/01/18/S/SPY_20250118.parquet"
        ]
        
        for file_path in expected_files:
            assert file_path.exists()
            
            # Verify file contents
            df = pd.read_parquet(file_path)
            assert len(df) > 0
            assert all(col in df.columns for col in ['open', 'high', 'low', 'close', 'volume'])
        
        # Cleanup
        shutil.rmtree(test_output_dir)
    
    @pytest.mark.integration
    async def test_prometheus_metrics_integration(self):
        """Test Prometheus metrics integration."""
        
        metrics_server = PrometheusMetricsServer(port=18080)
        await metrics_server.initialize()
        
        try:
            # Start metrics server
            server_task = asyncio.create_task(metrics_server.start_server())
            await asyncio.sleep(1)  # Allow server to start
            
            # Test metrics endpoint
            async with aiohttp.ClientSession() as session:
                async with session.get("http://localhost:18080/metrics") as response:
                    assert response.status == 200
                    content = await response.text()
                    assert "ats_total_instruments" in content
                    
                # Test health endpoint
                async with session.get("http://localhost:18080/health") as response:
                    assert response.status == 200
                    health_data = await response.json()
                    assert health_data['status'] == 'healthy'
                    
        finally:
            await metrics_server.close()
```

### 8.2 Performance Testing

#### 8.2.1 Load Testing
```python
class TestPerformanceCharacteristics:
    """Performance and load testing."""
    
    @pytest.mark.performance
    @pytest.mark.asyncio
    async def test_concurrent_processing_performance(self):
        """Test performance under concurrent load."""
        
        # Create backfill system
        backfill = DailyMinuteBarBackfill(lookback_days=1)
        
        # Mock fast API responses
        backfill.firstrate = create_fast_mock_client()
        
        # Generate large instrument list
        test_instruments = [(f"SYM{i:04d}", "stock", "NYSE") for i in range(1000)]
        
        # Measure processing time
        start_time = time.time()
        
        await backfill.run_daily_backfill(
            instrument_filter=[inst[0] for inst in test_instruments[:100]],
            test_limit=100
        )
        
        processing_time = time.time() - start_time
        
        # Performance assertions
        assert processing_time < 120  # Should complete within 2 minutes
        assert backfill.stats['instruments_processed'] == 100
        
        # Calculate throughput
        throughput = 100 / processing_time  # instruments per second
        assert throughput > 1.0  # At least 1 instrument per second
    
    @pytest.mark.performance
    async def test_memory_usage_under_load(self):
        """Test memory usage during heavy processing."""
        
        import psutil
        process = psutil.Process()
        
        # Record initial memory usage
        initial_memory = process.memory_info().rss / 1024 / 1024  # MB
        
        # Create backfill system with large dataset
        backfill = DailyMinuteBarBackfill(lookback_days=7)
        
        # Process large number of instruments
        await backfill.run_daily_backfill(test_limit=500)
        
        # Check memory usage
        peak_memory = process.memory_info().rss / 1024 / 1024  # MB
        memory_increase = peak_memory - initial_memory
        
        # Memory usage assertions
        assert peak_memory < 8000  # Less than 8GB
        assert memory_increase < 6000  # Memory increase less than 6GB
```

---

## 9. Deployment & Operations Design

### 9.1 Container Deployment Strategy

#### 9.1.1 Multi-Container Architecture
```yaml
# Production deployment configuration
version: '3.8'

networks:
  ats-intg-network:
    external: true

volumes:
  ats-data:
    driver: local
    driver_opts:
      type: none
      device: /mnt/d/ats-data
      o: bind

services:
  scheduler:
    image: dragonflyer762/ats-genai:latest
    deploy:
      replicas: 1
      resources:
        limits:
          memory: 8G
          cpus: '4'
        reservations:
          memory: 4G
          cpus: '2'
      restart_policy:
        condition: unless-stopped
        delay: 30s
        max_attempts: 3
    
  metrics:
    image: dragonflyer762/ats-genai:latest
    deploy:
      replicas: 1
      resources:
        limits:
          memory: 2G
          cpus: '1'
        reservations:
          memory: 1G
          cpus: '0.5'
      restart_policy:
        condition: unless-stopped
    
  notifications:
    image: dragonflyer762/ats-genai:latest
    deploy:
      replicas: 1
      resources:
        limits:
          memory: 1G
          cpus: '0.5'
        reservations:
          memory: 512M
          cpus: '0.25'
```

#### 9.1.2 Health Check Configuration
```yaml
healthcheck:
  test: |
    bash -c '
    # Check main process
    pgrep cron || exit 1
    
    # Check database connectivity
    python3 -c "
    import asyncio
    import asyncpg
    async def check():
        try:
            conn = await asyncpg.connect(
                host=\"ats-intg-postgres\",
                port=5432,
                user=\"postgres\", 
                password=\"intg_password\",
                database=\"intg_db\"
            )
            await conn.fetchval(\"SELECT 1\")
            await conn.close()
        except Exception as e:
            exit(1)
    asyncio.run(check())
    " || exit 1
    
    # Check Prometheus metrics if applicable
    if [ \"$SERVICE_NAME\" = \"metrics\" ]; then
      curl -f http://localhost:8080/health || exit 1
    fi
    '
  interval: 2m
  timeout: 30s
  retries: 3
  start_period: 1m
```

### 9.2 Operational Procedures

#### 9.2.1 Deployment Procedure
```bash
#!/bin/bash
# deploy_minute_bars_system.sh

set -euo pipefail

echo "🚀 Deploying ATS-INTG Daily Minute Bars System"

# Verify prerequisites
echo "📋 Checking prerequisites..."
docker --version || { echo "Docker not available"; exit 1; }
docker-compose --version || { echo "Docker Compose not available"; exit 1; }

# Check required environment variables
required_vars=("SLACK_WEBHOOK_URL" "FIRSTRATE_API_KEY")
for var in "${required_vars[@]}"; do
    if [ -z "${!var:-}" ]; then
        echo "❌ Required environment variable $var is not set"
        exit 1
    fi
done

# Create required directories
echo "📁 Creating directory structure..."
python3 scripts/setup_daily_minute_bars_structure.py --years 2024,2025

# Pull latest images
echo "📦 Pulling latest container images..."
docker-compose -f docker-compose.minute-bars-jobs.yml pull

# Stop existing services
echo "🔄 Stopping existing services..."
docker-compose -f docker-compose.minute-bars-jobs.yml down || true

# Start services
echo "▶️  Starting minute bars services..."
docker-compose -f docker-compose.minute-bars-jobs.yml up -d

# Wait for services to become healthy
echo "🏥 Waiting for services to become healthy..."
timeout 300 bash -c '
until docker-compose -f docker-compose.minute-bars-jobs.yml ps | grep -q "healthy"; do
    echo "Waiting for health checks..."
    sleep 10
done
'

# Verify deployment
echo "✅ Verifying deployment..."
curl -f http://localhost:4080/health || { echo "Metrics server health check failed"; exit 1; }

# Run test processing
echo "🧪 Running test processing..."
docker exec ats-intg-minute-bars-scheduler \
    python3 scripts/daily_minute_bars_backfill.py --test --symbols AAPL --days 1

echo "🎉 Deployment completed successfully!"
```

#### 9.2.2 Monitoring & Maintenance
```bash
#!/bin/bash
# monitor_minute_bars_system.sh

# Daily health check script
check_system_health() {
    echo "🏥 Daily Health Check - $(date)"
    
    # Check container status
    echo "📊 Container Status:"
    docker-compose -f docker-compose.minute-bars-jobs.yml ps
    
    # Check disk usage
    echo "💾 Disk Usage:"
    df -h /mnt/d/ats-data/firstrate-data/daily/
    
    # Check recent logs for errors
    echo "📝 Recent Errors:"
    docker logs ats-intg-minute-bars-scheduler --since 24h | grep -i error | tail -10
    
    # Check metrics endpoint
    echo "📊 Metrics Endpoint:"
    curl -s http://localhost:4080/health | jq .
    
    # Check processing statistics
    echo "📈 Recent Processing:"
    find /mnt/d/ats-data/firstrate-data/daily/ -name "*.parquet" -mtime -1 | wc -l
    echo "files created in last 24 hours"
}

# Weekly maintenance
weekly_maintenance() {
    echo "🔧 Weekly Maintenance - $(date)"
    
    # Clean up old temporary files
    find /mnt/d/ats-data/firstrate-data/daily/ -name "*.tmp*" -mtime +7 -delete
    
    # Rotate logs
    docker-compose -f docker-compose.minute-bars-jobs.yml exec scheduler \
        logrotate /etc/logrotate.conf
    
    # Update container images
    docker-compose -f docker-compose.minute-bars-jobs.yml pull
    
    # Restart services for fresh state
    docker-compose -f docker-compose.minute-bars-jobs.yml restart
    
    # Generate weekly report
    docker exec ats-intg-slack-notifier \
        python3 scripts/slack_minute_bars_summary.py --weekly
}

# Execute based on argument
case "${1:-health}" in
    "health")
        check_system_health
        ;;
    "maintenance")
        weekly_maintenance
        ;;
    *)
        echo "Usage: $0 [health|maintenance]"
        exit 1
        ;;
esac
```

---

## 10. Appendices

### Appendix A: Configuration Examples

#### A.1 Environment Variables
```bash
# Required Environment Variables
export SLACK_WEBHOOK_URL="https://hooks.slack.com/services/..."
export FIRSTRATE_API_KEY="your_firstrate_api_key"
export PROMETHEUS_PUSHGATEWAY_URL="http://ats-intg-prometheus-metrics:8080"

# Database Configuration
export DB_HOST="ats-intg-postgres"
export DB_PORT="5432"
export DB_USER="postgres"
export DB_PASSWORD="intg_password"
export DB_NAME="intg_db"

# Processing Configuration
export LOOKBACK_DAYS="7"
export BATCH_SIZE="50"
export MAX_MEMORY_MB="8192"

# Monitoring Configuration
export METRICS_REFRESH_INTERVAL="300"
export HEALTH_CHECK_INTERVAL="120"
```

#### A.2 Cron Schedule Configuration
```bash
# /etc/cron.d/ats-minute-bars
SHELL=/bin/bash
PATH=/usr/local/sbin:/usr/local/bin:/sbin:/bin:/usr/sbin:/usr/bin
PYTHONPATH=/workspace/src

# Main daily backfill - 4:00 AM EST
0 4 * * * root cd /workspace && python3 scripts/daily_minute_bars_backfill.py --production >> /logs/minute-bars-backfill.log 2>&1

# Critical ETFs priority - 4:30 AM EST
30 4 * * * root cd /workspace && python3 scripts/daily_minute_bars_backfill.py --instrument-types critical_etf --days 3 >> /logs/minute-bars-critical-etf.log 2>&1

# Weekend catch-up - Saturday 6:00 AM EST
0 6 * * 6 root cd /workspace && python3 scripts/daily_minute_bars_backfill.py --production --days 10 >> /logs/minute-bars-weekend.log 2>&1

# Health check - Every 6 hours
0 */6 * * * root cd /workspace && python3 scripts/daily_minute_bars_backfill.py --test --symbols AAPL,SPY --days 1 >> /logs/minute-bars-health.log 2>&1
```

### Appendix B: API Documentation

#### B.1 FirstRate API Integration
```python
# API call examples and response formats
class FirstRateAPIDocumentation:
    """Documentation for FirstRate API integration."""
    
    async def get_minute_bars_example(self):
        """Example API call for 1-minute bars."""
        
        # Request format
        request = {
            "method": "GET",
            "url": "https://api.firstrate.com/v1/bars/minute",
            "headers": {
                "Authorization": "Bearer {api_key}",
                "Accept": "application/json"
            },
            "params": {
                "symbol": "AAPL",
                "start": "2025-01-18T09:30:00Z",
                "end": "2025-01-18T16:00:00Z",
                "timeframe": "1min"
            }
        }
        
        # Response format
        response = {
            "status": "success",
            "data": [
                {
                    "timestamp": "2025-01-18T09:30:00Z",
                    "open": 150.25,
                    "high": 150.75,
                    "low": 150.10,
                    "close": 150.50,
                    "volume": 125000
                },
                # ... more minute bars
            ],
            "metadata": {
                "symbol": "AAPL",
                "total_bars": 390,
                "start_time": "2025-01-18T09:30:00Z",
                "end_time": "2025-01-18T16:00:00Z"
            }
        }
```

### Appendix C: Troubleshooting Guide

#### C.1 Common Issues and Resolutions
```markdown
## Common Issues and Resolutions

### Issue 1: Processing Delays
**Symptoms**: Daily backfill taking >2 hours to complete
**Causes**: 
- FirstRate API rate limiting
- High database connection latency
- Insufficient processing resources

**Resolution**:
1. Check API rate limit status: `curl -H "Authorization: Bearer $API_KEY" https://api.firstrate.com/v1/ratelimit`
2. Monitor container resources: `docker stats ats-intg-minute-bars-scheduler`
3. Increase batch size: Set `BATCH_SIZE=100` in environment
4. Add more processing containers if needed

### Issue 2: File System Errors
**Symptoms**: Files not created or corruption detected
**Causes**:
- Insufficient disk space
- Permission issues
- Concurrent write conflicts

**Resolution**:
1. Check disk space: `df -h /mnt/d/ats-data/`
2. Verify permissions: `ls -la /mnt/d/ats-data/firstrate-data/daily/`
3. Check file locks: `lsof +D /mnt/d/ats-data/firstrate-data/daily/`
4. Restart processing with cleanup: `docker-compose restart scheduler`

### Issue 3: Missing Notifications
**Symptoms**: No Slack notifications received
**Causes**:
- Invalid webhook URL
- Network connectivity issues
- Cron job failures

**Resolution**:
1. Test webhook: `curl -X POST -H 'Content-type: application/json' --data '{"text":"Test"}' $SLACK_WEBHOOK_URL`
2. Check cron logs: `docker exec ats-intg-slack-notifier grep CRON /var/log/syslog`
3. Manual notification test: `docker exec ats-intg-slack-notifier python3 scripts/slack_minute_bars_summary.py --test`

### Issue 4: Prometheus Metrics Missing
**Symptoms**: Metrics endpoint returning errors
**Causes**:
- Database connection issues
- Server port conflicts
- Memory exhaustion

**Resolution**:
1. Check metrics server health: `curl http://localhost:4080/health`
2. Verify database connectivity: `docker exec ats-intg-prometheus-metrics python3 -c "import asyncpg; print('DB OK')"`
3. Check port availability: `netstat -tlnp | grep 4080`
4. Restart metrics server: `docker-compose restart ats-intg-prometheus-metrics`
```

---

**Document Control**  
**Version**: 1.0  
**Approved By**: Platform Engineering Team  
**Next Review**: 2025-02-18  
**Classification**: Internal Use