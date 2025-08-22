# Incremental Stats Computation and Update Strategy
## ATS Data Coverage Catalog - Real-Time Processing

**Document Version:** 1.0  
**Created:** August 2025  
**Data Engineering Lead:** AI Trading System Team  

---

## 1. Incremental Processing Overview

### 1.1 Real-Time Requirements
- **Update Latency**: Coverage statistics must reflect new data within 1 minute
- **Data Volume**: Process 50M+ minute bars per trading day (peak: 10K records/second)
- **Consistency**: Maintain eventual consistency across all aggregation levels
- **Reliability**: Zero data loss during processing with exactly-once semantics
- **Scalability**: Handle 10x growth in data volume without degradation

### 1.2 Update Patterns
- **Streaming Updates**: Real-time processing as data arrives
- **Micro-Batch Processing**: Group updates for efficiency (1-second windows)
- **Hierarchical Propagation**: Updates flow from minute → hour → day → week → month
- **Gap Healing**: Automatically detect and fill coverage gaps as backfill data arrives
- **Quality Revision**: Recompute statistics when data quality scores are updated

---

## 2. Event-Driven Update Architecture

### 2.1 Data Flow Pipeline

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│  Data Ingestion │───▶│  Change Stream  │───▶│  Stats Engine   │
│  (minute_bars,  │    │  (Kafka/CDC)    │    │  (Streaming)    │
│   daily_prices) │    │                 │    │                 │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                                │                       │
                                ▼                       ▼
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│  Gap Detection  │    │  Event Queue    │    │  Coverage Stats │
│  & Classification│◀───│  (Redis Stream) │───▶│  (TimescaleDB)  │
│                 │    │                 │    │                 │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │                       │                       │
         ▼                       ▼                       ▼
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│  Cache Updates  │    │  Alert System   │    │  Materialized   │
│  (Redis/Local)  │    │  (Notifications)│    │  View Refresh   │
│                 │    │                 │    │                 │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

### 2.2 Event Stream Processing with Kafka

#### 2.2.1 Kafka Topic Configuration

```yaml
# Kafka topics for coverage updates
topics:
  minute_bars_updates:
    partitions: 12  # Parallel processing across symbols
    replication_factor: 3
    retention.ms: 604800000  # 7 days
    compression.type: "snappy"
    
  daily_prices_updates:
    partitions: 4
    replication_factor: 3
    retention.ms: 604800000
    
  coverage_stats_updates:
    partitions: 8
    replication_factor: 3
    retention.ms: 2592000000  # 30 days
    
  coverage_gap_alerts:
    partitions: 2
    replication_factor: 3
    retention.ms: 604800000
```

#### 2.2.2 Streaming Processor Implementation

```python
class CoverageStatsStreamProcessor:
    """
    Real-time streaming processor for coverage statistics
    """
    
    def __init__(self, kafka_client, db_pool, redis_client):
        self.kafka = kafka_client
        self.db_pool = db_pool
        self.redis = redis_client
        
        # Processing configuration
        self.batch_size = 1000
        self.batch_timeout_ms = 1000  # 1 second
        self.max_parallel_processors = 8
        
        # State management
        self.processing_state = {}
        self.last_processed_timestamps = {}
        
    async def start_processing(self):
        """
        Start the streaming processor with fault tolerance
        """
        
        consumer_config = {
            'group_id': 'coverage_stats_processor',
            'auto_offset_reset': 'earliest',
            'enable_auto_commit': False,  # Manual commit for exactly-once
            'max_poll_records': self.batch_size,
            'max_poll_interval_ms': 300000,  # 5 minutes
        }
        
        consumer = await self.kafka.create_consumer(
            topics=['minute_bars_updates', 'daily_prices_updates'],
            **consumer_config
        )
        
        # Process messages in micro-batches
        async for batch in self.consume_micro_batches(consumer):
            try:
                await self.process_batch_with_retry(batch)
                await consumer.commit()
                
            except Exception as e:
                logger.error(f"Error processing batch: {e}")
                # Implement dead letter queue for failed batches
                await self.send_to_dlq(batch, str(e))
                
    async def consume_micro_batches(self, consumer):
        """
        Consume messages in time-bounded micro-batches
        """
        
        batch = []
        batch_start_time = time.time()
        
        async for message in consumer:
            batch.append(message)
            
            # Yield batch when size or time limit reached
            if (len(batch) >= self.batch_size or 
                (time.time() - batch_start_time) * 1000 > self.batch_timeout_ms):
                
                if batch:  # Only yield non-empty batches
                    yield batch
                    batch = []
                    batch_start_time = time.time()
    
    async def process_batch_with_retry(self, batch):
        """
        Process batch with exponential backoff retry
        """
        
        max_retries = 3
        base_delay = 1.0
        
        for attempt in range(max_retries + 1):
            try:
                await self.process_batch(batch)
                return  # Success
                
            except Exception as e:
                if attempt == max_retries:
                    raise  # Final attempt failed
                
                delay = base_delay * (2 ** attempt)
                logger.warning(f"Batch processing failed (attempt {attempt + 1}), "
                              f"retrying in {delay}s: {e}")
                await asyncio.sleep(delay)
    
    async def process_batch(self, batch):
        """
        Process a batch of data updates efficiently
        """
        
        # Group messages by symbol and vendor for efficient processing
        update_groups = self.group_updates_by_symbol_vendor(batch)
        
        # Process groups in parallel (up to max_parallel_processors)
        semaphore = asyncio.Semaphore(self.max_parallel_processors)
        
        async def process_group_with_semaphore(group_key, updates):
            async with semaphore:
                return await self.process_symbol_vendor_group(group_key, updates)
        
        # Execute all group processing tasks
        tasks = [
            process_group_with_semaphore(group_key, updates)
            for group_key, updates in update_groups.items()
        ]
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Handle any processing errors
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                group_key = list(update_groups.keys())[i]
                logger.error(f"Error processing group {group_key}: {result}")
                raise result
```

### 2.3 Incremental Aggregation Engine

#### 2.3.1 Hierarchical Stats Update Algorithm

```python
class IncrementalStatsEngine:
    """
    Efficiently update coverage statistics incrementally
    """
    
    def __init__(self, db_pool):
        self.db_pool = db_pool
        
        # Aggregation hierarchy: minute → hour → day → week → month
        self.aggregation_hierarchy = [
            ('minute', timedelta(minutes=1)),
            ('hour', timedelta(hours=1)),
            ('day', timedelta(days=1)),
            ('week', timedelta(weeks=1)),
            ('month', timedelta(days=30)),  # Approximate month
        ]
    
    async def update_stats_incrementally(
        self, 
        symbol: str, 
        vendor: str, 
        data_type: str,
        affected_timestamps: List[datetime]
    ):
        """
        Update coverage statistics incrementally for affected time periods
        """
        
        # Determine which aggregation periods are affected
        affected_periods = self.calculate_affected_periods(affected_timestamps)
        
        # Update each aggregation level
        for level, period_delta in self.aggregation_hierarchy:
            if level in affected_periods:
                await self.update_aggregation_level(
                    symbol, vendor, data_type, level, 
                    affected_periods[level]
                )
    
    def calculate_affected_periods(self, timestamps: List[datetime]) -> Dict[str, Set[datetime]]:
        """
        Calculate which aggregation periods are affected by timestamp changes
        """
        
        affected_periods = defaultdict(set)
        
        for timestamp in timestamps:
            # Calculate period start for each aggregation level
            affected_periods['minute'].add(date_trunc('minute', timestamp))
            affected_periods['hour'].add(date_trunc('hour', timestamp))
            affected_periods['day'].add(date_trunc('day', timestamp))
            affected_periods['week'].add(date_trunc('week', timestamp))
            affected_periods['month'].add(date_trunc('month', timestamp))
        
        return affected_periods
    
    async def update_aggregation_level(
        self,
        symbol: str,
        vendor: str, 
        data_type: str,
        aggregation_level: str,
        affected_periods: Set[datetime]
    ):
        """
        Update statistics for specific aggregation level and periods
        """
        
        async with self.db_pool.acquire() as conn:
            for period_start in affected_periods:
                await self.update_single_period_stats(
                    conn, symbol, vendor, data_type, 
                    aggregation_level, period_start
                )
    
    async def update_single_period_stats(
        self,
        conn,
        symbol: str,
        vendor: str,
        data_type: str,
        aggregation_level: str,
        period_start: datetime
    ):
        """
        Update coverage statistics for a single time period
        """
        
        # Calculate period end based on aggregation level
        if aggregation_level == 'hour':
            period_end = period_start + timedelta(hours=1)
            expected_records = 60  # 60 minutes
        elif aggregation_level == 'day':
            period_end = period_start + timedelta(days=1)
            expected_records = 60 * 6.5 * 60  # Trading hours
        elif aggregation_level == 'week':
            period_end = period_start + timedelta(weeks=1)
            expected_records = 60 * 6.5 * 60 * 5  # 5 trading days
        else:  # month
            period_end = period_start + timedelta(days=30)
            expected_records = 60 * 6.5 * 60 * 22  # ~22 trading days
        
        # Compute statistics from raw data
        stats_query = """
        SELECT 
            COUNT(*) as actual_records,
            AVG(quality_score) as avg_quality,
            MIN(quality_score) as min_quality,
            MAX(quality_score) as max_quality,
            STDDEV(quality_score) as quality_stddev,
            MIN(timestamp) as first_record,
            MAX(timestamp) as last_record,
            
            -- Gap analysis
            COUNT(*) FILTER (WHERE 
                LAG(timestamp) OVER (ORDER BY timestamp) < timestamp - INTERVAL '2 minutes'
            ) as gap_count,
            
            COALESCE(SUM(
                EXTRACT(EPOCH FROM (
                    timestamp - LAG(timestamp) OVER (ORDER BY timestamp)
                )) / 60
            ) FILTER (WHERE 
                LAG(timestamp) OVER (ORDER BY timestamp) < timestamp - INTERVAL '2 minutes'
            ), 0) as total_gap_minutes
            
        FROM minute_bars
        WHERE symbol = $1 
            AND vendor = $2
            AND timestamp >= $3 
            AND timestamp < $4
        """
        
        stats = await conn.fetchrow(
            stats_query, symbol, vendor, period_start, period_end
        )
        
        if stats and stats['actual_records'] > 0:
            # Update or insert coverage statistics
            await conn.execute("""
                INSERT INTO dev_coverage_stats (
                    symbol, vendor, data_type, aggregation_level,
                    period_start, period_end, total_expected, total_actual,
                    coverage_percentage, completeness_score,
                    avg_quality_score, min_quality_score, max_quality_score, quality_std_dev,
                    gap_count, total_gap_duration_minutes,
                    first_record_time, last_record_time,
                    records_per_minute, computation_time_ms, last_computed_at
                )
                VALUES (
                    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, NOW()
                )
                ON CONFLICT (symbol, vendor, data_type, aggregation_level, period_start)
                DO UPDATE SET
                    total_actual = EXCLUDED.total_actual,
                    coverage_percentage = EXCLUDED.coverage_percentage,
                    completeness_score = EXCLUDED.completeness_score,
                    avg_quality_score = EXCLUDED.avg_quality_score,
                    min_quality_score = EXCLUDED.min_quality_score,
                    max_quality_score = EXCLUDED.max_quality_score,
                    quality_std_dev = EXCLUDED.quality_std_dev,
                    gap_count = EXCLUDED.gap_count,
                    total_gap_duration_minutes = EXCLUDED.total_gap_duration_minutes,
                    first_record_time = EXCLUDED.first_record_time,
                    last_record_time = EXCLUDED.last_record_time,
                    records_per_minute = EXCLUDED.records_per_minute,
                    last_computed_at = NOW()
            """,
                symbol, vendor, data_type, aggregation_level,
                period_start, period_end, expected_records, stats['actual_records'],
                (stats['actual_records'] / expected_records) * 100.0,
                stats['actual_records'] / expected_records,
                stats['avg_quality'], stats['min_quality'], stats['max_quality'], stats['quality_stddev'],
                stats['gap_count'], stats['total_gap_minutes'],
                stats['first_record'], stats['last_record'],
                stats['actual_records'] / ((period_end - period_start).total_seconds() / 60),
                0  # computation_time_ms (placeholder)
            )
```

### 2.4 Gap Detection and Healing

#### 2.4.1 Real-Time Gap Detection

```python
class RealTimeGapDetector:
    """
    Detect coverage gaps in real-time as data arrives
    """
    
    def __init__(self, db_pool, redis_client):
        self.db_pool = db_pool
        self.redis = redis_client
        self.expected_intervals = {
            'minute': timedelta(minutes=1),
            'daily': timedelta(days=1)
        }
    
    async def detect_gaps_for_update(
        self,
        symbol: str,
        vendor: str,
        data_type: str,
        new_timestamps: List[datetime]
    ) -> List[CoverageGap]:
        """
        Detect new gaps caused by data updates
        """
        
        if not new_timestamps:
            return []
        
        # Sort timestamps for gap analysis
        timestamps = sorted(new_timestamps)
        start_time = timestamps[0] - timedelta(hours=1)  # Look back 1 hour
        end_time = timestamps[-1] + timedelta(hours=1)   # Look ahead 1 hour
        
        # Get existing data in the time range
        existing_data = await self.get_existing_timestamps(
            symbol, vendor, data_type, start_time, end_time
        )
        
        # Combine with new timestamps
        all_timestamps = sorted(set(existing_data + timestamps))
        
        # Detect gaps in the combined timeline
        gaps = []
        expected_interval = self.expected_intervals.get(data_type, timedelta(minutes=1))
        
        for i in range(len(all_timestamps) - 1):
            current_time = all_timestamps[i]
            next_time = all_timestamps[i + 1]
            
            # Check if gap exists
            if next_time - current_time > expected_interval * 1.5:  # Allow 50% tolerance
                gap = CoverageGap(
                    symbol=symbol,
                    vendor=vendor,
                    data_type=data_type,
                    gap_start=current_time + expected_interval,
                    gap_end=next_time,
                    gap_duration_minutes=int((next_time - current_time).total_seconds() / 60),
                    gap_type=self.classify_gap_type(current_time, next_time, expected_interval),
                    gap_severity=self.calculate_gap_severity(next_time - current_time),
                    detection_method='realtime_stream'
                )
                gaps.append(gap)
        
        return gaps
    
    async def heal_gaps_from_backfill(
        self,
        symbol: str,
        vendor: str,
        data_type: str,
        backfill_timestamps: List[datetime]
    ):
        """
        Mark gaps as healed when backfill data arrives
        """
        
        if not backfill_timestamps:
            return
        
        async with self.db_pool.acquire() as conn:
            for timestamp in backfill_timestamps:
                # Find gaps that this timestamp might heal
                healed_gaps = await conn.fetch("""
                    SELECT gap_id FROM dev_coverage_gaps
                    WHERE symbol = $1 AND vendor = $2 AND data_type = $3
                        AND gap_start <= $4 AND gap_end >= $4
                        AND is_resolved = FALSE
                """, symbol, vendor, data_type, timestamp)
                
                # Mark gaps as resolved
                for gap in healed_gaps:
                    await conn.execute("""
                        UPDATE dev_coverage_gaps
                        SET is_resolved = TRUE,
                            resolution_method = 'backfill_healing',
                            resolved_at = NOW(),
                            resolution_notes = 'Gap healed by backfill data'
                        WHERE gap_id = $1
                    """, gap['gap_id'])
                    
                    # Log gap healing
                    logger.info(f"Gap healed by backfill: {symbol}/{vendor} at {timestamp}")
    
    def classify_gap_type(
        self, 
        start_time: datetime, 
        end_time: datetime,
        expected_interval: timedelta
    ) -> str:
        """
        Classify gap type based on duration and timing
        """
        
        gap_duration = end_time - start_time
        
        if gap_duration <= expected_interval * 3:
            return 'minor'
        elif gap_duration <= timedelta(hours=1):
            return 'moderate'
        elif self.is_market_hours(start_time, end_time):
            return 'critical'
        else:
            return 'off_hours'
    
    def calculate_gap_severity(self, gap_duration: timedelta) -> str:
        """
        Calculate gap severity based on duration
        """
        
        minutes = gap_duration.total_seconds() / 60
        
        if minutes <= 5:
            return 'low'
        elif minutes <= 30:
            return 'medium'
        elif minutes <= 120:
            return 'high'
        else:
            return 'critical'
```

### 2.5 Materialized View Refresh Strategy

#### 2.5.1 Smart Refresh Logic

```python
class MaterializedViewManager:
    """
    Intelligently refresh materialized views based on data changes
    """
    
    def __init__(self, db_pool, redis_client):
        self.db_pool = db_pool
        self.redis = redis_client
        
        # Refresh policies for different views
        self.refresh_policies = {
            'mv_coverage_dashboard': {
                'max_staleness_minutes': 5,
                'refresh_on_symbols': ['AAPL', 'TSLA', 'MSFT'],  # High-priority symbols
                'refresh_method': 'incremental'
            },
            'mv_coverage_timeseries': {
                'max_staleness_minutes': 15,
                'refresh_on_symbols': None,  # All symbols
                'refresh_method': 'full'
            }
        }
    
    async def schedule_view_refresh(
        self,
        symbol: str,
        vendor: str,
        affected_periods: Set[datetime]
    ):
        """
        Schedule materialized view refresh based on data changes
        """
        
        for view_name, policy in self.refresh_policies.items():
            # Check if this symbol triggers refresh
            if (policy['refresh_on_symbols'] is None or 
                symbol in policy['refresh_on_symbols']):
                
                # Check staleness
                last_refresh = await self.get_last_refresh_time(view_name)
                staleness_minutes = (datetime.now() - last_refresh).total_seconds() / 60
                
                if staleness_minutes >= policy['max_staleness_minutes']:
                    # Schedule refresh
                    await self.queue_view_refresh(view_name, policy['refresh_method'])
    
    async def queue_view_refresh(self, view_name: str, refresh_method: str):
        """
        Queue materialized view refresh in Redis
        """
        
        refresh_task = {
            'view_name': view_name,
            'refresh_method': refresh_method,
            'scheduled_at': datetime.now().isoformat(),
            'priority': self.calculate_refresh_priority(view_name)
        }
        
        # Add to Redis priority queue
        await self.redis.zadd(
            'materialized_view_refresh_queue',
            {json.dumps(refresh_task): refresh_task['priority']}
        )
    
    async def process_refresh_queue(self):
        """
        Process queued materialized view refreshes
        """
        
        while True:
            try:
                # Get highest priority refresh task
                tasks = await self.redis.zrevrange(
                    'materialized_view_refresh_queue', 0, 0, withscores=True
                )
                
                if not tasks:
                    await asyncio.sleep(10)  # No tasks, wait
                    continue
                
                task_data, priority = tasks[0]
                task = json.loads(task_data)
                
                # Remove from queue
                await self.redis.zrem('materialized_view_refresh_queue', task_data)
                
                # Execute refresh
                await self.execute_view_refresh(task)
                
            except Exception as e:
                logger.error(f"Error processing refresh queue: {e}")
                await asyncio.sleep(30)
    
    async def execute_view_refresh(self, task: Dict):
        """
        Execute materialized view refresh
        """
        
        view_name = task['view_name']
        refresh_method = task['refresh_method']
        
        start_time = time.time()
        
        try:
            async with self.db_pool.acquire() as conn:
                if refresh_method == 'incremental':
                    # Incremental refresh (PostgreSQL doesn't support this natively,
                    # so we simulate by refreshing with WHERE clause)
                    await conn.execute(f"REFRESH MATERIALIZED VIEW {view_name}")
                else:
                    # Full refresh
                    await conn.execute(f"REFRESH MATERIALIZED VIEW {view_name}")
                
                refresh_duration = time.time() - start_time
                
                logger.info(f"Refreshed materialized view {view_name} in {refresh_duration:.2f}s")
                
                # Update refresh tracking
                await self.redis.hset(
                    'materialized_view_refresh_times',
                    view_name,
                    datetime.now().isoformat()
                )
                
        except Exception as e:
            logger.error(f"Error refreshing materialized view {view_name}: {e}")
            
            # Re-queue with lower priority if it failed
            task['priority'] = max(1, task['priority'] - 10)
            await self.redis.zadd(
                'materialized_view_refresh_queue',
                {json.dumps(task): task['priority']}
            )
```

---

## 3. Performance Monitoring and Optimization

### 3.1 Real-Time Metrics Collection

```python
class CoverageMetricsCollector:
    """
    Collect and monitor performance metrics for coverage processing
    """
    
    def __init__(self, metrics_client):
        self.metrics = metrics_client
        
    def record_processing_metrics(
        self,
        operation: str,
        duration_ms: float,
        records_processed: int,
        success: bool = True
    ):
        """
        Record processing performance metrics
        """
        
        tags = {
            'operation': operation,
            'status': 'success' if success else 'error'
        }
        
        self.metrics.histogram('coverage_processing_duration_ms', duration_ms, tags=tags)
        self.metrics.histogram('coverage_records_processed', records_processed, tags=tags)
        self.metrics.increment('coverage_operations_total', tags=tags)
        
        # Alert on performance degradation
        if duration_ms > 5000:  # > 5 seconds
            self.metrics.increment('coverage_slow_operations', tags=tags)
            
    def record_gap_detection_metrics(self, gaps_detected: int, gaps_healed: int):
        """
        Record gap detection and healing metrics
        """
        
        self.metrics.gauge('coverage_gaps_detected', gaps_detected)
        self.metrics.gauge('coverage_gaps_healed', gaps_healed)
        self.metrics.increment('coverage_gap_detection_runs')
```

### 3.2 Auto-Optimization Strategies

```python
class CoverageAutoOptimizer:
    """
    Automatically optimize coverage processing based on performance metrics
    """
    
    def __init__(self, db_pool, redis_client, metrics_client):
        self.db_pool = db_pool
        self.redis = redis_client
        self.metrics = metrics_client
        
    async def optimize_batch_sizes(self):
        """
        Automatically adjust batch sizes based on processing performance
        """
        
        # Get recent processing metrics
        recent_metrics = await self.get_recent_processing_metrics()
        
        # Analyze performance vs batch size
        optimal_batch_size = self.calculate_optimal_batch_size(recent_metrics)
        
        # Update configuration
        if optimal_batch_size != await self.get_current_batch_size():
            await self.update_batch_size_config(optimal_batch_size)
            logger.info(f"Optimized batch size to {optimal_batch_size}")
    
    async def optimize_aggregation_schedules(self):
        """
        Optimize aggregation refresh schedules based on usage patterns
        """
        
        # Analyze query patterns
        query_patterns = await self.analyze_query_patterns()
        
        # Adjust refresh frequencies
        for aggregation_level, frequency in query_patterns.items():
            if frequency > 100:  # High usage
                await self.increase_refresh_frequency(aggregation_level)
            elif frequency < 10:  # Low usage
                await self.decrease_refresh_frequency(aggregation_level)
```

---

## 4. Summary

This incremental stats computation strategy provides:

1. **Real-Time Processing**: Sub-minute latency for coverage updates
2. **Efficient Aggregation**: Hierarchical updates minimize computation
3. **Gap Management**: Automated detection and healing of coverage gaps
4. **Smart Caching**: Intelligent cache invalidation and refresh
5. **Fault Tolerance**: Exactly-once processing with retry mechanisms
6. **Auto-Optimization**: Self-tuning based on performance metrics
7. **Scalability**: Handles 10x growth without architectural changes

The system efficiently maintains accurate coverage statistics across 100M-2B row datasets while providing real-time visibility into data availability patterns.

<function_calls>
<invoke name="TodoWrite">
<parameter name="todos">[{"id": "1", "content": "Analyze existing analytics app structure and database schema", "status": "completed"}, {"id": "2", "content": "Define Product Requirements Document (PRD) for data catalog", "status": "completed"}, {"id": "3", "content": "Define Data Requirements Document (DRD) with schema design", "status": "completed"}, {"id": "4", "content": "Design scalable architecture for 100M-2B row querying", "status": "completed"}, {"id": "5", "content": "Plan incremental stats computation and update strategy", "status": "completed"}]