# DRD: Multi-Timeframe OHLC and Signal States Computation System

**Document Version:** 1.1  
**Date:** 2025-09-02  
**Author:** ATS Development Team  
**Status:** Implementation Complete - Indicator Builder Integration  

## Executive Summary

This Design Requirements Document (DRD) provides detailed technical specifications for implementing the Multi-Timeframe OHLC and Signal States Computation System. It defines the architecture, algorithms, data structures, and implementation details required to deliver the functionality outlined in the corresponding PRD.

## 1. System Architecture

### 1.1 Component Overview

```
┌─────────────────────────────────────────────────────────────┐
│                  ATS Training Data System                    │
├─────────────────────────────────────────────────────────────┤
│  IntervalBasedTrainingDataCallback                          │
│  ├─ Multi-timeframe feature extraction                     │
│  ├─ Signal-aware training example generation               │
│  └─ Structured output (.riegeli files)                    │
└─────────────────────────────────────────────────────────────┘
                           │
                           ▼ (API Calls)
┌─────────────────────────────────────────────────────────────┐
│            FileBasedMinuteMarketDataManager                 │
├─────────────────────────────────────────────────────────────┤
│  Enhanced API Layer:                                       │
│  ├─ get_ohlc_for_interval()                               │
│  ├─ get_ohlc_with_signals()                               │
│  └─ get_multi_timeframe_data()                            │
├─────────────────────────────────────────────────────────────┤
│  OHLC Aggregation Engine:                                  │
│  ├─ Interval notation parser                               │
│  ├─ Time series resampling                                 │
│  └─ Multi-symbol batch processing                          │
├─────────────────────────────────────────────────────────────┤
│  Indicator Builder Integration:                            │
│  ├─ OHLCV to InstrumentInterval adapter                   │
│  ├─ Indicator configuration per timeframe                 │
│  ├─ Existing indicator classes (SMA, EMA, RSI, etc.)     │
│  └─ Gin-configurable indicator sets                       │
├─────────────────────────────────────────────────────────────┤
│  Caching & Performance Layer:                              │
│  ├─ In-memory DataFrame cache                              │
│  ├─ Query result caching                                   │
│  └─ Batch operation optimization                           │
└─────────────────────────────────────────────────────────────┘
                           │
                           ▼ (File I/O)
┌─────────────────────────────────────────────────────────────┐
│                FileBasedMinuteManager                       │
├─────────────────────────────────────────────────────────────┤
│  ├─ Parquet file reading                                   │
│  ├─ Date-based file organization                           │
│  └─ Symbol-based directory structure                       │
└─────────────────────────────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────┐
│              Minute-Level OHLCV Data Storage                │
│        /mnt/d/ats-data/minute-bars/{vendor}/{symbol}/       │
└─────────────────────────────────────────────────────────────┘
```

### 1.2 Data Flow Architecture

```
Input: 1-minute OHLCV Parquet Files
           │
           ▼
    [File Reading Layer]
           │
           ▼
    [Data Standardization]
    ├─ Column name normalization
    ├─ Data type enforcement
    └─ Timestamp timezone handling
           │
           ▼
    [OHLC Aggregation Engine]
    ├─ Interval parsing (1m→5m,15m,1h,1d,1w)
    ├─ Pandas resampling operations
    └─ Aggregation rule application
           │
           ▼
    [Technical Signal Computation]
    ├─ Moving averages calculation
    ├─ Oscillator computation
    ├─ Volatility indicator generation
    └─ Custom signal derivation
           │
           ▼
    [Caching & Optimization]
    ├─ Result caching by query signature
    ├─ Incremental computation
    └─ Memory usage optimization
           │
           ▼
Output: Multi-timeframe DataFrames with Signals
```

## 2. Detailed Technical Specifications

### 2.1 OHLC Aggregation Algorithm

#### 2.1.1 Interval Parsing Algorithm

```python
def _parse_interval_to_minutes(interval: str) -> int:
    \"\"\"
    Convert interval notation to minutes for pandas resampling.
    
    Algorithm:
    1. Normalize input (lowercase, strip whitespace)
    2. Extract numeric prefix and unit suffix
    3. Apply unit-specific multiplication factors
    4. Validate against supported intervals
    5. Return total minutes as integer
    \"\"\"
    
    UNIT_MULTIPLIERS = {
        'm': 1,          # minutes
        'h': 60,         # hours to minutes  
        'd': 1440,       # days to minutes (24 * 60)
        'w': 10080,      # weeks to minutes (7 * 24 * 60)
        'M': 43800       # months to minutes (30.4 * 24 * 60, average)
    }
    
    # Implementation details...
```

#### 2.1.2 OHLC Aggregation Rules

**Standard OHLC Aggregation:**
- **Open**: `first()` - First non-null open price in the period
- **High**: `max()` - Maximum high price in the period
- **Low**: `min()` - Minimum low price in the period
- **Close**: `last()` - Last non-null close price in the period
- **Volume**: `sum()` - Total volume for the period

**Extended Aggregation for Additional Fields:**
- **VWAP**: `mean()` - Average VWAP in the period
- **Trade Count**: `sum()` - Total number of trades
- **Vendor**: `first()` - First vendor identifier

**Pandas Implementation:**
```python
agg_rules = {
    'open': 'first',
    'high': 'max', 
    'low': 'min',
    'close': 'last',
    'volume': 'sum',
    'vwap': 'mean',
    'trade_count': 'sum'
}

resampled = df.resample(f'{target_minutes}min').agg(agg_rules)
```

#### 2.1.3 Time Series Resampling Process

1. **Timestamp Indexing**: Set timestamp column as pandas DatetimeIndex
2. **Timezone Handling**: Ensure consistent UTC timezone across all data
3. **Resampling**: Use pandas `resample()` with appropriate frequency string
4. **Gap Handling**: Drop periods with insufficient data (NaN OHLC values)
5. **Index Reset**: Convert back to column-based format for consistency

### 2.2 Indicator Builder Integration Architecture

#### 2.2.1 OHLCV to InstrumentInterval Adapter

```python
class OHLCVToIntervalAdapter:
    """
    Adapter pattern for converting pandas DataFrames to InstrumentInterval objects.
    
    Responsibilities:
    1. Convert DataFrame rows to InstrumentInterval objects
    2. Handle timestamp conversion and timezone normalization
    3. Integrate with indicator builder for signal computation
    4. Extract flattened indicator values for DataFrame integration
    """
    
    def convert_dataframe_to_intervals(self, df: pd.DataFrame, symbol: str) -> List[InstrumentInterval]:
        """Convert OHLCV DataFrame to InstrumentInterval objects."""
        intervals = []
        for idx, row in df.iterrows():
            interval = InstrumentInterval(
                instrument_id=self.instrument_id,
                start_date_time=timestamp,
                end_date_time=timestamp,
                symbol=symbol,
                open=float(row['open']),
                high=float(row['high']),
                low=float(row['low']),
                close=float(row['close']),
                volume=int(row['volume']),
                status='ok'
            )
            intervals.append(interval)
        return intervals
    
    def compute_indicators_for_timeframe(self, df: pd.DataFrame, indicator_config, symbol: str):
        """Compute indicators using the indicator builder system."""
        intervals = self.convert_dataframe_to_intervals(df, symbol)
        indicators = indicator_config.create_indicator_instances()
        
        results = {}
        for name, indicator in indicators.items():
            indicator.update(intervals)
            results[name] = {
                'value': indicator.get_value(),
                'status': getattr(indicator, 'status', 'ok'),
                'update_at': getattr(indicator, 'update_at', None)
            }
        
        return results
```

#### 2.2.2 Gin Configuration Integration

**Indicator Configuration per Timeframe:**
```gin
# Base configuration in config/base.gin
FileBasedMinuteMarketDataManager.indicator_configs = {
    '1m': @IndicatorConfig.basic_config(),
    '5m': @IndicatorConfig.multi_timeframe_config(), 
    '15m': @IndicatorConfig.multi_timeframe_config(),
    '1h': @IndicatorConfig.standard_technical_config(),
    '1d': @IndicatorConfig.standard_technical_config()
}

# Standard Technical Indicators Configuration
IndicatorConfig.standard_technical_config.indicators = {
    'SMA_20': @SMA(20),
    'EMA_20': @EMA(20),
    'RSI_14': @RSI(14),
    'VWAP': @VWAP(),
    'BB_20': @BollingerBands(20, 2.0),
    'MACD': @MACD(12, 26, 9),
    'Stoch_14': @StochasticOscillator(14, 3)
}
```

**Runtime Indicator Selection:**
```python
def _compute_technical_signals(self, df: pd.DataFrame, signals: List[str], interval: str = '1m'):
    """
    Compute indicators using the indicator builder system.
    
    Flow:
    1. Get indicator configuration for timeframe from Gin config
    2. Convert DataFrame to InstrumentInterval objects via adapter
    3. Compute indicators using existing indicator classes
    4. Add indicator values as DataFrame columns
    5. Handle errors gracefully with fallback to NaN values
    """
    
    # Get timeframe-specific indicator configuration
    indicator_config = self.indicator_configs.get(interval, IndicatorConfig.multi_timeframe_config())
    
    # Compute indicators via adapter
    indicator_results = self.ohlcv_adapter.compute_indicators_for_timeframe(
        df, indicator_config, symbol
    )
    
    # Add results to DataFrame
    for indicator_name, result in indicator_results.items():
        if result['status'] == 'ok' and result['value'] is not None:
            df[indicator_name] = result['value']
        else:
            df[indicator_name] = pd.NA
    
    return df
```

#### 2.2.3 Existing Indicator Class Integration

**Leveraged Indicator Classes:**
- **Base Indicators**: PL, OneOneHigh, OneOneLow, OneOneDot, EnvelopeTop, EnvelopeBot
- **Standard Technical**: SMA, EMA, RSI, VWAP, BollingerBands, MACD, StochasticOscillator
- **All indicators**: Inherit from base Indicator class, use InstrumentInterval objects
- **No modifications**: Existing indicator classes work without changes

**Indicator Builder Workflow:**
1. **Configuration**: Gin files define which indicators are used per timeframe
2. **Instantiation**: IndicatorConfig creates indicator instances on demand
3. **Data Flow**: OHLCV data → InstrumentInterval objects → Indicator.update() → results
4. **Integration**: Results added as DataFrame columns with proper naming

### 2.3 Technical Signal Computation Algorithms

#### 2.2.1 Moving Averages

**Simple Moving Average (SMA):**
```python
def compute_sma(prices: pd.Series, period: int) -> pd.Series:
    \"\"\"
    SMA(n) = (P1 + P2 + ... + Pn) / n
    
    Where P1...Pn are the last n closing prices.
    \"\"\"
    return prices.rolling(window=period, min_periods=1).mean()
```

**Exponential Moving Average (EMA):**
```python
def compute_ema(prices: pd.Series, span: int) -> pd.Series:
    \"\"\"
    EMA calculation using pandas ewm() with span parameter.
    
    EMA(today) = (Price(today) * K) + (EMA(yesterday) * (1 - K))
    Where K = 2 / (span + 1)
    \"\"\"
    return prices.ewm(span=span).mean()
```

#### 2.2.2 Oscillators

**Relative Strength Index (RSI):**
```python
def compute_rsi(prices: pd.Series, period: int = 14) -> pd.Series:
    \"\"\"
    RSI = 100 - (100 / (1 + RS))
    RS = Average Gain / Average Loss over period
    
    Algorithm:
    1. Calculate price changes (delta = price[i] - price[i-1])
    2. Separate gains (positive deltas) and losses (negative deltas)
    3. Calculate average gain and average loss over period
    4. Compute RS and apply RSI formula
    \"\"\"
    delta = prices.diff()
    gain = delta.where(delta > 0, 0)
    loss = -delta.where(delta < 0, 0)
    
    avg_gain = gain.rolling(window=period).mean()
    avg_loss = loss.rolling(window=period).mean()
    
    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))
    
    return rsi
```

#### 2.2.3 Volatility Indicators

**Bollinger Bands:**
```python
def compute_bollinger_bands(prices: pd.Series, period: int = 20, 
                          std_dev: float = 2.0) -> Dict[str, pd.Series]:
    \"\"\"
    Bollinger Bands calculation:
    - Middle Band = SMA(period)
    - Upper Band = SMA(period) + (std_dev * standard_deviation(period))
    - Lower Band = SMA(period) - (std_dev * standard_deviation(period))
    \"\"\"
    sma = prices.rolling(period).mean()
    std = prices.rolling(period).std()
    
    return {
        'bb_middle': sma,
        'bb_upper': sma + (std * std_dev),
        'bb_lower': sma - (std * std_dev)
    }
```

#### 2.2.4 Custom ATS Signals

**Envelope Indicators (ETOP/EBOT):**
```python
def compute_envelope_top(df: pd.DataFrame, period: int = 20, 
                        pct: float = 0.05) -> pd.Series:
    \"\"\"
    ETOP = SMA(period) * (1 + percentage)
    
    Creates upper envelope line above SMA by specified percentage.
    Used for identifying overbought conditions.
    \"\"\"
    sma = df['close'].rolling(period).mean()
    return sma * (1 + pct)

def compute_envelope_bottom(df: pd.DataFrame, period: int = 20,
                          pct: float = 0.05) -> pd.Series:
    \"\"\"
    EBOT = SMA(period) * (1 - percentage)
    
    Creates lower envelope line below SMA by specified percentage.
    Used for identifying oversold conditions.
    \"\"\"
    sma = df['close'].rolling(period).mean()
    return sma * (1 - pct)
```

**Pivot Line Dot (PLDOT):**
```python
def compute_pivot_line_dot(df: pd.DataFrame) -> pd.Series:
    \"\"\"
    PLDOT = (High + Low + Close) / 3
    
    Classical pivot point calculation using typical price.
    Represents the average price for the period.
    \"\"\"
    return (df['high'] + df['low'] + df['close']) / 3
```

**Volume Weighted Average Price (VWAP):**
```python
def compute_vwap(df: pd.DataFrame) -> pd.Series:
    \"\"\"
    VWAP = Σ(Typical_Price * Volume) / Σ(Volume)
    
    Where Typical_Price = (High + Low + Close) / 3
    
    Cumulative calculation provides running VWAP.
    \"\"\"
    typical_price = (df['high'] + df['low'] + df['close']) / 3
    vwap = (typical_price * df['volume']).cumsum() / df['volume'].cumsum()
    return vwap
```

### 2.3 Caching and Performance Architecture

#### 2.3.1 Multi-Level Caching Strategy

**Level 1: Query Result Cache**
```python
class QueryResultCache:
    \"\"\"
    Cache complete query results by signature hash.
    
    Cache Key: hash(symbols, start, end, interval, signals)
    Cache Value: Dict[str, pd.DataFrame]
    TTL: 1 hour for intraday, 24 hours for daily+
    \"\"\"
    
    def get_cache_key(self, symbols, start, end, interval, signals):
        # Generate deterministic hash from parameters
        pass
        
    def should_cache(self, interval: str) -> bool:
        # Cache longer timeframes longer
        pass
```

**Level 2: DataFrame Fragment Cache**
```python
class DataFrameCache:
    \"\"\"
    Cache individual symbol DataFrames by date range.
    
    Cache Key: f"{symbol}_{start_date}_{end_date}_{interval}"
    Cache Value: pd.DataFrame
    Memory Limit: 1GB total cache size
    \"\"\"
    
    def eviction_policy(self):
        # LRU eviction when memory limit reached
        pass
```

#### 2.3.2 Batch Processing Optimization

**Parallel Symbol Processing:**
```python
async def process_symbols_batch(symbols: List[str]) -> Dict[str, pd.DataFrame]:
    \"\"\"
    Process multiple symbols concurrently using asyncio.
    
    Strategy:
    1. Split symbols into optimal batch sizes (10-20 per batch)
    2. Process batches concurrently with asyncio.gather()
    3. Limit concurrent file I/O to prevent resource exhaustion
    4. Aggregate results and handle individual failures gracefully
    \"\"\"
    
    tasks = [process_single_symbol(symbol) for symbol in symbols]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    
    # Handle results and exceptions...
```

**Memory-Efficient Processing:**
- **Streaming Processing**: Process large datasets in chunks
- **Lazy Loading**: Load data only when needed
- **Memory Monitoring**: Track memory usage and trigger cleanup
- **Garbage Collection**: Explicit cleanup of large DataFrames

### 2.4 Error Handling and Edge Cases

#### 2.4.1 Data Quality Issues

**Missing Data Handling:**
```python
def handle_missing_data(df: pd.DataFrame) -> pd.DataFrame:
    \"\"\"
    Strategy for handling missing/corrupted data:
    
    1. Forward fill for small gaps (< 5 minutes)
    2. Drop periods with insufficient data for aggregation
    3. Interpolate for technical signals when appropriate
    4. Mark questionable periods with data quality flags
    \"\"\"
    
    # Forward fill small gaps
    df = df.fillna(method='ffill', limit=5)
    
    # Drop periods with no OHLC data
    df = df.dropna(subset=['open', 'high', 'low', 'close'])
    
    return df
```

**Data Validation:**
```python
def validate_ohlc_data(df: pd.DataFrame) -> bool:
    \"\"\"
    Validate OHLC data integrity:
    
    1. High >= Low for all periods
    2. Open and Close within High/Low range
    3. Volume >= 0
    4. No extreme price movements (> 50% in single period)
    5. Timestamps in ascending order
    \"\"\"
    
    validations = [
        (df['high'] >= df['low']).all(),
        (df['open'] <= df['high']).all(),
        (df['open'] >= df['low']).all(),
        (df['close'] <= df['high']).all(),
        (df['close'] >= df['low']).all(),
        (df['volume'] >= 0).all()
    ]
    
    return all(validations)
```

#### 2.4.2 Performance Edge Cases

**Large Dataset Handling:**
- **Memory Limits**: Process datasets larger than available RAM
- **Time Limits**: Implement query timeouts to prevent hanging
- **Resource Limits**: Limit concurrent processing to prevent system overload

**Extreme Market Conditions:**
- **Flash Crashes**: Handle extreme price movements gracefully
- **Low Volume**: Adjust signal calculations for low-volume periods
- **Market Holidays**: Handle gaps in data due to market closures

### 2.5 API Implementation Details

#### 2.5.1 Core API Methods

**Primary OHLC Aggregation:**
```python
async def get_ohlc_for_interval(
    self,
    symbols: List[str],
    start: datetime,
    end: datetime,
    interval: str = '1m'
) -> Dict[str, pd.DataFrame]:
    \"\"\"
    Implementation flow:
    1. Validate input parameters
    2. Parse interval notation to minutes
    3. Check cache for existing results
    4. Load 1-minute base data for symbols
    5. Apply aggregation rules for target interval
    6. Cache results for future queries
    7. Return standardized DataFrames
    \"\"\"
    
    # Parameter validation
    self._validate_symbols(symbols)
    self._validate_date_range(start, end)
    
    # Parse interval
    target_minutes = self._parse_interval_to_minutes(interval)
    
    # Check cache
    cache_key = self._get_cache_key(symbols, start, end, interval)
    if cached_result := self._cache.get(cache_key):
        return cached_result
    
    # Process data
    result = {}
    for symbol in symbols:
        try:
            # Load base data
            base_data = await self._get_symbol_minute_data(symbol, start, end)
            
            # Aggregate if needed
            if target_minutes > 1:
                aggregated = self._aggregate_to_timeframe(base_data, target_minutes)
            else:
                aggregated = base_data
            
            result[symbol] = self._standardize_dataframe(aggregated)
            
        except Exception as e:
            logger.error(f\"Failed processing {symbol}: {e}\")
            result[symbol] = pd.DataFrame()  # Empty DataFrame for failed symbols
    
    # Cache and return
    self._cache.set(cache_key, result)
    return result
```

**Enhanced Signal Integration:**
```python
async def get_ohlc_with_signals(
    self,
    symbols: List[str],
    start: datetime,
    end: datetime,
    interval: str = '1m',
    signals: List[str] = None
) -> Dict[str, pd.DataFrame]:
    \"\"\"
    Implementation flow:
    1. Get base OHLC data using get_ohlc_for_interval()
    2. Apply signal computations to each symbol's data
    3. Handle signal computation failures gracefully
    4. Return enhanced DataFrames with signal columns
    \"\"\"
    
    # Get base OHLC data
    ohlc_data = await self.get_ohlc_for_interval(symbols, start, end, interval)
    
    # Default signal set
    if signals is None:
        signals = ['sma_20', 'ema_12', 'rsi_14', 'etop', 'ebot', 'pldot', 'vwap']
    
    # Compute signals for each symbol
    result = {}
    for symbol, df in ohlc_data.items():
        if df.empty:
            result[symbol] = df
            continue
            
        try:
            enhanced_df = self._compute_technical_signals(df.copy(), signals)
            result[symbol] = enhanced_df
            
        except Exception as e:
            logger.error(f\"Signal computation failed for {symbol}: {e}\")
            result[symbol] = df  # Return OHLC without signals
    
    return result
```

#### 2.5.2 Signal Computation Pipeline

```python
def _compute_technical_signals(self, df: pd.DataFrame, signals: List[str]) -> pd.DataFrame:
    \"\"\"
    Signal computation pipeline with error handling.
    
    Pipeline stages:
    1. Data validation and preprocessing
    2. Signal-specific computations in dependency order
    3. Post-processing and validation
    4. Error handling and fallback values
    \"\"\"
    
    # Stage 1: Validation
    if not self._validate_ohlc_data(df):
        logger.warning(\"Invalid OHLC data detected\")
    
    # Stage 2: Compute signals in dependency order
    signal_groups = {
        'moving_averages': ['sma_20', 'sma_50', 'ema_12', 'ema_26'],
        'oscillators': ['rsi_14', 'stoch_k', 'stoch_d'],
        'volatility': ['bb_upper', 'bb_middle', 'bb_lower', 'atr_14'],
        'custom': ['etop', 'ebot', 'pldot', 'vwap']
    }
    
    for group_name, group_signals in signal_groups.items():
        applicable_signals = [s for s in signals if s in group_signals]
        if applicable_signals:
            df = self._compute_signal_group(df, group_name, applicable_signals)
    
    # Stage 3: Post-processing
    df = self._validate_signal_values(df, signals)
    
    return df
```

## 3. Database and Storage Design

### 3.1 File Organization Structure

```
/mnt/d/ats-data/minute-bars/
├── firstrate/                    # Vendor-specific directories
│   ├── AAPL/                    # Symbol-specific directories  
│   │   ├── 2024/                # Year-based organization
│   │   │   ├── 01/              # Month-based organization
│   │   │   │   ├── AAPL_20240101.parquet
│   │   │   │   ├── AAPL_20240102.parquet
│   │   │   │   └── ...
│   │   │   └── ...
│   │   └── ...
│   └── ...
├── polygon/
└── tiingo/
```

### 3.2 Parquet Schema Specification

```python
MINUTE_OHLCV_SCHEMA = {
    'timestamp': 'datetime64[ns, UTC]',  # UTC timestamp
    'open': 'float64',                  # Opening price
    'high': 'float64',                  # Highest price
    'low': 'float64',                   # Lowest price
    'close': 'float64',                 # Closing price
    'volume': 'int64',                  # Volume traded
    'vwap': 'float64',                  # Optional: Volume weighted average price
    'trade_count': 'int64',             # Optional: Number of trades
    'vendor': 'string'                  # Optional: Data vendor identifier
}
```

### 3.3 Cache Storage Design

**In-Memory Cache:**
```python
class MemoryCache:
    \"\"\"
    LRU cache with memory limit and TTL support.
    
    Structure:
    - cache: OrderedDict for LRU ordering
    - timestamps: Dict for TTL tracking  
    - memory_usage: Running total of cached data size
    - max_memory: Maximum memory limit (default: 1GB)
    \"\"\"
    
    def __init__(self, max_memory_mb: int = 1024):
        self.cache = OrderedDict()
        self.timestamps = {}
        self.memory_usage = 0
        self.max_memory = max_memory_mb * 1024 * 1024  # Convert to bytes
```

## 4. Testing Strategy

### 4.1 Unit Testing Requirements

**OHLC Aggregation Tests:**
```python
class TestOHLCAggregation:
    def test_interval_parsing(self):
        \"\"\"Test interval notation parsing accuracy.\"\"\"
        
    def test_aggregation_rules(self):
        \"\"\"Test OHLC aggregation mathematical correctness.\"\"\"
        
    def test_edge_cases(self):
        \"\"\"Test handling of missing data, single-bar periods, etc.\"\"\"
```

**Signal Computation Tests:**
```python
class TestTechnicalSignals:
    def test_sma_calculation(self):
        \"\"\"Verify SMA matches manual calculation.\"\"\"
        
    def test_rsi_boundary_conditions(self):
        \"\"\"Test RSI stays within 0-100 range.\"\"\"
        
    def test_custom_signals(self):
        \"\"\"Test ETOP, EBOT, PLDOT calculations.\"\"\"
```

### 4.2 Integration Testing

**End-to-End Data Flow:**
```python
class TestDataFlowIntegration:
    async def test_full_pipeline(self):
        \"\"\"
        Test complete flow from parquet files to training data.
        
        1. Load test parquet files
        2. Call multi-timeframe API
        3. Verify signal computations
        4. Generate training examples
        5. Validate output format
        \"\"\"
```

### 4.3 Performance Testing

**Benchmark Test Cases:**
```python
class TestPerformance:
    async def test_single_symbol_latency(self):
        \"\"\"Measure API response time for single symbol queries.\"\"\"
        
    async def test_batch_processing_throughput(self):
        \"\"\"Measure throughput for multi-symbol batch requests.\"\"\"
        
    async def test_memory_usage(self):
        \"\"\"Monitor memory usage during large dataset processing.\"\"\"
```

## 5. Monitoring and Observability

### 5.1 Performance Metrics

**API Metrics:**
- Request latency (p50, p95, p99)
- Request throughput (requests/second)
- Error rates by endpoint
- Cache hit/miss rates

**Resource Metrics:**
- Memory usage per request
- CPU utilization during processing
- Disk I/O rates for file operations
- Network I/O for distributed processing

### 5.2 Business Metrics

**Data Quality Metrics:**
- Signal computation success rates
- Data validation failure rates
- Missing data percentage by symbol
- Processing completeness rates

**Usage Metrics:**
- Most requested intervals
- Most computed signals
- Training data generation frequency
- Peak usage patterns

### 5.3 Alerting Strategy

**Critical Alerts:**
- API error rate > 1%
- Response time > 10 seconds
- Memory usage > 80% of limit
- Cache failure conditions

**Warning Alerts:**
- Response time > 5 seconds
- Cache hit rate < 50%
- Data quality issues detected
- Unusual usage patterns

## 6. Deployment Considerations

### 6.1 Environment Configuration

**Development Environment:**
- Local parquet files for testing
- In-memory cache for development
- Debug logging enabled
- Performance profiling tools

**Production Environment:**
- High-performance SSD storage for parquet files
- Redis cache for distributed caching
- Optimized logging levels
- Production monitoring and alerting

### 6.2 Scaling Strategy

**Horizontal Scaling:**
- Multiple instances for high availability
- Load balancing across instances
- Distributed caching with Redis cluster
- Database connection pooling

**Vertical Scaling:**
- Memory optimization for large datasets
- CPU optimization for signal computations
- Storage optimization for file access
- Network optimization for batch operations

## 7. Security Considerations

### 7.1 Data Access Security

**Authentication & Authorization:**
- API key authentication for external access
- Role-based permissions for different user types
- Audit logging for all data access
- Rate limiting to prevent abuse

**Data Protection:**
- Encryption in transit for all API calls
- Secure storage of cached data
- Data retention policies for cached results
- Compliance with financial data regulations

### 7.2 Operational Security

**System Hardening:**
- Regular security updates for dependencies
- Secure configuration of cache systems
- Network security for distributed components
- Monitoring for security anomalies

## 8. Conclusion

This DRD provides comprehensive technical specifications for implementing a robust, scalable, and performant multi-timeframe OHLC and signal computation system. The design emphasizes:

- **Mathematical Correctness**: Accurate implementation of technical indicators
- **Performance Optimization**: Efficient processing of large datasets
- **Extensibility**: Easy addition of new signals and intervals
- **Reliability**: Robust error handling and data validation
- **Maintainability**: Clean architecture and comprehensive testing

The implementation will provide a solid foundation for machine learning training data generation while maintaining the flexibility to support additional use cases in analytics and trading systems.