# PRD: Multi-Timeframe OHLC and Signal States Computation System

**Document Version:** 1.1  
**Date:** 2025-09-02  
**Author:** ATS Development Team  
**Status:** Implementation Complete - Indicator Builder Integration  

## Executive Summary

This document defines the requirements for a comprehensive multi-timeframe OHLC (Open-High-Low-Close) aggregation and technical signal computation system within the ATS platform. The system provides standardized APIs for converting 1-minute base market data into multiple timeframes with computed technical indicators for machine learning training data generation.

## 1. Problem Statement

### Current Challenges
- **Fragmented OHLC Processing**: Ad-hoc aggregation logic scattered across different components
- **Inconsistent Signal Computation**: Technical indicators computed differently in various modules
- **No Standardized Intervals**: Different systems use different interval notation
- **Manual Aggregation**: Training data generation requires manual timeframe aggregation
- **Limited Signal Coverage**: Missing key technical indicators needed for ML models

### Business Impact
- **Development Inefficiency**: Duplicate aggregation logic increases maintenance burden
- **Data Inconsistency**: Different aggregation methods produce inconsistent training data
- **Limited ML Features**: Insufficient technical signals reduce model performance
- **Integration Complexity**: Complex integration between market data and training systems

## 2. Solution Overview

### Core Components
1. **Enhanced FileBasedMinuteMarketDataManager**: Centralized OHLC aggregation with indicator builder integration
2. **Indicator Builder System**: Unified technical signal computation using existing indicator infrastructure
3. **Gin-Configurable Indicators**: Flexible indicator configuration per timeframe via Gin dependency injection
4. **OHLCV to InstrumentInterval Adapter**: Bridge between DataFrame data and indicator system
5. **Standardized Interval API**: Uniform interval notation ('1m', '5m', '15m', '1h', '1d', '1w')
6. **Multi-Timeframe Data Retrieval**: Single API call for multiple timeframes with signals
7. **Training Data Integration**: Seamless integration with training data generation callbacks

### Key Benefits
- **Unified Architecture**: Leverages existing indicator builder system for consistent signal computation
- **Gin Configuration**: Flexible indicator selection per timeframe through dependency injection
- **InstrumentInterval Compatibility**: Seamless integration with existing indicator classes
- **Standardized APIs**: Consistent interval notation and data formats
- **Comprehensive Signals**: Full suite of technical indicators via modular indicator classes
- **Performance Optimization**: Efficient caching and batch processing
- **Easy Integration**: Simple APIs for training data generation and analysis
- **Extensibility**: Easy addition of new indicators through the indicator builder system

## 3. Functional Requirements

### 3.1 OHLC Aggregation Requirements

#### FR-OHLC-001: Interval-Based OHLC Aggregation
- **Requirement**: System SHALL aggregate 1-minute OHLC data to standard intervals
- **Intervals**: '1m', '5m', '15m', '30m', '1h', '2h', '4h', '1d', '1w', '1M'
- **Aggregation Rules**:
  - **Open**: First open price of the period
  - **High**: Maximum high price of the period  
  - **Low**: Minimum low price of the period
  - **Close**: Last close price of the period
  - **Volume**: Sum of volume for the period
- **Data Integrity**: No data loss during aggregation process
- **Performance**: Aggregation SHALL complete within 2 seconds for 10,000 1-minute bars

#### FR-OHLC-002: Standardized Interval Notation
- **Requirement**: System SHALL support standard interval string notation
- **Format Examples**:
  - Minutes: '1m', '5m', '15m', '30m'
  - Hours: '1h', '2h', '4h', '6h', '12h'
  - Days: '1d', '2d', '3d'
  - Weeks: '1w', '2w'
  - Months: '1M', '3M'
- **Validation**: Invalid interval strings SHALL raise ValueError with clear message
- **Conversion**: Internal conversion to minutes for processing

#### FR-OHLC-003: Multi-Symbol Batch Processing
- **Requirement**: System SHALL process multiple symbols in single API call
- **Batch Size**: Support up to 100 symbols per batch
- **Error Handling**: Individual symbol failures SHALL NOT affect other symbols in batch
- **Return Format**: Dictionary mapping symbol to OHLC DataFrame
- **Performance**: Batch processing SHALL be 50% faster than individual symbol calls

### 3.2 Indicator Builder Integration Requirements

#### FR-INDICATOR-001: Unified Indicator System
- **Requirement**: System SHALL use the existing indicator builder for all technical signal computation
- **Architecture**: FileBasedMinuteMarketDataManager integrates with IndicatorBuilder, IndicatorConfig, and existing indicator classes
- **Data Flow**: OHLCV DataFrames converted to InstrumentInterval objects for indicator processing
- **Configuration**: Gin dependency injection determines which indicators are computed for each timeframe
- **Backward Compatibility**: Existing indicator classes (PL, EnvelopeTop, EnvelopeBot, etc.) work without modification

#### FR-INDICATOR-002: Gin-Configurable Indicator Sets
- **Requirement**: Each timeframe SHALL have configurable indicator sets via Gin configuration files
- **Timeframe Mappings**:
  - '1m': Basic indicators (OneOneDot, OneOneHigh, OneOneLow) for high-frequency data
  - '5m', '15m', '30m': Multi-timeframe indicators (SMA, EMA, RSI, ETOP, EBOT, PLDOT)  
  - '1h', '2h', '4h', '1d', '1w': Standard technical indicators (SMA, EMA, RSI, VWAP, Bollinger Bands, MACD, Stochastic)
- **Flexibility**: Indicators can be added/removed per timeframe through configuration changes
- **Runtime Configuration**: Gin configs allow indicator parameters to be modified without code changes

#### FR-INDICATOR-003: OHLCV to InstrumentInterval Adapter
- **Requirement**: System SHALL provide seamless conversion between pandas DataFrames and InstrumentInterval objects
- **Adapter Functionality**: 
  - Convert OHLCV DataFrame rows to InstrumentInterval objects
  - Handle timestamp conversion and timezone normalization
  - Support multi-timeframe data conversion
  - Provide indicator computation integration methods
- **Error Handling**: Graceful handling of missing data, invalid timestamps, and conversion failures
- **Performance**: Efficient conversion with minimal memory overhead

### 3.3 Technical Signal Requirements

#### FR-SIGNAL-001: Moving Averages
- **Simple Moving Average (SMA)**: Configurable periods (default: 20, 50, 200)
- **Exponential Moving Average (EMA)**: Configurable spans (default: 12, 26, 50)
- **Computation**: Rolling calculations with minimum periods = 1
- **Edge Cases**: Handle insufficient data gracefully with NaN values

#### FR-SIGNAL-002: Oscillators
- **RSI (Relative Strength Index)**: 
  - Default period: 14
  - Range: 0-100
  - Computation: Standard RSI formula with Wilder's smoothing
- **Stochastic Oscillator**: %K and %D lines with configurable periods
- **Williams %R**: Momentum indicator with configurable lookback

#### FR-SIGNAL-003: Volatility Indicators
- **Bollinger Bands**:
  - Upper Band: SMA(20) + 2 * StdDev(20)
  - Middle Band: SMA(20)
  - Lower Band: SMA(20) - 2 * StdDev(20)
- **Average True Range (ATR)**: 14-period default
- **Volatility**: Standard deviation of returns

#### FR-SIGNAL-004: Custom ATS Signals
- **ETOP (Envelope Top)**: SMA(20) * (1 + 5%) - Upper envelope line
- **EBOT (Envelope Bottom)**: SMA(20) * (1 - 5%) - Lower envelope line  
- **PLDOT (Pivot Line Dot)**: (High + Low + Close) / 3 - Daily pivot point
- **VWAP**: Volume Weighted Average Price - Intraday benchmark

#### FR-SIGNAL-005: Signal State Management
- **State Persistence**: Signal values SHALL be consistent across calls
- **Incremental Updates**: New data SHALL update existing signals incrementally
- **Missing Data Handling**: NaN/null values handled appropriately
- **Signal Validation**: Computed values SHALL be within expected ranges

### 3.3 API Requirements

#### FR-API-001: Primary OHLC API
```python
async def get_ohlc_for_interval(
    symbols: List[str],
    start: datetime,
    end: datetime, 
    interval: str = '1m'
) -> Dict[str, pd.DataFrame]
```
- **Parameters**: Symbols list, date range, interval string
- **Returns**: Dictionary mapping symbol to OHLC DataFrame
- **Performance**: < 1 second for single symbol, single day
- **Error Handling**: Descriptive exceptions for invalid parameters

#### FR-API-002: Enhanced OHLC with Signals API  
```python
async def get_ohlc_with_signals(
    symbols: List[str],
    start: datetime,
    end: datetime,
    interval: str = '1m',
    signals: List[str] = None
) -> Dict[str, pd.DataFrame]
```
- **Parameters**: Symbols, date range, interval, signal list (deprecated)
- **Indicator Selection**: Indicators determined by Gin configuration for each timeframe
- **Returns**: DataFrame with OHLCV + indicator columns based on configured indicator set
- **Flexibility**: Indicators configurable through Gin files without code changes
- **Backward Compatibility**: signals parameter maintained for compatibility but indicators determined by configuration

#### FR-API-003: Multi-Timeframe API
```python
async def get_multi_timeframe_data(
    symbols: List[str],
    start: datetime,
    end: datetime,
    intervals: List[str] = None,
    signals: List[str] = None  
) -> Dict[str, Dict[str, pd.DataFrame]]
```
- **Parameters**: Symbols, date range, intervals list, signals list
- **Default Intervals**: ['5m', '15m', '1h', '1d', '1w']
- **Returns**: Nested dict {symbol: {interval: DataFrame}}
- **Use Case**: Training data generation with multiple timeframes

## 4. Non-Functional Requirements

### 4.1 Performance Requirements
- **Latency**: API calls SHALL complete within 5 seconds for typical use cases
- **Throughput**: System SHALL handle 100 concurrent API calls
- **Memory**: Memory usage SHALL NOT exceed 2GB during processing
- **Caching**: Frequently accessed data SHALL be cached for 1 hour

### 4.2 Reliability Requirements
- **Availability**: System SHALL be available 99.9% of the time
- **Error Recovery**: Graceful handling of data corruption or missing files
- **Data Integrity**: Checksums and validation for all processed data
- **Logging**: Comprehensive logging for debugging and monitoring

### 4.3 Scalability Requirements
- **Data Volume**: Support processing of 1TB+ historical minute data
- **Symbol Count**: Support 10,000+ unique symbols
- **Time Range**: Support 20+ years of historical data
- **Concurrent Users**: Support 50 concurrent training data generation jobs

### 4.4 Maintainability Requirements
- **Code Quality**: 90%+ test coverage for all signal computation functions
- **Documentation**: Comprehensive API documentation with examples
- **Monitoring**: Performance metrics and health checks
- **Extensibility**: Easy addition of new technical indicators

## 5. Data Requirements

### 5.1 Input Data Format
- **Source**: 1-minute OHLCV data in Parquet format
- **Schema**: timestamp, open, high, low, close, volume
- **Quality**: Clean data with no gaps or anomalies
- **Timezone**: All timestamps in UTC

### 5.2 Output Data Format
- **OHLC DataFrames**: Standardized column names and types
- **Signal Columns**: Consistent naming convention (e.g., 'sma_20', 'ema_12')
- **Data Types**: float64 for prices, int64 for volume, datetime64 for timestamps
- **Missing Values**: NaN for insufficient data periods

### 5.3 Storage Requirements
- **Caching**: Redis/in-memory cache for frequently accessed data
- **Temporary Files**: Efficient cleanup of intermediate processing files
- **Backup**: No backup required (source data is authoritative)

## 6. Integration Requirements

### 6.1 Training Data Integration
- **Callback Integration**: Seamless integration with IntervalBasedTrainingDataCallback
- **Feature Generation**: Direct API calls for multi-timeframe feature extraction
- **Data Consistency**: Same aggregation logic across all training data generation

### 6.2 Analytics Integration
- **Dashboard APIs**: Support for real-time analytics dashboards
- **Batch Processing**: Integration with scheduled data processing jobs
- **Export APIs**: Support for data export to external systems

## 7. Security Requirements

### 7.1 Data Access
- **Authorization**: Role-based access to market data APIs
- **Audit Logging**: Log all API calls with user identification
- **Rate Limiting**: Prevent abuse of computational resources

### 7.2 Data Protection
- **Encryption**: Data in transit encrypted via HTTPS
- **Access Control**: Restrict access to authorized applications only
- **Compliance**: Adhere to financial data handling regulations

## 8. Success Metrics

### 8.1 Performance Metrics
- **API Response Time**: 95th percentile < 2 seconds
- **Cache Hit Rate**: > 80% for frequently accessed data
- **Error Rate**: < 0.1% of API calls
- **Memory Efficiency**: < 1GB memory usage per processing job

### 8.2 Quality Metrics
- **Signal Accuracy**: 100% mathematical correctness for standard indicators
- **Data Completeness**: > 99.9% successful processing of valid input data
- **Test Coverage**: > 95% code coverage for all components
- **Documentation Coverage**: 100% of public APIs documented

### 8.3 Business Metrics
- **Development Efficiency**: 50% reduction in custom aggregation code
- **Training Data Quality**: 25% improvement in ML model performance
- **Integration Speed**: New signal additions completed in < 1 day
- **System Reliability**: 99.9% uptime for production deployments

## 9. Implementation Timeline

### Phase 1: Core OHLC Aggregation (Week 1-2)
- Enhanced FileBasedMinuteMarketDataManager with interval APIs
- Standard interval notation support
- Basic aggregation functionality with comprehensive testing

### Phase 2: Technical Signals Library (Week 3-4)  
- Implementation of all standard technical indicators
- Custom ATS signals (ETOP, EBOT, PLDOT)
- Signal computation optimization and caching

### Phase 3: Training Data Integration (Week 5)
- Update IntervalBasedTrainingDataCallback to use new APIs
- Multi-timeframe data retrieval integration
- End-to-end testing with training data generation

### Phase 4: Performance & Documentation (Week 6)
- Performance optimization and caching implementation
- Comprehensive API documentation
- Production deployment and monitoring setup

## 10. Risk Assessment

### High Risk Items
- **Performance**: Large dataset processing may exceed performance requirements
- **Data Quality**: Inconsistent source data may affect signal computation accuracy
- **Integration Complexity**: Breaking changes may impact existing training workflows

### Mitigation Strategies
- **Performance Testing**: Comprehensive benchmarking with production-scale data
- **Data Validation**: Robust input validation and error handling
- **Backward Compatibility**: Maintain existing APIs during transition period

## 11. Appendix

### A. Supported Technical Indicators
| Indicator | Parameters | Description |
|-----------|------------|-------------|
| SMA | period=20 | Simple Moving Average |
| EMA | span=12 | Exponential Moving Average |
| RSI | period=14 | Relative Strength Index |
| Bollinger Bands | period=20, std=2 | Volatility bands |
| ETOP | period=20, pct=5% | Envelope top line |
| EBOT | period=20, pct=5% | Envelope bottom line |
| PLDOT | - | Pivot line dot |
| VWAP | - | Volume weighted average price |

### B. Interval Notation Reference
| Notation | Minutes | Description |
|----------|---------|-------------|
| 1m | 1 | 1-minute bars |
| 5m | 5 | 5-minute bars |
| 15m | 15 | 15-minute bars |
| 30m | 30 | 30-minute bars |
| 1h | 60 | 1-hour bars |
| 2h | 120 | 2-hour bars |
| 4h | 240 | 4-hour bars |
| 1d | 1440 | Daily bars |
| 1w | 10080 | Weekly bars |
| 1M | 43800 | Monthly bars (approx) |

### C. API Usage Examples
```python
# Basic OHLC aggregation
data = await manager.get_ohlc_for_interval(['AAPL'], start, end, '5m')

# OHLC with signals
data = await manager.get_ohlc_with_signals(['AAPL'], start, end, '1h', ['sma_20', 'rsi_14'])

# Multi-timeframe data
data = await manager.get_multi_timeframe_data(['AAPL'], start, end, ['5m', '1h', '1d'])
```