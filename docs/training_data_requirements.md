# Training Data Requirements Document (TRD)

## Overview
This document specifies the comprehensive requirements for multi-timeframe training data generation with proper technical indicators and data structure.

## 1. Technical Indicator Requirements

### 1.1 Indicator Scaling Requirements
- **NO NORMALIZATION**: All technical indicators must return actual values, NOT normalized to [-1, 1] range
- **EnvelopeTop/EnvelopeBot**: Must return actual price levels (e.g., 100.5 for $100.50 stock)
- **PLDOT**: Must return actual momentum values, not normalized
- **OneOneHigh/OneOneLow**: Must return actual price levels
- **Z1B, Z2B, Z5T, Z6T**: Must return actual calculated values without scaling

### 1.2 Missing Indicators to Implement
Current training data is missing these critical indicators:
- **Z1B**: Zone 1 Buy signal indicator
- **Z2B**: Zone 2 Buy signal indicator  
- **Z5T**: Zone 5 Top signal indicator
- **Z6T**: Zone 6 Top signal indicator

These must be added to complete the 15-indicator system.

## 2. Multi-Timeframe Data Structure

### 2.1 Primary Data Structure
- **Base Interval**: One row per hour (hourly aggregation)
- **Each hourly row contains**:
  - Hourly OHLCV data
  - Daily OHLCV data (for the containing day)
  - Weekly OHLCV data (for the containing week)
  - ALL 15 technical indicators calculated at each timeframe

### 2.2 Multi-Timeframe Indicators
For each hourly row, calculate indicators at multiple timeframes:
- **Hourly indicators**: EnvelopeTop, EnvelopeBot, PLDOT, OneOneHigh, OneOneLow, Z1B, Z2B, Z5T, Z6T
- **Daily indicators**: Same indicators calculated on daily timeframe
- **Weekly indicators**: Same indicators calculated on weekly timeframe

### 2.3 Short-Term Sequence Data
Each hourly row must also include sequences of recent intervals:
- **Last 10 5-minute intervals**: OHLCV + all technical indicators
- **Last 10 15-minute intervals**: OHLCV + all technical indicators

## 3. Data Schema Structure

### 3.1 Hourly Row Schema
```python
{
    "timestamp": "2025-08-31T14:00:00",  # Hour timestamp
    
    # Hourly OHLCV
    "hour_open": 150.25,
    "hour_high": 152.80,
    "hour_low": 149.90,
    "hour_close": 151.75,
    "hour_volume": 125000,
    
    # Daily OHLCV (containing day)
    "day_open": 148.50,
    "day_high": 153.20,
    "day_low": 147.80,
    "day_close": 151.75,  # Current if day not complete
    "day_volume": 2500000,
    
    # Weekly OHLCV (containing week)
    "week_open": 145.00,
    "week_high": 155.50,
    "week_low": 144.20,
    "week_close": 151.75,  # Current if week not complete
    "week_volume": 15000000,
    
    # Hourly Technical Indicators
    "hour_envelope_top": 152.45,
    "hour_envelope_bot": 149.25,
    "hour_pldot": 0.0125,
    "hour_oneone_high": 152.80,
    "hour_oneone_low": 149.90,
    "hour_z1b": 1.0,
    "hour_z2b": 0.0,
    "hour_z5t": 0.0,
    "hour_z6t": 1.0,
    
    # Daily Technical Indicators  
    "day_envelope_top": 153.85,
    "day_envelope_bot": 147.15,
    "day_pldot": 0.0089,
    # ... all daily indicators
    
    # Weekly Technical Indicators
    "week_envelope_top": 156.20,
    "week_envelope_bot": 143.80,
    "week_pldot": 0.0156,
    # ... all weekly indicators
    
    # 5-minute sequence (last 10 intervals = 50 minutes)
    "minute5_sequence": [
        {
            "timestamp": "2025-08-31T13:55:00",
            "open": 151.20, "high": 151.60, "low": 151.05, "close": 151.45, "volume": 8500,
            "envelope_top": 151.65, "envelope_bot": 151.00, "pldot": 0.0012,
            # ... all indicators for this 5-min interval
        },
        # ... 9 more 5-minute intervals
    ],
    
    # 15-minute sequence (last 10 intervals = 150 minutes)  
    "minute15_sequence": [
        {
            "timestamp": "2025-08-31T13:45:00",
            "open": 150.80, "high": 151.80, "low": 150.60, "close": 151.45, "volume": 25500,
            "envelope_top": 151.85, "envelope_bot": 150.55, "pldot": 0.0089,
            # ... all indicators for this 15-min interval
        },
        # ... 9 more 15-minute intervals
    ]
}
```

## 4. Implementation Requirements

### 4.1 Indicator Implementation
- Fix existing indicators to remove any normalization
- Implement missing Z1B, Z2B, Z5T, Z6T indicators
- Add comprehensive unit tests for each indicator
- Verify all indicators return actual values, not normalized

### 4.2 Multi-Timeframe Data Collection
- Implement FileBasedMinuteManager integration for real minute data
- Add aggregation logic for 5min, 15min, hourly, daily, weekly timeframes
- Implement indicator calculation at each timeframe
- Add sequence collection for recent 5min and 15min intervals

### 4.3 Training Data Pipeline
- Update TrainingDataJobRunner to use multi-timeframe structure
- Implement hourly-based training data generation
- Add proper feature engineering for sequence data
- Implement data validation for multi-timeframe consistency

## 5. Testing Requirements

### 5.1 Unit Tests Required
- Indicator scaling tests (ensure no normalization)
- Multi-timeframe aggregation tests
- Sequence data collection tests
- Data structure validation tests

### 5.2 Integration Tests Required  
- End-to-end training data generation with real market data
- Multi-symbol training data generation
- Performance tests with large datasets
- Data quality validation across all timeframes

## 6. Validation Criteria

### 6.1 Technical Indicators
- ✅ **COMPLETED**: EnvelopeTop/Bot return price levels (e.g., 117.68-137.12, not 0.75)
- ✅ **COMPLETED**: PLDOT returns actual momentum (e.g., 116.48-135.56, not 0.5)
- ✅ **COMPLETED**: All 9 core indicators implemented and tested (EnvelopeTop, EnvelopeBot, PLDOT, OneOneHigh, OneOneLow, Z1B, Z2B, Z5T, Z6T)
- ✅ **COMPLETED**: No indicator values between 0 and 1 (all return actual price/zone values)

### 6.2 Data Structure
- ✅ Each hour produces exactly one training row
- ✅ Each row contains hourly, daily, weekly OHLCV + indicators
- ✅ Each row contains 10x 5-minute and 10x 15-minute sequences
- ✅ All timeframes are properly aligned and consistent

### 6.3 Performance
- ✅ Generate 1000+ hourly rows per minute of processing time
- ✅ Support multiple symbols simultaneously
- ✅ Efficient memory usage for large datasets
- ✅ Proper error handling and data validation

## 7. Implementation Status (2025-08-31)

### 7.1 COMPLETED ✅
1. **Fixed Indicator Scaling**: All indicators now return actual values instead of normalized [0,1] values
   - EnvelopeTop/Bot: Return actual price levels (117.68-137.12 range)
   - PLDOT: Returns actual momentum values (116.48-135.56 range) 
   - OneOneHigh/Low: Return actual price levels
   - Z1B, Z2B, Z5T, Z6T: All implemented and return actual zone values
   
2. **Added Missing Indicators**: All 9 core indicators now included in training data generation
   - Previously missing Z1B, Z2B, Z5T, Z6T are now properly integrated
   - Total indicators: 5 OHLCV + 9 technical = 14 features per training sample

3. **Proper Indicator Implementation**: Replaced normalized calculation methods with actual indicator classes
   - Uses proper `src/signals/indicator.py` classes instead of normalized helper methods
   - All indicators validated with unit tests showing non-normalized outputs

### 7.2 PENDING IMPLEMENTATION 🚧

#### 7.2.1 Multi-Timeframe Architecture (Major Change Required)
- **Current**: Sequence-based training data (60-day sequences)
- **Required**: Hourly-row based with embedded multi-timeframe data
- **Challenge**: Requires access to minute-level data for aggregation

#### 7.2.2 Required Components for Multi-Timeframe:
1. **FileBasedMinuteManager Integration**: Access to minute OHLCV data
2. **Timeframe Aggregation Engine**: 
   - 5-minute intervals aggregation
   - 15-minute intervals aggregation  
   - Hourly aggregation
   - Daily aggregation (existing)
   - Weekly aggregation
3. **Multi-Timeframe Indicator Calculator**: Calculate all 9 indicators at each timeframe
4. **Sequence Data Embedder**: Embed recent 10x5min and 10x15min sequences in each hourly row

#### 7.2.3 Data Schema Transformation Required:
```python
# Current: Sequence-based
features.shape = (197, 60, 14)  # 197 sequences, 60 days, 14 features

# Required: Hourly-row based  
hourly_row = {
    "timestamp": "2025-08-31T14:00:00",
    "hour_ohlcv": {...},     # 5 fields
    "day_ohlcv": {...},      # 5 fields  
    "week_ohlcv": {...},     # 5 fields
    "hour_indicators": {...}, # 9 fields
    "day_indicators": {...},  # 9 fields
    "week_indicators": {...}, # 9 fields
    "minute5_sequence": [...], # 10 × (5 OHLCV + 9 indicators) = 140 fields
    "minute15_sequence": [...] # 10 × (5 OHLCV + 9 indicators) = 140 fields
}
# Total: 322 fields per hourly row
```

## 8. Implementation Roadmap

### Phase 1: COMPLETED ✅ (2025-08-31)
- ✅ Fix indicator scaling normalization
- ✅ Add missing Z indicators  
- ✅ Validate all indicators return actual values
- ✅ Update training data generation to use proper indicator classes

### Phase 2: Multi-Timeframe Infrastructure (Estimated: 2-3 weeks)
1. **FileBasedMinuteManager Integration** (Week 1)
   - Access minute-level parquet files  
   - Implement minute data loading for training date ranges
   - Add proper timezone handling and market hours filtering

2. **Timeframe Aggregation Engine** (Week 1-2) 
   - Implement OHLCV aggregation: minute → 5min, 15min, hour, day, week
   - Add proper volume aggregation and timestamp alignment
   - Handle market holidays and gaps in data

3. **Multi-Timeframe Training Generator** (Week 2-3)
   - Redesign training data generator for hourly-row approach
   - Calculate indicators at all timeframes for each row
   - Implement sequence embedding for 5min/15min intervals
   - Add comprehensive validation and testing

### Phase 3: Validation and Optimization (Week 3-4)
- Comprehensive test suite for multi-timeframe data consistency
- Performance optimization for large-scale data generation
- Data quality validation across all timeframes
- Integration testing with existing ML pipeline

## 9. Success Metrics

- **✅ ACHIEVED**: All 9 indicators return actual values (validated 2025-08-31)
- **🚧 PENDING**: Data Coverage: 24 hours/day × 5 days/week of hourly training rows
- **🚧 PENDING**: Indicator Coverage: All 9 indicators at 3 timeframes (27 indicator values per row)  
- **🚧 PENDING**: Sequence Coverage: 10 intervals × 2 timeframes × full indicators (280 sequence values per row)
- **🚧 PENDING**: Data Quality: >99% valid rows with all required fields populated
- **🚧 PENDING**: Performance: Process 1 symbol × 1 year of data in <10 minutes

**Current Achievement**: Core indicator normalization issues resolved. Multi-timeframe architecture requires significant additional development.