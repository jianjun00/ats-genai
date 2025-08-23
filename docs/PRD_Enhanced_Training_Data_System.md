# PRD: Enhanced Multi-Timeframe Training Data System

## Executive Summary

This PRD outlines the enhancement of our training data generation system to support **typed features with multi-timeframe OHLC intervals** and **cross-timeframe technical indicators**. The system will generate one comprehensive training dataset per run with rich metadata and support advanced multi-timeframe visualization.

## Business Objectives

### Primary Goals
1. **Simplify Data Management**: One training dataset per run instead of multiple fragmented datasets
2. **Rich Feature Typing**: Explicit feature types enabling intelligent visualization and analysis
3. **Multi-Timeframe Analysis**: Support 5min, 15min, 1hour, daily, weekly, monthly timeframes
4. **Advanced Visualization**: Comprehensive chart overlays with cross-timeframe indicators
5. **Enhanced ML Pipeline**: Better feature engineering for improved model performance

### Success Metrics
- Training data generation time reduced by 40%
- Feature interpretability increased via typed metadata
- Model performance improved through richer feature sets
- Developer productivity enhanced via better visualization tools

## Core Requirements

### 1. Single Dataset Per Run Architecture
- **Current**: Multiple datasets per training run
- **Enhanced**: One comprehensive dataset containing all features and targets
- **Benefit**: Simplified data management, consistent versioning, atomic operations

### 2. Typed Feature System

#### 2.1 OHLC Interval Features
```
Feature Type: OHLC_INTERVALS
Dimension: [time_steps, 4]  # time_steps × [Open, High, Low, Close]
Timeframes: 5min, 15min, 1hour, daily, weekly, monthly
Intervals: 8, 16, 32 (configurable)
```

**Example**:
- `ohlc_5min_8` = 8 time steps of 5-minute OHLC data (shape: [8, 4])
- `ohlc_1hour_16` = 16 time steps of 1-hour OHLC data (shape: [16, 4])

#### 2.2 Technical Indicator Intervals
```
Feature Type: PRICE_INDICATOR_INTERVALS  
Dimension: [time_steps, 1]  # time_steps × indicator_value
Indicators: ETOP, EBOT, PLDOT, EMA, RSI, MACD
Timeframes: 5min, 15min, 1hour, daily
Intervals: 8, 16, 32 (configurable)
```

**Example**:
- `etop_5min_8` = 8 time steps of 5-minute ETOP values (shape: [8, 1])
- `pldot_1hour_16` = 16 time steps of 1-hour PLDOT values (shape: [16, 1])

#### 2.3 Cross-Timeframe Features
```
Feature Type: CROSS_TIMEFRAME_INDICATORS
Purpose: Overlay longer timeframe indicators on shorter timeframe data
```

**Example**:
- `etop_1hour_on_5min` = 1-hour ETOP values aligned to 5-minute intervals
- `pldot_daily_on_15min` = Daily PLDOT values aligned to 15-minute intervals

### 3. Enhanced Database Schema

#### 3.1 Training Dataset Metadata
```sql
-- Enhanced fields in dev_training_dataset table
feature_type_mapping: jsonb     -- Maps feature names to types
timeframe_metadata: jsonb       -- Timeframe specifications per feature  
interval_configurations: jsonb  -- Time step configurations
cross_timeframe_mappings: jsonb -- Cross-timeframe alignment rules
visualization_hints: jsonb      -- Rendering hints for frontend
```

#### 3.2 Feature Type Registry
```json
{
  "ohlc_5min_8": {
    "type": "OHLC_INTERVALS",
    "timeframe": "5min", 
    "intervals": 8,
    "dimensions": [8, 4],
    "visualization": "candlestick_sequence"
  },
  "etop_15min_16": {
    "type": "PRICE_INDICATOR_INTERVALS",
    "timeframe": "15min",
    "intervals": 16, 
    "indicator": "ETOP",
    "dimensions": [16, 1],
    "visualization": "line_overlay"
  }
}
```

### 4. Multi-Timeframe Visualization Requirements

#### 4.1 Comprehensive Chart Views
For each training example, provide **6 synchronized chart views**:

1. **Monthly Chart** (12 months) - Major trend analysis
2. **Weekly Chart** (52 weeks) - Intermediate trend analysis  
3. **Daily Chart** (252 days) - Daily price action
4. **1-Hour Chart** (24×30 = 720 hours) - Intraday analysis
5. **15-Minute Chart** (4×24×10 = 960 intervals) - Short-term patterns
6. **5-Minute Chart** (12×24×5 = 1440 intervals) - Micro patterns

#### 4.2 Cross-Timeframe Overlays
- **1-hour indicators** overlaid on **5-minute charts**
- **Daily indicators** overlaid on **15-minute charts**  
- **Weekly indicators** overlaid on **daily charts**
- **Synchronized crosshairs** across all timeframes

#### 4.3 Technical Indicator Visualization
```
Chart Elements:
├── OHLC Candlesticks (primary data)
├── ETOP Line (envelope top - resistance)
├── EBOT Line (envelope bottom - support) 
├── PLDOT Points (pivot dots - key levels)
├── EMA Lines (exponential moving average)
├── Volume Bars (secondary axis)
└── Cross-timeframe indicator overlays
```

## User Stories

### Data Scientist User Stories
1. **As a data scientist**, I want to generate one comprehensive training dataset per experiment so that I can manage data versions atomically
2. **As a data scientist**, I want typed features with metadata so that I can understand feature semantics without guessing
3. **As a data scientist**, I want multi-timeframe OHLC data so that I can capture patterns across different time horizons

### Model Developer User Stories  
1. **As a model developer**, I want 2D OHLC feature matrices so that I can use CNN/RNN architectures effectively
2. **As a model developer**, I want cross-timeframe indicators so that I can model multi-scale market dynamics
3. **As a model developer**, I want feature type metadata so that I can apply appropriate preprocessing automatically

### Analyst User Stories
1. **As an analyst**, I want multi-timeframe visualization so that I can understand training examples across all relevant time scales
2. **As an analyst**, I want cross-timeframe overlays so that I can see how longer-term patterns influence shorter-term movements
3. **As an analyst**, I want synchronized chart navigation so that I can explore the same time period across different timeframes

## Technical Architecture Overview

### Backend Architecture
```
Training Data Generator
├── Feature Type Registry (manages all feature types)
├── Multi-Timeframe Data Collector (OHLC + indicators)  
├── Cross-Timeframe Aligner (synchronizes different intervals)
├── Typed Feature Assembler (creates typed feature matrices)
├── Metadata Generator (creates comprehensive metadata)
└── Single Dataset Packager (bundles everything together)
```

### Frontend Architecture  
```
Training Data Visualizer
├── Multi-Timeframe Chart Manager (6 synchronized charts)
├── Cross-Timeframe Overlay Engine (indicator alignment)
├── Feature Type Renderer (typed visualization logic)
├── Navigation Synchronizer (crosshair coordination)
└── Interactive Analysis Tools (zoom, pan, measure)
```

### Data Flow
1. **Input**: Symbol list, date range, timeframe configurations
2. **Collection**: Multi-timeframe OHLC + technical indicators  
3. **Processing**: Cross-timeframe alignment, feature typing
4. **Assembly**: Single typed dataset with comprehensive metadata
5. **Storage**: Database + file system with rich metadata
6. **Visualization**: Multi-timeframe charts with overlays

## Implementation Priority

### Phase 1: Core Typed Features (Week 1-2)
- Implement OHLC_INTERVALS feature type
- Implement PRICE_INDICATOR_INTERVALS feature type  
- Update database schema with feature type metadata
- Create single dataset per run architecture

### Phase 2: Multi-Timeframe Data Collection (Week 3)
- Build multi-timeframe OHLC collector
- Build multi-timeframe indicator calculator
- Implement cross-timeframe alignment logic
- Add comprehensive metadata generation

### Phase 3: Enhanced Visualization (Week 4-5)
- Build 6-chart synchronized view system
- Implement cross-timeframe overlay engine
- Add interactive navigation and analysis tools
- Integrate with existing training data explorer

### Phase 4: Advanced Features (Week 6)
- Add custom timeframe configurations
- Implement advanced cross-timeframe features
- Add real-time data integration
- Performance optimization and caching

## Risk Assessment

### Technical Risks
- **Data Volume**: Multi-timeframe data will significantly increase storage requirements
- **Processing Complexity**: Cross-timeframe alignment may be computationally expensive  
- **Visualization Performance**: 6 synchronized charts may impact browser performance

### Mitigation Strategies
- Implement data compression and efficient storage formats (HDF5, Parquet)
- Use caching and lazy loading for data processing
- Optimize frontend rendering with canvas-based charts and virtualization

## Success Criteria

### Functional Requirements Met
- [ ] Generate single comprehensive dataset per training run
- [ ] Support typed OHLC and indicator features with metadata
- [ ] Provide 6-timeframe synchronized visualization  
- [ ] Enable cross-timeframe indicator overlays
- [ ] Maintain backwards compatibility with existing system

### Performance Requirements Met
- [ ] Training data generation completes within 5 minutes for 100 symbols × 1 year
- [ ] Multi-timeframe visualization renders within 3 seconds
- [ ] Cross-timeframe navigation responds within 500ms
- [ ] Dataset file sizes optimized (compression ratio > 60%)

### Quality Requirements Met
- [ ] Feature type metadata accuracy = 100%
- [ ] Cross-timeframe alignment precision < 1 minute error
- [ ] Visualization consistency across all 6 timeframes
- [ ] System reliability > 99.5% for training data generation

---

**Document Status**: Draft v1.0  
**Author**: ATS Development Team  
**Review Date**: 2025-08-22  
**Approval**: Pending Technical Review