# DRD: Enhanced Multi-Timeframe Training Data System

## Technical Design Specification

This DRD provides comprehensive technical design for implementing the multi-timeframe, typed-feature training data system outlined in the PRD.

## System Architecture

### 1. High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                     Training Data Pipeline                      │
├─────────────────────────────────────────────────────────────────┤
│  Input Layer                                                    │
│  ├── Symbol Selection Engine                                    │
│  ├── Date Range Validator                                       │
│  └── Timeframe Configuration Parser                             │
├─────────────────────────────────────────────────────────────────┤
│  Data Collection Layer                                          │
│  ├── Multi-Timeframe OHLC Collector                            │
│  ├── Technical Indicator Calculator                             │
│  ├── Cross-Timeframe Data Synchronizer                         │
│  └── Data Quality Validator                                     │
├─────────────────────────────────────────────────────────────────┤
│  Feature Engineering Layer                                      │
│  ├── Typed Feature Assembler                                   │
│  ├── Cross-Timeframe Feature Aligner                           │
│  ├── Feature Metadata Generator                                │
│  └── Sequence Windowing Engine                                 │
├─────────────────────────────────────────────────────────────────┤
│  Storage Layer                                                  │
│  ├── Single Dataset Packager                                   │
│  ├── Metadata Database Writer                                  │
│  ├── Compressed File Storage                                   │
│  └── Version Control System                                    │
├─────────────────────────────────────────────────────────────────┤
│  Visualization Layer                                            │
│  ├── Multi-Timeframe Chart Renderer                            │
│  ├── Cross-Timeframe Overlay Engine                            │
│  ├── Interactive Navigation Controller                         │
│  └── Real-time Data Synchronizer                               │
└─────────────────────────────────────────────────────────────────┘
```

### 2. Component Design Details

## Backend Implementation

### 2.1 Enhanced Feature Type System

#### Core Feature Types
```python
from enum import Enum
from dataclasses import dataclass
from typing import List, Dict, Tuple, Optional, Any
import numpy as np

class FeatureType(Enum):
    """Enhanced feature types for multi-timeframe analysis."""
    
    # OHLC Data Types
    OHLC_INTERVALS = "ohlc_intervals"           # [time_steps, 4] OHLC matrices
    OHLC_SEQUENCES = "ohlc_sequences"           # Variable length OHLC sequences
    
    # Technical Indicator Types  
    PRICE_INDICATOR_INTERVALS = "price_indicator_intervals"  # [time_steps, 1] indicator arrays
    VOLUME_INDICATOR_INTERVALS = "volume_indicator_intervals"  # [time_steps, 1] volume arrays
    
    # Cross-Timeframe Types
    CROSS_TIMEFRAME_INDICATORS = "cross_timeframe_indicators"  # Aligned multi-timeframe data
    TIMEFRAME_ALIGNED_FEATURES = "timeframe_aligned_features"   # Synchronized features
    
    # Traditional Types (backward compatibility)
    SCALAR_FEATURES = "scalar_features"         # Single values
    SEQUENCE_FEATURES = "sequence_features"     # 1D sequences
    CATEGORICAL_FEATURES = "categorical_features"  # Category labels

class TimeframeSpec(Enum):
    """Supported timeframe specifications."""
    MINUTE_5 = "5min"
    MINUTE_15 = "15min"  
    HOUR_1 = "1hour"
    DAILY = "daily"
    WEEKLY = "weekly"
    MONTHLY = "monthly"

@dataclass
class FeatureSpecification:
    """Complete specification for a typed feature."""
    name: str
    feature_type: FeatureType
    timeframe: TimeframeSpec
    intervals: int  # Number of time steps
    dimensions: Tuple[int, ...]  # Feature shape
    indicator_type: Optional[str] = None  # ETOP, EBOT, PLDOT, etc.
    source_timeframe: Optional[TimeframeSpec] = None  # For cross-timeframe features
    metadata: Dict[str, Any] = None
    
    def __post_init__(self):
        if self.metadata is None:
            self.metadata = {}
```

#### Feature Registry Implementation
```python
class EnhancedFeatureRegistry:
    """Registry for all supported feature types and configurations."""
    
    def __init__(self):
        self.feature_specs: Dict[str, FeatureSpecification] = {}
        self.timeframe_configs: Dict[str, Dict] = {}
        self._register_default_features()
    
    def _register_default_features(self):
        """Register standard feature configurations."""
        
        # OHLC Interval Features
        for timeframe in [TimeframeSpec.MINUTE_5, TimeframeSpec.MINUTE_15, 
                         TimeframeSpec.HOUR_1, TimeframeSpec.DAILY]:
            for intervals in [8, 16, 32]:
                feature_name = f"ohlc_{timeframe.value}_{intervals}"
                self.register_feature(FeatureSpecification(
                    name=feature_name,
                    feature_type=FeatureType.OHLC_INTERVALS,
                    timeframe=timeframe,
                    intervals=intervals,
                    dimensions=(intervals, 4),
                    metadata={
                        "visualization": "candlestick_sequence",
                        "columns": ["open", "high", "low", "close"]
                    }
                ))
        
        # Technical Indicator Features
        for indicator in ["etop", "ebot", "pldot", "ema", "rsi"]:
            for timeframe in [TimeframeSpec.MINUTE_5, TimeframeSpec.MINUTE_15, 
                             TimeframeSpec.HOUR_1]:
                for intervals in [8, 16, 32]:
                    feature_name = f"{indicator}_{timeframe.value}_{intervals}"
                    self.register_feature(FeatureSpecification(
                        name=feature_name,
                        feature_type=FeatureType.PRICE_INDICATOR_INTERVALS,
                        timeframe=timeframe,
                        intervals=intervals,
                        dimensions=(intervals, 1),
                        indicator_type=indicator.upper(),
                        metadata={
                            "visualization": "line_overlay",
                            "color_scheme": self._get_indicator_colors(indicator)
                        }
                    ))
        
        # Cross-Timeframe Features
        cross_mappings = [
            ("1hour", "5min"), ("daily", "15min"), ("weekly", "daily")
        ]
        for source_tf, target_tf in cross_mappings:
            for indicator in ["etop", "ebot", "pldot"]:
                feature_name = f"{indicator}_{source_tf}_on_{target_tf}"
                self.register_feature(FeatureSpecification(
                    name=feature_name,
                    feature_type=FeatureType.CROSS_TIMEFRAME_INDICATORS,
                    timeframe=TimeframeSpec(target_tf),
                    intervals=16,  # Standard for cross-timeframe
                    dimensions=(16, 1),
                    indicator_type=indicator.upper(),
                    source_timeframe=TimeframeSpec(source_tf),
                    metadata={
                        "visualization": "cross_timeframe_overlay",
                        "opacity": 0.7
                    }
                ))
    
    def register_feature(self, spec: FeatureSpecification):
        """Register a new feature specification."""
        self.feature_specs[spec.name] = spec
    
    def get_feature_spec(self, name: str) -> Optional[FeatureSpecification]:
        """Get feature specification by name."""
        return self.feature_specs.get(name)
    
    def list_features_by_type(self, feature_type: FeatureType) -> List[FeatureSpecification]:
        """List all features of a specific type."""
        return [spec for spec in self.feature_specs.values() 
                if spec.feature_type == feature_type]
```

### 2.2 Multi-Timeframe Data Collection Engine

#### Data Collector Implementation
```python
import asyncio
import asyncpg
from typing import Dict, List, Optional
import pandas as pd
import numpy as np

class MultiTimeframeDataCollector:
    """Collects OHLC and indicator data across multiple timeframes."""
    
    def __init__(self, db_pool: asyncpg.Pool, feature_registry: EnhancedFeatureRegistry):
        self.db_pool = db_pool
        self.feature_registry = feature_registry
    
    async def collect_training_data(self, 
                                  symbols: List[str], 
                                  start_date: str, 
                                  end_date: str,
                                  feature_specs: List[FeatureSpecification]) -> Dict[str, np.ndarray]:
        """Collect all required data for specified features."""
        
        # Group features by timeframe for efficient collection
        timeframe_features = self._group_features_by_timeframe(feature_specs)
        collected_data = {}
        
        for timeframe, features in timeframe_features.items():
            timeframe_data = await self._collect_timeframe_data(
                symbols, start_date, end_date, timeframe, features
            )
            collected_data.update(timeframe_data)
        
        # Process cross-timeframe features
        cross_timeframe_data = await self._process_cross_timeframe_features(
            collected_data, feature_specs, symbols, start_date, end_date
        )
        collected_data.update(cross_timeframe_data)
        
        return collected_data
    
    async def _collect_timeframe_data(self, 
                                    symbols: List[str], 
                                    start_date: str, 
                                    end_date: str,
                                    timeframe: TimeframeSpec,
                                    features: List[FeatureSpecification]) -> Dict[str, np.ndarray]:
        """Collect data for a specific timeframe."""
        
        # Get base OHLC data
        ohlc_data = await self._get_ohlc_data(symbols, start_date, end_date, timeframe)
        
        # Calculate technical indicators
        indicator_data = await self._calculate_indicators(ohlc_data, features)
        
        # Create feature matrices
        feature_matrices = self._create_feature_matrices(
            ohlc_data, indicator_data, features
        )
        
        return feature_matrices
    
    async def _get_ohlc_data(self, symbols: List[str], start_date: str, end_date: str, 
                           timeframe: TimeframeSpec) -> pd.DataFrame:
        """Get OHLC data for specified timeframe."""
        
        # Map timeframe to appropriate table/aggregation
        if timeframe == TimeframeSpec.MINUTE_5:
            table_name = "dev_minute_prices_unified"
            time_column = "timestamp"
        elif timeframe == TimeframeSpec.MINUTE_15:
            table_name = "dev_minute_prices_unified"
            time_column = "timestamp"
        elif timeframe == TimeframeSpec.HOUR_1:
            table_name = "dev_daily_prices"  # Will aggregate
            time_column = "date"
        else:  # daily, weekly, monthly
            table_name = "dev_daily_prices"
            time_column = "date"
        
        async with self.db_pool.acquire() as conn:
            query = f"""
            SELECT 
                i.symbol,
                dp.{time_column},
                dp.open_price as open,
                dp.high_price as high, 
                dp.low_price as low,
                dp.close as close,
                dp.volume
            FROM {table_name} dp
            JOIN dev_instruments i ON dp.instrument_id = i.id
            WHERE i.symbol = ANY($1)
            AND dp.{time_column} BETWEEN $2 AND $3
            ORDER BY i.symbol, dp.{time_column}
            """
            
            rows = await conn.fetch(query, symbols, start_date, end_date)
            df = pd.DataFrame(rows)
            
            # Apply timeframe aggregation if needed
            if timeframe in [TimeframeSpec.MINUTE_15, TimeframeSpec.HOUR_1]:
                df = self._aggregate_timeframe(df, timeframe)
            elif timeframe in [TimeframeSpec.WEEKLY, TimeframeSpec.MONTHLY]:
                df = self._aggregate_daily_to_timeframe(df, timeframe)
                
            return df
    
    def _create_feature_matrices(self, ohlc_data: pd.DataFrame, 
                               indicator_data: Dict[str, pd.DataFrame],
                               features: List[FeatureSpecification]) -> Dict[str, np.ndarray]:
        """Create typed feature matrices from raw data."""
        
        feature_matrices = {}
        
        for feature_spec in features:
            if feature_spec.feature_type == FeatureType.OHLC_INTERVALS:
                matrix = self._create_ohlc_matrix(ohlc_data, feature_spec)
            elif feature_spec.feature_type == FeatureType.PRICE_INDICATOR_INTERVALS:
                matrix = self._create_indicator_matrix(indicator_data, feature_spec)
            else:
                continue  # Handle other types
                
            feature_matrices[feature_spec.name] = matrix
        
        return feature_matrices
    
    def _create_ohlc_matrix(self, ohlc_data: pd.DataFrame, 
                          feature_spec: FeatureSpecification) -> np.ndarray:
        """Create OHLC matrix with shape [samples, time_steps, 4]."""
        
        symbols = ohlc_data['symbol'].unique()
        intervals = feature_spec.intervals
        
        # Group by symbol and create sequences
        sequences = []
        for symbol in symbols:
            symbol_data = ohlc_data[ohlc_data['symbol'] == symbol]
            ohlc_values = symbol_data[['open', 'high', 'low', 'close']].values
            
            # Create sliding windows
            for i in range(intervals, len(ohlc_values)):
                sequence = ohlc_values[i-intervals:i]  # Shape: [intervals, 4]
                sequences.append(sequence)
        
        return np.array(sequences)  # Shape: [num_samples, intervals, 4]
    
    def _create_indicator_matrix(self, indicator_data: Dict[str, pd.DataFrame],
                               feature_spec: FeatureSpecification) -> np.ndarray:
        """Create indicator matrix with shape [samples, time_steps, 1]."""
        
        indicator_type = feature_spec.indicator_type.lower()
        data = indicator_data.get(indicator_type)
        
        if data is None:
            return np.array([])
        
        symbols = data['symbol'].unique()
        intervals = feature_spec.intervals
        
        sequences = []
        for symbol in symbols:
            symbol_data = data[data['symbol'] == symbol]
            indicator_values = symbol_data[indicator_type].values.reshape(-1, 1)
            
            # Create sliding windows
            for i in range(intervals, len(indicator_values)):
                sequence = indicator_values[i-intervals:i]  # Shape: [intervals, 1]
                sequences.append(sequence)
        
        return np.array(sequences)  # Shape: [num_samples, intervals, 1]
```

### 2.3 Cross-Timeframe Alignment Engine

```python
class CrossTimeframeAligner:
    """Aligns indicators from different timeframes."""
    
    def __init__(self):
        self.timeframe_multipliers = {
            TimeframeSpec.MINUTE_5: 1,
            TimeframeSpec.MINUTE_15: 3,
            TimeframeSpec.HOUR_1: 12,
            TimeframeSpec.DAILY: 288,  # Assuming 5min base
            TimeframeSpec.WEEKLY: 2016,
            TimeframeSpec.MONTHLY: 8640
        }
    
    async def align_cross_timeframe_features(self, 
                                           base_data: Dict[str, np.ndarray],
                                           cross_specs: List[FeatureSpecification]) -> Dict[str, np.ndarray]:
        """Align higher timeframe indicators to lower timeframe intervals."""
        
        aligned_features = {}
        
        for spec in cross_specs:
            if spec.feature_type != FeatureType.CROSS_TIMEFRAME_INDICATORS:
                continue
                
            # Get source and target timeframes
            source_tf = spec.source_timeframe
            target_tf = spec.timeframe
            
            # Find source data
            source_feature_name = f"{spec.indicator_type.lower()}_{source_tf.value}_{spec.intervals}"
            source_data = base_data.get(source_feature_name)
            
            if source_data is None:
                continue
            
            # Perform alignment
            aligned_data = self._align_timeframes(
                source_data, source_tf, target_tf, spec.intervals
            )
            
            aligned_features[spec.name] = aligned_data
        
        return aligned_features
    
    def _align_timeframes(self, source_data: np.ndarray, 
                        source_tf: TimeframeSpec, 
                        target_tf: TimeframeSpec,
                        target_intervals: int) -> np.ndarray:
        """Align source timeframe data to target timeframe intervals."""
        
        multiplier = self.timeframe_multipliers[target_tf] / self.timeframe_multipliers[source_tf]
        
        # Expand source data to match target timeframe resolution
        aligned_sequences = []
        
        for sample in source_data:
            # Repeat each source value for the appropriate number of target intervals
            expanded_sample = np.repeat(sample, int(multiplier), axis=0)
            
            # Truncate or pad to match target intervals
            if len(expanded_sample) > target_intervals:
                expanded_sample = expanded_sample[-target_intervals:]
            elif len(expanded_sample) < target_intervals:
                pad_length = target_intervals - len(expanded_sample)
                padding = np.tile(expanded_sample[-1:], (pad_length, 1))
                expanded_sample = np.vstack([expanded_sample, padding])
            
            aligned_sequences.append(expanded_sample)
        
        return np.array(aligned_sequences)
```

### 2.4 Enhanced Database Schema

```sql
-- Enhanced training dataset table
ALTER TABLE dev_training_dataset 
ADD COLUMN IF NOT EXISTS feature_type_registry jsonb,
ADD COLUMN IF NOT EXISTS timeframe_specifications jsonb,
ADD COLUMN IF NOT EXISTS cross_timeframe_mappings jsonb,
ADD COLUMN IF NOT EXISTS visualization_metadata jsonb,
ADD COLUMN IF NOT EXISTS data_compression_info jsonb;

-- Feature type registry example
-- feature_type_registry jsonb format:
{
  "ohlc_5min_8": {
    "type": "OHLC_INTERVALS",
    "timeframe": "5min",
    "intervals": 8,
    "dimensions": [8, 4],
    "visualization": {
      "type": "candlestick_sequence",
      "colors": {"up": "#00C851", "down": "#ff4444"},
      "show_volume": true
    }
  },
  "etop_15min_16": {
    "type": "PRICE_INDICATOR_INTERVALS", 
    "timeframe": "15min",
    "intervals": 16,
    "indicator": "ETOP",
    "dimensions": [16, 1],
    "visualization": {
      "type": "line_overlay",
      "color": "#2196F3",
      "line_width": 2,
      "opacity": 0.8
    }
  },
  "pldot_1hour_on_5min": {
    "type": "CROSS_TIMEFRAME_INDICATORS",
    "source_timeframe": "1hour",
    "target_timeframe": "5min", 
    "intervals": 16,
    "indicator": "PLDOT",
    "dimensions": [16, 1],
    "visualization": {
      "type": "scatter_overlay",
      "color": "#FF5722",
      "marker_size": 6,
      "opacity": 0.9
    }
  }
}

-- Timeframe specifications
-- timeframe_specifications jsonb format:
{
  "supported_timeframes": ["5min", "15min", "1hour", "daily", "weekly", "monthly"],
  "base_timeframe": "5min",
  "alignment_rules": {
    "1hour_to_5min": {"multiplier": 12, "method": "repeat"},
    "daily_to_15min": {"multiplier": 96, "method": "interpolate"}
  }
}

-- Create indexes for performance
CREATE INDEX IF NOT EXISTS idx_training_dataset_feature_types 
ON dev_training_dataset USING GIN (feature_type_registry);

CREATE INDEX IF NOT EXISTS idx_training_dataset_timeframes
ON dev_training_dataset USING GIN (timeframe_specifications);
```

## Frontend Implementation

### 3.1 Multi-Timeframe Chart Architecture

```typescript
// Enhanced TypeScript interfaces
interface TimeframeSpec {
  id: string;
  label: string;
  intervals: number;
  multiplier: number;
  chartHeight: number;
}

interface FeatureVisualization {
  featureName: string;
  type: 'candlestick_sequence' | 'line_overlay' | 'scatter_overlay' | 'cross_timeframe_overlay';
  data: number[][] | number[];
  metadata: {
    color?: string;
    opacity?: number;
    lineWidth?: number;
    showVolume?: boolean;
  };
}

interface TrainingExampleView {
  exampleId: string;
  symbol: string;
  timestamp: string;
  timeframes: TimeframeSpec[];
  features: FeatureVisualization[];
  crossTimeframeMapping: Record<string, string[]>;
}

// Multi-timeframe chart manager
class MultiTimeframeChartManager {
  private charts: Map<string, Chart> = new Map();
  private syncController: ChartSyncController;
  
  constructor(private containerElement: HTMLElement) {
    this.syncController = new ChartSyncController();
    this.initializeTimeframes();
  }
  
  private initializeTimeframes(): void {
    const timeframes: TimeframeSpec[] = [
      { id: 'monthly', label: 'Monthly', intervals: 12, multiplier: 8640, chartHeight: 200 },
      { id: 'weekly', label: 'Weekly', intervals: 52, multiplier: 2016, chartHeight: 200 },
      { id: 'daily', label: 'Daily', intervals: 252, multiplier: 288, chartHeight: 250 },
      { id: '1hour', label: '1 Hour', intervals: 720, multiplier: 12, chartHeight: 250 },
      { id: '15min', label: '15 Min', intervals: 960, multiplier: 3, chartHeight: 300 },
      { id: '5min', label: '5 Min', intervals: 1440, multiplier: 1, chartHeight: 300 }
    ];
    
    timeframes.forEach(tf => this.createTimeframeChart(tf));
  }
  
  private createTimeframeChart(timeframe: TimeframeSpec): void {
    const chartContainer = this.createChartContainer(timeframe);
    
    const chart = new Chart(chartContainer, {
      type: 'candlestick',
      data: { datasets: [] },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        height: timeframe.chartHeight,
        scales: {
          x: {
            type: 'time',
            time: { unit: this.getTimeUnit(timeframe.id) }
          },
          y: { position: 'right' },
          volume: {
            type: 'linear',
            position: 'right',
            max: 1,
            display: false
          }
        },
        plugins: {
          crosshair: {
            sync: { enabled: true, group: 'training-example' }
          },
          zoom: {
            zoom: { wheel: { enabled: true }, mode: 'x' },
            pan: { enabled: true, mode: 'x' }
          }
        }
      }
    });
    
    this.charts.set(timeframe.id, chart);
    this.syncController.addChart(timeframe.id, chart);
  }
  
  public async renderTrainingExample(example: TrainingExampleView): Promise<void> {
    // Clear existing data
    this.clearAllCharts();
    
    // Render features for each timeframe
    for (const timeframe of example.timeframes) {
      await this.renderTimeframeFeatures(timeframe, example.features);
    }
    
    // Apply cross-timeframe overlays
    await this.renderCrossTimeframeOverlays(example);
    
    // Update all charts
    this.updateAllCharts();
  }
  
  private async renderTimeframeFeatures(timeframe: TimeframeSpec, 
                                       features: FeatureVisualization[]): Promise<void> {
    const chart = this.charts.get(timeframe.id);
    if (!chart) return;
    
    const timeframeFeatures = features.filter(f => 
      f.featureName.includes(timeframe.id)
    );
    
    for (const feature of timeframeFeatures) {
      await this.addFeatureToChart(chart, feature, timeframe);
    }
  }
  
  private async addFeatureToChart(chart: Chart, 
                                feature: FeatureVisualization,
                                timeframe: TimeframeSpec): Promise<void> {
    switch (feature.type) {
      case 'candlestick_sequence':
        this.addCandlestickData(chart, feature, timeframe);
        break;
      case 'line_overlay':
        this.addLineOverlay(chart, feature, timeframe);
        break;
      case 'scatter_overlay':
        this.addScatterOverlay(chart, feature, timeframe);
        break;
      case 'cross_timeframe_overlay':
        this.addCrossTimeframeOverlay(chart, feature, timeframe);
        break;
    }
  }
  
  private addCandlestickData(chart: Chart, feature: FeatureVisualization, 
                           timeframe: TimeframeSpec): void {
    // Feature data shape: [intervals, 4] for OHLC
    const ohlcData = feature.data as number[][];
    const candlestickData = ohlcData.map((candle, index) => ({
      x: this.calculateTimestamp(index, timeframe),
      o: candle[0], // open
      h: candle[1], // high  
      l: candle[2], // low
      c: candle[3]  // close
    }));
    
    chart.data.datasets.push({
      label: `OHLC ${timeframe.label}`,
      type: 'candlestick',
      data: candlestickData,
      borderColor: feature.metadata.color || '#333',
      backgroundColor: 'transparent'
    });
  }
  
  private addLineOverlay(chart: Chart, feature: FeatureVisualization,
                        timeframe: TimeframeSpec): void {
    // Feature data shape: [intervals, 1] for indicators
    const indicatorData = feature.data as number[][];
    const lineData = indicatorData.map((value, index) => ({
      x: this.calculateTimestamp(index, timeframe),
      y: value[0]
    }));
    
    chart.data.datasets.push({
      label: feature.featureName,
      type: 'line',
      data: lineData,
      borderColor: feature.metadata.color || '#2196F3',
      backgroundColor: 'transparent',
      borderWidth: feature.metadata.lineWidth || 2,
      pointRadius: 0
    });
  }
}

// Chart synchronization controller
class ChartSyncController {
  private charts: Map<string, Chart> = new Map();
  private syncGroup = 'training-example';
  
  public addChart(id: string, chart: Chart): void {
    this.charts.set(id, chart);
    this.setupSyncEvents(chart);
  }
  
  private setupSyncEvents(chart: Chart): void {
    chart.canvas.addEventListener('mousemove', (event) => {
      this.syncCrosshair(chart, event);
    });
    
    chart.canvas.addEventListener('wheel', (event) => {
      this.syncZoom(chart, event);
    });
  }
  
  private syncCrosshair(sourceChart: Chart, event: MouseEvent): void {
    const position = Chart.helpers.getRelativePosition(event, sourceChart);
    const dataX = sourceChart.scales.x.getValueForPixel(position.x);
    
    // Sync crosshair position to all other charts
    this.charts.forEach((chart, id) => {
      if (chart !== sourceChart) {
        this.updateCrosshair(chart, dataX);
      }
    });
  }
  
  private updateCrosshair(chart: Chart, xValue: number): void {
    // Update crosshair plugin for synchronized navigation
    if (chart.options.plugins?.crosshair) {
      chart.options.plugins.crosshair.sync = { 
        enabled: true, 
        group: this.syncGroup,
        position: xValue
      };
      chart.update('none');
    }
  }
}
```

### 3.2 API Integration Layer

```typescript
// Enhanced API service for typed training data
class EnhancedTrainingDataAPI {
  private baseURL: string;
  
  constructor(baseURL: string) {
    this.baseURL = baseURL;
  }
  
  public async getTrainingExample(datasetId: string, exampleIndex: number): Promise<TrainingExampleView> {
    const response = await fetch(
      `${this.baseURL}/api/v1/datasets/${datasetId}/examples/${exampleIndex}/enhanced`
    );
    
    if (!response.ok) {
      throw new Error(`Failed to fetch training example: ${response.statusText}`);
    }
    
    const data = await response.json();
    return this.transformToTrainingExampleView(data);
  }
  
  public async getFeatureTypeRegistry(datasetId: string): Promise<Record<string, any>> {
    const response = await fetch(
      `${this.baseURL}/api/v1/datasets/${datasetId}/feature-types`
    );
    
    return await response.json();
  }
  
  private transformToTrainingExampleView(data: any): TrainingExampleView {
    return {
      exampleId: data.example_id,
      symbol: data.symbol,
      timestamp: data.timestamp,
      timeframes: data.timeframe_specs,
      features: data.features.map((f: any) => ({
        featureName: f.name,
        type: f.visualization_type,
        data: f.data,
        metadata: f.metadata
      })),
      crossTimeframeMapping: data.cross_timeframe_mappings
    };
  }
}
```

## Performance Optimization

### 4.1 Data Storage Optimization

```python
# Compressed storage using HDF5
import h5py
import numpy as np
from typing import Dict, Any

class OptimizedDatasetStorage:
    """Optimized storage for large multi-timeframe datasets."""
    
    def __init__(self, file_path: str):
        self.file_path = file_path
        self.compression = 'gzip'
        self.compression_opts = 9
    
    def save_dataset(self, features: Dict[str, np.ndarray], 
                    metadata: Dict[str, Any]) -> None:
        """Save dataset with optimized compression."""
        
        with h5py.File(self.file_path, 'w') as f:
            # Store feature data with compression
            feature_group = f.create_group('features')
            for name, data in features.items():
                feature_group.create_dataset(
                    name, 
                    data=data,
                    compression=self.compression,
                    compression_opts=self.compression_opts,
                    shuffle=True  # Improves compression
                )
            
            # Store metadata
            metadata_group = f.create_group('metadata')
            for key, value in metadata.items():
                if isinstance(value, (str, int, float)):
                    metadata_group.attrs[key] = value
                else:
                    # Store complex metadata as JSON strings
                    metadata_group.attrs[key] = json.dumps(value)
    
    def load_feature(self, feature_name: str) -> Optional[np.ndarray]:
        """Load specific feature with lazy loading."""
        
        try:
            with h5py.File(self.file_path, 'r') as f:
                return f['features'][feature_name][:]
        except KeyError:
            return None
    
    def get_dataset_info(self) -> Dict[str, Any]:
        """Get dataset metadata without loading full data."""
        
        with h5py.File(self.file_path, 'r') as f:
            metadata = dict(f['metadata'].attrs)
            feature_info = {
                name: {
                    'shape': dataset.shape,
                    'dtype': str(dataset.dtype),
                    'compression': dataset.compression
                }
                for name, dataset in f['features'].items()
            }
            
        return {
            'metadata': metadata,
            'features': feature_info,
            'file_size_mb': os.path.getsize(self.file_path) / (1024 * 1024)
        }
```

### 4.2 Caching Strategy

```python
from functools import lru_cache
import asyncio
from typing import Dict, List, Optional
import redis.asyncio as redis

class TrainingDataCache:
    """Redis-based caching for training data."""
    
    def __init__(self, redis_url: str):
        self.redis = redis.from_url(redis_url)
        self.cache_ttl = 3600  # 1 hour
    
    async def get_cached_features(self, dataset_id: str, 
                                 example_id: str) -> Optional[Dict[str, Any]]:
        """Get cached training example."""
        
        cache_key = f"training_data:{dataset_id}:{example_id}"
        cached_data = await self.redis.get(cache_key)
        
        if cached_data:
            return json.loads(cached_data)
        return None
    
    async def cache_features(self, dataset_id: str, example_id: str,
                           features: Dict[str, Any]) -> None:
        """Cache training example with TTL."""
        
        cache_key = f"training_data:{dataset_id}:{example_id}"
        serialized_data = json.dumps(features, cls=NumpyEncoder)
        
        await self.redis.setex(
            cache_key, 
            self.cache_ttl, 
            serialized_data
        )
```

## Quality Assurance & Testing

### 5.1 Unit Test Framework

```python
import pytest
import numpy as np
from unittest.mock import AsyncMock, MagicMock

class TestEnhancedTrainingDataSystem:
    """Comprehensive test suite for enhanced training data system."""
    
    @pytest.fixture
    def feature_registry(self):
        return EnhancedFeatureRegistry()
    
    @pytest.fixture
    def mock_db_pool(self):
        return AsyncMock()
    
    @pytest.fixture
    def data_collector(self, mock_db_pool, feature_registry):
        return MultiTimeframeDataCollector(mock_db_pool, feature_registry)
    
    def test_feature_registry_initialization(self, feature_registry):
        """Test that feature registry initializes with default features."""
        
        # Test OHLC features
        ohlc_features = feature_registry.list_features_by_type(FeatureType.OHLC_INTERVALS)
        assert len(ohlc_features) > 0
        
        # Test specific feature specification
        ohlc_5min_8 = feature_registry.get_feature_spec("ohlc_5min_8")
        assert ohlc_5min_8 is not None
        assert ohlc_5min_8.dimensions == (8, 4)
        assert ohlc_5min_8.timeframe == TimeframeSpec.MINUTE_5
    
    def test_ohlc_matrix_creation(self, data_collector):
        """Test OHLC matrix creation with correct dimensions."""
        
        # Mock OHLC data
        ohlc_data = pd.DataFrame({
            'symbol': ['AAPL'] * 100,
            'timestamp': pd.date_range('2024-01-01', periods=100, freq='5min'),
            'open': np.random.random(100) * 100,
            'high': np.random.random(100) * 100 + 5,
            'low': np.random.random(100) * 100 - 5,
            'close': np.random.random(100) * 100
        })
        
        feature_spec = FeatureSpecification(
            name="ohlc_5min_8",
            feature_type=FeatureType.OHLC_INTERVALS,
            timeframe=TimeframeSpec.MINUTE_5,
            intervals=8,
            dimensions=(8, 4)
        )
        
        matrix = data_collector._create_ohlc_matrix(ohlc_data, feature_spec)
        
        # Verify matrix dimensions
        assert matrix.ndim == 3
        assert matrix.shape[1] == 8  # intervals
        assert matrix.shape[2] == 4  # OHLC
        assert matrix.shape[0] == 100 - 8 + 1  # sliding window samples
    
    @pytest.mark.asyncio
    async def test_cross_timeframe_alignment(self):
        """Test cross-timeframe feature alignment."""
        
        aligner = CrossTimeframeAligner()
        
        # Mock 1-hour data (shape: [samples, 4, 1])
        hour_data = np.random.random((10, 4, 1))
        
        # Align to 5-minute intervals (12x expansion)
        aligned_data = aligner._align_timeframes(
            hour_data, 
            TimeframeSpec.HOUR_1, 
            TimeframeSpec.MINUTE_5, 
            48  # 4 hours worth of 5min intervals
        )
        
        # Verify alignment
        assert aligned_data.shape[0] == 10  # same number of samples
        assert aligned_data.shape[1] == 48  # expanded intervals
        assert aligned_data.shape[2] == 1   # same feature dimension
    
    def test_feature_visualization_metadata(self, feature_registry):
        """Test that features contain proper visualization metadata."""
        
        etop_feature = feature_registry.get_feature_spec("etop_5min_8")
        assert etop_feature is not None
        assert "visualization" in etop_feature.metadata
        assert etop_feature.metadata["visualization"] == "line_overlay"
    
    @pytest.mark.integration
    async def test_end_to_end_data_generation(self, data_collector):
        """Integration test for complete data generation pipeline."""
        
        symbols = ["AAPL", "TSLA"]
        start_date = "2024-01-01"
        end_date = "2024-01-31"
        
        feature_specs = [
            FeatureSpecification(
                name="ohlc_5min_8",
                feature_type=FeatureType.OHLC_INTERVALS,
                timeframe=TimeframeSpec.MINUTE_5,
                intervals=8,
                dimensions=(8, 4)
            ),
            FeatureSpecification(
                name="etop_5min_8", 
                feature_type=FeatureType.PRICE_INDICATOR_INTERVALS,
                timeframe=TimeframeSpec.MINUTE_5,
                intervals=8,
                dimensions=(8, 1),
                indicator_type="ETOP"
            )
        ]
        
        # Mock database responses
        data_collector.db_pool.acquire.return_value.__aenter__.return_value.fetch = AsyncMock(
            return_value=self._generate_mock_db_data(symbols, start_date, end_date)
        )
        
        # Generate training data
        result = await data_collector.collect_training_data(
            symbols, start_date, end_date, feature_specs
        )
        
        # Verify results
        assert "ohlc_5min_8" in result
        assert "etop_5min_8" in result
        assert result["ohlc_5min_8"].shape[2] == 4  # OHLC dimensions
        assert result["etop_5min_8"].shape[2] == 1  # Indicator dimensions
```

## Deployment Strategy

### 6.1 Kubernetes Configuration

```yaml
# Enhanced training data generation job
apiVersion: batch/v1
kind: Job
metadata:
  name: enhanced-training-data-generation
  namespace: ats-dev
spec:
  template:
    spec:
      containers:
      - name: training-data-generator
        image: ats-platform:latest
        command:
          - python
          - -m
          - src.modeling.enhanced_training_data_generator
        args:
          - --symbols=AAPL,TSLA,MSFT,GOOGL
          - --start-date=2024-01-01
          - --end-date=2024-12-31
          - --feature-config=multi_timeframe_config.json
          - --output-format=optimized_hdf5
        env:
        - name: DB_HOST
          value: postgres-simple
        - name: DB_PASSWORD
          valueFrom:
            secretKeyRef:
              name: postgres-secret
              key: password
        - name: REDIS_URL
          value: redis://redis-service:6379
        resources:
          requests:
            memory: "8Gi"
            cpu: "2"
          limits:
            memory: "16Gi"  
            cpu: "4"
        volumeMounts:
        - name: training-data-storage
          mountPath: /data/training
        - name: cache-storage
          mountPath: /tmp/cache
      volumes:
      - name: training-data-storage
        persistentVolumeClaim:
          claimName: training-data-pvc
      - name: cache-storage
        emptyDir:
          sizeLimit: 4Gi
      restartPolicy: Never
  backoffLimit: 3
```

### 6.2 Monitoring & Alerting

```yaml
# Prometheus monitoring for training data generation
apiVersion: monitoring.coreos.com/v1
kind: ServiceMonitor
metadata:
  name: training-data-metrics
  namespace: ats-dev
spec:
  selector:
    matchLabels:
      app: enhanced-training-data
  endpoints:
  - port: metrics
    interval: 30s
    path: /metrics
---
# Grafana dashboard configuration
apiVersion: v1
kind: ConfigMap
metadata:
  name: training-data-dashboard
  namespace: ats-dev
data:
  dashboard.json: |
    {
      "dashboard": {
        "title": "Enhanced Training Data System",
        "panels": [
          {
            "title": "Data Generation Performance",
            "type": "graph",
            "targets": [
              {
                "expr": "training_data_generation_duration_seconds",
                "legendFormat": "Generation Time"
              }
            ]
          },
          {
            "title": "Feature Matrix Dimensions",
            "type": "table",
            "targets": [
              {
                "expr": "training_data_feature_dimensions",
                "format": "table"
              }
            ]
          }
        ]
      }
    }
```

---

**Document Status**: Draft v1.0  
**Author**: ATS Development Team  
**Technical Review**: Pending  
**Implementation Timeline**: 6 weeks  
**Priority**: High