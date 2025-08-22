export interface FeatureMetadata {
  name: string;
  feature_type: 'int' | 'float' | 'ohlc' | 'price_indicator' | 'volume_indicator' | 'return' | 'classification' | 'binary' | 'normalized';
  data_type: string;
  dimension: number;
  description: string;
  source_column?: string;
  lag_periods?: number;
  lead_periods?: number;
  window_size?: number;
  parameters: Record<string, any>;
  visualization_type: 'histogram' | 'time_series' | 'candlestick' | 'line_chart' | 'bar_chart' | 'scatter_plot' | 'correlation_matrix' | 'distribution';
  min_value?: number;
  max_value?: number;
  mean_value?: number;
  std_value?: number;
  null_count: number;
  is_primary_key: boolean;
}

export interface LabelMetadata {
  name: string;
  label_type: string;
  data_type: string;
  dimension: number;
  description: string;
  lead_periods: number;
  parameters: Record<string, any>;
  visualization_type: 'histogram' | 'time_series' | 'candlestick' | 'line_chart' | 'bar_chart' | 'scatter_plot' | 'correlation_matrix' | 'distribution';
  min_value?: number;
  max_value?: number;
  unique_values?: any[];
  class_distribution?: Record<string, number>;
}

export interface TrainingDataMetadata {
  dataset_name: string;
  creation_timestamp: string;
  total_sequences: number;
  sequence_length: number;
  prediction_horizon: number;
  feature_count: number;
  label_count: number;
  features: FeatureMetadata[];
  labels: LabelMetadata[];
  symbols: string[];
  date_range: {
    start: string;
    end: string;
  };
  data_sources: string[];
  data_file_path?: string;
  feature_file_path?: string;
  label_file_path?: string;
  sample_ids: string[];
  primary_key_feature?: string;
  primary_key_type?: string;
  gin_config_path?: string;
  generation_parameters: Record<string, any>;
  data_quality_metrics: Record<string, number>;
  outlier_count: number;
  missing_data_ratio: number;
}

export interface TrainingSequence {
  id: string;
  dataset_id: string;
  sequence_index: number;
  features: number[][][]; // [time_steps, features]
  labels: number[][][];   // [prediction_horizon, labels]
  feature_masks: boolean[][][];
  label_masks: boolean[][][];
  symbol?: string;
  start_date?: string;
  end_date?: string;
}

export interface DatasetSummary {
  id: string;
  name: string;
  creation_timestamp: string;
  total_sequences: number;
  feature_count: number;
  label_count: number;
  symbols: string[];
  date_range: {
    start: string;
    end: string;
  };
  quality_score: number;
  size_mb: number;
}

export interface FeatureDistribution {
  feature_name: string;
  feature_type: string;
  histogram: {
    bins: number[];
    counts: number[];
  };
  statistics: {
    mean: number;
    std: number;
    min: number;
    max: number;
    percentiles: {
      p25: number;
      p50: number;
      p75: number;
      p90: number;
      p95: number;
      p99: number;
    };
  };
  time_series?: {
    timestamps: string[];
    values: number[];
  };
}

export interface OHLCData {
  timestamp: string;
  open: number;
  high: number;
  low: number;
  close: number;
  volume?: number;
}

export interface ComparisonResult {
  feature_name: string;
  dataset1_stats: {
    mean: number;
    std: number;
    min: number;
    max: number;
  };
  dataset2_stats: {
    mean: number;
    std: number;
    min: number;
    max: number;
  };
  statistical_tests: {
    ks_test: {
      statistic: number;
      p_value: number;
      significant: boolean;
    };
    t_test: {
      statistic: number;
      p_value: number;
      significant: boolean;
    };
  };
  distribution_difference: number;
}