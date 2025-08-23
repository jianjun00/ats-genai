"""
Configurable Training Data Generator using Feature and Label Registries.

Generates training data with configurable features and labels defined in gin configuration.
Supports multiple timeframes, custom indicators, and flexible windowing strategies.
"""

import gin
import numpy as np
import pandas as pd

# Optional PyTorch import for tensor operations
try:
    import torch
    TORCH_AVAILABLE = True
except ImportError:
    torch = None
    TORCH_AVAILABLE = False
from typing import Dict, List, Any, Optional, Tuple, Union
from dataclasses import dataclass, field
from datetime import datetime, date, timedelta
import json
import os
from pathlib import Path

from signals.feature_registry import FeatureRegistry, FeatureConfig
from signals.label_registry import LabelRegistry, LabelConfig  
from signals.indicator_factory import IndicatorFactory
from state.runner_callback import RunnerCallback
from config.environment import Environment
from modeling.training_data_metadata import (
    TrainingDataMetadataManager, 
    TrainingDataMetadata,
    FeatureType,
    VisualizationType
)

@gin.configurable
@dataclass
class ConfigurableTrainingDataConfig:
    """Configuration for configurable training data generation."""
    
    # Sequence configuration
    sequence_length: int = 60  # Number of time steps in input sequence
    prediction_horizon: int = 5  # Number of time steps to predict
    
    # Data windowing
    window_stride: int = 1  # Stride between windows
    min_valid_ratio: float = 0.8  # Minimum ratio of non-NaN values required
    
    # Feature and label registries
    feature_registry: Optional[FeatureRegistry] = None
    label_registry: Optional[LabelRegistry] = None
    indicator_factory: Optional[IndicatorFactory] = None
    
    # Output configuration
    normalize_features: bool = True
    normalize_labels: bool = False
    feature_scaling_method: str = 'robust'  # 'standard', 'robust', 'minmax'
    label_scaling_method: str = 'none'  # 'standard', 'robust', 'minmax', 'none'
    
    # Multi-timeframe support
    base_timeframe: str = '1d'  # Base timeframe for data
    additional_timeframes: List[str] = field(default_factory=list)  # ['1h', '4h']
    
    # Data validation
    outlier_threshold: float = 5.0  # Standard deviations for outlier detection
    remove_outliers: bool = True
    
    # Output format
    output_format: str = 'numpy'  # 'pytorch', 'numpy', 'pandas' (default to numpy if torch not available)
    device: str = 'cpu'  # 'cpu', 'cuda'

class ConfigurableTrainingDataGenerator:
    """Generates training data with configurable features and labels."""
    
    def __init__(self, config: ConfigurableTrainingDataConfig, output_dir: Optional[str] = None):
        self.config = config
        self.feature_registry = config.feature_registry or FeatureRegistry()
        self.label_registry = config.label_registry or LabelRegistry()
        self.indicator_factory = config.indicator_factory or IndicatorFactory(
            feature_registry=self.feature_registry,
            label_registry=self.label_registry
        )
        
        self.feature_names = None
        self.label_names = None
        self.feature_scalers = {}
        self.label_scalers = {}
        
        # Metadata management
        self.output_dir = output_dir or "training_data_output"
        self.metadata_manager = TrainingDataMetadataManager(self.output_dir)
        self.dataset_id = None
    
    def generate_training_data(self, 
                             data: pd.DataFrame,
                             symbols: Optional[List[str]] = None) -> Dict[str, Any]:
        """Generate training data from market data."""
        
        print(f"[ConfigurableTrainingDataGenerator] Generating training data for {len(data)} rows")
        print(f"[ConfigurableTrainingDataGenerator] Data columns: {list(data.columns)}")
        print(f"[ConfigurableTrainingDataGenerator] Date range: {data.index.min()} to {data.index.max()}")
        
        # Ensure data is properly indexed by date
        if not isinstance(data.index, pd.DatetimeIndex):
            if 'date' in data.columns:
                data = data.set_index('date')
            else:
                raise ValueError("Data must have a date index or 'date' column")
        
        # Generate features and labels for each symbol
        all_features = []
        all_labels = []
        all_masks = []
        symbol_list = symbols or data['symbol'].unique() if 'symbol' in data.columns else ['single_symbol']
        
        for symbol in symbol_list:
            if 'symbol' in data.columns:
                symbol_data = data[data['symbol'] == symbol].copy()
            else:
                symbol_data = data.copy()
            
            if len(symbol_data) < self.config.sequence_length + self.config.prediction_horizon:
                print(f"[Warning] Insufficient data for symbol {symbol}: {len(symbol_data)} rows")
                continue
            
            # Sort by date to ensure proper time ordering
            symbol_data = symbol_data.sort_index()
            
            # Generate features and labels
            features_df, labels_df = self._generate_features_and_labels(symbol_data)
            
            if features_df.empty or labels_df.empty:
                print(f"[Warning] No features or labels generated for symbol {symbol}")
                continue
            
            # Create sequences
            sequences = self._create_sequences(features_df, labels_df, symbol)
            
            if sequences:
                all_features.extend(sequences['features'])
                all_labels.extend(sequences['labels'])
                all_masks.extend(sequences['masks'])
        
        if not all_features:
            raise ValueError("No training sequences generated. Check data quality and configuration.")
        
        print(f"[ConfigurableTrainingDataGenerator] Generated {len(all_features)} training sequences")
        
        # Extract data information for metadata
        data_info = self.update_data_info(data, symbol_list)
        
        # Convert to final format with metadata
        return self._format_output(all_features, all_labels, all_masks, symbol_list, data_info)
    
    def _generate_features_and_labels(self, data: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Generate features and labels for a single symbol."""
        
        # Ensure required OHLCV columns exist
        required_columns = ['open', 'high', 'low', 'close']
        missing_columns = [col for col in required_columns if col not in data.columns]
        if missing_columns:
            raise ValueError(f"Missing required columns: {missing_columns}")
        
        # Add volume if missing (set to 0)
        if 'volume' not in data.columns:
            data['volume'] = 0.0
            print("[Warning] Volume column missing, using 0.0")
        
        # Generate features
        print(f"[ConfigurableTrainingDataGenerator] Generating features...")
        features_df = self.feature_registry.generate_features(data)
        
        # Generate labels
        print(f"[ConfigurableTrainingDataGenerator] Generating labels...")
        labels_df = self.label_registry.generate_labels(data)
        
        # Store feature and label names
        if self.feature_names is None:
            self.feature_names = list(features_df.columns)
        if self.label_names is None:
            self.label_names = list(labels_df.columns)
        
        print(f"[ConfigurableTrainingDataGenerator] Generated {len(features_df.columns)} features: {list(features_df.columns)[:5]}...")
        print(f"[ConfigurableTrainingDataGenerator] Generated {len(labels_df.columns)} labels: {list(labels_df.columns)}")
        
        # Handle missing values
        features_df = self._handle_missing_values(features_df, 'features')
        labels_df = self._handle_missing_values(labels_df, 'labels')
        
        # Remove outliers if configured
        if self.config.remove_outliers:
            features_df = self._remove_outliers(features_df)
            labels_df = self._remove_outliers(labels_df)
        
        # Apply scaling
        if self.config.normalize_features:
            features_df = self._apply_scaling(features_df, 'features')
        if self.config.normalize_labels:
            labels_df = self._apply_scaling(labels_df, 'labels')
        
        return features_df, labels_df
    
    def _create_sequences(self, features_df: pd.DataFrame, labels_df: pd.DataFrame, symbol: str) -> Dict[str, List]:
        """Create time series sequences from features and labels."""
        
        sequences = {'features': [], 'labels': [], 'masks': []}
        
        # Align features and labels by index
        common_index = features_df.index.intersection(labels_df.index)
        features_aligned = features_df.loc[common_index]
        labels_aligned = labels_df.loc[common_index]
        
        if len(common_index) < self.config.sequence_length + self.config.prediction_horizon:
            print(f"[Warning] Insufficient aligned data for {symbol}: {len(common_index)} rows")
            return sequences
        
        # Convert to numpy arrays
        features_array = features_aligned.values
        labels_array = labels_aligned.values
        
        # Create sliding windows
        max_start_idx = len(features_array) - self.config.sequence_length - self.config.prediction_horizon + 1
        
        for start_idx in range(0, max_start_idx, self.config.window_stride):
            # Extract feature sequence
            feature_seq = features_array[start_idx:start_idx + self.config.sequence_length]
            
            # Extract label sequence
            label_start = start_idx + self.config.sequence_length
            label_seq = labels_array[label_start:label_start + self.config.prediction_horizon]
            
            # Check for minimum valid data ratio
            feature_valid_ratio = np.mean(~np.isnan(feature_seq))
            label_valid_ratio = np.mean(~np.isnan(label_seq))
            
            if (feature_valid_ratio >= self.config.min_valid_ratio and 
                label_valid_ratio >= self.config.min_valid_ratio):
                
                # Create mask for valid values
                feature_mask = ~np.isnan(feature_seq)
                label_mask = ~np.isnan(label_seq)
                
                # Fill NaN values with 0 (will be masked out)
                feature_seq = np.nan_to_num(feature_seq, nan=0.0)
                label_seq = np.nan_to_num(label_seq, nan=0.0)
                
                sequences['features'].append(feature_seq)
                sequences['labels'].append(label_seq)
                sequences['masks'].append({'features': feature_mask, 'labels': label_mask})
        
        print(f"[ConfigurableTrainingDataGenerator] Created {len(sequences['features'])} sequences for {symbol}")
        return sequences
    
    def _handle_missing_values(self, df: pd.DataFrame, data_type: str) -> pd.DataFrame:
        """Handle missing values in the data."""
        
        # Forward fill first, then backward fill
        df_filled = df.ffill().bfill()
        
        # Report missing value statistics
        missing_before = df.isnull().sum().sum()
        missing_after = df_filled.isnull().sum().sum()
        
        if missing_before > 0:
            print(f"[ConfigurableTrainingDataGenerator] {data_type}: Filled {missing_before - missing_after} missing values")
            if missing_after > 0:
                print(f"[Warning] {missing_after} missing values remain after filling")
        
        return df_filled
    
    def _remove_outliers(self, df: pd.DataFrame) -> pd.DataFrame:
        """Remove outliers based on standard deviation threshold."""
        
        outlier_count = 0
        df_clean = df.copy()
        
        for column in df.columns:
            if df[column].dtype in ['float64', 'float32', 'int64', 'int32']:
                mean_val = df[column].mean()
                std_val = df[column].std()
                
                if std_val > 0:
                    outlier_mask = np.abs(df[column] - mean_val) > (self.config.outlier_threshold * std_val)
                    outlier_count += outlier_mask.sum()
                    df_clean.loc[outlier_mask, column] = np.nan
        
        if outlier_count > 0:
            print(f"[ConfigurableTrainingDataGenerator] Removed {outlier_count} outlier values")
        
        return df_clean
    
    def _apply_scaling(self, df: pd.DataFrame, data_type: str) -> pd.DataFrame:
        """Apply scaling to features or labels."""
        
        scaling_method = (self.config.feature_scaling_method if data_type == 'features' 
                         else self.config.label_scaling_method)
        
        if scaling_method == 'none':
            return df
        
        from sklearn.preprocessing import StandardScaler, RobustScaler, MinMaxScaler
        
        if scaling_method == 'standard':
            scaler = StandardScaler()
        elif scaling_method == 'robust':
            scaler = RobustScaler()
        elif scaling_method == 'minmax':
            scaler = MinMaxScaler()
        else:
            print(f"[Warning] Unknown scaling method: {scaling_method}")
            return df
        
        # Store scaler for later inverse transformation
        if data_type not in self.feature_scalers:
            self.feature_scalers[data_type] = {}
        
        df_scaled = df.copy()
        
        for column in df.columns:
            if df[column].dtype in ['float64', 'float32', 'int64', 'int32']:
                column_data = df[column].values.reshape(-1, 1)
                valid_mask = ~np.isnan(column_data.flatten())
                
                if valid_mask.sum() > 0:
                    scaler.fit(column_data[valid_mask].reshape(-1, 1))
                    scaled_values = scaler.transform(column_data)
                    df_scaled[column] = scaled_values.flatten()
                    
                    # Store scaler
                    self.feature_scalers[data_type][column] = scaler
        
        print(f"[ConfigurableTrainingDataGenerator] Applied {scaling_method} scaling to {data_type}")
        return df_scaled
    
    def _format_output(self, features: List, labels: List, masks: List, 
                      symbols: Optional[List[str]] = None, 
                      data_info: Optional[Dict] = None) -> Dict[str, Any]:
        """Format output according to configuration and generate metadata."""
        
        # Convert to numpy arrays
        features_array = np.array(features, dtype=np.float32)
        labels_array = np.array(labels, dtype=np.float32)
        
        # Create combined masks
        feature_masks = np.array([m['features'] for m in masks], dtype=np.float32)
        label_masks = np.array([m['labels'] for m in masks], dtype=np.float32)
        
        print(f"[ConfigurableTrainingDataGenerator] Output shapes:")
        print(f"  Features: {features_array.shape}")
        print(f"  Labels: {labels_array.shape}")
        print(f"  Feature masks: {feature_masks.shape}")
        print(f"  Label masks: {label_masks.shape}")
        
        # Generate unique dataset ID
        self.dataset_id = f"dataset_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        # Save training data files
        data_files = self._save_training_data(features_array, labels_array, feature_masks, label_masks)
        
        # Generate and save metadata
        metadata = self._generate_metadata(
            features_array, labels_array, 
            symbols or ['unknown'], 
            data_info or {},
            data_files
        )
        metadata_file = self.metadata_manager.save_metadata(metadata, f"{self.dataset_id}_metadata.json")
        
        # Create sample IDs for tracking
        sample_ids = [f"{self.dataset_id}_sample_{i:06d}" for i in range(features_array.shape[0])]
        
        result = {
            'features': features_array,
            'labels': labels_array,
            'feature_masks': feature_masks,
            'label_masks': label_masks,
            'feature_names': self.feature_names,
            'label_names': self.label_names,
            'config': self.config,
            'metadata': metadata,
            'dataset_id': self.dataset_id,
            'sample_ids': sample_ids,
            'data_files': data_files,
            'metadata_file': metadata_file
        }
        
        if self.config.output_format == 'pytorch':
            if not TORCH_AVAILABLE:
                raise ImportError("PyTorch is not available. Install torch or use 'numpy' or 'pandas' output format.")
            result['features'] = torch.tensor(features_array, device=self.config.device)
            result['labels'] = torch.tensor(labels_array, device=self.config.device)
            result['feature_masks'] = torch.tensor(feature_masks, device=self.config.device)
            result['label_masks'] = torch.tensor(label_masks, device=self.config.device)
        
        elif self.config.output_format == 'pandas':
            # Convert back to DataFrames (flattened)
            n_samples, seq_len, n_features = features_array.shape
            features_flat = features_array.reshape(-1, n_features)
            feature_df = pd.DataFrame(features_flat, columns=self.feature_names)
            result['features'] = feature_df
            
            n_samples, pred_len, n_labels = labels_array.shape
            labels_flat = labels_array.reshape(-1, n_labels)
            label_df = pd.DataFrame(labels_flat, columns=self.label_names)
            result['labels'] = label_df
        
        return result
    
    def _save_training_data(self, features: np.ndarray, labels: np.ndarray, 
                           feature_masks: np.ndarray, label_masks: np.ndarray) -> Dict[str, str]:
        """Save training data arrays to files."""
        
        data_files = {}
        
        # Save features
        features_file = os.path.join(self.output_dir, f"{self.dataset_id}_features.npy")
        np.save(features_file, features)
        data_files['features'] = features_file
        
        # Save labels  
        labels_file = os.path.join(self.output_dir, f"{self.dataset_id}_labels.npy")
        np.save(labels_file, labels)
        data_files['labels'] = labels_file
        
        # Save masks
        feature_masks_file = os.path.join(self.output_dir, f"{self.dataset_id}_feature_masks.npy")
        np.save(feature_masks_file, feature_masks)
        data_files['feature_masks'] = feature_masks_file
        
        label_masks_file = os.path.join(self.output_dir, f"{self.dataset_id}_label_masks.npy")
        np.save(label_masks_file, label_masks)
        data_files['label_masks'] = label_masks_file
        
        # Save feature and label names
        names_file = os.path.join(self.output_dir, f"{self.dataset_id}_names.json")
        with open(names_file, 'w') as f:
            json.dump({
                'feature_names': self.feature_names,
                'label_names': self.label_names
            }, f, indent=2)
        data_files['names'] = names_file
        
        print(f"[ConfigurableTrainingDataGenerator] Saved training data to {self.output_dir}")
        return data_files
    
    def _generate_metadata(self, features: np.ndarray, labels: np.ndarray, 
                          symbols: List[str], data_info: Dict, 
                          data_files: Dict[str, str]) -> TrainingDataMetadata:
        """Generate comprehensive metadata for the training dataset."""
        
        # Get feature and label configurations
        feature_configs = [config.__dict__ if hasattr(config, '__dict__') else config 
                          for config in self.feature_registry.features]
        label_configs = [config.__dict__ if hasattr(config, '__dict__') else config 
                        for config in self.label_registry.labels]
        
        # Determine date range from data_info
        date_range = data_info.get('date_range', {
            'start': datetime.now().strftime('%Y-%m-%d'),
            'end': datetime.now().strftime('%Y-%m-%d')
        })
        
        metadata = self.metadata_manager.create_training_metadata(
            dataset_name=self.dataset_id,
            features_data=features,
            labels_data=labels,
            feature_names=self.feature_names,
            label_names=self.label_names,
            feature_configs=feature_configs,
            label_configs=label_configs,
            symbols=symbols,
            date_range=date_range,
            data_file_path=data_files.get('features'),
            feature_file_path=data_files.get('features'),
            label_file_path=data_files.get('labels'),
            gin_config_path=data_info.get('gin_config_path'),
            generation_parameters={
                'sequence_length': self.config.sequence_length,
                'prediction_horizon': self.config.prediction_horizon,
                'normalize_features': self.config.normalize_features,
                'normalize_labels': self.config.normalize_labels,
                'remove_outliers': self.config.remove_outliers,
                'min_valid_ratio': self.config.min_valid_ratio,
                'output_format': self.config.output_format
            },
            data_sources=data_info.get('data_sources', ['unknown'])
        )
        
        return metadata
    
    def update_data_info(self, data: pd.DataFrame, symbols: Optional[List[str]] = None) -> Dict:
        """Extract data information for metadata generation."""
        
        data_info = {}
        
        # Date range
        if isinstance(data.index, pd.DatetimeIndex):
            data_info['date_range'] = {
                'start': data.index.min().strftime('%Y-%m-%d'),
                'end': data.index.max().strftime('%Y-%m-%d')
            }
        
        # Symbols
        if symbols:
            data_info['symbols'] = symbols
        elif 'symbol' in data.columns:
            data_info['symbols'] = data['symbol'].unique().tolist()
        else:
            data_info['symbols'] = ['unknown']
        
        # Data sources (inferred from column names or metadata)
        data_sources = []
        if any(col.startswith('polygon_') for col in data.columns):
            data_sources.append('polygon')
        if any(col.startswith('tiingo_') for col in data.columns):
            data_sources.append('tiingo')
        if not data_sources:
            data_sources = ['unknown']
        data_info['data_sources'] = data_sources
        
        return data_info

class ConfigurableTrainDataCallback(RunnerCallback):
    """Callback for collecting data during IndicatorRunner execution."""
    
    def __init__(self, 
                 config: ConfigurableTrainingDataConfig,
                 output_path: str = "configurable_train_data.pt"):
        self.config = config
        self.output_path = output_path
        self.generator = ConfigurableTrainingDataGenerator(config)
        self.collected_data = []
    
    def handleStart(self, runner, current_time):
        """Initialize data collection."""
        self.collected_data = []
        print(f"[ConfigurableTrainDataCallback] Starting data collection")
    
    def handleInterval(self, runner, current_time):
        """Collect data for each interval."""
        
        # Get market data manager
        manager = runner.market_data_manager
        if manager is None:
            return
        
        # Get instrument IDs
        um = runner.get_universe_manager()
        instrument_ids = getattr(um, 'instrument_ids', []) if um else []
        
        if not instrument_ids:
            return
        
        current_date = current_time.date()
        
        # Collect OHLCV data for all instruments
        for instrument_id in instrument_ids:
            try:
                # Get OHLC data (async call made sync through manager)
                ohlc = asyncio.run(manager.get_ohlc(
                    instrument_id, 
                    current_time, 
                    current_time, 
                    current_date=current_date
                ))
                
                if ohlc is None:
                    continue
                
                # Get symbol mapping
                symbol = getattr(manager, '_id_to_symbol', {}).get(instrument_id, str(instrument_id))
                
                # Create data row
                row = {
                    'date': current_date,
                    'symbol': symbol,
                    'instrument_id': instrument_id,
                    **ohlc
                }
                
                self.collected_data.append(row)
                
            except Exception as e:
                print(f"[Error] Failed to collect data for instrument {instrument_id}: {e}")
    
    def handleEnd(self, runner, current_time):
        """Generate training data and save to file."""
        
        if not self.collected_data:
            print("[ConfigurableTrainDataCallback] No data collected")
            return
        
        # Convert to DataFrame
        df = pd.DataFrame(self.collected_data)
        df['date'] = pd.to_datetime(df['date'])
        df = df.set_index('date').sort_index()
        
        print(f"[ConfigurableTrainDataCallback] Collected {len(df)} data points")
        print(f"[ConfigurableTrainDataCallback] Symbols: {df['symbol'].unique()}")
        print(f"[ConfigurableTrainDataCallback] Date range: {df.index.min()} to {df.index.max()}")
        
        try:
            # Generate training data
            result = self.generator.generate_training_data(df)
            
            # Save to file
            if self.config.output_format == 'pytorch':
                if not TORCH_AVAILABLE:
                    raise ImportError("PyTorch is not available. Install torch or use 'numpy' or 'pandas' output format.")
                torch.save(result, self.output_path)
            else:
                import pickle
                with open(self.output_path, 'wb') as f:
                    pickle.dump(result, f)
            
            print(f"[ConfigurableTrainDataCallback] Saved training data to {self.output_path}")
            
        except Exception as e:
            print(f"[Error] Failed to generate training data: {e}")
            import traceback
            traceback.print_exc()

# Gin configuration helpers
@gin.configurable
def create_configurable_training_data_config(
    sequence_length: int = 60,
    prediction_horizon: int = 5,
    feature_registry: Optional[FeatureRegistry] = None,
    label_registry: Optional[LabelRegistry] = None,
    normalize_features: bool = True,
    normalize_labels: bool = False,
    **kwargs) -> ConfigurableTrainingDataConfig:
    """Create configurable training data config - gin configurable."""
    
    return ConfigurableTrainingDataConfig(
        sequence_length=sequence_length,
        prediction_horizon=prediction_horizon,
        feature_registry=feature_registry,
        label_registry=label_registry,
        normalize_features=normalize_features,
        normalize_labels=normalize_labels,
        **kwargs
    )