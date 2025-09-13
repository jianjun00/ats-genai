"""
Autonomous Driving Inspired Data Preprocessing for Financial Transformers

This module handles multi-timeframe financial data similar to how autonomous driving
systems process multi-sensor inputs. Each timeframe is treated as a different "sensor"
providing unique perspectives of market dynamics.
"""

import numpy as np
import pandas as pd
import torch
import torch.nn as nn
from torch.utils.data import Dataset, DataLoader
from pathlib import Path
from typing import Dict, List, Tuple, Optional, Any
import json
import logging
from datetime import datetime
from dataclasses import dataclass
import array_record

logger = logging.getLogger(__name__)


@dataclass
class TimeframeConfig:
    """Configuration for each timeframe 'sensor'"""
    name: str  # e.g., '5m', '15m', '1h', '1d', '1w'
    sequence_length: int  # Number of bars in sequence
    features: List[str]  # Feature names: ['open', 'high', 'low', 'close', 'volume', 'vwap']
    importance_weight: float = 1.0  # Relative importance for this timeframe


class MarketPositionEncoder(nn.Module):
    """
    Autonomous driving inspired position encoding for financial data.

    Similar to how AV systems encode 3D positions and temporal offsets,
    this encodes market data positions with:
    - timestamp_offset: Relative time from prediction point
    - timeframe_id: Which timeframe this data belongs to
    - bar_index: Position within the timeframe sequence
    - market_regime: Bull/bear/sideways state encoding
    """

    def __init__(self, d_model: int = 256, max_seq_length: int = 100):
        super().__init__()
        self.d_model = d_model
        self.max_seq_length = max_seq_length

        # Timeframe ID embeddings (like camera/sensor IDs in AV)
        self.timeframe_embeddings = nn.Embedding(10, d_model // 4)  # Support up to 10 timeframes

        # Bar index positional encoding
        self.position_embeddings = nn.Embedding(max_seq_length, d_model // 4)

        # Timestamp offset encoding (relative time)
        self.temporal_embeddings = nn.Embedding(1000, d_model // 4)  # Support large time offsets

        # Market regime encoding
        self.regime_embeddings = nn.Embedding(4, d_model // 4)  # Bull, Bear, Sideways, Transition

    def forward(self, timeframe_ids: torch.Tensor, bar_indices: torch.Tensor,
                temporal_offsets: torch.Tensor, market_regimes: torch.Tensor) -> torch.Tensor:
        """
        Args:
            timeframe_ids: [batch, seq_len] - ID for each timeframe (0=5m, 1=15m, etc.)
            bar_indices: [batch, seq_len] - Position within timeframe sequence
            temporal_offsets: [batch, seq_len] - Relative time from prediction point
            market_regimes: [batch, seq_len] - Market regime encoding

        Returns:
            position_encoding: [batch, seq_len, d_model]
        """
        tf_embed = self.timeframe_embeddings(timeframe_ids)      # [batch, seq_len, d_model//4]
        pos_embed = self.position_embeddings(bar_indices)       # [batch, seq_len, d_model//4]
        temp_embed = self.temporal_embeddings(temporal_offsets) # [batch, seq_len, d_model//4]
        regime_embed = self.regime_embeddings(market_regimes)   # [batch, seq_len, d_model//4]

        # Concatenate all position encodings
        position_encoding = torch.cat([tf_embed, pos_embed, temp_embed, regime_embed], dim=-1)

        return position_encoding


class TimeframeVariableSelector(nn.Module):
    """
    Variable selection network for each timeframe, inspired by Temporal Fusion Transformer.

    Learns which features (OHLCV, technical indicators) are most important
    for each timeframe, similar to how AV systems weight different sensor inputs.
    """

    def __init__(self, input_size: int, hidden_size: int = 64):
        super().__init__()
        self.input_size = input_size

        # Gated Residual Network for feature selection
        self.feature_selector = nn.Sequential(
            nn.Linear(input_size, hidden_size),
            nn.ReLU(),
            nn.Dropout(0.1),
            nn.Linear(hidden_size, hidden_size),
            nn.ReLU(),
            nn.Dropout(0.1),
            nn.Linear(hidden_size, input_size),
            nn.Sigmoid()  # Importance weights between 0-1
        )

        # Residual connection
        self.layer_norm = nn.LayerNorm(input_size)

    def forward(self, x: torch.Tensor) -> Tuple[torch.Tensor, torch.Tensor]:
        """
        Args:
            x: [batch, seq_len, input_size] - Raw timeframe features

        Returns:
            selected_features: [batch, seq_len, input_size] - Weighted features
            importance_weights: [batch, seq_len, input_size] - Feature importance
        """
        # Compute importance weights
        importance_weights = self.feature_selector(x)

        # Apply importance weighting with residual connection
        selected_features = x * importance_weights
        selected_features = self.layer_norm(selected_features + x * 0.1)  # Small residual

        return selected_features, importance_weights


class MultiTimeframeProcessor:
    """
    Processes multi-timeframe ArrayRecord data into transformer-ready tensors.

    Similar to autonomous driving sensor fusion, this combines multiple timeframe
    "sensors" into a unified representation for the transformer model.
    """

    def __init__(self, timeframe_configs: List[TimeframeConfig]):
        self.timeframe_configs = {cfg.name: cfg for cfg in timeframe_configs}
        self.timeframe_ids = {name: idx for idx, name in enumerate(self.timeframe_configs.keys())}

        logger.info(f"Initialized MultiTimeframeProcessor with {len(timeframe_configs)} timeframes")
        for cfg in timeframe_configs:
            logger.info(f"  {cfg.name}: {cfg.sequence_length} bars, {len(cfg.features)} features")

    def load_arrayrecord_data(self, arrayrecord_path: Path, columns_path: Path) -> pd.DataFrame:
        """Load data from ArrayRecord file with column information."""
        try:
            # Load column names
            with open(columns_path, 'r') as f:
                columns = json.load(f)

            # Read ArrayRecord data
            reader = array_record.ArrayRecordReader(str(arrayrecord_path))

            # First record contains column names (string), skip it
            records = []
            for i, record in enumerate(reader):
                if i == 0:
                    continue  # Skip column names record

                # Convert bytes back to numpy array
                data = np.frombuffer(record, dtype=np.float32)
                records.append(data)

            if not records:
                raise ValueError("No data records found in ArrayRecord file")

            # Convert to DataFrame
            data_array = np.vstack(records)
            df = pd.DataFrame(data_array, columns=columns)

            logger.info(f"Loaded {len(df)} records from {arrayrecord_path}")
            return df

        except Exception as e:
            logger.error(f"Failed to load ArrayRecord data from {arrayrecord_path}: {e}")
            raise

    def extract_timeframe_sequences(self, df: pd.DataFrame) -> Dict[str, torch.Tensor]:
        """
        Extract and organize sequences by timeframe from flattened DataFrame.

        Args:
            df: DataFrame with flattened multi-timeframe data

        Returns:
            timeframe_sequences: Dict mapping timeframe -> [batch, seq_len, features]
        """
        timeframe_sequences = {}

        for tf_name, tf_config in self.timeframe_configs.items():
            # Extract columns for this timeframe
            tf_columns = []
            for feature in tf_config.features:
                for i in range(tf_config.sequence_length):
                    col_name = f"{tf_name}_{feature}_{i:03d}"
                    if col_name in df.columns:
                        tf_columns.append(col_name)

            if not tf_columns:
                logger.warning(f"No columns found for timeframe {tf_name}")
                continue

            # Extract data for this timeframe
            tf_data = df[tf_columns].values  # [batch, flattened_features]

            # Reshape into sequences: [batch, seq_len, features]
            batch_size = tf_data.shape[0]
            seq_len = tf_config.sequence_length
            num_features = len(tf_config.features)

            # Reshape from [batch, seq_len * features] to [batch, seq_len, features]
            tf_sequences = tf_data.reshape(batch_size, seq_len, num_features)

            timeframe_sequences[tf_name] = torch.tensor(tf_sequences, dtype=torch.float32)

            logger.debug(f"Extracted {tf_name} sequences: {tf_sequences.shape}")

        return timeframe_sequences

    def create_position_encodings(self, timeframe_sequences: Dict[str, torch.Tensor],
                                prediction_timestamp: Optional[datetime] = None) -> Dict[str, Dict[str, torch.Tensor]]:
        """
        Create autonomous driving style position encodings for each timeframe.

        Args:
            timeframe_sequences: Multi-timeframe sequence data
            prediction_timestamp: Target prediction time (for temporal offsets)

        Returns:
            position_data: Dict mapping timeframe -> position encoding components
        """
        position_data = {}

        for tf_name, sequences in timeframe_sequences.items():
            batch_size, seq_len, _ = sequences.shape
            tf_id = self.timeframe_ids[tf_name]

            # Timeframe IDs (constant for each timeframe)
            timeframe_ids = torch.full((batch_size, seq_len), tf_id, dtype=torch.long)

            # Bar indices within sequence
            bar_indices = torch.arange(seq_len).unsqueeze(0).repeat(batch_size, 1)

            # Temporal offsets (mock for now - would use real timestamps)
            # In real implementation, calculate relative to prediction_timestamp
            temporal_offsets = torch.arange(seq_len, 0, -1).unsqueeze(0).repeat(batch_size, 1)

            # Market regime (mock - would detect from actual price data)
            # 0=Bull, 1=Bear, 2=Sideways, 3=Transition
            market_regimes = torch.zeros((batch_size, seq_len), dtype=torch.long)  # Mock as sideways

            position_data[tf_name] = {
                'timeframe_ids': timeframe_ids,
                'bar_indices': bar_indices,
                'temporal_offsets': temporal_offsets,
                'market_regimes': market_regimes
            }

        return position_data

    def create_prediction_targets(self, df: pd.DataFrame, target_hours: int = 10) -> torch.Tensor:
        """
        Create prediction targets for next N hours of price movement.

        For now, creates mock targets. In real implementation, would use
        forward-looking price data from the training dataset.

        Args:
            df: Input DataFrame
            target_hours: Number of hours to predict ahead

        Returns:
            targets: [batch, target_hours, 1] - Price movement targets
        """
        batch_size = len(df)

        # Mock targets - would compute real future price movements
        targets = torch.randn(batch_size, target_hours, 1) * 0.02  # ±2% price movements

        logger.debug(f"Created mock prediction targets: {targets.shape}")
        return targets


class AutonomousFinanceDataset(Dataset):
    """
    PyTorch Dataset for multi-timeframe financial data.

    Treats each timeframe as a different "sensor" modality, similar to
    autonomous driving datasets that combine camera, LiDAR, radar, etc.
    """

    def __init__(self, data_dir: Path, processor: MultiTimeframeProcessor,
                 symbol: str = "AAPL", sequence_id: Optional[str] = None):
        self.data_dir = Path(data_dir)
        self.processor = processor
        self.symbol = symbol
        self.sequence_id = sequence_id

        # Load data for all timeframes
        self.data_cache = {}
        self.metadata_cache = {}

        self._load_all_timeframes()

    def _load_all_timeframes(self):
        """Load ArrayRecord data for all timeframes."""
        if self.sequence_id:
            sequence_dir = self.data_dir / self.sequence_id
        else:
            # Find AAPL sequence directory
            sequence_dirs = list(self.data_dir.glob(f"{self.symbol}_*"))
            if not sequence_dirs:
                raise ValueError(f"No sequence directories found for {self.symbol} in {self.data_dir}")
            sequence_dir = sequence_dirs[0]  # Take first match

        logger.info(f"Loading data from sequence directory: {sequence_dir}")

        for tf_name in self.processor.timeframe_configs.keys():
            tf_dir = sequence_dir / tf_name
            if not tf_dir.exists():
                logger.warning(f"Timeframe directory not found: {tf_dir}")
                continue

            # Find ArrayRecord file and column file
            arrayrecord_files = list(tf_dir.glob("*.arrayrecord"))
            columns_files = list(tf_dir.glob("*_columns.json"))

            if not arrayrecord_files or not columns_files:
                logger.warning(f"Missing files in {tf_dir}")
                continue

            arrayrecord_path = arrayrecord_files[0]
            columns_path = columns_files[0]

            # Load data
            try:
                df = self.processor.load_arrayrecord_data(arrayrecord_path, columns_path)
                self.data_cache[tf_name] = df

                # Load metadata
                metadata_files = list(tf_dir.glob("*_metadata.json"))
                if metadata_files:
                    with open(metadata_files[0], 'r') as f:
                        self.metadata_cache[tf_name] = json.load(f)

            except Exception as e:
                logger.error(f"Failed to load {tf_name} data: {e}")

        logger.info(f"Loaded {len(self.data_cache)} timeframes")

    def __len__(self) -> int:
        """Return length based on first available timeframe."""
        if not self.data_cache:
            return 0

        first_tf = next(iter(self.data_cache.values()))
        return len(first_tf)

    def __getitem__(self, idx: int) -> Dict[str, Any]:
        """
        Get a single training sample with all timeframe data.

        Returns:
            sample: Dict containing:
                - timeframe_sequences: Multi-timeframe data
                - position_encodings: Position encoding components
                - targets: Prediction targets
                - metadata: Sample metadata
        """
        sample = {
            'timeframe_data': {},
            'position_data': {},
            'metadata': {
                'symbol': self.symbol,
                'index': idx,
                'timeframes': list(self.data_cache.keys())
            }
        }

        # Extract single rows from each timeframe
        single_row_data = {}
        for tf_name, df in self.data_cache.items():
            if idx < len(df):
                single_row_data[tf_name] = df.iloc[[idx]]  # Keep as DataFrame

        if not single_row_data:
            raise IndexError(f"Index {idx} out of range")

        # Process timeframe sequences
        # Note: Each "sample" is actually one row with all timeframes
        combined_df = pd.concat([df.assign(timeframe=tf_name) for tf_name, df in single_row_data.items()],
                               ignore_index=True)

        # For now, process the first row from first timeframe
        first_tf_name = next(iter(single_row_data.keys()))
        first_df = single_row_data[first_tf_name]

        timeframe_sequences = self.processor.extract_timeframe_sequences(first_df)
        position_data = self.processor.create_position_encodings(timeframe_sequences)
        targets = self.processor.create_prediction_targets(first_df)

        sample.update({
            'timeframe_sequences': timeframe_sequences,
            'position_data': position_data,
            'targets': targets
        })

        return sample


class AutonomousFinanceDataLoader:
    """
    DataLoader factory for autonomous driving inspired financial data.

    Creates DataLoaders that handle multi-timeframe financial data similar to
    how autonomous driving systems load multi-sensor data streams.
    """

    def __init__(self, data_dir: Path, batch_size: int = 32, num_workers: int = 4):
        self.data_dir = Path(data_dir)
        self.batch_size = batch_size
        self.num_workers = num_workers

        # Default timeframe configuration (matches our training data)
        self.timeframe_configs = [
            TimeframeConfig('5m', 52, ['open', 'high', 'low', 'close', 'volume', 'vwap'], 1.0),
            TimeframeConfig('15m', 52, ['open', 'high', 'low', 'close', 'volume', 'vwap'], 0.8),
            TimeframeConfig('1h', 24, ['open', 'high', 'low', 'close', 'volume', 'vwap'], 0.6),
            TimeframeConfig('1d', 20, ['open', 'high', 'low', 'close', 'volume', 'vwap'], 0.4),
            TimeframeConfig('1w', 12, ['open', 'high', 'low', 'close', 'volume', 'vwap'], 0.2)
        ]

        self.processor = MultiTimeframeProcessor(self.timeframe_configs)

    def create_train_loader(self, symbol: str = "AAPL", sequence_id: Optional[str] = None) -> DataLoader:
        """Create training DataLoader."""
        dataset = AutonomousFinanceDataset(
            self.data_dir, self.processor, symbol, sequence_id
        )

        return DataLoader(
            dataset,
            batch_size=self.batch_size,
            shuffle=True,
            num_workers=self.num_workers,
            pin_memory=True,
            collate_fn=self._collate_fn
        )

    def create_val_loader(self, symbol: str = "AAPL", sequence_id: Optional[str] = None) -> DataLoader:
        """Create validation DataLoader."""
        dataset = AutonomousFinanceDataset(
            self.data_dir, self.processor, symbol, sequence_id
        )

        return DataLoader(
            dataset,
            batch_size=self.batch_size,
            shuffle=False,
            num_workers=self.num_workers,
            pin_memory=True,
            collate_fn=self._collate_fn
        )

    def _collate_fn(self, batch: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Custom collate function for multi-timeframe batches.

        Combines individual samples into batched tensors, handling the
        multi-timeframe structure properly.
        """
        if not batch:
            return {}

        # Get all timeframe names from first sample
        first_sample = batch[0]
        timeframe_names = list(first_sample['timeframe_sequences'].keys())

        collated = {
            'timeframe_sequences': {},
            'position_data': {},
            'targets': [],
            'metadata': []
        }

        # Collate timeframe sequences
        for tf_name in timeframe_names:
            sequences = [sample['timeframe_sequences'][tf_name] for sample in batch
                        if tf_name in sample['timeframe_sequences']]
            if sequences:
                collated['timeframe_sequences'][tf_name] = torch.stack(sequences)

        # Collate position data
        for tf_name in timeframe_names:
            if tf_name not in collated['position_data']:
                collated['position_data'][tf_name] = {}

            for pos_key in ['timeframe_ids', 'bar_indices', 'temporal_offsets', 'market_regimes']:
                pos_data = [sample['position_data'][tf_name][pos_key] for sample in batch
                           if tf_name in sample['position_data'] and pos_key in sample['position_data'][tf_name]]
                if pos_data:
                    collated['position_data'][tf_name][pos_key] = torch.stack(pos_data)

        # Collate targets
        targets = [sample['targets'] for sample in batch if 'targets' in sample]
        if targets:
            collated['targets'] = torch.stack(targets)

        # Collect metadata
        collated['metadata'] = [sample['metadata'] for sample in batch if 'metadata' in sample]

        return collated


if __name__ == "__main__":
    # Test data loading with our actual training data
    logging.basicConfig(level=logging.INFO)

    data_dir = Path("/mnt/d/ats-data/training_data/83")

    # Create data loader
    data_loader_factory = AutonomousFinanceDataLoader(data_dir, batch_size=2)
    train_loader = data_loader_factory.create_train_loader("AAPL")

    # Test loading a batch
    print("Testing AutonomousFinanceDataLoader...")
    for batch_idx, batch in enumerate(train_loader):
        print(f"\nBatch {batch_idx}:")
        print(f"Timeframe sequences: {list(batch['timeframe_sequences'].keys())}")

        for tf_name, sequences in batch['timeframe_sequences'].items():
            print(f"  {tf_name}: {sequences.shape}")

        print(f"Targets: {batch['targets'].shape}")
        print(f"Metadata: {len(batch['metadata'])} samples")

        if batch_idx >= 2:  # Test a few batches
            break

    print("\n✅ Data loading test completed successfully!")