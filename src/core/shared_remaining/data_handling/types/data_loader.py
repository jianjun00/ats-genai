"""
Data Loading and Preprocessing for TFT Models

This module provides data loading and preprocessing utilities for the Temporal Fusion
Transformer, including integration with sentiment analysis features and proper sequence
preparation for multi-horizon forecasting.
"""

import logging
from typing import Dict, List, Tuple, Optional
from dataclasses import dataclass
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import torch
from torch.utils.data import Dataset, DataLoader
import asyncpg

from sentiment.sentiment_integrator import SentimentIntegrator, analyze_unified_sentiment

logger = logging.getLogger(__name__)


@dataclass
class TFTDataConfig:
    """Configuration for TFT data loading."""

    # Sequence configuration
    encoder_length: int = 120  # 2 hours of 1-minute data
    prediction_length: int = 30  # 30 minutes ahead

    # Feature configuration
    temporal_features: List[str] = None
    target_features: List[str] = None
    scaling_features: List[str] = None

    # Data filtering
    min_sequence_length: int = 150  # encoder + prediction
    max_missing_ratio: float = 0.1

    # Sentiment integration
    use_sentiment: bool = True
    sentiment_lookback_hours: int = 24

    # Normalization
    normalize_features: bool = True
    target_normalization: str = "standard"  # "standard", "minmax", "robust"

    def __post_init__(self):
        if self.temporal_features is None:
            self.temporal_features = [
                'open', 'high', 'low', 'close', 'volume',
                'returns', 'volatility', 'rsi', 'macd', 'bollinger_upper',
                'bollinger_lower', 'ema_12', 'ema_26', 'atr'
            ]
        if self.target_features is None:
            self.target_features = ['returns']
        if self.scaling_features is None:
            self.scaling_features = self.temporal_features + self.target_features


class FeatureNormalizer:
    """Feature normalization for TFT data."""

    def __init__(self, method: str = "standard"):
        self.method = method
        self.feature_stats = {}
        self.is_fitted = False

    def fit(self, data: pd.DataFrame, features: List[str]):
        """Fit normalization parameters."""
        self.feature_stats = {}

        for feature in features:
            if feature not in data.columns:
                continue

            values = data[feature].dropna()

            if self.method == "standard":
                self.feature_stats[feature] = {
                    'mean': values.mean(),
                    'std': values.std() + 1e-8  # Avoid division by zero
                }
            elif self.method == "minmax":
                self.feature_stats[feature] = {
                    'min': values.min(),
                    'max': values.max(),
                    'range': values.max() - values.min() + 1e-8
                }
            elif self.method == "robust":
                self.feature_stats[feature] = {
                    'median': values.median(),
                    'iqr': values.quantile(0.75) - values.quantile(0.25) + 1e-8
                }

        self.is_fitted = True
        logger.info(f"Fitted {self.method} normalizer for {len(self.feature_stats)} features")

    def transform(self, data: pd.DataFrame, features: List[str]) -> pd.DataFrame:
        """Transform features using fitted parameters."""
        if not self.is_fitted:
            raise ValueError("Normalizer must be fitted before transform")

        data = data.copy()

        for feature in features:
            if feature not in data.columns or feature not in self.feature_stats:
                continue

            stats = self.feature_stats[feature]

            if self.method == "standard":
                data[feature] = (data[feature] - stats['mean']) / stats['std']
            elif self.method == "minmax":
                data[feature] = (data[feature] - stats['min']) / stats['range']
            elif self.method == "robust":
                data[feature] = (data[feature] - stats['median']) / stats['iqr']

        return data

    def inverse_transform(self, data: np.ndarray, feature: str) -> np.ndarray:
        """Inverse transform for a specific feature."""
        if feature not in self.feature_stats:
            return data

        stats = self.feature_stats[feature]

        if self.method == "standard":
            return data * stats['std'] + stats['mean']
        elif self.method == "minmax":
            return data * stats['range'] + stats['min']
        elif self.method == "robust":
            return data * stats['iqr'] + stats['median']

        return data


class TFTDataset(Dataset):
    """PyTorch Dataset for TFT training."""

    def __init__(
        self,
        data: pd.DataFrame,
        config: TFTDataConfig,
        normalizer: FeatureNormalizer = None,
        sentiment_features: pd.DataFrame = None
    ):
        self.data = data
        self.config = config
        self.normalizer = normalizer
        self.sentiment_features = sentiment_features

        # Prepare sequences
        self.sequences = self._prepare_sequences()
        logger.info(f"Created TFT dataset with {len(self.sequences)} sequences")

    def _prepare_sequences(self) -> List[Dict]:
        """Prepare sequences for training."""
        sequences = []
        total_length = self.config.encoder_length + self.config.prediction_length

        # Group by symbol
        for symbol in self.data['symbol'].unique():
            symbol_data = self.data[self.data['symbol'] == symbol].sort_values('timestamp')

            if len(symbol_data) < total_length:
                continue

            # Create overlapping sequences
            for i in range(len(symbol_data) - total_length + 1):
                sequence_data = symbol_data.iloc[i:i + total_length]

                # Check for missing data
                missing_ratio = sequence_data[self.config.temporal_features].isnull().sum().sum()
                missing_ratio /= len(sequence_data) * len(self.config.temporal_features)

                if missing_ratio > self.config.max_missing_ratio:
                    continue

                # Extract encoder and decoder data
                encoder_end = self.config.encoder_length

                encoder_data = sequence_data.iloc[:encoder_end]
                decoder_data = sequence_data.iloc[encoder_end:]
                target_data = decoder_data[self.config.target_features]

                # Prepare sequence dictionary
                sequence = {
                    'symbol': symbol,
                    'start_time': encoder_data['timestamp'].iloc[0],
                    'end_time': decoder_data['timestamp'].iloc[-1],
                    'encoder_data': encoder_data[self.config.temporal_features],
                    'decoder_data': decoder_data[self.config.temporal_features],
                    'targets': target_data,
                    'encoder_length': len(encoder_data)
                }

                # Add sentiment features if available
                if self.config.use_sentiment and self.sentiment_features is not None:
                    sentiment_seq = self._get_sentiment_sequence(
                        symbol, encoder_data['timestamp'].iloc[0],
                        decoder_data['timestamp'].iloc[-1]
                    )
                    sequence['sentiment_features'] = sentiment_seq

                sequences.append(sequence)

        return sequences

    def _get_sentiment_sequence(
        self,
        symbol: str,
        start_time: datetime,
        end_time: datetime
    ) -> Optional[np.ndarray]:
        """Get sentiment features for the sequence."""
        if self.sentiment_features is None:
            return None

        # Filter sentiment data for the symbol and time range
        mask = (
            (self.sentiment_features['symbol'] == symbol) &
            (self.sentiment_features['created_at'] >= start_time) &
            (self.sentiment_features['created_at'] <= end_time)
        )

        sentiment_data = self.sentiment_features[mask]

        if len(sentiment_data) == 0:
            # Return zeros if no sentiment data available
            total_length = self.config.encoder_length + self.config.prediction_length
            return np.zeros((total_length, 23))  # 23 sentiment features

        # Interpolate sentiment features to match sequence length
        sentiment_features = sentiment_data[
            ['sentiment_score', 'confidence', 'momentum', 'volume_indicator',
             'consensus_score', 'divergence_score', 'risk_score'] +
            [f'feature_{i}' for i in range(1, 17)]  # Additional ML features
        ].values

        # Simple interpolation (could be improved)
        total_length = self.config.encoder_length + self.config.prediction_length
        if len(sentiment_features) == total_length:
            return sentiment_features

        # Pad or interpolate to match sequence length
        if len(sentiment_features) < total_length:
            # Pad with last value
            padding = np.tile(sentiment_features[-1], (total_length - len(sentiment_features), 1))
            return np.vstack([sentiment_features, padding])
        else:
            # Subsample
            indices = np.linspace(0, len(sentiment_features) - 1, total_length, dtype=int)
            return sentiment_features[indices]

    def __len__(self):
        return len(self.sequences)

    def __getitem__(self, idx):
        sequence = self.sequences[idx]

        # Convert to tensors
        encoder_input = torch.FloatTensor(sequence['encoder_data'].fillna(0).values)
        decoder_input = torch.FloatTensor(sequence['decoder_data'].fillna(0).values)
        targets = torch.FloatTensor(sequence['targets'].fillna(0).values)
        encoder_length = torch.LongTensor([sequence['encoder_length']])

        batch = {
            'encoder_input': encoder_input,
            'decoder_input': decoder_input,
            'targets': targets,
            'encoder_lengths': encoder_length,
            'symbol': sequence['symbol'],
            'start_time': sequence['start_time'],
            'end_time': sequence['end_time']
        }

        # Add sentiment features if available
        if 'sentiment_features' in sequence and sequence['sentiment_features'] is not None:
            batch['sentiment_features'] = torch.FloatTensor(sequence['sentiment_features'])

        return batch


class TFTDataLoader:
    """Data loader for TFT models with sentiment integration."""

    def __init__(self, pool: asyncpg.Pool, env_config):
        self.pool = pool
        self.env = env_config
        self.sentiment_integrator = SentimentIntegrator(pool, env_config)

    async def load_price_data(
        self,
        symbols: List[str],
        start_date: datetime,
        end_date: datetime,
        interval: str = "1min"
    ) -> pd.DataFrame:
        """Load price data for specified symbols and time range."""

        query = f"""
        SELECT
            symbol,
            timestamp,
            open, high, low, close, volume,
            (close - LAG(close) OVER (PARTITION BY symbol ORDER BY timestamp)) / LAG(close) OVER (PARTITION BY symbol ORDER BY timestamp) as returns
        FROM {self.env.get_table_name('daily_price_polygon')}
        WHERE symbol = ANY($1)
        AND timestamp BETWEEN $2 AND $3
        ORDER BY symbol, timestamp
        """

        async with self.pool.acquire() as conn:
            rows = await conn.fetch(query, symbols, start_date, end_date)

        if not rows:
            logger.warning(f"No price data found for symbols {symbols}")
            return pd.DataFrame()

        # Convert to DataFrame
        data = pd.DataFrame([dict(row) for row in rows])
        data['timestamp'] = pd.to_datetime(data['timestamp'])

        # Calculate technical indicators
        data = self._calculate_technical_indicators(data)

        logger.info(f"Loaded price data: {len(data)} rows for {len(symbols)} symbols")
        return data

    def _calculate_technical_indicators(self, data: pd.DataFrame) -> pd.DataFrame:
        """Calculate technical indicators for the price data."""

        result_dfs = []

        for symbol in data['symbol'].unique():
            symbol_data = data[data['symbol'] == symbol].copy()

            # RSI
            delta = symbol_data['close'].diff()
            gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
            loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
            rs = gain / loss
            symbol_data['rsi'] = 100 - (100 / (1 + rs))

            # MACD
            ema12 = symbol_data['close'].ewm(span=12).mean()
            ema26 = symbol_data['close'].ewm(span=26).mean()
            symbol_data['macd'] = ema12 - ema26
            symbol_data['ema_12'] = ema12
            symbol_data['ema_26'] = ema26

            # Bollinger Bands
            sma20 = symbol_data['close'].rolling(window=20).mean()
            std20 = symbol_data['close'].rolling(window=20).std()
            symbol_data['bollinger_upper'] = sma20 + (2 * std20)
            symbol_data['bollinger_lower'] = sma20 - (2 * std20)

            # ATR (Average True Range)
            high_low = symbol_data['high'] - symbol_data['low']
            high_close = np.abs(symbol_data['high'] - symbol_data['close'].shift())
            low_close = np.abs(symbol_data['low'] - symbol_data['close'].shift())
            tr = np.maximum(high_low, np.maximum(high_close, low_close))
            symbol_data['atr'] = tr.rolling(window=14).mean()

            # Volatility
            symbol_data['volatility'] = symbol_data['returns'].rolling(window=20).std()

            result_dfs.append(symbol_data)

        return pd.concat(result_dfs, ignore_index=True)

    async def load_sentiment_data(
        self,
        symbols: List[str],
        start_date: datetime,
        end_date: datetime
    ) -> pd.DataFrame:
        """Load sentiment data for specified symbols and time range."""

        if not hasattr(self, 'sentiment_integrator'):
            logger.warning("Sentiment integrator not available")
            return pd.DataFrame()

        try:
            # Get unified sentiment signals
            sentiment_data_list = []

            for symbol in symbols:
                signals = await analyze_unified_sentiment(
                    self.pool, self.env, [symbol], hours_back=24
                )

                if symbol in signals:
                    signal = signals[symbol]
                    sentiment_features = signal.sentiment_features

                    # Create DataFrame row
                    row = {
                        'symbol': symbol,
                        'created_at': signal.timestamp,
                        'sentiment_score': signal.overall_sentiment_score,
                        'confidence': signal.overall_confidence,
                        'momentum': sentiment_features.get('sentiment_momentum', 0),
                        'volume_indicator': signal.volume_indicator,
                        'consensus_score': signal.consensus_score,
                        'divergence_score': signal.divergence_score,
                        'risk_score': signal.risk_score
                    }

                    # Add ML features
                    for i in range(1, 17):
                        feature_key = f'feature_{i}'
                        row[feature_key] = sentiment_features.get(feature_key, 0)

                    sentiment_data_list.append(row)

            if sentiment_data_list:
                sentiment_df = pd.DataFrame(sentiment_data_list)
                sentiment_df['created_at'] = pd.to_datetime(sentiment_df['created_at'])
                logger.info(f"Loaded sentiment data: {len(sentiment_df)} rows")
                return sentiment_df

        except Exception as e:
            logger.error(f"Error loading sentiment data: {e}")

        return pd.DataFrame()

    async def create_datasets(
        self,
        symbols: List[str],
        start_date: datetime,
        end_date: datetime,
        config: TFTDataConfig,
        train_ratio: float = 0.7,
        val_ratio: float = 0.2
    ) -> Tuple[TFTDataset, TFTDataset, TFTDataset, FeatureNormalizer]:
        """
        Create train, validation, and test datasets.

        Args:
            symbols: List of stock symbols
            start_date: Start date for data
            end_date: End date for data
            config: TFT data configuration
            train_ratio: Ratio of data for training
            val_ratio: Ratio of data for validation

        Returns:
            Tuple of (train_dataset, val_dataset, test_dataset, normalizer)
        """

        # Load price data
        price_data = await self.load_price_data(symbols, start_date, end_date)

        if len(price_data) == 0:
            raise ValueError("No price data loaded")

        # Load sentiment data if configured
        sentiment_data = None
        if config.use_sentiment:
            sentiment_data = await self.load_sentiment_data(symbols, start_date, end_date)
            if len(sentiment_data) == 0:
                logger.warning("No sentiment data loaded, proceeding without sentiment features")
                config.use_sentiment = False

        # Split data by time
        total_days = (end_date - start_date).days
        train_days = int(total_days * train_ratio)
        val_days = int(total_days * val_ratio)

        train_end = start_date + timedelta(days=train_days)
        val_end = start_date + timedelta(days=train_days + val_days)

        # Split price data
        train_data = price_data[price_data['timestamp'] <= train_end]
        val_data = price_data[
            (price_data['timestamp'] > train_end) &
            (price_data['timestamp'] <= val_end)
        ]
        test_data = price_data[price_data['timestamp'] > val_end]

        # Split sentiment data
        train_sentiment = None
        val_sentiment = None
        test_sentiment = None

        if sentiment_data is not None and len(sentiment_data) > 0:
            train_sentiment = sentiment_data[sentiment_data['created_at'] <= train_end]
            val_sentiment = sentiment_data[
                (sentiment_data['created_at'] > train_end) &
                (sentiment_data['created_at'] <= val_end)
            ]
            test_sentiment = sentiment_data[sentiment_data['created_at'] > val_end]

        # Fit normalizer on training data
        normalizer = FeatureNormalizer(config.target_normalization)
        normalizer.fit(train_data, config.scaling_features)

        # Normalize datasets
        if config.normalize_features:
            train_data = normalizer.transform(train_data, config.scaling_features)
            val_data = normalizer.transform(val_data, config.scaling_features)
            test_data = normalizer.transform(test_data, config.scaling_features)

        # Create datasets
        train_dataset = TFTDataset(train_data, config, normalizer, train_sentiment)
        val_dataset = TFTDataset(val_data, config, normalizer, val_sentiment)
        test_dataset = TFTDataset(test_data, config, normalizer, test_sentiment)

        logger.info(
            f"Created datasets - Train: {len(train_dataset)}, "
            f"Val: {len(val_dataset)}, Test: {len(test_dataset)}"
        )

        return train_dataset, val_dataset, test_dataset, normalizer

    async def create_data_loaders(
        self,
        symbols: List[str],
        start_date: datetime,
        end_date: datetime,
        config: TFTDataConfig,
        batch_size: int = 32,
        num_workers: int = 0
    ) -> Tuple[DataLoader, DataLoader, DataLoader, FeatureNormalizer]:
        """Create PyTorch DataLoaders for training."""

        train_dataset, val_dataset, test_dataset, normalizer = await self.create_datasets(
            symbols, start_date, end_date, config
        )

        train_loader = DataLoader(
            train_dataset, batch_size=batch_size, shuffle=True,
            num_workers=num_workers, pin_memory=torch.cuda.is_available()
        )

        val_loader = DataLoader(
            val_dataset, batch_size=batch_size, shuffle=False,
            num_workers=num_workers, pin_memory=torch.cuda.is_available()
        )

        test_loader = DataLoader(
            test_dataset, batch_size=batch_size, shuffle=False,
            num_workers=num_workers, pin_memory=torch.cuda.is_available()
        )

        return train_loader, val_loader, test_loader, normalizer


def collate_fn(batch):
    """Custom collate function for variable-length sequences."""

    # Extract maximum lengths
    max_encoder_length = max(item['encoder_input'].size(0) for item in batch)
    max_decoder_length = max(item['decoder_input'].size(0) for item in batch)

    batch_size = len(batch)
    encoder_feature_size = batch[0]['encoder_input'].size(1)
    decoder_feature_size = batch[0]['decoder_input'].size(1)
    target_size = batch[0]['targets'].size(1)

    # Initialize padded tensors
    encoder_inputs = torch.zeros(batch_size, max_encoder_length, encoder_feature_size)
    decoder_inputs = torch.zeros(batch_size, max_decoder_length, decoder_feature_size)
    targets = torch.zeros(batch_size, max_decoder_length, target_size)
    encoder_lengths = torch.zeros(batch_size, dtype=torch.long)

    # Optional sentiment features
    sentiment_features = None
    if 'sentiment_features' in batch[0]:
        sentiment_feature_size = batch[0]['sentiment_features'].size(1)
        sentiment_features = torch.zeros(
            batch_size, max_encoder_length + max_decoder_length, sentiment_feature_size
        )

    # Fill tensors
    symbols = []
    start_times = []
    end_times = []

    for i, item in enumerate(batch):
        enc_len = item['encoder_input'].size(0)
        dec_len = item['decoder_input'].size(0)

        encoder_inputs[i, :enc_len] = item['encoder_input']
        decoder_inputs[i, :dec_len] = item['decoder_input']
        targets[i, :dec_len] = item['targets']
        encoder_lengths[i] = enc_len

        if sentiment_features is not None and 'sentiment_features' in item:
            sent_len = item['sentiment_features'].size(0)
            sentiment_features[i, :sent_len] = item['sentiment_features']

        symbols.append(item['symbol'])
        start_times.append(item['start_time'])
        end_times.append(item['end_time'])

    result = {
        'encoder_input': encoder_inputs,
        'decoder_input': decoder_inputs,
        'targets': targets,
        'encoder_lengths': encoder_lengths,
        'symbols': symbols,
        'start_times': start_times,
        'end_times': end_times
    }

    if sentiment_features is not None:
        result['sentiment_features'] = sentiment_features

    return result