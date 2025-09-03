"""
Training Data Generator for Residual Return Prediction with Schema Management.

This module provides comprehensive training data generation capabilities for the ATS platform,
with integrated schema management, validation, and EDA preparation. The generator creates
financial ML datasets with automatic feature type classification, validation, and metadata
tracking for optimal model training and analysis workflows.

Key Features:
- Schema-aware training data generation with automatic feature classification
- Financial-specific feature types (OHLC, technical indicators, returns, volatility)
- Comprehensive data validation with confidence scoring  
- Database integration for schema versioning and tracking
- EDA integration with visualization recommendations
- Backwards compatibility with existing DataFrame-based workflows

Classes:
    TrainingConfig: Configuration for training data generation parameters
    TrainingSample: Individual training sample with features and targets
    TrainingDatasetResult: Schema-aware training dataset result with validation
    ResidualReturnTrainingDataGenerator: Main generator class with schema management

Functions:
    generate_residual_return_training_data: Convenience function for schema-aware generation
    generate_residual_return_training_data_legacy: Legacy DataFrame compatibility function

Example:
    # Schema-aware training data generation
    result = await generate_residual_return_training_data(
        connection_pool=pool, env=environment, universe_state_manager=manager,
        start_date=datetime(2023, 1, 1), end_date=datetime(2023, 12, 31),
        instrument_ids=[1, 2, 3], include_schema=True,
        output_path="/data/training/residual_returns_2023"
    )
    
    # Access schema-aware results
    features_array = result.features_array     # NumPy array with features
    labels_array = result.labels_array         # NumPy array with labels
    schema = result.schema                     # TrainingDatasetSchema object
    validation = result.validation_result     # ValidationResult object
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta
from dataclasses import dataclass
import logging
import asyncpg
import json
import os

from state.universe_state_manager import UniverseStateManager
from domains.trading.services.enhanced_indicators import calculate_all_technical_indicators, ResidualReturnIndicatorConfig
from modeling.factor_models import ResidualReturnCalculator
from modeling.event_features import EventSequenceExtractor, EventCalendar, flatten_event_features_for_model
from src.schema.training_schema import TrainingDatasetSchema, FeatureSchema, LabelSchema, DatasetMetadata, FeatureType, DataType, ValidationResult
from src.dao.training_schema_dao import TrainingSchemaDAO

logger = logging.getLogger(__name__)


@dataclass
class TrainingConfig:
    """Configuration for training data generation."""
    lookback_days: int = 252  # Days of historical data for features
    prediction_horizons: List[int] = None  # [1, 2, 3, 4, 5] days
    min_history_days: int = 50  # Minimum history required
    factor_model_type: str = 'multi_factor'  # 'market_model', 'fama_french', 'multi_factor'
    include_technical_indicators: bool = True
    include_event_features: bool = True
    include_sector_features: bool = True
    exclude_weekends: bool = True
    min_price: float = 1.0  # Minimum stock price
    min_volume: int = 1000  # Minimum daily volume
    
    def __post_init__(self):
        if self.prediction_horizons is None:
            self.prediction_horizons = [1, 2, 3, 4, 5]


@dataclass
class TrainingSample:
    """Single training sample with features and targets."""
    instrument_id: int
    date: datetime
    features: Dict[str, Any]
    targets: Dict[str, float]
    metadata: Dict[str, Any]


@dataclass
class TrainingDatasetResult:
    """Result of training data generation with schema."""
    dataset_path: str
    features_array: np.ndarray
    labels_array: np.ndarray
    schema: TrainingDatasetSchema
    validation_result: ValidationResult
    metadata: Dict[str, Any]


class ResidualReturnTrainingDataGenerator:
    """Generate training data for residual return prediction models with schema management."""
    
    def __init__(self, 
                 connection_pool: asyncpg.Pool,
                 env,
                 universe_state_manager: UniverseStateManager,
                 config: Optional[TrainingConfig] = None):
        self.pool = connection_pool
        self.env = env
        self.universe_state_manager = universe_state_manager
        self.config = config or TrainingConfig()
        
        # Initialize components
        self.residual_calculator = ResidualReturnCalculator(connection_pool, env)
        self.event_calendar = EventCalendar(connection_pool, env)
        self.event_extractor = EventSequenceExtractor(
            universe_state_manager, 
            self.event_calendar,
            lookback_days=5,
            forward_days=max(self.config.prediction_horizons)
        )
        
        # Technical indicators configuration
        self.indicator_config = ResidualReturnIndicatorConfig.comprehensive_config()
        
        # Schema management
        self.schema_dao = TrainingSchemaDAO(env)
        
        # Cache for performance
        self._sector_cache = {}
        self._market_cap_cache = {}
        self._feature_schema_cache = None
    
    async def generate_training_dataset(self, 
                                      start_date: datetime,
                                      end_date: datetime,
                                      instrument_ids: Optional[List[int]] = None,
                                      batch_size: int = 100,
                                      include_schema: bool = True,
                                      output_path: Optional[str] = None) -> TrainingDatasetResult:
        """
        Generate comprehensive training dataset.
        
        Args:
            start_date: Start date for data generation
            end_date: End date for data generation
            instrument_ids: List of instrument IDs (None = all available)
            batch_size: Number of instruments to process in each batch
            
        Returns:
            DataFrame with training samples
        """
        logger.info(f"Generating training data from {start_date} to {end_date}")
        
        # Get instrument universe
        if instrument_ids is None:
            instrument_ids = await self._get_active_instruments()
        
        logger.info(f"Processing {len(instrument_ids)} instruments")
        
        # Process in batches for memory efficiency
        all_samples = []
        
        for i in range(0, len(instrument_ids), batch_size):
            batch_ids = instrument_ids[i:i + batch_size]
            logger.info(f"Processing batch {i//batch_size + 1}/{(len(instrument_ids)-1)//batch_size + 1}")
            
            try:
                batch_samples = await self._generate_batch_samples(
                    batch_ids, start_date, end_date
                )
                all_samples.extend(batch_samples)
                
            except Exception as e:
                logger.error(f"Failed to process batch {i//batch_size + 1}: {e}")
                continue
        
        if not all_samples:
            logger.warning("No training samples generated")
            return pd.DataFrame()
        
        # Convert to DataFrame
        logger.info(f"Converting {len(all_samples)} samples to DataFrame")
        training_df = self._samples_to_dataframe(all_samples)
        
        # Final data cleaning and validation
        training_df = self._clean_training_data(training_df)
        
        logger.info(f"Generated training dataset with {len(training_df)} samples and {len(training_df.columns)} features")
        
        # Generate schema and validation if requested
        if include_schema:
            return await self._create_schema_aware_result(
                training_df, start_date, end_date, instrument_ids, output_path
            )
        else:
            # Return legacy DataFrame format for backwards compatibility
            return TrainingDatasetResult(
                dataset_path='',
                features_array=np.array([]),
                labels_array=np.array([]),
                schema=None,
                validation_result=None,
                metadata={'dataframe': training_df}
            )
    
    async def _get_active_instruments(self) -> List[int]:
        """Get list of active instruments."""
        instruments_table = self.env.get_table_name('instruments')
        
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(f"""
                SELECT id 
                FROM {instruments_table}
                WHERE is_active = true
                AND symbol IS NOT NULL
                AND symbol ~ '^[A-Z]+$'
                ORDER BY id
            """)
            
            return [row['id'] for row in rows]
    
    async def _generate_batch_samples(self, 
                                    instrument_ids: List[int],
                                    start_date: datetime,
                                    end_date: datetime) -> List[TrainingSample]:
        """Generate training samples for a batch of instruments."""
        # Pre-calculate residual returns for the batch
        logger.debug(f"Calculating residual returns for {len(instrument_ids)} instruments")
        residual_returns = await self.residual_calculator.calculate_residual_returns(
            instrument_ids, 
            start_date - timedelta(days=self.config.lookback_days), 
            end_date + timedelta(days=max(self.config.prediction_horizons)),
            self.config.factor_model_type
        )
        
        # Process each instrument
        batch_samples = []
        
        for instrument_id in instrument_ids:
            try:
                instrument_samples = await self._generate_instrument_samples(
                    instrument_id, start_date, end_date, residual_returns
                )
                batch_samples.extend(instrument_samples)
                
            except Exception as e:
                logger.warning(f"Failed to generate samples for instrument {instrument_id}: {e}")
                continue
        
        return batch_samples
    
    async def _generate_instrument_samples(self,
                                         instrument_id: int,
                                         start_date: datetime,
                                         end_date: datetime,
                                         residual_returns: pd.DataFrame) -> List[TrainingSample]:
        """Generate training samples for a single instrument."""
        samples = []
        
        # Filter residual returns for this instrument
        instrument_residuals = residual_returns[
            residual_returns['instrument_id'] == instrument_id
        ].set_index('date').sort_index()
        
        if instrument_residuals.empty:
            return samples
        
        # Generate samples for each date
        current_date = start_date
        while current_date <= end_date:
            # Skip weekends if configured
            if self.config.exclude_weekends and current_date.weekday() >= 5:
                current_date += timedelta(days=1)
                continue
            
            try:
                sample = await self._create_training_sample(
                    instrument_id, current_date, instrument_residuals
                )
                
                if sample:
                    samples.append(sample)
                    
            except Exception as e:
                logger.debug(f"Failed to create sample for {instrument_id} on {current_date}: {e}")
            
            current_date += timedelta(days=1)
        
        return samples
    
    async def _create_training_sample(self,
                                    instrument_id: int,
                                    current_date: datetime,
                                    instrument_residuals: pd.DataFrame) -> Optional[TrainingSample]:
        """Create a single training sample."""
        try:
            # Get historical price data for features
            historical_prices = self.universe_state_manager.get_lag_prices(
                instrument_id, current_date, self.config.lookback_days
            )
            
            if len(historical_prices) < self.config.min_history_days:
                return None
            
            # Apply basic filters
            if not self._passes_basic_filters(historical_prices):
                return None
            
            # Get future data for targets
            future_residuals = self._get_future_residuals(
                current_date, instrument_residuals, self.config.prediction_horizons
            )
            
            if not future_residuals:
                return None
            
            # Extract features
            features = {}
            
            # Technical indicators
            if self.config.include_technical_indicators:
                tech_features = self._extract_technical_features(historical_prices)
                features.update(tech_features)
            
            # Event features
            if self.config.include_event_features:
                event_features = await self._extract_event_features(instrument_id, current_date)
                features.update(event_features)
            
            # Sector features
            if self.config.include_sector_features:
                sector_features = await self._extract_sector_features(instrument_id, current_date)
                features.update(sector_features)
            
            # Market features
            market_features = self._extract_market_features(historical_prices, current_date)
            features.update(market_features)
            
            # Factor model features
            factor_features = self._extract_factor_features(current_date, instrument_residuals)
            features.update(factor_features)
            
            # Create targets
            targets = self._create_targets(future_residuals)
            
            # Metadata
            metadata = {
                'current_price': historical_prices['close'].iloc[-1] if 'close' in historical_prices.columns else historical_prices['high'].iloc[-1],
                'avg_volume': historical_prices.get('volume', pd.Series([0])).tail(20).mean(),
                'data_quality_score': self._calculate_data_quality_score(historical_prices),
                'prediction_date': current_date
            }
            
            return TrainingSample(
                instrument_id=instrument_id,
                date=current_date,
                features=features,
                targets=targets,
                metadata=metadata
            )
            
        except Exception as e:
            logger.debug(f"Error creating sample for {instrument_id} on {current_date}: {e}")
            return None
    
    def _passes_basic_filters(self, price_data: pd.DataFrame) -> bool:
        """Apply basic data quality filters."""
        if price_data.empty:
            return False
        
        # Price filter
        current_price = price_data['close'].iloc[-1] if 'close' in price_data.columns else price_data['high'].iloc[-1]
        if current_price < self.config.min_price:
            return False
        
        # Volume filter (if available)
        if 'volume' in price_data.columns:
            recent_volume = price_data['volume'].tail(5).mean()
            if recent_volume < self.config.min_volume:
                return False
        
        # Data completeness filter
        required_cols = ['high', 'low']
        for col in required_cols:
            if col not in price_data.columns:
                return False
            if price_data[col].isnull().sum() > len(price_data) * 0.1:  # Max 10% missing
                return False
        
        return True
    
    def _get_future_residuals(self, current_date: datetime, 
                             instrument_residuals: pd.DataFrame,
                             horizons: List[int]) -> Dict[int, float]:
        """Get future residual returns for target creation."""
        future_residuals = {}
        
        for horizon in horizons:
            future_date = current_date + timedelta(days=horizon)
            
            # Find closest date in residuals data
            available_dates = instrument_residuals.index
            if len(available_dates) == 0:
                continue
            
            # Find closest future date
            future_dates = available_dates[available_dates >= future_date.date()]
            if len(future_dates) == 0:
                continue
            
            closest_date = future_dates[0]
            if (closest_date - future_date.date()).days <= 3:  # Within 3 days tolerance
                residual_return = instrument_residuals.loc[closest_date, 'residual_return']
                if not pd.isna(residual_return):
                    future_residuals[horizon] = residual_return
        
        return future_residuals
    
    def _extract_technical_features(self, price_data: pd.DataFrame) -> Dict[str, Any]:
        """Extract technical indicator features."""
        try:
            tech_features = calculate_all_technical_indicators(
                price_data, self.indicator_config
            )
            
            # Add basic price features
            if 'close' in price_data.columns:
                close_prices = price_data['close']
                tech_features.update({
                    'return_1d': close_prices.pct_change().iloc[-1],
                    'return_5d': close_prices.pct_change(5).iloc[-1],
                    'return_20d': close_prices.pct_change(20).iloc[-1],
                    'volatility_20d': close_prices.pct_change().rolling(20).std().iloc[-1],
                    'price_momentum_10d': (close_prices.iloc[-1] / close_prices.iloc[-11]) - 1 if len(close_prices) >= 11 else 0
                })
            
            # Clean and validate features with improved numpy type handling
            cleaned_features = {}
            suspicious_values = []
            
            for key, value in tech_features.items():
                if value is None:
                    cleaned_features[key] = 0.0
                elif pd.isna(value):
                    cleaned_features[key] = 0.0
                elif isinstance(value, (int, float, np.number)):  # Include numpy types
                    if np.isinf(value):
                        cleaned_features[key] = 0.0
                    else:
                        cleaned_features[key] = float(value)
                        
                        # Log suspicious EMA values for debugging
                        if 'EMA' in key and '_value' in key:
                            if -2 < value < 2:  # EMA values should not be this small for most stocks
                                suspicious_values.append(f"{key}={value}")
                elif isinstance(value, str):
                    # Status fields and other string values should be skipped
                    continue
                else:
                    # Log unexpected data types for debugging
                    logger.warning(f"Unexpected data type for {key}: {type(value)} = {value}")
                    cleaned_features[key] = 0.0
            
            # Log suspicious EMA values that might indicate calculation errors
            if suspicious_values:
                logger.warning(f"Suspicious small EMA values detected: {', '.join(suspicious_values)}")
                logger.warning("Note: EMA ratios and slopes should be small, but EMA values should be close to stock price")
            
            return cleaned_features
            
        except Exception as e:
            logger.warning(f"Failed to extract technical features: {e}")
            return {}
    
    async def _extract_event_features(self, instrument_id: int, current_date: datetime) -> Dict[str, Any]:
        """Extract event-driven features."""
        try:
            event_features_obj = await self.event_extractor.extract_event_features(
                current_date, instrument_id
            )
            
            return flatten_event_features_for_model(event_features_obj)
            
        except Exception as e:
            logger.warning(f"Failed to extract event features for {instrument_id}: {e}")
            return {
                'event_proximity_score': 0.0,
                'event_importance_weighted_score': 0.0
            }
    
    async def _extract_sector_features(self, instrument_id: int, current_date: datetime) -> Dict[str, Any]:
        """Extract sector-specific features."""
        try:
            # Get sector information
            sector = await self._get_instrument_sector(instrument_id)
            
            # Get sector performance
            sector_return = await self._get_sector_return(sector, current_date)
            
            return {
                'sector': sector,
                'sector_return_1d': sector_return,
                'sector_momentum_5d': sector_return * 5,  # Simplified momentum
            }
            
        except Exception as e:
            logger.debug(f"Failed to extract sector features for {instrument_id}: {e}")
            return {
                'sector': 'Unknown',
                'sector_return_1d': 0.0,
                'sector_momentum_5d': 0.0
            }
    
    def _extract_market_features(self, price_data: pd.DataFrame, current_date: datetime) -> Dict[str, Any]:
        """Extract market-wide features."""
        features = {}
        
        # Market timing features
        features['day_of_week'] = current_date.weekday()
        features['day_of_month'] = current_date.day
        features['day_of_year'] = current_date.timetuple().tm_yday
        features['is_month_end'] = (current_date + timedelta(days=1)).day == 1
        features['is_quarter_end'] = (current_date.month % 3 == 0) and features['is_month_end']
        
        # Price level features
        if 'close' in price_data.columns:
            close_prices = price_data['close']
            
            # Support/resistance levels
            features['days_since_high_20d'] = self._days_since_high(close_prices, 20)
            features['days_since_low_20d'] = self._days_since_low(close_prices, 20)
            
            # Price relative to historical levels
            if len(close_prices) >= 252:
                features['price_vs_52w_high'] = close_prices.iloc[-1] / close_prices.tail(252).max() - 1
                features['price_vs_52w_low'] = close_prices.iloc[-1] / close_prices.tail(252).min() - 1
        
        return features
    
    def _extract_factor_features(self, current_date: datetime, 
                                instrument_residuals: pd.DataFrame) -> Dict[str, Any]:
        """Extract factor model features."""
        features = {}
        
        try:
            # Get recent factor loadings and model stats
            recent_data = instrument_residuals.tail(20)
            
            if not recent_data.empty:
                # Factor loadings (if available)
                loading_cols = [col for col in recent_data.columns if col.endswith('_loading')]
                for col in loading_cols:
                    features[f'recent_{col}'] = recent_data[col].mean()
                
                # Model quality
                if 'r_squared' in recent_data.columns:
                    features['model_r_squared'] = recent_data['r_squared'].mean()
                
                # Residual characteristics
                if 'residual_return' in recent_data.columns:
                    residuals = recent_data['residual_return'].dropna()
                    if len(residuals) > 0:
                        features['residual_volatility'] = residuals.std()
                        features['residual_skewness'] = residuals.skew()
                        features['residual_mean'] = residuals.mean()
        
        except Exception as e:
            logger.debug(f"Failed to extract factor features: {e}")
        
        return features
    
    def _create_targets(self, future_residuals: Dict[int, float]) -> Dict[str, float]:
        """Create target variables from future residual returns."""
        targets = {}
        
        for horizon, residual_return in future_residuals.items():
            # Direct residual return
            targets[f'residual_return_{horizon}d'] = residual_return
            
            # Binary classification targets
            targets[f'positive_return_{horizon}d'] = 1.0 if residual_return > 0 else 0.0
            targets[f'strong_positive_{horizon}d'] = 1.0 if residual_return > 0.02 else 0.0
            targets[f'strong_negative_{horizon}d'] = 1.0 if residual_return < -0.02 else 0.0
            
            # Quantile targets for distributional modeling
            targets[f'return_magnitude_{horizon}d'] = abs(residual_return)
        
        return targets
    
    def _calculate_data_quality_score(self, price_data: pd.DataFrame) -> float:
        """Calculate data quality score for the sample."""
        score = 1.0
        
        # Penalize missing data
        for col in ['high', 'low']:
            if col in price_data.columns:
                missing_pct = price_data[col].isnull().sum() / len(price_data)
                score *= (1 - missing_pct)
        
        # Penalize suspicious price patterns
        if 'close' in price_data.columns:
            returns = price_data['close'].pct_change().dropna()
            if len(returns) > 0:
                # Penalize extreme volatility
                volatility = returns.std()
                if volatility > 0.1:  # More than 10% daily volatility
                    score *= 0.8
                
                # Penalize zero return days (potential stale data)
                zero_returns = (returns == 0).sum() / len(returns)
                score *= (1 - zero_returns * 0.5)
        
        return max(score, 0.1)  # Minimum score of 0.1
    
    def _days_since_high(self, prices: pd.Series, window: int) -> int:
        """Calculate days since highest price in window."""
        if len(prices) < window:
            return window
        
        recent_prices = prices.tail(window)
        max_idx = recent_prices.idxmax()
        return len(recent_prices) - recent_prices.index.get_loc(max_idx) - 1
    
    def _days_since_low(self, prices: pd.Series, window: int) -> int:
        """Calculate days since lowest price in window."""
        if len(prices) < window:
            return window
        
        recent_prices = prices.tail(window)
        min_idx = recent_prices.idxmin()
        return len(recent_prices) - recent_prices.index.get_loc(min_idx) - 1
    
    async def _get_instrument_sector(self, instrument_id: int) -> str:
        """Get sector for instrument (cached)."""
        if instrument_id in self._sector_cache:
            return self._sector_cache[instrument_id]
        
        try:
            instruments_table = self.env.get_table_name('instruments')
            
            async with self.pool.acquire() as conn:
                row = await conn.fetchrow(f"""
                    SELECT sector, industry
                    FROM {instruments_table}
                    WHERE id = $1
                """, instrument_id)
                
                sector = row['sector'] if row and row['sector'] else 'Unknown'
                self._sector_cache[instrument_id] = sector
                return sector
                
        except Exception:
            self._sector_cache[instrument_id] = 'Unknown'
            return 'Unknown'
    
    async def _get_sector_return(self, sector: str, current_date: datetime) -> float:
        """Get recent sector return (simplified)."""
        # This would ideally use sector ETF returns or sector index data
        # For now, return a default value
        return 0.0
    
    def _samples_to_dataframe(self, samples: List[TrainingSample]) -> pd.DataFrame:
        """Convert training samples to DataFrame."""
        if not samples:
            return pd.DataFrame()
        
        # Collect all features and targets
        rows = []
        
        for sample in samples:
            row = {
                'instrument_id': sample.instrument_id,
                'date': sample.date
            }
            
            # Add features
            row.update(sample.features)
            
            # Add targets
            row.update(sample.targets)
            
            # Add metadata
            for key, value in sample.metadata.items():
                if key != 'prediction_date':  # Skip duplicate date
                    row[f'meta_{key}'] = value
            
            rows.append(row)
        
        return pd.DataFrame(rows)
    
    def _clean_training_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and validate training data."""
        if df.empty:
            return df
        
        logger.info(f"Cleaning training data: {len(df)} samples")
        
        # Remove rows with missing targets
        target_cols = [col for col in df.columns if col.startswith('residual_return_')]
        if target_cols:
            initial_rows = len(df)
            df = df.dropna(subset=target_cols, how='all')
            logger.info(f"Removed {initial_rows - len(df)} samples with missing targets")
        
        # Handle infinite values
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        df[numeric_cols] = df[numeric_cols].replace([np.inf, -np.inf], np.nan)
        
        # Fill remaining NaN values with appropriate defaults
        for col in numeric_cols:
            if col.startswith('residual_return_') or col.startswith('positive_return_'):
                continue  # Don't fill target variables
            
            # Fill with 0 for most features
            df[col] = df[col].fillna(0.0)
        
        # Remove duplicate samples
        initial_rows = len(df)
        df = df.drop_duplicates(subset=['instrument_id', 'date'])
        if initial_rows > len(df):
            logger.info(f"Removed {initial_rows - len(df)} duplicate samples")
        
        # Sort by date and instrument for consistency
        df = df.sort_values(['date', 'instrument_id']).reset_index(drop=True)
        
        logger.info(f"Final training data: {len(df)} samples with {len(df.columns)} columns")
        
        return df
    
    async def _create_schema_aware_result(
        self,
        training_df: pd.DataFrame,
        start_date: datetime,
        end_date: datetime,
        instrument_ids: Optional[List[int]],
        output_path: Optional[str]
    ) -> TrainingDatasetResult:
        """
        Create schema-aware training dataset result with comprehensive validation.
        
        This method transforms a standard training DataFrame into a schema-aware
        TrainingDatasetResult with automatic feature classification, validation,
        and file output preparation for ML pipelines and EDA integration.
        
        Args:
            training_df: Generated training DataFrame with features and targets
            start_date: Start date of training data generation
            end_date: End date of training data generation  
            instrument_ids: List of instrument IDs used in generation
            output_path: Directory path for saving training dataset files
            
        Returns:
            TrainingDatasetResult containing:
            - features_array: NumPy array of features (samples x features)
            - labels_array: NumPy array of labels/targets (samples x targets)
            - schema: TrainingDatasetSchema with feature metadata
            - validation_result: ValidationResult with quality assessment
            - metadata: Additional generation metadata
            
        Process:
            1. Creates comprehensive schema from DataFrame analysis
            2. Separates features and targets into NumPy arrays
            3. Validates data against schema constraints
            4. Saves dataset files with metadata
            5. Registers schema in database registry
            
        Example:
            result = await self._create_schema_aware_result(
                training_df, start_date, end_date, [1, 2, 3], "/data/train"
            )
            # result.schema contains feature type classifications
            # result.validation_result.confidence_score indicates data quality
        """
        
        # Create schema from training data
        schema = await self._create_training_schema(training_df, start_date, end_date, instrument_ids)
        
        # Separate features and labels
        feature_cols = [col for col in training_df.columns 
                       if not col.startswith(('residual_return_', 'positive_return_', 
                                             'strong_positive_', 'strong_negative_', 
                                             'return_magnitude_', 'instrument_id', 'date'))]
        
        target_cols = [col for col in training_df.columns 
                      if col.startswith(('residual_return_', 'positive_return_', 
                                        'strong_positive_', 'strong_negative_', 
                                        'return_magnitude_'))]
        
        # Convert to arrays
        features_array = training_df[feature_cols].values.astype(np.float32)
        labels_array = training_df[target_cols].values.astype(np.float32) if target_cols else np.array([])
        
        # Validate data against schema
        validation_result = self._validate_training_data(schema, features_array, labels_array)
        
        # Save to output path if specified
        dataset_path = output_path or self._generate_output_path()
        await self._save_training_dataset(
            dataset_path, features_array, labels_array, schema, validation_result, training_df
        )
        
        return TrainingDatasetResult(
            dataset_path=dataset_path,
            features_array=features_array,
            labels_array=labels_array,
            schema=schema,
            validation_result=validation_result,
            metadata={
                'num_samples': len(training_df),
                'num_features': len(feature_cols),
                'num_targets': len(target_cols),
                'date_range': (start_date, end_date),
                'instruments': instrument_ids or []
            }
        )
    
    async def _create_training_schema(
        self,
        training_df: pd.DataFrame,
        start_date: datetime,
        end_date: datetime,
        instrument_ids: Optional[List[int]]
    ) -> TrainingDatasetSchema:
        """Create comprehensive schema for training dataset."""
        
        # Get instrument symbols for metadata
        symbols = await self._get_instrument_symbols(instrument_ids) if instrument_ids else ['MULTI']
        
        # Analyze feature columns to determine types
        feature_schemas = []
        
        for col in training_df.columns:
            if col in ['instrument_id', 'date']:
                continue
            
            # Determine feature type based on column name
            feature_type = self._infer_feature_type(col)
            
            # Calculate statistics
            data = training_df[col]
            if pd.api.types.is_numeric_dtype(data):
                stats = {
                    'min': float(data.min()) if not data.empty else 0.0,
                    'max': float(data.max()) if not data.empty else 0.0,
                    'mean': float(data.mean()) if not data.empty else 0.0,
                    'std': float(data.std()) if not data.empty else 0.0,
                    'null_count': int(data.isnull().sum())
                }
            else:
                stats = {'null_count': int(data.isnull().sum())}
            
            feature_schema = FeatureSchema(
                name=col,
                type=feature_type,
                data_type=DataType.FLOAT32 if pd.api.types.is_numeric_dtype(data) else DataType.STRING,
                shape=[len(training_df)] if not data.empty else [0],
                description=self._generate_feature_description(col, feature_type),
                metadata={'statistics': stats}
            )
            
            feature_schemas.append(feature_schema)
        
        # Create label schemas for targets
        label_schemas = []
        target_cols = [col for col in training_df.columns 
                      if col.startswith(('residual_return_', 'positive_return_', 
                                        'strong_positive_', 'strong_negative_', 
                                        'return_magnitude_'))]
        
        for col in target_cols:
            data = training_df[col]
            # Determine label type
            if col.startswith('residual_return_') or col.startswith('return_magnitude_'):
                label_type = FeatureType.REGRESSION_LABEL
            else:
                label_type = FeatureType.CLASSIFICATION_LABEL
            
            label_schema = LabelSchema(
                name=col,
                type=label_type,
                data_type=DataType.FLOAT32,
                shape=[len(training_df)] if not data.empty else [0],
                description=self._generate_label_description(col),
                metadata={
                    'statistics': {
                        'min': float(data.min()) if not data.empty else 0.0,
                        'max': float(data.max()) if not data.empty else 0.0,
                        'mean': float(data.mean()) if not data.empty else 0.0,
                        'std': float(data.std()) if not data.empty else 0.0,
                        'null_count': int(data.isnull().sum())
                    },
                    'class_mapping': {} if label_type == FeatureType.REGRESSION_LABEL else {'negative': 0.0, 'positive': 1.0}
                }
            )
            label_schemas.append(label_schema)
        
        # Create metadata
        metadata = DatasetMetadata(
            symbol=symbols[0] if len(symbols) == 1 else 'MULTI',
            additional_symbols=symbols[1:] if len(symbols) > 1 else [],
            base_timeframe='daily',
            sequence_length=self.config.lookback_days,
            total_features=len(feature_schemas),
            total_samples=len(training_df),
            start_date=start_date.strftime('%Y-%m-%d'),
            end_date=end_date.strftime('%Y-%m-%d'),
            generation_params={
                'prediction_horizon': max(self.config.prediction_horizons),
                'model_type': 'residual_return_prediction',
                'feature_engineering_version': '1.0.0',
                'lookback_days': self.config.lookback_days
            }
        )
        
        # Create complete schema
        schema = TrainingDatasetSchema(
            schema_version='1.0.0',
            dataset_name=f"residual_return_{symbols[0]}_{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}",
            features=feature_schemas,
            labels=label_schemas,
            metadata=metadata
        )
        
        return schema
    
    def _infer_feature_type(self, column_name: str) -> FeatureType:
        """
        Automatically infer feature type from column name patterns.
        
        Uses pattern matching on column names to classify features into appropriate
        financial ML feature types. This classification enables proper handling
        in ML models, validation rules, and EDA visualizations.
        
        Args:
            column_name: Name of the feature column to classify
            
        Returns:
            FeatureType: Appropriate financial ML feature type
            
        Classification Rules:
            - RETURN_SERIES: return, pct_change, momentum patterns
            - TECHNICAL_INDICATOR: sma, ema, bb, macd, rsi, technical patterns  
            - VOLUME_SERIES: volume, shares, turnover patterns
            - VOLATILITY_SERIES: volatility, std, var patterns
            - MARKET_REGIME_INDICATORS: sector, industry, market patterns
            - SEASONAL_INDICATORS: day, month, quarter, time, seasonal patterns
            - Default: TECHNICAL_INDICATOR (safe fallback)
            
        Example:
            feature_type = self._infer_feature_type("sma_20")
            # Returns: FeatureType.TECHNICAL_INDICATOR
            
            feature_type = self._infer_feature_type("return_1d")
            # Returns: FeatureType.RETURN_SERIES
        """
        col_lower = column_name.lower()
        
        if any(x in col_lower for x in ['return', 'pct_change', 'momentum']):
            return FeatureType.RETURN_SERIES
        elif any(x in col_lower for x in ['sma', 'ema', 'bb', 'macd', 'rsi', 'technical']):
            return FeatureType.TECHNICAL_INDICATOR
        elif any(x in col_lower for x in ['volume', 'shares', 'turnover']):
            return FeatureType.VOLUME_SERIES
        elif any(x in col_lower for x in ['volatility', 'std', 'var']):
            return FeatureType.VOLATILITY_SERIES
        elif any(x in col_lower for x in ['sector', 'industry', 'market']):
            return FeatureType.MARKET_REGIME_INDICATORS
        elif any(x in col_lower for x in ['day', 'month', 'quarter', 'time', 'seasonal']):
            return FeatureType.SEASONAL_INDICATORS
        else:
            return FeatureType.TECHNICAL_INDICATOR  # Default fallback
    
    def _generate_feature_description(self, column_name: str, feature_type: FeatureType) -> str:
        """Generate description for feature."""
        descriptions = {
            FeatureType.RETURN_SERIES: f"Return-based feature: {column_name}",
            FeatureType.TECHNICAL_INDICATOR: f"Technical indicator: {column_name}",
            FeatureType.VOLUME_SERIES: f"Volume-related feature: {column_name}",
            FeatureType.VOLATILITY_SERIES: f"Volatility metric: {column_name}",
            FeatureType.MARKET_REGIME_INDICATORS: f"Market/sector feature: {column_name}",
            FeatureType.SEASONAL_INDICATORS: f"Time-based feature: {column_name}",
        }
        return descriptions.get(feature_type, f"Feature: {column_name}")
    
    def _generate_label_description(self, column_name: str) -> str:
        """Generate description for label."""
        if column_name.startswith('residual_return_'):
            return f"Residual return prediction target: {column_name}"
        elif column_name.startswith('positive_return_'):
            return f"Binary positive return indicator: {column_name}"
        elif column_name.startswith('strong_positive_'):
            return f"Strong positive return indicator (>2%): {column_name}"
        elif column_name.startswith('strong_negative_'):
            return f"Strong negative return indicator (<-2%): {column_name}"
        elif column_name.startswith('return_magnitude_'):
            return f"Return magnitude (absolute value): {column_name}"
        else:
            return f"Target variable: {column_name}"
    
    def _validate_training_data(
        self, 
        schema: TrainingDatasetSchema, 
        features: np.ndarray, 
        labels: np.ndarray
    ) -> ValidationResult:
        """Validate training data against schema."""
        errors = []
        warnings = []
        
        # Check feature array shape
        expected_features = len(schema.features)
        if features.shape[1] != expected_features:
            errors.append(f"Feature count mismatch: expected {expected_features}, got {features.shape[1]}")
        
        # Check label array shape
        expected_labels = len(schema.labels)
        if labels.size > 0 and labels.shape[1] != expected_labels:
            errors.append(f"Label count mismatch: expected {expected_labels}, got {labels.shape[1]}")
        
        # Check for NaN/Inf values
        if np.isnan(features).any():
            warnings.append("Features contain NaN values")
        if np.isinf(features).any():
            errors.append("Features contain infinite values")
        
        if labels.size > 0:
            if np.isnan(labels).any():
                warnings.append("Labels contain NaN values")
            if np.isinf(labels).any():
                errors.append("Labels contain infinite values")
        
        # Calculate confidence score
        confidence_score = 1.0
        if errors:
            confidence_score *= 0.5  # Reduce confidence for errors
        if warnings:
            confidence_score *= 0.8  # Slightly reduce for warnings
        
        return ValidationResult(
            is_valid=len(errors) == 0,
            errors=errors,
            warnings=warnings,
            confidence_score=confidence_score,
            validation_timestamp=datetime.now()
        )
    
    def _generate_output_path(self) -> str:
        """Generate output path for training dataset."""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        return f"training_data_residual_return_{timestamp}"
    
    async def _save_training_dataset(
        self,
        dataset_path: str,
        features_array: np.ndarray,
        labels_array: np.ndarray,
        schema: TrainingDatasetSchema,
        validation_result: ValidationResult,
        training_df: pd.DataFrame
    ):
        """Save training dataset with schema."""
        
        # Create output directory
        os.makedirs(dataset_path, exist_ok=True)
        
        # Save arrays
        np.save(os.path.join(dataset_path, 'features.npy'), features_array)
        if labels_array.size > 0:
            np.save(os.path.join(dataset_path, 'labels.npy'), labels_array)
        
        # Save schema
        schema_dict = schema.to_dict()
        with open(os.path.join(dataset_path, 'schema.json'), 'w') as f:
            json.dump(schema_dict, f, indent=2, default=str)
        
        # Save validation results
        validation_dict = {
            'is_valid': validation_result.is_valid,
            'errors': validation_result.errors,
            'warnings': validation_result.warnings,
            'confidence_score': validation_result.confidence_score,
            'validation_timestamp': validation_result.validation_timestamp.isoformat()
        }
        with open(os.path.join(dataset_path, 'validation.json'), 'w') as f:
            json.dump(validation_dict, f, indent=2)
        
        # Save raw dataframe for analysis
        training_df.to_parquet(os.path.join(dataset_path, 'raw_data.parquet'))
        
        # Register schema in database
        try:
            schema_hash = await self.schema_dao.register_schema(
                schema,
                created_by="ResidualReturnTrainingDataGenerator",
                tags=[schema.metadata.symbol, 'residual_return', 'daily'],
                description=f"Residual return training schema for {schema.metadata.symbol}"
            )
            
            # Save schema hash reference
            with open(os.path.join(dataset_path, 'schema_hash.txt'), 'w') as f:
                f.write(schema_hash)
                
            logger.info(f"Registered schema with hash: {schema_hash}")
            
        except Exception as e:
            logger.warning(f"Failed to register schema in database: {e}")
    
    async def _get_instrument_symbols(self, instrument_ids: List[int]) -> List[str]:
        """Get instrument symbols from IDs."""
        if not instrument_ids:
            return []
        
        instruments_table = self.env.get_table_name('instruments')
        
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(f"""
                SELECT symbol
                FROM {instruments_table}
                WHERE id = ANY($1)
                ORDER BY id
            """, instrument_ids)
            
            return [row['symbol'] for row in rows if row['symbol']]


# Convenience function for generating training data
async def generate_residual_return_training_data(
    connection_pool: asyncpg.Pool,
    env,
    universe_state_manager: UniverseStateManager,
    start_date: datetime,
    end_date: datetime,
    instrument_ids: Optional[List[int]] = None,
    config: Optional[TrainingConfig] = None,
    include_schema: bool = True,
    output_path: Optional[str] = None
) -> TrainingDatasetResult:
    """
    Convenience function to generate training data with schema management.
    
    Args:
        connection_pool: Database connection pool
        env: Environment configuration
        universe_state_manager: Universe state manager
        start_date: Start date for data generation
        end_date: End date for data generation  
        instrument_ids: List of instrument IDs (None = all available)
        config: Training configuration
        include_schema: Whether to include schema generation and validation
        output_path: Path to save training dataset files
        
    Returns:
        TrainingDatasetResult with schema-aware training data
    """
    generator = ResidualReturnTrainingDataGenerator(
        connection_pool, env, universe_state_manager, config
    )
    
    return await generator.generate_training_dataset(
        start_date, end_date, instrument_ids, include_schema=include_schema, output_path=output_path
    )


# Legacy function for backwards compatibility
async def generate_residual_return_training_data_legacy(
    connection_pool: asyncpg.Pool,
    env,
    universe_state_manager: UniverseStateManager,
    start_date: datetime,
    end_date: datetime,
    instrument_ids: Optional[List[int]] = None,
    config: Optional[TrainingConfig] = None
) -> pd.DataFrame:
    """
    Legacy convenience function that returns DataFrame (backwards compatibility).
    
    Returns DataFrame with comprehensive features and residual return targets.
    """
    result = await generate_residual_return_training_data(
        connection_pool, env, universe_state_manager, start_date, end_date, 
        instrument_ids, config, include_schema=False
    )
    
    # Return the DataFrame from metadata
    return result.metadata.get('dataframe', pd.DataFrame())