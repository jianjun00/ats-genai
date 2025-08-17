"""
Training Data Generator for Residual Return Prediction.
Integrates with UniverseStateManager to generate comprehensive training datasets.
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple, Any
from datetime import datetime, timedelta
from dataclasses import dataclass
import logging
import asyncio
import asyncpg

from state.universe_state_manager import UniverseStateManager
from signals.enhanced_indicators import calculate_all_technical_indicators, ResidualReturnIndicatorConfig
from modeling.factor_models import ResidualReturnCalculator
from modeling.event_features import EventSequenceExtractor, EventCalendar, flatten_event_features_for_model

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


class ResidualReturnTrainingDataGenerator:
    """Generate training data for residual return prediction models."""
    
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
        
        # Cache for performance
        self._sector_cache = {}
        self._market_cap_cache = {}
    
    async def generate_training_dataset(self, 
                                      start_date: datetime,
                                      end_date: datetime,
                                      instrument_ids: Optional[List[int]] = None,
                                      batch_size: int = 100) -> pd.DataFrame:
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
        
        return training_df
    
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
            
            # Clean and validate features
            cleaned_features = {}
            for key, value in tech_features.items():
                if pd.isna(value) or np.isinf(value):
                    cleaned_features[key] = 0.0
                elif isinstance(value, (int, float)):
                    cleaned_features[key] = float(value)
                else:
                    cleaned_features[key] = 0.0
            
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


# Convenience function for generating training data
async def generate_residual_return_training_data(
    connection_pool: asyncpg.Pool,
    env,
    universe_state_manager: UniverseStateManager,
    start_date: datetime,
    end_date: datetime,
    instrument_ids: Optional[List[int]] = None,
    config: Optional[TrainingConfig] = None
) -> pd.DataFrame:
    """
    Convenience function to generate training data.
    
    Returns DataFrame with comprehensive features and residual return targets.
    """
    generator = ResidualReturnTrainingDataGenerator(
        connection_pool, env, universe_state_manager, config
    )
    
    return await generator.generate_training_dataset(
        start_date, end_date, instrument_ids
    )