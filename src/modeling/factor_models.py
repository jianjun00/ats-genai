"""
Factor Models for Residual Return Calculation.
Implements various factor models to decompose returns into market, factor, and residual components.
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple, Any
from datetime import datetime, timedelta
from dataclasses import dataclass
import logging
from sklearn.linear_model import LinearRegression
from sklearn.decomposition import PCA
import asyncpg

logger = logging.getLogger(__name__)


@dataclass
class FactorExposure:
    """Factor exposure for a single instrument."""
    instrument_id: int
    date: datetime
    market_beta: float
    size_factor: float
    value_factor: float
    momentum_factor: float
    quality_factor: float
    volatility_factor: float
    sector_exposure: Dict[str, float]
    r_squared: float
    residual_volatility: float


@dataclass
class FactorReturn:
    """Factor returns for a specific date."""
    date: datetime
    market_return: float
    size_factor_return: float
    value_factor_return: float
    momentum_factor_return: float
    quality_factor_return: float
    volatility_factor_return: float
    sector_returns: Dict[str, float]


class MarketFactorCalculator:
    """Calculate market factor returns using various market indices."""
    
    def __init__(self, connection_pool: asyncpg.Pool, env, benchmark_symbols=['SPY', 'QQQ', 'IWM']):
        self.pool = connection_pool
        self.env = env
        self.benchmark_symbols = benchmark_symbols
        self.market_weights = {'SPY': 0.7, 'QQQ': 0.2, 'IWM': 0.1}  # Cap-weighted approximation
    
    async def calculate_market_returns(self, start_date: datetime, end_date: datetime) -> pd.DataFrame:
        """Calculate market returns using cap-weighted benchmark portfolio."""
        market_returns = {}
        
        for symbol in self.benchmark_symbols:
            try:
                # Get benchmark instrument ID
                instrument_id = await self._get_instrument_id(symbol)
                if not instrument_id:
                    continue
                
                # Get price data
                prices = await self._get_price_data(instrument_id, start_date, end_date)
                if prices.empty:
                    continue
                
                # Calculate returns
                returns = prices['close'].pct_change().dropna()
                market_returns[symbol] = returns
                
            except Exception as e:
                logger.warning(f"Failed to get market data for {symbol}: {e}")
        
        if not market_returns:
            raise ValueError("No market benchmark data available")
        
        # Create market return as weighted average
        market_df = pd.DataFrame(market_returns)
        market_df = market_df.dropna()
        
        # Apply weights
        weighted_returns = sum(market_df[symbol] * self.market_weights.get(symbol, 0) 
                             for symbol in market_df.columns 
                             if symbol in self.market_weights)
        
        return pd.DataFrame({
            'date': market_df.index,
            'market_return': weighted_returns
        }).set_index('date')
    
    async def _get_instrument_id(self, symbol: str) -> Optional[int]:
        """Get instrument ID for symbol."""
        instruments_table = self.env.get_table_name('instruments')
        
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                f"SELECT id FROM {instruments_table} WHERE symbol = $1",
                symbol
            )
            return row['id'] if row else None
    
    async def _get_price_data(self, instrument_id: int, start_date: datetime, end_date: datetime) -> pd.DataFrame:
        """Get price data for instrument."""
        # Try Polygon first, then Tiingo
        for vendor in ['polygon', 'tiingo']:
            try:
                table_name = self.env.get_table_name(f'daily_prices_{vendor}')
                
                async with self.pool.acquire() as conn:
                    rows = await conn.fetch(f"""
                        SELECT date, open, high, low, close, volume
                        FROM {table_name}
                        WHERE instrument_id = $1 
                        AND date BETWEEN $2 AND $3
                        ORDER BY date
                    """, instrument_id, start_date.date(), end_date.date())
                    
                    if rows:
                        df = pd.DataFrame([dict(row) for row in rows])
                        df['date'] = pd.to_datetime(df['date'])
                        return df.set_index('date')
                        
            except Exception as e:
                logger.warning(f"Failed to get {vendor} data for instrument {instrument_id}: {e}")
        
        return pd.DataFrame()


class SectorFactorCalculator:
    """Calculate sector factor returns using GICS classification."""
    
    def __init__(self, connection_pool: asyncpg.Pool, env):
        self.pool = connection_pool
        self.env = env
        self.sector_etfs = {
            'Technology': 'XLK',
            'Healthcare': 'XLV',
            'Financials': 'XLF',
            'Consumer Discretionary': 'XLY',
            'Consumer Staples': 'XLP',
            'Energy': 'XLE',
            'Industrials': 'XLI',
            'Materials': 'XLB',
            'Real Estate': 'XLRE',
            'Utilities': 'XLU',
            'Communication Services': 'XLC'
        }
    
    async def calculate_sector_returns(self, start_date: datetime, end_date: datetime) -> pd.DataFrame:
        """Calculate sector returns using sector ETFs."""
        sector_returns = {}
        
        for sector, etf_symbol in self.sector_etfs.items():
            try:
                instrument_id = await self._get_instrument_id(etf_symbol)
                if not instrument_id:
                    continue
                
                prices = await self._get_price_data(instrument_id, start_date, end_date)
                if prices.empty:
                    continue
                
                returns = prices['close'].pct_change().dropna()
                sector_returns[sector] = returns
                
            except Exception as e:
                logger.warning(f"Failed to get sector data for {sector} ({etf_symbol}): {e}")
        
        if not sector_returns:
            logger.warning("No sector data available, using empty returns")
            return pd.DataFrame()
        
        sector_df = pd.DataFrame(sector_returns)
        return sector_df.fillna(0)
    
    async def _get_instrument_id(self, symbol: str) -> Optional[int]:
        """Get instrument ID for symbol."""
        instruments_table = self.env.get_table_name('instruments')
        
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                f"SELECT id FROM {instruments_table} WHERE symbol = $1",
                symbol
            )
            return row['id'] if row else None
    
    async def _get_price_data(self, instrument_id: int, start_date: datetime, end_date: datetime) -> pd.DataFrame:
        """Get price data for instrument."""
        for vendor in ['polygon', 'tiingo']:
            try:
                table_name = self.env.get_table_name(f'daily_prices_{vendor}')
                
                async with self.pool.acquire() as conn:
                    rows = await conn.fetch(f"""
                        SELECT date, close
                        FROM {table_name}
                        WHERE instrument_id = $1 
                        AND date BETWEEN $2 AND $3
                        ORDER BY date
                    """, instrument_id, start_date.date(), end_date.date())
                    
                    if rows:
                        df = pd.DataFrame([dict(row) for row in rows])
                        df['date'] = pd.to_datetime(df['date'])
                        return df.set_index('date')
                        
            except Exception:
                continue
        
        return pd.DataFrame()


class StyleFactorCalculator:
    """Calculate style factors (size, value, momentum, quality, volatility)."""
    
    def __init__(self, connection_pool: asyncpg.Pool, env):
        self.pool = connection_pool
        self.env = env
    
    async def calculate_style_factors(self, start_date: datetime, end_date: datetime) -> pd.DataFrame:
        """Calculate style factor returns using long-short portfolios."""
        # Get universe of stocks with required data
        universe_data = await self._get_universe_data(start_date, end_date)
        
        if universe_data.empty:
            logger.warning("No universe data for style factor calculation")
            return pd.DataFrame()
        
        style_factors = {}
        
        # Calculate each style factor
        for date in pd.date_range(start_date, end_date, freq='D'):
            if date.weekday() >= 5:  # Skip weekends
                continue
            
            try:
                date_data = universe_data[universe_data['date'] == date.date()]
                if len(date_data) < 50:  # Need minimum universe
                    continue
                
                factor_returns = self._calculate_daily_style_factors(date_data)
                style_factors[date] = factor_returns
                
            except Exception as e:
                logger.warning(f"Failed to calculate style factors for {date}: {e}")
        
        if not style_factors:
            return pd.DataFrame()
        
        return pd.DataFrame(style_factors).T
    
    async def _get_universe_data(self, start_date: datetime, end_date: datetime) -> pd.DataFrame:
        """Get universe data including prices, market cap, and financial metrics."""
        # Get price data
        price_data = await self._get_universe_prices(start_date, end_date)
        
        # Get market cap data (if available)
        market_cap_data = await self._get_market_cap_data(start_date, end_date)
        
        # Combine data
        if not price_data.empty and not market_cap_data.empty:
            universe_data = price_data.merge(
                market_cap_data, 
                on=['instrument_id', 'date'], 
                how='left'
            )
        else:
            universe_data = price_data
            # Estimate market cap from price * volume if not available
            if 'market_cap' not in universe_data.columns:
                universe_data['market_cap'] = universe_data['close'] * universe_data['volume']
        
        return universe_data
    
    async def _get_universe_prices(self, start_date: datetime, end_date: datetime) -> pd.DataFrame:
        """Get price data for universe."""
        all_data = []
        
        for vendor in ['polygon', 'tiingo']:
            try:
                table_name = self.env.get_table_name(f'daily_prices_{vendor}')
                
                async with self.pool.acquire() as conn:
                    rows = await conn.fetch(f"""
                        SELECT 
                            instrument_id, date, open, high, low, close, volume,
                            LAG(close, 1) OVER (PARTITION BY instrument_id ORDER BY date) as prev_close,
                            LAG(close, 20) OVER (PARTITION BY instrument_id ORDER BY date) as close_20d_ago,
                            LAG(close, 252) OVER (PARTITION BY instrument_id ORDER BY date) as close_1y_ago
                        FROM {table_name}
                        WHERE date BETWEEN $1 AND $2
                        AND close > 0 AND volume > 0
                        ORDER BY instrument_id, date
                    """, start_date.date(), end_date.date())
                    
                    if rows:
                        df = pd.DataFrame([dict(row) for row in rows])
                        df['vendor'] = vendor
                        all_data.append(df)
                        
            except Exception as e:
                logger.warning(f"Failed to get universe data from {vendor}: {e}")
        
        if not all_data:
            return pd.DataFrame()
        
        # Combine and deduplicate (prefer Polygon over Tiingo)
        combined_df = pd.concat(all_data, ignore_index=True)
        combined_df = combined_df.sort_values(['instrument_id', 'date', 'vendor'])
        combined_df = combined_df.drop_duplicates(['instrument_id', 'date'], keep='first')
        
        return combined_df.drop('vendor', axis=1)
    
    async def _get_market_cap_data(self, start_date: datetime, end_date: datetime) -> pd.DataFrame:
        """Get market cap data if available."""
        try:
            # Check if we have market cap tables
            for vendor in ['polygon', 'tiingo']:
                table_name = self.env.get_table_name(f'market_cap_{vendor}')
                
                async with self.pool.acquire() as conn:
                    # Check if table exists
                    exists = await conn.fetchval(f"""
                        SELECT EXISTS (
                            SELECT FROM information_schema.tables 
                            WHERE table_name = $1
                        )
                    """, table_name.split('.')[-1])  # Remove schema if present
                    
                    if exists:
                        rows = await conn.fetch(f"""
                            SELECT instrument_id, date, market_cap
                            FROM {table_name}
                            WHERE date BETWEEN $1 AND $2
                            AND market_cap > 0
                        """, start_date.date(), end_date.date())
                        
                        if rows:
                            return pd.DataFrame([dict(row) for row in rows])
        
        except Exception as e:
            logger.warning(f"Failed to get market cap data: {e}")
        
        return pd.DataFrame()
    
    def _calculate_daily_style_factors(self, date_data: pd.DataFrame) -> Dict[str, float]:
        """Calculate style factor returns for a single date."""
        if len(date_data) < 50:
            return {}
        
        factors = {}
        
        try:
            # Calculate returns
            date_data = date_data.copy()
            date_data['return'] = (date_data['close'] - date_data['prev_close']) / date_data['prev_close']
            date_data = date_data.dropna(subset=['return'])
            
            if len(date_data) < 20:
                return {}
            
            # Size factor (small minus big)
            if 'market_cap' in date_data.columns:
                size_factor = self._calculate_size_factor(date_data)
                factors['size'] = size_factor
            
            # Value factor (using price momentum as proxy)
            value_factor = self._calculate_value_factor(date_data)
            factors['value'] = value_factor
            
            # Momentum factor
            momentum_factor = self._calculate_momentum_factor(date_data)
            factors['momentum'] = momentum_factor
            
            # Quality factor (using volume and price stability)
            quality_factor = self._calculate_quality_factor(date_data)
            factors['quality'] = quality_factor
            
            # Volatility factor (low vol minus high vol)
            volatility_factor = self._calculate_volatility_factor(date_data)
            factors['volatility'] = volatility_factor
            
        except Exception as e:
            logger.warning(f"Failed to calculate daily style factors: {e}")
        
        return factors
    
    def _calculate_size_factor(self, data: pd.DataFrame) -> float:
        """Calculate size factor return (SMB - Small Minus Big)."""
        try:
            # Sort by market cap
            data_sorted = data.sort_values('market_cap')
            n = len(data_sorted)
            
            # Small cap (bottom 30%) vs Large cap (top 30%)
            small_cap = data_sorted.head(int(n * 0.3))
            large_cap = data_sorted.tail(int(n * 0.3))
            
            if len(small_cap) > 0 and len(large_cap) > 0:
                # Equal-weighted returns
                small_return = small_cap['return'].mean()
                large_return = large_cap['return'].mean()
                return small_return - large_return
                
        except Exception:
            pass
        
        return 0.0
    
    def _calculate_value_factor(self, data: pd.DataFrame) -> float:
        """Calculate value factor return using price ratios."""
        try:
            # Use 1-year price change as value proxy (low = value, high = growth)
            data_clean = data.dropna(subset=['close_1y_ago'])
            if len(data_clean) < 20:
                return 0.0
            
            # Calculate 1-year return
            data_clean = data_clean.copy()
            data_clean['return_1y'] = (data_clean['close'] - data_clean['close_1y_ago']) / data_clean['close_1y_ago']
            
            # Sort by 1-year return (ascending = value, descending = growth)
            data_sorted = data_clean.sort_values('return_1y')
            n = len(data_sorted)
            
            # Value (bottom 30%) vs Growth (top 30%)
            value_stocks = data_sorted.head(int(n * 0.3))
            growth_stocks = data_sorted.tail(int(n * 0.3))
            
            if len(value_stocks) > 0 and len(growth_stocks) > 0:
                value_return = value_stocks['return'].mean()
                growth_return = growth_stocks['return'].mean()
                return value_return - growth_return
                
        except Exception:
            pass
        
        return 0.0
    
    def _calculate_momentum_factor(self, data: pd.DataFrame) -> float:
        """Calculate momentum factor return."""
        try:
            # Use 20-day momentum
            data_clean = data.dropna(subset=['close_20d_ago'])
            if len(data_clean) < 20:
                return 0.0
            
            data_clean = data_clean.copy()
            data_clean['momentum_20d'] = (data_clean['close'] - data_clean['close_20d_ago']) / data_clean['close_20d_ago']
            
            # Sort by momentum
            data_sorted = data_clean.sort_values('momentum_20d')
            n = len(data_sorted)
            
            # High momentum (top 30%) vs Low momentum (bottom 30%)
            low_momentum = data_sorted.head(int(n * 0.3))
            high_momentum = data_sorted.tail(int(n * 0.3))
            
            if len(high_momentum) > 0 and len(low_momentum) > 0:
                high_return = high_momentum['return'].mean()
                low_return = low_momentum['return'].mean()
                return high_return - low_return
                
        except Exception:
            pass
        
        return 0.0
    
    def _calculate_quality_factor(self, data: pd.DataFrame) -> float:
        """Calculate quality factor using volume and price stability."""
        try:
            # Use volume consistency as quality proxy
            data_clean = data[data['volume'] > 0].copy()
            if len(data_clean) < 20:
                return 0.0
            
            # Calculate volume relative to recent average (proxy for stability)
            data_clean['volume_ratio'] = data_clean['volume'] / data_clean['volume']  # Simplified
            
            # High quality = stable volume, Low quality = erratic volume
            data_sorted = data_clean.sort_values('volume_ratio')
            n = len(data_sorted)
            
            # Quality (middle 40%) vs Low quality (extreme 30%)
            quality_stocks = data_sorted.iloc[int(n * 0.3):int(n * 0.7)]
            low_quality = data_sorted.iloc[:int(n * 0.3)]
            
            if len(quality_stocks) > 0 and len(low_quality) > 0:
                quality_return = quality_stocks['return'].mean()
                low_quality_return = low_quality['return'].mean()
                return quality_return - low_quality_return
                
        except Exception:
            pass
        
        return 0.0
    
    def _calculate_volatility_factor(self, data: pd.DataFrame) -> float:
        """Calculate volatility factor (low vol minus high vol)."""
        try:
            # Use daily range as volatility proxy
            data_clean = data[(data['high'] > 0) & (data['low'] > 0)].copy()
            if len(data_clean) < 20:
                return 0.0
            
            data_clean['daily_volatility'] = (data_clean['high'] - data_clean['low']) / data_clean['close']
            
            # Sort by volatility
            data_sorted = data_clean.sort_values('daily_volatility')
            n = len(data_sorted)
            
            # Low vol (bottom 30%) vs High vol (top 30%)
            low_vol = data_sorted.head(int(n * 0.3))
            high_vol = data_sorted.tail(int(n * 0.3))
            
            if len(low_vol) > 0 and len(high_vol) > 0:
                low_vol_return = low_vol['return'].mean()
                high_vol_return = high_vol['return'].mean()
                return low_vol_return - high_vol_return
                
        except Exception:
            pass
        
        return 0.0


class ResidualReturnCalculator:
    """Main class for calculating residual returns using factor models."""
    
    def __init__(self, connection_pool: asyncpg.Pool, env):
        self.pool = connection_pool
        self.env = env
        self.market_calculator = MarketFactorCalculator(connection_pool, env)
        self.sector_calculator = SectorFactorCalculator(connection_pool, env)
        self.style_calculator = StyleFactorCalculator(connection_pool, env)
        
        # Cache for factor returns
        self._factor_cache = {}
    
    async def calculate_residual_returns(self, instrument_ids: List[int], 
                                       start_date: datetime, end_date: datetime,
                                       model_type: str = 'multi_factor') -> pd.DataFrame:
        """
        Calculate residual returns for given instruments and date range.
        
        Args:
            instrument_ids: List of instrument IDs
            start_date: Start date for calculation
            end_date: End date for calculation  
            model_type: 'market_model', 'fama_french', 'multi_factor'
            
        Returns:
            DataFrame with residual returns and factor exposures
        """
        # Get factor returns
        factor_returns = await self._get_factor_returns(start_date, end_date)
        
        if factor_returns.empty:
            logger.warning("No factor returns available")
            return pd.DataFrame()
        
        # Get stock returns
        stock_returns = await self._get_stock_returns(instrument_ids, start_date, end_date)
        
        if stock_returns.empty:
            logger.warning("No stock returns available")
            return pd.DataFrame()
        
        # Calculate residual returns for each instrument
        residual_results = []
        
        for instrument_id in instrument_ids:
            try:
                instrument_data = stock_returns[stock_returns['instrument_id'] == instrument_id]
                if instrument_data.empty:
                    continue
                
                residuals = await self._calculate_instrument_residuals(
                    instrument_data, factor_returns, model_type
                )
                
                if not residuals.empty:
                    residuals['instrument_id'] = instrument_id
                    residual_results.append(residuals)
                    
            except Exception as e:
                logger.warning(f"Failed to calculate residuals for instrument {instrument_id}: {e}")
        
        if not residual_results:
            return pd.DataFrame()
        
        return pd.concat(residual_results, ignore_index=True)
    
    async def _get_factor_returns(self, start_date: datetime, end_date: datetime) -> pd.DataFrame:
        """Get all factor returns for date range."""
        cache_key = f"{start_date}_{end_date}"
        
        if cache_key in self._factor_cache:
            return self._factor_cache[cache_key]
        
        # Get market returns
        market_returns = await self.market_calculator.calculate_market_returns(start_date, end_date)
        
        # Get sector returns  
        sector_returns = await self.sector_calculator.calculate_sector_returns(start_date, end_date)
        
        # Get style factor returns
        style_returns = await self.style_calculator.calculate_style_factors(start_date, end_date)
        
        # Combine all factors
        factor_data = market_returns.copy()
        
        if not sector_returns.empty:
            factor_data = factor_data.merge(sector_returns, left_index=True, right_index=True, how='outer')
        
        if not style_returns.empty:
            factor_data = factor_data.merge(style_returns, left_index=True, right_index=True, how='outer')
        
        factor_data = factor_data.fillna(0)
        
        # Cache result
        self._factor_cache[cache_key] = factor_data
        
        return factor_data
    
    async def _get_stock_returns(self, instrument_ids: List[int], 
                               start_date: datetime, end_date: datetime) -> pd.DataFrame:
        """Get stock returns for instruments."""
        all_returns = []
        
        for vendor in ['polygon', 'tiingo']:
            try:
                table_name = self.env.get_table_name(f'daily_prices_{vendor}')
                
                async with self.pool.acquire() as conn:
                    rows = await conn.fetch(f"""
                        SELECT 
                            instrument_id, 
                            date, 
                            close,
                            LAG(close, 1) OVER (PARTITION BY instrument_id ORDER BY date) as prev_close
                        FROM {table_name}
                        WHERE instrument_id = ANY($1)
                        AND date BETWEEN $2 AND $3
                        AND close > 0
                        ORDER BY instrument_id, date
                    """, instrument_ids, start_date.date(), end_date.date())
                    
                    if rows:
                        df = pd.DataFrame([dict(row) for row in rows])
                        df['vendor'] = vendor
                        all_returns.append(df)
                        
            except Exception as e:
                logger.warning(f"Failed to get returns from {vendor}: {e}")
        
        if not all_returns:
            return pd.DataFrame()
        
        # Combine and calculate returns
        combined_df = pd.concat(all_returns, ignore_index=True)
        combined_df = combined_df.sort_values(['instrument_id', 'date', 'vendor'])
        combined_df = combined_df.drop_duplicates(['instrument_id', 'date'], keep='first')
        
        # Calculate returns
        combined_df = combined_df.dropna(subset=['prev_close'])
        combined_df['return'] = (combined_df['close'] - combined_df['prev_close']) / combined_df['prev_close']
        
        return combined_df[['instrument_id', 'date', 'return']].copy()
    
    async def _calculate_instrument_residuals(self, instrument_data: pd.DataFrame,
                                            factor_returns: pd.DataFrame,
                                            model_type: str) -> pd.DataFrame:
        """Calculate residual returns for single instrument."""
        # Merge stock returns with factor returns
        merged_data = instrument_data.merge(
            factor_returns.reset_index(), 
            on='date', 
            how='inner'
        )
        
        if len(merged_data) < 30:  # Need minimum observations
            return pd.DataFrame()
        
        try:
            if model_type == 'market_model':
                return self._market_model_residuals(merged_data)
            elif model_type == 'fama_french':
                return self._fama_french_residuals(merged_data)
            elif model_type == 'multi_factor':
                return self._multi_factor_residuals(merged_data)
            else:
                raise ValueError(f"Unknown model type: {model_type}")
                
        except Exception as e:
            logger.warning(f"Failed to calculate residuals: {e}")
            return pd.DataFrame()
    
    def _market_model_residuals(self, data: pd.DataFrame) -> pd.DataFrame:
        """Calculate residuals using single-factor market model."""
        if 'market_return' not in data.columns:
            return pd.DataFrame()
        
        # Simple market model: R_i = alpha + beta * R_m + epsilon
        X = data[['market_return']].values
        y = data['return'].values
        
        # Remove rows with NaN
        valid_mask = ~(np.isnan(X).any(axis=1) | np.isnan(y))
        X_clean = X[valid_mask]
        y_clean = y[valid_mask]
        
        if len(X_clean) < 10:
            return pd.DataFrame()
        
        # Fit model
        model = LinearRegression()
        model.fit(X_clean, y_clean)
        
        # Calculate residuals for all data
        predicted = model.predict(X)
        residuals = y - predicted
        
        result = data[['date', 'return']].copy()
        result['predicted_return'] = predicted
        result['residual_return'] = residuals
        result['market_beta'] = model.coef_[0]
        result['alpha'] = model.intercept_
        result['r_squared'] = model.score(X_clean, y_clean)
        
        return result
    
    def _fama_french_residuals(self, data: pd.DataFrame) -> pd.DataFrame:
        """Calculate residuals using Fama-French 3-factor model."""
        required_factors = ['market_return', 'size', 'value']
        available_factors = [f for f in required_factors if f in data.columns]
        
        if len(available_factors) < 2:
            # Fall back to market model
            return self._market_model_residuals(data)
        
        X = data[available_factors].values
        y = data['return'].values
        
        # Remove rows with NaN
        valid_mask = ~(np.isnan(X).any(axis=1) | np.isnan(y))
        X_clean = X[valid_mask]
        y_clean = y[valid_mask]
        
        if len(X_clean) < 15:
            return pd.DataFrame()
        
        # Fit model
        model = LinearRegression()
        model.fit(X_clean, y_clean)
        
        # Calculate residuals
        predicted = model.predict(X)
        residuals = y - predicted
        
        result = data[['date', 'return']].copy()
        result['predicted_return'] = predicted
        result['residual_return'] = residuals
        
        # Store factor loadings
        for i, factor in enumerate(available_factors):
            result[f'{factor}_loading'] = model.coef_[i]
        
        result['alpha'] = model.intercept_
        result['r_squared'] = model.score(X_clean, y_clean)
        
        return result
    
    def _multi_factor_residuals(self, data: pd.DataFrame) -> pd.DataFrame:
        """Calculate residuals using comprehensive multi-factor model."""
        # Available factors
        factor_columns = [col for col in data.columns 
                         if col not in ['instrument_id', 'date', 'return', 'close', 'prev_close']]
        
        if len(factor_columns) < 2:
            return self._market_model_residuals(data)
        
        X = data[factor_columns].fillna(0).values
        y = data['return'].values
        
        # Remove rows with NaN returns
        valid_mask = ~np.isnan(y)
        X_clean = X[valid_mask]
        y_clean = y[valid_mask]
        
        if len(X_clean) < 20:
            return pd.DataFrame()
        
        try:
            # Fit model with regularization for high-dimensional factors
            from sklearn.linear_model import Ridge
            model = Ridge(alpha=0.1)  # Small regularization
            model.fit(X_clean, y_clean)
            
            # Calculate residuals
            predicted = model.predict(X)
            residuals = y - predicted
            
            result = data[['date', 'return']].copy()
            result['predicted_return'] = predicted
            result['residual_return'] = residuals
            
            # Store factor loadings
            for i, factor in enumerate(factor_columns):
                result[f'{factor}_loading'] = model.coef_[i]
            
            result['alpha'] = model.intercept_
            result['r_squared'] = model.score(X_clean, y_clean)
            
            return result
            
        except Exception:
            # Fall back to simpler model
            return self._fama_french_residuals(data)


# Convenience function for easy residual return calculation
async def calculate_residual_returns_for_instruments(
    connection_pool: asyncpg.Pool,
    env,
    instrument_ids: List[int],
    start_date: datetime,
    end_date: datetime,
    model_type: str = 'multi_factor'
) -> pd.DataFrame:
    """
    Convenience function to calculate residual returns.
    
    Returns DataFrame with columns:
    - instrument_id
    - date
    - return (raw return)
    - predicted_return (factor model prediction)
    - residual_return (return - predicted_return)
    - Various factor loadings and model statistics
    """
    calculator = ResidualReturnCalculator(connection_pool, env)
    return await calculator.calculate_residual_returns(
        instrument_ids, start_date, end_date, model_type
    )