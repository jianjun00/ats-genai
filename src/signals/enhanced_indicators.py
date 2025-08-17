"""
Enhanced Technical Indicators for Residual Return Prediction.
Comprehensive set of indicators including EMAs, ATR, RSI, VWAP, and volume analysis.
"""

import pandas as pd
import numpy as np
from typing import Dict, Any, Optional
from .indicator import Indicator


class EMAIndicator(Indicator):
    """Exponential Moving Average indicator with price relationship analysis."""
    
    def __init__(self, period: int):
        super().__init__()
        self.period = period
        self.name = f"EMA_{period}"
    
    def calculate(self, price_history: pd.DataFrame) -> Dict[str, Any]:
        """Calculate EMA and price relationship metrics."""
        if len(price_history) < self.period:
            return {'value': None, 'status': 'insufficient_data'}
        
        try:
            close_prices = price_history['close']
            ema = close_prices.ewm(span=self.period, adjust=False).mean().iloc[-1]
            current_price = close_prices.iloc[-1]
            
            # Price vs EMA ratio
            price_vs_ema = (current_price / ema) - 1
            
            # EMA slope (trend direction)
            if len(price_history) >= self.period + 5:
                ema_series = close_prices.ewm(span=self.period, adjust=False).mean()
                ema_slope = (ema_series.iloc[-1] - ema_series.iloc[-6]) / ema_series.iloc[-6]
            else:
                ema_slope = 0
            
            # Distance from EMA in standard deviations
            if len(price_history) >= self.period + 20:
                price_ema_diff = close_prices - close_prices.ewm(span=self.period, adjust=False).mean()
                std_dev = price_ema_diff.rolling(20).std().iloc[-1]
                ema_distance_std = price_ema_diff.iloc[-1] / std_dev if std_dev > 0 else 0
            else:
                ema_distance_std = 0
            
            return {
                'value': ema,
                'price_vs_ema': price_vs_ema,
                'ema_slope': ema_slope,
                'ema_distance_std': ema_distance_std,
                'status': 'valid'
            }
            
        except Exception as e:
            return {'value': None, 'status': f'calculation_error: {str(e)}'}


class ATRIndicator(Indicator):
    """Average True Range indicator with volatility analysis."""
    
    def __init__(self, period: int = 14):
        super().__init__()
        self.period = period
        self.name = f"ATR_{period}"
    
    def calculate(self, price_history: pd.DataFrame) -> Dict[str, Any]:
        """Calculate ATR and volatility metrics."""
        if len(price_history) < self.period + 1:
            return {'value': None, 'status': 'insufficient_data'}
        
        try:
            high = price_history['high']
            low = price_history['low']
            close = price_history['close']
            prev_close = close.shift(1)
            
            # True Range calculation
            tr1 = high - low
            tr2 = abs(high - prev_close)
            tr3 = abs(low - prev_close)
            
            true_range = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
            atr = true_range.rolling(window=self.period).mean().iloc[-1]
            
            # ATR as percentage of price
            current_price = close.iloc[-1]
            atr_percentage = atr / current_price if current_price > 0 else 0
            
            # ATR percentile (volatility regime)
            if len(price_history) >= self.period + 60:
                atr_series = true_range.rolling(window=self.period).mean()
                atr_percentile = (atr_series.iloc[-1] > atr_series.tail(60)).sum() / 60
            else:
                atr_percentile = 0.5
            
            # Volatility trend
            if len(price_history) >= self.period + 10:
                atr_series = true_range.rolling(window=self.period).mean()
                atr_trend = (atr_series.iloc[-1] - atr_series.iloc[-11]) / atr_series.iloc[-11]
            else:
                atr_trend = 0
            
            return {
                'value': atr,
                'atr_percentage': atr_percentage,
                'atr_percentile': atr_percentile,
                'volatility_trend': atr_trend,
                'status': 'valid'
            }
            
        except Exception as e:
            return {'value': None, 'status': f'calculation_error: {str(e)}'}


class RSIIndicator(Indicator):
    """Relative Strength Index with momentum analysis."""
    
    def __init__(self, period: int = 14):
        super().__init__()
        self.period = period
        self.name = f"RSI_{period}"
    
    def calculate(self, price_history: pd.DataFrame) -> Dict[str, Any]:
        """Calculate RSI and momentum signals."""
        if len(price_history) < self.period + 1:
            return {'value': None, 'status': 'insufficient_data'}
        
        try:
            close_prices = price_history['close']
            delta = close_prices.diff()
            
            gain = delta.where(delta > 0, 0)
            loss = -delta.where(delta < 0, 0)
            
            avg_gain = gain.rolling(window=self.period).mean()
            avg_loss = loss.rolling(window=self.period).mean()
            
            rs = avg_gain / avg_loss
            rsi = 100 - (100 / (1 + rs))
            current_rsi = rsi.iloc[-1]
            
            # RSI signal classification
            if current_rsi > 70:
                signal = 'overbought'
                signal_strength = (current_rsi - 70) / 30
            elif current_rsi < 30:
                signal = 'oversold'
                signal_strength = (30 - current_rsi) / 30
            else:
                signal = 'neutral'
                signal_strength = 0
            
            # RSI divergence (simplified)
            if len(price_history) >= self.period + 20:
                rsi_slope = (rsi.iloc[-1] - rsi.iloc[-11]) / 10
                price_slope = (close_prices.iloc[-1] - close_prices.iloc[-11]) / close_prices.iloc[-11]
                divergence = rsi_slope * price_slope < 0  # Opposite directions
            else:
                divergence = False
            
            # RSI momentum
            if len(price_history) >= self.period + 5:
                rsi_momentum = rsi.iloc[-1] - rsi.iloc[-6]
            else:
                rsi_momentum = 0
            
            return {
                'value': current_rsi,
                'signal': signal,
                'signal_strength': signal_strength,
                'divergence': divergence,
                'rsi_momentum': rsi_momentum,
                'status': 'valid'
            }
            
        except Exception as e:
            return {'value': None, 'status': f'calculation_error: {str(e)}'}


class VWAPIndicator(Indicator):
    """Volume Weighted Average Price with session analysis."""
    
    def __init__(self):
        super().__init__()
        self.name = "VWAP"
    
    def calculate(self, price_history: pd.DataFrame) -> Dict[str, Any]:
        """Calculate VWAP and volume-price relationships."""
        if price_history.empty or 'volume' not in price_history.columns:
            return {'value': None, 'status': 'no_volume_data'}
        
        try:
            # Calculate typical price
            typical_price = (
                price_history['high'] + 
                price_history['low'] + 
                price_history['close']
            ) / 3
            
            volume = price_history['volume']
            
            # VWAP calculation
            cum_vol_price = (typical_price * volume).cumsum()
            cum_volume = volume.cumsum()
            vwap = cum_vol_price / cum_volume
            
            current_vwap = vwap.iloc[-1]
            current_price = price_history['close'].iloc[-1]
            
            # Price vs VWAP
            price_vs_vwap = (current_price / current_vwap) - 1
            
            # VWAP slope (trend)
            if len(price_history) >= 10:
                vwap_slope = (vwap.iloc[-1] - vwap.iloc[-11]) / vwap.iloc[-11]
            else:
                vwap_slope = 0
            
            # Volume profile analysis
            above_vwap_volume = volume[price_history['close'] > vwap].sum()
            below_vwap_volume = volume[price_history['close'] <= vwap].sum()
            total_volume = volume.sum()
            
            volume_balance = (above_vwap_volume - below_vwap_volume) / total_volume if total_volume > 0 else 0
            
            return {
                'value': current_vwap,
                'price_vs_vwap': price_vs_vwap,
                'vwap_slope': vwap_slope,
                'volume_balance': volume_balance,
                'status': 'valid'
            }
            
        except Exception as e:
            return {'value': None, 'status': f'calculation_error: {str(e)}'}


class VolumeIndicators(Indicator):
    """Volume analysis indicators."""
    
    def __init__(self, sma_period: int = 20):
        super().__init__()
        self.sma_period = sma_period
        self.name = f"Volume_{sma_period}"
    
    def calculate(self, price_history: pd.DataFrame) -> Dict[str, Any]:
        """Calculate volume indicators."""
        if price_history.empty or 'volume' not in price_history.columns:
            return {'value': None, 'status': 'no_volume_data'}
        
        try:
            volume = price_history['volume']
            close_prices = price_history['close']
            
            # Volume SMA
            volume_sma = volume.rolling(window=self.sma_period).mean().iloc[-1]
            current_volume = volume.iloc[-1]
            
            # Volume ratio
            volume_ratio = current_volume / volume_sma if volume_sma > 0 else 1
            
            # Price-volume correlation
            if len(price_history) >= self.sma_period:
                returns = close_prices.pct_change().dropna()
                volume_changes = volume.pct_change().dropna()
                
                if len(returns) > 1 and len(volume_changes) > 1:
                    min_len = min(len(returns), len(volume_changes))
                    correlation = np.corrcoef(
                        returns.tail(min_len), 
                        volume_changes.tail(min_len)
                    )[0, 1]
                    if np.isnan(correlation):
                        correlation = 0
                else:
                    correlation = 0
            else:
                correlation = 0
            
            # On-balance volume (simplified)
            price_changes = close_prices.diff()
            obv_changes = np.where(price_changes > 0, volume, 
                                 np.where(price_changes < 0, -volume, 0))
            obv = pd.Series(obv_changes).cumsum().iloc[-1]
            
            # Volume momentum
            if len(price_history) >= 10:
                recent_avg_volume = volume.tail(5).mean()
                prev_avg_volume = volume.iloc[-10:-5].mean()
                volume_momentum = (recent_avg_volume - prev_avg_volume) / prev_avg_volume if prev_avg_volume > 0 else 0
            else:
                volume_momentum = 0
            
            return {
                'volume_sma': volume_sma,
                'volume_ratio': volume_ratio,
                'price_volume_correlation': correlation,
                'obv': obv,
                'volume_momentum': volume_momentum,
                'status': 'valid'
            }
            
        except Exception as e:
            return {'value': None, 'status': f'calculation_error: {str(e)}'}


class PriceActionIndicators(Indicator):
    """Price action and market microstructure indicators."""
    
    def __init__(self):
        super().__init__()
        self.name = "PriceAction"
    
    def calculate(self, price_history: pd.DataFrame) -> Dict[str, Any]:
        """Calculate price action indicators."""
        if len(price_history) < 5:
            return {'value': None, 'status': 'insufficient_data'}
        
        try:
            high = price_history['high']
            low = price_history['low']
            close = price_history['close']
            open_price = price_history['open']
            
            # Bid-ask spread proxy (high-low as % of close)
            current_spread_proxy = (high.iloc[-1] - low.iloc[-1]) / close.iloc[-1]
            avg_spread_proxy = ((high - low) / close).rolling(20).mean().iloc[-1]
            spread_ratio = current_spread_proxy / avg_spread_proxy if avg_spread_proxy > 0 else 1
            
            # Price impact proxy (gap from previous close)
            if len(price_history) >= 2:
                price_gap = (open_price.iloc[-1] - close.iloc[-2]) / close.iloc[-2]
            else:
                price_gap = 0
            
            # Intraday range
            intraday_range = (high.iloc[-1] - low.iloc[-1]) / open_price.iloc[-1] if open_price.iloc[-1] > 0 else 0
            
            # Body-to-range ratio (strength of directional move)
            body_size = abs(close.iloc[-1] - open_price.iloc[-1])
            range_size = high.iloc[-1] - low.iloc[-1]
            body_to_range = body_size / range_size if range_size > 0 else 0
            
            # Close position in range
            if range_size > 0:
                close_position = (close.iloc[-1] - low.iloc[-1]) / range_size
            else:
                close_position = 0.5
            
            # Momentum indicators
            returns_1d = (close.iloc[-1] - close.iloc[-2]) / close.iloc[-2] if len(price_history) >= 2 else 0
            returns_5d = (close.iloc[-1] - close.iloc[-6]) / close.iloc[-6] if len(price_history) >= 6 else 0
            
            return {
                'spread_proxy': current_spread_proxy,
                'spread_ratio': spread_ratio,
                'price_gap': price_gap,
                'intraday_range': intraday_range,
                'body_to_range': body_to_range,
                'close_position': close_position,
                'returns_1d': returns_1d,
                'returns_5d': returns_5d,
                'status': 'valid'
            }
            
        except Exception as e:
            return {'value': None, 'status': f'calculation_error: {str(e)}'}


# Enhanced configuration for comprehensive technical analysis
from .indicator_config import IndicatorConfig
from dataclasses import dataclass


@dataclass 
class ResidualReturnIndicatorConfig(IndicatorConfig):
    """Enhanced indicator configuration for residual return prediction."""
    
    @classmethod
    def comprehensive_config(cls) -> 'ResidualReturnIndicatorConfig':
        """Create comprehensive configuration for residual return prediction."""
        config = cls()
        
        # EMAs for multi-timeframe trend analysis
        for period in [5, 8, 21, 50, 170, 200]:
            config.add_indicator(f'EMA_{period}', lambda p=period: EMAIndicator(p))
        
        # Volatility and momentum indicators
        config.add_indicator('ATR_14', lambda: ATRIndicator(14))
        config.add_indicator('RSI_14', lambda: RSIIndicator(14))
        config.add_indicator('RSI_21', lambda: RSIIndicator(21))
        
        # Volume and microstructure
        config.add_indicator('VWAP', VWAPIndicator)
        config.add_indicator('Volume_20', lambda: VolumeIndicators(20))
        config.add_indicator('PriceAction', PriceActionIndicators)
        
        return config
    
    @classmethod
    def minimal_config(cls) -> 'ResidualReturnIndicatorConfig':
        """Create minimal configuration for basic prediction."""
        config = cls()
        
        # Essential indicators only
        config.add_indicator('EMA_21', lambda: EMAIndicator(21))
        config.add_indicator('EMA_50', lambda: EMAIndicator(50))
        config.add_indicator('ATR_14', lambda: ATRIndicator(14))
        config.add_indicator('RSI_14', lambda: RSIIndicator(14))
        config.add_indicator('VWAP', VWAPIndicator)
        
        return config


def calculate_all_technical_indicators(price_history: pd.DataFrame, 
                                     config: Optional[ResidualReturnIndicatorConfig] = None) -> Dict[str, Any]:
    """
    Calculate all technical indicators for given price history.
    
    Args:
        price_history: DataFrame with OHLCV data
        config: Indicator configuration (default: comprehensive)
        
    Returns:
        Dictionary with all calculated indicators
    """
    if config is None:
        config = ResidualReturnIndicatorConfig.comprehensive_config()
    
    indicators = config.create_indicator_instances()
    results = {}
    
    for name, indicator in indicators.items():
        try:
            result = indicator.calculate(price_history)
            
            # Flatten the result with prefixed keys
            for key, value in result.items():
                if key != 'status':
                    results[f'{name}_{key}'] = value
                else:
                    results[f'{name}_status'] = value
                    
        except Exception as e:
            # Log error but continue with other indicators
            results[f'{name}_status'] = f'error: {str(e)}'
    
    return results