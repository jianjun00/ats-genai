"""
Enhanced Technical Indicators for Residual Return Prediction.
Comprehensive set of indicators including EMAs, ATR, RSI, VWAP, and volume analysis.
"""

import pandas as pd
import numpy as np
import pytz
from datetime import time, datetime, timedelta
from typing import Dict, Any, Optional
from ..indicator import Indicator


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

            # Validate EMA calculation
            if pd.isna(ema) or np.isinf(ema) or ema <= 0:
                return {'value': None, 'status': 'invalid_ema_calculation'}

            # Log for debugging (if current price is significantly different from EMA, it might indicate data issues)
            if abs(current_price - ema) > current_price * 0.5:  # More than 50% difference
                import logging
                logger = logging.getLogger(__name__)
                logger.warning(f"Large EMA divergence detected: price={current_price:.2f}, EMA{self.period}={ema:.2f}")

            # Price vs EMA ratio (this should be a small decimal, e.g., -0.02 = -2%)
            price_vs_ema = (current_price / ema) - 1

            # EMA slope (trend direction) - percentage change per period
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

            # IMPORTANT: The 'value' field contains the actual EMA price level
            # The other fields are ratios/percentages and will be small numbers (typically -1 to +1)
            result = {
                'value': float(ema),  # Actual EMA value in price units
                'ema_price': float(ema),  # Duplicate for clarity - this is the actual EMA price
                'current_price': float(current_price),  # Current stock price for reference
                'price_vs_ema_ratio': float(price_vs_ema),  # Ratio: (price/EMA) - 1, expect small numbers like -0.02
                'ema_slope_pct': float(ema_slope),  # Percentage slope, expect small numbers like 0.01
                'ema_distance_std': float(ema_distance_std),  # Standard deviations, expect -3 to +3
                'status': 'valid'
            }

            # Legacy field names for backward compatibility
            result['price_vs_ema'] = result['price_vs_ema_ratio']
            result['ema_slope'] = result['ema_slope_pct']

            return result

        except Exception as e:
            import logging
            logger = logging.getLogger(__name__)
            logger.error(f"EMA{self.period} calculation failed: {str(e)}")
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


class SessionVWAPIndicator(Indicator):
    """Session-based VWAP indicator for specific market times and durations."""

    def __init__(self, session_type: str = 'us_open', duration_minutes: int = 30):
        """
        Initialize session VWAP indicator.

        Args:
            session_type: 'us_open', 'us_close', or 'london_close'
            duration_minutes: 30 or 60 minutes
        """
        super().__init__()
        self.session_type = session_type
        self.duration_minutes = duration_minutes
        self.name = f"SessionVWAP_{session_type}_{duration_minutes}min"

        # Define session times (in respective timezones)
        self.session_times = {
            'us_open': {
                'time': time(9, 30),  # 9:30 AM ET
                'timezone': pytz.timezone('US/Eastern'),
                'name': 'US Market Open'
            },
            'us_close': {
                'time': time(16, 0),  # 4:00 PM ET
                'timezone': pytz.timezone('US/Eastern'),
                'name': 'US Market Close'
            },
            'london_close': {
                'time': time(16, 30),  # 4:30 PM GMT
                'timezone': pytz.timezone('Europe/London'),
                'name': 'London Market Close'
            }
        }

    def calculate(self, price_history: pd.DataFrame) -> Dict[str, Any]:
        """Calculate session-based VWAP."""
        if price_history.empty or 'volume' not in price_history.columns:
            return {'value': None, 'status': 'no_volume_data'}

        try:
            # Ensure we have timestamp information
            if hasattr(price_history.index, 'tz_localize'):
                timestamps = price_history.index
            elif 'timestamp' in price_history.columns:
                timestamps = pd.to_datetime(price_history['timestamp'])
            else:
                return {'value': None, 'status': 'no_timestamp_data'}

            # Get session configuration
            session_config = self.session_times[self.session_type]
            session_tz = session_config['timezone']
            session_time = session_config['time']

            # Convert timestamps to session timezone if needed
            if timestamps.tz is None:
                # Assume UTC if no timezone info
                timestamps = timestamps.tz_localize('UTC')

            # Convert to session timezone
            timestamps_local = timestamps.tz_convert(session_tz)

            # Find session windows
            session_data = self._extract_session_windows(
                price_history, timestamps_local, session_time
            )

            if session_data.empty:
                return {'value': None, 'status': 'no_session_data'}

            # Calculate VWAP for session data
            vwap_result = self._calculate_session_vwap(session_data)

            return vwap_result

        except Exception as e:
            return {'value': None, 'status': f'calculation_error: {str(e)}'}

    def _extract_session_windows(self, price_history: pd.DataFrame,
                                timestamps: pd.DatetimeIndex,
                                session_time: time) -> pd.DataFrame:
        """Extract data within session windows."""

        session_data_list = []

        # Group by date to find session periods
        for date in timestamps.date:
            day_data = price_history[timestamps.date == date].copy()
            if day_data.empty:
                continue

            day_timestamps = timestamps[timestamps.date == date]

            # Create session start datetime
            session_start = datetime.combine(date, session_time)
            session_start = pytz.timezone(timestamps.tz.zone).localize(session_start)

            # Define session window
            session_end = session_start + timedelta(minutes=self.duration_minutes)

            # Filter data within session window
            mask = (day_timestamps >= session_start) & (day_timestamps <= session_end)
            session_window_data = day_data[mask]

            if not session_window_data.empty:
                # Add session metadata
                session_window_data = session_window_data.copy()
                session_window_data['session_start'] = session_start
                session_window_data['session_end'] = session_end
                session_window_data['session_date'] = date

                session_data_list.append(session_window_data)

        if session_data_list:
            return pd.concat(session_data_list, ignore_index=False)
        else:
            return pd.DataFrame()

    def _calculate_session_vwap(self, session_data: pd.DataFrame) -> Dict[str, Any]:
        """Calculate VWAP metrics for session data."""

        # Calculate typical price
        typical_price = (
            session_data['high'] +
            session_data['low'] +
            session_data['close']
        ) / 3

        volume = session_data['volume']

        # Calculate VWAP for the entire session period
        total_vol_price = (typical_price * volume).sum()
        total_volume = volume.sum()

        if total_volume == 0:
            return {'value': None, 'status': 'no_volume_in_session'}

        session_vwap = total_vol_price / total_volume

        # Current price (last close in session data)
        current_price = session_data['close'].iloc[-1]

        # Price vs Session VWAP
        price_vs_session_vwap = (current_price / session_vwap) - 1

        # Calculate session volume metrics
        above_vwap_volume = volume[session_data['close'] > session_vwap].sum()
        below_vwap_volume = volume[session_data['close'] <= session_vwap].sum()
        session_volume_balance = (above_vwap_volume - below_vwap_volume) / total_volume

        # Session VWAP trend (if enough data points)
        session_vwap_trend = 0
        if len(session_data) >= 3:
            # Calculate rolling VWAP within session
            rolling_vol_price = (typical_price * volume).rolling(window=3).sum()
            rolling_volume = volume.rolling(window=3).sum()
            rolling_vwap = rolling_vol_price / rolling_volume

            if len(rolling_vwap.dropna()) >= 2:
                session_vwap_trend = (rolling_vwap.iloc[-1] - rolling_vwap.iloc[-2]) / rolling_vwap.iloc[-2]

        # Session participation metrics
        session_bar_count = len(session_data)
        avg_volume_per_bar = total_volume / session_bar_count if session_bar_count > 0 else 0

        # Session price range analysis
        session_high = session_data['high'].max()
        session_low = session_data['low'].min()
        session_range = session_high - session_low
        vwap_position_in_range = (session_vwap - session_low) / session_range if session_range > 0 else 0.5

        return {
            'value': session_vwap,
            'session_vwap': session_vwap,
            'price_vs_session_vwap': price_vs_session_vwap,
            'session_volume_balance': session_volume_balance,
            'session_vwap_trend': session_vwap_trend,
            'total_session_volume': total_volume,
            'session_bar_count': session_bar_count,
            'avg_volume_per_bar': avg_volume_per_bar,
            'session_range': session_range,
            'vwap_position_in_range': vwap_position_in_range,
            'session_high': session_high,
            'session_low': session_low,
            'session_type': self.session_type,
            'duration_minutes': self.duration_minutes,
            'status': 'valid'
        }


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


class CumulativeVolumeIndicator(Indicator):
    """Cumulative Volume indicator for session and interval analysis."""

    def __init__(self, reset_interval: str = 'daily'):
        super().__init__()
        self.reset_interval = reset_interval  # 'daily', 'session', 'never'
        self.name = f"CumVolume_{reset_interval}"

    def calculate(self, price_history: pd.DataFrame) -> Dict[str, Any]:
        """Calculate cumulative volume indicators."""
        if price_history.empty or 'volume' not in price_history.columns:
            return {'value': None, 'status': 'no_volume_data'}

        try:
            volume = price_history['volume']
            timestamp = price_history.index if hasattr(price_history.index, 'date') else pd.to_datetime(price_history.get('timestamp', range(len(price_history))))

            # Calculate cumulative volume based on reset interval
            if self.reset_interval == 'daily':
                # Reset cumsum at start of each trading day
                if hasattr(timestamp, 'date'):
                    # Create daily groups and apply cumsum within each group
                    daily_groups = timestamp.normalize()  # Use normalize instead of dt.date
                    volume_with_groups = pd.DataFrame({'volume': volume, 'date': daily_groups})
                    cum_volume = volume_with_groups.groupby('date')['volume'].cumsum()
                else:
                    cum_volume = volume.cumsum()
            elif self.reset_interval == 'session':
                # Reset at market open (9:30 AM ET)
                if hasattr(timestamp, 'time'):
                    session_times = pd.Series(timestamp).dt.time
                    session_reset = session_times >= pd.Timestamp('09:30:00').time()
                    session_groups = session_reset.cumsum()
                    volume_with_groups = pd.DataFrame({'volume': volume, 'session': session_groups})
                    cum_volume = volume_with_groups.groupby('session')['volume'].cumsum()
                else:
                    cum_volume = volume.cumsum()
            else:  # 'never'
                cum_volume = volume.cumsum()

            current_cum_volume = cum_volume.iloc[-1]

            # Volume flow analysis
            close_prices = price_history['close']
            price_changes = close_prices.diff()

            # Positive/negative volume flow
            positive_volume = volume[price_changes > 0].sum()
            negative_volume = volume[price_changes < 0].sum()
            neutral_volume = volume[price_changes == 0].sum()

            total_volume = volume.sum()
            if total_volume > 0:
                positive_flow_ratio = positive_volume / total_volume
                negative_flow_ratio = negative_volume / total_volume
                volume_balance = (positive_volume - negative_volume) / total_volume
            else:
                positive_flow_ratio = 0
                negative_flow_ratio = 0
                volume_balance = 0

            # Volume acceleration
            if len(price_history) >= 10:
                recent_volume = volume.tail(5).sum()
                prev_volume = volume.iloc[-10:-5].sum()
                volume_acceleration = (recent_volume - prev_volume) / prev_volume if prev_volume > 0 else 0
            else:
                volume_acceleration = 0

            # Volume percentile (relative to recent history)
            if len(price_history) >= 20:
                volume_percentile = (volume.iloc[-1] > volume.tail(20)).sum() / 20
            else:
                volume_percentile = 0.5

            # Volume trend
            if len(price_history) >= 20:
                try:
                    volume_trend = volume.tail(20).corr(pd.Series(range(20)))
                    if np.isnan(volume_trend) or volume_trend is None:
                        volume_trend = 0
                except:
                    volume_trend = 0
            else:
                volume_trend = 0

            return {
                'value': current_cum_volume,
                'cumulative_volume': current_cum_volume,
                'positive_flow_ratio': positive_flow_ratio,
                'negative_flow_ratio': negative_flow_ratio,
                'volume_balance': volume_balance,
                'volume_acceleration': volume_acceleration,
                'volume_percentile': volume_percentile,
                'volume_trend': volume_trend,
                'total_session_volume': total_volume,
                'status': 'valid'
            }

        except Exception as e:
            return {'value': None, 'status': f'calculation_error: {str(e)}'}


class CumulativeDollarsIndicator(Indicator):
    """Cumulative Dollar Volume (price * volume) indicator for liquidity analysis."""

    def __init__(self, reset_interval: str = 'daily', price_method: str = 'typical'):
        super().__init__()
        self.reset_interval = reset_interval  # 'daily', 'session', 'never'
        self.price_method = price_method  # 'typical', 'close', 'vwap'
        self.name = f"CumDollars_{reset_interval}_{price_method}"

    def calculate(self, price_history: pd.DataFrame) -> Dict[str, Any]:
        """Calculate cumulative dollar volume indicators."""
        if price_history.empty or 'volume' not in price_history.columns:
            return {'value': None, 'status': 'no_volume_data'}

        try:
            volume = price_history['volume']
            timestamp = price_history.index if hasattr(price_history.index, 'date') else pd.to_datetime(price_history.get('timestamp', range(len(price_history))))

            # Calculate price based on method
            if self.price_method == 'typical':
                price = (price_history['high'] + price_history['low'] + price_history['close']) / 3
            elif self.price_method == 'close':
                price = price_history['close']
            elif self.price_method == 'vwap':
                # Calculate VWAP if possible
                typical_price = (price_history['high'] + price_history['low'] + price_history['close']) / 3
                cum_vol_price = (typical_price * volume).cumsum()
                cum_volume = volume.cumsum()
                price = cum_vol_price / cum_volume
            else:
                price = price_history['close']

            # Calculate dollar volume
            dollar_volume = price * volume

            # Calculate cumulative dollar volume based on reset interval
            if self.reset_interval == 'daily':
                if hasattr(timestamp, 'date'):
                    # Create daily groups and apply cumsum within each group
                    daily_groups = timestamp.normalize()  # Use normalize instead of dt.date
                    dollar_with_groups = pd.DataFrame({'dollar_volume': dollar_volume, 'date': daily_groups})
                    cum_dollar_volume = dollar_with_groups.groupby('date')['dollar_volume'].cumsum()
                else:
                    cum_dollar_volume = dollar_volume.cumsum()
            elif self.reset_interval == 'session':
                if hasattr(timestamp, 'time'):
                    session_times = pd.Series(timestamp).dt.time
                    session_reset = session_times >= pd.Timestamp('09:30:00').time()
                    session_groups = session_reset.cumsum()
                    dollar_with_groups = pd.DataFrame({'dollar_volume': dollar_volume, 'session': session_groups})
                    cum_dollar_volume = dollar_with_groups.groupby('session')['dollar_volume'].cumsum()
                else:
                    cum_dollar_volume = dollar_volume.cumsum()
            else:  # 'never'
                cum_dollar_volume = dollar_volume.cumsum()

            current_cum_dollars = cum_dollar_volume.iloc[-1]

            # Dollar flow analysis
            close_prices = price_history['close']
            price_changes = close_prices.diff()

            # Positive/negative dollar flow
            positive_dollar_flow = dollar_volume[price_changes > 0].sum()
            negative_dollar_flow = dollar_volume[price_changes < 0].sum()
            neutral_dollar_flow = dollar_volume[price_changes == 0].sum()

            total_dollar_volume = dollar_volume.sum()
            if total_dollar_volume > 0:
                positive_dollar_ratio = positive_dollar_flow / total_dollar_volume
                negative_dollar_ratio = negative_dollar_flow / total_dollar_volume
                dollar_balance = (positive_dollar_flow - negative_dollar_flow) / total_dollar_volume
            else:
                positive_dollar_ratio = 0
                negative_dollar_ratio = 0
                dollar_balance = 0

            # Average dollar per share
            avg_dollar_per_share = total_dollar_volume / volume.sum() if volume.sum() > 0 else 0

            # Dollar volume acceleration
            if len(price_history) >= 10:
                recent_dollars = dollar_volume.tail(5).sum()
                prev_dollars = dollar_volume.iloc[-10:-5].sum()
                dollar_acceleration = (recent_dollars - prev_dollars) / prev_dollars if prev_dollars > 0 else 0
            else:
                dollar_acceleration = 0

            # Dollar volume percentile
            if len(price_history) >= 20:
                dollar_percentile = (dollar_volume.iloc[-1] > dollar_volume.tail(20)).sum() / 20
            else:
                dollar_percentile = 0.5

            # Liquidity score (higher dollar volume = higher liquidity)
            if len(price_history) >= 20:
                avg_dollar_volume = dollar_volume.tail(20).mean()
                liquidity_score = min(current_cum_dollars / avg_dollar_volume, 5.0) if avg_dollar_volume > 0 else 0
            else:
                liquidity_score = 1.0

            # Money flow trend
            if len(price_history) >= 20:
                try:
                    dollar_trend = dollar_volume.tail(20).corr(pd.Series(range(20)))
                    if np.isnan(dollar_trend) or dollar_trend is None:
                        dollar_trend = 0
                except:
                    dollar_trend = 0
            else:
                dollar_trend = 0

            return {
                'value': current_cum_dollars,
                'cumulative_dollars': current_cum_dollars,
                'positive_dollar_ratio': positive_dollar_ratio,
                'negative_dollar_ratio': negative_dollar_ratio,
                'dollar_balance': dollar_balance,
                'avg_dollar_per_share': avg_dollar_per_share,
                'dollar_acceleration': dollar_acceleration,
                'dollar_percentile': dollar_percentile,
                'liquidity_score': liquidity_score,
                'dollar_trend': dollar_trend,
                'total_session_dollars': total_dollar_volume,
                'status': 'valid'
            }

        except Exception as e:
            return {'value': None, 'status': f'calculation_error: {str(e)}'}


# Enhanced configuration for comprehensive technical analysis
from ...indicator_config import IndicatorConfig
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

        # Session VWAP indicators
        config.add_indicator('SessionVWAP_us_open_30min', lambda: SessionVWAPIndicator('us_open', 30))
        config.add_indicator('SessionVWAP_us_open_60min', lambda: SessionVWAPIndicator('us_open', 60))
        config.add_indicator('SessionVWAP_us_close_30min', lambda: SessionVWAPIndicator('us_close', 30))
        config.add_indicator('SessionVWAP_us_close_60min', lambda: SessionVWAPIndicator('us_close', 60))
        config.add_indicator('SessionVWAP_london_close_30min', lambda: SessionVWAPIndicator('london_close', 30))
        config.add_indicator('SessionVWAP_london_close_60min', lambda: SessionVWAPIndicator('london_close', 60))

        # Cumulative volume indicators
        config.add_indicator('CumVolume_daily', lambda: CumulativeVolumeIndicator('daily'))
        config.add_indicator('CumVolume_session', lambda: CumulativeVolumeIndicator('session'))
        config.add_indicator('CumVolume_never', lambda: CumulativeVolumeIndicator('never'))

        # Cumulative dollars indicators
        config.add_indicator('CumDollars_daily_typical', lambda: CumulativeDollarsIndicator('daily', 'typical'))
        config.add_indicator('CumDollars_daily_close', lambda: CumulativeDollarsIndicator('daily', 'close'))
        config.add_indicator('CumDollars_session_vwap', lambda: CumulativeDollarsIndicator('session', 'vwap'))

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