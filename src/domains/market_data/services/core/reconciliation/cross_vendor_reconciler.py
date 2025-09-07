"""
Cross-Vendor Data Reconciliation Engine

Unified system for reconciling 1-minute OHLCV data from multiple vendors
(Polygon, Tiingo) to create high-quality, error-reduced datasets.
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Any
from dataclasses import dataclass
from enum import Enum
import logging
from concurrent.futures import ThreadPoolExecutor

logger = logging.getLogger(__name__)


class ReconciliationMethod(Enum):
    """Methods for reconciling conflicting data."""
    POLYGON_PRIORITY = "polygon_priority"
    TIINGO_PRIORITY = "tiingo_priority"
    AVERAGE = "average"
    WEIGHTED_AVERAGE = "weighted_average"
    BEST_QUALITY = "best_quality"
    CONSERVATIVE = "conservative"  # Use most conservative values


@dataclass
class VendorBar:
    """Standardized bar structure across vendors."""
    symbol: str
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    volume: int
    vendor: str
    quality_score: float = 1.0
    metadata: Optional[Dict[str, Any]] = None


@dataclass
class ReconciledBar:
    """Final reconciled bar with metadata about sources."""
    symbol: str
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    volume: int
    quality_score: float
    reconciliation_method: str
    source_vendors: List[str]
    vendor_count: int
    price_variance: float  # Variance across vendors
    volume_variance: float  # Volume variance across vendors
    metadata: Dict[str, Any]


@dataclass
class ReconciliationConfig:
    """Configuration for data reconciliation."""
    method: ReconciliationMethod = ReconciliationMethod.WEIGHTED_AVERAGE
    max_price_variance: float = 0.01  # 1% max variance allowed
    max_volume_variance: float = 0.50  # 50% max volume variance
    min_quality_score: float = 0.7
    require_multiple_sources: bool = True
    polygon_weight: float = 0.6  # Higher trust in Polygon
    tiingo_weight: float = 0.4
    outlier_detection: bool = True
    gap_tolerance_minutes: int = 2


class CrossVendorReconciler:
    """
    Cross-vendor data reconciliation engine.

    Combines data from multiple vendors to create unified, high-quality
    1-minute datasets with reduced errors and improved completeness.
    """

    def __init__(self, config: ReconciliationConfig = None):
        self.config = config or ReconciliationConfig()
        self.executor = ThreadPoolExecutor(max_workers=4)

    async def reconcile_minute_data(
        self,
        polygon_data: List[Dict[str, Any]],
        tiingo_data: List[Dict[str, Any]],
        symbol: str
    ) -> List[ReconciledBar]:
        """
        Reconcile minute data from multiple vendors.

        Args:
            polygon_data: List of Polygon minute bars
            tiingo_data: List of Tiingo minute bars
            symbol: Stock symbol

        Returns:
            List of reconciled bars
        """
        logger.info(f"Starting reconciliation for {symbol}: "
                   f"{len(polygon_data)} Polygon bars, {len(tiingo_data)} Tiingo bars")

        # Convert to standardized format
        polygon_bars = self._standardize_polygon_data(polygon_data, symbol)
        tiingo_bars = self._standardize_tiingo_data(tiingo_data, symbol)

        # Create unified timeline
        unified_timeline = self._create_unified_timeline(polygon_bars, tiingo_bars)

        # Reconcile data for each timestamp
        reconciled_bars = []
        for timestamp in unified_timeline:
            polygon_bar = self._find_bar_by_timestamp(polygon_bars, timestamp)
            tiingo_bar = self._find_bar_by_timestamp(tiingo_bars, timestamp)

            reconciled = await self._reconcile_single_bar(
                timestamp, polygon_bar, tiingo_bar, symbol
            )

            if reconciled:
                reconciled_bars.append(reconciled)

        # Post-process for gaps and anomalies
        reconciled_bars = self._fill_small_gaps(reconciled_bars)
        reconciled_bars = self._detect_and_flag_anomalies(reconciled_bars)

        logger.info(f"Reconciliation complete for {symbol}: "
                   f"{len(reconciled_bars)} final bars")

        return reconciled_bars

    def _standardize_polygon_data(
        self,
        data: List[Dict[str, Any]],
        symbol: str
    ) -> List[VendorBar]:
        """Convert Polygon data to standardized format."""
        bars = []
        for item in data:
            try:
                # Normalize timestamp to UTC timezone-aware
                timestamp = pd.to_datetime(item['timestamp'])
                if timestamp.tz is None:
                    timestamp = timestamp.tz_localize('UTC')
                else:
                    timestamp = timestamp.tz_convert('UTC')

                bar = VendorBar(
                    symbol=symbol,
                    timestamp=timestamp,
                    open=float(item['open']),
                    high=float(item['high']),
                    low=float(item['low']),
                    close=float(item['close']),
                    volume=int(item['volume']),
                    vendor='polygon',
                    quality_score=item.get('quality_score', 0.9),
                    metadata={
                        'vwap': item.get('vwap'),
                        'trade_count': item.get('trade_count')
                    }
                )
                bars.append(bar)
            except (KeyError, ValueError, TypeError) as e:
                logger.warning(f"Error standardizing Polygon bar: {e}")
                continue

        return bars

    def _standardize_tiingo_data(
        self,
        data: List[Dict[str, Any]],
        symbol: str
    ) -> List[VendorBar]:
        """Convert Tiingo data to standardized format."""
        bars = []
        for item in data:
            try:
                # Normalize timestamp to UTC timezone-aware
                timestamp = pd.to_datetime(item['timestamp'])
                if timestamp.tz is None:
                    timestamp = timestamp.tz_localize('UTC')
                else:
                    timestamp = timestamp.tz_convert('UTC')

                bar = VendorBar(
                    symbol=symbol,
                    timestamp=timestamp,
                    open=float(item['open']),
                    high=float(item['high']),
                    low=float(item['low']),
                    close=float(item['close']),
                    volume=int(item['volume']),
                    vendor='tiingo',
                    quality_score=item.get('quality_score', 0.8),  # Slightly lower default
                    metadata={}
                )
                bars.append(bar)
            except (KeyError, ValueError, TypeError) as e:
                logger.warning(f"Error standardizing Tiingo bar: {e}")
                continue

        return bars

    def _create_unified_timeline(
        self,
        polygon_bars: List[VendorBar],
        tiingo_bars: List[VendorBar]
    ) -> List[datetime]:
        """Create unified timeline covering all vendor data."""
        all_timestamps = set()

        for bar in polygon_bars:
            all_timestamps.add(bar.timestamp)

        for bar in tiingo_bars:
            all_timestamps.add(bar.timestamp)

        return sorted(all_timestamps)

    def _find_bar_by_timestamp(
        self,
        bars: List[VendorBar],
        timestamp: datetime
    ) -> Optional[VendorBar]:
        """Find bar with exact timestamp match."""
        for bar in bars:
            if bar.timestamp == timestamp:
                return bar
        return None

    async def _reconcile_single_bar(
        self,
        timestamp: datetime,
        polygon_bar: Optional[VendorBar],
        tiingo_bar: Optional[VendorBar],
        symbol: str
    ) -> Optional[ReconciledBar]:
        """Reconcile data for a single timestamp."""

        available_bars = [bar for bar in [polygon_bar, tiingo_bar] if bar is not None]

        if not available_bars:
            return None

        # If only one source, use it directly (with quality check)
        if len(available_bars) == 1:
            bar = available_bars[0]
            if bar.quality_score < self.config.min_quality_score:
                return None

            return ReconciledBar(
                symbol=symbol,
                timestamp=timestamp,
                open=bar.open,
                high=bar.high,
                low=bar.low,
                close=bar.close,
                volume=bar.volume,
                quality_score=bar.quality_score,
                reconciliation_method="single_source",
                source_vendors=[bar.vendor],
                vendor_count=1,
                price_variance=0.0,
                volume_variance=0.0,
                metadata={"single_vendor": bar.vendor}
            )

        # Multiple sources - reconcile based on method
        return await self._reconcile_multiple_sources(available_bars, timestamp, symbol)

    async def _reconcile_multiple_sources(
        self,
        bars: List[VendorBar],
        timestamp: datetime,
        symbol: str
    ) -> Optional[ReconciledBar]:
        """Reconcile data from multiple sources."""

        # Calculate price and volume variance
        prices = [bar.close for bar in bars]
        volumes = [bar.volume for bar in bars]

        price_variance = np.std(prices) / np.mean(prices) if len(prices) > 1 else 0.0
        volume_variance = np.std(volumes) / np.mean(volumes) if len(volumes) > 1 and np.mean(volumes) > 0 else 0.0

        # Check if variance is within acceptable limits
        if (price_variance > self.config.max_price_variance or
            volume_variance > self.config.max_volume_variance):
            logger.warning(f"High variance detected for {symbol} at {timestamp}: "
                          f"price_var={price_variance:.4f}, volume_var={volume_variance:.4f}")

        # Apply reconciliation method
        reconciled_values = self._apply_reconciliation_method(bars)

        # Calculate overall quality score
        quality_scores = [bar.quality_score for bar in bars]
        overall_quality = np.mean(quality_scores)

        # Adjust quality based on variance
        if price_variance > self.config.max_price_variance * 0.5:
            overall_quality *= 0.9  # Reduce quality for high variance

        return ReconciledBar(
            symbol=symbol,
            timestamp=timestamp,
            open=reconciled_values['open'],
            high=reconciled_values['high'],
            low=reconciled_values['low'],
            close=reconciled_values['close'],
            volume=reconciled_values['volume'],
            quality_score=overall_quality,
            reconciliation_method=self.config.method.value,
            source_vendors=[bar.vendor for bar in bars],
            vendor_count=len(bars),
            price_variance=price_variance,
            volume_variance=volume_variance,
            metadata={
                "individual_bars": [
                    {
                        "vendor": bar.vendor,
                        "open": bar.open,
                        "high": bar.high,
                        "low": bar.low,
                        "close": bar.close,
                        "volume": bar.volume,
                        "quality": bar.quality_score
                    }
                    for bar in bars
                ]
            }
        )

    def _apply_reconciliation_method(self, bars: List[VendorBar]) -> Dict[str, float]:
        """Apply the configured reconciliation method."""

        method = self.config.method

        if method == ReconciliationMethod.POLYGON_PRIORITY:
            polygon_bar = next((bar for bar in bars if bar.vendor == 'polygon'), None)
            if polygon_bar:
                return self._bar_to_dict(polygon_bar)
            else:
                return self._bar_to_dict(bars[0])  # Fallback

        elif method == ReconciliationMethod.TIINGO_PRIORITY:
            tiingo_bar = next((bar for bar in bars if bar.vendor == 'tiingo'), None)
            if tiingo_bar:
                return self._bar_to_dict(tiingo_bar)
            else:
                return self._bar_to_dict(bars[0])  # Fallback

        elif method == ReconciliationMethod.AVERAGE:
            return {
                'open': np.mean([bar.open for bar in bars]),
                'high': np.mean([bar.high for bar in bars]),
                'low': np.mean([bar.low for bar in bars]),
                'close': np.mean([bar.close for bar in bars]),
                'volume': int(np.mean([bar.volume for bar in bars]))
            }

        elif method == ReconciliationMethod.WEIGHTED_AVERAGE:
            weights = []
            for bar in bars:
                if bar.vendor == 'polygon':
                    weights.append(self.config.polygon_weight)
                elif bar.vendor == 'tiingo':
                    weights.append(self.config.tiingo_weight)
                else:
                    weights.append(0.5)  # Default weight

            weights = np.array(weights)
            weights = weights / np.sum(weights)  # Normalize

            return {
                'open': np.average([bar.open for bar in bars], weights=weights),
                'high': np.average([bar.high for bar in bars], weights=weights),
                'low': np.average([bar.low for bar in bars], weights=weights),
                'close': np.average([bar.close for bar in bars], weights=weights),
                'volume': int(np.average([bar.volume for bar in bars], weights=weights))
            }

        elif method == ReconciliationMethod.BEST_QUALITY:
            best_bar = max(bars, key=lambda x: x.quality_score)
            return self._bar_to_dict(best_bar)

        elif method == ReconciliationMethod.CONSERVATIVE:
            # Use most conservative values (lowest high, highest low, etc.)
            return {
                'open': np.median([bar.open for bar in bars]),  # Median as conservative
                'high': np.min([bar.high for bar in bars]),     # Lowest high
                'low': np.max([bar.low for bar in bars]),       # Highest low
                'close': np.median([bar.close for bar in bars]),
                'volume': int(np.min([bar.volume for bar in bars]))  # Lowest volume
            }

        else:
            # Default to weighted average
            return self._apply_reconciliation_method(bars)

    def _bar_to_dict(self, bar: VendorBar) -> Dict[str, float]:
        """Convert VendorBar to dictionary format."""
        return {
            'open': bar.open,
            'high': bar.high,
            'low': bar.low,
            'close': bar.close,
            'volume': bar.volume
        }

    def _fill_small_gaps(self, bars: List[ReconciledBar]) -> List[ReconciledBar]:
        """Fill small gaps in the data using interpolation."""
        if len(bars) < 2:
            return bars

        # Sort by timestamp
        bars = sorted(bars, key=lambda x: x.timestamp)
        filled_bars = []

        for i in range(len(bars)):
            filled_bars.append(bars[i])

            # Check gap to next bar
            if i < len(bars) - 1:
                current_time = bars[i].timestamp
                next_time = bars[i + 1].timestamp
                gap_minutes = (next_time - current_time).total_seconds() / 60

                if 1 < gap_minutes <= self.config.gap_tolerance_minutes:
                    # Fill small gaps with interpolated data
                    interpolated = self._interpolate_gap(bars[i], bars[i + 1])
                    filled_bars.extend(interpolated)

        return filled_bars

    def _interpolate_gap(
        self,
        before_bar: ReconciledBar,
        after_bar: ReconciledBar
    ) -> List[ReconciledBar]:
        """Interpolate data to fill gap between two bars."""
        interpolated = []

        time_diff = after_bar.timestamp - before_bar.timestamp
        gap_minutes = int(time_diff.total_seconds() / 60) - 1

        if gap_minutes <= 0:
            return interpolated

        for i in range(1, gap_minutes + 1):
            interp_time = before_bar.timestamp + timedelta(minutes=i)

            # Linear interpolation for prices
            ratio = i / (gap_minutes + 1)

            interpolated_bar = ReconciledBar(
                symbol=before_bar.symbol,
                timestamp=interp_time,
                open=before_bar.close,  # Use previous close as open
                high=before_bar.close + ratio * (after_bar.close - before_bar.close),
                low=before_bar.close + ratio * (after_bar.close - before_bar.close),
                close=before_bar.close + ratio * (after_bar.close - before_bar.close),
                volume=int((before_bar.volume + after_bar.volume) / 2),  # Average volume
                quality_score=min(before_bar.quality_score, after_bar.quality_score) * 0.8,  # Lower quality
                reconciliation_method="interpolated",
                source_vendors=["interpolated"],
                vendor_count=0,
                price_variance=0.0,
                volume_variance=0.0,
                metadata={"interpolated_between": [before_bar.timestamp, after_bar.timestamp]}
            )

            interpolated.append(interpolated_bar)

        return interpolated

    def _detect_and_flag_anomalies(self, bars: List[ReconciledBar]) -> List[ReconciledBar]:
        """Detect and flag potential anomalies in the data."""
        if len(bars) < 5:  # Need minimum data for anomaly detection
            return bars

        df = pd.DataFrame([
            {
                'timestamp': bar.timestamp,
                'open': bar.open,
                'high': bar.high,
                'low': bar.low,
                'close': bar.close,
                'volume': bar.volume,
                'quality_score': bar.quality_score
            }
            for bar in bars
        ])

        # Calculate rolling statistics
        df['returns'] = df['close'].pct_change()
        df['volume_ratio'] = df['volume'] / df['volume'].rolling(window=20, min_periods=1).mean()

        # Flag anomalies
        anomaly_flags = []

        for i, bar in enumerate(bars):
            flags = []

            # Extreme price movements
            if i > 0 and abs(df.loc[i, 'returns']) > 0.1:  # 10% move
                flags.append("extreme_price_move")

            # Volume anomalies
            if df.loc[i, 'volume_ratio'] > 10:  # 10x average volume
                flags.append("extreme_volume")

            # High variance between vendors
            if bar.price_variance > self.config.max_price_variance:
                flags.append("high_vendor_variance")

            # Update metadata with anomaly flags
            if flags:
                bar.metadata['anomaly_flags'] = flags
                bar.quality_score *= 0.9  # Reduce quality for anomalies

            anomaly_flags.append(flags)

        return bars

    async def reconcile_batch(
        self,
        batch_data: Dict[str, Dict[str, List[Dict[str, Any]]]]
    ) -> Dict[str, List[ReconciledBar]]:
        """
        Reconcile data for a batch of symbols.

        Args:
            batch_data: {symbol: {'polygon': [...], 'tiingo': [...]}}

        Returns:
            Dictionary mapping symbols to reconciled bars
        """
        results = {}

        for symbol, vendor_data in batch_data.items():
            polygon_data = vendor_data.get('polygon', [])
            tiingo_data = vendor_data.get('tiingo', [])

            try:
                reconciled = await self.reconcile_minute_data(
                    polygon_data, tiingo_data, symbol
                )
                results[symbol] = reconciled
            except Exception as e:
                logger.error(f"Error reconciling {symbol}: {e}")
                results[symbol] = []

        return results

    def get_reconciliation_stats(
        self,
        reconciled_bars: List[ReconciledBar]
    ) -> Dict[str, Any]:
        """Generate statistics about the reconciliation process."""
        if not reconciled_bars:
            return {}

        vendor_counts = {}
        method_counts = {}
        quality_scores = []
        variances = {'price': [], 'volume': []}

        for bar in reconciled_bars:
            # Vendor statistics
            vendor_key = ','.join(sorted(bar.source_vendors))
            vendor_counts[vendor_key] = vendor_counts.get(vendor_key, 0) + 1

            # Method statistics
            method_counts[bar.reconciliation_method] = method_counts.get(bar.reconciliation_method, 0) + 1

            # Quality statistics
            quality_scores.append(bar.quality_score)
            variances['price'].append(bar.price_variance)
            variances['volume'].append(bar.volume_variance)

        return {
            'total_bars': len(reconciled_bars),
            'vendor_combinations': vendor_counts,
            'reconciliation_methods': method_counts,
            'quality_stats': {
                'mean': np.mean(quality_scores),
                'min': np.min(quality_scores),
                'max': np.max(quality_scores),
                'std': np.std(quality_scores)
            },
            'variance_stats': {
                'price': {
                    'mean': np.mean(variances['price']),
                    'max': np.max(variances['price']),
                    'above_threshold': sum(1 for v in variances['price'] if v > self.config.max_price_variance)
                },
                'volume': {
                    'mean': np.mean(variances['volume']),
                    'max': np.max(variances['volume']),
                    'above_threshold': sum(1 for v in variances['volume'] if v > self.config.max_volume_variance)
                }
            }
        }

    def close(self):
        """Clean up resources."""
        self.executor.shutdown(wait=True)