"""
Comprehensive tests for Cross-Vendor Data Reconciliation Engine.

Tests all reconciliation methods, quality scoring, variance detection,
and edge cases for combining data from multiple vendors.
"""

import pytest
import numpy as np
from datetime import datetime, timedelta
from unittest.mock import patch

from domains.market_data.services.reconciliation.cross_vendor_reconciler import (
    CrossVendorReconciler,
    ReconciliationConfig,
    ReconciliationMethod,
    VendorBar,
    ReconciledBar
)


class TestVendorBar:
    """Test VendorBar data structure."""

    def test_vendor_bar_creation(self):
        """Test basic vendor bar creation."""
        timestamp = datetime(2024, 1, 1, 9, 30)
        bar = VendorBar(
            symbol='AAPL',
            timestamp=timestamp,
            open=180.00,
            high=181.00,
            low=179.50,
            close=180.50,
            volume=1000000,
            vendor='polygon'
        )

        assert bar.symbol == 'AAPL'
        assert bar.timestamp == timestamp
        assert bar.open == 180.00
        assert bar.high == 181.00
        assert bar.low == 179.50
        assert bar.close == 180.50
        assert bar.volume == 1000000
        assert bar.vendor == 'polygon'
        assert bar.quality_score == 1.0  # Default
        assert bar.metadata is None

    def test_vendor_bar_with_metadata(self):
        """Test vendor bar with metadata."""
        metadata = {'vwap': 180.25, 'trade_count': 1500}
        bar = VendorBar(
            symbol='MSFT',
            timestamp=datetime.now(),
            open=100.0, high=101.0, low=99.0, close=100.5,
            volume=500000,
            vendor='tiingo',
            quality_score=0.9,
            metadata=metadata
        )

        assert bar.quality_score == 0.9
        assert bar.metadata == metadata


class TestReconciledBar:
    """Test ReconciledBar data structure."""

    def test_reconciled_bar_creation(self):
        """Test reconciled bar creation."""
        bar = ReconciledBar(
            symbol='AAPL',
            timestamp=datetime(2024, 1, 1, 9, 30),
            open=180.00, high=181.00, low=179.50, close=180.50,
            volume=1000000,
            quality_score=0.95,
            reconciliation_method='weighted_average',
            source_vendors=['polygon', 'tiingo'],
            vendor_count=2,
            price_variance=0.002,
            volume_variance=0.05,
            metadata={'test': 'data'}
        )

        assert bar.symbol == 'AAPL'
        assert bar.reconciliation_method == 'weighted_average'
        assert bar.source_vendors == ['polygon', 'tiingo']
        assert bar.vendor_count == 2
        assert bar.price_variance == 0.002
        assert bar.volume_variance == 0.05


class TestReconciliationConfig:
    """Test ReconciliationConfig settings."""

    def test_default_config(self):
        """Test default configuration."""
        config = ReconciliationConfig()

        assert config.method == ReconciliationMethod.WEIGHTED_AVERAGE
        assert config.max_price_variance == 0.01
        assert config.max_volume_variance == 0.50
        assert config.min_quality_score == 0.7
        assert config.polygon_weight == 0.6
        assert config.tiingo_weight == 0.4
        assert config.outlier_detection is True

    def test_custom_config(self):
        """Test custom configuration."""
        config = ReconciliationConfig(
            method=ReconciliationMethod.POLYGON_PRIORITY,
            max_price_variance=0.005,
            polygon_weight=0.8,
            tiingo_weight=0.2
        )

        assert config.method == ReconciliationMethod.POLYGON_PRIORITY
        assert config.max_price_variance == 0.005
        assert config.polygon_weight == 0.8
        assert config.tiingo_weight == 0.2


class TestCrossVendorReconciler:
    """Test CrossVendorReconciler functionality."""

    def test_reconciler_initialization(self):
        """Test reconciler initialization."""
        reconciler = CrossVendorReconciler()

        assert isinstance(reconciler.config, ReconciliationConfig)
        assert reconciler.executor is not None

    def test_reconciler_custom_config(self):
        """Test reconciler with custom config."""
        config = ReconciliationConfig(method=ReconciliationMethod.BEST_QUALITY)
        reconciler = CrossVendorReconciler(config)

        assert reconciler.config.method == ReconciliationMethod.BEST_QUALITY

    def test_standardize_polygon_data(self):
        """Test standardizing Polygon data."""
        reconciler = CrossVendorReconciler()

        polygon_data = [
            {
                'timestamp': '2024-01-01T09:30:00Z',
                'open': 180.00,
                'high': 181.00,
                'low': 179.50,
                'close': 180.50,
                'volume': 1000000,
                'vwap': 180.25,
                'trade_count': 1500
            },
            {
                'timestamp': '2024-01-01T09:31:00Z',
                'open': 180.50,
                'high': 181.50,
                'low': 180.00,
                'close': 181.00,
                'volume': 800000
            }
        ]

        result = reconciler._standardize_polygon_data(polygon_data, 'AAPL')

        assert len(result) == 2
        assert all(isinstance(bar, VendorBar) for bar in result)
        assert result[0].symbol == 'AAPL'
        assert result[0].vendor == 'polygon'
        assert result[0].open == 180.00
        assert result[0].metadata['vwap'] == 180.25
        assert result[0].metadata['trade_count'] == 1500
        assert result[1].metadata['vwap'] is None  # Missing in second entry

    def test_standardize_polygon_data_invalid(self):
        """Test standardizing invalid Polygon data."""
        reconciler = CrossVendorReconciler()

        polygon_data = [
            {
                'timestamp': '2024-01-01T09:30:00Z',
                'open': 180.00,
                'high': 181.00,
                'low': 179.50,
                'close': 180.50,
                'volume': 1000000
            },
            {
                'timestamp': '2024-01-01T09:31:00Z',
                'open': 'invalid',  # Invalid data type
                'high': 181.50,
                'low': 180.00,
                'close': 181.00,
                'volume': 800000
            },
            {
                # Missing required fields
                'timestamp': '2024-01-01T09:32:00Z',
                'open': 181.00
            }
        ]

        with patch('market_data.reconciliation.cross_vendor_reconciler.logger') as mock_logger:
            result = reconciler._standardize_polygon_data(polygon_data, 'AAPL')

        assert len(result) == 1  # Only valid entry
        assert mock_logger.warning.call_count == 2

    def test_standardize_tiingo_data(self):
        """Test standardizing Tiingo data."""
        reconciler = CrossVendorReconciler()

        tiingo_data = [
            {
                'timestamp': '2024-01-01T09:30:00Z',
                'open': 180.10,
                'high': 181.10,
                'low': 179.60,
                'close': 180.60,
                'volume': 1050000,
                'quality_score': 0.85
            }
        ]

        result = reconciler._standardize_tiingo_data(tiingo_data, 'AAPL')

        assert len(result) == 1
        assert result[0].symbol == 'AAPL'
        assert result[0].vendor == 'tiingo'
        assert result[0].open == 180.10
        assert result[0].quality_score == 0.85

    def test_create_unified_timeline(self):
        """Test creating unified timeline."""
        reconciler = CrossVendorReconciler()

        time1 = datetime(2024, 1, 1, 9, 30)
        time2 = datetime(2024, 1, 1, 9, 31)
        time3 = datetime(2024, 1, 1, 9, 32)

        polygon_bars = [
            VendorBar('AAPL', time1, 180, 181, 179, 180.5, 1000, 'polygon'),
            VendorBar('AAPL', time3, 181, 182, 180, 181.5, 1200, 'polygon')
        ]

        tiingo_bars = [
            VendorBar('AAPL', time1, 180.1, 181.1, 179.1, 180.6, 1050, 'tiingo'),
            VendorBar('AAPL', time2, 180.6, 181.6, 180.1, 181.1, 900, 'tiingo')
        ]

        timeline = reconciler._create_unified_timeline(polygon_bars, tiingo_bars)

        assert len(timeline) == 3
        assert time1 in timeline
        assert time2 in timeline
        assert time3 in timeline
        assert timeline == sorted(timeline)  # Should be sorted

    def test_find_bar_by_timestamp(self):
        """Test finding bar by timestamp."""
        reconciler = CrossVendorReconciler()

        timestamp = datetime(2024, 1, 1, 9, 30)
        bars = [
            VendorBar('AAPL', timestamp, 180, 181, 179, 180.5, 1000, 'polygon'),
            VendorBar('AAPL', datetime(2024, 1, 1, 9, 31), 180.5, 181.5, 180, 181, 800, 'polygon')
        ]

        found = reconciler._find_bar_by_timestamp(bars, timestamp)
        not_found = reconciler._find_bar_by_timestamp(bars, datetime(2024, 1, 1, 9, 32))

        assert found is not None
        assert found.timestamp == timestamp
        assert not_found is None

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_reconcile_single_bar_no_data(self):
        """Test reconciling with no data."""
        reconciler = CrossVendorReconciler()

        result = await reconciler._reconcile_single_bar(
            datetime.now(), None, None, 'AAPL'
        )

        assert result is None

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_reconcile_single_bar_single_source(self):
        """Test reconciling with single data source."""
        reconciler = CrossVendorReconciler()

        timestamp = datetime(2024, 1, 1, 9, 30)
        polygon_bar = VendorBar('AAPL', timestamp, 180, 181, 179, 180.5, 1000, 'polygon', 0.9)

        result = await reconciler._reconcile_single_bar(
            timestamp, polygon_bar, None, 'AAPL'
        )

        assert result is not None
        assert result.symbol == 'AAPL'
        assert result.timestamp == timestamp
        assert result.open == 180.0
        assert result.close == 180.5
        assert result.quality_score == 0.9
        assert result.reconciliation_method == 'single_source'
        assert result.source_vendors == ['polygon']
        assert result.vendor_count == 1
        assert result.price_variance == 0.0
        assert result.volume_variance == 0.0

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_reconcile_single_bar_low_quality(self):
        """Test reconciling with low quality data."""
        config = ReconciliationConfig(min_quality_score=0.8)
        reconciler = CrossVendorReconciler(config)

        timestamp = datetime(2024, 1, 1, 9, 30)
        low_quality_bar = VendorBar('AAPL', timestamp, 180, 181, 179, 180.5, 1000, 'polygon', 0.5)

        result = await reconciler._reconcile_single_bar(
            timestamp, low_quality_bar, None, 'AAPL'
        )

        assert result is None  # Rejected due to low quality

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_reconcile_multiple_sources_weighted_average(self):
        """Test reconciling multiple sources with weighted average."""
        config = ReconciliationConfig(
            method=ReconciliationMethod.WEIGHTED_AVERAGE,
            polygon_weight=0.6,
            tiingo_weight=0.4
        )
        reconciler = CrossVendorReconciler(config)

        timestamp = datetime(2024, 1, 1, 9, 30)
        bars = [
            VendorBar('AAPL', timestamp, 180.0, 181.0, 179.0, 180.5, 1000, 'polygon', 0.9),
            VendorBar('AAPL', timestamp, 180.2, 181.2, 179.2, 180.7, 1100, 'tiingo', 0.8)
        ]

        result = await reconciler._reconcile_multiple_sources(bars, timestamp, 'AAPL')

        assert result is not None
        assert result.reconciliation_method == 'weighted_average'
        assert result.vendor_count == 2
        assert result.source_vendors == ['polygon', 'tiingo']

        # Check weighted average calculation
        # Expected open: 180.0 * 0.6 + 180.2 * 0.4 = 180.08
        assert abs(result.open - 180.08) < 0.001
        # Expected close: 180.5 * 0.6 + 180.7 * 0.4 = 180.58
        assert abs(result.close - 180.58) < 0.001

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_reconcile_multiple_sources_high_variance(self):
        """Test reconciling with high price variance."""
        config = ReconciliationConfig(max_price_variance=0.001)  # Very strict
        reconciler = CrossVendorReconciler(config)

        timestamp = datetime(2024, 1, 1, 9, 30)
        bars = [
            VendorBar('AAPL', timestamp, 180.0, 181.0, 179.0, 180.0, 1000, 'polygon'),
            VendorBar('AAPL', timestamp, 185.0, 186.0, 184.0, 185.0, 1100, 'tiingo')  # High variance
        ]

        with patch('market_data.reconciliation.cross_vendor_reconciler.logger') as mock_logger:
            result = await reconciler._reconcile_multiple_sources(bars, timestamp, 'AAPL')

        assert result is not None
        assert result.price_variance > 0.001
        assert result.quality_score < 0.9  # Quality reduced due to high variance
        mock_logger.warning.assert_called_once()

    def test_apply_reconciliation_method_polygon_priority(self):
        """Test Polygon priority reconciliation method."""
        config = ReconciliationConfig(method=ReconciliationMethod.POLYGON_PRIORITY)
        reconciler = CrossVendorReconciler(config)

        bars = [
            VendorBar('AAPL', datetime.now(), 180.0, 181.0, 179.0, 180.5, 1000, 'polygon'),
            VendorBar('AAPL', datetime.now(), 180.2, 181.2, 179.2, 180.7, 1100, 'tiingo')
        ]

        result = reconciler._apply_reconciliation_method(bars)

        # Should use Polygon values
        assert result['open'] == 180.0
        assert result['close'] == 180.5
        assert result['volume'] == 1000

    def test_apply_reconciliation_method_tiingo_priority(self):
        """Test Tiingo priority reconciliation method."""
        config = ReconciliationConfig(method=ReconciliationMethod.TIINGO_PRIORITY)
        reconciler = CrossVendorReconciler(config)

        bars = [
            VendorBar('AAPL', datetime.now(), 180.0, 181.0, 179.0, 180.5, 1000, 'polygon'),
            VendorBar('AAPL', datetime.now(), 180.2, 181.2, 179.2, 180.7, 1100, 'tiingo')
        ]

        result = reconciler._apply_reconciliation_method(bars)

        # Should use Tiingo values
        assert result['open'] == 180.2
        assert result['close'] == 180.7
        assert result['volume'] == 1100

    def test_apply_reconciliation_method_average(self):
        """Test average reconciliation method."""
        config = ReconciliationConfig(method=ReconciliationMethod.AVERAGE)
        reconciler = CrossVendorReconciler(config)

        bars = [
            VendorBar('AAPL', datetime.now(), 180.0, 181.0, 179.0, 180.0, 1000, 'polygon'),
            VendorBar('AAPL', datetime.now(), 180.2, 181.2, 179.2, 180.2, 1200, 'tiingo')
        ]

        result = reconciler._apply_reconciliation_method(bars)

        # Should be simple average
        assert result['open'] == 180.1
        assert result['close'] == 180.1
        assert result['volume'] == 1100

    def test_apply_reconciliation_method_best_quality(self):
        """Test best quality reconciliation method."""
        config = ReconciliationConfig(method=ReconciliationMethod.BEST_QUALITY)
        reconciler = CrossVendorReconciler(config)

        bars = [
            VendorBar('AAPL', datetime.now(), 180.0, 181.0, 179.0, 180.5, 1000, 'polygon', 0.8),
            VendorBar('AAPL', datetime.now(), 180.2, 181.2, 179.2, 180.7, 1100, 'tiingo', 0.9)
        ]

        result = reconciler._apply_reconciliation_method(bars)

        # Should use Tiingo values (higher quality)
        assert result['open'] == 180.2
        assert result['close'] == 180.7
        assert result['volume'] == 1100

    def test_apply_reconciliation_method_conservative(self):
        """Test conservative reconciliation method."""
        config = ReconciliationConfig(method=ReconciliationMethod.CONSERVATIVE)
        reconciler = CrossVendorReconciler(config)

        bars = [
            VendorBar('AAPL', datetime.now(), 180.0, 182.0, 178.0, 180.5, 1000, 'polygon'),
            VendorBar('AAPL', datetime.now(), 180.2, 181.0, 179.0, 180.7, 1200, 'tiingo')
        ]

        result = reconciler._apply_reconciliation_method(bars)

        # Conservative: lowest high, highest low, median open/close, lowest volume
        assert result['high'] == 181.0  # Min of 182.0, 181.0
        assert result['low'] == 179.0   # Max of 178.0, 179.0
        assert result['volume'] == 1000  # Min volume

    def test_bar_to_dict(self):
        """Test converting VendorBar to dictionary."""
        reconciler = CrossVendorReconciler()

        bar = VendorBar('AAPL', datetime.now(), 180.0, 181.0, 179.0, 180.5, 1000, 'polygon')
        result = reconciler._bar_to_dict(bar)

        expected = {
            'open': 180.0,
            'high': 181.0,
            'low': 179.0,
            'close': 180.5,
            'volume': 1000
        }

        assert result == expected

    def test_fill_small_gaps_no_gaps(self):
        """Test gap filling with no gaps."""
        reconciler = CrossVendorReconciler()

        base_time = datetime(2024, 1, 1, 9, 30)
        bars = [
            ReconciledBar(
                'AAPL', base_time + timedelta(minutes=i),
                180.0, 181.0, 179.0, 180.5, 1000, 0.9,
                'test', ['polygon'], 1, 0.0, 0.0, {}
            )
            for i in range(3)  # Consecutive bars
        ]

        result = reconciler._fill_small_gaps(bars)

        assert len(result) == 3  # No gaps to fill

    def test_fill_small_gaps_with_gaps(self):
        """Test gap filling with small gaps."""
        config = ReconciliationConfig(gap_tolerance_minutes=2)
        reconciler = CrossVendorReconciler(config)

        base_time = datetime(2024, 1, 1, 9, 30)
        bars = [
            ReconciledBar(
                'AAPL', base_time,
                180.0, 181.0, 179.0, 180.5, 1000, 0.9,
                'test', ['polygon'], 1, 0.0, 0.0, {}
            ),
            ReconciledBar(
                'AAPL', base_time + timedelta(minutes=2),  # 1-minute gap
                181.0, 182.0, 180.0, 181.5, 1100, 0.9,
                'test', ['polygon'], 1, 0.0, 0.0, {}
            )
        ]

        result = reconciler._fill_small_gaps(bars)

        assert len(result) == 3  # Original 2 + 1 interpolated
        assert result[1].reconciliation_method == 'interpolated'
        assert result[1].source_vendors == ['interpolated']

    def test_interpolate_gap(self):
        """Test gap interpolation."""
        reconciler = CrossVendorReconciler()

        before_bar = ReconciledBar(
            'AAPL', datetime(2024, 1, 1, 9, 30),
            180.0, 181.0, 179.0, 180.0, 1000, 0.9,
            'test', ['polygon'], 1, 0.0, 0.0, {}
        )

        after_bar = ReconciledBar(
            'AAPL', datetime(2024, 1, 1, 9, 33),  # 3-minute gap
            183.0, 184.0, 182.0, 183.0, 1300, 0.9,
            'test', ['polygon'], 1, 0.0, 0.0, {}
        )

        result = reconciler._interpolate_gap(before_bar, after_bar)

        assert len(result) == 2  # 2 interpolated bars for 3-minute gap
        assert all(bar.reconciliation_method == 'interpolated' for bar in result)
        assert result[0].timestamp == datetime(2024, 1, 1, 9, 31)
        assert result[1].timestamp == datetime(2024, 1, 1, 9, 32)

    def test_detect_and_flag_anomalies_insufficient_data(self):
        """Test anomaly detection with insufficient data."""
        reconciler = CrossVendorReconciler()

        bars = [
            ReconciledBar(
                'AAPL', datetime.now(),
                180.0, 181.0, 179.0, 180.5, 1000, 0.9,
                'test', ['polygon'], 1, 0.0, 0.0, {}
            )
        ]

        result = reconciler._detect_and_flag_anomalies(bars)

        assert len(result) == 1
        assert result == bars  # Unchanged due to insufficient data

    def test_detect_and_flag_anomalies_with_outliers(self):
        """Test anomaly detection with outliers."""
        reconciler = CrossVendorReconciler()

        base_time = datetime(2024, 1, 1, 9, 30)
        bars = []

        # Create normal bars
        for i in range(8):
            bars.append(ReconciledBar(
                'AAPL', base_time + timedelta(minutes=i),
                180.0 + i * 0.1, 181.0 + i * 0.1, 179.0 + i * 0.1, 180.5 + i * 0.1,
                1000, 0.9, 'test', ['polygon'], 1, 0.0, 0.0, {}
            ))

        # Add outlier bar (large price jump)
        bars.append(ReconciledBar(
            'AAPL', base_time + timedelta(minutes=8),
            200.0, 201.0, 199.0, 200.5,  # 10%+ jump
            10000,  # 10x volume
            0.9, 'test', ['polygon'], 1, 0.05, 0.0, {}  # High variance
        ))

        result = reconciler._detect_and_flag_anomalies(bars)

        # Check that anomalies were flagged
        anomaly_bar = result[-1]
        assert 'anomaly_flags' in anomaly_bar.metadata
        flags = anomaly_bar.metadata['anomaly_flags']
        assert 'extreme_price_move' in flags
        assert 'extreme_volume' in flags
        assert 'high_vendor_variance' in flags
        assert anomaly_bar.quality_score < 0.9  # Quality reduced

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_reconcile_minute_data_integration(self):
        """Test complete minute data reconciliation."""
        reconciler = CrossVendorReconciler()

        polygon_data = [
            {
                'timestamp': '2024-01-01T09:30:00Z',
                'open': 180.00, 'high': 181.00, 'low': 179.00, 'close': 180.50,
                'volume': 1000000, 'vendor': 'polygon'
            },
            {
                'timestamp': '2024-01-01T09:31:00Z',
                'open': 180.50, 'high': 181.50, 'low': 180.00, 'close': 181.00,
                'volume': 800000, 'vendor': 'polygon'
            }
        ]

        tiingo_data = [
            {
                'timestamp': '2024-01-01T09:30:00Z',
                'open': 180.10, 'high': 181.10, 'low': 179.10, 'close': 180.60,
                'volume': 1050000, 'vendor': 'tiingo'
            },
            {
                'timestamp': '2024-01-01T09:32:00Z',  # Different timestamp
                'open': 181.00, 'high': 182.00, 'low': 180.50, 'close': 181.50,
                'volume': 900000, 'vendor': 'tiingo'
            }
        ]

        result = await reconciler.reconcile_minute_data(polygon_data, tiingo_data, 'AAPL')

        assert len(result) >= 3  # At least 3 bars (may include interpolated)

        # Check first bar (should be reconciled from both sources)
        first_bar = next(bar for bar in result if bar.timestamp.minute == 30)
        assert first_bar.vendor_count == 2
        assert 'polygon' in first_bar.source_vendors
        assert 'tiingo' in first_bar.source_vendors

        # Check single-source bars
        single_source_bars = [bar for bar in result if bar.vendor_count == 1]
        assert len(single_source_bars) >= 1

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_reconcile_batch(self):
        """Test batch reconciliation."""
        reconciler = CrossVendorReconciler()

        batch_data = {
            'AAPL': {
                'polygon': [
                    {
                        'timestamp': '2024-01-01T09:30:00Z',
                        'open': 180.00, 'high': 181.00, 'low': 179.00, 'close': 180.50,
                        'volume': 1000000, 'vendor': 'polygon'
                    }
                ],
                'tiingo': [
                    {
                        'timestamp': '2024-01-01T09:30:00Z',
                        'open': 180.10, 'high': 181.10, 'low': 179.10, 'close': 180.60,
                        'volume': 1050000, 'vendor': 'tiingo'
                    }
                ]
            },
            'MSFT': {
                'polygon': [
                    {
                        'timestamp': '2024-01-01T09:30:00Z',
                        'open': 100.00, 'high': 101.00, 'low': 99.00, 'close': 100.50,
                        'volume': 500000, 'vendor': 'polygon'
                    }
                ],
                'tiingo': []  # Empty Tiingo data
            }
        }

        result = await reconciler.reconcile_batch(batch_data)

        assert 'AAPL' in result
        assert 'MSFT' in result
        assert len(result['AAPL']) >= 1
        assert len(result['MSFT']) >= 1  # Should handle single source

    def test_get_reconciliation_stats_empty(self):
        """Test reconciliation statistics with empty data."""
        reconciler = CrossVendorReconciler()

        result = reconciler.get_reconciliation_stats([])

        assert result == {}

    def test_get_reconciliation_stats_with_data(self):
        """Test reconciliation statistics with data."""
        reconciler = CrossVendorReconciler()

        bars = [
            ReconciledBar(
                'AAPL', datetime.now(),
                180.0, 181.0, 179.0, 180.5, 1000, 0.9,
                'weighted_average', ['polygon', 'tiingo'], 2, 0.002, 0.05, {}
            ),
            ReconciledBar(
                'AAPL', datetime.now(),
                180.5, 181.5, 180.0, 181.0, 800, 0.85,
                'single_source', ['polygon'], 1, 0.0, 0.0, {}
            ),
            ReconciledBar(
                'MSFT', datetime.now(),
                100.0, 101.0, 99.0, 100.5, 500, 0.95,
                'weighted_average', ['polygon', 'tiingo'], 2, 0.001, 0.02, {}
            )
        ]

        result = reconciler.get_reconciliation_stats(bars)

        assert result['total_bars'] == 3
        assert 'polygon,tiingo' in result['vendor_combinations']
        assert 'polygon' in result['vendor_combinations']
        assert result['vendor_combinations']['polygon,tiingo'] == 2
        assert result['vendor_combinations']['polygon'] == 1

        assert 'weighted_average' in result['reconciliation_methods']
        assert 'single_source' in result['reconciliation_methods']

        assert 'quality_stats' in result
        assert result['quality_stats']['mean'] == pytest.approx(0.9)

        assert 'variance_stats' in result
        assert 'price' in result['variance_stats']
        assert 'volume' in result['variance_stats']

    def test_close(self):
        """Test resource cleanup."""
        reconciler = CrossVendorReconciler()

        # Should not raise any exceptions
        reconciler.close()


@pytest.mark.integration
class TestReconciliationIntegration:
    """Integration tests for reconciliation workflows."""

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_complete_reconciliation_workflow(self):
        """Test complete reconciliation workflow."""
        config = ReconciliationConfig(
            method=ReconciliationMethod.WEIGHTED_AVERAGE,
            max_price_variance=0.02,
            gap_tolerance_minutes=2
        )
        reconciler = CrossVendorReconciler(config)

        # Simulate realistic market data
        polygon_data = []
        tiingo_data = []

        base_time = datetime(2024, 1, 1, 9, 30)
        base_price = 180.0

        # Generate 30 minutes of data with slight variations
        for i in range(30):
            timestamp = base_time + timedelta(minutes=i)
            price_drift = i * 0.02

            # Polygon data (slightly higher prices)
            polygon_data.append({
                'timestamp': timestamp.isoformat(),
                'open': base_price + price_drift,
                'high': base_price + price_drift + 0.5,
                'low': base_price + price_drift - 0.3,
                'close': base_price + price_drift + 0.2,
                'volume': 1000000 - i * 10000,
                'vendor': 'polygon'
            })

            # Tiingo data (slightly lower prices, some gaps)
            if i % 3 != 0:  # Create some gaps
                tiingo_data.append({
                    'timestamp': timestamp.isoformat(),
                    'open': base_price + price_drift - 0.05,
                    'high': base_price + price_drift + 0.45,
                    'low': base_price + price_drift - 0.35,
                    'close': base_price + price_drift + 0.15,
                    'volume': 1100000 - i * 12000,
                    'vendor': 'tiingo'
                })

        result = await reconciler.reconcile_minute_data(polygon_data, tiingo_data, 'AAPL')

        # Validate results
        assert len(result) >= 30  # Should have data for all 30 minutes (including interpolated)

        # Check that we have both single-source and multi-source bars
        single_source = [bar for bar in result if bar.vendor_count == 1]
        multi_source = [bar for bar in result if bar.vendor_count > 1]

        assert len(single_source) > 0
        assert len(multi_source) > 0

        # Check quality metrics
        avg_quality = np.mean([bar.quality_score for bar in result])
        assert avg_quality > 0.7

        # Check that timestamps are properly ordered
        timestamps = [bar.timestamp for bar in result]
        assert timestamps == sorted(timestamps)

        # Get statistics
        stats = reconciler.get_reconciliation_stats(result)
        assert stats['total_bars'] == len(result)
        assert 'polygon,tiingo' in stats['vendor_combinations']

        reconciler.close()


if __name__ == '__main__':
    pytest.main([__file__, '-v'])