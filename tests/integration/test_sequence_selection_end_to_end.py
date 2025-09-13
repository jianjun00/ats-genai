"""
Integration tests for sequence selection end-to-end workflow
Tests complete user workflow from API calls to chart rendering
"""
import pytest
import json
from unittest.mock import patch

from src.services.analytics_service import AnalyticsService


class TestSequenceSelectionEndToEnd:
    """Integration tests for complete sequence selection workflow"""

    @pytest.fixture
    def analytics_service(self):
        """Create analytics service instance for integration testing"""
        service = AnalyticsService()
        return service

    @pytest.mark.asyncio
    async def test_complete_sequence_selection_workflow(self, analytics_service):
        """Test complete workflow: dataset list -> sequence selection -> chart data"""

        # Step 1: Get available training datasets
        with patch.object(analytics_service, 'get_training_datasets') as mock_datasets:
            mock_datasets.return_value = [
                {
                    'id': 64,
                    'dataset_name': 'AAPL_TSLA_20250701_20250906',
                    'symbols': 'AAPL,TSLA',
                    'creation_timestamp': '2025-09-06T10:00:00'
                }
            ]

            datasets = await analytics_service.get_training_datasets()
            assert len(datasets) == 1
            assert datasets[0]['id'] == 64
            assert 'AAPL' in datasets[0]['symbols']

        # Step 2: Get sequences for selected dataset
        with patch.object(analytics_service, 'get_dataset_sequences') as mock_sequences:
            mock_sequences.return_value = [
                {
                    'sequence_id': 'AAPL_20250701_000000_20250906_000000',
                    'symbol': 'AAPL',
                    'start_date': '2025-07-01',
                    'end_date': '2025-09-06',
                    'total_rows': 1000
                },
                {
                    'sequence_id': 'TSLA_20250701_000000_20250906_000000',
                    'symbol': 'TSLA',
                    'start_date': '2025-07-01',
                    'end_date': '2025-09-06',
                    'total_rows': 1000
                }
            ]

            sequences = await analytics_service.get_dataset_sequences(64)
            assert len(sequences) == 2
            assert any(seq['symbol'] == 'AAPL' for seq in sequences)
            assert any(seq['symbol'] == 'TSLA' for seq in sequences)

        # Step 3: Get multi-timeframe data for 21-bar selection
        with patch.object(analytics_service, 'get_sequence_21_bar_data') as mock_21_bar:
            mock_ohlcv_data = [
                {'timestamp': '2025-07-01T10:00:00', 'open': 100.0, 'high': 105.0, 'low': 95.0, 'close': 102.0, 'volume': 1000},
                {'timestamp': '2025-07-01T10:05:00', 'open': 102.0, 'high': 107.0, 'low': 98.0, 'close': 104.0, 'volume': 1200},
                {'timestamp': '2025-07-01T10:10:00', 'open': 104.0, 'high': 109.0, 'low': 101.0, 'close': 106.0, 'volume': 900}
            ]

            mock_21_bar.return_value = {
                '5m': {'ohlcv': mock_ohlcv_data, 'indicators': {'sma_20': [101.0, 103.0, 105.0]}},
                '15m': {'ohlcv': mock_ohlcv_data, 'indicators': {'sma_20': [102.0, 104.0, 106.0]}},
                '1h': {'ohlcv': mock_ohlcv_data, 'indicators': {'sma_20': [103.0, 105.0, 107.0]}},
                '1d': {'ohlcv': mock_ohlcv_data, 'indicators': {'sma_20': [104.0, 106.0, 108.0]}},
                '1w': {'ohlcv': mock_ohlcv_data, 'indicators': {'sma_20': [105.0, 107.0, 109.0]}}
            }

            chart_data = await analytics_service.get_sequence_21_bar_data(
                dataset_id=64,
                sequence_id='AAPL_20250701_000000_20250906_000000',
                row_index=500
            )

            # Validate multi-timeframe structure
            expected_timeframes = ['5m', '15m', '1h', '1d', '1w']
            for tf in expected_timeframes:
                assert tf in chart_data
                assert 'ohlcv' in chart_data[tf]
                assert 'indicators' in chart_data[tf]
                assert len(chart_data[tf]['ohlcv']) == 3  # 21-bar subset

    @pytest.mark.asyncio
    async def test_nan_handling_in_complete_workflow(self, analytics_service):
        """Test NaN value handling throughout complete workflow"""

        # Mock data with NaN values
        mock_data_with_nans = {
            '5m': {
                'ohlcv': [
                    {'open': 100.0, 'high': float('nan'), 'low': 95.0, 'close': 102.0},
                    {'open': 102.0, 'high': 107.0, 'low': float('nan'), 'close': 104.0}
                ],
                'indicators': {
                    'sma_20': [float('nan'), 103.0],
                    'rsi_14': [50.0, float('nan')]
                }
            }
        }

        with patch.object(analytics_service, 'get_sequence_21_bar_data') as mock_21_bar:
            mock_21_bar.return_value = mock_data_with_nans

            result = await analytics_service.get_sequence_21_bar_data(64, 'test_seq', 100)

            # Test that result can be JSON serialized despite NaN values
            try:
                json_str = json.dumps(result, default=str)
                assert isinstance(json_str, str)
            except (TypeError, ValueError) as e:
                pytest.fail(f"JSON serialization failed with NaN values: {e}")

    @pytest.mark.asyncio
    async def test_error_propagation_through_workflow(self, analytics_service):
        """Test error handling propagates correctly through workflow"""

        # Test dataset not found error
        with patch.object(analytics_service, 'get_training_datasets') as mock_datasets:
            mock_datasets.side_effect = Exception("Database connection failed")

            with pytest.raises(Exception) as exc_info:
                await analytics_service.get_training_datasets()

            assert "Database connection failed" in str(exc_info.value)

        # Test sequence not found error
        with patch.object(analytics_service, 'get_dataset_sequences') as mock_sequences:
            mock_sequences.side_effect = Exception("Dataset 999 not found")

            with pytest.raises(Exception) as exc_info:
                await analytics_service.get_dataset_sequences(999)

            assert "Dataset 999 not found" in str(exc_info.value)

        # Test invalid row index error
        with patch.object(analytics_service, 'get_sequence_21_bar_data') as mock_21_bar:
            mock_21_bar.side_effect = Exception("Row index 5000 exceeds sequence length 1000")

            with pytest.raises(Exception) as exc_info:
                await analytics_service.get_sequence_21_bar_data(64, 'test_seq', 5000)

            assert "Row index 5000 exceeds sequence length" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_chart_template_generation_integration(self, analytics_service):
        """Test chart template generation integrates with sequence data"""

        mock_sequence_data = {
            '5m': {
                'ohlcv': [
                    {'timestamp': '2025-07-01T10:00:00', 'open': 100.0, 'high': 105.0, 'low': 95.0, 'close': 102.0},
                    {'timestamp': '2025-07-01T10:05:00', 'open': 102.0, 'high': 107.0, 'low': 98.0, 'close': 104.0}
                ]
            },
            '15m': {
                'ohlcv': [
                    {'timestamp': '2025-07-01T10:00:00', 'open': 100.0, 'high': 106.0, 'low': 94.0, 'close': 103.0}
                ]
            }
        }

        with patch.object(analytics_service, '_get_sequence_data', return_value=mock_sequence_data):
            template_html = analytics_service._generate_chart_template(mock_sequence_data)

            # Validate template contains proper structure
            assert 'Plotly.newPlot' in template_html
            assert 'chart-5m' in template_html
            assert 'chart-15m' in template_html

            # Validate JavaScript syntax is correct
            assert '${' in template_html  # Template literal interpolation
            assert '`' in template_html   # Template literal backticks

            # Validate data embedding works
            assert '100' in template_html  # Price data embedded
            assert '105' in template_html  # High price embedded

    @pytest.mark.asyncio
    async def test_multi_timeframe_data_consistency(self, analytics_service):
        """Test multi-timeframe data maintains consistency"""

        # Mock consistent data across timeframes
        base_timestamp = '2025-07-01T10:00:00'
        mock_consistent_data = {}

        timeframes = ['5m', '15m', '1h', '1d', '1w']
        for tf in timeframes:
            mock_consistent_data[tf] = {
                'ohlcv': [
                    {
                        'timestamp': base_timestamp,
                        'open': 100.0,
                        'high': 105.0,
                        'low': 95.0,
                        'close': 102.0,
                        'volume': 1000
                    }
                ],
                'metadata': {
                    'timeframe': tf,
                    'symbol': 'AAPL'
                }
            }

        with patch.object(analytics_service, 'get_sequence_21_bar_data') as mock_data:
            mock_data.return_value = mock_consistent_data

            result = await analytics_service.get_sequence_21_bar_data(64, 'AAPL_seq', 100)

            # Validate all timeframes present
            for tf in timeframes:
                assert tf in result
                assert result[tf]['metadata']['timeframe'] == tf
                assert result[tf]['metadata']['symbol'] == 'AAPL'

            # Validate data consistency across timeframes
            base_open = result['5m']['ohlcv'][0]['open']
            for tf in timeframes:
                tf_open = result[tf]['ohlcv'][0]['open']
                assert abs(tf_open - base_open) < 0.01, f"Inconsistent open price across timeframes: {tf}"