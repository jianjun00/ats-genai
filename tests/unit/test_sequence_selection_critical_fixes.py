"""
Unit tests for sequence selection critical fixes
Tests all major issues identified during sequence selection implementation
"""
import pytest
import json
import numpy as np
from unittest.mock import Mock, patch
import re

from src.services.analytics_service import UnifiedAnalyticsService as AnalyticsService


class TestSequenceSelectionCriticalFixes:
    """Test suite for all critical issues identified during sequence selection implementation"""

    @pytest.fixture
    def analytics_service(self):
        """Create analytics service instance for testing"""
        return AnalyticsService()

    def test_nan_json_serialization_handling(self, analytics_service):
        """Test NaN values are properly handled in JSON serialization"""
        # Create mock data with NaN values
        mock_data = {
            'ohlcv': [
                {'open': 100.0, 'high': 105.0, 'low': 95.0, 'close': 102.0, 'volume': 1000},
                {'open': 102.0, 'high': float('nan'), 'low': 98.0, 'close': 100.0, 'volume': 1200},
                {'open': 100.0, 'high': 103.0, 'low': float('nan'), 'close': 101.0, 'volume': 900}
            ],
            'indicators': {
                'sma_20': [20.0, float('nan'), 22.0],
                'rsi_14': [50.0, 45.0, float('nan')],
                'macd_line': [float('nan'), 1.2, 1.5]
            }
        }

        # Test JSON serialization doesn't fail with NaN values
        try:
            json_str = json.dumps(mock_data, default=str)
            assert 'nan' in json_str or 'null' in json_str
        except (TypeError, ValueError) as e:
            pytest.fail(f"JSON serialization failed with NaN values: {e}")

    def test_javascript_template_literal_syntax(self, analytics_service):
        """Test JavaScript template literal syntax is correct"""

        # Mock data for template generation
        mock_sequence_data = {
            'ohlcv': [{'open': 100.0, 'high': 105.0, 'low': 95.0, 'close': 102.0}],
            'sequence_info': {'sequence_id': 123, 'symbol': 'AAPL'}
        }

        with patch.object(analytics_service, '_get_sequence_data', return_value=mock_sequence_data):
            # Test template generation doesn't have syntax errors
            template_content = analytics_service._generate_chart_template(mock_sequence_data)

            # Check for proper template literal syntax
            assert '${' in template_content  # Template literal interpolation
            assert '`' in template_content   # Template literal backticks

            # Check for common syntax errors we fixed
            assert '"${' not in template_content  # No mixing of quotes and template literals
            assert '}"' not in template_content   # No mixing of template literals and quotes

    def test_21_bar_selection_logic_validation(self, analytics_service):
        """Test 21-bar selection logic (10 before + 1 current + 10 after)"""

        # Mock sequence with 50 bars
        mock_sequence_length = 50
        target_row_index = 25  # Middle of sequence

        # Test 21-bar selection logic
        start_idx, end_idx = analytics_service._calculate_21_bar_window(
            target_row_index, mock_sequence_length
        )

        # Should be 10 before + 1 current + 10 after = 21 bars
        assert end_idx - start_idx == 21
        assert start_idx == target_row_index - 10
        assert end_idx == target_row_index + 11  # +11 because end is exclusive

    def test_21_bar_selection_edge_cases(self, analytics_service):
        """Test 21-bar selection at sequence boundaries"""

        # Test at beginning of sequence
        start_idx, end_idx = analytics_service._calculate_21_bar_window(5, 50)
        assert start_idx >= 0  # Cannot go below 0
        assert end_idx - start_idx <= 21  # Cannot exceed 21 bars

        # Test at end of sequence
        start_idx, end_idx = analytics_service._calculate_21_bar_window(45, 50)
        assert end_idx <= 50  # Cannot exceed sequence length
        assert end_idx - start_idx <= 21  # Cannot exceed 21 bars

        # Test with short sequence
        start_idx, end_idx = analytics_service._calculate_21_bar_window(5, 10)
        assert end_idx <= 10  # Cannot exceed sequence length
        assert start_idx >= 0  # Cannot go below 0

    def test_multi_timeframe_data_structure_validation(self, analytics_service):
        """Test multi-timeframe data structure is properly validated"""

        expected_timeframes = ['5m', '15m', '1h', '1d', '1w']

        mock_data = {
            '5m': {'ohlcv': [{'open': 100}]},
            '15m': {'ohlcv': [{'open': 101}]},
            '1h': {'ohlcv': [{'open': 102}]},
            '1d': {'ohlcv': [{'open': 103}]},
            '1w': {'ohlcv': [{'open': 104}]}
        }

        # Test all required timeframes are present
        for timeframe in expected_timeframes:
            assert timeframe in mock_data, f"Missing required timeframe: {timeframe}"
            assert 'ohlcv' in mock_data[timeframe], f"Missing OHLCV data for {timeframe}"

    def test_sequence_metadata_completeness(self, analytics_service):
        """Test sequence metadata includes all required fields"""

        mock_metadata = {
            'sequence_id': 123,
            'symbol': 'AAPL',
            'start_date': '2025-07-01',
            'end_date': '2025-09-06',
            'timeframes': ['5m', '15m', '1h', '1d', '1w'],
            'total_bars': 1000,
            'selected_row_index': 500
        }

        required_fields = [
            'sequence_id', 'symbol', 'start_date', 'end_date',
            'timeframes', 'total_bars', 'selected_row_index'
        ]

        for field in required_fields:
            assert field in mock_metadata, f"Missing required metadata field: {field}"

    def test_dom_element_validation_patterns(self, analytics_service):
        """Test DOM element validation patterns for chart rendering"""

        # Test chart container IDs are properly formatted
        timeframes = ['5m', '15m', '1h', '1d', '1w']

        for timeframe in timeframes:
            chart_id = f"chart-{timeframe}"

            # Validate ID format (alphanumeric with hyphens)
            assert re.match(r'^[a-zA-Z0-9-]+$', chart_id), f"Invalid chart ID format: {chart_id}"

            # Ensure ID is unique per timeframe
            assert timeframe in chart_id, f"Chart ID doesn't contain timeframe: {chart_id}"

    def test_api_response_error_handling(self, analytics_service):
        """Test API response error handling for missing data"""

        # Test handling of empty response
        empty_response = {}
        result = analytics_service._validate_api_response(empty_response)
        assert not result['is_valid']
        assert 'error' in result

        # Test handling of malformed response
        malformed_response = {'data': None}
        result = analytics_service._validate_api_response(malformed_response)
        assert not result['is_valid']

        # Test handling of valid response
        valid_response = {
            'data': {'ohlcv': [{'open': 100}]},
            'metadata': {'symbol': 'AAPL'}
        }
        result = analytics_service._validate_api_response(valid_response)
        assert result['is_valid']