#!/usr/bin/env python3
"""
Comprehensive unit tests for shared.utils.vendor_api_keys module.

Tests the API key management utility that replaces repetitive patterns
found in 15+ vendor integration files.
"""

import pytest
import os
from unittest.mock import patch, Mock
import sys
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent.parent / 'src'))

from shared.utils.vendor_api_keys import (
    get_vendor_api_key,
    get_all_vendor_api_keys,
    validate_vendor_api_key,
    get_polygon_api_key,
    get_eodhd_api_key,
    get_tiingo_api_key,
    VENDOR_API_KEY_MAP
)


class TestVendorAPIKeyMapping:
    """Test the vendor to environment variable mapping"""

    def test_vendor_mapping_completeness(self):
        """Test that all expected vendors are mapped"""
        expected_vendors = [
            'polygon', 'eodhd', 'tiingo', 'alpha_vantage',
            'finnhub', 'quandl', 'iex', 'newsapi'
        ]

        for vendor in expected_vendors:
            assert vendor in VENDOR_API_KEY_MAP
            assert VENDOR_API_KEY_MAP[vendor].endswith('_API_KEY')

    def test_vendor_mapping_consistency(self):
        """Test that vendor mapping follows consistent patterns"""
        for vendor, env_var in VENDOR_API_KEY_MAP.items():
            # Environment variable should be uppercase version of vendor + _API_KEY
            expected_prefix = vendor.upper().replace('_', '_')
            assert env_var.startswith(expected_prefix)
            assert env_var.endswith('_API_KEY')


class TestGetVendorAPIKey:
    """Test the main get_vendor_api_key function"""

    @patch.dict(os.environ, {}, clear=True)
    def test_get_api_key_from_environment_variable(self):
        """Test getting API key from environment variable (highest priority)"""
        test_key = "test_polygon_api_key_12345"

        with patch.dict(os.environ, {'POLYGON_API_KEY': test_key}):
            result = get_vendor_api_key('polygon')
            assert result == test_key

    def test_get_api_key_case_insensitive_vendor(self):
        """Test that vendor names are case insensitive"""
        test_key = "test_key_12345"

        with patch.dict(os.environ, {'POLYGON_API_KEY': test_key}):
            assert get_vendor_api_key('POLYGON') == test_key
            assert get_vendor_api_key('Polygon') == test_key
            assert get_vendor_api_key('polygon') == test_key

    def test_get_api_key_strips_whitespace(self):
        """Test that vendor names have whitespace stripped"""
        test_key = "test_key_12345"

        with patch.dict(os.environ, {'POLYGON_API_KEY': test_key}):
            assert get_vendor_api_key(' polygon ') == test_key
            assert get_vendor_api_key('\tpolygon\n') == test_key

    def test_unknown_vendor_required_true(self):
        """Test handling of unknown vendor with required=True"""
        with patch('shared.utils.vendor_api_keys.logger') as mock_logger:
            result = get_vendor_api_key('unknown_vendor', required=True)
            assert result is None
            mock_logger.error.assert_called_once()
            assert 'Unknown vendor' in mock_logger.error.call_args[0][0]

    def test_unknown_vendor_required_false(self):
        """Test handling of unknown vendor with required=False"""
        with patch('shared.utils.vendor_api_keys.logger') as mock_logger:
            result = get_vendor_api_key('unknown_vendor', required=False)
            assert result is None
            mock_logger.error.assert_not_called()

    @patch.dict(os.environ, {}, clear=True)
    def test_api_key_from_polygon_utils(self):
        """Test getting API key from polygon utils system"""
        test_key = "polygon_utils_key_12345"

        # Mock the polygon utils import
        mock_utils = Mock()
        mock_utils.POLYGON_API_KEY = test_key

        with patch.dict('sys.modules', {'infrastructure.vendor.polygon.utils': mock_utils}):
            result = get_vendor_api_key('polygon')
            assert result == test_key

    @patch.dict(os.environ, {}, clear=True)
    def test_api_key_from_eodhd_utils(self):
        """Test getting API key from eodhd utils system"""
        test_key = "eodhd_utils_key_12345"

        # Mock the eodhd utils import
        mock_utils = Mock()
        mock_utils.EODHD_API_KEY = test_key

        with patch.dict('sys.modules', {'infrastructure.vendor.eodhd.utils': mock_utils}):
            result = get_vendor_api_key('eodhd')
            assert result == test_key

    @patch.dict(os.environ, {}, clear=True)
    def test_api_key_from_tiingo_utils(self):
        """Test getting API key from tiingo utils system"""
        test_key = "tiingo_utils_key_12345"

        # Mock the tiingo utils import
        mock_utils = Mock()
        mock_utils.TIINGO_API_KEY = test_key

        with patch.dict('sys.modules', {'infrastructure.vendor.tiingo.utils': mock_utils}):
            result = get_vendor_api_key('tiingo')
            assert result == test_key

    @patch.dict(os.environ, {}, clear=True)
    def test_api_key_from_environment_gin_config(self):
        """Test getting API key from environment gin config"""
        test_key = "gin_config_key_12345"

        # Mock environment with get_api_key method
        mock_env = Mock()
        mock_env.get_api_key.return_value = test_key

        result = get_vendor_api_key('polygon', env=mock_env)
        assert result == test_key
        mock_env.get_api_key.assert_called_once_with('polygon')

    def test_priority_order_environment_wins(self):
        """Test that environment variable has highest priority"""
        env_key = "env_key_12345"
        utils_key = "utils_key_12345"
        gin_key = "gin_key_12345"

        # Mock utils system
        mock_utils = Mock()
        mock_utils.POLYGON_API_KEY = utils_key

        # Mock environment
        mock_env = Mock()
        mock_env.get_api_key.return_value = gin_key

        with patch.dict(os.environ, {'POLYGON_API_KEY': env_key}):
            with patch.dict('sys.modules', {'infrastructure.vendor.polygon.utils': mock_utils}):
                result = get_vendor_api_key('polygon', env=mock_env)
                assert result == env_key  # Environment variable wins
                mock_env.get_api_key.assert_not_called()  # Gin config not checked

    def test_priority_order_utils_over_gin(self):
        """Test that utils system has priority over gin config"""
        utils_key = "utils_key_12345"
        gin_key = "gin_key_12345"

        # Mock utils system
        mock_utils = Mock()
        mock_utils.POLYGON_API_KEY = utils_key

        # Mock environment
        mock_env = Mock()
        mock_env.get_api_key.return_value = gin_key

        with patch.dict(os.environ, {}, clear=True):
            with patch.dict('sys.modules', {'infrastructure.vendor.polygon.utils': mock_utils}):
                result = get_vendor_api_key('polygon', env=mock_env)
                assert result == utils_key  # Utils system wins
                mock_env.get_api_key.assert_not_called()  # Gin config not checked

    @patch.dict(os.environ, {}, clear=True)
    def test_fallback_to_gin_config(self):
        """Test fallback to gin config when environment and utils fail"""
        gin_key = "gin_config_key_12345"

        # Mock environment
        mock_env = Mock()
        mock_env.get_api_key.return_value = gin_key

        # Mock utils import failure
        with patch('builtins.__import__', side_effect=ImportError("Module not found")):
            result = get_vendor_api_key('polygon', env=mock_env)
            assert result == gin_key
            mock_env.get_api_key.assert_called_once_with('polygon')

    @patch.dict(os.environ, {}, clear=True)
    def test_no_api_key_found_required_true(self):
        """Test behavior when no API key is found and required=True"""
        with patch('shared.utils.vendor_api_keys.logger') as mock_logger:
            result = get_vendor_api_key('polygon', required=True)
            assert result is None
            mock_logger.error.assert_called()
            assert 'No API key found' in mock_logger.error.call_args[0][0]

    @patch.dict(os.environ, {}, clear=True)
    def test_no_api_key_found_required_false(self):
        """Test behavior when no API key is found and required=False"""
        with patch('shared.utils.vendor_api_keys.logger') as mock_logger:
            result = get_vendor_api_key('polygon', required=False)
            assert result is None
            mock_logger.error.assert_not_called()

    def test_utils_import_error_handled_gracefully(self):
        """Test that utils import errors are handled gracefully"""
        test_key = "gin_key_12345"
        mock_env = Mock()
        mock_env.get_api_key.return_value = test_key

        with patch.dict(os.environ, {}, clear=True):
            with patch('builtins.__import__', side_effect=ImportError("Module not found")):
                result = get_vendor_api_key('polygon', env=mock_env)
                assert result == test_key  # Falls back to gin config

    def test_gin_config_error_handled_gracefully(self):
        """Test that gin config errors are handled gracefully"""
        mock_env = Mock()
        mock_env.get_api_key.side_effect = Exception("Gin config error")

        with patch.dict(os.environ, {}, clear=True):
            with patch('shared.utils.vendor_api_keys.logger') as mock_logger:
                result = get_vendor_api_key('polygon', env=mock_env, required=False)
                assert result is None
                mock_logger.error.assert_not_called()  # Should not log error when required=False


class TestGetAllVendorAPIKeys:
    """Test the get_all_vendor_api_keys function"""

    def test_get_all_keys_with_environment_variables(self):
        """Test getting all available API keys"""
        test_keys = {
            'POLYGON_API_KEY': 'polygon_key_123',
            'EODHD_API_KEY': 'eodhd_key_456',
            'TIINGO_API_KEY': 'tiingo_key_789'
        }

        with patch.dict(os.environ, test_keys):
            result = get_all_vendor_api_keys()

            assert 'polygon' in result
            assert 'eodhd' in result
            assert 'tiingo' in result
            assert result['polygon'] == 'polygon_key_123'
            assert result['eodhd'] == 'eodhd_key_456'
            assert result['tiingo'] == 'tiingo_key_789'

    @patch.dict(os.environ, {}, clear=True)
    def test_get_all_keys_empty_result(self):
        """Test getting all keys when none are available"""
        result = get_all_vendor_api_keys()
        assert result == {}

    def test_get_all_keys_with_required_vendors(self):
        """Test getting keys with required vendors specified"""
        test_keys = {
            'POLYGON_API_KEY': 'polygon_key_123',
            'TIINGO_API_KEY': 'tiingo_key_789'
        }

        with patch.dict(os.environ, test_keys):
            with patch('shared.utils.vendor_api_keys.logger') as mock_logger:
                result = get_all_vendor_api_keys(required_vendors=['polygon', 'eodhd'])

                assert 'polygon' in result
                assert result['polygon'] == 'polygon_key_123'
                # EODHD should trigger error log because it's required but not found
                mock_logger.error.assert_called()

    def test_get_all_keys_partial_availability(self):
        """Test getting keys when only some vendors have keys"""
        test_keys = {
            'POLYGON_API_KEY': 'polygon_key_123'
        }

        with patch.dict(os.environ, test_keys):
            result = get_all_vendor_api_keys()

            assert 'polygon' in result
            assert result['polygon'] == 'polygon_key_123'
            assert 'eodhd' not in result
            assert 'tiingo' not in result


class TestValidateVendorAPIKey:
    """Test the validate_vendor_api_key function"""

    def test_validate_polygon_key_valid(self):
        """Test validation of valid Polygon API keys"""
        valid_keys = [
            "abcdef123456789",  # Basic alphanumeric
            "abc_def_123456789",  # With underscores
            "abc.def.123456789",  # With dots
            "68aa0c7d2fe831.67386369",  # Real format example
        ]

        for key in valid_keys:
            assert validate_vendor_api_key('polygon', key) is True

    def test_validate_polygon_key_invalid(self):
        """Test validation of invalid Polygon API keys"""
        invalid_keys = [
            "",  # Empty
            "short",  # Too short
            None,  # None
            123,  # Not string
            "abc def 123",  # Spaces not allowed
            "abc@def#123",  # Special characters not allowed
        ]

        for key in invalid_keys:
            assert validate_vendor_api_key('polygon', key) is False

    def test_validate_eodhd_key_valid(self):
        """Test validation of valid EODHD API keys"""
        valid_keys = [
            "675b5a33b36f43.67825763",  # Real format example
            "abcdef123456789",
            "abc_def_123456789",
        ]

        for key in valid_keys:
            assert validate_vendor_api_key('eodhd', key) is True

    def test_validate_tiingo_key_valid(self):
        """Test validation of valid Tiingo API keys"""
        valid_keys = [
            "abcdef123456789",
            "abc-def-123456789",  # Tiingo allows hyphens
            "abc_def_123456789",
        ]

        for key in valid_keys:
            assert validate_vendor_api_key('tiingo', key) is True

    def test_validate_alpha_vantage_key_valid(self):
        """Test validation of valid Alpha Vantage API keys"""
        valid_keys = [
            "ABCDEF123456789",  # Alpha Vantage is typically alphanumeric only
            "abc123def456ghi789",
        ]

        for key in valid_keys:
            assert validate_vendor_api_key('alpha_vantage', key) is True

    def test_validate_alpha_vantage_key_invalid(self):
        """Test validation of invalid Alpha Vantage API keys"""
        invalid_keys = [
            "abc_def_123",  # Alpha Vantage doesn't allow underscores
            "abc-def-123",  # Or hyphens
        ]

        for key in invalid_keys:
            assert validate_vendor_api_key('alpha_vantage', key) is False

    def test_validate_unknown_vendor_defaults(self):
        """Test validation of unknown vendor uses default validation"""
        # Unknown vendor should use default validation (length > 5)
        assert validate_vendor_api_key('unknown_vendor', 'short') is False
        assert validate_vendor_api_key('unknown_vendor', 'long_enough_key') is True


class TestConvenienceFunctions:
    """Test convenience functions for common vendors"""

    def test_get_polygon_api_key(self):
        """Test get_polygon_api_key convenience function"""
        test_key = "polygon_key_12345"

        with patch('shared.utils.vendor_api_keys.get_vendor_api_key') as mock_get:
            mock_get.return_value = test_key

            result = get_polygon_api_key()
            assert result == test_key
            mock_get.assert_called_once_with('polygon', env=None, required=True)

    def test_get_polygon_api_key_with_params(self):
        """Test get_polygon_api_key with custom parameters"""
        test_key = "polygon_key_12345"
        mock_env = Mock()

        with patch('shared.utils.vendor_api_keys.get_vendor_api_key') as mock_get:
            mock_get.return_value = test_key

            result = get_polygon_api_key(env=mock_env, required=False)
            assert result == test_key
            mock_get.assert_called_once_with('polygon', env=mock_env, required=False)

    def test_get_eodhd_api_key(self):
        """Test get_eodhd_api_key convenience function"""
        test_key = "eodhd_key_12345"

        with patch('shared.utils.vendor_api_keys.get_vendor_api_key') as mock_get:
            mock_get.return_value = test_key

            result = get_eodhd_api_key()
            assert result == test_key
            mock_get.assert_called_once_with('eodhd', env=None, required=True)

    def test_get_tiingo_api_key(self):
        """Test get_tiingo_api_key convenience function"""
        test_key = "tiingo_key_12345"

        with patch('shared.utils.vendor_api_keys.get_vendor_api_key') as mock_get:
            mock_get.return_value = test_key

            result = get_tiingo_api_key()
            assert result == test_key
            mock_get.assert_called_once_with('tiingo', env=None, required=True)


class TestEdgeCases:
    """Test edge cases and error conditions"""

    def test_empty_string_vendor(self):
        """Test handling of empty string vendor"""
        with patch('shared.utils.vendor_api_keys.logger') as mock_logger:
            result = get_vendor_api_key('', required=True)
            assert result is None
            mock_logger.error.assert_called_once()

    def test_none_vendor(self):
        """Test handling of None vendor"""
        with pytest.raises(AttributeError):
            get_vendor_api_key(None)

    def test_empty_api_key_from_environment(self):
        """Test handling of empty API key from environment"""
        with patch.dict(os.environ, {'POLYGON_API_KEY': ''}):
            result = get_vendor_api_key('polygon', required=False)
            # Empty string should be treated as no key
            assert result is None or result == ''

    def test_whitespace_only_api_key(self):
        """Test handling of whitespace-only API key"""
        with patch.dict(os.environ, {'POLYGON_API_KEY': '   \t\n   '}):
            result = get_vendor_api_key('polygon', required=False)
            # Whitespace-only should be treated as no key or returned as-is
            assert result is None or result.strip() == ''


class TestLogging:
    """Test logging behavior"""

    def test_debug_logging_environment_variable(self):
        """Test debug logging when using environment variable"""
        test_key = "test_key_12345"

        with patch.dict(os.environ, {'POLYGON_API_KEY': test_key}):
            with patch('shared.utils.vendor_api_keys.logger') as mock_logger:
                result = get_vendor_api_key('polygon')
                assert result == test_key
                mock_logger.debug.assert_called_once()
                assert 'environment variable' in mock_logger.debug.call_args[0][0]

    def test_debug_logging_utils_system(self):
        """Test debug logging when using utils system"""
        test_key = "utils_key_12345"
        mock_utils = Mock()
        mock_utils.POLYGON_API_KEY = test_key

        with patch.dict(os.environ, {}, clear=True):
            with patch.dict('sys.modules', {'infrastructure.vendor.polygon.utils': mock_utils}):
                with patch('shared.utils.vendor_api_keys.logger') as mock_logger:
                    result = get_vendor_api_key('polygon')
                    assert result == test_key
                    mock_logger.debug.assert_called_once()
                    assert 'utils system' in mock_logger.debug.call_args[0][0]

    def test_debug_logging_gin_config(self):
        """Test debug logging when using gin configuration"""
        test_key = "gin_key_12345"
        mock_env = Mock()
        mock_env.get_api_key.return_value = test_key

        with patch.dict(os.environ, {}, clear=True):
            with patch('shared.utils.vendor_api_keys.logger') as mock_logger:
                result = get_vendor_api_key('polygon', env=mock_env)
                assert result == test_key
                mock_logger.debug.assert_called_once()
                assert 'gin configuration' in mock_logger.debug.call_args[0][0]


if __name__ == '__main__':
    pytest.main([__file__])