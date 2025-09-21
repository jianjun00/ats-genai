#!/usr/bin/env python3
"""
Comprehensive test cases for database-driven feature mapping system.

Tests the complete feature mapping flow:
1. FeatureExtractionDAO database operations
2. Feature mapping logic (exact match, pattern matching, fallbacks)
3. Integration with training data callback
4. Performance and caching behavior
5. Error handling and resilience
"""

import pytest
import asyncio
from datetime import date, datetime
from typing import Dict, Any, List
from unittest.mock import Mock, patch, AsyncMock

from core.platform.config.environment import Environment, EnvironmentType
from domains.ml.services.training_data.dao.feature_extraction_dao import (
    FeatureExtractionDAO, 
    FeatureGroup, 
    FeatureCatalog, 
    FeaturePattern, 
    FeatureMappingResult
)
from domains.ml.services.training_data.callbacks.training_data_callback import IntervalBasedTrainingDataCallback


class TestFeatureExtractionDAO:
    """Test FeatureExtractionDAO database operations and caching."""

    @pytest.fixture
    async def test_environment(self):
        """Create test environment with mock database."""
        env = Mock(spec=Environment)
        env.env_type = EnvironmentType.TEST
        env.get_connection = AsyncMock()
        return env

    @pytest.fixture
    async def feature_dao(self, test_environment):
        """Create FeatureExtractionDAO with test environment."""
        return FeatureExtractionDAO(test_environment)

    @pytest.fixture
    def sample_feature_groups(self):
        """Sample feature groups for testing."""
        return [
            FeatureGroup(
                id=1,
                group_name="ohlcv_basic",
                display_name="Basic OHLCV Features",
                category="basic",
                update_frequency="daily"
            ),
            FeatureGroup(
                id=2,
                group_name="technical_momentum",
                display_name="Technical Momentum",
                category="technical",
                update_frequency="daily"
            ),
            FeatureGroup(
                id=3,
                group_name="technical_volatility",
                display_name="Technical Volatility",
                category="technical",
                update_frequency="daily"
            )
        ]

    @pytest.fixture
    def sample_feature_catalog(self):
        """Sample feature catalog entries for testing."""
        return [
            FeatureCatalog(
                feature_name="open",
                feature_group_id=1,
                data_type="FLOAT64",
                column_position=2
            ),
            FeatureCatalog(
                feature_name="close",
                feature_group_id=1,
                data_type="FLOAT64",
                column_position=5
            ),
            FeatureCatalog(
                feature_name="sma_20",
                feature_group_id=2,
                data_type="FLOAT64",
                column_position=2
            ),
            FeatureCatalog(
                feature_name="rsi_14",
                feature_group_id=2,
                data_type="FLOAT64",
                column_position=4
            )
        ]

    @pytest.fixture
    def sample_feature_patterns(self):
        """Sample feature patterns for testing."""
        return [
            FeaturePattern(
                id=1,
                pattern="sma_",
                feature_group_id=2,
                pattern_type="starts_with",
                priority=100
            ),
            FeaturePattern(
                id=2,
                pattern="ema_",
                feature_group_id=2,
                pattern_type="starts_with",
                priority=101
            ),
            FeaturePattern(
                id=3,
                pattern="bb_",
                feature_group_id=3,
                pattern_type="starts_with",
                priority=200
            ),
            FeaturePattern(
                id=4,
                pattern=".*_vol.*",
                feature_group_id=3,
                pattern_type="regex",
                priority=300
            )
        ]

    async def test_get_feature_groups(self, feature_dao, test_environment, sample_feature_groups):
        """Test retrieving feature groups from database."""
        # Mock database response
        mock_conn = AsyncMock()
        mock_rows = [
            {
                'id': fg.id,
                'group_name': fg.group_name,
                'display_name': fg.display_name,
                'description': None,
                'category': fg.category,
                'update_frequency': fg.update_frequency,
                'computation_lag_minutes': 0,
                'dependencies': [],
                'storage_format': 'arrayrecord',
                'retention_months': 60,
                'is_active': True,
                'created_at': datetime.now(),
                'updated_at': datetime.now()
            }
            for fg in sample_feature_groups
        ]
        mock_conn.fetch.return_value = mock_rows
        test_environment.get_connection.return_value.__aenter__.return_value = mock_conn

        # Test method
        result = await feature_dao.get_feature_groups()

        # Verify results
        assert len(result) == len(sample_feature_groups)
        for i, fg in enumerate(result):
            assert fg.group_name == sample_feature_groups[i].group_name
            assert fg.display_name == sample_feature_groups[i].display_name

    async def test_feature_mapping_exact_match(self, feature_dao, test_environment, 
                                             sample_feature_groups, sample_feature_catalog):
        """Test exact feature name matching."""
        # Setup cache manually for testing
        feature_dao._feature_groups_cache = {fg.id: fg for fg in sample_feature_groups}
        feature_dao._feature_catalog_cache = {fc.feature_name: fc for fc in sample_feature_catalog}
        feature_dao._feature_patterns_cache = []
        feature_dao._cache_loaded = True

        # Test exact match
        result = await feature_dao.get_feature_group_mapping("open")

        # Verify exact match result
        assert result.feature_name == "open"
        assert result.feature_group_name == "ohlcv_basic"
        assert result.match_type == "exact"
        assert result.confidence == 1.0

    async def test_feature_mapping_pattern_match(self, feature_dao, test_environment,
                                               sample_feature_groups, sample_feature_patterns):
        """Test pattern-based feature matching."""
        # Setup cache manually for testing
        feature_dao._feature_groups_cache = {fg.id: fg for fg in sample_feature_groups}
        feature_dao._feature_catalog_cache = {}  # No exact matches
        feature_dao._feature_patterns_cache = sample_feature_patterns
        feature_dao._cache_loaded = True

        # Test starts_with pattern match
        result = await feature_dao.get_feature_group_mapping("sma_50")

        # Verify pattern match result
        assert result.feature_name == "sma_50"
        assert result.feature_group_name == "technical_momentum"
        assert result.match_type == "pattern"
        assert result.pattern_matched == "sma_"
        assert result.confidence >= 0.6

    async def test_feature_mapping_regex_pattern(self, feature_dao, test_environment,
                                               sample_feature_groups, sample_feature_patterns):
        """Test regex pattern matching."""
        # Setup cache manually for testing
        feature_dao._feature_groups_cache = {fg.id: fg for fg in sample_feature_groups}
        feature_dao._feature_catalog_cache = {}  # No exact matches
        feature_dao._feature_patterns_cache = sample_feature_patterns
        feature_dao._cache_loaded = True

        # Test regex pattern match
        result = await feature_dao.get_feature_group_mapping("realized_vol_20d")

        # Verify regex match result
        assert result.feature_name == "realized_vol_20d"
        assert result.feature_group_name == "technical_volatility"
        assert result.match_type == "pattern"
        assert result.pattern_matched == ".*_vol.*"

    async def test_feature_mapping_fallback_to_default(self, feature_dao, test_environment, sample_feature_groups):
        """Test fallback to default group for unknown features."""
        # Setup cache with only feature groups, no catalog or patterns
        feature_dao._feature_groups_cache = {fg.id: fg for fg in sample_feature_groups}
        feature_dao._feature_catalog_cache = {}
        feature_dao._feature_patterns_cache = []
        feature_dao._cache_loaded = True

        # Test unknown feature
        result = await feature_dao.get_feature_group_mapping("unknown_feature")

        # Verify fallback result
        assert result.feature_name == "unknown_feature"
        assert result.feature_group_name == "ohlcv_basic"  # Default group
        assert result.match_type == "default"
        assert result.confidence == 0.3

    async def test_feature_mapping_batch_operation(self, feature_dao, test_environment,
                                                  sample_feature_groups, sample_feature_catalog):
        """Test batch feature mapping for performance."""
        # Setup cache manually for testing
        feature_dao._feature_groups_cache = {fg.id: fg for fg in sample_feature_groups}
        feature_dao._feature_catalog_cache = {fc.feature_name: fc for fc in sample_feature_catalog}
        feature_dao._feature_patterns_cache = []
        feature_dao._cache_loaded = True

        # Test batch mapping
        feature_names = ["open", "close", "sma_20", "unknown_feature"]
        results = await feature_dao.get_feature_mappings_batch(feature_names)

        # Verify batch results
        assert len(results) == len(feature_names)
        
        # Check individual mappings
        open_result = next(r for r in results if r.feature_name == "open")
        assert open_result.feature_group_name == "ohlcv_basic"
        assert open_result.match_type == "exact"
        
        sma_result = next(r for r in results if r.feature_name == "sma_20")
        assert sma_result.feature_group_name == "technical_momentum"
        assert sma_result.match_type == "exact"

    async def test_pattern_matching_priority_order(self, feature_dao, test_environment,
                                                  sample_feature_groups, sample_feature_patterns):
        """Test that patterns are matched in priority order."""
        # Add overlapping patterns with different priorities
        overlapping_patterns = sample_feature_patterns + [
            FeaturePattern(
                id=5,
                pattern="s",  # Very broad pattern
                feature_group_id=1,
                pattern_type="starts_with",
                priority=1000  # Low priority (high number)
            )
        ]
        
        # Setup cache
        feature_dao._feature_groups_cache = {fg.id: fg for fg in sample_feature_groups}
        feature_dao._feature_catalog_cache = {}
        feature_dao._feature_patterns_cache = sorted(overlapping_patterns, key=lambda p: p.priority)
        feature_dao._cache_loaded = True

        # Test that higher priority pattern wins
        result = await feature_dao.get_feature_group_mapping("sma_20")

        # Should match "sma_" pattern (priority 100) not "s" pattern (priority 1000)
        assert result.feature_group_name == "technical_momentum"
        assert result.pattern_matched == "sma_"

    def test_pattern_match_methods(self, feature_dao):
        """Test individual pattern matching methods."""
        patterns = [
            FeaturePattern(pattern="test", pattern_type="exact"),
            FeaturePattern(pattern="sma_", pattern_type="starts_with"),
            FeaturePattern(pattern="_vol", pattern_type="ends_with"),
            FeaturePattern(pattern="bb", pattern_type="contains"),
            FeaturePattern(pattern=".*_[0-9]+d", pattern_type="regex")
        ]

        # Test exact match
        assert feature_dao._test_pattern_match("test", patterns[0]) == True
        assert feature_dao._test_pattern_match("test123", patterns[0]) == False

        # Test starts_with
        assert feature_dao._test_pattern_match("sma_20", patterns[1]) == True
        assert feature_dao._test_pattern_match("ema_20", patterns[1]) == False

        # Test ends_with
        assert feature_dao._test_pattern_match("realized_vol", patterns[2]) == True
        assert feature_dao._test_pattern_match("volume", patterns[2]) == False

        # Test contains
        assert feature_dao._test_pattern_match("bb_upper", patterns[3]) == True
        assert feature_dao._test_pattern_match("atr_14", patterns[3]) == False

        # Test regex
        assert feature_dao._test_pattern_match("vol_20d", patterns[4]) == True
        assert feature_dao._test_pattern_match("vol_20", patterns[4]) == False


class TestTrainingDataCallbackIntegration:
    """Test integration of database-driven feature mapping with training data callback."""

    @pytest.fixture
    def mock_runner(self):
        """Create mock runner with environment."""
        runner = Mock()
        env = Mock(spec=Environment)
        env.env_type = EnvironmentType.TEST
        env.get_connection = AsyncMock()
        runner.get_environment.return_value = env
        return runner

    @pytest.fixture
    def training_callback(self):
        """Create training data callback for testing."""
        return IntervalBasedTrainingDataCallback(
            symbols=["AAPL"],
            start_date="2025-07-01",
            end_date="2025-07-01"
        )

    @pytest.fixture
    def sample_features_5m(self):
        """Sample 5-minute timeframe features."""
        return {
            "5m_open": 150.0,
            "5m_high": 152.0,
            "5m_low": 149.5,
            "5m_close": 151.5,
            "5m_volume": 10000,
            "5m_vwap": 151.0,
            "5m_sma_20": 150.8,
            "5m_ema_12": 151.2,
            "5m_rsi_14": 65.5,
            "5m_bb_upper": 155.0,
            "5m_bb_lower": 147.0,
            "5m_atr_14": 2.5,
            "5m_unknown_indicator": 42.0
        }

    async def test_callback_feature_categorization_database_driven(self, training_callback, mock_runner, 
                                                                 sample_features_5m):
        """Test that callback uses database-driven feature categorization."""
        # Mock the feature DAO to return specific mappings
        mock_feature_dao = AsyncMock()
        mock_mappings = [
            FeatureMappingResult("open", "ohlcv_basic", 1, "exact", confidence=1.0),
            FeatureMappingResult("high", "ohlcv_basic", 1, "exact", confidence=1.0),
            FeatureMappingResult("low", "ohlcv_basic", 1, "exact", confidence=1.0),
            FeatureMappingResult("close", "ohlcv_basic", 1, "exact", confidence=1.0),
            FeatureMappingResult("volume", "ohlcv_basic", 1, "exact", confidence=1.0),
            FeatureMappingResult("vwap", "ohlcv_basic", 1, "exact", confidence=1.0),
            FeatureMappingResult("sma_20", "technical_momentum", 2, "exact", confidence=1.0),
            FeatureMappingResult("ema_12", "technical_momentum", 2, "exact", confidence=1.0),
            FeatureMappingResult("rsi_14", "technical_momentum", 2, "exact", confidence=1.0),
            FeatureMappingResult("bb_upper", "technical_volatility", 3, "pattern", "bb_", confidence=0.9),
            FeatureMappingResult("bb_lower", "technical_volatility", 3, "pattern", "bb_", confidence=0.9),
            FeatureMappingResult("atr_14", "technical_volatility", 3, "exact", confidence=1.0),
            FeatureMappingResult("unknown_indicator", "ohlcv_basic", 1, "default", confidence=0.3)
        ]
        mock_feature_dao.get_feature_mappings_batch.return_value = mock_mappings

        # Mock the _get_feature_dao method
        training_callback._get_feature_dao = AsyncMock(return_value=mock_feature_dao)

        # Test categorization
        result = await training_callback._categorize_features_by_group(
            sample_features_5m, "5m", mock_feature_dao
        )

        # Verify feature groups were created correctly
        assert "ohlcv_basic" in result
        assert "technical_momentum" in result
        assert "technical_volatility" in result

        # Verify OHLCV features
        ohlcv_features = result["ohlcv_basic"]
        expected_ohlcv = {"open", "high", "low", "close", "volume", "vwap", "unknown_indicator"}
        assert set(ohlcv_features.keys()) == expected_ohlcv

        # Verify technical momentum features
        momentum_features = result["technical_momentum"]
        expected_momentum = {"sma_20", "ema_12", "rsi_14"}
        assert set(momentum_features.keys()) == expected_momentum

        # Verify technical volatility features
        volatility_features = result["technical_volatility"]
        expected_volatility = {"bb_upper", "bb_lower", "atr_14"}
        assert set(volatility_features.keys()) == expected_volatility

        # Verify feature DAO was called correctly
        mock_feature_dao.get_feature_mappings_batch.assert_called_once()
        call_args = mock_feature_dao.get_feature_mappings_batch.call_args[0][0]
        expected_base_features = [
            "open", "high", "low", "close", "volume", "vwap",
            "sma_20", "ema_12", "rsi_14", "bb_upper", "bb_lower", "atr_14", "unknown_indicator"
        ]
        assert set(call_args) == set(expected_base_features)

    async def test_callback_fallback_when_database_fails(self, training_callback, mock_runner, sample_features_5m):
        """Test that callback falls back to basic categorization when database fails."""
        # Mock feature DAO to raise exception
        mock_feature_dao = AsyncMock()
        mock_feature_dao.get_feature_mappings_batch.side_effect = Exception("Database connection failed")

        training_callback._get_feature_dao = AsyncMock(return_value=mock_feature_dao)

        # Test categorization with database failure
        result = await training_callback._categorize_features_by_group(
            sample_features_5m, "5m", mock_feature_dao
        )

        # Verify fallback categorization was used
        assert "ohlcv_basic" in result
        assert "technical_momentum" in result

        # OHLCV features should be categorized correctly
        ohlcv_features = result["ohlcv_basic"]
        expected_ohlcv = {"open", "high", "low", "close", "volume", "vwap"}
        assert expected_ohlcv.issubset(set(ohlcv_features.keys()))

        # Unknown features should default to technical_momentum
        momentum_features = result["technical_momentum"]
        assert "unknown_indicator" in momentum_features

    async def test_feature_dao_lazy_initialization(self, training_callback, mock_runner):
        """Test that feature DAO is initialized lazily."""
        # Initially should be None
        assert training_callback._feature_dao is None

        # After first call, should be initialized
        feature_dao = await training_callback._get_feature_dao(mock_runner)
        assert feature_dao is not None
        assert training_callback._feature_dao is feature_dao

        # Subsequent calls should return same instance
        feature_dao2 = await training_callback._get_feature_dao(mock_runner)
        assert feature_dao2 is feature_dao

    async def test_feature_dao_caching_behavior(self, training_callback, mock_runner):
        """Test that feature DAO properly caches database queries."""
        # Mock database responses
        mock_conn = AsyncMock()
        
        # Mock feature groups query
        mock_groups_rows = [
            {
                'id': 1, 'group_name': 'ohlcv_basic', 'display_name': 'Basic OHLCV',
                'description': None, 'category': 'basic', 'update_frequency': 'daily',
                'computation_lag_minutes': 0, 'dependencies': [], 'storage_format': 'arrayrecord',
                'retention_months': 60, 'is_active': True, 'created_at': datetime.now(),
                'updated_at': datetime.now()
            }
        ]
        
        # Mock feature catalog query
        mock_catalog_rows = [
            {
                'feature_name': 'open', 'feature_group_id': 1, 'data_type': 'FLOAT64',
                'column_position': 2, 'description': None, 'computation_method': None,
                'dependencies': [], 'validation_rules': None
            }
        ]
        
        # Mock feature patterns query
        mock_patterns_rows = []
        
        # Setup mock responses in order they'll be called
        mock_conn.fetch.side_effect = [mock_groups_rows, mock_catalog_rows, mock_patterns_rows]
        
        mock_env = mock_runner.get_environment.return_value
        mock_env.get_connection.return_value.__aenter__.return_value = mock_conn

        # Get feature DAO and trigger cache loading
        feature_dao = await training_callback._get_feature_dao(mock_runner)
        await feature_dao._ensure_cache_loaded()

        # Verify cache was loaded
        assert feature_dao._cache_loaded == True
        assert feature_dao._feature_groups_cache is not None
        assert feature_dao._feature_catalog_cache is not None
        assert feature_dao._feature_patterns_cache is not None

        # Verify database was called expected number of times
        assert mock_conn.fetch.call_count == 3


class TestFeatureMappingPerformance:
    """Test performance and efficiency of feature mapping system."""

    @pytest.fixture
    async def large_feature_dao(self, test_environment):
        """Create FeatureExtractionDAO with large dataset for performance testing."""
        dao = FeatureExtractionDAO(test_environment)
        
        # Mock large feature catalog (1000 features)
        large_catalog = {}
        for i in range(1000):
            feature_name = f"feature_{i:04d}"
            large_catalog[feature_name] = FeatureCatalog(
                feature_name=feature_name,
                feature_group_id=(i % 4) + 1,  # Distribute across 4 groups
                data_type="FLOAT64",
                column_position=i
            )
        
        # Mock large pattern list (100 patterns)
        large_patterns = []
        for i in range(100):
            large_patterns.append(FeaturePattern(
                id=i,
                pattern=f"pattern_{i:02d}_",
                feature_group_id=(i % 4) + 1,
                pattern_type="starts_with",
                priority=i * 10
            ))
        
        # Setup cache
        dao._feature_catalog_cache = large_catalog
        dao._feature_patterns_cache = large_patterns
        dao._feature_groups_cache = {
            1: FeatureGroup(id=1, group_name="group_1"),
            2: FeatureGroup(id=2, group_name="group_2"),
            3: FeatureGroup(id=3, group_name="group_3"),
            4: FeatureGroup(id=4, group_name="group_4")
        }
        dao._cache_loaded = True
        
        return dao

    async def test_batch_mapping_performance(self, large_feature_dao):
        """Test performance of batch feature mapping with large datasets."""
        # Create large batch of feature names (500 features)
        feature_names = [f"feature_{i:04d}" for i in range(500)]
        
        # Measure batch mapping time
        import time
        start_time = time.time()
        
        results = await large_feature_dao.get_feature_mappings_batch(feature_names)
        
        end_time = time.time()
        execution_time = end_time - start_time
        
        # Verify results
        assert len(results) == 500
        
        # Performance assertion - should complete within reasonable time
        assert execution_time < 1.0  # Should complete within 1 second
        
        # Verify all mappings are exact matches
        exact_matches = [r for r in results if r.match_type == "exact"]
        assert len(exact_matches) == 500

    async def test_cache_efficiency(self, large_feature_dao):
        """Test that caching improves performance on repeated queries."""
        feature_names = ["feature_0001", "feature_0002", "feature_0003"]
        
        # First call - cache should be used
        import time
        start_time = time.time()
        results1 = await large_feature_dao.get_feature_mappings_batch(feature_names)
        first_call_time = time.time() - start_time
        
        # Second call - should be even faster due to cache
        start_time = time.time()
        results2 = await large_feature_dao.get_feature_mappings_batch(feature_names)
        second_call_time = time.time() - start_time
        
        # Verify results are identical
        assert len(results1) == len(results2)
        for r1, r2 in zip(results1, results2):
            assert r1.feature_name == r2.feature_name
            assert r1.feature_group_name == r2.feature_group_name
        
        # Performance should be consistent (both calls use cache)
        assert second_call_time <= first_call_time * 1.1  # Allow 10% variance


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])