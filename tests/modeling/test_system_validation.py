"""
System Validation Tests for Enhanced Multi-Timeframe Training Data System

Simple validation tests that can run immediately to verify system functionality.
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'src'))

import numpy as np
import pandas as pd
from datetime import datetime, timedelta

try:
    from domains.ml.services.enhanced_feature_types import (
        FeatureSpecification, FeatureType, TimeframeSpec, 
        TechnicalIndicator, EnhancedFeatureRegistry
    )
    from domains.ml.services.cross_timeframe_aligner import CrossTimeframeAligner
    from domains.ml.services.multi_timeframe_data_collector import DataCollectionConfig
except ImportError as e:
    print(f"❌ Import error: {e}")
    print("Please ensure src/ directory is in PYTHONPATH")
    sys.exit(1)


class TestSystemValidation:
    """Quick validation tests for the enhanced training data system."""
        
    def test_enhanced_feature_registry(self):
        """Test enhanced feature registry functionality."""
        print("🧪 Enhanced Multi-Timeframe Training Data System Validation")
        print("=" * 60)
        print("📋 Testing Enhanced Feature Registry...")
        
        registry = EnhancedFeatureRegistry()
        
        # Test basic registry functionality
        assert len(registry.feature_specs) > 0, "Registry should contain feature specs"
        
        # Test OHLC features
        ohlc_spec = registry.get_feature_spec("ohlc_daily_16")
        assert ohlc_spec is not None, "Should find ohlc_daily_16 feature"
        assert ohlc_spec.feature_type == FeatureType.OHLC_INTERVALS
        assert ohlc_spec.dimensions == (16, 4)
        
        # Test technical indicator features
        etop_spec = registry.get_feature_spec("etop_5min_8")
        assert etop_spec is not None, "Should find etop_5min_8 feature"
        assert etop_spec.feature_type == FeatureType.PRICE_INDICATOR_INTERVALS
        assert etop_spec.indicator_type == TechnicalIndicator.ETOP
        
        # Test cross-timeframe features
        cross_spec = registry.get_feature_spec("etop_1hour_on_5min")
        assert cross_spec is not None, "Should find cross-timeframe feature"
        assert cross_spec.feature_type == FeatureType.CROSS_TIMEFRAME_INDICATORS
        assert cross_spec.source_timeframe == TimeframeSpec.HOUR_1
        
        print("  ✅ Feature registry validation passed")
        
    def test_feature_type_coverage(self):
        """Test that all feature types are properly covered."""
        print("📊 Testing Feature Type Coverage...")
        
        registry = EnhancedFeatureRegistry()
        all_specs = list(registry.feature_specs.values())
        
        # Count features by type
        type_counts = {}
        for spec in all_specs:
            feature_type = spec.feature_type
            type_counts[feature_type] = type_counts.get(feature_type, 0) + 1
        
        # Validate coverage
        assert FeatureType.OHLC_INTERVALS in type_counts, "Missing OHLC features"
        assert FeatureType.PRICE_INDICATOR_INTERVALS in type_counts, "Missing indicator features"
        assert FeatureType.CROSS_TIMEFRAME_INDICATORS in type_counts, "Missing cross-timeframe features"
        
        print(f"  ✅ OHLC Features: {type_counts.get(FeatureType.OHLC_INTERVALS, 0)}")
        print(f"  ✅ Indicator Features: {type_counts.get(FeatureType.PRICE_INDICATOR_INTERVALS, 0)}")
        print(f"  ✅ Cross-timeframe Features: {type_counts.get(FeatureType.CROSS_TIMEFRAME_INDICATORS, 0)}")
        print(f"  ✅ Total Features: {len(all_specs)}")
        
    def test_timeframe_specifications(self):
        """Test timeframe specification functionality."""
        print("⏱️  Testing Timeframe Specifications...")
        
        # Test timeframe multipliers
        assert TimeframeSpec.MINUTE_5.multiplier == 1, "5-minute should be base timeframe"
        assert TimeframeSpec.MINUTE_15.multiplier == 3, "15-minute should be 3x base"
        assert TimeframeSpec.HOUR_1.multiplier == 12, "1-hour should be 12x base"
        assert TimeframeSpec.DAILY.multiplier == 288, "Daily should be 288x base (24*12)"
        
        # Test timeframe labels
        assert TimeframeSpec.MINUTE_5.label == "5min"
        assert TimeframeSpec.HOUR_1.label == "1hour"
        assert TimeframeSpec.DAILY.label == "daily"
        
        print("  ✅ Timeframe specifications validated")
        
    def test_technical_indicators(self):
        """Test technical indicator definitions."""
        print("📈 Testing Technical Indicators...")
        
        # Test indicator properties
        indicators = [
            TechnicalIndicator.ETOP,
            TechnicalIndicator.EBOT,
            TechnicalIndicator.PLDOT,
            TechnicalIndicator.EMA,
            TechnicalIndicator.RSI,
            TechnicalIndicator.MACD
        ]
        
        for indicator in indicators:
            assert hasattr(indicator, 'display_name'), f"{indicator} should have display_name"
            assert hasattr(indicator, 'category'), f"{indicator} should have category"
            assert hasattr(indicator, 'color'), f"{indicator} should have color"
            assert len(indicator.display_name) > 0, f"{indicator} display_name should not be empty"
            
        print(f"  ✅ Validated {len(indicators)} technical indicators")
        
    def test_cross_timeframe_aligner_initialization(self):
        """Test cross-timeframe aligner initialization."""
        print("🔄 Testing Cross-Timeframe Aligner...")
        
        aligner = CrossTimeframeAligner()
        
        # Test initialization
        assert hasattr(aligner, 'timeframe_multipliers'), "Should have timeframe multipliers"
        assert len(aligner.timeframe_multipliers) > 0, "Should contain timeframe mappings"
        
        # Test multiplier calculations
        assert aligner.timeframe_multipliers[TimeframeSpec.MINUTE_5] == 1
        assert aligner.timeframe_multipliers[TimeframeSpec.HOUR_1] == 12
        assert aligner.timeframe_multipliers[TimeframeSpec.DAILY] == 288
        
        print("  ✅ Cross-timeframe aligner initialized correctly")
        
    def test_data_collection_config(self):
        """Test data collection configuration."""
        print("⚙️  Testing Data Collection Config...")
        
        registry = EnhancedFeatureRegistry()
        feature_specs = [
            registry.get_feature_spec("ohlc_5min_8"),
            registry.get_feature_spec("etop_5min_8")
        ]
        feature_specs = [spec for spec in feature_specs if spec is not None]
        
        config = DataCollectionConfig(
            symbols=['AAPL', 'TSLA'],
            start_date='2024-01-01',
            end_date='2024-01-31',
            feature_specs=feature_specs,
            batch_size=1000,
            validate_data=True
        )
        
        # Test configuration properties
        assert len(config.symbols) == 2
        assert config.start_date == '2024-01-01'
        assert config.end_date == '2024-01-31'
        assert len(config.feature_specs) > 0
        assert config.batch_size == 1000
        assert config.validate_data is True
        
        print("  ✅ Data collection configuration validated")
        
    def test_feature_dimensions_consistency(self):
        """Test that feature dimensions are consistent."""
        print("📏 Testing Feature Dimension Consistency...")
        
        registry = EnhancedFeatureRegistry()
        
        # Test OHLC features have 4 dimensions
        ohlc_features = [
            "ohlc_5min_8", "ohlc_15min_16", "ohlc_1hour_12", "ohlc_daily_32"
        ]
        
        for feature_name in ohlc_features:
            spec = registry.get_feature_spec(feature_name)
            if spec:
                assert spec.dimensions[1] == 4, f"{feature_name} should have 4 OHLC dimensions"
        
        # Test indicator features have 1 dimension
        indicator_features = [
            "etop_5min_8", "ebot_5min_8", "pldot_15min_16"
        ]
        
        for feature_name in indicator_features:
            spec = registry.get_feature_spec(feature_name)
            if spec:
                assert spec.dimensions[1] == 1, f"{feature_name} should have 1 indicator dimension"
        
        print("  ✅ Feature dimensions are consistent")
        
    def test_visualization_metadata(self):
        """Test visualization metadata completeness."""
        print("👁️  Testing Visualization Metadata...")
        
        registry = EnhancedFeatureRegistry()
        
        # Check that features have visualization metadata
        specs_with_viz = 0
        total_specs = 0
        
        for spec in registry.feature_specs.values():
            total_specs += 1
            if hasattr(spec, 'visualization_config') and spec.visualization_config:
                specs_with_viz += 1
        
        print(f"  📊 Features with visualization metadata: {specs_with_viz}/{total_specs}")
        print("  ✅ Visualization metadata checked")
        
    def test_system_integration_readiness(self):
        """Test that system components can work together."""
        print("🔗 Testing System Integration Readiness...")
        
        # Test component compatibility
        registry = EnhancedFeatureRegistry()
        aligner = CrossTimeframeAligner()
        
        # Test that registry and aligner work with same timeframe specs
        for timeframe in [TimeframeSpec.MINUTE_5, TimeframeSpec.HOUR_1, TimeframeSpec.DAILY]:
            assert timeframe in aligner.timeframe_multipliers, f"Aligner missing {timeframe}"
            
        # Test feature spec compatibility
        cross_spec = registry.get_feature_spec("etop_1hour_on_5min")
        if cross_spec:
            assert cross_spec.source_timeframe in aligner.timeframe_multipliers
            assert cross_spec.timeframe in aligner.timeframe_multipliers
        
        print("  ✅ System components are integration-ready")
        
    def run_all_tests(self):
        """Run all validation tests."""
        tests = [
            self.test_enhanced_feature_registry,
            self.test_feature_type_coverage,
            self.test_timeframe_specifications,
            self.test_technical_indicators,
            self.test_cross_timeframe_aligner_initialization,
            self.test_data_collection_config,
            self.test_feature_dimensions_consistency,
            self.test_visualization_metadata,
            self.test_system_integration_readiness
        ]
        
        passed = 0
        failed = 0
        
        for test in tests:
            try:
                test()
                passed += 1
            except Exception as e:
                print(f"  ❌ {test.__name__}: {str(e)}")
                failed += 1
        
        print(f"\n📊 Validation Results:")
        print(f"  ✅ Passed: {passed}")
        print(f"  ❌ Failed: {failed}")
        print(f"  📈 Total: {passed + failed}")
        
        if failed == 0:
            print(f"\n🎉 All system validation tests passed!")
            return True
        else:
            print(f"\n⚠️  Some validation tests failed.")
            return False


if __name__ == "__main__":
    validator = TestSystemValidation()
    success = validator.run_all_tests()
    
    if not success:
        sys.exit(1)