"""
Comprehensive tests for Enhanced Feature Type System
"""

import pytest
import json
import tempfile
import os
from pathlib import Path

from src.modeling.enhanced_feature_types import (
    FeatureType, TimeframeSpec, TechnicalIndicator,
    FeatureSpecification, VisualizationMetadata,
    EnhancedFeatureRegistry
)


class TestFeatureType:
    """Test FeatureType enum."""
    
    def test_feature_type_values(self):
        """Test that all feature types have correct values."""
        assert FeatureType.OHLC_INTERVALS.value == "ohlc_intervals"
        assert FeatureType.PRICE_INDICATOR_INTERVALS.value == "price_indicator_intervals"
        assert FeatureType.CROSS_TIMEFRAME_INDICATORS.value == "cross_timeframe_indicators"
    
    def test_feature_type_count(self):
        """Test expected number of feature types."""
        assert len(FeatureType) == 7  # Update if adding more types


class TestTimeframeSpec:
    """Test TimeframeSpec enum with multipliers."""
    
    def test_timeframe_multipliers(self):
        """Test timeframe multiplier calculations."""
        assert TimeframeSpec.MINUTE_5.multiplier == 1
        assert TimeframeSpec.MINUTE_15.multiplier == 3
        assert TimeframeSpec.HOUR_1.multiplier == 12
        assert TimeframeSpec.DAILY.multiplier == 288
        assert TimeframeSpec.WEEKLY.multiplier == 2016
        assert TimeframeSpec.MONTHLY.multiplier == 8640
    
    def test_timeframe_labels(self):
        """Test timeframe label values."""
        assert TimeframeSpec.MINUTE_5.label == "5min"
        assert TimeframeSpec.MINUTE_15.label == "15min"
        assert TimeframeSpec.HOUR_1.label == "1hour"
        assert TimeframeSpec.DAILY.label == "daily"


class TestTechnicalIndicator:
    """Test TechnicalIndicator enum."""
    
    def test_indicator_properties(self):
        """Test technical indicator properties."""
        etop = TechnicalIndicator.ETOP
        assert etop.code == "etop"
        assert etop.display_name == "Envelope Top"
        assert etop.color == "#FF5722"
        assert etop.category == "resistance"
    
    def test_all_indicators_have_colors(self):
        """Test that all indicators have color assignments."""
        for indicator in TechnicalIndicator:
            assert indicator.color.startswith("#")
            assert len(indicator.color) == 7  # #RRGGBB format


class TestVisualizationMetadata:
    """Test VisualizationMetadata dataclass."""
    
    def test_default_visualization_metadata(self):
        """Test default visualization metadata creation."""
        viz = VisualizationMetadata(type="candlestick_sequence")
        
        assert viz.type == "candlestick_sequence"
        assert viz.opacity == 1.0
        assert viz.line_width == 2
        assert viz.y_axis == "price"
        assert viz.layer == 1
    
    def test_custom_visualization_metadata(self):
        """Test custom visualization metadata."""
        viz = VisualizationMetadata(
            type="line_overlay",
            color="#FF0000",
            opacity=0.8,
            line_width=3,
            layer=2
        )
        
        assert viz.color == "#FF0000"
        assert viz.opacity == 0.8
        assert viz.line_width == 3
        assert viz.layer == 2
    
    def test_to_dict_serialization(self):
        """Test conversion to dictionary."""
        viz = VisualizationMetadata(
            type="line_overlay",
            color="#FF0000",
            opacity=0.7
        )
        
        result = viz.to_dict()
        expected_keys = {"type", "color", "opacity", "line_width", "marker_size", 
                        "show_volume", "y_axis", "layer"}
        
        assert set(result.keys()) == expected_keys
        assert result["type"] == "line_overlay"
        assert result["color"] == "#FF0000"
        assert result["opacity"] == 0.7


class TestFeatureSpecification:
    """Test FeatureSpecification dataclass."""
    
    def test_ohlc_feature_specification(self):
        """Test OHLC feature specification creation."""
        spec = FeatureSpecification(
            name="ohlc_5min_8",
            feature_type=FeatureType.OHLC_INTERVALS,
            timeframe=TimeframeSpec.MINUTE_5,
            intervals=8,
            dimensions=(8, 4),
            description="8 intervals of 5-minute OHLC data"
        )
        
        assert spec.name == "ohlc_5min_8"
        assert spec.feature_type == FeatureType.OHLC_INTERVALS
        assert spec.timeframe == TimeframeSpec.MINUTE_5
        assert spec.intervals == 8
        assert spec.dimensions == (8, 4)
        assert spec.indicator_type is None
        assert spec.source_timeframe is None
    
    def test_indicator_feature_specification(self):
        """Test technical indicator feature specification."""
        spec = FeatureSpecification(
            name="etop_15min_16",
            feature_type=FeatureType.PRICE_INDICATOR_INTERVALS,
            timeframe=TimeframeSpec.MINUTE_15,
            intervals=16,
            dimensions=(16, 1),
            indicator_type=TechnicalIndicator.ETOP
        )
        
        assert spec.indicator_type == TechnicalIndicator.ETOP
        assert spec.dimensions == (16, 1)
        assert spec.visualization.type == "line_overlay"
        assert spec.visualization.color == TechnicalIndicator.ETOP.color
    
    def test_cross_timeframe_feature_specification(self):
        """Test cross-timeframe feature specification."""
        spec = FeatureSpecification(
            name="etop_1hour_on_5min",
            feature_type=FeatureType.CROSS_TIMEFRAME_INDICATORS,
            timeframe=TimeframeSpec.MINUTE_5,
            intervals=16,
            dimensions=(16, 1),
            indicator_type=TechnicalIndicator.ETOP,
            source_timeframe=TimeframeSpec.HOUR_1
        )
        
        assert spec.source_timeframe == TimeframeSpec.HOUR_1
        assert spec.timeframe == TimeframeSpec.MINUTE_5
        assert spec.visualization.type == "cross_timeframe_overlay"
        assert spec.visualization.opacity == 0.6
    
    def test_default_visualization_creation(self):
        """Test automatic visualization metadata creation."""
        # OHLC feature should get candlestick visualization
        ohlc_spec = FeatureSpecification(
            name="test_ohlc",
            feature_type=FeatureType.OHLC_INTERVALS,
            timeframe=TimeframeSpec.MINUTE_5,
            intervals=8,
            dimensions=(8, 4)
        )
        
        assert ohlc_spec.visualization.type == "candlestick_sequence"
        assert ohlc_spec.visualization.show_volume is True
        
        # Indicator feature should get line overlay
        ind_spec = FeatureSpecification(
            name="test_etop",
            feature_type=FeatureType.PRICE_INDICATOR_INTERVALS,
            timeframe=TimeframeSpec.MINUTE_5,
            intervals=8,
            dimensions=(8, 1),
            indicator_type=TechnicalIndicator.ETOP
        )
        
        assert ind_spec.visualization.type == "line_overlay"
        assert ind_spec.visualization.color == TechnicalIndicator.ETOP.color
    
    def test_to_dict_serialization(self):
        """Test feature specification serialization."""
        spec = FeatureSpecification(
            name="etop_5min_8",
            feature_type=FeatureType.PRICE_INDICATOR_INTERVALS,
            timeframe=TimeframeSpec.MINUTE_5,
            intervals=8,
            dimensions=(8, 1),
            indicator_type=TechnicalIndicator.ETOP,
            description="Test ETOP feature"
        )
        
        result = spec.to_dict()
        
        assert result["name"] == "etop_5min_8"
        assert result["feature_type"] == "price_indicator_intervals"
        assert result["timeframe"] == "5min"
        assert result["intervals"] == 8
        assert result["dimensions"] == [8, 1]
        assert result["indicator_type"] == "etop"
        assert result["description"] == "Test ETOP feature"
        assert "visualization" in result
    
    def test_from_dict_deserialization(self):
        """Test feature specification deserialization."""
        data = {
            "name": "etop_5min_8",
            "feature_type": "price_indicator_intervals",
            "timeframe": "5min",
            "intervals": 8,
            "dimensions": [8, 1],
            "indicator_type": "etop",
            "description": "Test ETOP feature",
            "visualization": {
                "type": "line_overlay",
                "color": "#FF5722",
                "opacity": 0.8,
                "line_width": 2,
                "marker_size": 4,
                "show_volume": False,
                "y_axis": "price",
                "layer": 2
            },
            "metadata": {"test": "value"}
        }
        
        spec = FeatureSpecification.from_dict(data)
        
        assert spec.name == "etop_5min_8"
        assert spec.feature_type == FeatureType.PRICE_INDICATOR_INTERVALS
        assert spec.timeframe == TimeframeSpec.MINUTE_5
        assert spec.intervals == 8
        assert spec.dimensions == (8, 1)
        assert spec.indicator_type == TechnicalIndicator.ETOP
        assert spec.description == "Test ETOP feature"
        assert spec.visualization.color == "#FF5722"
        assert spec.metadata["test"] == "value"
    
    def test_roundtrip_serialization(self):
        """Test roundtrip serialization (to_dict -> from_dict)."""
        original = FeatureSpecification(
            name="test_feature",
            feature_type=FeatureType.CROSS_TIMEFRAME_INDICATORS,
            timeframe=TimeframeSpec.MINUTE_15,
            intervals=16,
            dimensions=(16, 1),
            indicator_type=TechnicalIndicator.PLDOT,
            source_timeframe=TimeframeSpec.HOUR_1,
            description="Test cross-timeframe feature",
            metadata={"param1": "value1", "param2": 42}
        )
        
        # Serialize and deserialize
        data = original.to_dict()
        reconstructed = FeatureSpecification.from_dict(data)
        
        # Compare key attributes
        assert reconstructed.name == original.name
        assert reconstructed.feature_type == original.feature_type
        assert reconstructed.timeframe == original.timeframe
        assert reconstructed.intervals == original.intervals
        assert reconstructed.dimensions == original.dimensions
        assert reconstructed.indicator_type == original.indicator_type
        assert reconstructed.source_timeframe == original.source_timeframe
        assert reconstructed.description == original.description
        assert reconstructed.metadata == original.metadata


class TestEnhancedFeatureRegistry:
    """Test EnhancedFeatureRegistry class."""
    
    @pytest.fixture
    def empty_registry(self):
        """Create empty feature registry for testing."""
        registry = EnhancedFeatureRegistry()
        registry.feature_specs.clear()  # Start with empty registry
        return registry
    
    @pytest.fixture
    def sample_registry(self):
        """Create registry with sample features."""
        return EnhancedFeatureRegistry()  # Uses default initialization
    
    def test_registry_initialization(self, sample_registry):
        """Test that registry initializes with default features."""
        assert len(sample_registry.feature_specs) > 0
        
        # Should have OHLC features
        ohlc_features = sample_registry.list_features_by_type(FeatureType.OHLC_INTERVALS)
        assert len(ohlc_features) > 0
        
        # Should have indicator features  
        ind_features = sample_registry.list_features_by_type(FeatureType.PRICE_INDICATOR_INTERVALS)
        assert len(ind_features) > 0
        
        # Should have cross-timeframe features
        cross_features = sample_registry.list_features_by_type(FeatureType.CROSS_TIMEFRAME_INDICATORS)
        assert len(cross_features) > 0
    
    def test_register_custom_feature(self, empty_registry):
        """Test registering custom features."""
        spec = FeatureSpecification(
            name="custom_ohlc",
            feature_type=FeatureType.OHLC_INTERVALS,
            timeframe=TimeframeSpec.MINUTE_5,
            intervals=10,
            dimensions=(10, 4)
        )
        
        empty_registry.register_feature(spec)
        
        assert len(empty_registry.feature_specs) == 1
        retrieved = empty_registry.get_feature_spec("custom_ohlc")
        assert retrieved == spec
    
    def test_get_nonexistent_feature(self, sample_registry):
        """Test getting non-existent feature returns None."""
        result = sample_registry.get_feature_spec("nonexistent_feature")
        assert result is None
    
    def test_list_features_by_type(self, sample_registry):
        """Test filtering features by type."""
        ohlc_features = sample_registry.list_features_by_type(FeatureType.OHLC_INTERVALS)
        
        assert len(ohlc_features) > 0
        for feature in ohlc_features:
            assert feature.feature_type == FeatureType.OHLC_INTERVALS
            assert feature.dimensions[1] == 4  # OHLC has 4 columns
    
    def test_list_features_by_timeframe(self, sample_registry):
        """Test filtering features by timeframe."""
        min5_features = sample_registry.list_features_by_timeframe(TimeframeSpec.MINUTE_5)
        
        assert len(min5_features) > 0
        for feature in min5_features:
            assert feature.timeframe == TimeframeSpec.MINUTE_5
    
    def test_list_indicator_features(self, sample_registry):
        """Test filtering features by technical indicator."""
        etop_features = sample_registry.list_indicator_features(TechnicalIndicator.ETOP)
        
        assert len(etop_features) > 0
        for feature in etop_features:
            assert feature.indicator_type == TechnicalIndicator.ETOP
    
    def test_get_cross_timeframe_features(self, sample_registry):
        """Test getting cross-timeframe features."""
        cross_features = sample_registry.get_cross_timeframe_features()
        
        assert len(cross_features) > 0
        for feature in cross_features:
            assert feature.feature_type == FeatureType.CROSS_TIMEFRAME_INDICATORS
            assert feature.source_timeframe is not None
    
    def test_export_and_load_registry(self, sample_registry):
        """Test exporting and loading feature registry."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            temp_file = f.name
        
        try:
            # Export registry
            sample_registry.export_registry(temp_file)
            assert os.path.exists(temp_file)
            
            # Verify export file structure
            with open(temp_file, 'r') as f:
                data = json.load(f)
            
            assert "features" in data
            assert "metadata" in data
            assert len(data["features"]) == len(sample_registry.feature_specs)
            
            # Create new registry and load
            new_registry = EnhancedFeatureRegistry()
            new_registry.feature_specs.clear()  # Clear defaults
            new_registry.load_registry(temp_file)
            
            # Compare registries
            assert len(new_registry.feature_specs) == len(sample_registry.feature_specs)
            
            # Test a few specific features
            for name in ["ohlc_5min_8", "etop_15min_16"]:
                original = sample_registry.get_feature_spec(name)
                loaded = new_registry.get_feature_spec(name)
                if original and loaded:  # Both should exist
                    assert loaded.name == original.name
                    assert loaded.feature_type == original.feature_type
                    assert loaded.timeframe == original.timeframe
        
        finally:
            # Cleanup
            if os.path.exists(temp_file):
                os.unlink(temp_file)
    
    def test_get_visualization_config(self, sample_registry):
        """Test getting visualization configuration for multiple features."""
        feature_names = ["ohlc_5min_8", "etop_5min_16", "pldot_1hour_on_5min"]
        
        config = sample_registry.get_visualization_config(feature_names)
        
        assert "timeframes" in config
        assert "features" in config
        assert "overlays" in config
        
        # Should have timeframe info
        assert "5min" in config["timeframes"]
        
        # Should have feature configs
        for name in feature_names:
            if sample_registry.get_feature_spec(name):  # Only if feature exists
                assert name in config["features"]
        
        # Should have overlay info for cross-timeframe features
        cross_features = [name for name in feature_names 
                         if name.find("_on_") > 0]  # Cross-timeframe naming pattern
        if cross_features:
            assert len(config["overlays"]) > 0
    
    def test_specific_default_features(self, sample_registry):
        """Test that specific expected default features exist."""
        # Test key OHLC features
        assert sample_registry.get_feature_spec("ohlc_5min_8") is not None
        assert sample_registry.get_feature_spec("ohlc_15min_16") is not None
        assert sample_registry.get_feature_spec("ohlc_1hour_32") is not None
        
        # Test key indicator features
        assert sample_registry.get_feature_spec("etop_5min_8") is not None
        assert sample_registry.get_feature_spec("ebot_15min_16") is not None
        assert sample_registry.get_feature_spec("pldot_1hour_32") is not None
        
        # Test cross-timeframe features
        assert sample_registry.get_feature_spec("etop_1hour_on_5min") is not None
        assert sample_registry.get_feature_spec("pldot_daily_on_15min") is not None
    
    def test_feature_naming_convention(self, sample_registry):
        """Test that features follow expected naming conventions."""
        all_features = sample_registry.feature_specs.values()
        
        for feature in all_features:
            name = feature.name
            
            if feature.feature_type == FeatureType.OHLC_INTERVALS:
                # Should be: ohlc_{timeframe}_{intervals}
                assert name.startswith("ohlc_")
                assert f"_{feature.intervals}" in name
            
            elif feature.feature_type == FeatureType.PRICE_INDICATOR_INTERVALS:
                # Should be: {indicator}_{timeframe}_{intervals}
                if feature.indicator_type:
                    assert name.startswith(feature.indicator_type.code + "_")
                    assert f"_{feature.intervals}" in name
            
            elif feature.feature_type == FeatureType.CROSS_TIMEFRAME_INDICATORS:
                # Should be: {indicator}_{source_timeframe}_on_{target_timeframe}
                assert "_on_" in name
                if feature.source_timeframe and feature.indicator_type:
                    expected_start = f"{feature.indicator_type.code}_{feature.source_timeframe.label}_on_"
                    assert name.startswith(expected_start)
    
    def test_feature_dimensions_consistency(self, sample_registry):
        """Test that feature dimensions are consistent with their types."""
        for feature in sample_registry.feature_specs.values():
            if feature.feature_type == FeatureType.OHLC_INTERVALS:
                # OHLC should be [intervals, 4]
                assert len(feature.dimensions) == 2
                assert feature.dimensions[0] == feature.intervals
                assert feature.dimensions[1] == 4
            
            elif feature.feature_type in [FeatureType.PRICE_INDICATOR_INTERVALS, 
                                         FeatureType.CROSS_TIMEFRAME_INDICATORS]:
                # Indicators should be [intervals, 1]
                assert len(feature.dimensions) == 2
                assert feature.dimensions[0] == feature.intervals
                assert feature.dimensions[1] == 1


class TestIntegration:
    """Integration tests for the complete feature type system."""
    
    def test_end_to_end_feature_workflow(self):
        """Test complete workflow from registry creation to export."""
        # Create registry
        registry = EnhancedFeatureRegistry()
        
        # Verify it has default features
        assert len(registry.feature_specs) > 50  # Should have many default features
        
        # Add custom feature
        custom_spec = FeatureSpecification(
            name="custom_rsi_daily_20",
            feature_type=FeatureType.PRICE_INDICATOR_INTERVALS,
            timeframe=TimeframeSpec.DAILY,
            intervals=20,
            dimensions=(20, 1),
            indicator_type=TechnicalIndicator.RSI,
            description="20-day RSI indicator"
        )
        registry.register_feature(custom_spec)
        
        # Test filtering and retrieval
        rsi_features = registry.list_indicator_features(TechnicalIndicator.RSI)
        assert len(rsi_features) > 0
        
        daily_features = registry.list_features_by_timeframe(TimeframeSpec.DAILY)
        assert len(daily_features) > 0
        
        # Test visualization config generation
        test_features = ["ohlc_5min_8", "etop_5min_16", "custom_rsi_daily_20"]
        viz_config = registry.get_visualization_config(test_features)
        
        assert "5min" in viz_config["timeframes"]
        assert "daily" in viz_config["timeframes"]
        assert "custom_rsi_daily_20" in viz_config["features"]
        
        # Test export/import roundtrip
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            temp_file = f.name
        
        try:
            registry.export_registry(temp_file)
            
            new_registry = EnhancedFeatureRegistry()
            new_registry.feature_specs.clear()
            new_registry.load_registry(temp_file)
            
            # Verify custom feature survived roundtrip
            loaded_custom = new_registry.get_feature_spec("custom_rsi_daily_20")
            assert loaded_custom is not None
            assert loaded_custom.indicator_type == TechnicalIndicator.RSI
            assert loaded_custom.intervals == 20
        
        finally:
            if os.path.exists(temp_file):
                os.unlink(temp_file)


if __name__ == "__main__":
    # Run tests manually for debugging
    import sys
    
    test_classes = [
        TestFeatureType, TestTimeframeSpec, TestTechnicalIndicator,
        TestVisualizationMetadata, TestFeatureSpecification, 
        TestEnhancedFeatureRegistry, TestIntegration
    ]
    
    passed = 0
    failed = 0
    
    for test_class in test_classes:
        print(f"\n=== Running {test_class.__name__} ===")
        
        # Get test methods
        test_methods = [method for method in dir(test_class) 
                       if method.startswith('test_')]
        
        for method_name in test_methods:
            try:
                # Create instance and run test
                instance = test_class()
                
                # Handle fixture dependencies (simplified)
                if hasattr(instance, method_name):
                    method = getattr(instance, method_name)
                    
                    # Simple fixture handling
                    if 'empty_registry' in method.__code__.co_varnames:
                        empty_reg = EnhancedFeatureRegistry()
                        empty_reg.feature_specs.clear()
                        method(empty_reg)
                    elif 'sample_registry' in method.__code__.co_varnames:
                        sample_reg = EnhancedFeatureRegistry()
                        method(sample_reg)
                    else:
                        method()
                    
                    print(f"  ✓ {method_name}")
                    passed += 1
                    
            except Exception as e:
                print(f"  ✗ {method_name}: {str(e)}")
                failed += 1
    
    print(f"\n=== Test Summary ===")
    print(f"Passed: {passed}")
    print(f"Failed: {failed}")
    print(f"Total: {passed + failed}")
    
    if failed > 0:
        sys.exit(1)