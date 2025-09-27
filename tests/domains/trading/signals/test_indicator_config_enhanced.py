from domains.trading.services.indicators_config import IndicatorConfig
# PL indicator does not exist, OneOneHigh, OneOneLow, OneOneDot, EBot, ETop, Indicator

class MockIndicator(Indicator):
    """Mock indicator for testing purposes."""

    def __init__(self, name="MockIndicator"):
        self.name = name

    def run(self, instrument_interval):
        return {"mock_value": 42.0, "status": "ok"}

class TestIndicatorConfigEnhanced:
    """Enhanced test coverage for IndicatorConfig."""

    def test_post_init_empty_initialization(self):
        """Test that __post_init__ properly handles empty initialization."""
        # Test explicit empty dict
        config = IndicatorConfig(indicators={})
        assert config.indicators == {}
        assert len(config) == 0

        # Test None initialization (should default to empty dict)
        config = IndicatorConfig()
        assert config.indicators == {}
        assert len(config) == 0

    def test_add_indicator_overwrite(self):
        """Test overwriting existing indicators."""
        config = IndicatorConfig()

        # Add initial indicator
        config.add_indicator('TestIndicator', PL)
        assert config.indicators['TestIndicator'] == PL
        assert len(config) == 1

        # Overwrite with different indicator class
        config.add_indicator('TestIndicator', OneOneDot)
        assert config.indicators['TestIndicator'] == OneOneDot
        assert len(config) == 1  # Should not increase length

    def test_add_multiple_same_class(self):
        """Test adding multiple indicators with same class but different names."""
        config = IndicatorConfig()

        config.add_indicator('PL_5min', PL)
        config.add_indicator('PL_15min', PL)
        config.add_indicator('PL_hourly', PL)

        assert len(config) == 3
        assert config.has_indicator('PL_5min')
        assert config.has_indicator('PL_15min')
        assert config.has_indicator('PL_hourly')

        # All should reference the same class
        assert config.indicators['PL_5min'] == PL
        assert config.indicators['PL_15min'] == PL
        assert config.indicators['PL_hourly'] == PL

    def test_remove_indicator_nonexistent(self):
        """Test removing non-existent indicators doesn't raise errors."""
        config = IndicatorConfig()

        # Should not raise exception
        config.remove_indicator('NonExistent')
        assert len(config) == 0

        # Add an indicator then remove different one
        config.add_indicator('PL', PL)
        config.remove_indicator('NonExistent')
        assert len(config) == 1
        assert config.has_indicator('PL')

    def test_has_indicator_case_sensitivity(self):
        """Test that has_indicator is case sensitive."""
        config = IndicatorConfig()
        config.add_indicator('PL', PL)

        assert config.has_indicator('PL') is True
        assert config.has_indicator('pl') is False
        assert config.has_indicator('Pl') is False
        assert config.has_indicator('pL') is False

    def test_get_indicator_names_order(self):
        """Test that get_indicator_names returns list in consistent order."""
        config = IndicatorConfig()

        # Add indicators in specific order
        indicators_order = ['Zebra', 'Alpha', 'Beta', 'Charlie']
        for name in indicators_order:
            config.add_indicator(name, PL)

        names = config.get_indicator_names()
        assert len(names) == 4
        assert set(names) == set(indicators_order)

        # Should be deterministic (same order each call)
        names2 = config.get_indicator_names()
        assert names == names2

    def test_create_indicator_instances_empty(self):
        """Test creating instances when no indicators configured."""
        config = IndicatorConfig()
        instances = config.create_indicator_instances()

        assert isinstance(instances, dict)
        assert len(instances) == 0

    def test_create_indicator_instances_independence(self):
        """Test that created instances are independent."""
        config = IndicatorConfig()
        config.add_indicator('Mock1', MockIndicator)
        config.add_indicator('Mock2', MockIndicator)

        instances = config.create_indicator_instances()

        # Should be different instances
        assert instances['Mock1'] is not instances['Mock2']
        assert isinstance(instances['Mock1'], MockIndicator)
        assert isinstance(instances['Mock2'], MockIndicator)

        # Each call creates new instances
        instances2 = config.create_indicator_instances()
        assert instances['Mock1'] is not instances2['Mock1']
        assert instances['Mock2'] is not instances2['Mock2']

    def test_factory_methods_immutability(self):
        """Test that factory methods return independent configurations."""
        config1 = IndicatorConfig.default_config()
        config2 = IndicatorConfig.default_config()

        # Should be separate objects
        assert config1 is not config2

        # Modifying one should not affect the other
        config1.add_indicator('Custom', MockIndicator)
        assert config1.has_indicator('Custom')
        assert not config2.has_indicator('Custom')

    def test_default_config_completeness(self):
        """Test that default_config includes all expected indicators."""
        config = IndicatorConfig.default_config()

        expected_classes = [PL, OneOneHigh, OneOneLow, OneOneDot, EBot, ETop]
        expected_names = ['PL', 'OneOneHigh', 'OneOneLow', 'OneOneDot', 'EBot', 'ETop']

        assert len(config) == len(expected_classes)

        for name, expected_class in zip(expected_names, expected_classes):
            assert config.has_indicator(name)
            assert config.indicators[name] == expected_class

    def test_basic_config_subset(self):
        """Test that basic_config is a proper subset of default_config."""
        basic = IndicatorConfig.basic_config()
        default = IndicatorConfig.default_config()

        # Basic should be smaller than default
        assert len(basic) < len(default)

        # All basic indicators should be in default
        for name in basic.get_indicator_names():
            assert default.has_indicator(name)
            assert basic.indicators[name] == default.indicators[name]

    def test_empty_config_factory(self):
        """Test empty_config factory method."""
        config = IndicatorConfig.empty_config()

        assert len(config) == 0
        assert config.get_indicator_names() == []
        assert config.create_indicator_instances() == {}
        assert not config.has_indicator('anything')

    def test_contains_operator_consistency(self):
        """Test that __contains__ is consistent with has_indicator."""
        config = IndicatorConfig()
        config.add_indicator('PL', PL)
        config.add_indicator('OneOneDot', OneOneDot)

        test_names = ['PL', 'OneOneDot', 'NonExistent', 'pl', 'EBot']

        for name in test_names:
            assert (name in config) == config.has_indicator(name)

    def test_len_consistency(self):
        """Test that __len__ is consistent with actual indicator count."""
        config = IndicatorConfig()
        assert len(config) == len(config.indicators)
        assert len(config) == len(config.get_indicator_names())

        config.add_indicator('Test1', PL)
        assert len(config) == len(config.indicators)
        assert len(config) == len(config.get_indicator_names())

        config.add_indicator('Test2', OneOneDot)
        assert len(config) == len(config.indicators)
        assert len(config) == len(config.get_indicator_names())

        config.remove_indicator('Test1')
        assert len(config) == len(config.indicators)
        assert len(config) == len(config.get_indicator_names())

    def test_iteration_completeness(self):
        """Test that iteration covers all indicators."""
        config = IndicatorConfig()
        test_indicators = {'PL': PL, 'OneOneDot': OneOneDot, 'EBot': EBot}

        for name, class_type in test_indicators.items():
            config.add_indicator(name, class_type)

        # Collect all items through iteration
        iterated_items = {}
        for name, class_type in config:
            iterated_items[name] = class_type

        # Should match exactly
        assert iterated_items == test_indicators
        assert len(iterated_items) == len(config)

    def test_edge_case_empty_string_name(self):
        """Test handling of edge case names."""
        config = IndicatorConfig()

        # Empty string name
        config.add_indicator('', PL)
        assert config.has_indicator('')
        assert len(config) == 1

        # Space-only name
        config.add_indicator('   ', OneOneDot)
        assert config.has_indicator('   ')
        assert len(config) == 2

        # Names with special characters
        config.add_indicator('indicator-with-dashes', EBot)
        config.add_indicator('indicator_with_underscores', ETop)
        config.add_indicator('indicator.with.dots', PL)

        assert config.has_indicator('indicator-with-dashes')
        assert config.has_indicator('indicator_with_underscores')
        assert config.has_indicator('indicator.with.dots')
        assert len(config) == 5

    def test_numeric_string_names(self):
        """Test handling of numeric string names."""
        config = IndicatorConfig()

        numeric_names = ['1', '2', '123', '0', '-1', '3.14']

        for name in numeric_names:
            config.add_indicator(name, PL)

        for name in numeric_names:
            assert config.has_indicator(name)

        assert len(config) == len(numeric_names)

    def test_unicode_names(self):
        """Test handling of unicode names."""
        config = IndicatorConfig()

        unicode_names = ['指标', 'индикатор', '🚀', 'café', 'naïve']

        for name in unicode_names:
            config.add_indicator(name, PL)

        for name in unicode_names:
            assert config.has_indicator(name)

        assert len(config) == len(unicode_names)

    def test_configuration_isolation(self):
        """Test that different IndicatorConfig instances are isolated."""
        config1 = IndicatorConfig()
        config2 = IndicatorConfig()

        config1.add_indicator('Shared', PL)
        config2.add_indicator('Shared', OneOneDot)  # Same name, different class

        # Should have different associations
        assert config1.indicators['Shared'] == PL
        assert config2.indicators['Shared'] == OneOneDot

        # Should not affect each other
        config1.remove_indicator('Shared')
        assert not config1.has_indicator('Shared')
        assert config2.has_indicator('Shared')

    def test_large_configuration(self):
        """Test handling of large number of indicators."""
        config = IndicatorConfig()

        # Add many indicators
        for i in range(1000):
            config.add_indicator(f'indicator_{i}', PL)

        assert len(config) == 1000
        assert len(config.get_indicator_names()) == 1000

        # Test random access
        assert config.has_indicator('indicator_500')
        assert config.has_indicator('indicator_999')
        assert not config.has_indicator('indicator_1000')

        # Test instances creation (might be slow but should work)
        instances = config.create_indicator_instances()
        assert len(instances) == 1000