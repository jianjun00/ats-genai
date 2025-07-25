import pytest
from signals.indicator_config import IndicatorConfig
from signals.indicator import PL, OneOneHigh, OneOneLow, OneOneDot, EBot, ETop


def test_indicator_config_init():
    """Test IndicatorConfig initialization."""
    # Test empty initialization
    config = IndicatorConfig()
    assert len(config.indicators) == 0
    assert len(config) == 0
    
    # Test initialization with indicators
    indicators = {'PL': PL, 'OneOneDot': OneOneDot}
    config = IndicatorConfig(indicators=indicators)
    assert len(config.indicators) == 2
    assert config.has_indicator('PL')
    assert config.has_indicator('OneOneDot')


def test_add_remove_indicator():
    """Test adding and removing indicators."""
    config = IndicatorConfig()
    
    # Add indicators
    config.add_indicator('PL', PL)
    config.add_indicator('OneOneLow', OneOneLow)
    
    assert len(config) == 2
    assert config.has_indicator('PL')
    assert config.has_indicator('OneOneLow')
    assert 'PL' in config
    assert 'OneOneLow' in config
    
    # Remove indicator
    config.remove_indicator('PL')
    assert len(config) == 1
    assert not config.has_indicator('PL')
    assert config.has_indicator('OneOneLow')
    
    # Remove non-existent indicator (should not error)
    config.remove_indicator('NonExistent')
    assert len(config) == 1


def test_get_indicator_names():
    """Test getting indicator names."""
    config = IndicatorConfig()
    config.add_indicator('PL', PL)
    config.add_indicator('OneOneHigh', OneOneHigh)
    config.add_indicator('OneOneLow', OneOneLow)
    
    names = config.get_indicator_names()
    assert len(names) == 3
    assert 'PL' in names
    assert 'OneOneHigh' in names
    assert 'OneOneLow' in names


def test_create_indicator_instances():
    """Test creating indicator instances."""
    config = IndicatorConfig()
    config.add_indicator('PL', PL)
    config.add_indicator('OneOneDot', OneOneDot)
    
    instances = config.create_indicator_instances()
    assert len(instances) == 2
    assert 'PL' in instances
    assert 'OneOneDot' in instances
    assert isinstance(instances['PL'], PL)
    assert isinstance(instances['OneOneDot'], OneOneDot)
    
    # Each call should create new instances
    instances2 = config.create_indicator_instances()
    assert instances['PL'] is not instances2['PL']
    assert instances['OneOneDot'] is not instances2['OneOneDot']


def test_default_config():
    """Test default configuration."""
    config = IndicatorConfig.default_config()
    
    expected_indicators = ['PL', 'OneOneHigh', 'OneOneLow', 'OneOneDot', 'EBot', 'ETop']
    assert len(config) == len(expected_indicators)
    
    for indicator_name in expected_indicators:
        assert config.has_indicator(indicator_name)
    
    # Test that instances can be created
    instances = config.create_indicator_instances()
    assert len(instances) == len(expected_indicators)


def test_basic_config():
    """Test basic configuration."""
    config = IndicatorConfig.basic_config()
    
    expected_indicators = ['OneOneDot', 'OneOneHigh', 'OneOneLow']
    assert len(config) == len(expected_indicators)
    
    for indicator_name in expected_indicators:
        assert config.has_indicator(indicator_name)
    
    # Should not have complex indicators
    assert not config.has_indicator('PL')
    assert not config.has_indicator('EBot')
    assert not config.has_indicator('ETop')


def test_empty_config():
    """Test empty configuration."""
    config = IndicatorConfig.empty_config()
    
    assert len(config) == 0
    assert len(config.get_indicator_names()) == 0
    assert len(config.create_indicator_instances()) == 0


def test_iteration():
    """Test iterating over configuration."""
    config = IndicatorConfig()
    config.add_indicator('PL', PL)
    config.add_indicator('OneOneLow', OneOneLow)
    
    items = list(config)
    assert len(items) == 2
    
    names, classes = zip(*items)
    assert 'PL' in names
    assert 'OneOneLow' in names
    assert PL in classes
    assert OneOneLow in classes


def test_contains_operator():
    """Test __contains__ operator."""
    config = IndicatorConfig()
    config.add_indicator('PL', PL)
    
    assert 'PL' in config
    assert 'NonExistent' not in config


def test_len_operator():
    """Test __len__ operator."""
    config = IndicatorConfig()
    assert len(config) == 0
    
    config.add_indicator('PL', PL)
    assert len(config) == 1
    
    config.add_indicator('OneOneDot', OneOneDot)
    assert len(config) == 2
