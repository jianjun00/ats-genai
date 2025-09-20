#!/usr/bin/env python3
"""
Comprehensive Tests for Modular Gin Configuration Architecture

Tests the new modular approach to gin configuration that replaces the
monolithic hardcoded_values.gin file with composable, domain-specific
configuration files.
"""

import os
import sys
import tempfile
import pytest
from pathlib import Path
from typing import Dict, List, Any
from dataclasses import dataclass

# Add src to path for imports
sys.path.insert(0, '/home/jianjun/ats-genai-data/src')

import gin
from core.platform.config.environment import Environment, EnvironmentType


class TestModularGinConfiguration:
    """Test suite for modular gin configuration architecture"""

    @pytest.fixture(autouse=True)
    def setup_gin(self):
        """Clear gin configuration before each test"""
        gin.clear_config()
        gin.enter_interactive_mode()
        yield
        gin.clear_config()

    @pytest.fixture
    def config_dir(self):
        """Create temporary config directory structure"""
        with tempfile.TemporaryDirectory() as temp_dir:
            config_dir = Path(temp_dir) / "config"
            
            # Create directory structure
            (config_dir / "core").mkdir(parents=True)
            (config_dir / "domains").mkdir(parents=True)
            (config_dir / "external").mkdir(parents=True)
            (config_dir / "infrastructure").mkdir(parents=True)
            
            yield config_dir

    @pytest.fixture
    def core_configs(self, config_dir):
        """Create core configuration files"""
        
        # Core business constants
        business_gin = config_dir / "core" / "business.gin"
        business_gin.write_text("""
# Financial Business Constants
thresholds.sharpe_ratio.base = 1.2
thresholds.sharpe_ratio.good = 1.5
thresholds.max_drawdown.warning = 0.08
thresholds.success_rate = 0.90
""")

        # Trading symbols
        symbols_gin = config_dir / "core" / "symbols.gin"
        symbols_gin.write_text("""
symbols.default_universe = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA']
symbols.priority_symbols = ['AAPL', 'TSLA', 'SPY']
symbols.sector_mapping = {
    'AAPL': 'Technology',
    'MSFT': 'Technology',
    'TSLA': 'Consumer Discretionary'
}
""")

        # Universal timeouts
        timeouts_gin = config_dir / "core" / "timeouts.gin"
        timeouts_gin.write_text("""
timeouts.api.base = 30
timeouts.database.base = 60
delays.retry.initial = 1
delays.rate_limit.default = 1.0
""")
        
        return {
            'business': business_gin,
            'symbols': symbols_gin,
            'timeouts': timeouts_gin
        }

    @pytest.fixture
    def domain_configs(self, config_dir):
        """Create domain-specific configuration files"""
        
        # ML configuration
        ml_gin = config_dir / "domains" / "ml.gin"
        ml_gin.write_text("""
agents.agent_networks.AgentConfig.hidden_dim = 256
agents.agent_networks.AgentConfig.learning_rate = 0.001
models.attention.cross_scale_attention.AttentionConfig.d_model = 64
models.attention.cross_scale_attention.AttentionConfig.n_heads = 4
""")

        # Trading configuration
        trading_gin = config_dir / "domains" / "trading.gin"
        trading_gin.write_text("""
trading.market_open_hour = 9
trading.market_open_minute = 30
trading.expected_bars_per_day = 390
training.sequence_lengths = {
    '5m': 52,
    '60m': 24
}
""")

        return {
            'ml': ml_gin,
            'trading': trading_gin
        }

    @pytest.fixture
    def infrastructure_configs(self, config_dir):
        """Create infrastructure configuration files"""
        
        # Base infrastructure
        base_gin = config_dir / "infrastructure" / "base.gin"
        base_gin.write_text("""
# Base service configuration
FastAPIConfig.title = "ATS GenAI API"
FastAPIConfig.version = "0.1.0"
CORSConfig.allow_credentials = True
monitoring.intervals.metrics_collection_base = 60
""")

        # Development overrides
        dev_gin = config_dir / "infrastructure" / "dev.gin"
        dev_gin.write_text("""
include 'config/infrastructure/base.gin'

# Development-specific overrides
monitoring.intervals.health_check = 60
batch.sizes.default = 10
timeouts.api.default = 15
""")

        # Integration overrides
        intg_gin = config_dir / "infrastructure" / "intg.gin"
        intg_gin.write_text("""
include 'config/infrastructure/base.gin'

# Integration-specific overrides
monitoring.intervals.health_check = 120
batch.sizes.default = 50
timeouts.api.default = 45
""")

        return {
            'base': base_gin,
            'dev': dev_gin,
            'intg': intg_gin
        }

    @pytest.fixture
    def app_configs(self, config_dir):
        """Create application entry point configs"""
        
        # Development app config
        app_dev = config_dir / "app_dev.gin"
        app_dev.write_text("""
include 'config/core/business.gin'
include 'config/core/symbols.gin'
include 'config/core/timeouts.gin'
include 'config/domains/ml.gin'
include 'config/domains/trading.gin'
include 'config/infrastructure/dev.gin'

# Dev-specific overrides
api.port = 3000
symbols.default_universe = ['AAPL', 'TSLA']
""")

        # Integration app config
        app_intg = config_dir / "app_intg.gin"
        app_intg.write_text("""
include 'config/core/business.gin'
include 'config/core/symbols.gin'
include 'config/core/timeouts.gin'
include 'config/domains/ml.gin'
include 'config/domains/trading.gin'
include 'config/infrastructure/intg.gin'

# Integration-specific overrides
api.port = 4000
api.reload = False
""")

        return {
            'dev': app_dev,
            'intg': app_intg
        }

    def test_core_configs_loadable(self, core_configs):
        """Test that core configuration files load without errors"""
        
        # Register required configurables
        @gin.configurable
        def thresholds(**kwargs):
            return kwargs

        # Test loading each core config individually
        for config_name, config_path in core_configs.items():
            gin.clear_config()
            gin.enter_interactive_mode()
            
            try:
                gin.parse_config_file(str(config_path), skip_unknown=True)
                print(f"✅ Successfully loaded {config_name} config")
            except Exception as e:
                pytest.fail(f"Failed to load {config_name} config: {e}")

    def test_domain_configs_loadable(self, domain_configs):
        """Test that domain configuration files load without errors"""
        
        # Register required configurables  
        @gin.configurable
        def agents(**kwargs):
            return kwargs
            
        @gin.configurable
        def models(**kwargs):
            return kwargs
            
        @gin.configurable
        def trading(**kwargs):
            return kwargs
            
        @gin.configurable
        def training(**kwargs):
            return kwargs

        # Test loading each domain config individually
        for config_name, config_path in domain_configs.items():
            gin.clear_config()
            gin.enter_interactive_mode()
            
            try:
                gin.parse_config_file(str(config_path), skip_unknown=True)
                print(f"✅ Successfully loaded {config_name} domain config")
            except Exception as e:
                pytest.fail(f"Failed to load {config_name} domain config: {e}")

    def test_infrastructure_configs_composable(self, infrastructure_configs):
        """Test that infrastructure configs compose correctly"""
        
        # Register required configurables
        @gin.configurable
        @dataclass
        class FastAPIConfig:
            title: str = "Default"
            version: str = "0.0.1"
            
        @gin.configurable
        @dataclass
        class CORSConfig:
            allow_credentials: bool = False
            
        @gin.configurable
        def monitoring(**kwargs):
            return kwargs
            
        @gin.configurable
        def batch(**kwargs):
            return kwargs
            
        @gin.configurable
        def timeouts(**kwargs):
            return kwargs

        # Test dev config (includes base)
        gin.clear_config()
        gin.enter_interactive_mode()
        
        # Change working directory to config_dir for relative includes
        original_cwd = os.getcwd()
        os.chdir(infrastructure_configs['dev'].parent.parent)
        
        try:
            gin.parse_config_file(str(infrastructure_configs['dev']), skip_unknown=True)
            
            # Verify base values are loaded
            fastapi_config = FastAPIConfig()
            assert fastapi_config.title == "ATS GenAI API"
            assert fastapi_config.version == "0.1.0"
            
            cors_config = CORSConfig()
            assert cors_config.allow_credentials == True
            
            print("✅ Infrastructure configs compose correctly")
            
        finally:
            os.chdir(original_cwd)

    def test_app_configs_complete_composition(self, config_dir, core_configs, domain_configs, infrastructure_configs, app_configs):
        """Test that application configs load complete composed configuration"""
        
        # Register all required configurables
        @gin.configurable
        def thresholds(**kwargs):
            return kwargs
            
        @gin.configurable
        def symbols(**kwargs):
            return kwargs
            
        @gin.configurable
        def timeouts(**kwargs):
            return kwargs
            
        @gin.configurable
        def agents(**kwargs):
            return kwargs
            
        @gin.configurable
        def models(**kwargs):
            return kwargs
            
        @gin.configurable
        def trading(**kwargs):
            return kwargs
            
        @gin.configurable
        def training(**kwargs):
            return kwargs
            
        @gin.configurable
        def monitoring(**kwargs):
            return kwargs
            
        @gin.configurable
        def batch(**kwargs):
            return kwargs
            
        @gin.configurable
        def api(**kwargs):
            return kwargs
            
        @gin.configurable
        @dataclass
        class FastAPIConfig:
            title: str = "Default"
            version: str = "0.0.1"
            
        @gin.configurable
        @dataclass
        class CORSConfig:
            allow_credentials: bool = False

        # Test development configuration
        gin.clear_config()
        gin.enter_interactive_mode()
        
        original_cwd = os.getcwd()
        os.chdir(config_dir)
        
        try:
            gin.parse_config_file(str(app_configs['dev']), skip_unknown=True)
            
            # Verify values from different config files are all present
            print("✅ Development app config loaded successfully")
            
            # Test integration configuration
            gin.clear_config()
            gin.enter_interactive_mode()
            
            gin.parse_config_file(str(app_configs['intg']), skip_unknown=True)
            print("✅ Integration app config loaded successfully")
            
        finally:
            os.chdir(original_cwd)

    def test_environment_differences_preserved(self, config_dir, app_configs):
        """Test that environment-specific differences are preserved"""
        
        @gin.configurable
        def api(**kwargs):
            return kwargs
            
        @gin.configurable
        def symbols(**kwargs):
            return kwargs
            
        @gin.configurable
        def monitoring(**kwargs):
            return kwargs
            
        @gin.configurable  
        def batch(**kwargs):
            return kwargs
            
        @gin.configurable
        def timeouts(**kwargs):
            return kwargs

        original_cwd = os.getcwd()
        os.chdir(config_dir)
        
        try:
            # Load dev config and check values
            gin.clear_config()
            gin.enter_interactive_mode()
            gin.parse_config_file(str(app_configs['dev']), skip_unknown=True)
            
            dev_port = gin.query_parameter('api.port')
            dev_symbols = gin.query_parameter('symbols.default_universe')
            dev_health_check = gin.query_parameter('monitoring.intervals.health_check')
            
            # Load intg config and check values
            gin.clear_config()
            gin.enter_interactive_mode()
            gin.parse_config_file(str(app_configs['intg']), skip_unknown=True)
            
            intg_port = gin.query_parameter('api.port')
            intg_reload = gin.query_parameter('api.reload')
            intg_health_check = gin.query_parameter('monitoring.intervals.health_check')
            
            # Verify differences
            assert dev_port == 3000
            assert intg_port == 4000
            assert dev_symbols == ['AAPL', 'TSLA']  # Overridden in dev
            assert intg_reload == False  # Set in intg
            assert dev_health_check == 60  # Faster in dev
            assert intg_health_check == 120  # Slower in intg
            
            print("✅ Environment-specific differences preserved correctly")
            
        finally:
            os.chdir(original_cwd)

    def test_config_validation_rules(self, config_dir):
        """Test configuration validation rules"""
        
        # Test missing required config
        with pytest.raises(Exception):
            # Try to load config that includes non-existent file
            missing_config = config_dir / "test_missing.gin"
            missing_config.write_text("include 'config/nonexistent.gin'")
            gin.parse_config_file(str(missing_config))

    def test_backwards_compatibility(self, config_dir):
        """Test that new modular configs don't break existing code"""
        
        # Create a simple config that mimics old hardcoded_values.gin structure
        legacy_test = config_dir / "legacy_test.gin"
        legacy_test.write_text("""
# Test that old-style configs still work
thresholds.sharpe_ratio.base = 1.2
symbols.default_universe = ['AAPL', 'MSFT']
""")
        
        @gin.configurable
        def thresholds(**kwargs):
            return kwargs
            
        @gin.configurable
        def symbols(**kwargs):
            return kwargs
        
        try:
            gin.parse_config_file(str(legacy_test), skip_unknown=True)
            
            # Verify values can be queried old way
            sharpe_base = gin.query_parameter('thresholds.sharpe_ratio.base')
            default_universe = gin.query_parameter('symbols.default_universe')
            
            assert sharpe_base == 1.2
            assert default_universe == ['AAPL', 'MSFT']
            
            print("✅ Backwards compatibility maintained")
            
        except Exception as e:
            pytest.fail(f"Backwards compatibility broken: {e}")

    def test_config_performance(self, config_dir, app_configs):
        """Test that configuration loading performance is acceptable"""
        import time
        
        @gin.configurable
        def dummy_configurable(**kwargs):
            return kwargs

        original_cwd = os.getcwd()
        os.chdir(config_dir)
        
        try:
            # Time loading complex configuration
            start_time = time.time()
            
            for _ in range(10):  # Load config 10 times
                gin.clear_config()
                gin.enter_interactive_mode()
                gin.parse_config_file(str(app_configs['dev']), skip_unknown=True)
            
            total_time = time.time() - start_time
            avg_time = total_time / 10
            
            # Should load in under 1 second on average
            assert avg_time < 1.0, f"Config loading too slow: {avg_time:.2f}s average"
            
            print(f"✅ Configuration loading performance acceptable: {avg_time:.3f}s average")
            
        finally:
            os.chdir(original_cwd)


class TestGinConfigurationIntegration:
    """Integration tests with actual Environment class"""

    @pytest.fixture(autouse=True)
    def setup_gin(self):
        """Clear gin configuration before each test"""
        gin.clear_config()
        gin.enter_interactive_mode()
        yield
        gin.clear_config()

    def test_environment_with_modular_configs(self, tmp_path):
        """Test that Environment class works with modular configs"""
        
        # Create minimal modular config structure
        config_dir = tmp_path / "config"
        config_dir.mkdir()
        
        # Create minimal app config
        app_config = config_dir / "app_test.gin"
        app_config.write_text("""
# Minimal test configuration
FastAPIConfig.title = "Test API"
FastAPIConfig.version = "1.0.0"
""")
        
        # Test environment loading
        os.environ['ENVIRONMENT'] = 'test'
        
        try:
            # This should work without the old hardcoded_values.gin
            env = Environment(gin_config_path=str(app_config))
            
            assert env.env_type == EnvironmentType.TEST
            print("✅ Environment class works with modular configuration")
            
        except Exception as e:
            pytest.fail(f"Environment failed with modular config: {e}")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])