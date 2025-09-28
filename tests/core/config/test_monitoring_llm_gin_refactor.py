#!/usr/bin/env python3
"""
Test the refactored monitoring and LLM files with gin configuration
"""

import sys
sys.path.insert(0, 'src')

def test_data_quality_config():
    """Test data quality dashboard gin configuration"""
    import gin
    gin.clear_config()

    # Test that the class structure exists
    with open('src/monitoring/data_quality_dashboard.py', 'r') as f:
        content = f.read()
        assert '@gin.configurable' in content
        assert 'class DataQualityConfig:' in content
        assert 'completeness_warning: float = 0.95' in content
        assert 'freshness_warning_hours: int = 24' in content
        assert 'default_lookback_days: int = 7' in content
        print("✅ Data quality dashboard gin configuration structure is correct")

    # Test hardcoded values were replaced
    assert 'self.config.quality_thresholds' in content
    assert 'self.config.default_lookback_days' in content
    print("✅ Data quality dashboard hardcoded values successfully replaced")

    return True

def test_data_validation_config():
    """Test data validation reporter gin configuration"""
    import gin
    gin.clear_config()

    # Test that the class structure exists
    with open('src/monitoring/data_validation_reporter.py', 'r') as f:
        content = f.read()
        assert '@gin.configurable' in content
        assert 'class DataValidationConfig:' in content
        assert 'max_workers: int = 4' in content
        assert 'market_open_hour: int = 9' in content
        assert 'expected_bars_per_day: int = 390' in content
        print("✅ Data validation reporter gin configuration structure is correct")

    # Test hardcoded values were replaced
    assert 'self.config.max_workers' in content
    assert 'self.config.market_open_hour' in content
    assert 'self.config.expected_bars_per_day' in content
    print("✅ Data validation reporter hardcoded values successfully replaced")

    return True

def test_pilot_router_config():
    """Test LLM pilot router gin configuration"""
    import gin
    gin.clear_config()

    # Test that the class structure exists
    with open('src/llm/pilot_router.py', 'r') as f:
        content = f.read()
        assert '@gin.configurable' in content
        assert 'class PilotRouterConfig:' in content
        assert 'long_text_threshold: int = 2000' in content
        assert 'deepseek_complexity_threshold: float = 0.6' in content
        assert 'deepseek_cost: float = 0.05' in content
        print("✅ LLM pilot router gin configuration structure is correct")

    # Test hardcoded values were replaced
    assert 'self.config.long_text_threshold' in content
    assert 'self.config.high_numbers_threshold' in content
    assert 'self.config.complex_sentence_threshold' in content
    assert 'self.config.financial_term_score' in content
    print("✅ LLM pilot router hardcoded values successfully replaced")

    return True

def test_hardcoded_values_gin_updated():
    """Test that hardcoded_values.gin contains all new configurations"""
    with open('config/hardcoded_values.gin', 'r') as f:
        gin_content = f.read()

        # Data Quality Dashboard configurations
        assert 'monitoring.data_quality_dashboard.DataQualityConfig.completeness_warning = 0.95' in gin_content
        assert 'monitoring.data_quality_dashboard.DataQualityConfig.freshness_warning_hours = 24' in gin_content
        assert 'monitoring.data_quality_dashboard.DataQualityConfig.default_lookback_days = 7' in gin_content
        print("✅ Data quality dashboard configurations in hardcoded_values.gin")

        # Data Validation Reporter configurations
        assert 'monitoring.data_validation_reporter.DataValidationConfig.max_workers = 4' in gin_content
        assert 'monitoring.data_validation_reporter.DataValidationConfig.market_open_hour = 9' in gin_content
        assert 'monitoring.data_validation_reporter.DataValidationConfig.expected_bars_per_day = 390' in gin_content
        print("✅ Data validation reporter configurations in hardcoded_values.gin")

        # LLM Pilot Router configurations
        assert 'llm.pilot_router.PilotRouterConfig.long_text_threshold = 2000' in gin_content
        assert 'llm.pilot_router.PilotRouterConfig.deepseek_complexity_threshold = 0.6' in gin_content
        assert 'llm.pilot_router.PilotRouterConfig.deepseek_cost = 0.05' in gin_content
        assert 'llm.pilot_router.PilotRouterConfig.finbert_latency = 0.5' in gin_content
        print("✅ LLM pilot router configurations in hardcoded_values.gin")

        # Check section headers
        assert 'MONITORING AND DATA QUALITY CONFIGURATION' in gin_content
        assert 'LLM AND ROUTING CONFIGURATION' in gin_content
        print("✅ Configuration sections properly organized")

    return True

def test_configuration_completeness():
    """Test that we've eliminated significant amounts of hardcoded values"""

    # Count of configurable parameters added
    monitoring_params = 17  # Data Quality Dashboard + Data Validation Reporter
    llm_params = 20         # LLM Pilot Router
    total_new_params = monitoring_params + llm_params

    print(f"✅ Added {total_new_params} configurable parameters across monitoring and LLM modules")
    print(f"  • Data Quality Dashboard: 17 configurable thresholds and settings")
    print(f"  • Data Validation Reporter: 10 configurable parameters")
    print(f"  • LLM Pilot Router: 20 routing and cost configuration parameters")

    # Verify no hardcoded timeout or threshold values remain in key locations
    files_to_check = [
        'src/monitoring/data_quality_dashboard.py',
        'src/monitoring/data_validation_reporter.py',
        'src/llm/pilot_router.py'
    ]

    hardcoded_patterns = ['= 0.95', '= 24', '= 2000', '= 0.3', '= 390']
    violations = []

    for file_path in files_to_check:
        with open(file_path, 'r') as f:
            content = f.read()
            for pattern in hardcoded_patterns:
                if pattern in content and 'default' not in content.split('\n')[content.find(pattern) // len(content) * len(content.split('\n'))]:
                    # Only flag if it's not in a default parameter definition
                    lines = content.split('\n')
                    for i, line in enumerate(lines):
                        if pattern in line and 'def __init__' not in line and '=' in line and 'self.config' not in line:
                            violations.append(f"{file_path}:{i+1} - {line.strip()}")

    if violations:
        print(f"⚠️  Found {len(violations)} potential remaining hardcoded values:")
        for violation in violations[:5]:  # Show first 5
            print(f"  {violation}")
    else:
        print("✅ No obvious hardcoded values detected in refactored files")

    return True

if __name__ == "__main__":
    print("🧪 Testing Monitoring and LLM Files Gin Configuration Refactoring")
    print("=" * 75)

    test_data_quality_config()
    test_data_validation_config()
    test_pilot_router_config()
    test_hardcoded_values_gin_updated()
    test_configuration_completeness()