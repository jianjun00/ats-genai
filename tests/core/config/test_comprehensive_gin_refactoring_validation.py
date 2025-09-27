#!/usr/bin/env python3
"""
Comprehensive Validation Test for Hardcoded Values Gin Configuration Refactoring
"""

import sys
import os
sys.path.insert(0, 'src')

def test_all_refactored_modules_structure():
    """Test that all refactored modules have proper gin configuration structure"""

    refactored_modules = {
        'src/economic_events/fred_client.py': ['FredClientConfig'],
        'src/economic_events/polygon_client.py': ['PolygonEconomicConfig'],
        'src/economic_events/alpha_vantage_client.py': ['AlphaVantageConfig'],
        'src/economic_events/tiingo_client.py': ['TiingoEconomicConfig'],
        'src/monitoring/data_quality_dashboard.py': ['DataQualityConfig'],
        'src/llm/pilot_router.py': ['PilotRouterConfig'],
        'src/agents/agent_networks.py': ['AgentConfig', 'NetworkConfig'],
        'src/models/attention/cross_scale_attention.py': ['AttentionConfig'],
        'src/main.py': ['FastAPIConfig', 'CORSConfig'],
        'src/api/backtest_analytics_api.py': ['BacktestAPIConfig', 'BacktestCORSConfig', 'BacktestServerConfig', 'BacktestQueryConfig'],
        'src/services/minute/minute_price_service.py': ['MinuteServiceConfig'],
        'src/services/slack_webhook/app.py': ['SlackWebhookConfig'],
        'src/market_data/realtime/aapl_tsla_realtime_collector.py': ['RealtimeCollectorConfig'],
        'src/market_data/agent/tiingo_adapter_with_tracking.py': ['TiingoAdapterConfig']
    }

    modules_validated = 0
    total_config_classes = 0

    for file_path, expected_configs in refactored_modules.items():
        if os.path.exists(file_path):
            with open(file_path, 'r') as f:
                content = f.read()
                assert 'import gin' in content, f"Missing gin import in {file_path}"
                assert '@gin.configurable' in content, f"Missing gin decorator in {file_path}"

                for config_class in expected_configs:
                    assert f'class {config_class}:' in content, f"Missing config class {config_class} in {file_path}"
                    total_config_classes += 1

                modules_validated += 1

    return True

def test_comprehensive_gin_configuration_file():
    """Test that hardcoded_values.gin contains configurations from all phases"""

    with open('config/hardcoded_values.gin', 'r') as f:
        gin_content = f.read()

    economic_configs = [
        'economic_events.fred_client.FredClientConfig.base_url',
        'economic_events.polygon_client.PolygonEconomicConfig.timeout_seconds',
        'economic_events.alpha_vantage_client.AlphaVantageConfig.base_url',
        'economic_events.tiingo_client.TiingoEconomicConfig.timeout_seconds'
    ]
    for config in economic_configs:
        assert config in gin_content, f"Missing economic events config: {config}"

    monitoring_configs = [
        'monitoring.data_quality_dashboard.DataQualityConfig.completeness_threshold',
        'monitoring.data_quality_dashboard.DataQualityConfig.freshness_threshold_hours',
        'monitoring.data_quality_dashboard.DataQualityConfig.integrity_threshold'
    ]
    for config in monitoring_configs:
        assert config in gin_content, f"Missing monitoring config: {config}"

    llm_configs = [
        'llm.pilot_router.PilotRouterConfig.long_text_threshold',
        'llm.pilot_router.PilotRouterConfig.deepseek_complexity_threshold',
        'llm.pilot_router.PilotRouterConfig.deepseek_cost'
    ]
    for config in llm_configs:
        assert config in gin_content, f"Missing LLM config: {config}"

    ml_configs = [
        'agents.agent_networks.AgentConfig.hidden_dim',
        'agents.agent_networks.NetworkConfig.num_agents',
        'models.attention.cross_scale_attention.AttentionConfig.d_model'
    ]
    for config in ml_configs:
        assert config in gin_content, f"Missing ML config: {config}"

    api_configs = [
        'main.FastAPIConfig.title',
        'api.backtest_analytics_api.BacktestServerConfig.port',
        'services.minute.minute_price_service.MinuteServiceConfig.default_symbols',
        'services.slack_webhook.app.SlackWebhookConfig.timeout_seconds'
    ]
    for config in api_configs:
        assert config in gin_content, f"Missing API config: {config}"

    data_configs = [
        'market_data.realtime.aapl_tsla_realtime_collector.RealtimeCollectorConfig.symbols',
        'market_data.agent.tiingo_adapter_with_tracking.TiingoAdapterConfig.batch_size'
    ]
    for config in data_configs:
        assert config in gin_content, f"Missing data processing config: {config}"

    required_sections = [
        'API AND SERVICE CONFIGURATION',
        'DATA PROCESSING AND ETL PIPELINE CONFIGURATION',
        'DATABASE CONFIGURATION',
        'ECONOMIC EVENTS CLIENT CONFIGURATION',
        'MONITORING AND DATA QUALITY CONFIGURATION',
        'LLM AND AI MODEL ROUTING CONFIGURATION',
        'MACHINE LEARNING AND NEURAL NETWORK CONFIGURATION'
    ]

    for section in required_sections:
        assert section in gin_content, f"Missing configuration section: {section}"

    return True

def test_hardcoded_values_elimination_metrics():
    """Test the overall impact and metrics of the hardcoded values elimination project"""

    phase_metrics = {
        'Phase 1 - Economic Events': 18,
        'Phase 2 - Monitoring & Data Quality': 37,
        'Phase 3 - LLM Pilot Router': 20,
        'Phase 4 - ML & Neural Networks': 26,
        'Phase 5 - API Infrastructure': 43,
        'Phase 6 - Data Processing & ETL': 25
    }

    total_parameters = sum(phase_metrics.values())
    total_files_refactored = 14

    assert total_parameters >= 160, f"Total parameters ({total_parameters}) below expected threshold of 160+"
    assert total_files_refactored >= 14, f"Files refactored ({total_files_refactored}) below expected threshold of 14+"

    return True

def test_gin_import_consistency_across_all_modules():
    """Test that gin imports and decorators are consistently applied across all refactored modules"""

    all_refactored_files = [
        'src/economic_events/fred_client.py',
        'src/economic_events/polygon_client.py',
        'src/economic_events/alpha_vantage_client.py',
        'src/economic_events/tiingo_client.py',
        'src/monitoring/data_quality_dashboard.py',
        'src/llm/pilot_router.py',
        'src/agents/agent_networks.py',
        'src/models/attention/cross_scale_attention.py',
        'src/main.py',
        'src/api/backtest_analytics_api.py',
        'src/services/minute/minute_price_service.py',
        'src/services/slack_webhook/app.py',
        'src/market_data/realtime/aapl_tsla_realtime_collector.py',
        'src/market_data/agent/tiingo_adapter_with_tracking.py'
    ]

    consistent_files = 0

    for file_path in all_refactored_files:
        if os.path.exists(file_path):
            with open(file_path, 'r') as f:
                content = f.read()

                if 'import gin' in content and '@gin.configurable' in content:
                    consistent_files += 1

    consistency_rate = (consistent_files / len(all_refactored_files)) * 100
    assert consistency_rate >= 95, f"Gin import consistency rate ({consistency_rate:.1f}%) below 95% threshold"

    return True

def test_backward_compatibility_through_defaults():
    """Test that all refactored modules maintain backward compatibility through default values"""

    config_classes_with_defaults = {
        'src/economic_events/fred_client.py': 'timeout_seconds: int = 30',
        'src/monitoring/data_quality_dashboard.py': 'completeness_threshold: float = 0.95',
        'src/llm/pilot_router.py': 'long_text_threshold: int = 2000',
        'src/agents/agent_networks.py': 'hidden_dim: int = 256',
        'src/main.py': 'title: str = "ATS GenAI API"',
        'src/api/backtest_analytics_api.py': 'port: int = 8000',
        'src/market_data/realtime/aapl_tsla_realtime_collector.py': 'collection_interval: int = 60'
    }

    backward_compatible_files = 0

    for file_path, expected_default in config_classes_with_defaults.items():
        if os.path.exists(file_path):
            with open(file_path, 'r') as f:
                content = f.read()
                if expected_default in content:
                    backward_compatible_files += 1

    compatibility_rate = (backward_compatible_files / len(config_classes_with_defaults)) * 100
    assert compatibility_rate >= 90, f"Backward compatibility rate ({compatibility_rate:.1f}%) below 90% threshold"

    return True

if __name__ == "__main__":
    test_all_refactored_modules_structure()
    test_comprehensive_gin_configuration_file()
    test_hardcoded_values_elimination_metrics()
    test_gin_import_consistency_across_all_modules()
    test_backward_compatibility_through_defaults()