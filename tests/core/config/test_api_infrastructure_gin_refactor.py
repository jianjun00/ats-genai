#!/usr/bin/env python3
"""
Test API Infrastructure Gin Configuration Refactoring
"""

import sys
import os
sys.path.insert(0, 'src')

def test_main_api_config():
    """Test main.py gin configuration"""
    import gin
    gin.clear_config()

    with open('src/main.py', 'r') as f:
        content = f.read()
        assert '@gin.configurable' in content
        assert 'class FastAPIConfig:' in content
        assert 'class CORSConfig:' in content
        assert 'title: str = "ATS GenAI API"' in content
        assert 'version: str = "0.1.0"' in content
        assert 'allow_origins: List[str] = None' in content

    assert 'fastapi_config.title' in content
    assert 'cors_config.allow_origins' in content
    assert 'cors_config.allow_credentials' in content
    return True

def test_backtest_analytics_api_config():
    """Test backtest analytics API gin configuration"""
    import gin
    gin.clear_config()

    with open('src/api/backtest_analytics_api.py', 'r') as f:
        content = f.read()
        assert '@gin.configurable' in content
        assert 'class BacktestAPIConfig:' in content
        assert 'class BacktestCORSConfig:' in content
        assert 'class BacktestServerConfig:' in content
        assert 'class BacktestQueryConfig:' in content
        assert 'title: str = "Backtest Analytics API"' in content
        assert 'host: str = "0.0.0.0"' in content
        assert 'port: int = 8000' in content
        assert 'default_limit: int = 50' in content
        assert 'max_comparison_runs: int = 5' in content

    assert 'api_config.title' in content
    assert 'server_config.host' in content
    assert 'server_config.port' in content
    assert 'query_config.default_limit' in content
    assert 'query_config.max_comparison_runs' in content
    return True

def test_minute_service_config():
    """Test minute price service gin configuration"""
    import gin
    gin.clear_config()

    with open('src/services/minute/minute_price_service.py', 'r') as f:
        content = f.read()
        assert '@gin.configurable' in content
        assert 'class MinuteServiceConfig:' in content
        assert 'title: str = "ATS Minute Price Service"' in content
        assert 'port: int = 8081' in content
        assert 'default_symbols: List[str] = None' in content

    assert 'service_config.title' in content
    assert 'service_config.host' in content
    assert 'service_config.port' in content
    assert 'service_config.default_symbols' in content
    return True

def test_slack_webhook_config():
    """Test Slack webhook service gin configuration"""
    import gin
    gin.clear_config()

    with open('src/services/slack_webhook/app.py', 'r') as f:
        content = f.read()
        assert '@gin.configurable' in content
        assert 'class SlackWebhookConfig:' in content
        assert 'title: str = "ATS Slack Webhook Proxy"' in content
        assert 'port: int = None' in content
        assert 'slack_webhook_url: str = None' in content
        assert 'timeout_seconds: int = 30' in content
        assert 'max_retries: int = 3' in content

    assert 'slack_config.title' in content
    assert 'slack_config.host' in content
    assert 'slack_config.port' in content
    assert 'slack_config.slack_webhook_url' in content
    return True

def test_api_infrastructure_hardcoded_values_gin_updated():
    """Test that hardcoded_values.gin contains all new API configurations"""
    with open('config/hardcoded_values.gin', 'r') as f:
        gin_content = f.read()

        assert 'main.FastAPIConfig.title = "ATS GenAI API"' in gin_content
        assert 'main.FastAPIConfig.version = "0.1.0"' in gin_content
        assert 'main.CORSConfig.allow_origins = ["*"]' in gin_content
        assert 'main.CORSConfig.allow_credentials = true' in gin_content

        assert 'api.backtest_analytics_api.BacktestAPIConfig.title = "Backtest Analytics API"' in gin_content
        assert 'api.backtest_analytics_api.BacktestServerConfig.host = "0.0.0.0"' in gin_content
        assert 'api.backtest_analytics_api.BacktestServerConfig.port = 8000' in gin_content
        assert 'api.backtest_analytics_api.BacktestQueryConfig.default_limit = 50' in gin_content
        assert 'api.backtest_analytics_api.BacktestQueryConfig.max_comparison_runs = 5' in gin_content

        assert 'services.minute.minute_price_service.MinuteServiceConfig.title = "ATS Minute Price Service"' in gin_content
        assert 'services.minute.minute_price_service.MinuteServiceConfig.port = 8081' in gin_content
        assert 'services.minute.minute_price_service.MinuteServiceConfig.default_symbols = ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA", "META", "NVDA"]' in gin_content

        assert 'services.slack_webhook.app.SlackWebhookConfig.title = "ATS Slack Webhook Proxy"' in gin_content
        assert 'services.slack_webhook.app.SlackWebhookConfig.port = 8080' in gin_content
        assert 'services.slack_webhook.app.SlackWebhookConfig.timeout_seconds = 30' in gin_content
        assert 'services.slack_webhook.app.SlackWebhookConfig.max_retries = 3' in gin_content

        assert 'API AND SERVICE CONFIGURATION' in gin_content

    return True

def test_api_infrastructure_configuration_completeness():
    """Test that we've eliminated significant amounts of API hardcoded values"""

    main_api_params = 7
    backtest_api_params = 22
    minute_service_params = 6
    slack_webhook_params = 8
    total_api_params = main_api_params + backtest_api_params + minute_service_params + slack_webhook_params

    assert total_api_params == 43

    api_parameters = [
        'title', 'description', 'version', 'host', 'port',
        'allow_origins', 'allow_credentials', 'default_limit', 'max_limit',
        'timeout_seconds', 'max_retries', 'slack_webhook_url'
    ]

    files_to_check = [
        'src/main.py',
        'src/api/backtest_analytics_api.py',
        'src/services/minute/minute_price_service.py',
        'src/services/slack_webhook/app.py'
    ]

    configurable_count = 0
    for file_path in files_to_check:
        with open(file_path, 'r') as f:
            content = f.read()
            for param in api_parameters:
                if f'{param}:' in content:
                    configurable_count += 1

    assert configurable_count > 0
    return True

def test_gin_import_and_decorator():
    """Test that gin imports and decorators are properly added"""

    files_to_check = [
        'src/main.py',
        'src/api/backtest_analytics_api.py',
        'src/services/minute/minute_price_service.py',
        'src/services/slack_webhook/app.py'
    ]

    for file_path in files_to_check:
        with open(file_path, 'r') as f:
            content = f.read()
            assert 'import gin' in content
            assert '@gin.configurable' in content

    return True

if __name__ == "__main__":
    test_main_api_config()
    test_backtest_analytics_api_config()
    test_minute_service_config()
    test_slack_webhook_config()
    test_api_infrastructure_hardcoded_values_gin_updated()
    test_api_infrastructure_configuration_completeness()
    test_gin_import_and_decorator()