# CI/CD Integration Guide - News Collection System

## Overview

This guide provides complete CI/CD integration for the news collection system, ensuring that news-related bugs are caught before deployment and that production deployments include proper validation.

## Pre-Deployment Tests

### 1. GitHub Actions Workflow

Create `.github/workflows/news-collection-tests.yml`:

```yaml
name: News Collection Tests

on:
  push:
    paths:
      - 'scripts/*news*'
      - 'src/domains/news/**'
      - 'tests/**/*news*'
  pull_request:
    paths:
      - 'scripts/*news*'
      - 'src/domains/news/**'
      - 'tests/**/*news*'

jobs:
  test-news-collection:
    runs-on: ubuntu-latest
    
    services:
      postgres:
        image: postgres:13
        env:
          POSTGRES_PASSWORD: test_password
          POSTGRES_DB: test_db
        options: >-
          --health-cmd pg_isready
          --health-interval 10s
          --health-timeout 5s
          --health-retries 5
    
    steps:
    - uses: actions/checkout@v3
    
    - name: Set up Python
      uses: actions/setup-python@v4
      with:
        python-version: '3.12'
        
    - name: Install dependencies
      run: |
        pip install -r requirements.txt
        pip install pytest asyncpg aiohttp
        
    - name: Setup test database
      run: |
        PGPASSWORD=test_password psql -h localhost -U postgres -d test_db -c "
          CREATE TABLE test_news_polygon (
            id SERIAL PRIMARY KEY,
            vendor_id TEXT UNIQUE NOT NULL,
            title TEXT,
            description TEXT,
            author TEXT,
            published_utc TIMESTAMP WITH TIME ZONE,
            article_url TEXT,
            publisher_name TEXT,
            tickers TEXT[],
            keywords TEXT[],
            data JSONB,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
          );"
    
    - name: Run news collection unit tests
      env:
        POLYGON_API_KEY: ${{ secrets.POLYGON_API_KEY_TEST }}
        PYTHONPATH: src
      run: |
        python scripts/run_news_collection_tests.py --category unit --ci --environment test
    
    - name: Run news collection integration tests
      env:
        POLYGON_API_KEY: ${{ secrets.POLYGON_API_KEY_TEST }}
        PYTHONPATH: src
        DB_HOST: localhost
        DB_PORT: 5432
        DB_USER: postgres
        DB_PASSWORD: test_password
        DB_NAME: test_db
      run: |
        python scripts/run_news_collection_tests.py --category integration --ci --environment test
    
    - name: Run monitoring tests
      env:
        PYTHONPATH: src
        DB_HOST: localhost
        DB_PORT: 5432
        DB_USER: postgres
        DB_PASSWORD: test_password
        DB_NAME: test_db
      run: |
        python scripts/run_news_collection_tests.py --category monitoring --ci --environment test
    
    - name: Upload test results
      uses: actions/upload-artifact@v3
      if: always()
      with:
        name: news-test-results
        path: news_test_report.json
```

### 2. Pre-commit Hooks

Create `.pre-commit-config.yaml`:

```yaml
repos:
  - repo: local
    hooks:
      - id: news-collection-lint
        name: News Collection Code Quality
        entry: python scripts/run_news_collection_tests.py --category unit --fail-fast
        language: system
        files: '^(scripts/.*news.*|src/domains/news/|tests/.*news.*)'
        pass_filenames: false
```

Install pre-commit hooks:
```bash
pip install pre-commit
pre-commit install
```

## Deployment Pipeline

### 1. Staging Deployment

```yaml
deploy-staging:
  needs: test-news-collection
  runs-on: ubuntu-latest
  if: github.ref == 'refs/heads/main'
  
  steps:
  - uses: actions/checkout@v3
  
  - name: Deploy to staging
    run: |
      # Deploy application to staging
      ./scripts/deploy_staging.sh
  
  - name: Run staging news health check
    env:
      STAGING_DB_HOST: ${{ secrets.STAGING_DB_HOST }}
      STAGING_API_KEY: ${{ secrets.STAGING_API_KEY }}
    run: |
      # Wait for deployment
      sleep 30
      
      # Run comprehensive health check
      python tests/monitoring/test_news_data_monitoring.py \
        --environment staging \
        --output json > staging_health_report.json
      
      # Check if health is acceptable
      if [ "$(cat staging_health_report.json | jq -r '.overall_health')" != "HEALTHY" ]; then
        echo "❌ Staging health check failed"
        cat staging_health_report.json | jq '.alerts'
        exit 1
      fi
      
      echo "✅ Staging health check passed"
  
  - name: Upload staging health report
    uses: actions/upload-artifact@v3
    with:
      name: staging-health-report
      path: staging_health_report.json
```

### 2. Production Deployment Gate

```yaml
deploy-production:
  needs: deploy-staging
  runs-on: ubuntu-latest
  environment: production
  
  steps:
  - uses: actions/checkout@v3
  
  - name: Pre-deployment news validation
    env:
      PROD_DB_HOST: ${{ secrets.PROD_DB_HOST }}
      PROD_API_KEY: ${{ secrets.PROD_API_KEY }}
    run: |
      # Test API connectivity
      python scripts/test_api_connectivity.py --environment prod
      
      # Validate current news data health
      python tests/monitoring/test_news_data_monitoring.py \
        --environment prod \
        --output json > pre_deploy_health.json
      
      # Allow deployment only if current system is healthy
      current_health=$(cat pre_deploy_health.json | jq -r '.overall_health')
      if [ "$current_health" = "UNHEALTHY" ]; then
        critical_alerts=$(cat pre_deploy_health.json | jq '[.alerts[] | select(.severity == "critical")] | length')
        if [ "$critical_alerts" -gt 0 ]; then
          echo "❌ Cannot deploy: Production news system has critical issues"
          cat pre_deploy_health.json | jq '.alerts[] | select(.severity == "critical")'
          exit 1
        fi
      fi
  
  - name: Deploy to production
    run: |
      ./scripts/deploy_production.sh
  
  - name: Post-deployment verification
    env:
      PROD_DB_HOST: ${{ secrets.PROD_DB_HOST }}
      PROD_API_KEY: ${{ secrets.PROD_API_KEY }}
      SLACK_WEBHOOK: ${{ secrets.SLACK_WEBHOOK_PROD }}
    run: |
      # Wait for deployment to stabilize
      sleep 60
      
      # Run end-to-end test
      python scripts/run_news_collection_tests.py \
        --category end_to_end \
        --environment prod \
        --report-file prod_e2e_report.json
      
      # Verify health after deployment
      python tests/monitoring/test_news_data_monitoring.py \
        --environment prod \
        --output json > post_deploy_health.json
      
      # Send success notification
      if [ "$(cat post_deploy_health.json | jq -r '.overall_health')" = "HEALTHY" ]; then
        curl -X POST -H 'Content-type: application/json' \
          --data '{"text":"✅ News collection system deployed successfully to production"}' \
          "$SLACK_WEBHOOK"
      else
        curl -X POST -H 'Content-type: application/json' \
          --data '{"text":"⚠️ News collection deployment completed but health check shows issues"}' \
          "$SLACK_WEBHOOK"
      fi
```

## Environment Configuration

### 1. Required Secrets

Add to GitHub repository secrets:

```
# API Keys
POLYGON_API_KEY_TEST=test_api_key_here
POLYGON_API_KEY_STAGING=staging_api_key_here  
POLYGON_API_KEY_PROD=production_api_key_here

# Database Connections
STAGING_DB_HOST=staging-db.example.com
STAGING_DB_PASSWORD=staging_db_password

PROD_DB_HOST=prod-db.example.com
PROD_DB_PASSWORD=prod_db_password

# Alerting
SLACK_WEBHOOK_STAGING=https://hooks.slack.com/services/...
SLACK_WEBHOOK_PROD=https://hooks.slack.com/services/...
```

### 2. Environment-specific Configuration

Create `config/environments/` files:

**config/environments/test.yml:**
```yaml
news_collection:
  polygon_api_key: env:POLYGON_API_KEY_TEST
  rate_limits:
    requests_per_minute: 5  # Lower for testing
  database:
    host: localhost
    port: 5432
    name: test_db
```

**config/environments/staging.yml:**
```yaml
news_collection:
  polygon_api_key: env:POLYGON_API_KEY_STAGING
  rate_limits:
    requests_per_minute: 200
  monitoring:
    health_check_interval: 300  # 5 minutes
    alert_thresholds:
      data_freshness_hours: 6
```

**config/environments/prod.yml:**
```yaml
news_collection:
  polygon_api_key: env:POLYGON_API_KEY_PROD
  rate_limits:
    requests_per_minute: 1000
  monitoring:
    health_check_interval: 900  # 15 minutes
    alert_thresholds:
      data_freshness_hours: 2
```

## Monitoring Integration

### 1. Health Check Automation

```bash
# Add to deployment scripts
post_deploy_health_check() {
    local environment=$1
    local max_attempts=10
    local attempt=1
    
    echo "🏥 Running post-deployment health checks..."
    
    while [ $attempt -le $max_attempts ]; do
        if python tests/monitoring/test_news_data_monitoring.py \
           --environment "$environment" \
           --output json > health_check_result.json; then
            
            local health=$(cat health_check_result.json | jq -r '.overall_health')
            
            if [ "$health" = "HEALTHY" ]; then
                echo "✅ Health check passed on attempt $attempt"
                return 0
            else
                echo "⚠️ Health check shows issues on attempt $attempt"
                cat health_check_result.json | jq '.alerts'
            fi
        else
            echo "❌ Health check failed on attempt $attempt"
        fi
        
        echo "Waiting 30 seconds before retry..."
        sleep 30
        attempt=$((attempt + 1))
    done
    
    echo "❌ Health checks failed after $max_attempts attempts"
    return 1
}
```

### 2. Rollback Automation

```bash
# Automatic rollback on health check failure
deploy_with_rollback() {
    local environment=$1
    
    # Backup current state
    create_deployment_backup "$environment"
    
    # Deploy new version
    if deploy_application "$environment"; then
        echo "✅ Deployment completed"
        
        # Verify health
        if post_deploy_health_check "$environment"; then
            echo "✅ Deployment verified - health checks passed"
            return 0
        else
            echo "❌ Health checks failed - initiating rollback"
            rollback_deployment "$environment"
            return 1
        fi
    else
        echo "❌ Deployment failed"
        return 1
    fi
}
```

## Test Automation

### 1. Scheduled Test Runs

```yaml
# .github/workflows/scheduled-news-tests.yml
name: Scheduled News Health Tests

on:
  schedule:
    - cron: '0 */6 * * *'  # Every 6 hours
  workflow_dispatch:

jobs:
  health-check:
    runs-on: ubuntu-latest
    strategy:
      matrix:
        environment: [staging, production]
    
    steps:
    - uses: actions/checkout@v3
    
    - name: Run health monitoring
      env:
        ENV: ${{ matrix.environment }}
        SLACK_WEBHOOK: ${{ secrets.SLACK_WEBHOOK_PROD }}
      run: |
        python tests/monitoring/test_news_data_monitoring.py \
          --environment "$ENV" \
          --output json \
          --alert-slack > health_report_$ENV.json
    
    - name: Upload health reports
      uses: actions/upload-artifact@v3
      with:
        name: health-reports-${{ github.run_id }}
        path: health_report_*.json
```

### 2. Performance Regression Tests

```python
# tests/performance/test_news_collection_performance.py
import time
import pytest

@pytest.mark.performance
def test_backfill_performance():
    """Test that news backfill meets performance requirements"""
    start_time = time.time()
    
    # Run small backfill
    result = run_backfill_test(limit=100)
    
    duration = time.time() - start_time
    
    # Should process at least 500 records/minute
    min_rate = 500 / 60  # records per second
    actual_rate = 100 / duration
    
    assert actual_rate >= min_rate, f"Performance regression: {actual_rate:.1f} < {min_rate:.1f} records/sec"
    assert result.success_rate >= 0.95, f"Success rate too low: {result.success_rate}"
```

## Production Readiness Checklist

### Before Going Live:
- [ ] All tests pass in CI/CD pipeline
- [ ] Staging deployment successful with health checks
- [ ] API keys configured for production
- [ ] Monitoring alerts configured
- [ ] Backup procedures tested
- [ ] Rollback procedures tested
- [ ] Performance benchmarks established
- [ ] Documentation updated

### Go-Live Deployment:
- [ ] Deploy during low-traffic period
- [ ] Monitor health checks every 5 minutes for first hour
- [ ] Verify data collection working within 1 hour
- [ ] Confirm monitoring alerts functional
- [ ] Validate analytics service shows new data

### Post-Deployment:
- [ ] Daily health monitoring active
- [ ] Weekly backup verification
- [ ] Monthly performance review
- [ ] Quarterly disaster recovery test

## Troubleshooting Common CI/CD Issues

### Test Failures:
1. **API Key Issues**: Verify secrets are set correctly
2. **Database Connection**: Check PostgreSQL service configuration
3. **Network Issues**: Ensure container networking works
4. **Race Conditions**: Add appropriate wait times

### Deployment Issues:
1. **Health Check Failures**: Review monitoring thresholds
2. **Performance Regression**: Check database indexes
3. **Configuration Drift**: Validate environment configs
4. **Rollback Failures**: Test rollback procedures regularly

This CI/CD integration ensures that news collection remains reliable through automated testing, monitoring, and deployment validation.