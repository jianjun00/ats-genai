#!/usr/bin/env python3
"""
Deploy Real Collector with Valid API Keys

This script configures and deploys the real-time collector with actual vendor API keys
instead of synthetic data generation.
"""

import subprocess
import os
import sys
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def update_docker_compose_with_real_keys():
    """Update docker compose with real API keys for production collection using centralized system."""
    logger.info("🔑 Configuring real vendor API keys with centralized management...")

    # Use centralized API key management system
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))
    from config.environment import Environment, EnvironmentType

    env = Environment(env_type=EnvironmentType.PRODUCTION)

    # Get keys using centralized system
    api_keys = {
        'POLYGON_API_KEY': env.get_api_key('polygon'),
        'TIINGO_API_KEY': env.get_api_key('tiingo'),
        'EODHD_API_KEY': env.get_api_key('eodhd')
    }

    logger.info("🔧 API Key Configuration (Centralized System):")
    for key, value in api_keys.items():
        if value:
            masked_value = value[:8] + "..." + value[-4:] if len(value) > 12 else "NOT_SET"
            logger.info(f"  ✅ {key}: {masked_value}")
        else:
            logger.warning(f"  ❌ {key}: NOT_FOUND")

    return api_keys

def stop_synthetic_collectors():
    """Stop any running synthetic collectors."""
    logger.info("🛑 Stopping synthetic collectors...")

    # Kill any background synthetic processes
    result = subprocess.run([
        'docker', 'exec', 'ats-intg-analytics',
        'pkill', '-f', 'synthetic_collector'
    ], capture_output=True, text=True)

    if result.returncode == 0:
        logger.info("✅ Synthetic collectors stopped")
    else:
        logger.info("ℹ️ No synthetic collectors found running")

def deploy_real_collector():
    """Deploy the real collector with API authentication."""
    logger.info("🚀 Deploying real-time collector...")

    # Real collector command with proper API keys
    collector_cmd = [
        'docker', 'exec', '-d', 'ats-intg-analytics',
        'bash', '-c',
        f'''cd /workspace &&
        PYTHONPATH=src
        POLYGON_API_KEY={os.getenv('POLYGON_API_KEY', '')}
        TIINGO_API_KEY={os.getenv('TIINGO_API_KEY', '')}
        EODHD_API_KEY={os.getenv('EODHD_API_KEY', '')}
        python3 src/domains/market_data/services/realtime/aapl_tsla_realtime_collector.py
        '''
    ]

    result = subprocess.run(collector_cmd, capture_output=True, text=True, timeout=10)

    if result.returncode == 0:
        logger.info("✅ Real collector deployed successfully")
        return True
    else:
        logger.error(f"❌ Failed to deploy collector: {result.stderr}")
        return False

def verify_real_data_collection():
    """Verify that real data collection is working."""
    logger.info("🔍 Verifying real data collection...")

    import time
    import asyncpg
    import asyncio

    async def check_data():
        conn = await asyncpg.connect(
            host='localhost',
            port=4432,
            user='postgres',
            password='intg_password',
            database='intg_db'
        )

        # Wait 2 minutes for some data collection attempts
        logger.info("⏳ Waiting 2 minutes for data collection...")
        await asyncio.sleep(120)

        # Check API calls table for real attempts
        api_calls = await conn.fetchval(
            "SELECT COUNT(*) FROM intg_api_calls WHERE request_timestamp >= NOW() - INTERVAL '5 minutes'"
        )

        # Check minute bar collection metrics
        collection_metrics = await conn.fetchval(
            "SELECT COUNT(*) FROM intg_minute_bar_collection_metrics WHERE collection_timestamp >= NOW() - INTERVAL '5 minutes'"
        )

        logger.info(f"📊 API calls in last 5 min: {api_calls}")
        logger.info(f"📊 Collection metrics in last 5 min: {collection_metrics}")

        if api_calls > 0:
            logger.info("✅ Real API calls detected - collector is working!")
        else:
            logger.warning("⚠️ No API calls detected - may need valid API keys")

        await conn.close()

    asyncio.run(check_data())

def main():
    """Main deployment function."""
    logger.info("="*60)
    logger.info("🎯 DEPLOYING REAL COLLECTOR")
    logger.info("="*60)

    # Step 1: Configure API keys
    api_keys = update_docker_compose_with_real_keys()

    # Step 2: Stop synthetic collectors
    stop_synthetic_collectors()

    # Step 3: Deploy real collector
    success = deploy_real_collector()

    if success:
        logger.info("🎉 Real collector deployment completed!")
        logger.info("")
        logger.info("📋 Next Steps:")
        logger.info("1. Provide valid API keys via environment variables:")
        logger.info("   export POLYGON_API_KEY='your_polygon_key'")
        logger.info("   export TIINGO_API_KEY='your_tiingo_key'")
        logger.info("2. Monitor logs: docker logs ats-intg-analytics")
        logger.info("3. Check Grafana dashboard for real data patterns")
        logger.info("")
        logger.info("🔍 Key Differences from Synthetic:")
        logger.info("- Real API calls to vendor endpoints")
        logger.info("- Authentication required (403/401 errors expected without valid keys)")
        logger.info("- Data only during market hours (not 24/7)")
        logger.info("- Rate limits and API quotas apply")

        # Step 4: Verify deployment
        verify_real_data_collection()

    else:
        logger.error("❌ Deployment failed!")
        sys.exit(1)

if __name__ == "__main__":
    main()