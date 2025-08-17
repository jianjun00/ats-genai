#!/usr/bin/env python3
"""
Test script for validation integration.
"""

import asyncio
import os
from datetime import date, timedelta

from config.environment import Environment, EnvironmentType
from config.database import get_connection_pool
from frontfill.validation_integration import ValidationIntegration, ValidationConfig

async def test_validation_integration():
    """Test the validation integration system."""
    print("🧪 Testing validation integration system...")
    
    # Setup
    env = Environment(EnvironmentType.DEV)
    api_keys = {
        "polygon": os.getenv("POLYGON_API_KEY", "test_key"),
        "tiingo": os.getenv("TIINGO_API_KEY", "test_key")
    }
    
    connection_pool = await get_connection_pool(env)
    
    try:
        # Initialize validation integration
        config = ValidationConfig(
            enable_post_frontfill_validation=True,
            enable_missing_data_detection=True,
            enable_automatic_backfill=False,  # Disable for testing
            quality_threshold=70.0,
            backfill_priority_threshold=3
        )
        
        integration = ValidationIntegration(connection_pool, env, api_keys, config)
        await integration.initialize()
        
        print("✅ Validation integration initialized successfully")
        
        # Test validation for yesterday
        validation_date = date.today() - timedelta(days=1)
        print(f"📊 Testing validation for {validation_date}")
        
        results = await integration.run_post_frontfill_validation(validation_date)
        
        print(f"✅ Validation completed:")
        print(f"   Quality Score: {results['quality_score']:.2f}/100")
        print(f"   Validation Passed: {results['validation_passed']}")
        print(f"   Instruments Validated: {results.get('instruments_validated', 0)}")
        print(f"   Total Issues: {results.get('total_issues', 0)}")
        print(f"   Actions Taken: {results.get('actions_taken', [])}")
        
        # Test missing data analysis
        print(f"\n📈 Testing missing data analysis")
        start_date = validation_date - timedelta(days=7)
        
        report = await integration.run_missing_data_analysis(start_date, validation_date)
        
        print(f"✅ Missing data analysis completed:")
        summary = report.get("summary", {})
        print(f"   Total gaps: {summary.get('total_gaps', 0)}")
        print(f"   Missing days: {summary.get('total_missing_days', 0)}")
        print(f"   Symbols affected: {summary.get('symbols_affected', 0)}")
        
        print("\n🎉 All tests passed successfully!")
        return True
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        return False
        
    finally:
        await connection_pool.close()

if __name__ == "__main__":
    success = asyncio.run(test_validation_integration())
    exit(0 if success else 1)