#!/usr/bin/env python3
"""
Isolated test of database registration function to identify the specific error
"""

import sys
import os
import asyncio
from datetime import date
from pathlib import Path

sys.path.append('/home/jianjun/ats-genai-admin/src')

# Test the database registration function directly
async def test_database_registration():
    print("🔍 Testing database registration in isolation...")
    
    try:
        from shared.data_handling.utils.environment import Environment
        from domains.ml.services.training_data.timeseries_sequence_training_generator import TrainingDataConfig
        
        print("✅ Imports successful")
        
        # Create environment manually using environment variables (bypass gin config issues)
        import os
        os.environ['ENVIRONMENT_TYPE'] = 'intg'
        os.environ['DB_HOST'] = 'localhost'
        os.environ['DB_PORT'] = '4432'
        os.environ['DB_USER'] = 'postgres'
        os.environ['DB_PASSWORD'] = 'intg_password'
        os.environ['DB_NAME'] = 'intg_db'
        
        from shared.data_handling.utils.environment import EnvironmentType
        environment = Environment(gin_config_path=None, env_type=EnvironmentType.INTEGRATION)
        print("✅ Environment created")
        
        # Create training config (simplified)
        config = TrainingDataConfig()
        print("✅ Training config created")
        
        # Import and test the registration function
        from domains.ml.services.training_data.runners.training_data_callback_runner import register_training_dataset
        
        print("✅ register_training_dataset function imported")
        
        # Test the function
        print("🔄 Attempting database registration...")
        
        db_dataset_id = await register_training_dataset(
            environment=environment,
            symbols=['TSLA'],
            start_date=date(2025, 7, 1),
            end_date=date(2025, 7, 2),
            config=config,
            output_dir="/data/training_data",
            storage_format="arrayrecord"
        )
        
        print(f"✅ SUCCESS: Database registration completed with ID: {db_dataset_id}")
        return True
        
    except Exception as e:
        print(f"❌ FAILED: Database registration error: {e}")
        import traceback
        print(f"📋 Full traceback:\n{traceback.format_exc()}")
        return False

if __name__ == "__main__":
    success = asyncio.run(test_database_registration())
    if success:
        print("\n✅ Database registration function works correctly")
    else:
        print("\n❌ Database registration function has issues - this explains why training data isn't appearing in UI")