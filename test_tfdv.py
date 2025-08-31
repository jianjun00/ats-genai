#!/usr/bin/env python3
"""
Test script for TFDV integration
"""
import sys
import os
sys.path.append('src')

try:
    from services.tfdv_integration_service import TFDVIntegrationService
    print("✅ TFDV integration service imported successfully")
    
    # Test service initialization
    service = TFDVIntegrationService()
    print("✅ TFDV service initialized successfully")
    
    print(f"✅ TFDV available: {hasattr(service, 'TFDV_AVAILABLE') and service.__class__.__module__}")
    
except ImportError as e:
    print(f"❌ Failed to import TFDV service: {e}")
except Exception as e:
    print(f"❌ Error testing TFDV service: {e}")

try:
    from dao.training_dataset_dao import TrainingDatasetDAO, TrainingDatasetRecord
    print("✅ Training dataset DAO imported successfully")
    
    # Test TrainingDatasetRecord creation
    record = TrainingDatasetRecord(
        dataset_name="test_dataset",
        run_id=1,
        total_sequences=100,
        feature_count=10,
        label_count=2
    )
    print("✅ TrainingDatasetRecord created successfully")
    
except ImportError as e:
    print(f"❌ Failed to import DAO: {e}")
except Exception as e:
    print(f"❌ Error testing DAO: {e}")

print("🎯 Test completed")