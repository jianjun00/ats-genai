#!/usr/bin/env python3
"""Test API imports in Docker environment"""
import sys
sys.path.append('src')

try:
    from api.datasets_api import router as datasets_router
    print("✅ Datasets API router imported successfully")
    print(f"   Routes: {len(datasets_router.routes)}")
except ImportError as e:
    print(f"❌ Failed to import datasets API: {e}")

try:
    from api.training_dataset_api import router as training_router
    print("✅ Training dataset API router imported successfully") 
    print(f"   Routes: {len(training_router.routes)}")
except ImportError as e:
    print(f"❌ Failed to import training dataset API: {e}")