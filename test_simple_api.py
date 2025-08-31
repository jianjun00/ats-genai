#!/usr/bin/env python3
"""Test simple API import"""
import sys
sys.path.append('src')

try:
    from api.training_dataset_simple_api import router as training_router
    print("✅ Training dataset simple API imported successfully") 
    print(f"   Routes: {len(training_router.routes)}")
    for route in training_router.routes:
        print(f"   - {route.methods} {route.path}")
except ImportError as e:
    print(f"❌ Failed to import simple API: {e}")

try:
    from api.datasets_api import router as datasets_router
    print("✅ Datasets API imported successfully")
    print(f"   Routes: {len(datasets_router.routes)}")
except ImportError as e:
    print(f"❌ Failed to import datasets API: {e}")