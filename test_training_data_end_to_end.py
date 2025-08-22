#!/usr/bin/env python3
"""
End-to-end test for training data visibility in web app.

This test verifies the complete integration:
1. Database contains training dataset records
2. Backend API can retrieve training datasets
3. Frontend JavaScript can load and display training data
4. Job run tracking is working for training data generation

This test should be run after the corrected web app deployment.
"""

import asyncio
import asyncpg
import json
import logging
from pathlib import Path
import requests
import time
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def test_training_data_end_to_end():
    """Test complete end-to-end training data visibility."""
    
    logger.info("🚀 Starting End-to-End Training Data Visibility Test")
    logger.info("=" * 60)
    
    success_count = 0
    total_tests = 6
    
    try:
        # Test 1: Verify database records exist
        logger.info("Test 1: Verifying database records exist...")
        
        db_url = "postgresql://postgres:dev_password@localhost:5433/dev_db"
        conn = await asyncpg.connect(db_url)
        
        # Check training dataset table
        dataset_count = await conn.fetchval("SELECT COUNT(*) FROM dev_training_dataset")
        logger.info(f"   Training datasets in database: {dataset_count}")
        
        if dataset_count > 0:
            success_count += 1
            logger.info("   ✅ Test 1 PASSED: Database contains training datasets")
        else:
            logger.error("   ❌ Test 1 FAILED: No training datasets found in database")
        
        # Get sample record
        sample_dataset = await conn.fetchrow("SELECT * FROM dev_training_dataset LIMIT 1")
        if sample_dataset:
            logger.info(f"   Sample dataset: {sample_dataset['dataset_name']} ({sample_dataset['symbols']})")
        
        await conn.close()
        
    except Exception as e:
        logger.error(f"   ❌ Test 1 FAILED: Database connection error: {e}")
    
    # Test 2: Verify job run records exist
    logger.info("\nTest 2: Verifying job run records exist...")
    
    try:
        conn = await asyncpg.connect(db_url)
        
        run_count = await conn.fetchval("""
            SELECT COUNT(*) FROM dev_runs 
            WHERE run_type = 'training_data_generation'
        """)
        logger.info(f"   Training data generation runs: {run_count}")
        
        if run_count > 0:
            success_count += 1
            logger.info("   ✅ Test 2 PASSED: Job run records exist")
            
            # Get sample run
            sample_run = await conn.fetchrow("""
                SELECT id, run_type, status, total_symbols 
                FROM dev_runs 
                WHERE run_type = 'training_data_generation' 
                LIMIT 1
            """)
            if sample_run:
                logger.info(f"   Sample run: ID {sample_run['id']}, Status: {sample_run['status']}")
        else:
            logger.error("   ❌ Test 2 FAILED: No training data generation runs found")
        
        await conn.close()
        
    except Exception as e:
        logger.error(f"   ❌ Test 2 FAILED: Database query error: {e}")
    
    # Test 3: Test backend API - training datasets
    logger.info("\nTest 3: Testing backend API - training datasets...")
    
    try:
        response = requests.get("http://localhost:3000/api/v1/training-datasets", timeout=10)
        
        if response.status_code == 200:
            datasets = response.json()
            logger.info(f"   API returned {len(datasets)} training datasets")
            
            if len(datasets) > 0:
                success_count += 1
                logger.info("   ✅ Test 3 PASSED: Backend API returns training datasets")
                
                # Verify dataset structure
                first_dataset = datasets[0]
                required_fields = ['dataset_name', 'total_sequences', 'symbols', 'data_quality_score']
                missing_fields = [field for field in required_fields if field not in first_dataset]
                
                if not missing_fields:
                    logger.info("   Dataset structure is complete")
                    logger.info(f"   Sample: {first_dataset['dataset_name']} - {first_dataset['total_sequences']:,} sequences")
                else:
                    logger.warning(f"   Missing fields in dataset: {missing_fields}")
            else:
                logger.error("   ❌ Test 3 FAILED: API returned empty array")
        else:
            logger.error(f"   ❌ Test 3 FAILED: API returned status {response.status_code}")
            
    except Exception as e:
        logger.error(f"   ❌ Test 3 FAILED: API request error: {e}")
    
    # Test 4: Test backend API - job runs
    logger.info("\nTest 4: Testing backend API - job runs...")
    
    try:
        response = requests.get("http://localhost:3000/api/v1/job-runs?run_type=training_data_generation", timeout=10)
        
        if response.status_code == 200:
            runs = response.json()
            logger.info(f"   API returned {len(runs)} training data generation runs")
            
            if len(runs) > 0:
                success_count += 1
                logger.info("   ✅ Test 4 PASSED: Backend API returns job runs")
                
                first_run = runs[0]
                logger.info(f"   Sample run: ID {first_run['run_id']}, Status: {first_run['status']}")
            else:
                logger.error("   ❌ Test 4 FAILED: API returned no job runs")
        else:
            logger.error(f"   ❌ Test 4 FAILED: API returned status {response.status_code}")
            
    except Exception as e:
        logger.error(f"   ❌ Test 4 FAILED: API request error: {e}")
    
    # Test 5: Test web app frontend HTML
    logger.info("\nTest 5: Testing web app frontend HTML structure...")
    
    try:
        response = requests.get("http://localhost:3000/", timeout=10)
        
        if response.status_code == 200:
            html = response.text
            
            # Check for required HTML elements and JavaScript
            required_elements = [
                'training-data',          # Tab ID
                'Training Data</h2>',     # Tab header
                'loadTrainingData',       # JavaScript function
                '/api/v1/training-datasets',  # API endpoint
                'training-datasets-list'  # Container ID
            ]
            
            missing_elements = []
            for element in required_elements:
                if element not in html:
                    missing_elements.append(element)
            
            if not missing_elements:
                success_count += 1
                logger.info("   ✅ Test 5 PASSED: Frontend HTML structure is complete")
            else:
                logger.error(f"   ❌ Test 5 FAILED: Missing HTML elements: {missing_elements}")
        else:
            logger.error(f"   ❌ Test 5 FAILED: Web app returned status {response.status_code}")
            
    except Exception as e:
        logger.error(f"   ❌ Test 5 FAILED: Web app request error: {e}")
    
    # Test 6: Test health endpoint
    logger.info("\nTest 6: Testing health endpoint...")
    
    try:
        response = requests.get("http://localhost:3000/health", timeout=10)
        
        if response.status_code == 200:
            health = response.json()
            if health.get('status') == 'ok':
                success_count += 1
                logger.info("   ✅ Test 6 PASSED: Health endpoint is working")
            else:
                logger.error(f"   ❌ Test 6 FAILED: Unhealthy status: {health}")
        else:
            logger.error(f"   ❌ Test 6 FAILED: Health endpoint returned status {response.status_code}")
            
    except Exception as e:
        logger.error(f"   ❌ Test 6 FAILED: Health endpoint error: {e}")
    
    # Summary
    logger.info(f"\n📊 End-to-End Test Results")
    logger.info("=" * 40)
    logger.info(f"Passed: {success_count}/{total_tests} tests")
    logger.info(f"Success Rate: {(success_count/total_tests)*100:.1f}%")
    
    if success_count == total_tests:
        logger.info("🎉 ALL TESTS PASSED! Training data system is fully functional!")
        logger.info("\n✅ Complete End-to-End Integration Verified:")
        logger.info("   • Database contains training dataset records")
        logger.info("   • Job run tracking is working")
        logger.info("   • Backend APIs return correct data") 
        logger.info("   • Frontend can display training data")
        logger.info("   • Web app is healthy and responsive")
        logger.info(f"\n🌐 View results at: http://localhost:3000")
        logger.info("   Click the 'Training Data' tab to see your AAPL dataset!")
        return True
    else:
        logger.error(f"❌ {total_tests - success_count} tests failed. Check output above for details.")
        return False

async def main():
    """Main test function."""
    logger.info("Training Data System - End-to-End Integration Test")
    logger.info("Verifying complete database → API → frontend integration")
    logger.info("")
    
    success = await test_training_data_end_to_end()
    
    if success:
        logger.info("\n🎯 MISSION ACCOMPLISHED!")
        logger.info("Training data generation, storage, and visualization is working end-to-end!")
    else:
        logger.error("\n💥 INTEGRATION ISSUES DETECTED!")
        logger.error("Some components are not working correctly.")
        exit(1)

if __name__ == "__main__":
    asyncio.run(main())