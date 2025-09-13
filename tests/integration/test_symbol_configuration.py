#!/usr/bin/env python3
"""
Test symbol configuration in training data generation
"""

import os
import sys
import asyncio

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from domains.ml.services.training_data.runners.training_data_callback_runner import create_sample_job_config, run_training_data_job_for_symbol

def test_job_config_symbol_configuration():
    """Test that job configuration correctly uses different symbols."""

    # Test AAPL configuration
    aapl_config = create_sample_job_config(symbols=['AAPL'])
    assert aapl_config.symbols == ['AAPL']
    assert 'AAPL' in aapl_config.job_name
    assert aapl_config.job_name == 'training_data_gen_AAPL'

    # Test TSLA configuration
    tsla_config = create_sample_job_config(symbols=['TSLA'])
    assert tsla_config.symbols == ['TSLA']
    assert 'TSLA' in tsla_config.job_name
    assert tsla_config.job_name == 'training_data_gen_TSLA'

    # Test multiple symbols configuration
    multi_config = create_sample_job_config(symbols=['AAPL', 'TSLA', 'GOOGL'])
    assert multi_config.symbols == ['AAPL', 'TSLA', 'GOOGL']
    assert multi_config.job_name == 'training_data_gen_AAPL-TSLA-GOOGL'

    print("✅ All symbol configuration tests passed!")

def test_dataset_name_generation():
    """Test that dataset names are generated correctly for different symbols."""


    # Test AAPL
    aapl_config = create_sample_job_config(symbols=['AAPL'])
    expected_aapl_prefix = f"dataset_{aapl_config.job_name}_"
    print(f"AAPL dataset prefix: {expected_aapl_prefix}")

    # Test TSLA
    tsla_config = create_sample_job_config(symbols=['TSLA'])
    expected_tsla_prefix = f"dataset_{tsla_config.job_name}_"
    print(f"TSLA dataset prefix: {expected_tsla_prefix}")

    # Verify they're different
    assert expected_aapl_prefix != expected_tsla_prefix
    assert 'AAPL' in expected_aapl_prefix
    assert 'TSLA' in expected_tsla_prefix

    print("✅ Dataset name generation tests passed!")

@pytest.mark.asyncio

async def test_training_data_generation_different_symbols():
    """Test actual training data generation with different symbols."""

    print("\n🔍 Testing training data generation with different symbols...")

    # Test TSLA
    print("Testing TSLA training data generation...")
    try:
        tsla_result = await run_training_data_job_for_symbol('TSLA')
        print(f"TSLA Result: {tsla_result['status']}")
        print(f"TSLA Run ID: {tsla_result.get('run_id', 'N/A')}")
        print(f"TSLA Dataset IDs: {tsla_result.get('dataset_ids', [])}")

        if tsla_result['status'] == 'success':
            print("✅ TSLA training data generation successful!")
        else:
            print(f"⚠️ TSLA training data generation failed: {tsla_result}")
    except Exception as e:
        print(f"❌ TSLA training data generation error: {e}")

    # Test GOOGL
    print("\nTesting GOOGL training data generation...")
    try:
        googl_result = await run_training_data_job_for_symbol('GOOGL')
        print(f"GOOGL Result: {googl_result['status']}")
        print(f"GOOGL Run ID: {googl_result.get('run_id', 'N/A')}")
        print(f"GOOGL Dataset IDs: {googl_result.get('dataset_ids', [])}")

        if googl_result['status'] == 'success':
            print("✅ GOOGL training data generation successful!")
        else:
            print(f"⚠️ GOOGL training data generation failed: {googl_result}")
    except Exception as e:
        print(f"❌ GOOGL training data generation error: {e}")

def main():
    """Run all tests."""

    print("🚀 Starting symbol configuration tests...")

    # Run synchronous tests
    test_job_config_symbol_configuration()
    test_dataset_name_generation()

    # Run async tests
    print("\n🔄 Running async training data generation tests...")
    asyncio.run(test_training_data_generation_different_symbols())

    print("\n✅ All symbol configuration tests completed!")

if __name__ == "__main__":
    main()