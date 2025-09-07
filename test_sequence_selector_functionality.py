#!/usr/bin/env python3
"""
Test the sequence selector functionality specifically.
This tests the dropdown that shows "no sequence found" error.
"""
import requests
import json

def test_sequence_selector():
    """Test sequence selector API and functionality."""
    print("🧪 Testing Sequence Selector Functionality")
    print("=" * 50)

    # Step 1: Get available datasets
    print("1️⃣ Getting training datasets...")
    response = requests.get("http://localhost:3000/api/v1/training-datasets", timeout=10)
    assert response.status_code == 200, f"Datasets API failed: {response.status_code}"

    datasets = response.json()["datasets"]
    print(f"✅ Found {len(datasets)} training datasets")

    for dataset in datasets[:2]:  # Test first 2 datasets
        dataset_id = dataset["id"]
        dataset_name = dataset["dataset_name"]
        print(f"\n2️⃣ Testing Sequence Selector for Dataset {dataset_id}: {dataset_name}")

        # Step 2: Test default visualization data (no sequence specified)
        print("   📊 Testing default sequence (sequence_id = null)...")
        viz_response = requests.get(
            f"http://localhost:3000/api/v1/training-datasets/{dataset_id}/visualization-data",
            timeout=10
        )
        assert viz_response.status_code == 200, f"Default visualization failed for dataset {dataset_id}"

        viz_data = viz_response.json()
        default_data_count = len(viz_data.get("data", []))
        print(f"   ✅ Default sequence returns {default_data_count} records")

        # Step 3: Test specific sequence selection
        print("   🔍 Testing specific sequence selection (sequence_id = 0)...")
        sequence_response = requests.get(
            f"http://localhost:3000/api/v1/training-datasets/{dataset_id}/visualization-data?sequence_id=0",
            timeout=10
        )
        assert sequence_response.status_code == 200, f"Sequence selection failed for dataset {dataset_id}"

        sequence_data = sequence_response.json()
        sequence_data_count = len(sequence_data.get("data", []))
        sequence_id_returned = sequence_data.get("sequence_id")

        print(f"   📈 Sequence 0 returns {sequence_data_count} records")
        print(f"   🎯 Sequence ID returned: {sequence_id_returned}")

        # Step 4: Test out-of-range sequence
        print("   🚫 Testing out-of-range sequence (sequence_id = 999)...")
        invalid_response = requests.get(
            f"http://localhost:3000/api/v1/training-datasets/{dataset_id}/visualization-data?sequence_id=999",
            timeout=10
        )
        assert invalid_response.status_code == 200, f"Invalid sequence handling failed for dataset {dataset_id}"

        invalid_data = invalid_response.json()
        invalid_data_count = len(invalid_data.get("data", []))
        error_message = invalid_data.get("error", "")

        print(f"   ❌ Invalid sequence returns {invalid_data_count} records")
        if error_message:
            print(f"   📝 Error message: {error_message}")

        # Step 5: Check sequence metadata
        print("   🔢 Checking sequence metadata...")
        total_sequences = viz_data.get("total_sequences", 0)
        sequence_length = viz_data.get("sequence_length", 0)

        print(f"   📊 Total sequences reported: {total_sequences}")
        print(f"   📏 Sequence length reported: {sequence_length}")

        # Step 6: Test if sequences are available for selection
        if default_data_count > 0:
            expected_sequences = max(1, default_data_count - 20)  # Usually data_count - window_size
            print(f"   🎯 Expected available sequences: ~{expected_sequences}")

            if total_sequences == 0:
                print(f"   ⚠️  BUG FOUND: Data exists ({default_data_count} records) but total_sequences=0")
                print(f"   🔧 This causes 'no sequence found' in the dropdown!")
                return False
            else:
                print(f"   ✅ Sequences properly reported for selection")

        print()

    # Step 7: Test sequence dropdown API endpoint (if it exists)
    print("3️⃣ Testing sequence dropdown API...")

    test_dataset_id = datasets[0]["id"]

    # Check if there's a dedicated sequences endpoint
    sequences_response = requests.get(
        f"http://localhost:3000/api/v1/training-datasets/{test_dataset_id}/sequences",
        timeout=10
    )

    if sequences_response.status_code == 200:
        sequences_data = sequences_response.json()
        print(f"   ✅ Sequences API exists: {sequences_data}")
    elif sequences_response.status_code == 404:
        print(f"   ℹ️  No dedicated sequences API endpoint")
    else:
        print(f"   ⚠️  Sequences API error: {sequences_response.status_code}")

    print("\n🎉 Sequence Selector Test Complete!")
    return True

def test_frontend_sequence_logic():
    """Test frontend sequence logic by examining the EDA page."""
    print("\n🧪 Testing Frontend Sequence Logic")
    print("=" * 40)

    # Get EDA page content
    eda_response = requests.get("http://localhost:3000/eda", timeout=10)
    assert eda_response.status_code == 200, "EDA page should load"

    page_content = eda_response.text

    # Look for sequence-related JavaScript
    print("🔍 Analyzing frontend sequence logic...")

    # Check for sequence selector elements
    if 'id="sequence-selector"' in page_content:
        print("✅ Sequence selector element found")
    else:
        print("❌ Sequence selector element missing")

    # Check for sequence population logic
    if 'populateSequenceSelector' in page_content or 'sequence-selector' in page_content:
        print("✅ Sequence population logic found")

        # Extract and analyze the logic
        lines = page_content.split('\n')
        for i, line in enumerate(lines):
            if 'sequence-selector' in line.lower() and ('option' in line or 'populate' in line):
                print(f"   📝 Line {i}: {line.strip()}")
    else:
        print("❌ Sequence population logic missing")

    # Check for "no sequence found" handling
    if 'no sequence found' in page_content.lower() or 'No sequence' in page_content:
        print("✅ 'No sequence found' handling exists")

        # Find the specific lines
        lines = page_content.split('\n')
        for i, line in enumerate(lines):
            if 'no sequence' in line.lower():
                print(f"   📝 Line {i}: {line.strip()}")
    else:
        print("❌ 'No sequence found' handling missing")

    return True

def main():
    """Run all sequence selector tests."""
    try:
        success1 = test_sequence_selector()
        success2 = test_frontend_sequence_logic()

        if success1 and success2:
            print("\n✅ ALL SEQUENCE TESTS PASSED")
        else:
            print("\n❌ SEQUENCE TESTS FOUND ISSUES")
            print("🔧 Likely issue: total_sequences metadata not set correctly")

        return success1 and success2
    except Exception as e:
        print(f"\n❌ Sequence test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)