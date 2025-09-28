#!/usr/bin/env python3
"""
Comprehensive investigation of training dataset sequence visualization issues.

This test diagnoses why sequence data shows "No sequence data available" and
investigates the complete data flow from API to UI visualization.
"""

import pytest
from playwright.async_api import async_playwright
import json

@pytest.mark.asyncio
async def test_sequence_data_complete_investigation():
    """Complete investigation of sequence data visualization issues."""

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context()
        page = await context.new_page()

        # Capture all network requests to see what's happening
        network_requests = []
        page.on("request", lambda request: network_requests.append({
            "url": request.url,
            "method": request.method
        }))

        # Capture responses
        network_responses = []
        page.on("response", lambda response: network_responses.append({
            "url": response.url,
            "status": response.status,
            "ok": response.ok
        }))

        # Capture console logs and errors
        console_logs = []
        page.on("console", lambda msg: console_logs.append(f"[{msg.type.upper()}] {msg.text}"))

        page_errors = []
        page.on("pageerror", lambda error: page_errors.append(str(error)))

        print("🔍 STEP 1: Navigate to training datasets page")
        await page.goto("http://localhost:3000/api/v1/training-datasets", timeout=15000)
        await page.wait_for_load_state("networkidle")

        print("🔍 STEP 2: Check if datasets are displayed")

        # Get the raw API data
        datasets_response = await page.evaluate("""
            fetch('/api/v1/training-datasets')
                .then(r => r.json())
                .catch(e => ({error: e.message}))
        """)

        print(f"✅ Found {len(datasets_response.get('datasets', []))} datasets in API")

        if len(datasets_response.get('datasets', [])) > 0:
            first_dataset = datasets_response['datasets'][0]
            dataset_id = first_dataset['id']
            dataset_name = first_dataset['dataset_name']

            print(f"🔍 STEP 3: Testing dataset ID {dataset_id}: {dataset_name}")

            # Check sequence data API
            sequences_response = await page.evaluate(f"""
                fetch('/api/v1/training-datasets/{dataset_id}/sequences')
                    .then(r => r.json())
                    .catch(e => ({{error: e.message}}))
            """)

            print("🔍 STEP 4: Analyze sequence API response")
            print(f"Sequences API response: {json.dumps(sequences_response, indent=2)}")

            # Check if there are actually sequences
            sequences = sequences_response.get('sequences', [])
            total_count = sequences_response.get('total_count', 0)

            print(f"📊 Sequence data analysis:")
            print(f"  - Total sequences available: {total_count}")
            print(f"  - Sequences in response: {len(sequences)}")
            print(f"  - Dataset shows total_sequences: {first_dataset.get('total_sequences', 0)}")

            # This is the key issue!
            if first_dataset.get('total_sequences', 0) > total_count:
                print("❌ PROBLEM IDENTIFIED: Dataset metadata shows more sequences than API returns!")
                print(f"   Expected: {first_dataset['total_sequences']} sequences")
                print(f"   Got: {total_count} sequences")
                print("   This explains why 'No sequence data available' appears")

            print("🔍 STEP 5: Test actual sequence data for first sequence")
            if sequences:
                first_sequence = sequences[0]
                print(f"First sequence: {json.dumps(first_sequence, indent=2)}")

                # Try to get the actual sequence data
                sequence_detail = await page.evaluate(f"""
                    fetch('/api/v1/training-datasets/{dataset_id}/sequences/{first_sequence['id']}/data')
                        .then(r => r.json())
                        .catch(e => ({{error: e.message}}))
                """)

                print(f"Sequence detail API: {json.dumps(sequence_detail, indent=2)}")

            print("🔍 STEP 6: Check UI behavior")

            # Look for specific UI elements that might be causing the issue
            ui_elements = await page.evaluate("""
                {
                    tablesFound: document.querySelectorAll('table').length,
                    hasDatasetName: document.body.textContent.includes('training_AAPL'),
                    hasSequenceSection: document.body.textContent.includes('Training Sequence Data'),
                    hasNoDataMessage: document.body.textContent.includes('No sequence data available'),
                    plotlyContainers: document.querySelectorAll('[id*="plotly"], .js-plotly-plot').length,
                    plotlyLoaded: typeof Plotly !== 'undefined'
                }
            """)

            print(f"UI Elements Analysis: {json.dumps(ui_elements, indent=2)}")

            print("🔍 STEP 7: Check for specific sequence data endpoints")
            # Test additional sequence-related endpoints
            additional_endpoints = [
                f'/api/v1/training-datasets/{dataset_id}/sequences/0/ohlc',
                f'/api/v1/training-datasets/{dataset_id}/sequences/0/features',
                f'/api/ray-analytics/{dataset_id}/sequences',
            ]

            for endpoint in additional_endpoints:
                response = await page.evaluate(f"""
                    fetch('{endpoint}')
                        .then(r => r.json())
                        .catch(e => ({{error: e.message}}))
                """)
                print(f"Endpoint {endpoint}: {json.dumps(response, indent=2)}")

        print("🔍 STEP 8: Network analysis")
        print("Network requests made:")
        for req in network_requests[-10:]:  # Last 10 requests
            print(f"  {req['method']} {req['url']}")

        print("Network responses:")
        for resp in network_responses[-10:]:  # Last 10 responses
            print(f"  {resp['status']} {resp['url']} ({'OK' if resp['ok'] else 'FAILED'})")

        print("🔍 STEP 9: Console and error analysis")
        if console_logs:
            print("Console logs:")
            for log in console_logs:
                print(f"  {log}")

        if page_errors:
            print("Page errors:")
            for error in page_errors:
                print(f"  {error}")

        # Take screenshot for visual debugging
        await page.screenshot(path="sequence_investigation.png")
        print("📸 Screenshot saved as sequence_investigation.png")

        print("\n🔍 SUMMARY:")
        print("=" * 50)
        if first_dataset.get('total_sequences', 0) > total_count:
            print("❌ ISSUE CONFIRMED: Sequence data mismatch")
            print(f"   - Dataset metadata claims {first_dataset['total_sequences']} sequences")
            print(f"   - API only returns {total_count} sequences")
            print("   - This causes 'No sequence data available' in UI")
            print("   - Need to investigate sequence data storage/retrieval")
        else:
            print("✅ Sequence count matches - issue may be elsewhere")

if __name__ == "__main__":
    import sys
    pytest.main([__file__, "-v", "--tb=short"] + sys.argv[1:])