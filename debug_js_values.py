#!/usr/bin/env python3
"""
Debug JavaScript values being passed to loadDatasetVisualization
"""

import asyncio
from playwright.async_api import async_playwright

async def debug_js_values():
    print("🔍 DEBUGGING JAVASCRIPT VALUES")
    print("=" * 35)

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()

        try:
            # Setup
            await page.goto("http://localhost:3000", wait_until="networkidle")
            await page.get_by_role("button", name="🤖 Training Datasets").click()
            await page.wait_for_timeout(2000)

            await page.locator("#dataset-selector").select_option(value="63")
            await page.wait_for_timeout(2000)

            await page.locator("#sequence-selector").select_option(index=1)
            await page.wait_for_timeout(1000)

            print("📊 Checking values before clicking Visualize...")

            # Check the actual values that would be read by loadDatasetVisualization
            js_values = await page.evaluate("""
                () => {
                    const datasetSelector = document.getElementById('dataset-selector');
                    const sequenceSelector = document.getElementById('sequence-selector');
                    const rowSelector = document.getElementById('row-selector');

                    return {
                        datasetId: datasetSelector ? datasetSelector.value : 'MISSING',
                        sequenceId: sequenceSelector ? sequenceSelector.value : 'MISSING',
                        rowIndex: rowSelector ? rowSelector.value : 'MISSING',
                        datasetExists: !!datasetSelector,
                        sequenceExists: !!sequenceSelector,
                        rowExists: !!rowSelector
                    };
                }
            """)

            print(f"   Dataset ID: '{js_values['datasetId']}'")
            print(f"   Sequence ID: '{js_values['sequenceId']}'")
            print(f"   Row Index: '{js_values['rowIndex']}'")
            print(f"   Elements exist - Dataset: {js_values['datasetExists']}, Sequence: {js_values['sequenceExists']}, Row: {js_values['rowExists']}")

            # Test the validation logic
            validation_result = await page.evaluate("""
                () => {
                    const datasetId = document.getElementById('dataset-selector').value;
                    const sequenceId = document.getElementById('sequence-selector').value;

                    const validations = {
                        datasetEmpty: !datasetId,
                        sequenceEmpty: !sequenceId,
                        datasetCheck: datasetId ? 'PASS' : 'FAIL - will alert and return',
                        sequenceCheck: sequenceId ? 'PASS' : 'FAIL - will alert and return'
                    };

                    return validations;
                }
            """)

            print(f"\n🔍 Validation checks:")
            print(f"   Dataset check: {validation_result['datasetCheck']}")
            print(f"   Sequence check: {validation_result['sequenceCheck']}")

            if validation_result['datasetCheck'] == 'PASS' and validation_result['sequenceCheck'] == 'PASS':
                print("✅ All validations should pass - API call should be made")

                # Test the URL construction
                api_url_test = await page.evaluate("""
                    () => {
                        const datasetId = document.getElementById('dataset-selector').value;
                        const sequenceId = document.getElementById('sequence-selector').value;
                        const apiUrl = `/api/v1/training-datasets/${datasetId}/sequences/${sequenceId}/multi-timeframe`;
                        return {
                            apiUrl: apiUrl,
                            urlLooksValid: apiUrl.includes('training-datasets') && apiUrl.includes('multi-timeframe')
                        };
                    }
                """)

                print(f"\n🌐 API URL construction:")
                print(f"   URL: {api_url_test['apiUrl']}")
                print(f"   Valid: {api_url_test['urlLooksValid']}")

                # Now manually test the fetch
                print(f"\n🧪 Testing fetch call manually...")
                fetch_test = await page.evaluate(f"""
                    async () => {{
                        try {{
                            const apiUrl = '/api/v1/training-datasets/63/sequences/AAPL_20250801_000000_20250801_000000/multi-timeframe';
                            console.log('Testing fetch to:', apiUrl);
                            const response = await fetch(apiUrl);
                            const data = await response.json();

                            return {{
                                success: true,
                                status: response.status,
                                hasData: !!data,
                                dataKeys: Object.keys(data || {{}})
                            }};
                        }} catch (error) {{
                            return {{
                                success: false,
                                error: error.message
                            }};
                        }}
                    }}
                """)

                print(f"   Fetch test result:")
                print(f"     Success: {fetch_test.get('success', False)}")
                if fetch_test.get('success'):
                    print(f"     Status: {fetch_test.get('status')}")
                    print(f"     Data keys: {fetch_test.get('dataKeys', [])}")
                else:
                    print(f"     Error: {fetch_test.get('error')}")

                return fetch_test.get('success', False)
            else:
                print("❌ Validation will fail - explains why no API call is made")
                return False

        except Exception as e:
            print(f"❌ Debug failed: {e}")
            return False

        finally:
            await browser.close()

if __name__ == "__main__":
    result = asyncio.run(debug_js_values())
    print(f"\n{'✅ Debug complete' if result else '❌ Issues found'}")
    exit(0 if result else 1)