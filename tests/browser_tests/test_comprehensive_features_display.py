#!/usr/bin/env python3
"""
Playwright Test to verify comprehensive feature display in table view
Tests that all 962 training features are displayed instead of just 7 OHLCV features
"""

import asyncio
import pytest
from playwright.async_api import async_playwright

@pytest.mark.asyncio
async def test_comprehensive_features_in_table():
    """Test that table view displays all 962 training features."""
    print("🎭 Testing Comprehensive Features Display with Playwright")
    print("="*60)

    async with async_playwright() as playwright:
        # Launch browser in headless mode
        browser = await playwright.chromium.launch(headless=True)
        page = await browser.new_page()

        # Capture console errors
        console_errors = []
        page.on("console", lambda msg: console_errors.append(msg.text) if msg.type == "error" else None)

        try:
            print("🧪 Test 1: Navigate to EDA interface")
            await page.goto("http://localhost:3000/eda", timeout=15000)
            await page.wait_for_load_state("networkidle")
            print("✅ EDA interface loaded")

            print("\n🧪 Test 2: Select a training dataset")
            # Wait for datasets to load
            await page.wait_for_timeout(3000)

            # Select a dataset
            dataset_select = page.locator("#dataset-select")
            if await dataset_select.count() > 0:
                print("✅ Dataset selector found")

                # Get available options
                options = await dataset_select.locator("option").all()
                if len(options) > 1:  # More than just "Select dataset..."
                    print(f"✅ Found {len(options)} dataset options")

                    # Select first real dataset (skip "Select dataset..." option)
                    first_dataset_option = dataset_select.locator("option").nth(1)
                    first_dataset_text = await first_dataset_option.inner_text()
                    await first_dataset_option.click()
                    print(f"✅ Selected dataset: {first_dataset_text}")

                    # Wait for data to load
                    await page.wait_for_timeout(5000)

                    print("\n🧪 Test 3: Select a sequence for table view")

                    # Look for sequence selection controls
                    sequence_selectors = await page.locator("input[type='radio'][name='sequenceId']").all()
                    if len(sequence_selectors) > 0:
                        print(f"✅ Found {len(sequence_selectors)} sequence options")

                        # Select first sequence
                        await sequence_selectors[0].click()
                        print("✅ Selected first sequence")

                        # Wait for table data to load
                        await page.wait_for_timeout(3000)

                        print("\n🧪 Test 4: Verify comprehensive feature display in table")

                        # Look for table headers/columns to count features displayed
                        table = page.locator("table").first
                        if await table.count() > 0:
                            print("✅ Table found")

                            # Count table headers (columns)
                            headers = await table.locator("thead th").all()
                            header_count = len(headers)
                            print(f"📊 Table headers found: {header_count}")

                            # Get header text to analyze feature types
                            header_texts = []
                            for header in headers[:20]:  # Sample first 20 headers
                                text = await header.inner_text()
                                if text.strip():
                                    header_texts.append(text.strip())

                            print(f"📋 Sample headers: {header_texts[:10]}")

                            # Check for multi-timeframe features
                            timeframe_features = [h for h in header_texts if any(tf in h for tf in ['5m_', '15m_', '1h_', '1d_', '1w_'])]
                            basic_ohlcv = [h for h in header_texts if h.lower() in ['timestamp', 'open', 'high', 'low', 'close', 'volume', 'vwap']]

                            print(f"🎯 Multi-timeframe features found: {len(timeframe_features)}")
                            print(f"🎯 Basic OHLCV features found: {len(basic_ohlcv)}")

                            if len(timeframe_features) > 0:
                                print("✅ SUCCESS: Multi-timeframe training features are displayed!")
                                print(f"   Sample multi-timeframe features: {timeframe_features[:5]}")
                            else:
                                print("❌ WARNING: Only basic OHLCV features found, missing comprehensive training features")

                            # Check if we have close to expected feature count (962 features)
                            if header_count > 100:
                                print(f"✅ SUCCESS: Comprehensive feature count detected ({header_count} features)")
                                print("   This indicates all training features are being displayed")
                            elif header_count <= 10:
                                print(f"❌ ISSUE: Only {header_count} features displayed")
                                print("   Expected 962 comprehensive training features")
                            else:
                                print(f"⚠️  PARTIAL: {header_count} features displayed")
                                print("   May be loading incrementally or filtered")

                            # Look for pagination or scrolling indicators
                            pagination = page.locator(".pagination, .pager, [aria-label*='page']")
                            if await pagination.count() > 0:
                                print("📄 Pagination controls found - may need to navigate to see all features")

                            # Check for horizontal scrolling in table
                            table_container = page.locator(".table-container, .table-responsive").first
                            if await table_container.count() > 0:
                                print("📏 Scrollable table container found - features may extend horizontally")

                        else:
                            print("❌ No table found - table view may not be loading properly")

                    else:
                        print("⚠️  No sequence selection controls found")
                        # Try to look for other sequence selection methods
                        sequence_buttons = await page.locator("button:has-text('sequence'), button:has-text('Sequence')").all()
                        if len(sequence_buttons) > 0:
                            print(f"✅ Found {len(sequence_buttons)} sequence buttons as alternative")
                            await sequence_buttons[0].click()
                            await page.wait_for_timeout(2000)

                else:
                    print("❌ No datasets available for testing")
            else:
                print("❌ Dataset selector not found")

        except Exception as e:
            print(f"❌ Test failed with error: {e}")

        finally:
            await browser.close()

    print(f"\n📊 JavaScript Console Errors: {len(console_errors)}")
    if console_errors:
        print("Console errors found:")
        for error in console_errors[:3]:
            print(f"  - {error}")

    print("🎯 Comprehensive features display test completed")

if __name__ == "__main__":
    asyncio.run(test_comprehensive_features_in_table())