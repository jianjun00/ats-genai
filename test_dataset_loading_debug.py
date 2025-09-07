#!/usr/bin/env python3
"""
Test Dataset Loading Debug
Specifically test the dataset loading issue
"""

from playwright.sync_api import sync_playwright
import time

def test_dataset_loading():
    """Test dataset loading in browser with console capture."""
    print("🔍 Dataset Loading Debug Test")
    print("=" * 50)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()

        # Capture all console messages
        console_messages = []
        def handle_console(msg):
            message = f"[{msg.type}] {msg.text}"
            console_messages.append(message)
            print(f"🖥️  CONSOLE: {message}")

        page.on('console', handle_console)

        # Capture network requests
        def handle_request(request):
            if 'training-datasets' in request.url:
                print(f"🌐 REQUEST: {request.method} {request.url}")

        def handle_response(response):
            if 'training-datasets' in response.url:
                print(f"🌐 RESPONSE: {response.status} {response.url}")

        page.on('request', handle_request)
        page.on('response', handle_response)

        try:
            print("🎯 Step 1: Navigate to dashboard")
            page.goto("http://localhost:3000", timeout=30000)
            time.sleep(2)

            print("🎯 Step 2: Click Training Datasets button")
            page.click('button:has-text("Training Datasets")')

            print("🎯 Step 3: Wait for content to load")
            time.sleep(8)  # Give it more time to load and make API calls

            print("🎯 Step 4: Check dataset selector state")
            selector_exists = page.locator("#dataset-selector").count()
            print(f"📊 Dataset selector exists: {selector_exists > 0}")

            if selector_exists > 0:
                option_count = page.locator("#dataset-selector option").count()
                print(f"📊 Dataset options count: {option_count}")

                if option_count > 1:
                    # Get the text of the first few options
                    for i in range(min(3, option_count)):
                        option_text = page.locator(f"#dataset-selector option:nth-child({i+1})").text_content()
                        print(f"📊 Option {i+1}: {option_text}")
                else:
                    print("❌ No dataset options found")
            else:
                print("❌ Dataset selector not found")

            print("🎯 Step 5: Check for error messages")
            content_html = page.locator("#analysis-content").inner_html()
            if "Error" in content_html:
                print("❌ Error found in content")
                print(f"📝 Content: {content_html[:200]}...")

            print("🎯 Step 6: Summary of console messages")
            dataset_messages = [msg for msg in console_messages if 'DATASET DEBUG' in msg]
            print(f"📊 Dataset debug messages: {len(dataset_messages)}")

            for msg in dataset_messages[-10:]:  # Last 10 debug messages
                print(f"   {msg}")

        except Exception as e:
            print(f"❌ Error: {e}")

        finally:
            browser.close()

    print("\n✅ Dataset loading debug test complete")

if __name__ == "__main__":
    test_dataset_loading()