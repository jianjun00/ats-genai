#!/usr/bin/env python3
"""
Setup script for Playwright UI testing in ATS EDA tool
Installs Playwright and runs basic validation tests
"""

import subprocess
import sys
import os
import time

def run_command(cmd, description):
    """Run shell command with error handling"""
    print(f"🔧 {description}...")
    try:
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=300)
        if result.returncode == 0:
            print(f"✅ {description} completed successfully")
            return True
        else:
            print(f"❌ {description} failed: {result.stderr}")
            return False
    except subprocess.TimeoutExpired:
        print(f"⏰ {description} timed out after 5 minutes")
        return False
    except Exception as e:
        print(f"❌ {description} error: {e}")
        return False

def check_service_health():
    """Check if EDA service is running"""
    try:
        import requests
        response = requests.get("http://localhost:3000/health", timeout=5)
        if response.status_code == 200:
            print("✅ EDA service is running and healthy")
            return True
        else:
            print(f"⚠️ EDA service returned status {response.status_code}")
            return False
    except Exception as e:
        print(f"❌ EDA service not accessible: {e}")
        return False

def main():
    """Main setup function"""
    print("🎭 **PLAYWRIGHT SETUP FOR ATS EDA TOOL**")
    print("=" * 50)
    
    # Step 1: Install Python packages
    print("\n1️⃣ **Installing Playwright Python packages**")
    success = run_command(
        "pip install playwright pytest-playwright",
        "Installing Playwright and pytest plugin"
    )
    if not success:
        print("❌ Failed to install Playwright packages")
        return False
    
    # Step 2: Install browser binaries
    print("\n2️⃣ **Installing Playwright browsers**")
    success = run_command(
        "playwright install chromium",
        "Installing Chromium browser"
    )
    if not success:
        print("⚠️ Browser installation failed - trying alternative")
        # Fallback: install all browsers
        run_command("playwright install", "Installing all browsers (fallback)")
    
    # Step 3: Verify EDA service
    print("\n3️⃣ **Checking EDA service status**")
    service_healthy = check_service_health()
    if not service_healthy:
        print("🚀 Starting EDA service...")
        start_success = run_command(
            "python3 scripts/run_dev.py start --service analytics",
            "Starting EDA analytics service"
        )
        if start_success:
            time.sleep(5)  # Give service time to start
            service_healthy = check_service_health()
    
    # Step 4: Run basic validation test
    print("\n4️⃣ **Running Playwright validation test**")
    if service_healthy:
        # Create basic validation test
        validation_test = '''
import asyncio
from playwright.async_api import async_playwright

async def validate_setup():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        
        try:
            await page.goto("http://localhost:3000/eda", timeout=10000)
            title = await page.title()
            
            if "ATS EDA" in title:
                print("✅ Playwright can access EDA page successfully")
                
                # Check for unified tabs
                db_tab = page.locator("text=Database Tables")
                if await db_tab.count() > 0:
                    print("✅ Database Tables tab found")
                    
                training_tab = page.locator("text=Training Datasets")
                if await training_tab.count() > 0:
                    print("✅ Training Datasets tab found")
                
                return True
            else:
                print(f"❌ Unexpected page title: {title}")
                return False
                
        except Exception as e:
            print(f"❌ Validation test failed: {e}")
            return False
        finally:
            await browser.close()

# Run validation
if __name__ == "__main__":
    result = asyncio.run(validate_setup())
    exit(0 if result else 1)
        '''
        
        # Write and run validation test
        with open("/tmp/playwright_validation.py", "w") as f:
            f.write(validation_test)
        
        validation_success = run_command(
            "python3 /tmp/playwright_validation.py",
            "Running Playwright validation test"
        )
        
        # Cleanup
        os.remove("/tmp/playwright_validation.py")
        
        if validation_success:
            print("\n🎉 **PLAYWRIGHT SETUP COMPLETE!**")
            print("\n📋 **Next Steps:**")
            print("  1. Run full test suite:")
            print("     pytest tests/ui/playwright_eda_tests.py -v")
            print("  2. Run with visible browser:")
            print("     PLAYWRIGHT_HEADLESS=false pytest tests/ui/ -v")
            print("  3. Run specific test:")
            print("     pytest tests/ui/ -k 'test_database_tables_tab' -v")
            print("\n🌐 **EDA Tool Access**: http://localhost:3000/eda")
            return True
        else:
            print("❌ Validation test failed")
            return False
    else:
        print("❌ Cannot run validation without healthy EDA service")
        return False

if __name__ == "__main__":
    success = main()
    if success:
        print("\n✅ **PLAYWRIGHT READY FOR EDA UI TESTING!**")
    else:
        print("\n❌ **SETUP FAILED - CHECK ERRORS ABOVE**")
    sys.exit(0 if success else 1)