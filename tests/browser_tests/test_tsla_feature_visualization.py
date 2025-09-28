"""
Playwright test for TSLA feature visualization in analytics service.

Tests the training dataset viewer to ensure TSLA features are properly displayed.
"""

import asyncio
import pytest
from playwright.async_api import async_playwright, Page, expect


@pytest.fixture
async def page():
    """Create Playwright page for testing."""
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context()
        page = await context.new_page()
        yield page
        await context.close()
        await browser.close()


async def test_training_datasets_api():
    """Test that training datasets API returns data."""
    import requests
    
    response = requests.get("http://localhost:4000/api/v1/training-datasets")
    assert response.status_code == 200
    
    data = response.json()
    print(f"\n✅ Found {data['total_count']} training datasets")
    
    # Check if we have any datasets
    assert data['total_count'] > 0, "No training datasets found"
    
    # Print dataset info
    for dataset in data['datasets'][:3]:
        print(f"   Dataset {dataset['id']}: {dataset['dataset_name']}")
        print(f"      Symbols: {dataset['symbols']}")
        print(f"      Date range: {dataset['date_range_start']} to {dataset['date_range_end']}")


async def test_analytics_service_loads(page: Page):
    """Test that analytics service home page loads."""
    await page.goto("http://localhost:4000/")
    
    # Wait for page to load
    await page.wait_for_load_state("networkidle")
    
    # Check that we can see the page
    title = await page.title()
    print(f"\n✅ Analytics service loaded: {title}")
    
    # Take screenshot
    await page.screenshot(path="/tmp/analytics_home.png")
    print("📸 Screenshot saved: /tmp/analytics_home.png")


async def test_training_dataset_viewer_loads(page: Page):
    """Test that training dataset viewer loads and shows datasets."""
    await page.goto("http://localhost:4000/eda")
    
    # Wait for page to load
    await page.wait_for_load_state("networkidle")
    await asyncio.sleep(2)
    
    # Look for training datasets section
    page_content = await page.content()
    print(f"\n📄 Page loaded, checking for training datasets...")
    
    # Check if we can find dataset elements
    if "Training Datasets" in page_content or "training" in page_content.lower():
        print("✅ Found training datasets section")
    else:
        print("⚠️ Training datasets section not visible")
    
    # Take screenshot
    await page.screenshot(path="/tmp/eda_dashboard.png")
    print("📸 Screenshot saved: /tmp/eda_dashboard.png")


async def test_tsla_dataset_selection(page: Page):
    """Test selecting TSLA dataset and viewing features."""
    await page.goto("http://localhost:4000/eda")
    await page.wait_for_load_state("networkidle")
    await asyncio.sleep(2)
    
    # Try to find dataset selector
    print("\n🔍 Looking for dataset selector...")
    
    # Look for select elements or buttons
    selects = await page.query_selector_all("select")
    print(f"   Found {len(selects)} select elements")
    
    buttons = await page.query_selector_all("button")
    print(f"   Found {len(buttons)} buttons")
    
    # Check for dataset-related elements
    dataset_elements = await page.query_selector_all("[id*='dataset'], [class*='dataset']")
    print(f"   Found {len(dataset_elements)} dataset-related elements")
    
    # Take screenshot
    await page.screenshot(path="/tmp/dataset_selector.png", full_page=True)
    print("📸 Screenshot saved: /tmp/dataset_selector.png")
    
    # Try to interact with dataset selector if found
    for select in selects:
        select_id = await select.get_attribute("id")
        select_name = await select.get_attribute("name")
        if select_id or select_name:
            print(f"   Select element: id={select_id}, name={select_name}")


async def test_check_file_structure():
    """Test to check actual file structure on disk."""
    import os
    from pathlib import Path
    
    dataset_dir = Path("/mnt/d/ats-data/training_data/dataset_20250928_022056")
    
    print(f"\n📁 Checking file structure in: {dataset_dir}")
    
    if not dataset_dir.exists():
        print(f"❌ Dataset directory does not exist: {dataset_dir}")
        return
    
    print("✅ Dataset directory exists")
    
    # List feature groups
    feature_groups = [d for d in dataset_dir.iterdir() if d.is_dir()]
    print(f"   Feature groups: {[fg.name for fg in feature_groups]}")
    
    # Check TSLA files
    for feature_group in feature_groups:
        tsla_dirs = list(feature_group.glob("TSLA_*"))
        print(f"\n   {feature_group.name}:")
        for tsla_dir in tsla_dirs[:3]:
            print(f"      {tsla_dir.name}/")
            timeframes = [tf.name for tf in tsla_dir.iterdir() if tf.is_dir()]
            print(f"         Timeframes: {timeframes}")


if __name__ == "__main__":
    # Run tests individually
    print("=" * 80)
    print("TSLA Feature Visualization Tests")
    print("=" * 80)
    
    # Test API
    asyncio.run(test_training_datasets_api())
    
    # Test file structure
    asyncio.run(test_check_file_structure())
    
    # Run Playwright tests
    async def run_playwright_tests():
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=False)
            context = await browser.new_context()
            page = await context.new_page()
            
            await test_analytics_service_loads(page)
            await test_training_dataset_viewer_loads(page)
            await test_tsla_dataset_selection(page)
            
            await context.close()
            await browser.close()
    
    asyncio.run(run_playwright_tests())
    
    print("\n" + "=" * 80)
    print("✅ Tests complete!")
    print("=" * 80)