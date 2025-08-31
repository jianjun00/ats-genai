"""
Playwright configuration for EDA UI tests
"""

import pytest
import asyncio
from playwright.async_api import async_playwright, Browser, BrowserContext, Page

@pytest.fixture(scope="session")
def event_loop():
    """Create an instance of the default event loop for the test session."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()

@pytest.fixture(scope="session")
async def browser():
    """Launch browser for the session"""
    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,  # Set to False for debugging
            slow_mo=100     # Slow down for better visibility during debugging
        )
        yield browser
        await browser.close()

@pytest.fixture(scope="session") 
async def browser_context(browser):
    """Create browser context for the session"""
    context = await browser.new_context(
        viewport={"width": 1920, "height": 1080},
        ignore_https_errors=True,
        record_video_dir="tests/ui/videos" if not browser._impl_obj._is_headless else None
    )
    yield context
    await context.close()

@pytest.fixture
async def page(browser_context):
    """Create a new page for each test"""
    page = await browser_context.new_page()
    
    # Set up page error handling
    page.on("pageerror", lambda error: print(f"Page error: {error}"))
    page.on("console", lambda msg: print(f"Console {msg.type}: {msg.text}"))
    
    yield page
    await page.close()

@pytest.fixture
async def eda_page(page):
    """Pre-loaded EDA page fixture"""
    await page.goto("http://localhost:3000/eda", timeout=30000)
    await page.wait_for_load_state('networkidle')
    return page