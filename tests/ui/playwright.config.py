"""
Playwright configuration for EDA UI testing
"""

from playwright.sync_api import sync_playwright
import os

# Playwright configuration
PLAYWRIGHT_CONFIG = {
    # Browser settings
    "headless": os.getenv("PLAYWRIGHT_HEADLESS", "true").lower() == "true",
    "slow_mo": int(os.getenv("PLAYWRIGHT_SLOW_MO", "100")),
    
    # Viewport settings
    "viewport": {
        "width": int(os.getenv("PLAYWRIGHT_WIDTH", "1920")),
        "height": int(os.getenv("PLAYWRIGHT_HEIGHT", "1080"))
    },
    
    # Test settings
    "timeout": int(os.getenv("PLAYWRIGHT_TIMEOUT", "30000")),
    "base_url": os.getenv("EDA_BASE_URL", "http://localhost:3000"),
    
    # Recording settings
    "video_dir": "tests/ui/videos",
    "screenshot_dir": "tests/ui/screenshots",
    
    # Browser types to test
    "browsers": ["chromium", "firefox", "webkit"] if os.getenv("PLAYWRIGHT_ALL_BROWSERS") else ["chromium"]
}

def setup_playwright():
    """Setup Playwright with required browsers"""
    with sync_playwright() as p:
        for browser_name in PLAYWRIGHT_CONFIG["browsers"]:
            browser = getattr(p, browser_name)
            print(f"Setting up {browser_name}...")
            browser.launch()  # This will download the browser if needed

if __name__ == "__main__":
    setup_playwright()