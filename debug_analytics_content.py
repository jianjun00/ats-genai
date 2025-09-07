#!/usr/bin/env python3
"""Debug analytics content to see what's actually being rendered."""

import asyncio
from playwright.async_api import async_playwright

async def debug_page_content(url, service_name):
    """Debug what content is actually being rendered."""
    print(f"\n🔍 Debugging {service_name} content at {url}")

    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(headless=True)
        context = await browser.new_context()
        page = await context.new_page()

        response = await page.goto(f"{url}/eda")
        print(f"   Status: {response.status}")

        # Get all button texts
        buttons = await page.locator("button").all()
        print(f"   Found {len(buttons)} buttons:")
        for i, button in enumerate(buttons):
            text = await button.text_content()
            print(f"     {i+1}. '{text.strip()}'")

        # Get all text content
        body_text = await page.locator("body").text_content()
        print(f"   Full body text (first 500 chars):")
        print(f"   {body_text[:500]}...")

        # Check if onclick functions exist
        print("   Checking for onclick functions...")
        script_content = await page.evaluate("""
            () => {
                const buttons = document.querySelectorAll('button');
                const buttonInfo = [];
                buttons.forEach((btn, idx) => {
                    buttonInfo.push({
                        index: idx,
                        text: btn.textContent.trim(),
                        onclick: btn.getAttribute('onclick'),
                        hasClick: !!btn.onclick
                    });
                });
                return buttonInfo;
            }
        """)

        for info in script_content:
            print(f"     Button {info['index']}: '{info['text']}' - onclick: {info['onclick']}")

        await browser.close()

async def main():
    """Debug both services."""
    await debug_page_content("http://localhost:3000", "ATS-DEV")
    await debug_page_content("http://localhost:4000", "ATS-INTG")

if __name__ == "__main__":
    asyncio.run(main())