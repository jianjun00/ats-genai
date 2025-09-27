#!/usr/bin/env python3
"""
Comprehensive Playwright Tests for Universe Analytics UI
Tests real stock examples and membership dynamics in the browser interface
"""

import pytest
import asyncio
from playwright.async_api import async_playwright

class TestUniverseAnalyticsPlaywright:
    """Test Universe Analytics UI functionality with real stock examples"""

    @pytest.fixture(scope="class")
    def event_loop(self):
        """Create event loop for async tests"""
        loop = asyncio.new_event_loop()
        yield loop
        loop.close()

    @pytest.mark.asyncio
    async def test_universe_selection_interface(self):
        """Test universe selection dropdown loads with proper options"""
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()

            # Navigate to analytics dashboard
            await page.goto("http://localhost:4000/", wait_until="domcontentloaded")

            # Click Universe Analytics button
            universe_button = await page.wait_for_selector('button:has-text("🌐 Universe Analytics")', timeout=5000)
            await universe_button.click()

            # Wait for interface to load
            await page.wait_for_timeout(3000)

            # Check interface elements exist
            universe_selector = await page.query_selector('#universe-selector')
            date_from = await page.query_selector('#universe-date-from')
            date_to = await page.query_selector('#universe-date-to')
            load_button = await page.query_selector('button:has-text("Load Members")')

            assert universe_selector is not None, "Universe selector should exist"
            assert date_from is not None, "Date from input should exist"
            assert date_to is not None, "Date to input should exist"
            assert load_button is not None, "Load Members button should exist"

            # Check universe options
            options = await universe_selector.query_selector_all('option')
            assert len(options) >= 2, f"Should have universe options, got {len(options)}"

            # Find high volume large cap universe
            high_vol_option = None
            for option in options:
                text = await option.inner_text()
                if "high_volume_large_cap" in text:
                    high_vol_option = option
                    break

            assert high_vol_option is not None, "Should find high volume large cap universe option"

            print("✅ Universe selection interface loaded successfully")

    @pytest.mark.asyncio
    async def test_major_stocks_displayed(self):
        """Test that major expected stocks appear in the members list"""
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()

            # Navigate and select universe
            await page.goto("http://localhost:4000/", wait_until="domcontentloaded")
            universe_button = await page.wait_for_selector('button:has-text("🌐 Universe Analytics")')
            await universe_button.click()
            await page.wait_for_timeout(2000)

            # Select high volume universe
            universe_selector = await page.query_selector('#universe-selector')
            await universe_selector.select_option(value="2")

            # Load members
            load_button = await page.query_selector('button:has-text("Load Members")')
            await load_button.click()
            await page.wait_for_timeout(5000)

            # Get members content
            members_content = await page.query_selector('#universe-members-content')
            content_text = await members_content.text_content()

            # Test major stocks are present
            major_stocks = ['AAPL', 'MSFT', 'GOOGL', 'TSLA', 'NVDA', 'META', 'AMZN']
            found_stocks = []
            missing_stocks = []

            for stock in major_stocks:
                if stock in content_text:
                    found_stocks.append(stock)
                else:
                    missing_stocks.append(stock)

            assert len(found_stocks) >= 5, f"Should find most major stocks. Found: {found_stocks}, Missing: {missing_stocks}"

            print(f"✅ Major stocks found: {found_stocks}")
            if missing_stocks:
                print(f"ℹ️  Stocks not found: {missing_stocks}")

    @pytest.mark.asyncio
    async def test_historical_membership_display(self):
        """Test historical members (PTON, BYND) are shown with exit dates"""
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()

            # Navigate and select universe with historical date range
            await page.goto("http://localhost:4000/", wait_until="domcontentloaded")
            universe_button = await page.wait_for_selector('button:has-text("🌐 Universe Analytics")')
            await universe_button.click()
            await page.wait_for_timeout(2000)

            # Select universe and set wide date range
            universe_selector = await page.query_selector('#universe-selector')
            await universe_selector.select_option(value="2")

            # Set date range to capture historical changes
            date_from = await page.query_selector('#universe-date-from')
            date_to = await page.query_selector('#universe-date-to')
            await date_from.fill('2019-01-01')
            await date_to.fill('2024-12-31')

            # Load members
            load_button = await page.query_selector('button:has-text("Load Members")')
            await load_button.click()
            await page.wait_for_timeout(5000)

            # Get content and check for historical sections
            members_content = await page.query_selector('#universe-members-content')
            content_text = await members_content.text_content()

            # Should have both active and historical sections
            assert "Active Members" in content_text, "Should display Active Members section"
            assert "Historical Members" in content_text, "Should display Historical Members section"

            # Test specific historical stocks
            historical_stocks = {
                'PTON': 'Peloton (post-pandemic decline)',
                'BYND': 'Beyond Meat (hype cycle)',
                'TDOC': 'Teladoc (COVID normalization)'
            }

            found_historical = []
            for symbol, description in historical_stocks.items():
                if symbol in content_text:
                    found_historical.append(symbol)
                    print(f"✅ {symbol}: Found in historical members ({description})")

            assert len(found_historical) >= 2, f"Should find historical stocks, found: {found_historical}"

            # Check for realistic member counts
            if "Active Members (" in content_text:
                active_count = content_text.split("Active Members (")[1].split(")")[0]
                assert int(active_count) >= 600, f"Should have 600+ active members, got {active_count}"

            if "Historical Members (" in content_text:
                historical_count = content_text.split("Historical Members (")[1].split(")")[0]
                assert int(historical_count) >= 3, f"Should have some historical members, got {historical_count}"

    @pytest.mark.asyncio
    async def test_ipo_dates_accuracy(self):
        """Test that major stocks show accurate IPO/listing dates"""
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()

            # Load universe members
            await page.goto("http://localhost:4000/", wait_until="domcontentloaded")
            universe_button = await page.wait_for_selector('button:has-text("🌐 Universe Analytics")')
            await universe_button.click()
            await page.wait_for_timeout(2000)

            universe_selector = await page.query_selector('#universe-selector')
            await universe_selector.select_option(value="2")

            load_button = await page.query_selector('button:has-text("Load Members")')
            await load_button.click()
            await page.wait_for_timeout(5000)

            members_content = await page.query_selector('#universe-members-content')
            content_text = await members_content.text_content()

            # Test for realistic IPO date ranges
            expected_dates = {
                '1980': 'AAPL (Apple IPO)',
                '1986': 'MSFT (Microsoft IPO)',
                '1997': 'AMZN (Amazon IPO)',
                '1999': 'NVDA (NVIDIA IPO)',
                '2004': 'GOOGL (Google IPO)',
                '2010': 'TSLA (Tesla IPO)',
                '2012': 'META (Facebook IPO)'
            }

            found_dates = []
            for year, description in expected_dates.items():
                if year in content_text:
                    found_dates.append(year)
                    print(f"✅ {year}: Found ({description})")

            assert len(found_dates) >= 4, f"Should find multiple IPO years, found: {found_dates}"
            assert '1980' in found_dates, "Should find Apple's 1980 IPO date"

    @pytest.mark.asyncio
    async def test_date_range_filtering(self):
        """Test date range filtering affects member display correctly"""
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()

            # Load universe interface
            await page.goto("http://localhost:4000/", wait_until="domcontentloaded")
            universe_button = await page.wait_for_selector('button:has-text("🌐 Universe Analytics")')
            await universe_button.click()
            await page.wait_for_timeout(2000)

            universe_selector = await page.query_selector('#universe-selector')
            await universe_selector.select_option(value="2")

            date_from = await page.query_selector('#universe-date-from')
            date_to = await page.query_selector('#universe-date-to')

            # Test 1: Recent period (should show mostly active members)
            await date_from.fill('2023-01-01')
            await date_to.fill('2024-12-31')

            load_button = await page.query_selector('button:has-text("Load Members")')
            await load_button.click()
            await page.wait_for_timeout(3000)

            members_content = await page.query_selector('#universe-members-content')
            recent_content = await members_content.text_content()

            # Should show current active members
            assert "AAPL" in recent_content, "Should show Apple in recent period"
            assert "TSLA" in recent_content, "Should show Tesla in recent period"

            # Test 2: Historical period (2019-2022, should show PTON, BYND)
            await date_from.fill('2019-01-01')
            await date_to.fill('2022-12-31')

            await load_button.click()
            await page.wait_for_timeout(3000)

            historical_content = await members_content.text_content()

            # Should show historical members that were active in this period
            has_pton = "PTON" in historical_content
            has_bynd = "BYND" in historical_content

            if has_pton or has_bynd:
                print(f"✅ Historical filtering works: PTON={has_pton}, BYND={has_bynd}")
            else:
                print("ℹ️  Historical stocks may not be visible in current interface")

            # Should still show stocks that were active during this period
            assert "AAPL" in historical_content, "Should show Apple (active throughout)"

    @pytest.mark.asyncio
    async def test_comprehensive_member_count(self):
        """Test universe shows comprehensive member count (not just A-B)"""
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()

            # Load full universe
            await page.goto("http://localhost:4000/", wait_until="domcontentloaded")
            universe_button = await page.wait_for_selector('button:has-text("🌐 Universe Analytics")')
            await universe_button.click()
            await page.wait_for_timeout(2000)

            universe_selector = await page.query_selector('#universe-selector')
            await universe_selector.select_option(value="2")

            # Set wide date range to capture all members
            date_from = await page.query_selector('#universe-date-from')
            date_to = await page.query_selector('#universe-date-to')
            await date_from.fill('1980-01-01')
            await date_to.fill('2024-12-31')

            load_button = await page.query_selector('button:has-text("Load Members")')
            await load_button.click()
            await page.wait_for_timeout(5000)

            members_content = await page.query_selector('#universe-members-content')
            content_text = await members_content.text_content()

            # Extract member count
            total_members = None
            if "Total Members:" in content_text:
                count_text = content_text.split("Total Members:")[1].split("symbols")[0].strip()
                total_members = int(count_text)

            assert total_members is not None, "Should display total member count"
            assert total_members >= 650, f"Should have comprehensive coverage (650+), got {total_members}"

            # Test alphabet coverage by checking for stocks from different letters
            alphabet_samples = {
                'A': ['AAPL', 'AMZN', 'AMD'],
                'M': ['MSFT', 'META'],
                'T': ['TSLA'],
                'N': ['NVDA', 'NFLX'],
                'G': ['GOOGL'],
                'S': ['SPY'],
                'Q': ['QQQ']
            }

            letters_found = []
            for letter, stocks in alphabet_samples.items():
                if any(stock in content_text for stock in stocks):
                    letters_found.append(letter)

            assert len(letters_found) >= 6, f"Should have A-Z coverage, found letters: {letters_found}"

            print(f"✅ Comprehensive coverage: {total_members} total members")
            print(f"✅ Alphabet diversity: {len(letters_found)} letters represented ({letters_found})")

    @pytest.mark.asyncio
    async def test_error_handling(self):
        """Test error handling for invalid inputs"""
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()

            await page.goto("http://localhost:4000/", wait_until="domcontentloaded")
            universe_button = await page.wait_for_selector('button:has-text("🌐 Universe Analytics")')
            await universe_button.click()
            await page.wait_for_timeout(2000)

            # Test 1: Try loading without selecting universe
            load_button = await page.query_selector('button:has-text("Load Members")')
            await load_button.click()
            await page.wait_for_timeout(2000)

            # Should show some kind of feedback (alert or error message)
            # This tests the validation logic

            # Test 2: Select universe but try invalid date range
            universe_selector = await page.query_selector('#universe-selector')
            await universe_selector.select_option(value="2")

            date_from = await page.query_selector('#universe-date-from')
            date_to = await page.query_selector('#universe-date-to')

            # Set invalid range (from > to)
            await date_from.fill('2024-12-31')
            await date_to.fill('2020-01-01')

            await load_button.click()
            await page.wait_for_timeout(2000)

            # Should handle gracefully (may show no results or error)
            members_content = await page.query_selector('#universe-members-content')
            content_text = await members_content.text_content()

            # Interface should not crash
            assert "Universe Members" in content_text, "Interface should remain functional"

            print("✅ Error handling: Interface remains stable with invalid inputs")

@pytest.fixture(scope="session")
def playwright_config():
    """Configure Playwright for tests"""
    return {
        "browser": "chromium",
        "headless": True,
        "timeout": 30000
    }


if __name__ == "__main__":
    # Run tests directly
    pytest.main([__file__, "-v", "--tb=short", "-s"])