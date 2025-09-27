#!/usr/bin/env python3
"""
Fail-Fast Playwright Test Suite for Agent Start Functionality

This refactored version eliminates exception masking to reveal real issues.
Tests fail clearly when problems occur, enabling proper debugging.

BEFORE: Exception handling masked real issues
AFTER: Tests fail with clear stack traces revealing actual problems
"""

import asyncio
import pytest
from playwright.async_api import async_playwright, expect
import requests
import time
import json
from typing import Dict, Any

BASE_URL = "http://localhost:4000"

class AgentTestSuiteFailFast:
    def __init__(self, page):
        self.page = page
        self.test_results = []
    
    async def log_test_success(self, step: int, description: str, details: str = ""):
        """Log successful test step"""
        status = "✅ PASS"
        message = f"Step {step}: {status} - {description}"
        if details:
            message += f" | {details}"
        print(message)
        
        self.test_results.append({
            'step': step, 
            'description': description, 
            'success': True, 
            'details': details
        })

    async def test_step_1_service_health(self):
        """Step 1: Verify analytics service is healthy - NO EXCEPTION MASKING"""
        # Remove try/except - let real HTTP errors propagate with clear stack traces
        response = requests.get(f"{BASE_URL}/health", timeout=10)
        
        # Specific assertions that reveal actual problems
        assert response.status_code == 200, f"Expected 200, got {response.status_code}. Response: {response.text}"
        
        health_data = response.json()
        assert health_data.get('status') == 'healthy', f"Service not healthy: {health_data}"
        assert 'timestamp' in health_data, f"Health response missing timestamp: {health_data}"
        
        await self.log_test_success(1, "Service Health Check", 
                                   f"Status: {response.status_code}, Health: {health_data.get('status')}")

    async def test_step_2_reset_agent_to_idle(self):
        """Step 2: Reset agent to idle state - FAIL FAST ON REAL ISSUES"""
        # Check current status - let connection errors propagate
        status_response = requests.get(f"{BASE_URL}/agent/status", timeout=5)
        assert status_response.status_code == 200, f"Agent status endpoint failed: {status_response.status_code}"
        
        current_status = status_response.json()
        assert 'status' in current_status, f"Status response malformed: {current_status}"
        
        # Stop agent if running - let real failures surface
        if current_status.get('status') == 'active':
            stop_response = requests.post(f"{BASE_URL}/agent/stop", timeout=10)
            assert stop_response.status_code in [200, 202], f"Agent stop failed: {stop_response.status_code} - {stop_response.text}"
            await asyncio.sleep(2)
        
        # Restart service - let Docker command failures propagate
        import subprocess
        result = subprocess.run(
            ['docker-compose', '-f', 'docker-compose.intg.yml', 'restart', 'analytics-intg'], 
            capture_output=True, 
            text=True,
            timeout=60  # Explicit timeout instead of silent failure
        )
        
        assert result.returncode == 0, f"Docker restart failed (code {result.returncode}): stdout='{result.stdout}', stderr='{result.stderr}'"
        
        # Wait for service with clear failure detection
        await self._wait_for_service_ready()
        
        # Verify final state with specific assertions
        final_status_response = requests.get(f"{BASE_URL}/agent/status", timeout=5)
        assert final_status_response.status_code == 200, f"Agent status check failed after restart: {final_status_response.status_code}"
        
        final_status = final_status_response.json()
        assert final_status.get('status') == 'idle', f"Agent not idle after restart: {final_status}"
        assert 'agent_id' in final_status, f"Agent status missing agent_id: {final_status}"
        
        await self.log_test_success(2, "Reset Agent to Idle", 
                                   f"Agent status: {final_status.get('status')}, ID: {final_status.get('agent_id')}")

    async def _wait_for_service_ready(self, max_attempts: int = 10, delay: float = 2.0):
        """Wait for service to be ready - FAIL FAST with clear error"""
        for attempt in range(max_attempts):
            response = requests.get(f"{BASE_URL}/health", timeout=5)
            if response.status_code == 200:
                health_data = response.json()
                if health_data.get('status') == 'healthy':
                    return  # Success
            await asyncio.sleep(delay)
        
        # If we get here, service never came back up - FAIL CLEARLY
        raise RuntimeError(f"Service failed to become healthy after {max_attempts} attempts over {max_attempts * delay} seconds")

    async def test_step_3_navigate_to_dashboard(self):
        """Step 3: Navigate to dashboard - LET NAVIGATION ERRORS SURFACE"""
        # Remove exception handling - let Playwright errors propagate with full context
        await self.page.goto(f"{BASE_URL}")
        
        # Specific assertions that reveal actual DOM/loading issues
        await expect(self.page).to_have_title("ATS Analytics Platform", timeout=10000)
        
        # Wait for critical elements with clear failure messages
        dashboard_element = self.page.locator('[data-testid="dashboard"]')
        await expect(dashboard_element).to_be_visible(timeout=15000)
        
        # Verify dashboard functionality is actually loaded
        nav_menu = self.page.locator('nav')
        await expect(nav_menu).to_be_visible(timeout=5000)
        
        await self.log_test_success(3, "Navigate to Dashboard", "Dashboard loaded successfully")

    async def test_step_4_verify_agent_status_display(self):
        """Step 4: Verify agent status display - FAIL ON MISSING UI ELEMENTS"""
        # Let element location failures propagate with clear selectors
        agent_status_section = self.page.locator('[data-testid="agent-status"]')
        await expect(agent_status_section).to_be_visible(timeout=10000)
        
        # Check specific status text with exact expectations
        status_text = agent_status_section.locator('.status-text')
        await expect(status_text).to_be_visible(timeout=5000)
        
        status_value = await status_text.text_content()
        assert status_value in ['idle', 'stopped'], f"Unexpected agent status: '{status_value}'"
        
        # Verify start button is present and enabled
        start_button = self.page.locator('[data-testid="start-agent-button"]')
        await expect(start_button).to_be_visible(timeout=5000)
        await expect(start_button).to_be_enabled()
        
        await self.log_test_success(4, "Verify Agent Status Display", f"Status: {status_value}, Start button enabled")

    async def test_step_5_click_start_agent(self):
        """Step 5: Click start agent - LET CLICK FAILURES SURFACE"""
        start_button = self.page.locator('[data-testid="start-agent-button"]')
        
        # Ensure button is ready before clicking
        await expect(start_button).to_be_enabled(timeout=5000)
        
        # Click and let any JavaScript errors propagate
        await start_button.click()
        
        await self.log_test_success(5, "Click Start Agent", "Start button clicked successfully")

    async def test_step_6_verify_status_change_to_active(self):
        """Step 6: Verify status changes to active - FAIL ON TIMEOUT OR WRONG STATUS"""
        agent_status_section = self.page.locator('[data-testid="agent-status"]')
        status_text = agent_status_section.locator('.status-text')
        
        # Wait for status change with clear timeout failure
        await expect(status_text).to_have_text('active', timeout=30000)
        
        # Verify additional UI changes that should occur
        stop_button = self.page.locator('[data-testid="stop-agent-button"]')
        await expect(stop_button).to_be_visible(timeout=5000)
        await expect(stop_button).to_be_enabled()
        
        # Start button should be disabled when agent is active
        start_button = self.page.locator('[data-testid="start-agent-button"]')
        await expect(start_button).to_be_disabled(timeout=5000)
        
        await self.log_test_success(6, "Verify Status Change to Active", "Agent status changed to active, UI updated correctly")

    async def test_step_7_verify_api_endpoint_confirms_active(self):
        """Step 7: Verify API confirms active status - FAIL ON API INCONSISTENCY"""
        # Give a moment for backend to stabilize
        await asyncio.sleep(2)
        
        # Check API directly - let HTTP errors propagate
        response = requests.get(f"{BASE_URL}/agent/status", timeout=10)
        assert response.status_code == 200, f"Agent status API failed: {response.status_code} - {response.text}"
        
        status_data = response.json()
        assert status_data.get('status') == 'active', f"API reports wrong status: {status_data}"
        assert 'agent_id' in status_data, f"Missing agent_id in status: {status_data}"
        assert 'start_time' in status_data, f"Missing start_time in status: {status_data}"
        
        # Verify agent_id is valid
        agent_id = status_data.get('agent_id')
        assert agent_id is not None and agent_id.strip(), f"Invalid agent_id: '{agent_id}'"
        
        await self.log_test_success(7, "Verify API Confirms Active", 
                                   f"API status: {status_data.get('status')}, Agent ID: {agent_id}")

    async def test_step_8_verify_agent_activity_indicators(self):
        """Step 8: Verify activity indicators appear - FAIL ON MISSING INDICATORS"""
        # Check for activity indicators in UI
        activity_section = self.page.locator('[data-testid="agent-activity"]')
        await expect(activity_section).to_be_visible(timeout=15000)
        
        # Verify specific activity elements
        last_activity = activity_section.locator('[data-testid="last-activity"]')
        await expect(last_activity).to_be_visible(timeout=5000)
        
        activity_text = await last_activity.text_content()
        assert activity_text and activity_text.strip(), f"Empty activity text: '{activity_text}'"
        
        # Check for heartbeat or status updates
        status_updates = activity_section.locator('[data-testid="status-updates"]')
        await expect(status_updates).to_be_visible(timeout=10000)
        
        await self.log_test_success(8, "Verify Agent Activity Indicators", f"Activity shown: {activity_text}")

    async def run_comprehensive_test(self):
        """Run all test steps in sequence - FAIL FAST ON ANY STEP"""
        test_steps = [
            self.test_step_1_service_health,
            self.test_step_2_reset_agent_to_idle,
            self.test_step_3_navigate_to_dashboard,
            self.test_step_4_verify_agent_status_display,
            self.test_step_5_click_start_agent,
            self.test_step_6_verify_status_change_to_active,
            self.test_step_7_verify_api_endpoint_confirms_active,
            self.test_step_8_verify_agent_activity_indicators
        ]
        
        print("🚀 Starting fail-fast agent test suite...")
        print("❌ Tests will FAIL IMMEDIATELY on any issue to reveal root causes")
        
        for i, test_step in enumerate(test_steps, 1):
            print(f"\n📋 Executing step {i}: {test_step.__name__}")
            # NO exception handling - let failures propagate with full context
            await test_step()
        
        print(f"\n✅ ALL TESTS PASSED - Agent start functionality working correctly")
        return True


# Test runner without exception masking
async def test_agent_start_comprehensive_fail_fast():
    """Main test function - FAIL FAST WITH CLEAR ERRORS"""
    async with async_playwright() as p:
        # Launch browser - let startup failures propagate
        browser = await p.chromium.launch(headless=False, slow_mo=500)
        page = await browser.new_page()
        
        # Set up request/response logging for debugging
        page.on("requestfailed", lambda request: print(f"❌ Request failed: {request.url} - {request.failure}"))
        page.on("pageerror", lambda error: print(f"❌ Page error: {error}"))
        
        test_suite = AgentTestSuiteFailFast(page)
        
        # Run tests - any failure will propagate with full stack trace
        await test_suite.run_comprehensive_test()
        
        await browser.close()


if __name__ == "__main__":
    print("🔥 FAIL-FAST TESTING: All exceptions will propagate to reveal real issues")
    print("🚫 NO exception masking - debug actual problems instead of hiding them")
    
    # Let any startup failures propagate immediately
    asyncio.run(test_agent_start_comprehensive_fail_fast())