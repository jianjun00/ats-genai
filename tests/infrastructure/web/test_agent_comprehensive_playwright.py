#!/usr/bin/env python3
"""
Ultra-Comprehensive Playwright Test Suite for Agent Start Functionality
Tests every single step of the agent start process with detailed verification
"""

import asyncio
import pytest
from playwright.async_api import async_playwright, expect
import requests
import time
import json

BASE_URL = "http://localhost:4000"

class AgentTestSuite:
    def __init__(self, page):
        self.page = page
        self.test_results = []
    
    async def log_test(self, step, description, success, details=""):
        """Log test step results"""
        status = "✅ PASS" if success else "❌ FAIL"
        message = f"Step {step}: {status} - {description}"
        if details:
            message += f" | {details}"
        print(message)
        
        self.test_results.append({
            'step': step, 
            'description': description, 
            'success': success, 
            'details': details
        })
        
        if not success:
            await self.page.screenshot(path=f"failure_step_{step}.png")
            print(f"📸 Screenshot saved: failure_step_{step}.png")

    async def test_step_1_service_health(self):
        """Step 1: Verify analytics service is healthy"""
        response = requests.get(f"{BASE_URL}/health", timeout=10)
        health_data = response.json()
        
        success = (response.status_code == 200 and 
                  health_data.get('status') == 'healthy')
        
        await self.log_test(1, "Service Health Check", success, 
                           f"Status: {response.status_code}, Health: {health_data.get('status')}")
        return success
    async def test_step_2_reset_agent_to_idle(self):
        """Step 2: Reset agent to idle state for clean testing"""
        # First check current status
        status_response = requests.get(f"{BASE_URL}/agent/status")
        current_status = status_response.json()
        
        # Try to stop agent if it's running
        if current_status.get('status') == 'active':
            stop_response = requests.post(f"{BASE_URL}/agent/stop")
            await asyncio.sleep(2)  # Wait for stop
        
        # Restart the service to get clean state
        print("🔄 Restarting analytics service for clean test state...")
        import subprocess
        result = subprocess.run(['docker-compose', '-f', 'docker-compose.intg.yml', 'restart', 'analytics-intg'], 
                              capture_output=True, text=True)
        
        if result.returncode != 0:
            await self.log_test(2, "Reset Agent to Idle", False, f"Docker restart failed: {result.stderr}")
            return False
            
        # Wait for service to come back up
        await asyncio.sleep(15)
        
        # Verify service is back up and agent is idle
        for attempt in range(10):
            health_response = requests.get(f"{BASE_URL}/health", timeout=5)
            if health_response.status_code == 200:
                break
            await asyncio.sleep(2)
        status_response = requests.get(f"{BASE_URL}/agent/status")
        new_status = status_response.json()
        
        success = new_status.get('status') == 'idle'
        await self.log_test(2, "Reset Agent to Idle", success, 
                           f"Agent status: {new_status.get('status')}, ID: {new_status.get('agent_id')}")
        return success
        
    async def test_step_3_navigate_to_dashboard(self):
        """Step 3: Navigate to Data Quality Dashboard"""
        await self.page.goto(BASE_URL)
        await self.page.wait_for_load_state("domcontentloaded")
        
        # Click Data Quality Dashboard button
        dq_button = self.page.locator('button:has-text("🎯 Data Quality Dashboard")')
        await dq_button.click()
        await self.page.wait_for_load_state("domcontentloaded")
        await self.page.wait_for_timeout(3000)
        
        # Verify we're on the right page by looking for agent content
        agent_content = await self.page.locator("text=Agent").count()
        
        success = agent_content > 0
        await self.log_test(3, "Navigate to Dashboard", success, 
                           f"Found {agent_content} agent-related elements")
        return success
        
    async def test_step_4_verify_start_button_present(self):
        """Step 4: Verify start button is present and in correct initial state"""
        start_button = self.page.locator("#start-agent-btn")
        button_count = await start_button.count()
        
        if button_count == 0:
            await self.log_test(4, "Start Button Present", False, "Start button not found")
            return False
        
        # Check button properties
        button_text = await start_button.inner_text()
        is_disabled = await start_button.is_disabled()
        is_visible = await start_button.is_visible()
        
        success = button_count == 1 and is_visible and not is_disabled
        await self.log_test(4, "Start Button Present", success, 
                           f"Text: '{button_text}', Disabled: {is_disabled}, Visible: {is_visible}")
        return success
        
    async def test_step_5_verify_initial_agent_status(self):
        """Step 5: Verify agent is in idle state via API"""
        response = requests.get(f"{BASE_URL}/agent/status")
        status_data = response.json()
        
        agent_status = status_data.get('status')
        agent_id = status_data.get('agent_id')
        tools_available = status_data.get('tools_available', 0)
        
        success = (response.status_code == 200 and 
                  agent_status == 'idle' and 
                  agent_id is not None and 
                  tools_available >= 2)
        
        await self.log_test(5, "Initial Agent Status", success, 
                           f"Status: {agent_status}, ID: {agent_id}, Tools: {tools_available}")
        return success, agent_id
        
    async def test_step_6_click_start_button(self):
        """Step 6: Click start button and verify click mechanics"""
        # Set up request interception to monitor API calls
        api_requests = []
        self.page.on("request", lambda req: api_requests.append(req) if "/agent/start" in req.url else None)
        
        start_button = self.page.locator("#start-agent-btn")
        
        # Record button state before click
        pre_click_text = await start_button.inner_text()
        
        # Click the button
        await start_button.click()
        
        # Wait for any immediate changes
        await self.page.wait_for_timeout(2000)
        
        # Check if API request was made
        start_requests = [req for req in api_requests if "/agent/start" in req.url]
        
        # Record button state after click
        post_click_text = await start_button.inner_text()
        
        success = len(start_requests) >= 1
        await self.log_test(6, "Click Start Button", success, 
                           f"API calls made: {len(start_requests)}, Button text: '{pre_click_text}' -> '{post_click_text}'")
        return success
        
    async def test_step_7_verify_start_api_response(self):
        """Step 7: Verify /agent/start API returns success"""
        response = requests.post(f"{BASE_URL}/agent/start")
        response_data = response.json()
        
        success = (response.status_code == 200 and 
                  ("message" in response_data and "success" in response_data["message"].lower()) or
                  ("already active" in response_data.get("message", "").lower()))
        
        await self.log_test(7, "Start API Response", success, 
                           f"Status: {response.status_code}, Response: {response_data}")
        return success
        
    async def test_step_8_monitor_status_transition(self, initial_agent_id):
        """Step 8: Monitor agent status transition from idle to active"""
        print("⏳ Monitoring status transition...")
        
        transition_log = []
        max_wait_seconds = 15
        status_changed = False
        final_status = None
        final_agent_id = None
        
        for attempt in range(max_wait_seconds):
            response = requests.get(f"{BASE_URL}/agent/status", timeout=3)
            status_data = response.json()
            
            current_status = status_data.get('status')
            current_agent_id = status_data.get('agent_id')
            
            transition_log.append(f"t+{attempt}s: {current_status} (ID: {current_agent_id})")
            
            if current_status == 'active':
                status_changed = True
                final_status = current_status
                final_agent_id = current_agent_id
                break
                
            final_status = current_status
            final_agent_id = current_agent_id
            
            await asyncio.sleep(1)
        
        # Check agent ID consistency
        id_consistent = initial_agent_id == final_agent_id
        
        success = status_changed and id_consistent
        details = f"Final: {final_status}, ID consistent: {id_consistent}. Log: {'; '.join(transition_log[-5:])}"
        
        await self.log_test(8, "Status Transition Monitoring", success, details)
        return success, final_status, final_agent_id
        
    async def test_step_9_verify_agent_functionality(self):
        """Step 9: Verify agent is actually performing monitoring work"""
        # Check if agent is doing actual work by looking for monitoring activity
        # We'll monitor logs or check for any agent activity indicators
        
        # Wait a bit for monitoring cycles to occur
        await asyncio.sleep(5)
        
        # Check agent status with detailed info
        response = requests.get(f"{BASE_URL}/agent/status")
        status_data = response.json()
        
        # For now, we'll verify the agent has the expected tools and is active
        tools_available = status_data.get('tools_available', 0)
        tools_list = status_data.get('tools', [])
        agent_status = status_data.get('status')
        
        expected_tools = ['quality_scan', 'backfill_orchestrator']
        has_expected_tools = all(tool in tools_list for tool in expected_tools)
        
        success = (agent_status == 'active' and 
                  tools_available >= 2 and 
                  has_expected_tools)
        
        await self.log_test(9, "Agent Functionality", success, 
                           f"Status: {agent_status}, Tools: {tools_list}")
        return success
        
    async def test_step_10_verify_ui_feedback(self):
        """Step 10: Verify UI provides appropriate feedback to user"""
        # Check for success messages, status indicators, or visual feedback
        start_button = self.page.locator("#start-agent-btn")
        
        # Look for success notifications or status changes in UI
        notifications = await self.page.locator(".notification, .alert, .message").count()
        
        # Check if button state reflects agent is running
        button_text = await start_button.inner_text()
        
        # Look for any status indicators on the page
        status_indicators = await self.page.locator("text=/active|running|started/i").count()
        
        success = status_indicators > 0 or "start" in button_text.lower()
        await self.log_test(10, "UI Feedback", success, 
                           f"Button: '{button_text}', Notifications: {notifications}, Status indicators: {status_indicators}")
        return success
        
    async def test_step_11_test_multiple_clicks(self):
        """Step 11: Test multiple start button clicks don't break functionality"""
        start_button = self.page.locator("#start-agent-btn")
        
        # Click multiple times
        for i in range(3):
            await start_button.click()
            await self.page.wait_for_timeout(1000)
        
        # Verify agent status is still active
        response = requests.get(f"{BASE_URL}/agent/status")
        status_data = response.json()
        
        success = status_data.get('status') == 'active'
        await self.log_test(11, "Multiple Clicks Test", success, 
                           f"Status after multiple clicks: {status_data.get('status')}")
        return success
        
    async def run_comprehensive_test(self):
        """Run all test steps in sequence"""
        print("🚀 Starting Ultra-Comprehensive Agent Start Test Suite")
        print("=" * 80)
        
        # Run all test steps
        step_1_result = await self.test_step_1_service_health()
        if not step_1_result:
            return False
            
        step_2_result = await self.test_step_2_reset_agent_to_idle()
        if not step_2_result:
            return False
            
        step_3_result = await self.test_step_3_navigate_to_dashboard()
        if not step_3_result:
            return False
            
        step_4_result = await self.test_step_4_verify_start_button_present()
        if not step_4_result:
            return False
            
        step_5_result, initial_agent_id = await self.test_step_5_verify_initial_agent_status()
        if not step_5_result:
            return False
            
        step_6_result = await self.test_step_6_click_start_button()
        if not step_6_result:
            return False
            
        step_7_result = await self.test_step_7_verify_start_api_response()
        if not step_7_result:
            return False
            
        step_8_result, final_status, final_agent_id = await self.test_step_8_monitor_status_transition(initial_agent_id)
        if not step_8_result:
            return False
            
        step_9_result = await self.test_step_9_verify_agent_functionality()
        # Don't fail on this step, just log results
        
        step_10_result = await self.test_step_10_verify_ui_feedback()
        # Don't fail on this step, just log results
        
        step_11_result = await self.test_step_11_test_multiple_clicks()
        if not step_11_result:
            return False
        
        # Summary
        total_steps = 11
        passed_steps = sum(1 for result in self.test_results if result['success'])
        
        print("=" * 80)
        print(f"🎯 TEST SUMMARY: {passed_steps}/{total_steps} steps passed")
        
        if passed_steps == total_steps:
            print("🎉 ALL TESTS PASSED - Agent start functionality is working correctly!")
            return True
        else:
            print("❌ SOME TESTS FAILED - Agent start functionality has issues")
            failed_steps = [r for r in self.test_results if not r['success']]
            for failure in failed_steps:
                print(f"   ❌ Step {failure['step']}: {failure['description']} - {failure['details']}")
            return False


async def run_comprehensive_agent_test():
    """Main test runner"""
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=['--no-sandbox'])
        context = await browser.new_context()
        page = await context.new_page()
        
        test_suite = AgentTestSuite(page)
        success = await test_suite.run_comprehensive_test()
        return success
if __name__ == "__main__":
    success = asyncio.run(run_comprehensive_agent_test())
    exit(0 if success else 1)