#!/usr/bin/env python3
"""
Network Debug Test - Capture all HTTP requests from dashboard
"""

import asyncio
from playwright.async_api import async_playwright

BASE_URL = "http://localhost:4000"

async def debug_network_requests():
    print("🌐 NETWORK DEBUG - Capturing All HTTP Requests")
    print("=" * 60)
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=['--no-sandbox'])
        context = await browser.new_context()
        page = await context.new_page()
        
        # Track all network requests and responses
        requests = []
        responses = []
        
        page.on("request", lambda req: requests.append({
            'url': req.url,
            'method': req.method,
            'headers': dict(req.headers),
            'post_data': req.post_data
        }))
        
        page.on("response", lambda resp: responses.append({
            'url': resp.url,
            'status': resp.status,
            'status_text': resp.status_text,
            'headers': dict(resp.headers)
        }))
        
        try:
            print("📱 Loading Data Quality Dashboard...")
            
            # Load the page
            await page.goto(f"{BASE_URL}/data-quality/dashboard")
            await page.wait_for_load_state("domcontentloaded")
            
            # Wait for all network activity to complete
            await page.wait_for_timeout(8000)
            
            print(f"\n📊 CAPTURED REQUESTS ({len(requests)}):")
            print("-" * 40)
            
            for i, req in enumerate(requests, 1):
                print(f"{i:2d}. {req['method']} {req['url']}")
                if req['post_data']:
                    print(f"    POST DATA: {req['post_data']}")
            
            print(f"\n📊 CAPTURED RESPONSES ({len(responses)}):")
            print("-" * 40)
            
            for i, resp in enumerate(responses, 1):
                status_color = "✅" if resp['status'] == 200 else "❌"
                print(f"{i:2d}. {status_color} {resp['status']} {resp['status_text']} - {resp['url']}")
                
            # Find 404 errors
            errors_404 = [r for r in responses if r['status'] == 404]
            if errors_404:
                print(f"\n❌ 404 ERRORS FOUND ({len(errors_404)}):")
                print("-" * 40)
                for error in errors_404:
                    print(f"   404: {error['url']}")
                    
            # Find the data-quality API calls
            dq_calls = [r for r in responses if '/data-quality/api/issues' in r['url']]
            if dq_calls:
                print(f"\n🔍 DATA QUALITY API CALLS ({len(dq_calls)}):")
                print("-" * 40)
                for call in dq_calls:
                    print(f"   {call['status']}: {call['url']}")
            else:
                print(f"\n❌ NO DATA QUALITY API CALLS FOUND!")
                
            # Check if the issue is with favicon or other static resources
            static_404s = [r for r in responses if r['status'] == 404 and ('.ico' in r['url'] or '.js' in r['url'] or '.css' in r['url'])]
            api_404s = [r for r in responses if r['status'] == 404 and 'api' in r['url']]
            
            print(f"\n🔍 404 ERROR BREAKDOWN:")
            print(f"   Static Resource 404s: {len(static_404s)}")
            print(f"   API Endpoint 404s: {len(api_404s)}")
            
            if api_404s:
                print("   API 404 Details:")
                for api_error in api_404s:
                    print(f"     - {api_error['url']}")
            
            # Test the specific API call manually through the browser
            print(f"\n🧪 MANUAL API TEST FROM BROWSER:")
            api_test_result = await page.evaluate("""
                async () => {
                    try {
                        console.log('Testing API call...');
                        const response = await fetch('/data-quality/api/issues?page=1&page_size=5');
                        console.log('Response status:', response.status);
                        const text = await response.text();
                        console.log('Response text length:', text.length);
                        
                        let data;
                        try {
                            data = JSON.parse(text);
                        } catch (e) {
                            data = { parse_error: e.message, raw_text: text.substring(0, 200) };
                        }
                        
                        return {
                            success: true,
                            status: response.status,
                            ok: response.ok,
                            headers: Object.fromEntries([...response.headers.entries()]),
                            data_type: typeof data,
                            has_summary: !!data.summary,
                            has_issues: !!data.issues,
                            summary: data.summary,
                            issues_length: data.issues ? data.issues.length : 'no issues array',
                            raw_response: text.length > 500 ? text.substring(0, 500) + '...' : text
                        };
                    } catch (error) {
                        return {
                            success: false,
                            error: error.message
                        };
                    }
                }
            """)
            
            print(f"   API Call Success: {api_test_result['success']}")
            if api_test_result['success']:
                print(f"   Status: {api_test_result['status']} (OK: {api_test_result['ok']})")
                print(f"   Has Summary: {api_test_result['has_summary']}")
                print(f"   Has Issues: {api_test_result['has_issues']}")
                print(f"   Issues Length: {api_test_result['issues_length']}")
                if api_test_result['summary']:
                    print(f"   Summary: {api_test_result['summary']}")
                print(f"   Response Preview: {api_test_result['raw_response'][:200]}...")
            else:
                print(f"   Error: {api_test_result['error']}")
                
        except Exception as e:
            print(f"❌ Network debug error: {e}")
            
        finally:
            await browser.close()

if __name__ == "__main__":
    asyncio.run(debug_network_requests())