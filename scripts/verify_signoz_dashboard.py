#!/usr/bin/env python3
"""
SignOz Dashboard Verification Script

Uses proper SignOz API v4 endpoints and authentication to:
1. Verify metrics are accessible through SignOz
2. Create dashboard with proper schema
3. Test individual panel queries
4. Validate complete dashboard functionality
"""

import requests
import json
import time
from datetime import datetime, timedelta
import sys

class SignOzDashboardVerifier:
    def __init__(self, api_key, signoz_url="http://localhost:8080"):
        self.api_key = api_key
        self.signoz_url = signoz_url.rstrip('/')
        self.headers = {
            "SIGNOZ-API-KEY": api_key,
            "Content-Type": "application/json"
        }
        
    def test_authentication(self):
        """Test API authentication"""
        print("🔑 Testing SignOz API authentication...")
        try:
            response = requests.get(f"{self.signoz_url}/api/v1/dashboards", headers=self.headers)
            if response.status_code == 200:
                print("✅ Authentication successful")
                return True
            else:
                print(f"❌ Authentication failed: {response.status_code}")
                return False
        except Exception as e:
            print(f"❌ Authentication test failed: {e}")
            return False
    
    def verify_metrics_availability(self):
        """Verify our daily prices metrics are available through SignOz"""
        print("\n📊 Verifying metrics availability...")
        
        # Test metrics using Prometheus-compatible endpoint
        metrics_to_test = [
            "ats_daily_prices_coverage_percent",
            "ats_daily_prices_missing_symbols_total", 
            "ats_daily_prices_missing_records_total",
            "ats_daily_prices_bad_symbols_total",
            "ats_daily_prices_bad_records_total"
        ]
        
        available_metrics = []
        
        for metric in metrics_to_test:
            try:
                # Use Prometheus-compatible query endpoint
                url = f"{self.signoz_url}/api/v1/query"
                params = {"query": metric}
                
                response = requests.get(url, headers=self.headers, params=params)
                if response.status_code == 200:
                    data = response.json()
                    if data.get('data', {}).get('result'):
                        print(f"✅ {metric}: {len(data['data']['result'])} series found")
                        available_metrics.append(metric)
                    else:
                        print(f"⚠️ {metric}: Available but no data")
                        available_metrics.append(metric)
                else:
                    print(f"❌ {metric}: HTTP {response.status_code}")
            except Exception as e:
                print(f"❌ {metric}: Error - {e}")
        
        return available_metrics
    
    def test_query_range_api(self):
        """Test SignOz API v4 query_range endpoint"""
        print("\n🔍 Testing SignOz API v4 query_range...")
        
        try:
            # Use proper SignOz v4 API endpoint
            url = f"{self.signoz_url}/api/v4/query_range"
            
            # Calculate time range (last hour)
            end_time = int(time.time() * 1000000000)  # SignOz uses nanoseconds
            start_time = end_time - (3600 * 1000000000)  # 1 hour ago
            
            payload = {
                "start": start_time,
                "end": end_time,
                "step": 60,
                "queries": [
                    {
                        "query": "ats_daily_prices_coverage_percent",
                        "name": "coverage_test",
                        "legend": "Coverage Test"
                    }
                ],
                "compositeQuery": {
                    "buildOptions": {
                        "queryType": "promql",
                        "promqlOptions": {}
                    },
                    "queryType": "promql"
                }
            }
            
            response = requests.post(url, headers=self.headers, json=payload)
            print(f"API v4 Response Status: {response.status_code}")
            
            if response.status_code == 200:
                print("✅ SignOz API v4 query_range working")
                return True
            else:
                print(f"⚠️ API v4 returned: {response.text[:200]}")
                return False
                
        except Exception as e:
            print(f"❌ API v4 test failed: {e}")
            return False
    
    def create_proper_dashboard(self):
        """Create dashboard using proper SignOz schema"""
        print("\n🎨 Creating SignOz dashboard with proper schema...")
        
        dashboard_config = {
            "data": {
                "title": "📊 Daily Prices Quality Monitoring",
                "description": "Comprehensive monitoring of daily prices quality across all vendors",
                "tags": ["ats", "daily-prices", "data-quality", "monitoring"],
                "version": "v4",
                "layout": [
                    {"i": "missing-symbols", "x": 0, "y": 0, "w": 6, "h": 3},
                    {"i": "missing-records", "x": 6, "y": 0, "w": 6, "h": 3}, 
                    {"i": "coverage-percent", "x": 12, "y": 0, "w": 6, "h": 3},
                    {"i": "bad-symbols", "x": 18, "y": 0, "w": 6, "h": 3},
                    {"i": "coverage-trend", "x": 0, "y": 3, "w": 12, "h": 4},
                    {"i": "missing-trend", "x": 12, "y": 3, "w": 12, "h": 4}
                ],
                "widgets": [
                    {
                        "id": "missing-symbols",
                        "title": "🚨 Missing Symbols by Vendor",
                        "panelTypes": "value",
                        "query": {
                            "queryType": "promql",
                            "promql": [
                                {
                                    "query": "ats_daily_prices_missing_symbols_total",
                                    "legend": "{{vendor}} Missing Symbols",
                                    "disabled": False
                                }
                            ]
                        }
                    },
                    {
                        "id": "missing-records", 
                        "title": "📉 Missing Records by Vendor",
                        "panelTypes": "value",
                        "query": {
                            "queryType": "promql",
                            "promql": [
                                {
                                    "query": "ats_daily_prices_missing_records_total",
                                    "legend": "{{vendor}} Missing Records",
                                    "disabled": False
                                }
                            ]
                        }
                    },
                    {
                        "id": "coverage-percent",
                        "title": "✅ Coverage % by Vendor", 
                        "panelTypes": "value",
                        "query": {
                            "queryType": "promql",
                            "promql": [
                                {
                                    "query": "ats_daily_prices_coverage_percent",
                                    "legend": "{{vendor}} Coverage %",
                                    "disabled": False
                                }
                            ]
                        }
                    },
                    {
                        "id": "bad-symbols",
                        "title": "🚨 Bad Symbols by Vendor",
                        "panelTypes": "value",
                        "query": {
                            "queryType": "promql", 
                            "promql": [
                                {
                                    "query": "ats_daily_prices_bad_symbols_total",
                                    "legend": "{{vendor}} Bad Symbols",
                                    "disabled": False
                                }
                            ]
                        }
                    },
                    {
                        "id": "coverage-trend",
                        "title": "📈 Coverage Trend (24h)",
                        "panelTypes": "graph",
                        "query": {
                            "queryType": "promql",
                            "promql": [
                                {
                                    "query": "ats_daily_prices_coverage_percent",
                                    "legend": "{{vendor}} Coverage %",
                                    "disabled": False
                                }
                            ]
                        }
                    },
                    {
                        "id": "missing-trend",
                        "title": "🔍 Missing Data Trend (24h)", 
                        "panelTypes": "graph",
                        "query": {
                            "queryType": "promql",
                            "promql": [
                                {
                                    "query": "ats_daily_prices_missing_symbols_total",
                                    "legend": "{{vendor}} Missing Symbols",
                                    "disabled": False
                                }
                            ]
                        }
                    }
                ]
            }
        }
        
        try:
            url = f"{self.signoz_url}/api/v1/dashboards"
            response = requests.post(url, headers=self.headers, json=dashboard_config)
            
            if response.status_code == 200:
                data = response.json()
                dashboard_id = data.get('data', {}).get('id')
                print(f"✅ Dashboard created successfully: {dashboard_id}")
                return dashboard_id
            else:
                print(f"❌ Dashboard creation failed: {response.status_code}")
                print(f"Response: {response.text[:500]}")
                return None
                
        except Exception as e:
            print(f"❌ Dashboard creation error: {e}")
            return None
    
    def verify_dashboard_panels(self, dashboard_id):
        """Verify individual dashboard panels work correctly"""
        print(f"\n🔍 Verifying dashboard panels for {dashboard_id}...")
        
        # Get dashboard configuration
        try:
            url = f"{self.signoz_url}/api/v1/dashboards/{dashboard_id}"
            response = requests.get(url, headers=self.headers)
            
            if response.status_code == 200:
                dashboard = response.json()
                widgets = dashboard.get('data', {}).get('data', {}).get('widgets', [])
                
                print(f"✅ Dashboard retrieved: {len(widgets)} panels found")
                
                # Test each panel's query
                for widget in widgets:
                    panel_title = widget.get('title', 'Unknown')
                    query_info = widget.get('query', {})
                    promql_queries = query_info.get('promql', [])
                    
                    print(f"\n🧪 Testing panel: {panel_title}")
                    
                    for query_obj in promql_queries:
                        query = query_obj.get('query')
                        if query:
                            success = self._test_panel_query(query)
                            if success:
                                print(f"  ✅ Query works: {query}")
                            else:
                                print(f"  ❌ Query failed: {query}")
                
                return True
            else:
                print(f"❌ Failed to retrieve dashboard: {response.status_code}")
                return False
                
        except Exception as e:
            print(f"❌ Dashboard verification error: {e}")
            return False
    
    def _test_panel_query(self, query):
        """Test individual panel query"""
        try:
            url = f"{self.signoz_url}/api/v1/query"
            params = {"query": query}
            
            response = requests.get(url, headers=self.headers, params=params)
            return response.status_code == 200
        except:
            return False
    
    def generate_summary_report(self, dashboard_id, available_metrics):
        """Generate comprehensive verification report"""
        print("\n" + "="*60)
        print("📋 SIGNOZ DASHBOARD VERIFICATION REPORT")  
        print("="*60)
        
        print(f"\n🎯 Dashboard Details:")
        print(f"  Dashboard ID: {dashboard_id}")
        print(f"  SignOz URL: {self.signoz_url}")
        print(f"  Access URL: {self.signoz_url}/dashboard/{dashboard_id}")
        
        print(f"\n📊 Metrics Status:")
        print(f"  Available Metrics: {len(available_metrics)}/5")
        for metric in available_metrics:
            print(f"    ✅ {metric}")
        
        print(f"\n🚀 Next Steps:")
        print(f"  1. Open SignOz UI: {self.signoz_url}")
        print(f"  2. Navigate to Dashboards")
        print(f"  3. Find 'Daily Prices Quality Monitoring'")
        print(f"  4. Verify all panels display data correctly")
        
        print("\n" + "="*60)

def main():
    # Configuration
    API_KEY = "9RbijHam3W4B0a8h5fFB+7NgUgmXV+hFnzIPQUqtc6M="
    SIGNOZ_URL = "http://localhost:8080"
    
    print("🚀 SignOz Dashboard Verification Tool")
    print("="*50)
    
    # Initialize verifier
    verifier = SignOzDashboardVerifier(API_KEY, SIGNOZ_URL)
    
    # Step 1: Test authentication
    if not verifier.test_authentication():
        print("❌ Authentication failed. Cannot proceed.")
        sys.exit(1)
    
    # Step 2: Verify metrics availability
    available_metrics = verifier.verify_metrics_availability()
    if not available_metrics:
        print("❌ No metrics available. Run metrics generation first.")
        sys.exit(1)
    
    # Step 3: Test API v4 endpoint
    verifier.test_query_range_api()
    
    # Step 4: Create dashboard
    dashboard_id = verifier.create_proper_dashboard()
    if not dashboard_id:
        print("❌ Dashboard creation failed.")
        sys.exit(1)
    
    # Step 5: Verify dashboard panels
    verifier.verify_dashboard_panels(dashboard_id)
    
    # Step 6: Generate report
    verifier.generate_summary_report(dashboard_id, available_metrics)
    
    print("\n✅ Verification complete!")

if __name__ == "__main__":
    main()