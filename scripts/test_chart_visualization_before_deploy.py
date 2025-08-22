#!/usr/bin/env python3
"""
Pre-Deployment Chart Visualization Regression Protection

This script must be run before ANY deployment to ensure the chart visualization
functionality is working correctly. It protects against regressions that would
break the user experience.

CRITICAL: Run this before deploying any changes to production!

Usage:
    python scripts/test_chart_visualization_before_deploy.py

Exit Codes:
    0 = All tests passed, safe to deploy
    1 = Tests failed, DO NOT DEPLOY
"""

import requests
import re
import json
import sys
from typing import List, Tuple, Optional


class ChartVisualizationValidator:
    """
    Validates all critical chart visualization functionality before deployment
    """
    
    def __init__(self, base_url: str = "http://localhost:3000"):
        self.base_url = base_url
        self.errors: List[str] = []
        self.warnings: List[str] = []
        
    def validate_service_running(self) -> bool:
        """Verify the service is accessible"""
        try:
            response = requests.get(f"{self.base_url}/health", timeout=5)
            if response.status_code == 200:
                print("✅ Service is running and accessible")
                return True
            else:
                self.errors.append(f"Service returned status {response.status_code}")
                return False
        except requests.exceptions.RequestException as e:
            self.errors.append(f"Service not accessible: {e}")
            return False
    
    def validate_chartjs_integration(self) -> bool:
        """CRITICAL: Validate Chart.js library integration"""
        print("\n🔍 Validating Chart.js Integration...")
        
        try:
            response = requests.get(f"{self.base_url}/")
            html = response.text
            
            # Test 1: Chart.js script tag present
            if not re.search(r'<script\s+src="[^"]*chart\.js[^"]*"', html, re.IGNORECASE):
                self.errors.append("CRITICAL: Chart.js script tag missing - charts will not work!")
                return False
                
            # Test 2: Correct CDN URL
            if "cdn.jsdelivr.net/npm/chart.js" not in html:
                self.errors.append("CRITICAL: Chart.js CDN URL incorrect")
                return False
                
            print("✅ Chart.js library properly included")
            return True
            
        except Exception as e:
            self.errors.append(f"Failed to validate Chart.js: {e}")
            return False
    
    def validate_modal_system(self) -> bool:
        """CRITICAL: Validate modal system for chart display"""
        print("\n🔍 Validating Modal System...")
        
        try:
            response = requests.get(f"{self.base_url}/")
            html = response.text
            
            required_modals = [
                ('id="distributions-modal"', 'Distributions modal'),
                ('id="ohlc-modal"', 'OHLC modal'),
                ('id="distributions-content"', 'Distributions content container'),
                ('id="ohlc-content"', 'OHLC content container'),
                ('onclick="closeModal(', 'Modal close functionality')
            ]
            
            for element, description in required_modals:
                if element not in html:
                    self.errors.append(f"CRITICAL: {description} missing - modals will not work!")
                    return False
            
            print("✅ Modal system properly configured")
            return True
            
        except Exception as e:
            self.errors.append(f"Failed to validate modal system: {e}")
            return False
    
    def validate_visualization_buttons(self) -> bool:
        """CRITICAL: Validate buttons use JavaScript instead of raw JSON links"""
        print("\n🔍 Validating Visualization Buttons...")
        
        try:
            response = requests.get(f"{self.base_url}/")
            html = response.text
            
            # Test 1: Buttons use JavaScript functions (not raw links)
            if not re.search(r'onclick="showDistributions\([^)]+\)"', html):
                self.errors.append("CRITICAL: Distribution buttons not using JavaScript - will show raw JSON!")
                return False
                
            if not re.search(r'onclick="showOHLC\([^)]+\)"', html):
                self.errors.append("CRITICAL: OHLC buttons not using JavaScript - will show raw JSON!")
                return False
            
            # Test 2: NO raw API links in buttons (this was the original bug!)
            if '"/api/v1/datasets/' in html.replace('fetch(`/api/v1/datasets/', ''):
                self.errors.append("CRITICAL: Raw API links still present - REGRESSION DETECTED!")
                return False
            
            # Test 3: Button styling present
            if 'class="btn-chart"' not in html:
                self.errors.append("WARNING: Chart button styling missing")
                self.warnings.append("Button styling may be broken")
            
            print("✅ Visualization buttons properly configured")
            return True
            
        except Exception as e:
            self.errors.append(f"Failed to validate buttons: {e}")
            return False
    
    def validate_javascript_functions(self) -> bool:
        """CRITICAL: Validate JavaScript chart functions are present"""
        print("\n🔍 Validating JavaScript Functions...")
        
        try:
            response = requests.get(f"{self.base_url}/")
            html = response.text
            
            required_functions = [
                ('async function showDistributions(', 'showDistributions function'),
                ('async function showOHLC(', 'showOHLC function'),
                ('function closeModal(', 'closeModal function'),
                ('new Chart(', 'Chart.js instantiation'),
                ('fetch(`/api/v1/datasets/${datasetId}/distributions`)', 'Distribution data fetch'),
                ('fetch(`/api/v1/datasets/${datasetId}/ohlc`)', 'OHLC data fetch')
            ]
            
            for function_check, description in required_functions:
                if function_check not in html:
                    self.errors.append(f"CRITICAL: {description} missing - charts will not work!")
                    return False
            
            print("✅ JavaScript functions properly defined")
            return True
            
        except Exception as e:
            self.errors.append(f"Failed to validate JavaScript: {e}")
            return False
    
    def validate_api_endpoints(self) -> bool:
        """CRITICAL: Validate backend APIs are working"""
        print("\n🔍 Validating API Endpoints...")
        
        try:
            # Test datasets API
            response = requests.get(f"{self.base_url}/api/v1/datasets")
            if response.status_code != 200:
                self.errors.append("CRITICAL: Datasets API broken!")
                return False
                
            datasets = response.json()
            if not datasets.get('datasets') or len(datasets['datasets']) == 0:
                self.errors.append("CRITICAL: No datasets available for visualization!")
                return False
            
            # Test with first dataset
            dataset_id = datasets['datasets'][0]['dataset_id']
            dataset_name = datasets['datasets'][0]['dataset_name']
            
            # Test distributions API
            dist_response = requests.get(f"{self.base_url}/api/v1/datasets/{dataset_id}/distributions")
            if dist_response.status_code != 200:
                self.errors.append(f"CRITICAL: Distributions API broken for {dataset_name}!")
                return False
                
            dist_data = dist_response.json()
            if not dist_data.get('distributions'):
                self.errors.append("CRITICAL: Distributions API response format invalid!")
                return False
            
            # Test OHLC API
            ohlc_response = requests.get(f"{self.base_url}/api/v1/datasets/{dataset_id}/ohlc")
            if ohlc_response.status_code != 200:
                self.errors.append(f"CRITICAL: OHLC API broken for {dataset_name}!")
                return False
                
            ohlc_data = ohlc_response.json()
            if not ohlc_data.get('ohlc_data'):
                self.errors.append("CRITICAL: OHLC API response format invalid!")
                return False
            
            print(f"✅ API endpoints working correctly ({len(datasets['datasets'])} datasets)")
            return True
            
        except Exception as e:
            self.errors.append(f"Failed to validate APIs: {e}")
            return False
    
    def validate_data_quality(self) -> bool:
        """Validate data quality for chart rendering"""
        print("\n🔍 Validating Data Quality...")
        
        try:
            # Get first dataset
            datasets_response = requests.get(f"{self.base_url}/api/v1/datasets")
            datasets = datasets_response.json()
            dataset_id = datasets['datasets'][0]['dataset_id']
            
            # Check distributions data
            dist_response = requests.get(f"{self.base_url}/api/v1/datasets/{dataset_id}/distributions")
            dist_data = dist_response.json()
            
            for feature_name, feature_data in dist_data['distributions'].items():
                # Validate histogram data
                if not feature_data.get('histogram_bins') or not feature_data.get('histogram_counts'):
                    self.errors.append(f"CRITICAL: Missing histogram data for {feature_name}")
                    return False
                    
                if len(feature_data['histogram_bins']) < 2:
                    self.errors.append(f"CRITICAL: Insufficient histogram bins for {feature_name}")
                    return False
            
            # Check OHLC data
            ohlc_response = requests.get(f"{self.base_url}/api/v1/datasets/{dataset_id}/ohlc")
            ohlc_data = ohlc_response.json()
            
            if len(ohlc_data['ohlc_data']) == 0:
                self.errors.append("CRITICAL: No OHLC data points available")
                return False
            
            # Validate OHLC data structure
            first_point = ohlc_data['ohlc_data'][0]
            required_fields = ['date', 'open', 'high', 'low', 'close', 'volume']
            for field in required_fields:
                if field not in first_point:
                    self.errors.append(f"CRITICAL: OHLC data missing {field} field")
                    return False
            
            print("✅ Data quality sufficient for chart rendering")
            return True
            
        except Exception as e:
            self.errors.append(f"Failed to validate data quality: {e}")
            return False
    
    def run_all_validations(self) -> bool:
        """Run all validations and return True if all pass"""
        print("🛡️  CHART VISUALIZATION REGRESSION PROTECTION")
        print("=" * 60)
        print("This test prevents deployments that would break chart visualizations")
        print("=" * 60)
        
        validations = [
            self.validate_service_running,
            self.validate_chartjs_integration,
            self.validate_modal_system,
            self.validate_visualization_buttons,
            self.validate_javascript_functions,
            self.validate_api_endpoints,
            self.validate_data_quality
        ]
        
        all_passed = True
        for validation in validations:
            if not validation():
                all_passed = False
        
        # Print results
        print("\n" + "=" * 60)
        if all_passed:
            print("🎉 ALL TESTS PASSED - SAFE TO DEPLOY!")
            if self.warnings:
                print("\n⚠️  Warnings:")
                for warning in self.warnings:
                    print(f"   • {warning}")
        else:
            print("🚨 TESTS FAILED - DO NOT DEPLOY!")
            print("\n❌ Critical Errors:")
            for error in self.errors:
                print(f"   • {error}")
        
        print("=" * 60)
        return all_passed


def main():
    """Main execution function"""
    validator = ChartVisualizationValidator()
    
    if validator.run_all_validations():
        print("\n✅ Chart visualization regression protection: PASSED")
        sys.exit(0)
    else:
        print("\n❌ Chart visualization regression protection: FAILED")
        print("\nFix the errors above before deploying!")
        sys.exit(1)


if __name__ == "__main__":
    main()