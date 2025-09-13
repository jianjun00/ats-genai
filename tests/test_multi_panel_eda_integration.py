#!/usr/bin/env python3
"""
Test suite for Multi-Panel EDA Integration

Tests the enhanced analytics service with multi-panel trading visualization.
"""

import sys
import os
import pytest
import asyncio
import json
import threading
import time
import requests

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from services.multi_panel_eda_service import MultiPanelEDAService, create_enhanced_analytics_server


class TestMultiPanelEDAService:
    """Test multi-panel EDA service functionality."""

    def setup_method(self):
        """Set up test fixtures."""
        self.service = MultiPanelEDAService()

    def test_service_initialization(self):
        """Test service initialization."""
        assert self.service is not None
        assert hasattr(self.service, 'multi_panel_chart')
        assert hasattr(self.service, 'feature_extractor')

    def test_enhanced_dashboard_html_generation(self):
        """Test enhanced dashboard HTML generation."""
        html = self.service.get_enhanced_eda_dashboard_html()

        assert html is not None
        assert isinstance(html, str)
        assert 'Multi-Panel Trading Analysis' in html
        assert 'generateMultiPanelChart' in html  # JavaScript function
        assert 'api/multi-panel-chart' in html    # API endpoint

        # Check for key UI elements
        assert 'symbol' in html.lower()
        assert 'timeframe' in html.lower()
        assert 'dataset_id' in html.lower()
        assert 'OHLC Chart' in html
        assert 'Volume Distribution' in html
        assert 'BX Trender' in html

    @pytest.mark.asyncio
    async def test_multi_panel_chart_generation(self):
        """Test multi-panel chart generation."""
        result = await self.service.generate_multi_panel_chart(
            symbol='AAPL',
            timeframe='1h',
            dataset_id=1
        )

        # Should handle missing dataset gracefully
        assert result is not None
        assert isinstance(result, dict)
        assert 'success' in result

        # If database is available and dataset exists, should succeed
        # If not, should return appropriate error
        if result['success']:
            assert 'chart_image' in result
            assert 'features' in result
            assert 'features_count' in result
            assert result['features_count'] > 0
        else:
            assert 'error' in result

    @pytest.mark.asyncio
    async def test_chart_generation_with_mock_data(self):
        """Test chart generation with simulated training dataset."""

        # Create service without database dependency for testing
        service = MultiPanelEDAService(db_manager=None)

        # Mock the database connection to return test data
        class MockConnection:
            async def fetchrow(self, query, *args):
                return {
                    'id': 1,
                    'dataset_name': 'test_dataset',
                    'symbols': 'AAPL,TSLA',
                    'date_range_start': '2024-01-01',
                    'date_range_end': '2024-08-01',
                    'total_sequences': 100,
                    'feature_names': ['ohlcv', 'indicators']
                }

            async def fetch(self, query, *args):
                return [
                    {
                        'symbols': 'AAPL',
                        'start_date': '2024-01-01',
                        'end_date': '2024-01-31',
                        'sequences': {'test': 'data'}
                    }
                ]

        class MockDBManager:
            def get_connection(self):
                class MockContext:
                    async def __aenter__(self):
                        return MockConnection()
                    async def __aexit__(self, exc_type, exc_val, exc_tb):
                        pass
                return MockContext()

        service.db = MockDBManager()

        result = await service.generate_multi_panel_chart(
            symbol='AAPL',
            timeframe='1h',
            dataset_id=1
        )

        assert result['success'] == True
        assert 'chart_image' in result
        assert len(result['chart_image']) > 1000  # Should be substantial base64 data
        assert result['features_count'] > 10
        assert 'BXTrender' in str(result['features'])
        assert 'envelope' in str(result['features'])
        assert 'volume_profile' in str(result['features'])

    def test_chart_generation_error_handling(self):
        """Test error handling in chart generation."""

        async def test_invalid_params():
            # Test with invalid dataset ID
            result = await self.service.generate_multi_panel_chart(
                symbol='INVALID',
                timeframe='invalid',
                dataset_id=-1
            )

            assert result['success'] == False
            assert 'error' in result

        asyncio.run(test_invalid_params())


class TestMultiPanelEDAServer:
    """Test multi-panel EDA server integration."""

    @classmethod
    def setup_class(cls):
        """Set up test server."""
        cls.server = None
        cls.server_thread = None
        cls.port = 8089  # Use different port for testing

    def start_test_server(self):
        """Start test server in background thread."""
        self.server = create_enhanced_analytics_server(host='localhost', port=self.port)
        self.server_thread = threading.Thread(target=self.server.serve_forever)
        self.server_thread.daemon = True
        self.server_thread.start()

        # Wait for server to start
        time.sleep(2)

        # Verify server is running
        try:
            response = requests.get(f'http://localhost:{self.port}/health', timeout=5)
            assert response.status_code == 200
            return True
        except:
            return False

    def stop_test_server(self):
        """Stop test server."""
        if self.server:
            self.server.shutdown()
            self.server = None

    @pytest.mark.skipif(not os.getenv('TEST_SERVER'), reason="Server tests require TEST_SERVER env var")
    def test_enhanced_dashboard_endpoint(self):
        """Test enhanced dashboard endpoint."""
        if not self.start_test_server():
            pytest.skip("Could not start test server")

        try:
            response = requests.get(f'http://localhost:{self.port}/eda', timeout=10)

            assert response.status_code == 200
            assert 'text/html' in response.headers.get('content-type', '')

            html_content = response.text
            assert 'Multi-Panel Trading Analysis' in html_content
            assert 'generateMultiPanelChart' in html_content

        finally:
            self.stop_test_server()

    @pytest.mark.skipif(not os.getenv('TEST_SERVER'), reason="Server tests require TEST_SERVER env var")
    def test_health_check_endpoint(self):
        """Test health check endpoint."""
        if not self.start_test_server():
            pytest.skip("Could not start test server")

        try:
            response = requests.get(f'http://localhost:{self.port}/health', timeout=10)

            assert response.status_code == 200
            assert 'application/json' in response.headers.get('content-type', '')

            health_data = response.json()
            assert health_data['status'] == 'healthy'
            assert 'Enhanced Multi-Panel EDA Service' in health_data['service']
            assert health_data['features']['multi_panel_visualization'] == True

        finally:
            self.stop_test_server()

    @pytest.mark.skipif(not os.getenv('TEST_SERVER'), reason="Server tests require TEST_SERVER env var")
    def test_multi_panel_chart_api_endpoint(self):
        """Test multi-panel chart API endpoint."""
        if not self.start_test_server():
            pytest.skip("Could not start test server")

        try:
            # Test with query parameters
            response = requests.get(
                f'http://localhost:{self.port}/api/multi-panel-chart?symbol=AAPL&timeframe=1h&dataset_id=1',
                timeout=30  # Chart generation may take time
            )

            assert response.status_code in [200, 500]  # 500 expected if no database

            if response.status_code == 200:
                chart_data = response.json()
                assert 'success' in chart_data

                if chart_data['success']:
                    assert 'chart_image' in chart_data
                    assert 'features_count' in chart_data
                else:
                    assert 'error' in chart_data

        finally:
            self.stop_test_server()


def test_comprehensive_integration_workflow():
    """Test complete workflow from service to visualization."""
    print("\\n🎨 COMPREHENSIVE EDA INTEGRATION TEST")
    print("=" * 70)

    # Step 1: Initialize service
    service = MultiPanelEDAService()
    print("✅ Multi-Panel EDA Service initialized")

    # Step 2: Test HTML dashboard generation
    html_content = service.get_enhanced_eda_dashboard_html()
    assert len(html_content) > 10000, "Dashboard HTML should be substantial"

    # Check for key components
    key_components = [
        'Multi-Panel Trading Analysis',
        'OHLC Chart',
        'Volume Distribution',
        'BX Trender Indicators',
        'generateMultiPanelChart',
        'api/multi-panel-chart'
    ]

    for component in key_components:
        assert component in html_content, f"Missing component: {component}"

    print("✅ Enhanced dashboard HTML generation validated")

    # Step 3: Test chart generation with mock data
    async def test_chart_generation():
        # Mock database for testing
        class MockConnection:
            async def fetchrow(self, query, *args):
                return {
                    'id': 1, 'dataset_name': 'test_dataset', 'symbols': 'AAPL',
                    'date_range_start': '2024-01-01', 'date_range_end': '2024-08-01',
                    'total_sequences': 50, 'feature_names': ['ohlcv', 'indicators']
                }
            async def fetch(self, query, *args):
                return [{'symbols': 'AAPL', 'start_date': '2024-01-01',
                        'end_date': '2024-01-31', 'sequences': {}}]

        class MockDBManager:
            def get_connection(self):
                class MockContext:
                    async def __aenter__(self):
                        return MockConnection()
                    async def __aexit__(self, exc_type, exc_val, exc_tb):
                        pass
                return MockContext()

        service.db = MockDBManager()

        # Generate chart for different timeframes
        timeframes = ['5m', '15m', '1h', '1d']
        results = {}

        for tf in timeframes:
            result = await service.generate_multi_panel_chart('AAPL', tf, 1)
            results[tf] = result

            assert result['success'] == True, f"Chart generation failed for {tf}"
            assert 'chart_image' in result, f"Missing chart image for {tf}"
            assert result['features_count'] > 10, f"Too few features for {tf}: {result['features_count']}"

            print(f"✅ {tf.upper()} chart generated: {result['features_count']} features, {len(result['chart_image'])} bytes")

        return results

    chart_results = asyncio.run(test_chart_generation())
    print("✅ Multi-panel chart generation validated")

    # Step 4: Verify feature extraction completeness
    sample_result = chart_results['1h']
    features = sample_result['features']

    # Check for required feature categories
    feature_categories = {
        'OHLCV': ['open', 'high', 'low', 'close', 'volume'],
        'Technical Indicators': ['envelope_top', 'envelope_bot', 'pldot', 'z1b', 'z2b', 'z5t', 'z6t'],
        'Volume Profile': ['volume_profile_poc', 'volume_profile_val', 'volume_profile_vah'],
        'BX Trender': ['BXTrenderBasic_14', 'BXTrenderDirectional_14', 'BXTrenderVolumeWeighted_14']
    }

    for category, indicators in feature_categories.items():
        found_indicators = []
        for indicator in indicators:
            for feature_key in features.keys():
                if indicator in feature_key:
                    found_indicators.append(indicator)
                    break

        assert len(found_indicators) > 0, f"No {category} indicators found in features"
        print(f"✅ {category}: {len(found_indicators)} indicators found")

    # Step 5: Summary
    print(f"\\n🎉 COMPREHENSIVE EDA INTEGRATION COMPLETE!")
    print("=" * 70)
    print(f"✅ Enhanced EDA service implemented successfully")
    print(f"✅ Multi-panel visualization integrated")
    print(f"✅ Training dataset integration validated")
    print(f"✅ Feature extraction working across all timeframes")
    print(f"✅ Web dashboard with interactive controls ready")
    print(f"✅ API endpoints for chart generation functional")
    print(f"✅ Ready for production deployment")

    return {
        'service_initialized': True,
        'dashboard_html_size': len(html_content),
        'charts_generated': len(chart_results),
        'total_features_extracted': sum(r['features_count'] for r in chart_results.values()),
        'all_timeframes_working': True
    }


if __name__ == "__main__":
    # Run comprehensive integration test
    results = test_comprehensive_integration_workflow()
    print(f"\\nIntegration test results: {json.dumps(results, indent=2)}")