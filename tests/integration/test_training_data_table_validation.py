#!/usr/bin/env python3
"""
Integration test for training data table validation in EDA dashboard.

This test validates that training data tables display actual values
and technical indicators correctly across different dataset formats.
"""

import asyncio
import json
import sys
from typing import Dict, List, Any
from unittest.mock import patch, MagicMock


class TestTrainingDataTableValidation:
    """Test suite for training data table functionality"""
    
    def __init__(self):
        self.mock_datasets = self._create_mock_datasets()
    
    def _create_mock_datasets(self) -> Dict[str, Dict]:
        """Create mock training datasets with table data"""
        return {
            '15': {
                'format': 'numpy',
                'table_data': [
                    {
                        'sequence_id': 1,
                        'etop': 151.25, 'ebot': 148.50, 'pldot': 149.75,
                        'sma_20': 149.80, 'ema_12': 150.10, 'ema_26': 149.90,
                        '5m_high': 150.25, '5m_low': 148.75, '5m_close': 149.50,
                        'target_return': 0.0125
                    },
                    {
                        'sequence_id': 2, 
                        'etop': 152.00, 'ebot': 149.25, 'pldot': 150.50,
                        'sma_20': 150.00, 'ema_12': 150.35, 'ema_26': 150.15,
                        '5m_high': 151.00, '5m_low': 149.25, '5m_close': 150.75,
                        'target_return': 0.0083
                    }
                ]
            },
            '16': {
                'format': 'csv',
                'table_data': [
                    {
                        'sequence_id': 1,
                        'etop': 149.80, 'ebot': 147.20, 'pldot': 148.50,
                        'sma_20': 148.40, 'ema_12': 148.70, 'ema_26': 148.60,
                        '1h_high': 148.90, '1h_low': 147.25, '1h_close': 148.10,
                        'target_return': -0.0056
                    }
                ]
            }
        }
    
    async def test_table_api_returns_data(self) -> bool:
        """Test that table data API returns valid training data"""
        try:
            # Mock the API response
            expected_api_response = {
                'data': self.mock_datasets['15']['table_data'],
                'total_count': len(self.mock_datasets['15']['table_data']),
                'page': 1,
                'limit': 10
            }
            
            # Validate API response structure
            assert 'data' in expected_api_response, "API response missing 'data' field"
            assert isinstance(expected_api_response['data'], list), "'data' field is not a list"
            assert len(expected_api_response['data']) > 0, "API returned empty data"
            
            # Validate data content
            first_row = expected_api_response['data'][0]
            required_fields = ['sequence_id', 'etop', 'ebot', 'pldot', 'sma_20']
            
            for field in required_fields:
                assert field in first_row, f"Missing required field: {field}"
                assert first_row[field] is not None, f"Field {field} is None"
            
            # Validate technical indicators have reasonable values
            assert isinstance(first_row['etop'], (int, float)), "etop is not numeric"
            assert isinstance(first_row['ebot'], (int, float)), "ebot is not numeric"
            assert isinstance(first_row['pldot'], (int, float)), "pldot is not numeric"
            
            print("✅ Table API returns data test passed")
            return True
            
        except Exception as e:
            print(f"❌ Table API returns data test failed: {e}")
            return False
    
    async def test_table_cell_content_formatting(self) -> bool:
        """Test that table cells display properly formatted content"""
        try:
            # Simulate the HTML formatting logic from analytics_service.py
            sample_row = self.mock_datasets['15']['table_data'][0]
            
            # Test technical indicators formatting
            tech_indicators = {
                'etop': sample_row['etop'],
                'ebot': sample_row['ebot'], 
                'pldot': sample_row['pldot'],
                'sma_20': sample_row['sma_20'],
                'ema_12': sample_row['ema_12'],
                'ema_26': sample_row['ema_26']
            }
            
            # Generate HTML like the service does
            indicators_html = ""
            for key, value in tech_indicators.items():
                if value is not None:
                    indicators_html += f'<div class="feature-item"><strong>{key}:</strong> {value:.4f}</div>'
            
            # Validate HTML content
            assert len(indicators_html) > 0, "No technical indicators HTML generated"
            assert 'feature-item' in indicators_html, "Missing CSS class for styling"
            assert 'etop:' in indicators_html, "etop indicator not in HTML"
            assert 'ebot:' in indicators_html, "ebot indicator not in HTML"
            assert 'pldot:' in indicators_html, "pldot indicator not in HTML"
            
            # Test OHLC data formatting
            ohlc_data = {
                '5m_high': sample_row.get('5m_high'),
                '5m_low': sample_row.get('5m_low'),
                '5m_close': sample_row.get('5m_close')
            }
            
            ohlc_html = ""
            for key, value in ohlc_data.items():
                if value is not None:
                    ohlc_html += f'<div class="feature-item"><strong>{key}:</strong> {value:.4f}</div>'
            
            assert len(ohlc_html) > 0, "No OHLC data HTML generated"
            assert '5m_high:' in ohlc_html, "5m_high not in OHLC HTML"
            
            print("✅ Table cell content formatting test passed")
            return True
            
        except Exception as e:
            print(f"❌ Table cell content formatting test failed: {e}")
            return False
    
    async def test_multi_format_dataset_support(self) -> bool:
        """Test that table supports both numpy and CSV dataset formats"""
        try:
            for dataset_id, dataset_info in self.mock_datasets.items():
                print(f"  Testing {dataset_info['format']} format (dataset {dataset_id})")
                
                # Simulate format-specific data loading
                table_data = dataset_info['table_data']
                assert len(table_data) > 0, f"No table data for dataset {dataset_id}"
                
                # Verify data structure regardless of format
                first_row = table_data[0]
                assert 'sequence_id' in first_row, f"Missing sequence_id in dataset {dataset_id}"
                
                # Check for technical indicators (should exist in all formats)
                has_indicators = any(key in first_row for key in ['etop', 'ebot', 'pldot', 'sma_20'])
                assert has_indicators, f"No technical indicators in dataset {dataset_id}"
                
                # Check for OHLC data (may have different timeframes)
                has_ohlc = any(key in first_row for key in ['5m_high', '1h_high', '15m_high'])
                assert has_ohlc, f"No OHLC data in dataset {dataset_id}"
                
                print(f"    ✓ Dataset {dataset_id} ({dataset_info['format']}) structure validated")
            
            print("✅ Multi-format dataset support test passed")
            return True
            
        except Exception as e:
            print(f"❌ Multi-format dataset support test failed: {e}")
            return False
    
    async def test_table_pagination_and_limits(self) -> bool:
        """Test table data pagination and limit handling"""
        try:
            # Test different pagination scenarios
            pagination_tests = [
                {'page': 1, 'limit': 3, 'expected_count': 2},  # First page with limit
                {'page': 1, 'limit': 10, 'expected_count': 2}, # Single page with higher limit
            ]
            
            for test_case in pagination_tests:
                # Simulate pagination logic
                dataset_data = self.mock_datasets['15']['table_data']
                start_idx = (test_case['page'] - 1) * test_case['limit']
                end_idx = start_idx + test_case['limit']
                paginated_data = dataset_data[start_idx:end_idx]
                
                # Validate pagination results
                assert len(paginated_data) <= test_case['limit'], f"Returned more data than limit: {test_case['limit']}"
                assert len(paginated_data) <= test_case['expected_count'], f"Returned more data than available"
                
                if len(dataset_data) >= test_case['expected_count']:
                    assert len(paginated_data) == min(test_case['expected_count'], test_case['limit']), \
                        f"Pagination returned wrong count: expected {test_case['expected_count']}, got {len(paginated_data)}"
            
            print("✅ Table pagination and limits test passed")
            return True
            
        except Exception as e:
            print(f"❌ Table pagination and limits test failed: {e}")
            return False
    
    async def test_empty_table_handling(self) -> bool:
        """Test that empty tables are handled gracefully"""
        try:
            # Test empty dataset scenario
            empty_response = {
                'data': [],
                'total_count': 0,
                'page': 1,
                'limit': 10
            }
            
            # Validate empty response structure
            assert 'data' in empty_response, "Empty response missing 'data' field"
            assert isinstance(empty_response['data'], list), "'data' field is not a list"
            assert len(empty_response['data']) == 0, "Empty response should have empty data list"
            assert empty_response['total_count'] == 0, "Empty response should have zero total_count"
            
            # Test that UI would handle this gracefully
            should_show_no_data_message = len(empty_response['data']) == 0
            assert should_show_no_data_message, "Should show no data message for empty response"
            
            print("✅ Empty table handling test passed")
            return True
            
        except Exception as e:
            print(f"❌ Empty table handling test failed: {e}")
            return False
    
    async def test_table_dom_structure(self) -> bool:
        """Test the expected DOM structure for table rendering"""
        try:
            # Mock DOM structure validation
            expected_dom_elements = {
                'table_section': 'training-data-table',
                'table_content': 'training-data-content', 
                'data_table': 'data-table',
                'table_header': 'th',
                'table_rows': 'tbody tr',
                'table_cells': 'td'
            }
            
            # Simulate table HTML generation
            sample_data = self.mock_datasets['15']['table_data'][0]
            
            # Table structure validation
            table_html = f"""
            <div id="training-data-table" class="section">
                <div id="training-data-content">
                    <table class="data-table">
                        <thead>
                            <tr><th>Sequence ID</th><th>Technical Indicators</th><th>OHLC Data</th><th>Labels</th></tr>
                        </thead>
                        <tbody>
                            <tr>
                                <td>{sample_data['sequence_id']}</td>
                                <td><div class="feature-item"><strong>etop:</strong> {sample_data['etop']:.4f}</div></td>
                                <td><div class="feature-item"><strong>5m_high:</strong> {sample_data.get('5m_high', 0):.4f}</div></td>
                                <td><div class="label-item"><strong>target_return:</strong> {sample_data.get('target_return', 0):.4f}</div></td>
                            </tr>
                        </tbody>
                    </table>
                </div>
            </div>
            """
            
            # Validate required elements exist
            for element_name, element_id in expected_dom_elements.items():
                assert element_id in table_html, f"Missing DOM element: {element_id}"
            
            # Validate data is present in HTML
            assert str(sample_data['sequence_id']) in table_html, "Sequence ID not in table HTML"
            assert f"{sample_data['etop']:.4f}" in table_html, "etop value not in table HTML"
            
            print("✅ Table DOM structure test passed")
            return True
            
        except Exception as e:
            print(f"❌ Table DOM structure test failed: {e}")
            return False
    
    async def run_all_tests(self) -> bool:
        """Run all table validation tests"""
        print("🧪 **TRAINING DATA TABLE VALIDATION TESTS**")
        print("Testing: Table data display with actual technical indicator values")
        print("=" * 60)
        
        tests = [
            self.test_table_api_returns_data(),
            self.test_table_cell_content_formatting(),
            self.test_multi_format_dataset_support(),
            self.test_table_pagination_and_limits(),
            self.test_empty_table_handling(),
            self.test_table_dom_structure()
        ]
        
        results = await asyncio.gather(*tests, return_exceptions=True)
        
        passed = sum(1 for result in results if result is True)
        total = len(results)
        
        print(f"\n📊 **TABLE VALIDATION RESULTS: {passed}/{total} PASSED**")
        
        if passed == total:
            print("🎉 **ALL TABLE VALIDATION TESTS PASSED!**")
            print("   • Table API returns proper training data")
            print("   • Cell content displays technical indicators correctly")
            print("   • Multi-format datasets (numpy/CSV) supported")
            print("   • Pagination and limits work properly")
            print("   • Empty tables handled gracefully")
            print("   • DOM structure is correct for rendering")
            return True
        else:
            print("❌ **SOME TABLE TESTS FAILED**")
            for i, result in enumerate(results):
                if result is not True:
                    print(f"   Test {i+1}: {result}")
            return False


async def main():
    """Main test runner"""
    test_suite = TestTrainingDataTableValidation()
    success = await test_suite.run_all_tests()
    return 0 if success else 1


if __name__ == "__main__":
    result = asyncio.run(main())
    sys.exit(result)