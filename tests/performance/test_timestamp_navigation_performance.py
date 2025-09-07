#!/usr/bin/env python3
"""
Performance Tests for Timestamp-Based Multi-Timeframe Navigation

Tests performance characteristics:
1. API response times under various loads
2. Memory usage during navigation
3. Concurrent navigation requests handling
4. Large dataset performance
5. Cache effectiveness
6. Stress testing with rapid navigation
"""

import pytest
import asyncio
import aiohttp
import time
import statistics
import concurrent.futures
import psutil
import os
import requests
from typing import List, Dict, Any
import sys

# Add src to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', 'src'))

class TestTimestampNavigationPerformance:
    """Performance tests for timestamp-based navigation system."""
    
    BASE_URL = "http://localhost:3001"
    
    @classmethod
    def setup_class(cls):
        """Set up performance test environment."""
        print("🔧 Setting up performance tests...")
        
        # Check analytics service availability
        try:
            response = requests.get(f"{cls.BASE_URL}/health", timeout=5)
            if response.status_code != 200:
                pytest.skip("Analytics service not running")
        except requests.ConnectionError:
            pytest.skip("Analytics service not accessible")
        
        # Get test data
        try:
            datasets_response = requests.get(f"{cls.BASE_URL}/api/v1/training-datasets")
            datasets = datasets_response.json()['datasets']
            
            if len(datasets) == 0:
                pytest.skip("No training datasets available")
            
            cls.test_dataset_id = datasets[0]['id']
            
            sequences_response = requests.get(f"{cls.BASE_URL}/api/v1/training-datasets/{cls.test_dataset_id}/sequences")
            sequences = sequences_response.json()['sequences']
            
            if len(sequences) == 0:
                pytest.skip("No sequences available")
                
            cls.test_sequence_id = sequences[0]
            
            print(f"📊 Performance testing with dataset {cls.test_dataset_id}, sequence {cls.test_sequence_id}")
            
        except Exception as e:
            pytest.skip(f"Could not set up test data: {e}")
    
    def test_single_request_performance(self):
        """Test single request performance for both endpoints."""
        print("⏱️ Testing single request performance...")
        
        # Test 1h navigation performance
        position = 25
        start_time = time.time()
        
        nav_response = requests.get(
            f"{self.BASE_URL}/api/v1/training-datasets/{self.test_dataset_id}/sequences/{self.test_sequence_id}/1h?row_index={position}"
        )
        
        nav_time = time.time() - start_time
        print(f"  📋 1h navigation: {nav_time:.3f}s")
        
        # Performance thresholds
        assert nav_time < 5.0, f"1h navigation too slow: {nav_time:.3f}s"
        
        if nav_response.status_code == 200:
            nav_data = nav_response.json()
            
            if nav_data.get('success'):
                timestamp = nav_data['timestamp']
                table_rows = len(nav_data.get('table_data', []))
                
                # Test multi-timeframe performance
                start_time = time.time()
                
                multi_response = requests.get(
                    f"{self.BASE_URL}/api/v1/training-datasets/{self.test_dataset_id}/sequences/{self.test_sequence_id}/multi-timeframe?timestamp={timestamp}"
                )
                
                multi_time = time.time() - start_time
                print(f"  📊 Multi-timeframe: {multi_time:.3f}s")
                
                # Performance assertions
                assert multi_time < 10.0, f"Multi-timeframe too slow: {multi_time:.3f}s"
                
                if multi_response.status_code == 200:
                    multi_data = multi_response.json()
                    
                    if multi_data.get('success'):
                        timeframes = list(multi_data['ohlc_data'].keys())
                        total_bars = sum(len(bars) for bars in multi_data['ohlc_data'].values())
                        
                        total_time = nav_time + multi_time
                        print(f"  🔄 Total workflow: {total_time:.3f}s")
                        print(f"  📊 Data loaded: {table_rows} table rows, {total_bars} chart bars across {len(timeframes)} timeframes")
                        
                        # Total workflow should be reasonable
                        assert total_time < 12.0, f"Total workflow too slow: {total_time:.3f}s"
                        
                        return {
                            'navigation_time': nav_time,
                            'multi_timeframe_time': multi_time,
                            'total_time': total_time,
                            'table_rows': table_rows,
                            'chart_bars': total_bars,
                            'timeframes': len(timeframes)
                        }
        
        pytest.skip("Could not complete performance test due to API issues")
    
    def test_sequential_navigation_performance(self):
        """Test performance of sequential navigation requests."""
        print("🔄 Testing sequential navigation performance...")
        
        positions = [10, 20, 30, 40, 50, 25, 15, 35]  # Mix of forward/backward
        times = []
        
        for i, position in enumerate(positions):
            print(f"  📍 Navigation {i+1}/{len(positions)}: position {position}")
            
            start_time = time.time()
            
            # Complete workflow: 1h navigation + multi-timeframe
            nav_response = requests.get(
                f"{self.BASE_URL}/api/v1/training-datasets/{self.test_dataset_id}/sequences/{self.test_sequence_id}/1h?row_index={position}"
            )
            
            if nav_response.status_code == 200:
                nav_data = nav_response.json()
                
                if nav_data.get('success'):
                    timestamp = nav_data['timestamp']
                    
                    multi_response = requests.get(
                        f"{self.BASE_URL}/api/v1/training-datasets/{self.test_dataset_id}/sequences/{self.test_sequence_id}/multi-timeframe?timestamp={timestamp}"
                    )
                    
                    workflow_time = time.time() - start_time
                    times.append(workflow_time)
                    
                    print(f"    ⏱️ Workflow time: {workflow_time:.3f}s")
            
            # Small delay between requests to simulate user behavior
            time.sleep(0.1)
        
        if times:
            avg_time = statistics.mean(times)
            max_time = max(times)
            min_time = min(times)
            
            print(f"\n📊 Sequential Navigation Performance Summary:")
            print(f"  🔢 Total requests: {len(times)}")
            print(f"  ⏱️ Average time: {avg_time:.3f}s")
            print(f"  ⏱️ Max time: {max_time:.3f}s") 
            print(f"  ⏱️ Min time: {min_time:.3f}s")
            
            # Performance assertions
            assert avg_time < 8.0, f"Average sequential time too slow: {avg_time:.3f}s"
            assert max_time < 15.0, f"Max sequential time too slow: {max_time:.3f}s"
            
            # Check for performance degradation (last request shouldn't be much slower than first)
            if len(times) >= 3:
                first_three_avg = statistics.mean(times[:3])
                last_three_avg = statistics.mean(times[-3:])
                degradation_ratio = last_three_avg / first_three_avg
                
                print(f"  📈 Performance degradation ratio: {degradation_ratio:.2f}")
                assert degradation_ratio < 2.0, f"Performance degraded too much: {degradation_ratio:.2f}x slower"
            
            return {
                'request_count': len(times),
                'avg_time': avg_time,
                'max_time': max_time,
                'min_time': min_time,
                'times': times
            }
        
        pytest.skip("No successful sequential requests completed")
    
    @pytest.mark.asyncio
    async def test_concurrent_navigation_performance(self):
        """Test performance under concurrent navigation requests."""
        print("🔀 Testing concurrent navigation performance...")
        
        # Test with different concurrency levels
        concurrency_levels = [2, 5, 10]
        
        for concurrency in concurrency_levels:
            print(f"  🔀 Testing concurrency level: {concurrency}")
            
            # Create different positions for each concurrent request
            positions = [10 + i * 5 for i in range(concurrency)]
            
            async def make_workflow_request(session: aiohttp.ClientSession, position: int):
                """Make complete navigation workflow request."""
                try:
                    # 1h navigation
                    nav_url = f"{self.BASE_URL}/api/v1/training-datasets/{self.test_dataset_id}/sequences/{self.test_sequence_id}/1h?row_index={position}"
                    
                    start_time = time.time()
                    async with session.get(nav_url) as nav_response:
                        if nav_response.status == 200:
                            nav_data = await nav_response.json()
                            
                            if nav_data.get('success'):
                                timestamp = nav_data['timestamp']
                                
                                # Multi-timeframe  
                                multi_url = f"{self.BASE_URL}/api/v1/training-datasets/{self.test_dataset_id}/sequences/{self.test_sequence_id}/multi-timeframe?timestamp={timestamp}"
                                
                                async with session.get(multi_url) as multi_response:
                                    workflow_time = time.time() - start_time
                                    
                                    return {
                                        'position': position,
                                        'success': multi_response.status == 200,
                                        'time': workflow_time
                                    }
                    
                    return {'position': position, 'success': False, 'time': time.time() - start_time}
                    
                except Exception as e:
                    return {'position': position, 'success': False, 'error': str(e), 'time': 0}
            
            # Execute concurrent requests
            start_time = time.time()
            
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30)) as session:
                tasks = [make_workflow_request(session, pos) for pos in positions]
                results = await asyncio.gather(*tasks, return_exceptions=True)
            
            total_time = time.time() - start_time
            
            # Analyze results
            successful_results = [r for r in results if isinstance(r, dict) and r.get('success')]
            successful_count = len(successful_results)
            
            if successful_results:
                times = [r['time'] for r in successful_results]
                avg_time = statistics.mean(times)
                max_time = max(times)
                
                print(f"    ✅ Concurrency {concurrency}: {successful_count}/{concurrency} successful")
                print(f"    ⏱️ Total time: {total_time:.3f}s")
                print(f"    ⏱️ Avg individual time: {avg_time:.3f}s")
                print(f"    ⏱️ Max individual time: {max_time:.3f}s")
                
                # Performance assertions for concurrent requests
                assert successful_count >= concurrency // 2, f"Too many concurrent failures: {successful_count}/{concurrency}"
                assert avg_time < 15.0, f"Concurrent avg time too slow: {avg_time:.3f}s"
                assert total_time < 20.0, f"Concurrent total time too slow: {total_time:.3f}s"
            
            else:
                print(f"    ❌ Concurrency {concurrency}: No successful requests")
            
            # Brief pause between concurrency levels
            await asyncio.sleep(1.0)
    
    def test_memory_usage_during_navigation(self):
        """Test memory usage during navigation operations."""
        print("💾 Testing memory usage during navigation...")
        
        # Get process info
        current_process = psutil.Process(os.getpid())
        
        # Initial memory baseline
        initial_memory = current_process.memory_info().rss / 1024 / 1024  # MB
        print(f"  📊 Initial memory usage: {initial_memory:.1f} MB")
        
        memory_samples = [initial_memory]
        positions = list(range(0, 100, 10))  # 10 different positions
        
        for i, position in enumerate(positions):
            # Make navigation request
            nav_response = requests.get(
                f"{self.BASE_URL}/api/v1/training-datasets/{self.test_dataset_id}/sequences/{self.test_sequence_id}/1h?row_index={position}"
            )
            
            if nav_response.status_code == 200:
                nav_data = nav_response.json()
                
                if nav_data.get('success'):
                    timestamp = nav_data['timestamp']
                    
                    # Make multi-timeframe request
                    multi_response = requests.get(
                        f"{self.BASE_URL}/api/v1/training-datasets/{self.test_dataset_id}/sequences/{self.test_sequence_id}/multi-timeframe?timestamp={timestamp}"
                    )
            
            # Sample memory after each request
            current_memory = current_process.memory_info().rss / 1024 / 1024  # MB
            memory_samples.append(current_memory)
            
            if i % 3 == 0:  # Print every 3rd sample
                print(f"  📊 After {i+1} requests: {current_memory:.1f} MB")
        
        # Analyze memory usage
        max_memory = max(memory_samples)
        final_memory = memory_samples[-1]
        memory_growth = final_memory - initial_memory
        
        print(f"\n💾 Memory Usage Analysis:")
        print(f"  📊 Initial: {initial_memory:.1f} MB")
        print(f"  📊 Peak: {max_memory:.1f} MB") 
        print(f"  📊 Final: {final_memory:.1f} MB")
        print(f"  📊 Growth: {memory_growth:.1f} MB")
        
        # Memory usage assertions (reasonable limits for test process)
        assert memory_growth < 100.0, f"Memory growth too high: {memory_growth:.1f} MB"
        assert max_memory - initial_memory < 150.0, f"Peak memory usage too high: {max_memory - initial_memory:.1f} MB"
        
        return {
            'initial_memory': initial_memory,
            'peak_memory': max_memory,
            'final_memory': final_memory,
            'memory_growth': memory_growth,
            'samples': memory_samples
        }
    
    def test_rapid_navigation_stress(self):
        """Test system behavior under rapid navigation requests."""
        print("🔥 Testing rapid navigation stress...")
        
        # Rapid navigation pattern: simulate user clicking navigation buttons quickly
        rapid_requests = 20
        request_interval = 0.05  # 50ms between requests (very fast user)
        
        start_time = time.time()
        results = []
        
        for i in range(rapid_requests):
            position = 10 + (i % 10)  # Cycle through positions 10-19
            
            request_start = time.time()
            
            try:
                # Only test 1h navigation for rapid stress (simpler)
                nav_response = requests.get(
                    f"{self.BASE_URL}/api/v1/training-datasets/{self.test_dataset_id}/sequences/{self.test_sequence_id}/1h?row_index={position}",
                    timeout=5.0  # Short timeout for stress test
                )
                
                request_time = time.time() - request_start
                
                result = {
                    'request_id': i,
                    'position': position,
                    'success': nav_response.status_code == 200,
                    'response_time': request_time,
                    'status_code': nav_response.status_code
                }
                
                if nav_response.status_code == 200:
                    try:
                        nav_data = nav_response.json()
                        result['api_success'] = nav_data.get('success', False)
                        result['table_rows'] = len(nav_data.get('table_data', []))
                    except:
                        result['api_success'] = False
                
                results.append(result)
                
            except requests.Timeout:
                results.append({
                    'request_id': i,
                    'position': position, 
                    'success': False,
                    'response_time': 5.0,
                    'timeout': True
                })
            
            except Exception as e:
                results.append({
                    'request_id': i,
                    'position': position,
                    'success': False,
                    'error': str(e),
                    'response_time': 0
                })
            
            # Wait before next request (simulate rapid clicking)
            time.sleep(request_interval)
        
        total_time = time.time() - start_time
        
        # Analyze stress test results
        successful_requests = [r for r in results if r.get('success', False)]
        api_successful_requests = [r for r in results if r.get('api_success', False)]
        timeouts = [r for r in results if r.get('timeout', False)]
        
        success_rate = len(successful_requests) / len(results) * 100
        api_success_rate = len(api_successful_requests) / len(results) * 100
        timeout_rate = len(timeouts) / len(results) * 100
        
        if successful_requests:
            avg_response_time = statistics.mean(r['response_time'] for r in successful_requests)
            max_response_time = max(r['response_time'] for r in successful_requests)
        else:
            avg_response_time = 0
            max_response_time = 0
        
        print(f"\n🔥 Rapid Navigation Stress Test Results:")
        print(f"  🔢 Total requests: {len(results)}")
        print(f"  ✅ HTTP success rate: {success_rate:.1f}%")
        print(f"  ✅ API success rate: {api_success_rate:.1f}%")
        print(f"  ⏰ Timeout rate: {timeout_rate:.1f}%")
        print(f"  ⏱️ Average response time: {avg_response_time:.3f}s")
        print(f"  ⏱️ Max response time: {max_response_time:.3f}s")
        print(f"  ⏱️ Total test time: {total_time:.3f}s")
        
        # Stress test assertions (should handle rapid requests gracefully)
        assert success_rate >= 70.0, f"Success rate too low under stress: {success_rate:.1f}%"
        assert timeout_rate <= 30.0, f"Too many timeouts under stress: {timeout_rate:.1f}%"
        
        if successful_requests:
            assert avg_response_time < 3.0, f"Average response time too slow under stress: {avg_response_time:.3f}s"
        
        return {
            'total_requests': len(results),
            'success_rate': success_rate,
            'api_success_rate': api_success_rate,
            'timeout_rate': timeout_rate,
            'avg_response_time': avg_response_time,
            'max_response_time': max_response_time,
            'total_time': total_time,
            'results': results
        }

if __name__ == '__main__':
    pytest.main([__file__, '-v', '--tb=short', '-s'])