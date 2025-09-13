#!/usr/bin/env python3
"""
InstrumentService Performance Benchmarking Script

Comprehensive performance analysis of the InstrumentService including:
1. Single operation latency benchmarks
2. Concurrent operation throughput tests
3. Load testing scenarios
4. Resource utilization monitoring
5. Performance regression detection
"""

import asyncio
import time
import statistics
import argparse
import json
import sys
from datetime import datetime
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from domains.instruments.services.interfaces.instrument_service_interface import (
    InstrumentDTO,
    InstrumentSearchCriteria
)
from domains.instruments.services.config.service_container import get_instrument_service
from core.platform.config.environment import Environment, EnvironmentType
from infrastructure.monitoring.instrument_service_monitor import get_instrument_service_monitor


class InstrumentServiceBenchmark:
    """Comprehensive InstrumentService benchmarking suite"""
    
    def __init__(self, environment_type: str = "dev"):
        self.env_type = EnvironmentType.DEV if environment_type == "dev" else EnvironmentType.INTG
        self.results = {}
        self.service = None
        self.monitor = None
    
    async def initialize(self):
        """Initialize service and monitoring"""
        print("Initializing InstrumentService benchmark...")
        env = Environment(None, self.env_type)
        self.service = await get_instrument_service(env)
        self.monitor = get_instrument_service_monitor()
        print("✅ Service initialized")
    
    async def run_single_operation_benchmarks(self) -> dict:
        """Benchmark individual operation performance"""
        print("\n📊 Running single operation benchmarks...")
        
        operations = [
            ('validate_symbol', lambda: self.service.validate_symbol("AAPL")),
            ('get_instrument_count', lambda: self.service.get_instrument_count()),
            ('list_instruments_small', lambda: self.service.list_instruments(InstrumentSearchCriteria(limit=10))),
            ('list_instruments_medium', lambda: self.service.list_instruments(InstrumentSearchCriteria(limit=50))),
            ('list_instruments_large', lambda: self.service.list_instruments(InstrumentSearchCriteria(limit=100)))
        ]
        
        results = {}
        
        for operation_name, operation_func in operations:
            print(f"  Benchmarking {operation_name}...")
            
            # Warmup
            for _ in range(5):
                try:
                    await operation_func()
                except:
                    pass
            
            # Benchmark runs
            iterations = 100
            latencies = []
            success_count = 0
            
            start_time = time.time()
            
            for _ in range(iterations):
                op_start = time.perf_counter()
                try:
                    result = await operation_func()
                    op_end = time.perf_counter()
                    latencies.append((op_end - op_start) * 1000)  # ms
                    success_count += 1
                except Exception as e:
                    print(f"    Operation failed: {e}")
                    pass
            
            total_time = time.time() - start_time
            
            if latencies:
                results[operation_name] = {
                    'iterations': iterations,
                    'success_count': success_count,
                    'success_rate': success_count / iterations,
                    'total_time_s': total_time,
                    'avg_latency_ms': statistics.mean(latencies),
                    'median_latency_ms': statistics.median(latencies),
                    'p95_latency_ms': statistics.quantiles(latencies, n=20)[18] if len(latencies) >= 20 else max(latencies),
                    'p99_latency_ms': statistics.quantiles(latencies, n=100)[98] if len(latencies) >= 100 else max(latencies),
                    'min_latency_ms': min(latencies),
                    'max_latency_ms': max(latencies),
                    'std_dev_ms': statistics.stdev(latencies) if len(latencies) > 1 else 0,
                    'operations_per_second': success_count / total_time if total_time > 0 else 0
                }
                
                print(f"    ✅ {operation_name}: {results[operation_name]['operations_per_second']:.1f} ops/sec, "
                      f"avg: {results[operation_name]['avg_latency_ms']:.1f}ms, "
                      f"p95: {results[operation_name]['p95_latency_ms']:.1f}ms")
            else:
                results[operation_name] = {'error': 'All operations failed'}
                print(f"    ❌ {operation_name}: All operations failed")
        
        return results
    
    async def run_concurrency_benchmarks(self) -> dict:
        """Benchmark concurrent operation performance"""
        print("\n🔄 Running concurrency benchmarks...")
        
        concurrency_levels = [1, 5, 10, 20, 50]
        results = {}
        
        async def test_operation():
            try:
                await self.service.validate_symbol("AAPL")
                return True
            except:
                return False
        
        for concurrency in concurrency_levels:
            print(f"  Testing concurrency level: {concurrency}")
            
            operations_per_worker = 20
            total_operations = concurrency * operations_per_worker
            
            start_time = time.time()
            
            # Create concurrent tasks
            tasks = []
            for worker in range(concurrency):
                worker_tasks = [test_operation() for _ in range(operations_per_worker)]
                tasks.extend(worker_tasks)
            
            # Execute all tasks concurrently
            task_results = await asyncio.gather(*tasks, return_exceptions=True)
            
            total_time = time.time() - start_time
            success_count = sum(1 for r in task_results if r is True)
            
            results[concurrency] = {
                'concurrency_level': concurrency,
                'total_operations': total_operations,
                'success_count': success_count,
                'success_rate': success_count / total_operations if total_operations > 0 else 0,
                'total_time_s': total_time,
                'operations_per_second': success_count / total_time if total_time > 0 else 0,
                'avg_latency_ms': (total_time * 1000) / success_count if success_count > 0 else 0
            }
            
            print(f"    ✅ Concurrency {concurrency}: {results[concurrency]['operations_per_second']:.1f} ops/sec, "
                  f"success rate: {results[concurrency]['success_rate']*100:.1f}%")
        
        return results
    
    async def run_sustained_load_test(self, duration_seconds: int = 60) -> dict:
        """Run sustained load test"""
        print(f"\n⏱️  Running sustained load test ({duration_seconds}s)...")
        
        target_ops_per_second = 10
        interval_seconds = 1.0 / target_ops_per_second
        
        successful_operations = 0
        failed_operations = 0
        response_times = []
        
        start_time = time.time()
        end_time = start_time + duration_seconds
        
        async def rate_limited_operations():
            nonlocal successful_operations, failed_operations, response_times
            
            while time.time() < end_time:
                op_start = time.perf_counter()
                try:
                    await self.service.validate_symbol("AAPL")
                    op_end = time.perf_counter()
                    response_times.append((op_end - op_start) * 1000)
                    successful_operations += 1
                except:
                    failed_operations += 1
                
                # Rate limiting
                await asyncio.sleep(interval_seconds)
        
        # Run load test
        await rate_limited_operations()
        
        actual_duration = time.time() - start_time
        total_operations = successful_operations + failed_operations
        
        results = {
            'duration_s': actual_duration,
            'target_ops_per_second': target_ops_per_second,
            'total_operations': total_operations,
            'successful_operations': successful_operations,
            'failed_operations': failed_operations,
            'success_rate': successful_operations / total_operations if total_operations > 0 else 0,
            'actual_ops_per_second': successful_operations / actual_duration if actual_duration > 0 else 0
        }
        
        if response_times:
            results.update({
                'avg_response_time_ms': statistics.mean(response_times),
                'p95_response_time_ms': statistics.quantiles(response_times, n=20)[18] if len(response_times) >= 20 else max(response_times),
                'max_response_time_ms': max(response_times),
                'min_response_time_ms': min(response_times)
            })
        
        print(f"    ✅ Sustained load: {results['actual_ops_per_second']:.1f} ops/sec, "
              f"success rate: {results['success_rate']*100:.1f}%")
        
        return results
    
    async def run_monitoring_performance_test(self) -> dict:
        """Test monitoring system performance impact"""
        print("\n📈 Testing monitoring performance impact...")
        
        # Test without explicit monitoring calls
        iterations = 50
        start_time = time.time()
        
        for _ in range(iterations):
            await self.service.validate_symbol("AAPL")
        
        base_time = time.time() - start_time
        
        # Test with monitoring dashboard generation
        start_time = time.time()
        
        for _ in range(iterations):
            await self.service.validate_symbol("GOOGL")
        
        # Generate monitoring dashboard
        dashboard_start = time.time()
        dashboard_data = await self.monitor.get_monitoring_dashboard()
        dashboard_time = time.time() - dashboard_start
        
        monitored_time = time.time() - start_time
        
        results = {
            'iterations': iterations,
            'base_time_s': base_time,
            'monitored_time_s': monitored_time,
            'dashboard_generation_time_s': dashboard_time,
            'base_ops_per_second': iterations / base_time if base_time > 0 else 0,
            'monitored_ops_per_second': iterations / monitored_time if monitored_time > 0 else 0,
            'monitoring_overhead_percent': ((monitored_time - base_time) / base_time * 100) if base_time > 0 else 0,
            'dashboard_available': dashboard_data is not None
        }
        
        print(f"    ✅ Base performance: {results['base_ops_per_second']:.1f} ops/sec")
        print(f"    ✅ Monitored performance: {results['monitored_ops_per_second']:.1f} ops/sec")
        print(f"    📊 Monitoring overhead: {results['monitoring_overhead_percent']:.1f}%")
        print(f"    📊 Dashboard generation: {dashboard_time*1000:.1f}ms")
        
        return results
    
    async def run_comprehensive_benchmark(self, 
                                        include_load_test: bool = True,
                                        load_test_duration: int = 30) -> dict:
        """Run all benchmarks and return comprehensive results"""
        print(f"🚀 Starting comprehensive InstrumentService benchmark")
        print(f"Environment: {self.env_type.value}")
        print(f"Timestamp: {datetime.utcnow().isoformat()}")
        
        benchmark_start = time.time()
        
        # Initialize results structure
        all_results = {
            'metadata': {
                'timestamp': datetime.utcnow().isoformat(),
                'environment': self.env_type.value,
                'benchmark_version': '1.0.0'
            },
            'benchmarks': {}
        }
        
        try:
            # Single operation benchmarks
            all_results['benchmarks']['single_operations'] = await self.run_single_operation_benchmarks()
            
            # Concurrency benchmarks
            all_results['benchmarks']['concurrency'] = await self.run_concurrency_benchmarks()
            
            # Monitoring performance test
            all_results['benchmarks']['monitoring_impact'] = await self.run_monitoring_performance_test()
            
            # Optional sustained load test
            if include_load_test:
                all_results['benchmarks']['sustained_load'] = await self.run_sustained_load_test(load_test_duration)
            
            benchmark_duration = time.time() - benchmark_start
            all_results['metadata']['total_benchmark_time_s'] = benchmark_duration
            
            print(f"\n✅ Benchmark completed in {benchmark_duration:.1f} seconds")
            
            return all_results
            
        except Exception as e:
            print(f"\n❌ Benchmark failed: {e}")
            all_results['error'] = str(e)
            return all_results
    
    def generate_report(self, results: dict) -> str:
        """Generate human-readable benchmark report"""
        report = []
        report.append("=" * 80)
        report.append("INSTRUMENTSERVICE PERFORMANCE BENCHMARK REPORT")
        report.append("=" * 80)
        
        metadata = results.get('metadata', {})
        report.append(f"Timestamp: {metadata.get('timestamp', 'unknown')}")
        report.append(f"Environment: {metadata.get('environment', 'unknown')}")
        report.append(f"Total benchmark time: {metadata.get('total_benchmark_time_s', 0):.1f}s")
        report.append("")
        
        benchmarks = results.get('benchmarks', {})
        
        # Single operations report
        if 'single_operations' in benchmarks:
            report.append("SINGLE OPERATION PERFORMANCE")
            report.append("-" * 40)
            
            for op_name, stats in benchmarks['single_operations'].items():
                if 'error' in stats:
                    report.append(f"{op_name:25}: ERROR - {stats['error']}")
                else:
                    report.append(f"{op_name:25}: {stats['operations_per_second']:8.1f} ops/sec | "
                                f"avg: {stats['avg_latency_ms']:6.1f}ms | "
                                f"p95: {stats['p95_latency_ms']:6.1f}ms | "
                                f"success: {stats['success_rate']*100:5.1f}%")
            report.append("")
        
        # Concurrency report
        if 'concurrency' in benchmarks:
            report.append("CONCURRENCY PERFORMANCE")
            report.append("-" * 40)
            
            for concurrency, stats in benchmarks['concurrency'].items():
                report.append(f"Concurrency {concurrency:2d}: {stats['operations_per_second']:8.1f} ops/sec | "
                            f"success: {stats['success_rate']*100:5.1f}% | "
                            f"avg latency: {stats['avg_latency_ms']:6.1f}ms")
            report.append("")
        
        # Monitoring impact report
        if 'monitoring_impact' in benchmarks:
            stats = benchmarks['monitoring_impact']
            report.append("MONITORING SYSTEM IMPACT")
            report.append("-" * 40)
            report.append(f"Base performance:      {stats['base_ops_per_second']:8.1f} ops/sec")
            report.append(f"Monitored performance: {stats['monitored_ops_per_second']:8.1f} ops/sec")
            report.append(f"Monitoring overhead:   {stats['monitoring_overhead_percent']:8.1f}%")
            report.append(f"Dashboard generation:  {stats['dashboard_generation_time_s']*1000:8.1f}ms")
            report.append("")
        
        # Sustained load report
        if 'sustained_load' in benchmarks:
            stats = benchmarks['sustained_load']
            report.append("SUSTAINED LOAD TEST")
            report.append("-" * 40)
            report.append(f"Duration:              {stats['duration_s']:8.1f}s")
            report.append(f"Target ops/sec:        {stats['target_ops_per_second']:8.1f}")
            report.append(f"Actual ops/sec:        {stats['actual_ops_per_second']:8.1f}")
            report.append(f"Success rate:          {stats['success_rate']*100:8.1f}%")
            if 'avg_response_time_ms' in stats:
                report.append(f"Avg response time:     {stats['avg_response_time_ms']:8.1f}ms")
                report.append(f"P95 response time:     {stats['p95_response_time_ms']:8.1f}ms")
            report.append("")
        
        report.append("=" * 80)
        return "\n".join(report)


async def main():
    """Main benchmark execution function"""
    parser = argparse.ArgumentParser(description="InstrumentService Performance Benchmark")
    parser.add_argument("--environment", choices=["dev", "intg"], default="dev",
                       help="Environment to benchmark (default: dev)")
    parser.add_argument("--no-load-test", action="store_true",
                       help="Skip sustained load test")
    parser.add_argument("--load-duration", type=int, default=30,
                       help="Sustained load test duration in seconds (default: 30)")
    parser.add_argument("--output", "-o", help="Output file for results (JSON format)")
    parser.add_argument("--report", "-r", help="Output file for human-readable report")
    
    args = parser.parse_args()
    
    # Create and run benchmark
    benchmark = InstrumentServiceBenchmark(args.environment)
    
    try:
        await benchmark.initialize()
        
        results = await benchmark.run_comprehensive_benchmark(
            include_load_test=not args.no_load_test,
            load_test_duration=args.load_duration
        )
        
        # Save JSON results
        if args.output:
            with open(args.output, 'w') as f:
                json.dump(results, f, indent=2)
            print(f"📁 Results saved to {args.output}")
        
        # Generate and save report
        report = benchmark.generate_report(results)
        
        if args.report:
            with open(args.report, 'w') as f:
                f.write(report)
            print(f"📄 Report saved to {args.report}")
        else:
            print("\n" + report)
        
    except KeyboardInterrupt:
        print("\n⚠️  Benchmark interrupted by user")
    except Exception as e:
        print(f"\n❌ Benchmark failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())