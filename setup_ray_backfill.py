#!/usr/bin/env python3
"""
Setup Ray for FirstRate Parallel Backfill
Install Ray and test parallel processing capability
"""

import subprocess
import sys
import os
import multiprocessing
from pathlib import Path

def install_ray():
    """Install Ray if not already available"""
    
    print("🚀 Setting up Ray for parallel FirstRate backfill...")
    
    try:
        import ray
        print("✅ Ray is already installed")
        print(f"   Ray version: {ray.__version__}")
        return True
    except ImportError:
        print("📦 Installing Ray...")
        
        try:
            subprocess.run([
                sys.executable, '-m', 'pip', 'install', 
                'ray[default]', 'psutil'
            ], check=True)
            
            print("✅ Ray installation completed")
            return True
            
        except subprocess.CalledProcessError as e:
            print(f"❌ Failed to install Ray: {e}")
            return False

def test_ray_setup():
    """Test Ray setup with simple parallel task"""
    
    print("\n🧪 Testing Ray parallel processing...")
    
    try:
        import ray
        
        # Initialize Ray
        ray.init(ignore_reinit_error=True)
        
        @ray.remote
        def test_task(x):
            """Simple test task"""
            import time
            time.sleep(0.1)  # Simulate work
            return x * x
        
        # Test parallel execution
        print("   Creating test tasks...")
        futures = [test_task.remote(i) for i in range(10)]
        
        print("   Executing tasks in parallel...")
        results = ray.get(futures)
        
        print(f"✅ Ray test successful! Results: {results}")
        
        # Show cluster resources
        resources = ray.cluster_resources()
        print(f"📊 Ray cluster resources: {resources}")
        
        ray.shutdown()
        return True
        
    except Exception as e:
        print(f"❌ Ray test failed: {e}")
        return False

def estimate_performance_improvement():
    """Estimate potential performance improvements with Ray"""
    
    print("\n📊 Performance Improvement Analysis:")
    
    # Current system specs
    cpu_count = multiprocessing.cpu_count()
    print(f"   CPU cores available: {cpu_count}")
    
    # Current sequential processing estimates
    total_symbols = 6827
    est_time_per_symbol = 2 * 60  # 2 minutes per symbol
    sequential_time_hours = (total_symbols * est_time_per_symbol) / 3600
    
    print(f"   Total symbols to process: {total_symbols}")
    print(f"   Est. sequential time: {sequential_time_hours:.1f} hours ({sequential_time_hours/24:.1f} days)")
    
    # Parallel processing estimates
    # Assume 70% efficiency due to I/O constraints and coordination overhead
    parallel_efficiency = 0.7
    effective_cores = int(cpu_count * parallel_efficiency)
    
    parallel_time_hours = sequential_time_hours / effective_cores
    speedup = sequential_time_hours / parallel_time_hours
    
    print(f"\n🚀 Ray Parallel Processing Estimates:")
    print(f"   Effective parallel workers: {effective_cores}")
    print(f"   Est. parallel time: {parallel_time_hours:.1f} hours ({parallel_time_hours/24:.1f} days)")
    print(f"   Expected speedup: {speedup:.1f}x")
    print(f"   Time savings: {sequential_time_hours - parallel_time_hours:.1f} hours")
    
    # Multi-machine potential
    if cpu_count >= 8:
        print(f"\n🌐 Multi-Machine Cluster Potential:")
        for machines in [2, 4, 8]:
            cluster_cores = cpu_count * machines * parallel_efficiency
            cluster_time = sequential_time_hours / cluster_cores
            cluster_speedup = sequential_time_hours / cluster_time
            
            print(f"   {machines} machines ({int(cluster_cores)} cores): {cluster_time:.1f}h ({cluster_speedup:.1f}x speedup)")

def create_ray_config():
    """Create Ray configuration for FirstRate backfill"""
    
    print("\n⚙️  Creating Ray configuration...")
    
    config_content = f"""# Ray Configuration for FirstRate Parallel Backfill

## System Specifications
- CPU Cores: {multiprocessing.cpu_count()}
- Recommended Workers: {max(1, int(multiprocessing.cpu_count() * 0.8))}

## Usage Commands

# Test Ray setup
python setup_ray_backfill.py

# Run small test batch (50 symbols)
python ray_firstrate_parallel_backfill.py --test-mode

# Run limited batch (500 symbols) 
python ray_firstrate_parallel_backfill.py --limit 500

# Run full parallel backfill (6,827 symbols)
python ray_firstrate_parallel_backfill.py

# Run with specific number of workers
python ray_firstrate_parallel_backfill.py --num-workers 12

## Expected Performance
- Sequential processing: ~227 hours
- Ray parallel ({max(1, int(multiprocessing.cpu_count() * 0.8))} workers): ~{227 / max(1, int(multiprocessing.cpu_count() * 0.8)):.1f} hours
- Potential speedup: {max(1, int(multiprocessing.cpu_count() * 0.8)):.1f}x

## Ray Cluster Setup (Optional)
# For multi-machine processing:
# ray start --head --port=10001
# ray start --address='head_node_ip:10001' 
# python ray_firstrate_parallel_backfill.py --ray-address ray://head_node_ip:10001
"""
    
    config_file = Path("RAY_BACKFILL_CONFIG.md")
    config_file.write_text(config_content)
    
    print(f"📄 Configuration saved to: {config_file}")

def main():
    """Main setup function"""
    
    print("=" * 60)
    print("Ray FirstRate Parallel Backfill Setup")
    print("=" * 60)
    
    # Check system requirements
    print(f"🖥️  System: {os.uname().sysname} - {multiprocessing.cpu_count()} CPU cores")
    
    # Install Ray
    if not install_ray():
        print("❌ Setup failed - could not install Ray")
        return False
    
    # Test Ray functionality
    if not test_ray_setup():
        print("❌ Setup failed - Ray test unsuccessful")
        return False
    
    # Performance analysis
    estimate_performance_improvement()
    
    # Create configuration
    create_ray_config()
    
    print(f"\n{'='*60}")
    print("✅ RAY SETUP COMPLETED SUCCESSFULLY!")
    print(f"{'='*60}")
    print()
    print("🚀 Ready for parallel FirstRate backfill!")
    print("📋 Next steps:")
    print("   1. Test: python ray_firstrate_parallel_backfill.py --test-mode")
    print("   2. Limited run: python ray_firstrate_parallel_backfill.py --limit 100") 
    print("   3. Full run: python ray_firstrate_parallel_backfill.py")
    print()
    print("📊 Expected performance:")
    cores = max(1, int(multiprocessing.cpu_count() * 0.8))
    print(f"   Sequential: ~227 hours")
    print(f"   Ray parallel: ~{227/cores:.1f} hours ({cores}x speedup)")
    
    return True

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)