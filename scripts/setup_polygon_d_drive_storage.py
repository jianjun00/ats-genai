#!/usr/bin/env python3
"""
Polygon D: Drive Storage Setup for 30-Year Population

Sets up the proper directory structure and verifies D: drive access
specifically for Polygon 30-year minute bar data collection.

This script handles:
- WSL/Windows path mapping for D: drive
- Polygon-specific directory structure
- API key validation and rate limit detection
- Storage estimation for Polygon data volumes
- Environment variable configuration
- Polygon API premium plan detection
"""

import os
import sys
import requests
import time
from pathlib import Path
import subprocess
import json
from datetime import datetime, timedelta
import shutil

def check_polygon_api():
    """Check Polygon API access and determine plan type"""
    api_key = os.getenv('POLYGON_API_KEY')
    if not api_key:
        return {
            'valid': False,
            'error': 'POLYGON_API_KEY environment variable not set',
            'plan_type': 'unknown',
            'rate_limit': 0
        }
    
    try:
        # Test API with a simple request
        url = f"https://api.polygon.io/v3/reference/tickers?active=true&limit=1&apikey={api_key}"
        
        start_time = time.time()
        response = requests.get(url)
        request_time = time.time() - start_time
        
        if response.status_code == 200:
            data = response.json()
            
            # Try to determine plan type based on response headers or make additional requests
            plan_info = {
                'valid': True,
                'status_code': response.status_code,
                'request_time': request_time,
                'results_count': len(data.get('results', [])),
                'plan_type': 'unknown',
                'rate_limit': 5  # Default to free tier
            }
            
            # Check for rate limit headers
            if 'X-RateLimit-Limit' in response.headers:
                plan_info['rate_limit'] = int(response.headers['X-RateLimit-Limit'])
                plan_info['plan_type'] = 'premium' if plan_info['rate_limit'] > 10 else 'free'
            else:
                # Attempt to detect plan by testing rate limits
                plan_info['plan_type'] = 'free'  # Conservative assumption
                plan_info['rate_limit'] = 5
            
            return plan_info
            
        else:
            return {
                'valid': False,
                'error': f'API returned status {response.status_code}: {response.text}',
                'plan_type': 'unknown',
                'rate_limit': 0
            }
            
    except Exception as e:
        return {
            'valid': False,
            'error': f'API test failed: {e}',
            'plan_type': 'unknown',
            'rate_limit': 0
        }

def detect_d_drive_path():
    """Detect the correct D: drive path for different environments"""
    possible_paths = [
        "/mnt/d",           # WSL2 default
        "/mnt/d/",          # WSL2 with trailing slash
        "D:\\",             # Windows native
        "D:/",              # Windows with forward slashes
        "/d",               # Some WSL configurations
    ]
    
    for path in possible_paths:
        if os.path.exists(path) and os.access(path, os.W_OK):
            return path
    
    return None

def get_disk_usage(path):
    """Get disk usage statistics for given path"""
    try:
        usage = shutil.disk_usage(path)
        return {
            'total': usage.total,
            'used': usage.used,
            'free': usage.free,
            'total_gb': round(usage.total / (1024**3), 2),
            'used_gb': round(usage.used / (1024**3), 2),
            'free_gb': round(usage.free / (1024**3), 2)
        }
    except Exception as e:
        return {'error': str(e)}

def estimate_polygon_storage_requirements():
    """Estimate storage requirements for 30 years of Polygon minute data"""
    
    # Polygon-specific estimates
    assumptions = {
        'symbols': 8000,  # Polygon has extensive coverage
        'trading_days_per_year': 252,
        'minutes_per_day': 390,  # 6.5 hours * 60 minutes
        'years': 30,
        'bytes_per_bar': 60,  # Polygon provides more fields (VWAP, trade count)
        'compression_ratio': 0.6,  # Snappy compression
        'metadata_overhead': 1.2,  # 20% overhead for metadata and indices
        'quality_metadata': 1.1,  # Additional 10% for quality scores and validation
    }
    
    total_bars = (assumptions['symbols'] * 
                  assumptions['trading_days_per_year'] * 
                  assumptions['minutes_per_day'] * 
                  assumptions['years'])
    
    raw_bytes = total_bars * assumptions['bytes_per_bar']
    compressed_bytes = raw_bytes * assumptions['compression_ratio']
    total_bytes = compressed_bytes * assumptions['metadata_overhead'] * assumptions['quality_metadata']
    
    return {
        'assumptions': assumptions,
        'total_bars': total_bars,
        'estimated_size_bytes': total_bytes,
        'estimated_size_gb': round(total_bytes / (1024**3), 2),
        'estimated_size_tb': round(total_bytes / (1024**4), 2)
    }

def create_polygon_directory_structure(base_path):
    """Create the required directory structure for Polygon data"""
    
    directories = [
        "minute-bars",                    # Main minute bar storage
        "minute-bars/polygon",            # Polygon-specific data
        "minute-bars/backups",            # Backup files
        "minute-bars/metadata",           # File metadata
        "minute-bars/temp",               # Temporary files during processing
        "minute-bars/quality-reports",    # Data quality validation reports
        "logs/polygon",                   # Polygon processing logs
        "checkpoints/polygon",            # Polygon checkpoint files
        "reports/polygon",                # Population reports and statistics
        "config/polygon"                  # Polygon-specific configuration
    ]
    
    created_dirs = []
    failed_dirs = []
    
    for dir_name in directories:
        dir_path = Path(base_path) / "ats-data" / dir_name
        try:
            dir_path.mkdir(parents=True, exist_ok=True)
            created_dirs.append(str(dir_path))
            print(f"✅ Created: {dir_path}")
        except Exception as e:
            failed_dirs.append({'path': str(dir_path), 'error': str(e)})
            print(f"❌ Failed: {dir_path} - {e}")
    
    return created_dirs, failed_dirs

def generate_polygon_env_config(d_drive_path, api_info):
    """Generate environment configuration for Polygon population"""
    
    config = {
        'POLYGON_MINUTE_DATA_PATH': str(Path(d_drive_path) / "ats-data" / "minute-bars" / "polygon"),
        'POLYGON_CHECKPOINT_PATH': str(Path(d_drive_path) / "ats-data" / "checkpoints" / "polygon"),
        'POLYGON_LOG_PATH': str(Path(d_drive_path) / "ats-data" / "logs" / "polygon"),
        'POLYGON_BACKUP_PATH': str(Path(d_drive_path) / "ats-data" / "minute-bars" / "backups"),
        'POLYGON_QUALITY_REPORTS_PATH': str(Path(d_drive_path) / "ats-data" / "minute-bars" / "quality-reports"),
        'D_DRIVE_ROOT': d_drive_path,
        'POLYGON_PLAN_TYPE': api_info.get('plan_type', 'free'),
        'POLYGON_RATE_LIMIT': api_info.get('rate_limit', 5)
    }
    
    return config

def calculate_processing_time(api_info, estimates):
    """Calculate estimated processing time for Polygon population"""
    
    rate_limit = api_info.get('rate_limit', 5)
    symbols = estimates['assumptions']['symbols']
    years = estimates['assumptions']['years']
    days_per_year = estimates['assumptions']['trading_days_per_year']
    
    # Estimate API calls (chunked by month for efficiency)
    chunks_per_symbol = years * 12  # Monthly chunks
    total_api_calls = symbols * chunks_per_symbol
    
    # Calculate time based on rate limits
    requests_per_minute = rate_limit
    total_minutes = total_api_calls / requests_per_minute
    total_hours = total_minutes / 60
    total_days = total_hours / 24
    
    return {
        'total_api_calls': total_api_calls,
        'requests_per_minute': requests_per_minute,
        'estimated_minutes': total_minutes,
        'estimated_hours': total_hours,
        'estimated_days': total_days,
        'estimated_months': total_days / 30,
        'plan_type': api_info.get('plan_type', 'unknown')
    }

def main():
    """Main setup function"""
    
    print("🚀 Setting up D: drive storage for Polygon 30-year population")
    print("=" * 65)
    
    # Step 1: Check Polygon API
    print("1. Checking Polygon API access...")
    api_info = check_polygon_api()
    
    if not api_info['valid']:
        print(f"❌ Polygon API check failed: {api_info['error']}")
        print("Please ensure:")
        print("  - POLYGON_API_KEY environment variable is set")
        print("  - Your Polygon API key is valid")
        print("  - You have internet connectivity")
        return 1
    
    print(f"✅ Polygon API access verified")
    print(f"   Plan type: {api_info['plan_type']}")
    print(f"   Rate limit: {api_info['rate_limit']} requests/minute")
    print(f"   Response time: {api_info['request_time']:.2f}s")
    
    # Step 2: Detect D: drive
    print("\n2. Detecting D: drive path...")
    d_drive = detect_d_drive_path()
    
    if not d_drive:
        print("❌ D: drive not found or not accessible")
        print("Please ensure:")
        print("  - D: drive exists and is mounted")
        print("  - You have write permissions")
        print("  - WSL can access Windows drives")
        return 1
    
    print(f"✅ D: drive found at: {d_drive}")
    
    # Step 3: Check disk space
    print("\n3. Checking disk space...")
    usage = get_disk_usage(d_drive)
    
    if 'error' in usage:
        print(f"❌ Cannot check disk space: {usage['error']}")
        return 1
    
    print(f"💾 Disk usage:")
    print(f"   Total: {usage['total_gb']:,.1f} GB")
    print(f"   Used:  {usage['used_gb']:,.1f} GB")
    print(f"   Free:  {usage['free_gb']:,.1f} GB")
    
    # Step 4: Estimate storage requirements
    print("\n4. Estimating Polygon storage requirements...")
    estimates = estimate_polygon_storage_requirements()
    
    print(f"📊 Storage estimates for 30-year Polygon population:")
    print(f"   Symbols: {estimates['assumptions']['symbols']:,}")
    print(f"   Total bars: {estimates['total_bars']:,}")
    print(f"   Estimated size: {estimates['estimated_size_gb']:,.1f} GB ({estimates['estimated_size_tb']:.2f} TB)")
    
    if estimates['estimated_size_gb'] > usage['free_gb']:
        print("⚠️  WARNING: Estimated storage exceeds available space!")
        print("   Consider:")
        print("   - Processing in smaller batches")
        print("   - Using higher compression")
        print("   - Clearing space on D: drive")
        print("   - Processing only priority symbols first")
    else:
        print("✅ Sufficient space available")
    
    # Step 5: Calculate processing time
    print("\n5. Estimating processing time...")
    time_estimates = calculate_processing_time(api_info, estimates)
    
    print(f"⏱️  Processing time estimates:")
    print(f"   Total API calls: {time_estimates['total_api_calls']:,}")
    print(f"   Rate limit: {time_estimates['requests_per_minute']} req/min ({api_info['plan_type']} plan)")
    
    if time_estimates['estimated_days'] < 30:
        print(f"   Estimated time: {time_estimates['estimated_days']:.1f} days")
    elif time_estimates['estimated_days'] < 365:
        print(f"   Estimated time: {time_estimates['estimated_months']:.1f} months")
    else:
        print(f"   Estimated time: {time_estimates['estimated_days']/365:.1f} years")
    
    if api_info['plan_type'] == 'free' and time_estimates['estimated_months'] > 12:
        print("⚠️  WARNING: Very long processing time with free plan!")
        print("   Consider:")
        print("   - Upgrading to Polygon premium plan")
        print("   - Processing in smaller batches")
        print("   - Focusing on priority symbols only")
    
    # Step 6: Create directory structure
    print("\n6. Creating Polygon directory structure...")
    created, failed = create_polygon_directory_structure(d_drive)
    
    if failed:
        print(f"❌ {len(failed)} directories failed to create:")
        for failure in failed:
            print(f"   {failure['path']}: {failure['error']}")
        return 1
    
    print(f"✅ Created {len(created)} directories successfully")
    
    # Step 7: Generate configuration
    print("\n7. Generating Polygon configuration...")
    env_config = generate_polygon_env_config(d_drive, api_info)
    
    # Save config to file
    config_file = Path("polygon_d_drive_config.json")
    with open(config_file, 'w') as f:
        json.dump({
            'environment_config': env_config,
            'api_info': api_info,
            'storage_estimates': estimates,
            'time_estimates': time_estimates,
            'setup_timestamp': datetime.now().isoformat()
        }, f, indent=2)
    
    print(f"✅ Configuration saved to: {config_file}")
    
    # Step 8: Generate setup summary
    print("\n" + "=" * 65)
    print("🎉 POLYGON D: DRIVE SETUP COMPLETE!")
    print("=" * 65)
    
    print("\n📁 Polygon storage paths configured:")
    for key, value in env_config.items():
        if 'PATH' in key:
            print(f"   {key}: {value}")
    
    print(f"\n🚀 Ready to run Polygon population:")
    print(f"   # Debug mode (5 symbols, 1 year)")
    print(f"   python scripts/populate_30year_polygon_minute_bars.py \\")
    print(f"     --debug --limit 5 \\")
    print(f"     --start-date {(datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')} \\")
    print(f"     --storage-path {env_config['POLYGON_MINUTE_DATA_PATH']}")
    
    print(f"\n   # Full 30-year population")
    print(f"   python scripts/populate_30year_polygon_minute_bars.py \\")
    print(f"     --mode full \\")
    print(f"     {'--premium' if api_info['plan_type'] == 'premium' else ''} \\")
    print(f"     --storage-path {env_config['POLYGON_MINUTE_DATA_PATH']}")
    
    print(f"\n📋 Environment variables (add to your .bashrc or .zshrc):")
    for key, value in env_config.items():
        print(f"   export {key}='{value}'")
    
    print(f"\n⚡ Performance recommendations:")
    if api_info['plan_type'] == 'free':
        print(f"   - Use --concurrent 1 (single threaded for free tier)")
        print(f"   - Consider upgrading to premium for faster processing")
        print(f"   - Start with priority symbols: --symbols AAPL,MSFT,GOOGL,AMZN,TSLA")
    else:
        print(f"   - Use --concurrent 3 --premium (faster for premium tier)")
        print(f"   - Full population is feasible with premium plan")
    
    print(f"\n📈 Next steps:")
    print(f"   1. Test with debug mode first")
    print(f"   2. Monitor API usage and rate limits")  
    print(f"   3. Use checkpoint system for long-running jobs")
    print(f"   4. Set up monitoring for progress tracking")
    
    return 0

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)