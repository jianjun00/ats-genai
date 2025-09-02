#!/usr/bin/env python3
"""
D: Drive Storage Setup for EODHD 30-Year Population

Sets up the proper directory structure and verifies D: drive access
for storing 30 years of minute bar data from EODHD.

This script handles:
- WSL/Windows path mapping for D: drive
- Directory structure creation
- Permission verification
- Storage estimation and validation
- Environment variable configuration
"""

import os
import sys
from pathlib import Path
import subprocess
import json
from datetime import datetime
import shutil

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

def estimate_storage_requirements():
    """Estimate storage requirements for 30 years of minute data"""
    
    # Conservative estimates based on minute bar data
    assumptions = {
        'symbols': 3000,  # Major US stocks
        'trading_days_per_year': 252,
        'minutes_per_day': 390,  # 6.5 hours * 60 minutes
        'years': 30,
        'bytes_per_bar': 50,  # Conservative estimate for compressed Parquet
        'compression_ratio': 0.6,  # Snappy compression
        'metadata_overhead': 1.2,  # 20% overhead for metadata and indices
    }
    
    total_bars = (assumptions['symbols'] * 
                  assumptions['trading_days_per_year'] * 
                  assumptions['minutes_per_day'] * 
                  assumptions['years'])
    
    raw_bytes = total_bars * assumptions['bytes_per_bar']
    compressed_bytes = raw_bytes * assumptions['compression_ratio']
    total_bytes = compressed_bytes * assumptions['metadata_overhead']
    
    return {
        'assumptions': assumptions,
        'total_bars': total_bars,
        'estimated_size_bytes': total_bytes,
        'estimated_size_gb': round(total_bytes / (1024**3), 2),
        'estimated_size_tb': round(total_bytes / (1024**4), 2)
    }

def create_directory_structure(base_path):
    """Create the required directory structure"""
    
    directories = [
        "minute-bars",           # Main minute bar storage
        "minute-bars/backups",   # Backup files
        "minute-bars/metadata",  # File metadata
        "minute-bars/temp",      # Temporary files during processing
        "logs",                  # Processing logs
        "checkpoints",           # Checkpoint files for resumable processing
        "reports"                # Population reports and statistics
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

def create_test_file(base_path):
    """Create a test file to verify write permissions"""
    
    test_file = Path(base_path) / "ats-data" / "test_write.txt"
    
    try:
        with open(test_file, 'w') as f:
            f.write(f"Test write at {datetime.now()}\n")
        
        # Try to read it back
        with open(test_file, 'r') as f:
            content = f.read()
        
        # Clean up
        test_file.unlink()
        
        return True, "Write test successful"
    
    except Exception as e:
        return False, f"Write test failed: {e}"

def generate_env_config(d_drive_path):
    """Generate environment configuration for the population script"""
    
    config = {
        'MINUTE_DATA_PATH': str(Path(d_drive_path) / "ats-data" / "minute-bars"),
        'CHECKPOINT_PATH': str(Path(d_drive_path) / "ats-data" / "checkpoints"),
        'LOG_PATH': str(Path(d_drive_path) / "ats-data" / "logs"),
        'BACKUP_PATH': str(Path(d_drive_path) / "ats-data" / "minute-bars" / "backups"),
        'D_DRIVE_ROOT': d_drive_path
    }
    
    return config

def main():
    """Main setup function"""
    
    print("🚀 Setting up D: drive storage for EODHD 30-year population")
    print("=" * 60)
    
    # Step 1: Detect D: drive
    print("1. Detecting D: drive path...")
    d_drive = detect_d_drive_path()
    
    if not d_drive:
        print("❌ D: drive not found or not accessible")
        print("Please ensure:")
        print("  - D: drive exists and is mounted")
        print("  - You have write permissions")
        print("  - WSL can access Windows drives")
        return 1
    
    print(f"✅ D: drive found at: {d_drive}")
    
    # Step 2: Check disk space
    print("\n2. Checking disk space...")
    usage = get_disk_usage(d_drive)
    
    if 'error' in usage:
        print(f"❌ Cannot check disk space: {usage['error']}")
        return 1
    
    print(f"💾 Disk usage:")
    print(f"   Total: {usage['total_gb']:,.1f} GB")
    print(f"   Used:  {usage['used_gb']:,.1f} GB")
    print(f"   Free:  {usage['free_gb']:,.1f} GB")
    
    # Step 3: Estimate storage requirements
    print("\n3. Estimating storage requirements...")
    estimates = estimate_storage_requirements()
    
    print(f"📊 Storage estimates for 30-year population:")
    print(f"   Symbols: {estimates['assumptions']['symbols']:,}")
    print(f"   Total bars: {estimates['total_bars']:,}")
    print(f"   Estimated size: {estimates['estimated_size_gb']:,.1f} GB ({estimates['estimated_size_tb']:.2f} TB)")
    
    if estimates['estimated_size_gb'] > usage['free_gb']:
        print("⚠️  WARNING: Estimated storage exceeds available space!")
        print("   Consider:")
        print("   - Processing in smaller batches")
        print("   - Using higher compression")
        print("   - Clearing space on D: drive")
    else:
        print("✅ Sufficient space available")
    
    # Step 4: Create directory structure
    print("\n4. Creating directory structure...")
    created, failed = create_directory_structure(d_drive)
    
    if failed:
        print(f"❌ {len(failed)} directories failed to create:")
        for failure in failed:
            print(f"   {failure['path']}: {failure['error']}")
        return 1
    
    print(f"✅ Created {len(created)} directories successfully")
    
    # Step 5: Test write permissions
    print("\n5. Testing write permissions...")
    write_ok, write_msg = create_test_file(d_drive)
    
    if not write_ok:
        print(f"❌ {write_msg}")
        return 1
    
    print(f"✅ {write_msg}")
    
    # Step 6: Generate configuration
    print("\n6. Generating configuration...")
    env_config = generate_env_config(d_drive)
    
    # Save config to file
    config_file = Path("d_drive_config.json")
    with open(config_file, 'w') as f:
        json.dump(env_config, f, indent=2)
    
    print(f"✅ Configuration saved to: {config_file}")
    
    # Step 7: Generate setup summary
    print("\n" + "=" * 60)
    print("🎉 D: DRIVE SETUP COMPLETE!")
    print("=" * 60)
    
    print("\n📁 Storage paths configured:")
    for key, value in env_config.items():
        print(f"   {key}: {value}")
    
    print(f"\n🚀 Ready to run EODHD population:")
    print(f"   python scripts/populate_30year_eodhd_minute_bars.py \\")
    print(f"     --storage-path {env_config['MINUTE_DATA_PATH']} \\")
    print(f"     --checkpoint-file {Path(env_config['CHECKPOINT_PATH']) / 'eodhd_30year_checkpoint.json'} \\")
    print(f"     --debug  # Start with debug mode for testing")
    
    print(f"\n📋 Environment variables (add to your .bashrc or .zshrc):")
    for key, value in env_config.items():
        print(f"   export {key}='{value}'")
    
    return 0

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)