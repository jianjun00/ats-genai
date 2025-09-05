#!/usr/bin/env python3
"""
Run training data generation for AAPL and TSLA using existing infrastructure
"""
import subprocess
import sys
import os

def main():
    # Set proper environment
    os.environ['ENVIRONMENT_TYPE'] = 'dev'
    os.environ['PYTHONPATH'] = '/workspace/src'
    
    # Run the training data callback runner with proper arguments
    cmd = [
        'python3', 
        '/workspace/src/ml/training_data/runners/training_data_callback_runner.py',
        '--symbols', 'AAPL', 'TSLA',
        '--start-date', '2025-07-01', 
        '--end-date', '2025-09-03',
        '--environment', 'dev',
        '--output-dir', '/data/training',
        '--gin-config', '/workspace/config/training_data.gin'
    ]
    
    print(f"🚀 Running command: {' '.join(cmd)}")
    
    # Execute the command
    result = subprocess.run(cmd, capture_output=False)
    return result.returncode

if __name__ == "__main__":
    sys.exit(main())