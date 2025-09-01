#!/usr/bin/env python3
"""
Temporary worker script for Docker parallel backfill
Worker 1: 10 symbols
"""
import sys
import os
sys.path.append('/workspace/src')
os.chdir('/workspace')

from scripts.populate_firstrate_minute_bars import main
import argparse

if __name__ == "__main__":
    # Mock the command line arguments
    import sys
    sys.argv = [
        'populate_firstrate_minute_bars.py',
        '--asset-type', 'stock',
        '--symbols', 'AARD,AAUC,ABAT,ABI,ABL,ABLLL,ABLLW,ABLV,ABLVW,ABP',
        '--checkpoint-file', 'docker_parallel_worker_1_checkpoint.json',
        '--debug'
    ]
    main()
