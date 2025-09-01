#!/usr/bin/env python3
"""
Temporary worker script for Docker parallel backfill
Worker 2: 10 symbols
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
        '--symbols', 'ABPWW,ABTS,ABVE,ABVEW,ABVX,AC,ACCS,ACFN,ACGLN,ACGLO',
        '--checkpoint-file', 'docker_parallel_worker_2_checkpoint.json',
        '--debug'
    ]
    main()
