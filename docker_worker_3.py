#!/usr/bin/env python3
"""
Temporary worker script for Docker parallel backfill
Worker 3: 10 symbols
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
        '--symbols', 'ACHR,ACHV,ACI,ACIC,ACIU,ACIW,ACLS,ACLX,ACM,ACMR',
        '--checkpoint-file', 'docker_parallel_worker_3_checkpoint.json',
        '--debug'
    ]
    main()
