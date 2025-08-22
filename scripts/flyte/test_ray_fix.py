#!/usr/bin/env python
"""
Test script for Ray fixes in the instrument polygon workflow.

This script directly runs the instrument polygon workflow locally to test
the Ray fixes without needing Flyte remote execution.
"""

import os
import sys
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

# Import the workflow
from scripts.flyte.flyte_instrument_polygon_workflow import instrument_polygon_workflow

def main():
    """Run the instrument polygon workflow locally to test Ray fixes."""
    # Set environment variables for Ray local mode
    os.environ["RAY_TMPDIR"] = "/tmp/ray"
    os.environ["RAY_DISABLE_DASHBOARD"] = "1"
    os.environ["RAY_USAGE_STATS_ENABLED"] = "0"
    os.environ["RAY_WORKER_REGISTER_TIMEOUT_SECONDS"] = "60"
    os.environ["PYTHONUNBUFFERED"] = "1"
    os.environ["RAY_ADDRESS"] = "local"
    
    # Run the workflow locally
    job_type = "test"
    tickers = "AAPL,MSFT"
    custom_name = "test-ray-fix-local"
    output_dir = os.path.join(project_root, "k8s", "generated")
    
    print(f"Running instrument polygon workflow with job_type={job_type}, tickers={tickers}")
    print(f"Output directory: {output_dir}")
    
    result = instrument_polygon_workflow(
        job_type=job_type,
        tickers=tickers,
        memory_request="256Mi",
        memory_limit="512Mi",
        cpu_request="100m",
        cpu_limit="200m",
        custom_name=custom_name,
        should_apply=False,
        output_dir=output_dir
    )
    
    print(f"Workflow result: {result}")
    print(f"Check the generated YAML file in {output_dir} to verify Ray environment variables")

if __name__ == "__main__":
    main()
