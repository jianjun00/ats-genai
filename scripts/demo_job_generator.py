#!/usr/bin/env python
"""
Demo Script for Instrument Polygon Job Generator

This script demonstrates practical usage of the instrument_polygon_job_generator.py
with different parameter sets and scenarios.
"""

import os
import argparse
import subprocess
from instrument_polygon_job_generator import create_backfill_job, create_test_job, save_yaml


def generate_demo_jobs(output_dir):
    """Generate a set of demo jobs with different configurations."""
    os.makedirs(output_dir, exist_ok=True)
    
    # Demo 1: Standard test job with default tickers
    test_job = create_test_job()
    save_yaml(test_job, os.path.join(output_dir, "demo-test-default.yaml"))
    
    # Demo 2: Test job with custom tickers
    custom_tickers_job = create_test_job(tickers="AAPL,MSFT,AMZN,TSLA,NVDA")
    custom_tickers_job.name = "test-polygon-custom-tickers"
    save_yaml(custom_tickers_job, os.path.join(output_dir, "demo-test-custom-tickers.yaml"))
    
    # Demo 3: Test job with debug flag
    debug_job = create_test_job(tickers="AAPL,MSFT", debug=True)
    debug_job.name = "test-polygon-debug"
    save_yaml(debug_job, os.path.join(output_dir, "demo-test-debug.yaml"))
    
    # Demo 4: Standard backfill job
    backfill_job = create_backfill_job()
    save_yaml(backfill_job, os.path.join(output_dir, "demo-backfill-default.yaml"))
    
    # Demo 5: Backfill job with increased resources
    high_resource_job = create_backfill_job()
    high_resource_job.name = "instrument-polygon-backfill-high-resource"
    high_resource_job.memory_request = "1Gi"
    high_resource_job.memory_limit = "2Gi"
    high_resource_job.cpu_request = "300m"
    high_resource_job.cpu_limit = "600m"
    save_yaml(high_resource_job, os.path.join(output_dir, "demo-backfill-high-resource.yaml"))
    
    # Demo 6: Test job with custom name and namespace
    custom_job = create_test_job(tickers="META,NFLX")
    custom_job.name = "custom-polygon-job"
    custom_job.namespace = "custom-namespace"
    save_yaml(custom_job, os.path.join(output_dir, "demo-custom-job.yaml"))
    
    print(f"Generated 6 demo job YAML files in {output_dir}")


def compare_jobs(output_dir):
    """Compare the differences between generated job YAMLs."""
    # Compare test job vs backfill job
    try:
        result = subprocess.run(
            ['diff', '-y', '--suppress-common-lines',
             os.path.join(output_dir, "demo-test-default.yaml"),
             os.path.join(output_dir, "demo-backfill-default.yaml")],
            capture_output=True,
            text=True
        )
        print("\n=== Key differences between test and backfill jobs ===")
        print(result.stdout)
    except subprocess.CalledProcessError:
        print("Error comparing files")
    
    # Compare default test job vs debug test job
    try:
        result = subprocess.run(
            ['diff', '-y', '--suppress-common-lines',
             os.path.join(output_dir, "demo-test-default.yaml"),
             os.path.join(output_dir, "demo-test-debug.yaml")],
            capture_output=True,
            text=True
        )
        print("\n=== Key differences between default and debug test jobs ===")
        print(result.stdout)
    except subprocess.CalledProcessError:
        print("Error comparing files")


def main():
    parser = argparse.ArgumentParser(description="Demo for Kubernetes job generator")
    parser.add_argument('--output-dir', type=str, default="/home/jianjun/ats-genai/k8s/generated/demo",
                        help='Directory to save the generated YAML files')
    parser.add_argument('--compare', action='store_true',
                        help='Compare the differences between generated job YAMLs')
    
    args = parser.parse_args()
    
    generate_demo_jobs(args.output_dir)
    
    if args.compare:
        compare_jobs(args.output_dir)
    
    print("\nDemo completed successfully!")
    print(f"To apply a job to your cluster: kubectl apply -f {args.output_dir}/demo-test-default.yaml")


if __name__ == "__main__":
    main()
