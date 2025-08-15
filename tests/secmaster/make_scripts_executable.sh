#!/bin/bash
set -e

# Make all test scripts executable
chmod +x /home/jianjun/ats-genai/tests/secmaster/run_polygon_test.sh
chmod +x /home/jianjun/ats-genai/tests/secmaster/test_k8s_environment.sh
chmod +x /home/jianjun/ats-genai/tests/secmaster/docker_k8s_test.sh
chmod +x /home/jianjun/ats-genai/tests/secmaster/verify_k8s_job.sh

echo "All test scripts are now executable"
echo "Run the verification script first:"
echo "./tests/secmaster/verify_k8s_job.sh"
