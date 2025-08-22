#!/bin/bash
# Script to organize the scripts directory

# Create directories if they don't exist
mkdir -p kubernetes database flyte utils testing deployment

# Move Kubernetes-related scripts
echo "Moving Kubernetes scripts..."
mv -f k8s-config.yaml kubernetes/ 2>/dev/null || true
mv -f setup-cluster.sh kubernetes/ 2>/dev/null || true
mv -f setup_docker_desktop_k8s.sh kubernetes/ 2>/dev/null || true
mv -f setup_kind_k8s.sh kubernetes/ 2>/dev/null || true
mv -f setup_minikube_k8s.sh kubernetes/ 2>/dev/null || true
mv -f setup_minikube_autostart.sh kubernetes/ 2>/dev/null || true
mv -f start-kubernetes.sh kubernetes/ 2>/dev/null || true
mv -f stop-kubernetes.sh kubernetes/ 2>/dev/null || true
mv -f start_minikube_wsl.sh kubernetes/ 2>/dev/null || true
mv -f update_k8s_scripts.py kubernetes/ 2>/dev/null || true
mv -f test_minikube_job.py kubernetes/ 2>/dev/null || true
mv -f README_k8s_job_runner.md kubernetes/ 2>/dev/null || true
mv -f README_job_generator.md kubernetes/ 2>/dev/null || true
mv -f WSL-KUBERNETES-SETUP.md kubernetes/ 2>/dev/null || true

# Move testing scripts
echo "Moving testing scripts..."
mv -f test_api_connectivity.py testing/ 2>/dev/null || true
mv -f test_populate_instrument_polygon.py testing/ 2>/dev/null || true
mv -f integration_test_run.py testing/ 2>/dev/null || true
mv -f api_verification/ testing/ 2>/dev/null || true
mv -f automated_dev_tests/ testing/ 2>/dev/null || true

# Move deployment scripts
echo "Moving deployment scripts..."
mv -f create_prod_simple.py deployment/ 2>/dev/null || true

# Move utility scripts
echo "Moving utility scripts..."
mv -f print_env_logging.py utils/ 2>/dev/null || true
mv -f wsl-kubernetes.service utils/ 2>/dev/null || true
mv -f minikube-autostart.service utils/ 2>/dev/null || true
mv -f start-wsl-kubernetes.bat utils/ 2>/dev/null || true
mv -f start-wsl-kubernetes.ps1 utils/ 2>/dev/null || true

# Move database scripts
echo "Moving database scripts..."
mv -f setup_db_env.sh database/ 2>/dev/null || true
mv -f db_checks/ database/ 2>/dev/null || true

echo "Script organization complete!"
