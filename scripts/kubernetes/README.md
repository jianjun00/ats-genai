# Kubernetes Scripts

This directory contains scripts for managing Kubernetes deployments, jobs, and configurations for the ATS-GenAI project.

## Key Scripts

- **k8s_job_generator.py** - Generates Kubernetes job YAML files with parameterized configurations
- **run_k8s_job.py** - Runs Kubernetes jobs directly using kubectl
- **instrument_polygon_job_generator.py** - Generates job YAML files for instrument-polygon operations
- **demo_job_generator.py** - Examples of different job configurations
- **custom_job_generator.py** - Creates custom job configurations

## Setup Scripts

- **setup-cluster.sh** - Sets up a Kubernetes cluster
- **setup_docker_desktop_k8s.sh** - Configures Docker Desktop for Kubernetes
- **setup_kind_k8s.sh** - Sets up a Kind Kubernetes cluster
- **setup_minikube_k8s.sh** - Sets up a Minikube Kubernetes cluster
- **setup_minikube_autostart.sh** - Configures Minikube to start automatically

## Secret Management

- **create_k8s_secrets.sh** - Creates Kubernetes secrets
- **env_to_k8s_secrets.py** - Converts environment variables to Kubernetes secrets
- **setup_k8s_secrets.sh** - Sets up Kubernetes secrets
- **test_k8s_secrets.sh** - Tests Kubernetes secrets
- **update-secrets.sh** - Updates Kubernetes secrets

## Documentation

- **README_job_generator.md** - Documentation for the job generator
- **README_k8s_job_runner.md** - Documentation for the job runner
- **WSL-KUBERNETES-SETUP.md** - Instructions for setting up Kubernetes in WSL
