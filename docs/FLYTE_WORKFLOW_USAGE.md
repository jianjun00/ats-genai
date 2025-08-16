# Flyte Workflow for Kubernetes Job Management

This document describes the Flyte workflow implementation for generating and applying Kubernetes jobs for instrument polygon operations.

## Overview

The Flyte workflow provides a dynamic way to generate Kubernetes job YAML configurations and optionally apply them to the cluster. It supports two job types:

1. **Test Job**: For testing instrument polygon operations with specific tickers
2. **Backfill Job**: For backfilling instrument polygon data

## Key Features

- Dynamic workflow that avoids conditional logic issues in Flyte
- Parameterized job configuration (resources, names, etc.)
- Option to save generated YAML to a file
- Option to apply the job directly to the Kubernetes cluster
- Support for custom resource requirements

## Usage

### Command Line Interface

```bash
python scripts/flyte_instrument_polygon_workflow.py [OPTIONS]
```

#### Required Arguments

- `--job-type {backfill,test}`: Type of job to generate

#### Optional Arguments

- `--tickers STRING`: Comma-separated list of tickers (for test job only)
- `--memory-request STRING`: Memory request (e.g., 256Mi)
- `--memory-limit STRING`: Memory limit (e.g., 512Mi)
- `--cpu-request STRING`: CPU request (e.g., 100m)
- `--cpu-limit STRING`: CPU limit (e.g., 250m)
- `--debug`: Enable debug mode
- `--custom-name STRING`: Custom job name
- `--apply`: Apply the job to the cluster
- `--output-dir STRING`: Directory to save the generated YAML (default: /home/jianjun/ats-genai/k8s/generated)

### Examples

#### Generate a Test Job YAML

```bash
python scripts/flyte_instrument_polygon_workflow.py \
  --job-type test \
  --tickers "AAPL,MSFT,GOOG" \
  --output-dir /home/jianjun/ats-genai/k8s/generated
```

#### Generate a Backfill Job YAML with Custom Resources

```bash
python scripts/flyte_instrument_polygon_workflow.py \
  --job-type backfill \
  --memory-request "1Gi" \
  --memory-limit "2Gi" \
  --cpu-request "500m" \
  --cpu-limit "1000m" \
  --output-dir /home/jianjun/ats-genai/k8s/generated
```

#### Generate and Apply a Job to the Cluster

```bash
python scripts/flyte_instrument_polygon_workflow.py \
  --job-type test \
  --tickers "AAPL,TSLA" \
  --custom-name "custom-test-job" \
  --apply
```

## Implementation Details

The workflow uses Flyte's dynamic workflow feature to avoid conditional logic issues. The main components are:

1. **Task Functions**:
   - `generate_test_job_yaml`: Generates YAML for test jobs
   - `generate_backfill_job_yaml`: Generates YAML for backfill jobs
   - `save_yaml_to_file`: Saves the generated YAML to a file
   - `apply_to_kubernetes`: Applies the job to the Kubernetes cluster
   - `format_result`: Formats the result message

2. **Dynamic Workflow**:
   - `dynamic_job_workflow`: Handles job type selection at runtime

3. **Main Workflow**:
   - `instrument_polygon_workflow`: Entry point that calls the dynamic workflow

## Integration with Other Tools

This Flyte workflow can be integrated with:

1. **GitHub Actions**: For automated job generation and application
2. **CI/CD Pipelines**: For scheduled job execution
3. **Flyte Console**: For visual workflow management

## Troubleshooting

If you encounter issues:

1. Ensure kubectl is properly configured
2. Check that the output directory exists and is writable
3. Verify that the job parameters are valid
4. For apply failures, check Kubernetes permissions
