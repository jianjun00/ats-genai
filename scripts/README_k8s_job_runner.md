# Kubernetes Job Runner for ATS-GenAI

This tool allows you to generate and directly apply Kubernetes job configurations for instrument-polygon operations. It builds on the job generator functionality but adds the ability to apply jobs directly to your Kubernetes cluster.

## Features

- Generate Kubernetes job YAML files for both backfill and test operations
- Apply generated jobs directly to your Kubernetes cluster
- Support for dry-run validation before applying
- Customize job parameters like resource limits, tickers, and job names
- Print YAML to stdout for inspection

## Prerequisites

- Python 3.8+
- PyYAML package
- kubectl configured with access to your Kubernetes cluster
- Kubernetes cluster with the ats-dev namespace

## Usage

### Basic Usage

```bash
# Generate a job YAML and save it to a file
python scripts/run_k8s_job.py \
  --job-type test \
  --tickers "AAPL,MSFT,AMZN" \
  --output k8s/generated/test-job.yaml

# Generate a job YAML and apply it to the cluster
python scripts/run_k8s_job.py \
  --job-type test \
  --tickers "AAPL,MSFT,AMZN" \
  --apply

# Generate a job YAML, save it, and apply it to the cluster
python scripts/run_k8s_job.py \
  --job-type test \
  --tickers "AAPL,MSFT,AMZN" \
  --output k8s/generated/test-job.yaml \
  --apply

# Print the job YAML to stdout without saving or applying
python scripts/run_k8s_job.py \
  --job-type backfill
```

### Advanced Usage

```bash
# Validate a job without applying it (dry-run)
python scripts/run_k8s_job.py \
  --job-type test \
  --tickers "AAPL,MSFT,AMZN" \
  --apply \
  --dry-run

# Generate a job with custom resource requirements
python scripts/run_k8s_job.py \
  --job-type backfill \
  --memory-request "1Gi" \
  --memory-limit "2Gi" \
  --cpu-request "300m" \
  --cpu-limit "600m" \
  --apply
```

## Command Line Arguments

| Argument | Description | Default |
|----------|-------------|---------|
| `--job-type` | Type of job to generate (`backfill` or `test`) | (Required) |
| `--tickers` | Comma-separated list of tickers (for test job only) | "NFLX,GOOG,AVGO,ADBE,COST" |
| `--debug` | Add debug flag to the command | False |
| `--custom-name` | Custom name for the job | (Job type dependent) |
| `--memory-request` | Memory request override | (Job type dependent) |
| `--memory-limit` | Memory limit override | (Job type dependent) |
| `--cpu-request` | CPU request override | (Job type dependent) |
| `--cpu-limit` | CPU limit override | (Job type dependent) |
| `--output` | Output YAML file path | None (print to stdout) |
| `--apply` | Apply the job to the Kubernetes cluster | False |
| `--dry-run` | Validate the job without applying it (only with --apply) | False |

## Examples

### Example 1: Generate and apply a test job with specific tickers

```bash
python scripts/run_k8s_job.py \
  --job-type test \
  --tickers "AAPL,MSFT,AMZN" \
  --debug \
  --apply
```

This will:
1. Generate a test job configuration for the specified tickers
2. Add the debug flag to the job command
3. Apply the job directly to your Kubernetes cluster

### Example 2: Generate a backfill job with increased resources and validate it

```bash
python scripts/run_k8s_job.py \
  --job-type backfill \
  --memory-request "1Gi" \
  --memory-limit "2Gi" \
  --cpu-request "300m" \
  --cpu-limit "600m" \
  --apply \
  --dry-run
```

This will:
1. Generate a backfill job configuration with increased resource limits
2. Validate the job configuration against your Kubernetes cluster without actually applying it

### Example 3: Generate a job with a custom name and save it

```bash
python scripts/run_k8s_job.py \
  --job-type test \
  --tickers "TSLA,NVDA,META" \
  --custom-name "urgent-polygon-update" \
  --output k8s/generated/urgent-job.yaml
```

This will:
1. Generate a test job configuration for the specified tickers
2. Use a custom name for the job
3. Save the job YAML to the specified file

## Integration with CI/CD

This script can be integrated into your CI/CD pipeline to automatically generate and apply jobs based on different environments or conditions.

Example GitHub Actions workflow:

```yaml
name: Deploy Instrument Polygon Job

on:
  workflow_dispatch:
    inputs:
      job_type:
        description: 'Job type (backfill or test)'
        required: true
        default: 'test'
      tickers:
        description: 'Tickers for test job'
        required: false
        default: 'AAPL,MSFT,AMZN'
      apply:
        description: 'Apply to cluster'
        required: true
        default: 'false'
        type: boolean

jobs:
  deploy:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - name: Set up Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.10'
      - name: Install dependencies
        run: pip install pyyaml
      - name: Set up kubectl
        uses: azure/setup-kubectl@v3
        with:
          version: 'latest'
      - name: Configure kubectl
        run: |
          # Set up kubectl configuration here
          # This will depend on your cluster provider
      - name: Generate and apply job
        run: |
          python scripts/run_k8s_job.py \
            --job-type ${{ github.event.inputs.job_type }} \
            --tickers ${{ github.event.inputs.tickers }} \
            --output k8s/generated/job.yaml \
            ${{ github.event.inputs.apply == 'true' && '--apply' || '' }}
```
