# Kubernetes Job Generator for ATS-GenAI

This tool provides a parameterized approach to generate Kubernetes job YAML files for instrument-polygon operations. It allows you to create different job configurations from a single template, avoiding duplication of job logic.

## Features

- Generate Kubernetes job YAML files for both backfill and test operations
- Customize job parameters like resource limits, tickers, and job names
- Apply generated jobs directly to your Kubernetes cluster
- Consistent job structure with proper environment variables and secrets

## Prerequisites

- Python 3.8+
- PyYAML package
- Kubernetes cluster (for applying jobs)
- kubectl (for applying jobs)

## Installation

No installation is required. The script can be run directly from the project directory.

## Usage

### Basic Usage

```bash
# Generate a test job with specific tickers
python scripts/instrument_polygon_job_generator.py \
  --job-type test \
  --tickers "AAPL,MSFT,AMZN" \
  --output k8s/generated/test-job.yaml

# Generate a backfill job
python scripts/instrument_polygon_job_generator.py \
  --job-type backfill \
  --output k8s/generated/backfill-job.yaml
```

### Advanced Usage

```bash
# Generate a test job with custom name and resource requirements
python scripts/instrument_polygon_job_generator.py \
  --job-type test \
  --tickers "TSLA,NVDA,META" \
  --custom-name "custom-polygon-job" \
  --memory-request "384Mi" \
  --memory-limit "768Mi" \
  --cpu-request "150m" \
  --cpu-limit "300m" \
  --output k8s/generated/custom-job.yaml

# Generate and apply a job directly to the cluster
python scripts/instrument_polygon_job_generator.py \
  --job-type test \
  --tickers "AAPL,MSFT,AMZN" \
  --output k8s/generated/test-job.yaml \
  --apply
```

## Command Line Arguments

| Argument | Description | Default |
|----------|-------------|---------|
| `--job-type` | Type of job to generate (`backfill` or `test`) | (Required) |
| `--output` | Output YAML file path | (Required) |
| `--tickers` | Comma-separated list of tickers (for test job only) | "NFLX,GOOG,AVGO,ADBE,COST" |
| `--debug` | Add debug flag to the command | False |
| `--apply` | Apply the job to the Kubernetes cluster | False |
| `--custom-name` | Custom name for the job | (Job type dependent) |
| `--memory-request` | Memory request override | (Job type dependent) |
| `--memory-limit` | Memory limit override | (Job type dependent) |
| `--cpu-request` | CPU request override | (Job type dependent) |
| `--cpu-limit` | CPU limit override | (Job type dependent) |

## Job Types

### Backfill Job

The backfill job is designed for full instrument polygon data backfill operations. It has higher resource limits and a longer timeout to handle the larger workload.

Default configuration:
- Memory: 512Mi request, 1Gi limit
- CPU: 200m request, 500m limit
- Timeout: 2 hours (activeDeadlineSeconds: 7200)
- Restart policy: Never

### Test Job

The test job is designed for testing with a specific subset of tickers. It has lower resource limits and is configured for quick testing.

Default configuration:
- Memory: 256Mi request, 512Mi limit
- CPU: 100m request, 250m limit
- Restart policy: OnFailure
- Default tickers: "NFLX,GOOG,AVGO,ADBE,COST"

## Examples

### Example 1: Generate a test job with custom tickers

```bash
python scripts/instrument_polygon_job_generator.py \
  --job-type test \
  --tickers "AAPL,MSFT,AMZN" \
  --debug \
  --output k8s/generated/test-custom-tickers.yaml
```

### Example 2: Generate a backfill job with increased resources

```bash
python scripts/instrument_polygon_job_generator.py \
  --job-type backfill \
  --memory-request "1Gi" \
  --memory-limit "2Gi" \
  --cpu-request "300m" \
  --cpu-limit "600m" \
  --output k8s/generated/high-resource-backfill.yaml
```

### Example 3: Generate and apply a custom job

```bash
python scripts/instrument_polygon_job_generator.py \
  --job-type test \
  --tickers "TSLA,NVDA,META" \
  --custom-name "urgent-polygon-update" \
  --output k8s/generated/urgent-job.yaml \
  --apply
```

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
      - name: Generate job YAML
        run: |
          python scripts/instrument_polygon_job_generator.py \
            --job-type ${{ github.event.inputs.job_type }} \
            --tickers ${{ github.event.inputs.tickers }} \
            --output k8s/generated/job.yaml
      # Apply to cluster steps would follow
```
