# Instrument Polygon Kubernetes Job Testing

This directory contains test scripts and utilities for validating the instrument polygon data jobs before deploying to Kubernetes.

## Testing Process

Follow these steps to ensure your Kubernetes jobs will work correctly:

1. **Make scripts executable**
   ```bash
   ./make_scripts_executable.sh
   ```

2. **Verify Kubernetes YAML files**
   ```bash
   ./verify_k8s_job.sh
   ```
   This script checks:
   - YAML file existence
   - Environment parameter correctness
   - Gin config file existence
   - No problematic UniverseStateIntervalBuilder references
   - Dry run kubectl apply

3. **Run unit tests**
   ```bash
   ./run_polygon_test.sh
   ```
   This script:
   - Runs pytest unit tests for `populate_instrument_polygon.py`
   - Tests the script directly with AAPL ticker

4. **Test in simulated Kubernetes environment**
   ```bash
   ./test_k8s_environment.sh
   ```
   This script:
   - Creates a test directory simulating the container environment
   - Tests the exact command that will run in Kubernetes

5. **Test with Docker (closest to actual K8s environment)**
   ```bash
   ./docker_k8s_test.sh
   ```
   This script:
   - Uses the same Docker image as in Kubernetes
   - Tests both single ticker and backfill configurations

## Environment Configuration

The Kubernetes jobs are configured to use the `dev` environment, which maps to `config/app_docker.gin`. This is because:

1. The Kubernetes jobs run in the development namespace (`ats-dev`)
2. The environment parameter should match the namespace
3. The `app_intg.gin` file is used for integration testing, not for development

## Kubernetes Job Configurations

### Single Ticker Job
```yaml
command: ["python", "-m", "src.secmaster.populate_instrument_polygon", "--environment", "dev", "--ticker", "AAPL"]
```

### Backfill Job
```yaml
command: ["python", "-m", "src.secmaster.populate_instrument_polygon", "--environment", "dev"]
```

## Troubleshooting

If any test fails:

1. Check the error message for specific issues
2. Verify the environment parameter matches an existing Gin config
3. Ensure the Gin config doesn't contain incompatible references
4. Check for missing dependencies or environment variables

## Future Improvements

1. Consider creating a dedicated `app_dev.gin` file to better support the `dev` environment
2. Ensure the script's environment-to-config mapping correctly handles `dev` -> `app_docker.gin`
3. Integrate these tests into CI/CD pipeline
