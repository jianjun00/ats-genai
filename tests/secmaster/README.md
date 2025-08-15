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

6. **Check Kubernetes job status and logs**
   ```bash
   ./check_k8s_jobs.sh
   ```
   This script:
   - Verifies kubectl connectivity
   - Checks job and pod status in the ats-dev namespace
   - Retrieves logs from the most recent job pods

## Environment Configuration

The Kubernetes jobs are configured to use the `dev` environment, which maps to `config/app_docker.gin`. This is because:

1. The Kubernetes jobs run in the development namespace (`ats-dev`)
2. The environment parameter should match the namespace
3. The `app_intg.gin` file is used for integration testing, not for development

## Kubernetes Job Configurations

### Single Ticker Job
```yaml
command: ["python", "-m", "src.secmaster.populate_instrument_polygon", "--environment", "dev", "--gin_config", "/app/config/app_docker.gin", "--ticker", "AAPL"]
```

### Backfill Job
```yaml
command: ["python", "-m", "src.secmaster.populate_instrument_polygon", "--environment", "dev", "--gin_config", "/app/config/app_docker.gin"]
```

## Troubleshooting

If any test fails:

1. Check the error message for specific issues
2. Verify the environment parameter matches an existing Gin config
3. Ensure the Gin config doesn't contain incompatible references
4. Check for missing dependencies or environment variables
5. Always explicitly specify the Gin config file path with `--gin_config` to avoid any environment-to-config mapping issues

## Database Connection Configuration

The application needs to connect to different database hosts depending on the environment:

- In local development/test: Uses `localhost:5432`
- In Kubernetes `dev` environment: Uses `timescaledb.ats-dev.svc.cluster.local:5432`
- In Kubernetes `intg` environment: Uses `timescaledb.ats-intg.svc.cluster.local:5432`

The database connection is configured in `src/config/database.py` and automatically selects the appropriate host based on the `ENVIRONMENT` environment variable.

### Updating Database Connection Configuration

If you need to modify the database connection logic:

1. Update `src/config/database.py` with the new connection logic
2. Update the Docker image using the `update_db_connection.sh` script
3. Recreate the Kubernetes jobs using the `recreate_k8s_jobs.sh` script
4. Verify the database connection in the job logs

## Docker Image Update

The current Docker image only supports `test`, `intg`, and `prod` environments. To update the image to support the `dev` environment:

1. **Update environment.py**
   First, ensure that `src/config/environment.py` includes support for the `dev` environment:
   - Add `DEV = "dev"` to the `EnvironmentType` enum
   - Update the environment detection logic to handle `dev` environment
   - Update the config path selection logic to map `dev` to the appropriate Gin config

2. **Update Docker image with environment changes**
   ```bash
   ./update_docker_env.sh
   ```
   This script:
   - Creates a container from the existing image
   - Copies the updated `environment.py` file into the container
   - Commits the changes to create an updated image
   - Tests that the updated image properly supports the `dev` environment
   - Tags and pushes the updated image to Docker Hub

3. **Alternative: Build and test a completely new image**
   ```bash
   ./update_docker_image.sh
   ```
   This script:
   - Builds a new Docker image with `dev` environment support
   - Tests the image to verify it works with the `dev` environment
   - Provides commands to tag and push the updated image

4. **Redeploy Kubernetes jobs**
   After pushing the updated image, redeploy your Kubernetes jobs using the recreation script:
   ```bash
   ./recreate_k8s_jobs.sh
   ```
   This script:
   - Deletes existing jobs (required because job templates are immutable)
   - Creates new jobs with the updated configuration
   - Checks job status after creation

5. **Check job status and logs**
   ```bash
   ./check_pod_logs.sh
   ```
   This script:
   - Verifies kubectl connectivity
   - Checks pod status in the ats-dev namespace
   - Retrieves logs from the most recent job pods
   - Shows detailed event information for each pod

6. **Verify environment detection**
   When checking logs, look for this line to confirm the environment is detected correctly:
   ```
   [GIN DEBUG] Using Gin config: /app/config/app_docker.gin, env_type=EnvironmentType.DEV
   ```
   If you see a different environment type or an error about invalid environment type, the Docker image may not have the updated environment.py file.

## Future Improvements

1. Consider creating a dedicated `app_dev.gin` file to better support the `dev` environment
2. Ensure the script's environment-to-config mapping correctly handles `dev` -> `app_docker.gin`
3. Integrate these tests into CI/CD pipeline
