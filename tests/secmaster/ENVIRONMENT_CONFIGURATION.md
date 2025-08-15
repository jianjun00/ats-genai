# Kubernetes Environment Configuration Summary

## Problem Statement

The Kubernetes jobs were failing because:
1. The Docker image only supported 'test', 'intg', and 'prod' environments
2. The 'dev' environment was needed for proper operation in the ats-dev namespace
3. Gin config files were not being explicitly specified

## Solution Implemented

### 1. Kubernetes Job Configuration Updates
- Updated `instrument-polygon-job.yaml` and `instrument-polygon-backfill-job.yaml` to:
  - Use 'dev' environment explicitly
  - Specify the Gin config file path with `--gin_config /app/config/app_docker.gin`

### 2. Docker Image Updates
- Created `Dockerfile.update` to build an updated image with 'dev' environment support
- Created `update_docker_image.sh` script to:
  - Build the updated Docker image
  - Test that it supports the 'dev' environment
  - Provide commands for tagging and pushing to Docker Hub

### 3. Testing Scripts
- Updated `docker_k8s_test.sh` to use 'test' environment temporarily until Docker image is updated
- Created `verify_k8s_job.sh` to validate Kubernetes YAML files
- Created `check_k8s_jobs.sh` to monitor job status and logs
- Created `make_scripts_executable.sh` to ensure all scripts are executable

### 4. Documentation Updates
- Updated `README.md` with:
  - Detailed testing process
  - Environment configuration explanation
  - Docker image update instructions
  - Troubleshooting tips
- Updated `ats.context` with best practices for environment handling

## Best Practices Established

1. Always explicitly specify the Gin config file path with `--gin_config` in Kubernetes jobs
2. Match environment parameter to the deployment namespace:
   - Use 'dev' for ats-dev namespace
   - Use 'intg' for integration testing
   - Use 'prod' for production
3. Use `app_docker.gin` for containerized environments
4. When updating code that affects environment handling, ensure both Docker image and K8s configurations are updated

## Job Immutability Issue

When updating Kubernetes job configurations, we encountered the following issue:

```
The Job "instrument-polygon-aapl" is invalid: spec.template: Invalid value: ... field is immutable
```

This occurs because Kubernetes job templates are immutable after creation. To address this:

1. Created `recreate_k8s_jobs.sh` script that:
   - Deletes existing jobs with `kubectl delete job`
   - Waits for deletion to complete
   - Creates new jobs with the updated configuration
   - Verifies job creation status

## Next Steps

1. Push the updated Docker image to Docker Hub
2. Use the `recreate_k8s_jobs.sh` script to properly redeploy Kubernetes jobs
3. Monitor job status and logs using the check_k8s_jobs.sh script
4. Consider creating a dedicated `app_dev.gin` file for better environment support
