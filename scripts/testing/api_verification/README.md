# API Verification Scripts

This directory contains scripts for verifying and monitoring the ATS API in Kubernetes environments.

## Scripts

- **verify_api_and_db.py**: Comprehensive verification script that tests API endpoints, database connectivity, and compares data between them. Includes port-forwarding setup.
- **monitor_api_job.py**: Monitors the status of API test jobs in Kubernetes, shows logs, and can set up port-forwarding.
- **test_api_connectivity.py**: Simple script to test basic API connectivity and endpoints.

## Usage

Most scripts support command-line arguments for customization. Run with `--help` to see available options:

```bash
python scripts/api_verification/verify_api_and_db.py --help
python scripts/api_verification/monitor_api_job.py --help
```

For more detailed information about the API verification process, see the [API_VERIFICATION.md](../../docs/API_VERIFICATION.md) documentation.
