# Database Check Scripts

This directory contains scripts for checking and verifying database connectivity, schema, and data in various environments.

## Scripts

- **check_db.py**: General database check script that lists databases and examines their structure.
- **check_ats_dev_db.py**: Specifically checks the ATS development database.
- **check_ats_dev_k8s_db.py**: Checks the ATS development database in Kubernetes.
- **check_db_connection.py**: Tests basic database connectivity.
- **check_db_schema.py**: Verifies database schema against expected structure.
- **check_test_db.py**: Checks test databases.
- **check_trading_db.py**: Examines trading-specific database tables and data.

## Usage

Most scripts can be run directly:

```bash
python scripts/db_checks/check_db.py
python scripts/db_checks/check_ats_dev_db.py
```

Some scripts may require environment variables to be set for database connection parameters.
