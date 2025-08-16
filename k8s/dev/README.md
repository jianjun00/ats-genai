# Kubernetes Development Environment

This directory contains Kubernetes configuration files for the development environment.

## Database Setup

The database configuration has been consolidated into a single file: `database.yaml`. This file includes:

1. **Secret**: `db-credentials` - Contains database credentials
2. **ConfigMap**: `postgres-config` - Contains database configuration
3. **Service**: `postgres` - Exposes the database on port 5432
4. **Deployment**: `postgres` - Runs the TimescaleDB database with persistent storage
5. **PersistentVolumeClaim**: `postgres-pvc` - Provides persistent storage for database data
6. **Job**: `db-init` - Initializes the database schema
7. **Debug Pod**: `db-debug` - For debugging database connections

Additionally, a separate file `db-backup-job.yaml` contains:

8. **CronJob**: `db-backup` - Performs daily database backups
9. **PersistentVolumeClaim**: `db-backup-pvc` - Provides persistent storage for backups

### Applying the Database Configuration

```bash
# Apply the database configuration
kubectl apply -f k8s/dev/database.yaml

# Apply the database backup job (optional)
kubectl apply -f k8s/dev/db-backup-job.yaml

# Check the status of the database deployment
kubectl get pods -n ats-dev -l app=postgres

# Check if the database initialization job completed
kubectl get jobs -n ats-dev db-init

# Verify persistent volume claims were created
kubectl get pvc -n ats-dev
```

### Connecting to the Database

You can connect to the database using the debug pod:

```bash
# Start a shell in the debug pod
kubectl exec -it -n ats-dev db-debug -- /bin/bash

# Connect to the database from within the pod
psql -h postgres -U postgres -d ats_dev
```

Or you can port-forward the database service to your local machine:

```bash
# Port-forward the database service
kubectl port-forward -n ats-dev service/postgres 5432:5432

# Connect to the database from your local machine
psql -h localhost -U postgres -d ats_dev
```

### Database Credentials

The default database credentials are:

- **Host**: postgres
- **User**: postgres
- **Password**: dev_password
- **Database**: ats_dev

These credentials are stored in the `db-credentials` Secret and should be referenced in your applications using environment variables.

### Database Backups

The database backup job runs daily at 1:00 AM and stores backups in a persistent volume.

```bash
# Check the status of the backup job
kubectl get cronjobs -n ats-dev

# View backup job logs (replace with actual pod name)
kubectl logs -n ats-dev job/db-backup-<job-id>

# Manually trigger a backup
kubectl create job --from=cronjob/db-backup db-backup-manual -n ats-dev
```

### Health Checks

The database deployment includes both readiness and liveness probes to ensure the database is healthy. The probes use `pg_isready` to check the database status.

## Cleanup Notes

The following files have been archived to `k8s/dev/archive/`:

- `db-client-pod.yaml`
- `db-debug-pod.yaml`
- `db-credentials-secret.yaml`
- `init-db-job.yaml`
- `postgres-deployment.yaml`

These files have been consolidated into the `database.yaml` file for easier management.
