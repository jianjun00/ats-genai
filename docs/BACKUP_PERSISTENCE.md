# Database Backup Persistence in Minikube

## Issue Summary

**Question**: Does it mean if Minikube restarts, we lose backup data?  
**Answer**: **Yes, with the current Minikube setup, backup data would be lost if Minikube restarts.**

## Current Situation

### What We Have ✅
- **Working backup system**: Daily automated backups for ats-intg and ats-prod environments
- **Functional restore process**: Database restore works correctly by stopping/restarting PostgreSQL pods
- **Environment-specific notifications**: Slack alerts configured for each environment
- **Persistent database storage**: PostgreSQL data survives pod restarts using PersistentVolumes

### The Problem ⚠️
- **Backup files are stored inside Minikube VM**: Files exist at `/home/jianjun/ats-data/backups/` within the VM
- **VM storage is ephemeral**: When Minikube restarts, the VM filesystem resets
- **Host isolation**: Backup files don't automatically sync to the host machine

## Current Backup Storage Locations

```bash
# Inside Minikube VM (ephemeral)
/home/jianjun/ats-data/backups/ats-intg/
├── ats-intg-backup-20250823-193636.sql.custom
├── ats-intg-backup-20250823-193636.sql.gz
├── ats-intg-backup-20250823-194327.sql.custom
└── ats-intg-backup-20250823-194327.sql.gz

/home/jianjun/ats-data/backups/ats-prod/
├── ats-prod-backup-20250823-193641.sql.custom
└── ats-prod-backup-20250823-193641.sql.gz

# Host machine (empty)
/home/jianjun/ats-data/backups/ats-intg/ -> empty
/home/jianjun/ats-data/backups/ats-prod/ -> empty
```

## Solutions

### Option 1: Manual Backup Sync (Current)
Use the backup sync script when needed:

```bash
# Run manual sync before Minikube restart
./scripts/backup_sync.sh

# After Minikube restart, copy files back
minikube cp <host-backup-file> minikube:/path/to/restore/
```

### Option 2: Production Kubernetes Cluster (Recommended)
In a production environment:
- Use **real persistent volumes** that survive node restarts
- Use **network-attached storage** (NAS, EBS, GCP Persistent Disks)
- Configure **off-cluster backup storage** (S3, GCS, Azure Blob)

### Option 3: Enhanced Minikube Setup
Configure Minikube with persistent storage:

```bash
# Start Minikube with mounted volume
minikube start --mount --mount-string="/home/jianjun/ats-data:/host-data"

# Update backup jobs to use mounted path
volumes:
- name: backup-storage
  hostPath:
    path: /host-data/backups/ats-intg
    type: DirectoryOrCreate
```

## Verification Commands

### Check Backup Files in Minikube VM
```bash
minikube ssh "ls -la /home/jianjun/ats-data/backups/ats-intg/"
minikube ssh "ls -la /home/jianjun/ats-data/backups/ats-prod/"
```

### Check Host Machine
```bash
ls -la /home/jianjun/ats-data/backups/ats-intg/
ls -la /home/jianjun/ats-data/backups/ats-prod/
```

### Test Backup Job
```bash
kubectl create job --from=cronjob/ats-intg-database-backup test-backup -n ats-intg
kubectl logs job/test-backup -n ats-intg
```

## Current Status

- ✅ **Backup creation**: Working correctly
- ✅ **Database restore**: Working correctly
- ✅ **Slack notifications**: Working with environment-specific channels
- ⚠️ **Backup persistence**: Files lost on Minikube restart
- ✅ **Database persistence**: PostgreSQL data survives restarts

## Recommendations

1. **For Development/Testing**: Current setup is acceptable with manual sync when needed
2. **For Production**: Deploy to real Kubernetes cluster with persistent network storage
3. **For Critical Data**: Implement additional off-cluster backup strategy (cloud storage)

## Impact Assessment

- **Low Risk**: Development and testing can continue normally
- **Medium Risk**: Need to manually sync backups before Minikube maintenance
- **High Risk**: In production, this would be unacceptable - need proper persistent storage

The backup system is **functionally complete** and **operationally ready** - the only limitation is the Minikube environment's ephemeral storage nature.