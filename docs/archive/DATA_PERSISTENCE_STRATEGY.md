# Data Persistence Strategy for ATS-GenAI

## Problem Statement

Currently, when minikube is restarted, we lose all data including:
- Database content (universes, instruments, price data)
- Universe memberships and configurations  
- Job outputs and logs
- Persistent volume claims

## Persistence Strategy Options

### Option 1: External Database (RECOMMENDED)
✅ **Use the existing external TimescaleDB container**

**Benefits:**
- Data survives minikube restarts
- Better performance (no k8s networking overhead)
- Easier backup and maintenance
- Real database with proper persistence

**Implementation:**
```yaml
# Connect k8s jobs to external database
env:
- name: DB_HOST
  value: "172.19.0.2"  # External TimescaleDB IP
- name: DB_PORT  
  value: "5432"
- name: DB_USER
  value: "postgres"
- name: DB_PASSWORD
  value: "dev_password"  # Or correct external password
```

### Option 2: Persistent Volumes with Host Paths
⚠️ **Mount host directories for critical data**

**Benefits:**
- Data survives minikube restarts
- Can backup to host filesystem
- Good for development

**Implementation:**
```yaml
apiVersion: v1
kind: PersistentVolume
metadata:
  name: postgres-pv
spec:
  capacity:
    storage: 20Gi
  accessModes:
    - ReadWriteOnce
  hostPath:
    path: /home/jianjun/ats-data/postgres  # Host directory
  persistentVolumeReclaimPolicy: Retain
```

### Option 3: Backup/Restore Automation
🔄 **Automated backup before shutdown, restore on startup**

**Benefits:**
- Works with any storage
- Can version backups
- Disaster recovery ready

**Implementation:**
- Pre-shutdown hooks to backup
- Init containers to restore on startup
- Scheduled backups to external storage

## Recommended Hybrid Approach

### Phase 1: External Database Connection
```bash
# 1. Use existing external TimescaleDB (172.19.0.2:5432)
# 2. All k8s jobs connect to external DB
# 3. Data persists through minikube restarts
```

### Phase 2: Persistent Volume Claims for Outputs
```yaml
# Store job outputs, logs, and reports in PVCs
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: ats-outputs-pvc
spec:
  accessModes:
    - ReadWriteMany
  resources:
    requests:
      storage: 10Gi
  storageClassName: standard
```

### Phase 3: Automated Backup System
```yaml
# Daily backup job that survives restarts
apiVersion: batch/v1
kind: CronJob
metadata:
  name: ats-backup-job
spec:
  schedule: "0 2 * * *"  # Daily 2 AM
  jobTemplate:
    spec:
      template:
        spec:
          containers:
          - name: backup
            image: postgres:16
            command: ["/bin/bash", "-c"]
            args:
            - |
              # Backup to host-mounted volume
              pg_dump -h 172.19.0.2 -U postgres dev_db > /backup/$(date +%Y%m%d).sql
            volumeMounts:
            - name: backup-volume
              mountPath: /backup
          volumes:
          - name: backup-volume
            hostPath:
              path: /home/jianjun/ats-backups
```

## Implementation Plan

### Immediate Actions (Today)

1. **Fix External Database Connection**
   ```bash
   # Test direct connection from minikube to external DB
   kubectl run test-db --image=postgres:16 --rm -it -- psql -h 172.19.0.2 -U postgres -d dev_db
   ```

2. **Update Universe Creation Jobs**
   ```yaml
   env:
   - name: DB_HOST
     value: "172.19.0.2"  # External TimescaleDB
   - name: DB_PORT
     value: "5432"
   ```

3. **Create Host-Path Storage for Reports**
   ```bash
   mkdir -p /home/jianjun/ats-data/{outputs,backups,logs}
   ```

### Short-term (This Week)

1. **Set up automated backups**
2. **Create restore procedures**  
3. **Test full minikube restart cycle**
4. **Document data recovery procedures**

### Long-term (Next Month)

1. **Move to external Kubernetes cluster**
2. **Implement cloud storage for backups**
3. **Set up monitoring for data integrity**
4. **Create disaster recovery playbook**

## Data Directories Structure

```
/home/jianjun/ats-data/
├── postgres/           # Database files (if using internal DB)
├── outputs/            # Universe reports and analysis  
├── backups/            # Daily database backups
├── logs/               # Application logs
└── config/             # Configuration files
```

## Connection Patterns

### External Database Pattern (RECOMMENDED)
```python
# For all Python jobs
DB_URL = "postgresql://postgres:dev_password@172.19.0.2:5432/dev_db"

# Kubernetes environment
env:
- name: DB_HOST
  value: "172.19.0.2"
- name: DB_PASSWORD  
  value: "dev_password"
```

### Persistent Volume Pattern
```yaml
volumeMounts:
- name: ats-data
  mountPath: /ats-data
volumes:
- name: ats-data
  hostPath:
    path: /home/jianjun/ats-data
    type: DirectoryOrCreate
```

## Testing Strategy

### Pre-Restart Test
```bash
# 1. Create universe
# 2. Verify data in external DB
# 3. Create backup
# 4. Restart minikube
# 5. Verify data still exists
# 6. Redeploy jobs
# 7. Verify functionality
```

### Recovery Test
```bash
# 1. Simulate data loss
# 2. Restore from backup
# 3. Verify all universes restored
# 4. Test job functionality
```

## Benefits of This Approach

✅ **Data Survives Minikube Restarts**
- External DB keeps all universe data
- Host paths preserve outputs and backups

✅ **Development Friendly**
- Fast iteration without data loss
- Easy debugging with persistent logs

✅ **Production Ready**
- Same patterns work in real clusters
- Backup/restore procedures tested

✅ **Disaster Recovery**
- Multiple backup strategies
- Clear recovery procedures

## Next Steps

1. **Implement external DB connection** for current $10M universe job
2. **Create host-path volumes** for outputs  
3. **Set up automated backups** to host filesystem
4. **Test full restart cycle** to validate persistence
5. **Document procedures** for team use

This strategy ensures we never lose universe data or analysis results again!