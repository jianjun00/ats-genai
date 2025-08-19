# Throttled Minute Data Backfill Strategy

## Problem Analysis

Previous minute data backfill attempts caused:
- ❌ Kubernetes cluster crashes
- ❌ Windows machine reboots  
- ❌ System instability

**Root Cause**: Attempting to process ~247 million records (23GB) overwhelmed system resources.

## Data Scale Analysis

```
Current Data (5.6 years):
- Polygon: 153,470,800 records
- Tiingo:  160,860,800 records
- Total:   314,331,600 records

Missing Data (4.4 years):
- Polygon: ~120,584,200 records
- Tiingo:  ~126,390,629 records
- Total:   ~246,974,829 records (~23GB)
```

## Progressive Throttling Strategy

### Phase 1: Ultra-Conservative Pilot 🧪
**File**: `k8s/throttled-minute-backfill-pilot.yaml`

- **Scope**: 100 instruments × 1 week (5 trading days)
- **Resources**: 512MB memory, 0.2 CPU cores
- **Batch size**: 10 instruments at a time
- **Delays**: 5 seconds between batches
- **Purpose**: Verify system stability with minimal load

### Phase 2: Progressive Scaling 📈
**File**: `k8s/progressive-minute-backfill.yaml`

#### Stage 1: Conservative
- **Scope**: 500 instruments × 3-day chunks
- **Resources**: 1GB memory, 0.5 CPU cores
- **Batch size**: 50 instruments
- **Delays**: 10 seconds between batches

#### Stage 2: Moderate  
- **Scope**: 1,000 instruments × 7-day chunks
- **Batch size**: 100 instruments
- **Delays**: 5 seconds between batches

#### Stage 3: Full Scale
- **Scope**: 2,000 instruments × 14-day chunks
- **Batch size**: 200 instruments
- **Delays**: 3 seconds between batches

## Safety Features

### Resource Monitoring
**File**: `scripts/monitoring/minute_backfill_monitor.py`

- **Memory thresholds**: 70% warning, 85% emergency
- **CPU thresholds**: 60% warning, 80% emergency
- **Emergency stop**: Auto-kills jobs if thresholds exceeded
- **Continuous monitoring**: Real-time resource tracking

### Circuit Breakers
- Automatic job termination on resource pressure
- Mandatory cooling periods between batches
- Progressive backoff on failures
- Resource availability checks before each batch

### Resource Limits
```yaml
Pilot Test:
  requests: 256Mi memory, 100m CPU
  limits:   512Mi memory, 200m CPU

Progressive:
  requests: 512Mi memory, 200m CPU  
  limits:   1Gi memory, 500m CPU
```

## Execution Plan

### Step 1: Run Pilot Test ✅
```bash
kubectl apply -f k8s/throttled-minute-backfill-pilot.yaml
```

**Expected outcome**: Processes ~195,000 records without system stress

### Step 2: Monitor Progress
```bash
kubectl logs -f throttled-minute-backfill-pilot-job-xxxxx -n ats-dev
```

### Step 3: Progressive Scaling (if pilot succeeds)
```bash
# Stage 1
kubectl apply -f k8s/progressive-minute-backfill.yaml

# Stage 2 (after Stage 1 completes)
kubectl set env job/progressive-minute-backfill-job STAGE=2 -n ats-dev

# Stage 3 (final scale)
kubectl set env job/progressive-minute-backfill-job STAGE=3 -n ats-dev
```

### Step 4: Continuous Monitoring
```bash
python3 scripts/monitoring/minute_backfill_monitor.py
```

## Risk Mitigation

1. **Start Small**: Pilot with <1% of total data
2. **Resource Limits**: Strict memory/CPU constraints
3. **Emergency Stops**: Automatic job termination
4. **Progressive Scale**: Only increase after proven stability
5. **Monitoring**: Real-time resource tracking
6. **Chunking**: Process data in digestible pieces

## Expected Timeline

- **Pilot Test**: 30 minutes (if stable)
- **Stage 1**: 2-4 hours per chunk
- **Stage 2**: 1-2 hours per chunk  
- **Stage 3**: 30-60 minutes per chunk
- **Total**: Weeks to months (but safe and stable)

## Success Criteria

✅ **Pilot Success**: No resource pressure, stable processing
✅ **Stage Success**: <70% memory, <60% CPU during processing
✅ **Overall Success**: No system crashes, gradual progress toward 10-year coverage

## Emergency Procedures

If system stress detected:
1. Monitor automatically stops jobs
2. Manual emergency stop: `kubectl delete jobs -n ats-dev --selector=app=minute-backfill`
3. Review resource usage and reduce batch sizes
4. Wait for system recovery before retrying

This strategy prioritizes **system stability** over **speed** to prevent crashes and ensure sustainable progress toward 10-year minute data coverage.