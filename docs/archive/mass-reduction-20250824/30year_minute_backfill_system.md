# 30-Year Comprehensive Minute Data Backfill System

A production-ready system for populating 30 years (1995-2025) of 1-minute OHLCV data from all major market data vendors using checkpoint-based resumable processing.

## 🏗️ Architecture Overview

### System Components

1. **Comprehensive Orchestrator** (`scripts/backfill/comprehensive_30year_minute_backfill.py`)
   - Master orchestrator coordinating all vendors
   - Checkpoint-based resumable processing
   - Quality validation and gap detection
   - Progress monitoring and reporting

2. **Vendor-Specific Jobs** (Kubernetes deployments)
   - **Polygon**: Premium quality, 3-second rate limiting
   - **Tiingo**: IEX data source, 1-second rate limiting  
   - **FMP**: Financial Modeling Prep, 1.5-second rate limiting
   - **EODHD**: Conservative source, 3-second rate limiting

3. **Deployment Manager** (`scripts/backfill/deploy_30year_minute_backfill.py`)
   - Deploy and manage Kubernetes jobs
   - Monitor progress across all vendors
   - Checkpoint status tracking

## 📊 Scale and Estimates

### Data Volume Estimates
- **Total Estimated Bars**: ~30 billion per vendor (120 billion total)
- **Storage Requirement**: ~5.6 TB across all vendors
- **Processing Duration**: 60-90 days (parallel execution)
- **Target Instruments**: 10,000+ symbols with market cap > $100M

### Vendor-Specific Estimates
| Vendor | Priority | Duration | Rate Limit | Batch Size | Concurrency |
|--------|----------|----------|------------|------------|-------------|
| Polygon | 1 (Highest) | ~45 days | 3.0s | 25 symbols | 5 concurrent |
| Tiingo | 2 (High) | ~30 days | 1.0s | 50 symbols | 8 concurrent |
| FMP | 3 (Medium) | ~35 days | 1.5s | 40 symbols | 6 concurrent |
| EODHD | 4 (Lowest) | ~60 days | 3.0s | 20 symbols | 1 concurrent |

## 🚀 Quick Start

### Deploy All Vendors
```bash
# Deploy comprehensive orchestrator + all vendor jobs
python scripts/backfill/deploy_30year_minute_backfill.py --deploy all

# Monitor progress with live updates  
python scripts/backfill/deploy_30year_minute_backfill.py --monitor

# Check status
python scripts/backfill/deploy_30year_minute_backfill.py --status
```

### Deploy Individual Vendors
```bash
# Deploy high-priority vendors first
python scripts/backfill/deploy_30year_minute_backfill.py --deploy polygon
python scripts/backfill/deploy_30year_minute_backfill.py --deploy tiingo

# Deploy backup vendors
python scripts/backfill/deploy_30year_minute_backfill.py --deploy fmp  
python scripts/backfill/deploy_30year_minute_backfill.py --deploy eodhd
```

### Monitor and Manage
```bash
# View job logs
python scripts/backfill/deploy_30year_minute_backfill.py --logs polygon

# Check checkpoint status
python scripts/backfill/deploy_30year_minute_backfill.py --checkpoints

# Clean up completed jobs
python scripts/backfill/deploy_30year_minute_backfill.py --cleanup --keep-completed
```

## 📂 File Structure

```
├── scripts/backfill/
│   ├── comprehensive_30year_minute_backfill.py    # Main orchestrator
│   └── deploy_30year_minute_backfill.py           # Deployment manager
├── k8s/
│   ├── 30year-minute-backfill-orchestrator.yaml  # Master orchestrator job
│   ├── 30year-minute-backfill-polygon.yaml       # Polygon-specific job
│   ├── 30year-minute-backfill-tiingo.yaml        # Tiingo-specific job  
│   ├── 30year-minute-backfill-fmp.yaml           # FMP-specific job
│   └── 30year-minute-backfill-eodhd.yaml         # EODHD-specific job
└── /home/jianjun/ats-data/
    ├── minute-files/                              # Monthly Parquet storage
    │   ├── AAPL/2025/08/AAPL_2025_08.parquet
    │   └── MSFT/2025/08/MSFT_2025_08.parquet
    └── checkpoints/                               # Checkpoint files
        ├── master/
        ├── polygon/
        ├── tiingo/
        ├── fmp/
        └── eodhd/
```

## 🔄 Checkpoint Framework

### Automatic Checkpointing
- **Frequency**: Every 10-20 minutes (vendor-specific)
- **Granularity**: Symbol-level progress tracking
- **Resume Logic**: Automatic detection and continuation
- **Storage**: JSON checkpoint files with metadata

### Checkpoint Structure
```json
{
  "config": {
    "start_date": "1995-01-01",
    "end_date": "2025-08-31", 
    "enabled_vendors": ["polygon"],
    "storage_type": "file"
  },
  "progress": {
    "start_time": "2025-08-23T12:00:00Z",
    "last_checkpoint": "2025-08-23T16:35:00Z",
    "total_jobs": 50000,
    "jobs_completed": 12500,
    "total_bars_fetched": 125000000,
    "symbols_per_hour": 150.5,
    "estimated_completion": "2025-10-15T10:30:00Z"
  },
  "jobs": {
    "polygon_AAPL_1995_2000": {
      "symbol": "AAPL",
      "vendor": "polygon", 
      "status": "completed",
      "bars_fetched": 2635200,
      "quality_score": 0.95
    }
  }
}
```

## 💾 Storage Architecture

### File-Based Storage (Default)
- **Format**: Monthly Parquet files
- **Path**: `/data/minute-files/{SYMBOL}/{YEAR}/{MONTH}/{SYMBOL}_{YEAR}_{MONTH}.parquet`
- **Deduplication**: Automatic overlap detection and merging
- **Compression**: Snappy compression for optimal performance

### Database Storage (Optional)
- **Table**: `minute_bars` with TimescaleDB optimization
- **Conflict Resolution**: ON CONFLICT DO UPDATE for deduplication
- **Indexing**: Optimized for time-series queries

## 🔍 Quality Validation

### Data Quality Metrics
- **Gap Detection**: Identify missing time periods
- **Price Outlier Detection**: Flag extreme price movements (>15%)
- **Volume Consistency**: Validate volume data ranges
- **Completeness Score**: Percentage of expected data received

### Quality Thresholds
- **Minimum Quality Score**: 0.7 (configurable)
- **Gap Tolerance**: 15% missing data allowed
- **Retry Logic**: Up to 5 retries per symbol
- **Failure Threshold**: 20% symbol failure tolerance

## 🚦 Rate Limiting

### Vendor-Specific Limits
- **Polygon**: 3.0s between calls (conservative for free tier)
- **Tiingo**: 1.0s between calls (good throughput)
- **FMP**: 1.5s between calls (balanced approach)
- **EODHD**: 3.0s between calls (most conservative)

### Concurrency Controls
- **Per-Vendor Semaphores**: Limit concurrent symbol processing
- **Global Resource Management**: Prevent system overload
- **Adaptive Backoff**: Exponential backoff on rate limit errors

## 🔐 Security

### API Key Management
- **Kubernetes Secrets**: `market-data-api-keys`
- **Environment Variables**: Optional fallback
- **Secure Storage**: Base64 encoded in cluster secrets

### Secret Creation Example
```bash
kubectl create secret generic market-data-api-keys \
  --from-literal=polygon_api_key='YOUR_POLYGON_KEY_HERE' \
  --from-literal=tiingo_api_key='YOUR_TIINGO_KEY_HERE' \
  --from-literal=fmp_api_key='YOUR_FMP_KEY_HERE' \
  --from-literal=eodhd_api_key='YOUR_EODHD_KEY_HERE' \
  -n ats-dev
```

## 📈 Monitoring

### Progress Tracking
- **Real-time Status**: Live job monitoring
- **Progress Estimates**: ETA calculations based on current throughput
- **Resource Usage**: Memory and CPU tracking
- **Error Monitoring**: Failed symbol tracking and alerts

### Key Metrics
- **Symbols per Hour**: Processing throughput
- **Bars per Second**: Data ingestion rate  
- **Success Rate**: Percentage of successful symbols
- **Quality Distribution**: Average quality scores by vendor

## 🛠️ Configuration

### Environment Variables
```bash
export POLYGON_API_KEY="YOUR_POLYGON_KEY_HERE"
export TIINGO_API_KEY="YOUR_TIINGO_KEY_HERE"
export FMP_API_KEY="YOUR_FMP_KEY_HERE"
export EODHD_API_KEY="YOUR_EODHD_KEY_HERE"
```

### Resource Requirements
```yaml
resources:
  requests:
    memory: "1-2Gi"
    cpu: "500-1000m"
  limits:
    memory: "4-8Gi" 
    cpu: "1000-2000m"
```

## 🔧 Troubleshooting

### Common Issues

1. **Rate Limiting Errors**
   - Increase rate limit delays in configuration
   - Reduce batch sizes and concurrency

2. **Memory Issues**
   - Increase memory limits in Kubernetes configs
   - Reduce chunk size or batch size

3. **Storage Space**
   - Monitor `/home/jianjun/ats-data/minute-files` usage
   - ~5.6TB required for full 30-year backfill

4. **Checkpoint Corruption**
   - Checkpoint files are in `/home/jianjun/ats-data/checkpoints/`
   - Backup checkpoint files before major operations

### Log Analysis
```bash
# View job logs
kubectl logs job/polygon-30year-minute-backfill -n ats-dev

# Monitor resource usage  
kubectl top pods -n ats-dev --selector=component=minute-backfill

# Check persistent volumes
kubectl get pv | grep minute-files
```

## 🎯 Production Deployment

### Prerequisites
1. ✅ API keys for all vendors
2. ✅ ~6TB available storage space
3. ✅ Kubernetes cluster with sufficient resources
4. ✅ Network egress for API calls

### Deployment Checklist
- [ ] Create API key secrets
- [ ] Verify storage paths exist and are writable
- [ ] Test with small symbol set first
- [ ] Monitor resource usage during deployment
- [ ] Set up alerting for failures
- [ ] Schedule regular checkpoint backups

### Success Metrics
- **Target**: 30 billion+ bars across all vendors
- **Quality**: >95% data completeness
- **Timeframe**: 60-90 days total duration
- **Storage**: <6TB total space utilization

---

## 📞 Support

For issues or questions:
1. Check job logs using the deployment manager
2. Verify checkpoint status and resume capability  
3. Monitor resource usage and adjust limits if needed
4. Review vendor-specific rate limiting and API quotas

The system is designed to be resilient and resumable - checkpoint files allow recovery from any interruption without data loss.