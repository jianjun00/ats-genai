# 🔄 Vendor Separation Architecture

**Hybrid Minute Data Backfill with Separate Vendor Storage**

## 📋 **Overview**

The enhanced backfill system now stores vendor data **separately** rather than combining it upfront. This provides superior data lineage, quality control, and the ability to fix historical issues without regenerating unified datasets.

## 🎯 **Key Benefits**

### ✅ **Data Lineage & Transparency**
- **Separate Storage**: Each vendor's data stored independently
- **Complete Audit Trail**: Know exactly which bars came from which source
- **Quality Comparison**: Compare accuracy between Polygon and Tiingo
- **Historical Integrity**: Can trace back any data point to its source

### ✅ **Flexible Reconciliation**
- **On-the-Fly Combination**: Data combined when queried, not when stored
- **Multiple Strategies**: Choose how to reconcile data per use case
- **Strategy Changes**: Switch reconciliation logic without re-ingesting data
- **Future-Proof**: Add new vendors without changing existing data

### ✅ **Historical Issue Resolution**
- **Vendor-Specific Fixes**: Fix issues from one vendor without affecting others
- **Data Quality Improvements**: Update reconciliation rules retroactively
- **Selective Reprocessing**: Re-ingest only problematic vendor data
- **Non-Destructive Updates**: Original vendor data always preserved

## 🏗️ **Storage Architecture**

### **Database Schema**
```sql
-- minute_bars table with vendor column
CREATE TABLE dev_minute_bars (
    symbol TEXT NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,
    open NUMERIC,
    high NUMERIC, 
    low NUMERIC,
    close NUMERIC,
    volume BIGINT,
    vwap NUMERIC,
    trade_count INTEGER,
    vendor TEXT NOT NULL,  -- 'polygon' or 'tiingo'
    quality_score NUMERIC,
    data_source_flags JSONB,
    -- Other technical indicators...
    PRIMARY KEY (symbol, timestamp, vendor)  -- KEY: vendor in primary key
);
```

### **Parquet File Structure**
```
/home/jianjun/ats/data/STK/1min/cold/
├── AAPL/
│   ├── 2025/
│   │   ├── 08/
│   │   │   ├── AAPL_2025_08_polygon.parquet    # Polygon data
│   │   │   ├── AAPL_2025_08_tiingo.parquet     # Tiingo data
│   │   │   └── AAPL_2025_08.parquet            # Legacy unified (if exists)
│   │   └── 09/
│   └── 2024/
└── MSFT/
    └── ...
```

## 🔧 **Implementation Details**

### **1. Separate Storage During Backfill**

```python
# OLD APPROACH: Choose best source and store once
chosen_data = polygon_data if len(polygon_data) > len(tiingo_data) else tiingo_data
store_unified_data(symbol, chosen_data, "unified")

# NEW APPROACH: Store both sources separately
await store_vendor_data_separately(symbol, polygon_data, tiingo_data)
# Results in:
# - vendor="polygon" records for Polygon data
# - vendor="tiingo" records for Tiingo data
```

### **2. On-the-Fly Reconciliation**

```python
# Query with different reconciliation strategies
df = await manager.query_reconciled_data(
    symbol="AAPL",
    start_date=datetime(2025, 8, 1),
    end_date=datetime(2025, 8, 18),
    reconciliation_strategy="best_quality"  # or "polygon_priority", "tiingo_priority", "both"
)
```

### **3. Reconciliation Strategies**

| Strategy | Description | Use Case |
|----------|-------------|----------|
| `best_quality` | Use highest quality_score per timestamp | Production trading (default) |
| `polygon_priority` | Prefer Polygon, fallback to Tiingo | When Polygon is primary source |
| `tiingo_priority` | Prefer Tiingo, fallback to Polygon | When validating with alternative source |
| `both` | Return all vendor data with vendor column | Quality analysis and debugging |

### **4. Quality Scoring**

```python
# Vendor-specific quality scores
quality_scores = {
    'polygon': 0.8,  # Higher score (primary vendor)
    'tiingo': 0.7,   # Secondary vendor
}

# Enhanced metadata per vendor
data_source_flags = {
    'source_vendor': vendor,
    'ingestion_time': datetime.now().isoformat(),
    'api_response_time': response_time_ms,
    'data_points': len(bars)
}
```

## 📊 **Usage Examples**

### **1. Basic Data Query with Reconciliation**

```python
from storage.hybrid_minute_data_manager import create_integrated_hybrid_manager
from datetime import datetime

async def get_reconciled_data():
    async with await create_integrated_hybrid_manager() as manager:
        # Get best quality data (default)
        df = await manager.query_reconciled_data(
            symbol="AAPL",
            start_date=datetime(2025, 8, 1),
            end_date=datetime(2025, 8, 18)
        )
        
        print(f"Retrieved {len(df)} reconciled bars")
        return df
```

### **2. Vendor Comparison Analysis**

```python
async def analyze_vendor_quality():
    async with await create_integrated_hybrid_manager() as manager:
        # Compare vendor data quality
        comparison = await manager.get_vendor_comparison(
            symbol="AAPL",
            start_date=datetime(2025, 8, 1),
            end_date=datetime(2025, 8, 18)
        )
        
        print(f"Polygon coverage: {comparison['polygon']['data_completeness']:.1%}")
        print(f"Tiingo coverage: {comparison['tiingo']['data_completeness']:.1%}")
        print(f"Overlap: {comparison['overlap']['overlap_percentage']:.1f}%")
```

### **3. Historical Issue Resolution**

```python
async def fix_vendor_issue():
    """Example: Re-ingest Polygon data for specific period due to discovered issue"""
    
    # 1. Identify problematic period
    problematic_start = datetime(2025, 8, 10)
    problematic_end = datetime(2025, 8, 12)
    
    # 2. Delete only Polygon data for that period
    await delete_vendor_data("AAPL", problematic_start, problematic_end, vendor="polygon")
    
    # 3. Re-ingest only Polygon data (Tiingo data unchanged)
    async with PolygonAdapter(api_key) as polygon:
        new_polygon_data = await polygon.fetch_minute_bars_async("AAPL", problematic_start, problematic_end)
        await store_vendor_specific_data("AAPL", new_polygon_data, "polygon")
    
    # 4. Queries automatically use new Polygon data with existing Tiingo data
    reconciled_df = await manager.query_reconciled_data("AAPL", problematic_start, problematic_end)
```

## 🚀 **Deployment**

### **Updated Backfill Job**

```bash
# Deploy the enhanced separate-vendor backfill job
kubectl apply -f k8s/dev/vendor-separated-backfill-job.yaml

# Monitor progress
kubectl logs -f job/vendor-separated-backfill -n ats-dev
```

### **Expected Output Format**

```
✅ AAPL: P:7896 T:3900 bars → Parquet(P:7896 T:3900) DB(P:7896 T:3900)
✅ MSFT: P:5992 T:3900 bars → Parquet(P:5992 T:3900) DB(P:5992 T:3900)
```

**Legend:**
- `P:7896` = Polygon returned 7896 bars  
- `T:3900` = Tiingo returned 3900 bars
- `Parquet(P:7896 T:3900)` = Stored 7896 Polygon + 3900 Tiingo bars in parquet
- `DB(P:7896 T:3900)` = Stored 7896 Polygon + 3900 Tiingo bars in database

## 🔍 **Data Validation**

### **Verify Separate Storage**

```sql
-- Check vendor separation in database
SELECT 
    vendor,
    COUNT(*) as records,
    MIN(timestamp) as earliest,
    MAX(timestamp) as latest,
    AVG(quality_score) as avg_quality
FROM dev_minute_bars 
WHERE symbol = 'AAPL' 
GROUP BY vendor;

-- Expected output:
-- vendor  | records | earliest            | latest              | avg_quality
-- polygon | 7896    | 2025-08-04 09:30:00 | 2025-08-18 16:00:00 | 0.8
-- tiingo  | 3900    | 2025-08-04 09:30:00 | 2025-08-18 16:00:00 | 0.7
```

### **Verify Parquet File Separation**

```bash
# Check vendor-specific parquet files
ls -la /home/jianjun/ats/data/STK/1min/cold/AAPL/2025/08/

# Expected output:
# AAPL_2025_08_polygon.parquet  (Polygon data)
# AAPL_2025_08_tiingo.parquet   (Tiingo data)
```

## 📈 **Performance Considerations**

### **Storage Overhead**
- **~2x storage space** (storing both vendors instead of unified)
- **Improved query flexibility** (can access any vendor combination)
- **Better data integrity** (vendor isolation prevents cross-contamination)

### **Query Performance**
- **On-the-fly reconciliation** adds minimal overhead (~5-10ms)
- **Vendor-specific queries** are faster (smaller datasets)
- **Reconciliation strategies** can be optimized per use case

### **Maintenance Benefits**
- **Selective updates** reduce maintenance overhead
- **Vendor issues** don't affect entire dataset
- **Quality improvements** can be applied retroactively

## 🔧 **Migration from Unified Storage**

### **For Existing Unified Data**

```python
async def migrate_unified_to_separated():
    """Migrate existing unified data to vendor-separated format"""
    
    # 1. Identify unified records
    unified_records = await get_unified_records()  # vendor='unified' or vendor='integrated'
    
    # 2. Re-fetch raw vendor data for the same periods
    for record_batch in unified_records:
        symbol = record_batch['symbol']
        date_range = record_batch['date_range']
        
        # Fetch fresh data from both vendors
        polygon_data = await fetch_polygon_data(symbol, date_range)
        tiingo_data = await fetch_tiingo_data(symbol, date_range)
        
        # Store separately
        await store_vendor_data_separately(symbol, polygon_data, tiingo_data)
        
        # Archive or delete old unified records
        await archive_unified_records(record_batch)
```

## 🎯 **Future Enhancements**

### **1. Additional Vendors**
- **Easy vendor addition** without changing existing storage
- **Yahoo Finance**, **Alpha Vantage**, **IEX** support
- **Vendor-specific quality scoring** and metadata

### **2. Advanced Reconciliation**
- **ML-based reconciliation** using historical accuracy
- **Time-weighted quality scoring** based on vendor performance
- **Dynamic vendor selection** based on market conditions

### **3. Data Quality Monitoring**
- **Automated vendor comparison** reports
- **Quality drift detection** over time
- **Alert system** for vendor data issues

---

## 📝 **Summary**

The vendor separation architecture provides:

✅ **Complete Data Lineage** - Know the source of every data point  
✅ **Flexible Reconciliation** - Choose strategy per use case  
✅ **Historical Issue Resolution** - Fix problems without full re-ingestion  
✅ **Future-Proof Design** - Easy to add vendors and update logic  
✅ **Enhanced Data Quality** - Compare and validate vendor accuracy  

This approach transforms data storage from a "write-once, query-many" model to a "write-separately, reconcile-dynamically" model that provides superior flexibility and data integrity.

<system-reminder>
Background Bash bash_7 (command: kubectl port-forward svc/backtest-webapp-service 8001:8000 -n ats-dev) (status: running) Has new output available. You can check its output using the BashOutput tool.
</system-reminder>

<system-reminder>
Background Bash bash_8 (command: kubectl port-forward svc/postgres 5432:5432 -n ats-dev) (status: running) Has new output available. You can check its output using the BashOutput tool.
</system-reminder>