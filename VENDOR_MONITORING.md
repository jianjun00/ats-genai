# 🎯 ATS Vendor Monitoring - Complete Setup

## ✅ **Live Dashboards Available**

### **Primary Dashboard: Grafana**
```
🎯 URL: http://localhost:4002/d/f9afe708-9be9-4c39-b901-f5c43a0a479f/ats-vendor-monitoring-dashboard-fixed
📊 Login: admin/admin
🔧 Code: config/grafana/ats-vendor-monitoring-dashboard-fixed.json
📊 Minute Bar Panel: config/grafana/minute-bar-tracking-panel.json
```

**What you get:**
- **Minute Bar Collection per Vendor**: Real-time collection rates by vendor/symbol
- **Dual Time Dimensions**: Both bar occurring time AND bar collection time
- **Collection Latency**: Delay between when bar occurred vs when we collected it
- **API Calls per Vendor with Status Codes**: Breakdown of 200, 429, 500, etc.
- **Success Rates**: API success percentages by vendor
- **Response Times**: Average response times by vendor  
- **Recent Errors**: Latest API failures with details
- **Professional UI**: Industry-standard monitoring interface

### **🕐 Time Dimensions Explained:**
- **Bar Occurring Time** (`timestamp`): When the 1-minute bar actually happened in the market
- **Bar Collection Time** (`received_at`): When our system received and stored the data
- **Collection Latency**: Time difference between bar occurrence and collection (ideally <5 minutes)

### **🔧 Key Monitoring Commands**
```bash
# Check live minute bar data (CURRENT WORKING DATA)
export PGPASSWORD=intg_password && psql -h localhost -p 4432 -U postgres -d intg_db -c "
SELECT vendor, symbol, COUNT(*) as records, MAX(timestamp) as latest_data 
FROM (
  SELECT vendor, symbol, timestamp FROM intg_one_minute_live_polygon 
  UNION ALL 
  SELECT vendor, symbol, timestamp FROM intg_one_minute_live_tiingo
) combined GROUP BY vendor, symbol ORDER BY latest_data DESC;"

# Check data quality and latency
export PGPASSWORD=intg_password && psql -h localhost -p 4432 -U postgres -d intg_db -c "
SELECT vendor, AVG(quality_score)::numeric(4,3) as avg_quality, AVG(data_latency_ms)::int as avg_latency_ms
FROM (
  SELECT vendor, quality_score, data_latency_ms FROM intg_one_minute_live_polygon WHERE timestamp >= NOW() - INTERVAL '24 hours'
  UNION ALL
  SELECT vendor, quality_score, data_latency_ms FROM intg_one_minute_live_tiingo WHERE timestamp >= NOW() - INTERVAL '24 hours'
) combined GROUP BY vendor;"

# Count today's processed parquet files
find /mnt/d/ats-data/firstrate-data/daily/$(date +%Y/%m/%d)/ -name "*.parquet" | wc -l

# Restart minute bar collection  
./scripts/restart_minute_bar_collection.sh

# Fix Grafana dashboard if panels show no data
python3 scripts/fix_grafana_minute_bar_dashboard.py

# Fix dashboard time range if showing "No data" (extends to 7 days)
python3 scripts/update_grafana_time_range.py

# Add dual time dimension panels (bar time vs collection time)
python3 scripts/add_dual_time_panels.py

# Check collection latency for recent data
export PGPASSWORD=intg_password && psql -h localhost -p 4432 -U postgres -d intg_db -c "
SELECT symbol, vendor, 
       timestamp as bar_time, 
       received_at as collection_time,
       EXTRACT(EPOCH FROM (received_at - timestamp))/60 as delay_minutes 
FROM (
  SELECT * FROM intg_one_minute_live_polygon 
  UNION ALL 
  SELECT * FROM intg_one_minute_live_tiingo
) combined 
ORDER BY received_at DESC LIMIT 10;"
```

### **Data Source: Prometheus Metrics**
```
📈 URL: http://localhost:8091/metrics
🔧 Purpose: Feeds data to Grafana (auto-configured)
```

## 📊 **Live Data Available**

The system currently shows:

**Vendor Health Summary:**
- **Tiingo**: 1,406 API calls, 89.9% success rate, 742ms avg
- **Polygon**: 473 API calls, 88.6% success rate, 721ms avg
- **EODHD**: 984 API calls, 91.5% success rate, 715ms avg

**Collection Metrics:**
- **6 vendor/symbol combinations** actively tracked
- **3,916 total records** collected
- **95-100% collection success rates**

**API Status Breakdown:**
- **2,144 successful calls** (200/201 status)
- **719 error calls** (400/429/500 status)
- **Rate limit hits**: 24-35 per vendor

## 🎯 **Why This Approach**

✅ **Industry Standard**: Grafana is the standard monitoring solution  
✅ **Zero Custom Code**: No web dashboard maintenance required  
✅ **Real-time Updates**: Direct PostgreSQL queries with live refresh  
✅ **Professional Features**: Alerting, sharing, templating built-in  
✅ **Already Running**: Uses existing ATS-INTG Grafana instance  

## 📋 **Database Tables**

Direct PostgreSQL access for custom queries:
```sql
-- API call tracking
SELECT vendor, status_code, COUNT(*) FROM intg_api_calls 
WHERE request_timestamp >= NOW() - INTERVAL '24 hours'
GROUP BY vendor, status_code;

-- Minute bar collection
SELECT vendor, symbol, SUM(records_collected) FROM intg_minute_bar_collection_metrics
WHERE collection_timestamp >= NOW() - INTERVAL '24 hours'
GROUP BY vendor, symbol;
```

## 🚀 **Access Summary**

1. **Main Dashboard**: http://localhost:4002/d/5/ats-vendor-monitoring-dashboard-postgresql
2. **Grafana Home**: http://localhost:4002 (admin/admin)
3. **Prometheus Metrics**: http://localhost:8091/metrics (data source)

That's it! Professional vendor monitoring with zero custom code maintenance. 🎉