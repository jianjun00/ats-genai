# SignOZ Dashboard Import Instructions

## 🚀 Quick Import Guide

### **Step 1: Access SignOZ Dashboard UI**
```bash
# Open SignOZ in your browser
open http://localhost:8080
# OR
curl -I http://localhost:8080
```

### **Step 2: Navigate to Dashboard Import**
1. Go to **Dashboards** in the left sidebar
2. Click **"+ New Dashboard"** or **"Import"** button
3. Select **"Import from JSON"**

### **Step 3: Import the ATS Comprehensive Dashboard**
```bash
# Copy the dashboard JSON file
cat /home/jianjun/ats-genai-model/config/dashboards/ats-comprehensive-monitoring-dashboard.json | pbcopy

# OR copy file to clipboard for manual paste
cp /home/jianjun/ats-genai-model/config/dashboards/ats-comprehensive-monitoring-dashboard.json ~/Desktop/
```

1. Paste the JSON content into the import field
2. Click **"Load"** to validate the dashboard configuration
3. Review the dashboard settings and panels
4. Click **"Import"** to create the dashboard

### **Step 4: Verify Import Success**
- The dashboard should appear in your dashboards list as **"ATS Comprehensive Services Monitor"**
- All 14 panels should be visible and configured
- Template variables should be populated (environment, vendor, service)

## 🔧 Configuration & Setup

### **Required Metrics Sources**

**Ensure these metrics are being exported to your SignOZ instance:**

#### **Core Infrastructure Metrics:**
```bash
# Service health
up{job=~"ats-.*"}

# Database metrics
postgresql_connections_active
postgresql_connections_idle
postgresql_queries_per_second
postgresql_cache_hit_ratio
postgresql_cpu_percent
postgresql_memory_mb
postgresql_disk_usage_percent
postgresql_blocked_queries
postgresql_long_running_queries
```

#### **Business & Application Metrics:**
```bash
# Instruments and data
ats_instruments_total
ats_minute_bars_collected_total
ats_daily_prices_sync_prices_processed_total
ats_daily_prices_backfill_prices_collected_total

# API monitoring
ats_daily_prices_backfill_api_calls_total
ats_news_api_calls_total
api_errors_total

# Data quality
ats_price_coverage_percentage
ats_daily_prices_sync_success_rate
ats_daily_prices_backfill_success_rate

# Processing performance
ats_daily_prices_sync_duration_seconds_bucket
ats_daily_prices_backfill_duration_seconds_bucket
collection_duration_seconds_bucket
```

#### **Training & ML Metrics:**
```bash
# Training datasets
ats_training_datasets_created_total
ats_training_dataset_size_mb
ats_training_dataset_quality_score

# Symbol coverage
active_symbols
ats_daily_minute_backfill_symbols_by_type
ats_daily_minute_backfill_symbols_by_letter
```

### **Metrics Setup Verification**

**Check if metrics are available in SignOZ:**
```bash
# Test metrics endpoints
curl -s http://localhost:8080/api/v1/query?query=up | jq .
curl -s http://localhost:8080/api/v1/query?query=ats_instruments_total | jq .

# List all available ATS metrics
curl -s http://localhost:8080/api/v1/label/__name__/values | jq '.data[] | select(. | startswith("ats_"))'
```

**If metrics are missing, ensure monitoring services are running:**
```bash
# Check Prometheus exporters
python scripts/run_dev.py status
python scripts/run_intg.py status

# Verify metrics servers
curl -f http://localhost:4080/metrics | head -20
curl -f http://localhost:8001/metrics | head -20

# Start missing exporters
python src/infrastructure/monitoring/postgres_prometheus_exporter.py --port 8001
python scripts/prometheus_metrics_server.py --port 4080
```

## 🎛️ Dashboard Customization

### **Modify Time Ranges**
- **Default**: 1 hour with 30-second refresh
- **Recommended**:
  - Operations monitoring: 15 minutes - 4 hours
  - Capacity planning: 24 hours - 7 days
  - Trend analysis: 7 days - 30 days

### **Add Custom Panels**

**Example: Add Custom Business Metric**
```json
{
  "id": 15,
  "title": "🔥 Your Custom Metric",
  "type": "stat",
  "targets": [
    {
      "expr": "your_custom_metric",
      "legendFormat": "{{label}}",
      "refId": "A"
    }
  ],
  "gridPos": {"h": 6, "w": 8, "x": 0, "y": 76}
}
```

### **Configure Alerts**

**Set up alerts for critical metrics:**
```bash
# Navigate to: Alerts > Alert Rules > + New Rule

# Example alert conditions:
# Service Down Alert
up{job=~"ats-.*"} == 0

# High Error Rate Alert
rate(api_errors_total[5m]) > 0.1

# Low Data Coverage Alert
ats_price_coverage_percentage < 90

# Database Performance Alert
postgresql_cache_hit_ratio < 0.85
```

## 🔍 Troubleshooting

### **Common Import Issues**

**Issue 1: "Invalid JSON format"**
```bash
# Validate JSON syntax
cat /home/jianjun/ats-genai-model/config/dashboards/ats-comprehensive-monitoring-dashboard.json | jq .
```

**Issue 2: "Metrics not found"**
```bash
# Check if metrics are available
curl -s "http://localhost:8080/api/v1/query?query=ats_instruments_total" | jq .

# Verify SignOZ configuration
docker logs signoz-query-service --tail 50
```

**Issue 3: "Template variables not populating"**
```bash
# Check label values exist
curl -s "http://localhost:8080/api/v1/label/environment/values" | jq .
curl -s "http://localhost:8080/api/v1/label/vendor/values" | jq .
```

### **Panel Troubleshooting**

**Panels showing "No data":**
1. Verify metric names match your exported metrics
2. Check time range - some metrics may be sparse
3. Ensure label filters are not too restrictive
4. Test queries in SignOZ Query Builder

**Performance Issues:**
1. Reduce time range for heavy queries
2. Increase dashboard refresh interval
3. Use more specific label filters
4. Consider aggregating high-cardinality metrics

## 📊 Usage Best Practices

### **Daily Operations Workflow**
1. **Morning Health Check**:
   - Panel 1: Service status overview
   - Panel 12: Review alerts and issues
   - Panel 9: Data quality validation

2. **Performance Monitoring**:
   - Panel 6: Database performance
   - Panel 7: Resource utilization
   - Panel 11: Processing duration trends

3. **Business Metrics Review**:
   - Panel 2-4: Data volume validation
   - Panel 10: Vendor distribution analysis
   - Panel 13: ML pipeline status

### **Alert Response Procedures**
1. **Service Down (Panel 1, 12)**:
   - Check container status: `docker ps | grep ats-`
   - Review service logs: `docker logs <container-name> --tail 50`
   - Restart if needed: `python scripts/run_intg.py restart --service <service>`

2. **Database Issues (Panel 6)**:
   - Check connection counts and blocked queries
   - Review PostgreSQL logs: `docker logs ats-intg-postgres --tail 50`
   - Monitor disk space: `df -h`

3. **Data Quality Issues (Panel 9)**:
   - Identify affected vendors/symbols
   - Check API error rates (Panel 8)
   - Verify data collection services (Panel 3-5)

## 🔗 Additional Resources

### **Related Documentation**
- [ATS Operations Guide](/docs/OPERATIONS.md)
- [Infrastructure Overview](/docs/INFRASTRUCTURE.md)
- [Dashboard Panel Documentation](/docs/ATS_COMPREHENSIVE_MONITORING_DASHBOARD.md)

### **SignOZ Resources**
- [SignOZ Query Language Documentation](https://signoz.io/docs/userguide/query-builder/)
- [Dashboard Creation Guide](https://signoz.io/docs/userguide/manage-dashboards/)
- [Alert Configuration](https://signoz.io/docs/userguide/alerts-management/)

### **Support Commands**
```bash
# Export current dashboard for backup
curl -s "http://localhost:8080/api/dashboards/uid/<dashboard-uid>" | jq . > backup.json

# Reset dashboard to default
curl -X DELETE "http://localhost:8080/api/dashboards/uid/<dashboard-uid>"
# Then re-import from JSON

# Check SignOZ service health
curl -f http://localhost:8080/api/v1/version
docker logs signoz-frontend --tail 20
```

---

**🎯 Next Steps after Import:**
1. Verify all panels display data correctly
2. Set up relevant alerts for your operational needs
3. Customize time ranges and refresh intervals
4. Add team-specific panels as needed
5. Configure alert notification channels (Slack, email, etc.)