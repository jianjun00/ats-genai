# 🎯 Data Quality Dashboard Navigation

## How to Access the Data Quality Dashboard

### From Your Main Analytics Dashboard:

1. **Go to**: http://10.0.0.79:4000/ (or http://localhost:4000/)

2. **Click the button**: "🎯 Data Quality Dashboard" (it's the first button at the top)

3. **You'll be redirected to**: http://10.0.0.79:4000/data-quality/dashboard

### Direct Access:

You can also go directly to: http://10.0.0.79:4000/data-quality/dashboard

### What You'll See:

- **15 total issues** currently detected
- **Real-time dashboard** with auto-refresh every 60 seconds
- **Issue breakdown** by severity (Critical, High, Medium, Low)
- **Quality score**: 0/100 (CRITICAL status)

### Current Issues Detected:

1. **Missing Data Issues (5)**: No daily prices for Sep 5, 8, 9, 10, 11
2. **Stale Data Issues (10)**: Symbols with 1821-day-old data from 2020

### API Access:

- **JSON API**: http://10.0.0.79:4000/data-quality/api/issues
- Returns real-time data quality issues in JSON format

---

The data quality dashboard is now **fully integrated** into your existing analytics service at port 4000. No separate service needed!