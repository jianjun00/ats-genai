#!/bin/bash
#
# Events Data Validation Script
# Validates all event types and reports statistics
#

cd /home/jianjun/ats-genai-model

echo "=== Events Validation Report - $(date) ==="

echo "📊 Earnings Events:"
python3 scripts/run_intg.py query --query "
SELECT 
  COUNT(*) as total_events,
  COUNT(CASE WHEN updated_at >= CURRENT_DATE - INTERVAL '7 days' THEN 1 END) as recent_events,
  MAX(updated_at) as latest_update
FROM intg_earnings_events;"

echo "📰 News Events:"
python3 scripts/run_intg.py query --query "
SELECT 
  COUNT(*) as total_events,
  COUNT(CASE WHEN created_at >= CURRENT_DATE - INTERVAL '7 days' THEN 1 END) as recent_events,
  MAX(created_at) as latest_update  
FROM intg_news;"

echo "⚡ Gap Events:"
python3 scripts/run_intg.py query --query "
SELECT 
  COUNT(*) as total_events,
  COUNT(CASE WHEN updated_at >= CURRENT_DATE - INTERVAL '7 days' THEN 1 END) as recent_events,
  MAX(updated_at) as latest_update
FROM intg_gap_events;"

echo "💼 Financial Events (if available):"
python3 scripts/run_intg.py query --query "
SELECT 
  COUNT(*) as total_events,
  COUNT(CASE WHEN created_at >= CURRENT_DATE - INTERVAL '7 days' THEN 1 END) as recent_events,
  MAX(created_at) as latest_update
FROM intg_financial_events;" 2>/dev/null || echo "Table not yet created"

echo "=== Validation Complete ==="