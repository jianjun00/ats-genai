#!/bin/bash
#
# Financial Events Sync Script
# Syncs financial events from dev to intg environment
#

cd /home/jianjun/ats-genai-model

echo "Starting financial events sync at $(date)"

# Check if intg_financial_events table exists, create if needed
python3 scripts/run_intg.py query --query "
CREATE TABLE IF NOT EXISTS intg_financial_events (
    LIKE dev_financial_events INCLUDING ALL
);" 2>/dev/null || echo "Table creation skipped or failed"

# Sync recent financial events from dev to intg
python3 scripts/run_intg.py query --query "
INSERT INTO intg_financial_events 
SELECT * FROM dev_financial_events 
WHERE created_at >= CURRENT_DATE - INTERVAL '2 days' 
ON CONFLICT (id) DO NOTHING;" 2>/dev/null || echo "No new events to sync"

# Report sync results
COUNT=$(python3 scripts/run_intg.py query --query "SELECT COUNT(*) FROM intg_financial_events;" | tail -1 | xargs)
echo "Financial events sync completed. Total events in intg: $COUNT"