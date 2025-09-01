#!/bin/bash

# Parallel FirstRate Processing Launcher - V2 (No Symbol Filtering)
# Created: 2025-09-01 12:10
# Purpose: Launch multiple FirstRate workers to process remaining work in parallel
# Strategy: Let each worker process any available work without symbol restrictions

WORKERS=${1:-8}
CHECKPOINT_FILE="stock_backfill_priority_batch.json"

echo "🚀 Starting Parallel FirstRate Processing (V2 - No Symbol Filtering)"
echo "   Workers: $WORKERS" 
echo "   Strategy: Each worker processes any remaining work"
echo "   Checkpoint: $CHECKPOINT_FILE"
echo ""

# Launch parallel workers without symbol restrictions
for ((worker=0; worker<$WORKERS; worker++)); do
    echo "🔧 Worker $worker: Processing any remaining symbols"
    
    # Copy main checkpoint file to worker-specific checkpoint 
    cp "$CHECKPOINT_FILE" "worker_${worker}_${CHECKPOINT_FILE}"
    echo "   ✅ Copied main checkpoint to worker_${worker}_${CHECKPOINT_FILE}"
    
    # Launch worker with correct host-compatible paths
    nohup python3 scripts/populate_firstrate_minute_bars.py \
        --asset-type stock \
        --checkpoint-file "worker_${worker}_${CHECKPOINT_FILE}" \
        --data-path "/mnt/d/ats-data/firstrate-data" \
        --output-path "/mnt/d/ats-data/minute-bars/firstrate" \
        --resume \
        --debug \
        > "/tmp/firstrate_worker_${worker}.log" 2>&1 &
    
    echo "   PID: $!"
    echo ""
done

echo "✅ All workers launched! Each worker will process remaining symbols in parallel."
echo ""
echo "Monitor progress with:"
echo "   tail -f /tmp/firstrate_worker_*.log"
echo "   ps aux | grep populate_firstrate_minute_bars"
echo "   watch 'ls -la /mnt/d/ats-data/minute-bars/firstrate/ | wc -l'"