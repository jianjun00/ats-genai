#!/bin/bash
# Parallel FirstRate Processing Launcher
# Launches multiple instances of the working firstrate script in parallel

WORKERS=${1:-8}
CHECKPOINT_FILE="stock_backfill_priority_batch.json"
SYMBOLS="MSFT,GOOGL,GOOG,AMZN,META,TSLA,NVDA,ADBE,JPM,BAC,WFC,GS,MS,C,JNJ,PFE,ABT,MRK,PG,KO,PEP,WMT,HD,NKE,XOM,CVX,COP,GE,V,MA,DIS,NFLX,PYPL,COST,TMUS,AVGO,UNH"

echo "🚀 Starting Parallel FirstRate Processing"
echo "   Workers: $WORKERS"
echo "   Priority Symbols: 37 total"
echo "   Checkpoint: $CHECKPOINT_FILE"

# Split symbols into batches for parallel processing
IFS=',' read -ra SYMBOL_ARRAY <<< "$SYMBOLS"
TOTAL_SYMBOLS=${#SYMBOL_ARRAY[@]}
SYMBOLS_PER_WORKER=$((($TOTAL_SYMBOLS + $WORKERS - 1) / $WORKERS))

echo "   Symbols per worker: $SYMBOLS_PER_WORKER"
echo ""

# Launch parallel workers
for ((worker=0; worker<$WORKERS; worker++)); do
    start_idx=$((worker * SYMBOLS_PER_WORKER))
    end_idx=$(((worker + 1) * SYMBOLS_PER_WORKER))
    
    if [ $start_idx -ge $TOTAL_SYMBOLS ]; then
        break
    fi
    
    if [ $end_idx -gt $TOTAL_SYMBOLS ]; then
        end_idx=$TOTAL_SYMBOLS
    fi
    
    # Get symbols for this worker
    worker_symbols=""
    for ((i=start_idx; i<end_idx; i++)); do
        if [ -n "$worker_symbols" ]; then
            worker_symbols="$worker_symbols,${SYMBOL_ARRAY[$i]}"
        else
            worker_symbols="${SYMBOL_ARRAY[$i]}"
        fi
    done
    
    if [ -n "$worker_symbols" ]; then
        echo "🔧 Worker $worker: Processing symbols $((start_idx+1))-$end_idx ($worker_symbols)"
        
        # Copy main checkpoint file to worker-specific checkpoint 
        cp "$CHECKPOINT_FILE" "worker_${worker}_${CHECKPOINT_FILE}"
        echo "   ✅ Copied main checkpoint to worker_${worker}_${CHECKPOINT_FILE}"
        
        # Launch worker with unique checkpoint file and correct output path
        nohup python3 scripts/populate_firstrate_minute_bars.py \
            --asset-type stock \
            --symbols "$worker_symbols" \
            --checkpoint-file "worker_${worker}_${CHECKPOINT_FILE}" \
            --output-path "/mnt/d/ats-data/minute-bars/firstrate" \
            --debug \
            > "/tmp/firstrate_worker_${worker}.log" 2>&1 &
        
        echo "   PID: $!"
    fi
done

echo ""
echo "✅ All workers launched! Monitor progress with:"
echo "   tail -f /tmp/firstrate_worker_*.log"
echo "   ps aux | grep populate_firstrate_minute_bars"