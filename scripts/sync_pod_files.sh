#!/bin/bash

# Continuous sync script for backfill pod files
# Monitors and copies new files from running Kubernetes pods to host

NAMESPACE="ats-dev"
HOST_BASE_DIR="/home/jianjun/ats-data/minute-files"
SYNC_INTERVAL=300  # 5 minutes

echo "🔄 Starting continuous pod file sync..."
echo "📁 Target directory: $HOST_BASE_DIR"
echo "⏰ Sync interval: ${SYNC_INTERVAL}s"
echo "🔍 Monitoring namespace: $NAMESPACE"
echo ""

# Function to sync files from a pod
sync_pod_files() {
    local pod_name=$1
    local pod_path=$2
    local host_subdir=$3
    
    echo "🚀 Syncing from pod: $pod_name"
    echo "   📂 Pod path: $pod_path"
    echo "   💾 Host path: $HOST_BASE_DIR/$host_subdir"
    
    # Check if pod exists and is running
    if kubectl get pod -n $NAMESPACE $pod_name &>/dev/null; then
        status=$(kubectl get pod -n $NAMESPACE $pod_name -o jsonpath='{.status.phase}')
        if [[ "$status" == "Running" ]]; then
            # Count files in pod
            file_count=$(kubectl exec -n $NAMESPACE $pod_name -- find $pod_path -name "*.parquet" 2>/dev/null | wc -l)
            echo "   📊 Files in pod: $file_count"
            
            if [[ $file_count -gt 0 ]]; then
                # Create timestamped backup directory
                timestamp=$(date +"%Y%m%d_%H%M%S")
                backup_dir="$HOST_BASE_DIR/$host_subdir-$timestamp"
                
                echo "   📥 Copying to: $backup_dir"
                kubectl cp $NAMESPACE/$pod_name:$pod_path $backup_dir 2>/dev/null
                
                if [[ $? -eq 0 ]]; then
                    new_file_count=$(find $backup_dir -name "*.parquet" 2>/dev/null | wc -l)
                    echo "   ✅ Success: Copied $new_file_count files"
                else
                    echo "   ❌ Copy failed (files still being written)"
                fi
            else
                echo "   📝 No parquet files found"
            fi
        else
            echo "   ⚠️  Pod not running (status: $status)"
        fi
    else
        echo "   ❌ Pod not found"
    fi
    echo ""
}

# Main sync loop
while true; do
    echo "🔄 Starting sync cycle at $(date)"
    echo "=================================="
    
    # Sync from comprehensive job
    comprehensive_pod=$(kubectl get pods -n $NAMESPACE | grep "comprehensive-30year-all-vendors" | grep "Running" | awk '{print $1}')
    if [[ -n "$comprehensive_pod" ]]; then
        sync_pod_files "$comprehensive_pod" "/data/minute-files" "comprehensive-sync"
    fi
    
    # Sync from fixed polygon job  
    polygon_pod=$(kubectl get pods -n $NAMESPACE | grep "fixed-polygon-30year-minute-backfill" | grep "Running" | awk '{print $1}')
    if [[ -n "$polygon_pod" ]]; then
        sync_pod_files "$polygon_pod" "/data/minute-files" "polygon-sync"
    fi
    
    echo "⏰ Sync cycle complete. Waiting ${SYNC_INTERVAL}s..."
    echo ""
    sleep $SYNC_INTERVAL
done