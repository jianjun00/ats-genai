#!/bin/bash
"""
Run Analytics in ATS-Dev Kubernetes

This script provides easy commands to run analytics jobs in the ats-dev k8s environment
where you have access to the real database with dev_password.
"""

set -e

echo "🚀 ATS-Dev Kubernetes Analytics Runner"
echo "====================================="

# Function to run production backtest
run_backtest() {
    local start_date=${1:-"2024-01-01"}
    local end_date=${2:-"2024-12-31"}
    local universe=${3:-"sp500_liquid"}
    local capital=${4:-"1000000"}
    
    echo "📊 Running Production Backtest in ats-dev k8s..."
    echo "   Start Date: $start_date"
    echo "   End Date: $end_date"
    echo "   Universe: $universe"
    echo "   Capital: \$$capital"
    
    python scripts/kubernetes/analytics_job_generator.py \
        --job-type backtest \
        --start-date "$start_date" \
        --end-date "$end_date" \
        --universe "$universe" \
        --capital "$capital" \
        --apply
    
    echo "✅ Backtest job submitted to ats-dev cluster"
    echo "🔍 Monitor with: kubectl logs -f job/backtest-$start_date-$end_date-* -n ats-dev"
}

# Function to start analytics API
start_api() {
    echo "🌐 Starting Analytics API Server in ats-dev k8s..."
    
    python scripts/kubernetes/analytics_job_generator.py \
        --job-type api \
        --apply
    
    echo "✅ Analytics API job submitted to ats-dev cluster"
    echo "🔍 Monitor with: kubectl logs -f job/analytics-api-* -n ats-dev"
    echo "🌐 Access API docs at: http://<external-ip>:8000/docs"
}

# Function to create service
create_service() {
    echo "🔗 Creating Analytics API Service in ats-dev k8s..."
    
    python scripts/kubernetes/analytics_job_generator.py \
        --job-type service \
        --apply
    
    echo "✅ Analytics service created in ats-dev cluster"
    echo "🔍 Get external IP with: kubectl get svc analytics-api-service -n ats-dev"
}

# Function to check status
check_status() {
    echo "📋 Checking Analytics Jobs Status in ats-dev..."
    
    echo ""
    echo "🏃 Running Jobs:"
    kubectl get jobs -n ats-dev -l app=ats-analytics
    
    echo ""
    echo "📦 Pods:"
    kubectl get pods -n ats-dev -l app=ats-analytics
    
    echo ""
    echo "🔗 Services:"
    kubectl get services -n ats-dev -l app=ats-analytics
    
    echo ""
    echo "📊 Recent Logs (last 10 lines):"
    kubectl logs -l app=ats-analytics -n ats-dev --tail=10
}

# Function to get logs
get_logs() {
    local job_name=$1
    
    if [ -z "$job_name" ]; then
        echo "📊 Getting logs for all analytics jobs..."
        kubectl logs -l app=ats-analytics -n ats-dev --tail=50
    else
        echo "📊 Getting logs for job: $job_name"
        kubectl logs "job/$job_name" -n ats-dev
    fi
}

# Function to clean up
cleanup() {
    echo "🧹 Cleaning up analytics jobs in ats-dev..."
    
    kubectl delete jobs -n ats-dev -l app=ats-analytics
    kubectl delete services -n ats-dev -l app=ats-analytics
    
    echo "✅ Cleanup complete"
}

# Main menu
case "${1:-}" in
    "backtest")
        run_backtest "$2" "$3" "$4" "$5"
        ;;
    "api")
        start_api
        ;;
    "service")
        create_service
        ;;
    "status")
        check_status
        ;;
    "logs")
        get_logs "$2"
        ;;
    "cleanup")
        cleanup
        ;;
    *)
        echo "📖 Usage: $0 {backtest|api|service|status|logs|cleanup}"
        echo ""
        echo "🎯 Commands:"
        echo "  backtest [start_date] [end_date] [universe] [capital]"
        echo "    - Run production backtest with real data"
        echo "    - Example: $0 backtest 2024-01-01 2024-12-31 sp500_liquid 1000000"
        echo ""
        echo "  api"
        echo "    - Start analytics API server"
        echo "    - Access at http://<external-ip>:8000/docs"
        echo ""
        echo "  service"
        echo "    - Create LoadBalancer service for API access"
        echo ""
        echo "  status"
        echo "    - Check status of all analytics jobs"
        echo ""
        echo "  logs [job_name]"
        echo "    - Get logs from analytics jobs"
        echo ""
        echo "  cleanup"
        echo "    - Remove all analytics jobs and services"
        echo ""
        echo "🔗 Real Analytics Access:"
        echo "  1. Run: $0 backtest    # Generate analytics data"
        echo "  2. Run: $0 api         # Start API server"
        echo "  3. Run: $0 service     # Expose API externally"
        echo "  4. Check: $0 status    # Monitor progress"
        echo ""
        exit 1
        ;;
esac