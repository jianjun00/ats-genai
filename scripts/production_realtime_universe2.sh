#!/bin/bash
# Production Real-Time Minute Bar Collection for Universe ID 2
# High Volume Large Cap Universe (874+ symbols)

set -e

# Configuration
SERVICE_NAME="ats-realtime-universe2"
LOG_FILE="/var/log/${SERVICE_NAME}.log"
PID_FILE="/var/run/${SERVICE_NAME}.pid"

# Function to display usage
usage() {
    echo "Usage: $0 {start|stop|restart|status|logs}"
    echo ""
    echo "Commands:"
    echo "  start   - Start real-time collection service"
    echo "  stop    - Stop real-time collection service"
    echo "  restart - Restart real-time collection service"
    echo "  status  - Check service status"
    echo "  logs    - Show recent logs"
    exit 1
}

# Function to start the service
start_service() {
    if [ -f "$PID_FILE" ] && kill -0 "$(cat "$PID_FILE")" 2>/dev/null; then
        echo "❌ Service is already running (PID: $(cat "$PID_FILE"))"
        exit 1
    fi

    echo "🚀 Starting ATS Real-Time Collection for Universe ID 2"
    echo "📊 Universe: high_volume_large_cap (874+ symbols)"
    echo "📡 Target: Integration environment"
    echo ""

    # Set environment variables
    export POLYGON_API_KEY="wfrcZNX3ZJJt55Or_CmBXda8G8e8tABD"
    export TIINGO_API_KEY="5f40b4f36e171405746304ec0e5a6f3aa9ca77e5"
    export EODHD_API_KEY="68aa0c7d2fe831.67386369"
    export ENVIRONMENT="intg"
    export PYTHONPATH="src"

    # Change to project directory
    cd /home/jianjun/ats-genai-model

    # Start service in background
    nohup python3 scripts/realtime_minute_collector.py \
        --universe-id 2 \
        --db-host localhost \
        --db-port 4432 \
        --db-user postgres \
        --db-password intg_password \
        --db-name intg_db \
        > "$LOG_FILE" 2>&1 &

    # Save PID
    echo $! > "$PID_FILE"

    echo "✅ Service started successfully!"
    echo "📋 PID: $(cat "$PID_FILE")"
    echo "📋 Log file: $LOG_FILE"
    echo "📋 Run '$0 status' to check service health"
}

# Function to stop the service
stop_service() {
    if [ ! -f "$PID_FILE" ]; then
        echo "❌ Service is not running (no PID file found)"
        exit 1
    fi

    PID=$(cat "$PID_FILE")
    if ! kill -0 "$PID" 2>/dev/null; then
        echo "❌ Service is not running (PID $PID not found)"
        rm -f "$PID_FILE"
        exit 1
    fi

    echo "🛑 Stopping real-time collection service..."
    kill "$PID"

    # Wait for graceful shutdown
    for i in {1..10}; do
        if ! kill -0 "$PID" 2>/dev/null; then
            break
        fi
        echo "⏳ Waiting for graceful shutdown... ($i/10)"
        sleep 1
    done

    # Force kill if still running
    if kill -0 "$PID" 2>/dev/null; then
        echo "⚡ Force stopping service..."
        kill -9 "$PID"
    fi

    rm -f "$PID_FILE"
    echo "✅ Service stopped successfully"
}

# Function to check service status
check_status() {
    if [ ! -f "$PID_FILE" ]; then
        echo "❌ Service is not running (no PID file)"
        return 1
    fi

    PID=$(cat "$PID_FILE")
    if ! kill -0 "$PID" 2>/dev/null; then
        echo "❌ Service is not running (PID $PID not found)"
        rm -f "$PID_FILE"
        return 1
    fi

    echo "✅ Service is running"
    echo "📋 PID: $PID"
    echo "📋 Log file: $LOG_FILE"

    # Check recent activity
    if [ -f "$LOG_FILE" ]; then
        echo ""
        echo "📊 Recent activity (last 5 lines):"
        tail -5 "$LOG_FILE"
    fi
    return 0
}

# Function to show logs
show_logs() {
    if [ ! -f "$LOG_FILE" ]; then
        echo "❌ No log file found: $LOG_FILE"
        exit 1
    fi

    echo "📋 Real-time collection logs:"
    echo "=============================="
    tail -50 "$LOG_FILE"
}

# Main script logic
case "${1:-}" in
    start)
        start_service
        ;;
    stop)
        stop_service
        ;;
    restart)
        echo "🔄 Restarting service..."
        stop_service
        sleep 2
        start_service
        ;;
    status)
        check_status
        ;;
    logs)
        show_logs
        ;;
    *)
        usage
        ;;
esac