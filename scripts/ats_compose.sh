#!/bin/bash
"""
ATS Docker Compose Management Script

This script manages the complete ATS stack including:
- Dev and Intg PostgreSQL databases
- Analytics services for both environments  
- Monitoring (Grafana, Prometheus, AlertManager)
- Daily price collectors
"""

set -e

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
COMPOSE_FILE="$PROJECT_ROOT/docker-compose.ats.yml"

cd "$PROJECT_ROOT"

# Function to show usage
usage() {
    echo "Usage: $0 [COMMAND] [OPTIONS]"
    echo ""
    echo "Commands:"
    echo "  up [services...]     Start all services or specific services"
    echo "  down                 Stop all services"
    echo "  restart [services...]Restart all services or specific services"
    echo "  status               Show service status"
    echo "  logs [service]       Show logs for all or specific service"
    echo "  dev                  Start only dev environment services"
    echo "  intg                 Start only intg environment services"
    echo "  monitoring           Start only monitoring services"
    echo ""
    echo "Examples:"
    echo "  $0 up                # Start all services"
    echo "  $0 dev               # Start only dev services"
    echo "  $0 intg              # Start only intg services"
    echo "  $0 logs postgres-dev # Show dev PostgreSQL logs"
    echo "  $0 status            # Show all service status"
}

# Ensure environment file exists
setup_env() {
    if [ ! -f ".env" ] && [ -f ".env.ats" ]; then
        echo "📝 Creating .env from .env.ats template"
        cp .env.ats .env
        echo "⚠️  Please edit .env file with your API keys"
    fi
}

# Start all services
start_all() {
    echo "🚀 Starting complete ATS stack..."
    setup_env
    docker-compose -f docker-compose.ats.yml up -d "$@"
    echo "✅ ATS stack started"
    show_status
}

# Stop all services
stop_all() {
    echo "🛑 Stopping ATS stack..."
    docker-compose -f docker-compose.ats.yml down
    echo "✅ ATS stack stopped"
}

# Restart services
restart_services() {
    echo "🔄 Restarting ATS services..."
    docker-compose -f docker-compose.ats.yml restart "$@"
    echo "✅ ATS services restarted"
    show_status
}

# Show service status
show_status() {
    echo ""
    echo "📊 ATS Services Status:"
    docker-compose -f docker-compose.ats.yml ps
    echo ""
    echo "🌐 Service URLs:"
    echo "  - Dev Analytics:    http://localhost:3000"
    echo "  - Intg Analytics:   http://localhost:3002"
    echo "  - Grafana:          http://localhost:3001"
    echo "  - Prometheus:       http://localhost:9090"
    echo "  - AlertManager:     http://localhost:9093"
    echo "  - Node Exporter:    http://localhost:9100"
    echo "  - Dev PostgreSQL:   localhost:5432"
    echo "  - Intg PostgreSQL:  localhost:5433"
}

# Show logs
show_logs() {
    if [ -z "$1" ]; then
        docker-compose -f docker-compose.ats.yml logs -f
    else
        docker-compose -f docker-compose.ats.yml logs -f "$1"
    fi
}

# Start only dev environment
start_dev() {
    echo "🚀 Starting ATS Dev environment..."
    setup_env
    docker-compose -f docker-compose.ats.yml up -d postgres-dev analytics-dev price-collector-dev
    echo "✅ ATS Dev environment started"
    show_dev_status
}

# Start only intg environment
start_intg() {
    echo "🚀 Starting ATS Integration environment..."
    setup_env
    docker-compose -f docker-compose.ats.yml up -d postgres-intg analytics-intg price-collector-intg
    echo "✅ ATS Integration environment started"
    show_intg_status
}

# Start only monitoring
start_monitoring() {
    echo "🚀 Starting ATS Monitoring services..."
    docker-compose -f docker-compose.ats.yml up -d prometheus grafana alertmanager node-exporter
    echo "✅ ATS Monitoring services started"
    show_monitoring_status
}

# Show dev status
show_dev_status() {
    echo ""
    echo "📊 ATS Dev Environment Status:"
    docker-compose -f docker-compose.ats.yml ps postgres-dev analytics-dev price-collector-dev
    echo ""
    echo "🌐 Dev URLs:"
    echo "  - Analytics: http://localhost:3000"
    echo "  - PostgreSQL: localhost:5432"
}

# Show intg status  
show_intg_status() {
    echo ""
    echo "📊 ATS Integration Environment Status:"
    docker-compose -f docker-compose.ats.yml ps postgres-intg analytics-intg price-collector-intg
    echo ""
    echo "🌐 Integration URLs:"
    echo "  - Analytics: http://localhost:3002"
    echo "  - PostgreSQL: localhost:5433"
}

# Show monitoring status
show_monitoring_status() {
    echo ""
    echo "📊 ATS Monitoring Status:"
    docker-compose -f docker-compose.ats.yml ps prometheus grafana alertmanager node-exporter
    echo ""
    echo "🌐 Monitoring URLs:"
    echo "  - Grafana: http://localhost:3001"
    echo "  - Prometheus: http://localhost:9090"
    echo "  - AlertManager: http://localhost:9093"
}

# Main command processing
case "${1:-}" in
    up)
        shift
        start_all "$@"
        ;;
    down)
        stop_all
        ;;
    restart)
        shift
        restart_services "$@"
        ;;
    status)
        show_status
        ;;
    logs)
        shift
        show_logs "$@"
        ;;
    dev)
        start_dev
        ;;
    intg)
        start_intg
        ;;
    monitoring)
        start_monitoring
        ;;
    *)
        usage
        exit 1
        ;;
esac