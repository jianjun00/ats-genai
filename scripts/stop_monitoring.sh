#!/bin/bash
# Stop ATS Monitoring Stack
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="${SCRIPT_DIR}/.."

echo "🛑 Stopping ATS Monitoring Stack"
echo "================================="

cd "$PROJECT_DIR"

echo "📦 Stopping monitoring services..."
docker-compose -f docker-compose.monitoring.yml down

echo "🧹 Optional: Remove monitoring volumes (uncomment to use)"
echo "# docker-compose -f docker-compose.monitoring.yml down -v"

echo ""
echo "✅ Monitoring stack stopped"
echo ""
echo "To completely remove monitoring data:"
echo "docker-compose -f docker-compose.monitoring.yml down -v"