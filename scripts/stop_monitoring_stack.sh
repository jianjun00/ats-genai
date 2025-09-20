#!/bin/bash
# Stop ATS Coverage Monitoring Stack

set -e

cd /home/jianjun/ats-genai-pm

echo "🛑 Stopping ATS Data Coverage Monitoring Stack..."

# Stop the monitoring stack
docker-compose -f deployment/docker-compose.monitoring.yml down

echo "✅ ATS Coverage Monitoring Stack stopped"
