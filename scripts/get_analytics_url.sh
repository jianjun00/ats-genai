#!/bin/bash

# Script to get external access to ATS Analytics Service
# This handles minikube networking limitations in WSL2/Docker environments

echo "🔗 Getting ATS Analytics Service external access..."

# Check if service is running
if ! kubectl get deployment ats-analytics-service -n ats-dev &>/dev/null; then
    echo "❌ Analytics service is not deployed"
    exit 1
fi

# Check pod status
POD_STATUS=$(kubectl get pods -n ats-dev -l app=ats-analytics-service --no-headers | awk '{print $3}' | head -1)
if [ "$POD_STATUS" != "Running" ]; then
    echo "❌ Analytics service pod is not running (Status: $POD_STATUS)"
    kubectl get pods -n ats-dev -l app=ats-analytics-service
    exit 1
fi

echo "✅ Analytics service is running"

# Kill any existing port-forward processes
pkill -f "kubectl port-forward.*ats-analytics-service" 2>/dev/null

# Start port-forward in background
echo "🚀 Starting port-forward for external access..."
kubectl port-forward service/ats-analytics-service 3001:3000 -n ats-dev --address=0.0.0.0 &
PORT_FORWARD_PID=$!

# Wait for port-forward to be ready
sleep 3

# Test connection
if curl -s --connect-timeout 3 http://localhost:3001/health &>/dev/null; then
    echo "✅ Service is accessible!"
    echo ""
    echo "📊 ATS Analytics Service URLs:"
    echo "   🏠 Dashboard:    http://localhost:3001/"
    echo "   💚 Health:       http://localhost:3001/health"
    echo "   📈 Job Stats:    http://localhost:3001/api/v1/jobs/stats"
    echo "   📋 Jobs List:    http://localhost:3001/api/v1/jobs"
    echo ""
    echo "💡 Port-forward is running in background (PID: $PORT_FORWARD_PID)"
    echo "   To stop: kill $PORT_FORWARD_PID"
else
    echo "❌ Service is not accessible via port-forward"
    kill $PORT_FORWARD_PID 2>/dev/null
    exit 1
fi