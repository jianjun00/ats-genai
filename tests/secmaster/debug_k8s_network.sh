#!/bin/bash
set -e

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${YELLOW}Creating a debug pod to test network connectivity${NC}"

# Create a debug pod YAML
cat > debug-pod.yaml << EOF
apiVersion: v1
kind: Pod
metadata:
  name: network-debug-pod
  namespace: ats-dev
spec:
  containers:
  - name: network-debug
    image: dragonflyer762/ats-genai:dev-latest
    command: ["sleep", "3600"]
    env:
    - name: PYTHONPATH
      value: "/app/src"
    - name: LOG_LEVEL
      value: "DEBUG"
    - name: ENVIRONMENT
      value: "dev"
    - name: DB_USER
      value: "postgres"
    - name: DB_PASSWORD
      value: "postgres"
    - name: DB_NAME
      value: "trading_db"
  restartPolicy: Never
EOF

echo -e "${GREEN}Applying debug pod...${NC}"
kubectl apply -f debug-pod.yaml

echo -e "${GREEN}Waiting for debug pod to start...${NC}"
sleep 10

echo -e "${GREEN}Testing network connectivity...${NC}"
kubectl exec -n ats-dev network-debug-pod -- bash -c "apt-get update && apt-get install -y netcat iputils-ping dnsutils postgresql-client"
kubectl exec -n ats-dev network-debug-pod -- bash -c "echo 'Testing DNS resolution:' && nslookup timescaledb"
kubectl exec -n ats-dev network-debug-pod -- bash -c "echo 'Testing ping to timescaledb:' && ping -c 3 timescaledb || echo 'Ping failed (expected in some clusters)'"
kubectl exec -n ats-dev network-debug-pod -- bash -c "echo 'Testing port connectivity:' && nc -zv timescaledb 5432 || echo 'Port connectivity failed'"
kubectl exec -n ats-dev network-debug-pod -- bash -c "echo 'Testing PostgreSQL connection:' && PGPASSWORD=postgres psql -h timescaledb -U postgres -d trading_db -c '\\l' || echo 'PostgreSQL connection failed'"

echo -e "${GREEN}Cleaning up...${NC}"
kubectl delete pod -n ats-dev network-debug-pod

echo -e "${GREEN}Done!${NC}"
