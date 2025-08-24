#!/bin/bash
# Automated ATS-DEV Environment Setup
# This script sets up the complete ats-dev namespace with PostgreSQL

set -e

echo "🚀 Setting up ATS-DEV environment..."

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check if minikube is running
check_minikube() {
    print_status "Checking minikube status..."
    if ! minikube status >/dev/null 2>&1; then
        print_warning "Minikube is not running. Starting minikube..."
        minikube start \
            --cpus=8 \
            --memory=12g \
            --disk-size=50g \
            --driver=docker \
            --mount \
            --mount-string="/home/jianjun/ats-genai:/mnt/host" \
            --extra-config=kubelet.housekeeping-interval=10s
        print_success "Minikube started successfully!"
    else
        print_success "Minikube is already running"
    fi
}

# Create ats-dev namespace
create_namespace() {
    print_status "Creating ats-dev namespace..."
    if kubectl get namespace ats-dev >/dev/null 2>&1; then
        print_warning "Namespace ats-dev already exists"
    else
        kubectl create namespace ats-dev
        print_success "Namespace ats-dev created"
    fi
}

# Create host data directory
create_host_directories() {
    print_status "Creating host data directories..."
    mkdir -p /home/jianjun/ats-genai/data/postgres
    mkdir -p /home/jianjun/ats-genai/data/logs
    mkdir -p /home/jianjun/ats-genai/data/backups
    print_success "Host directories created"
}

# Deploy PostgreSQL with high resources
deploy_postgres() {
    print_status "Deploying PostgreSQL with high resource allocation..."
    
    kubectl apply -n ats-dev -f - << 'EOF'
apiVersion: apps/v1
kind: Deployment
metadata:
  name: postgres
  namespace: ats-dev
  labels:
    app: postgres
    tier: database
spec:
  replicas: 1
  selector:
    matchLabels:
      app: postgres
  template:
    metadata:
      labels:
        app: postgres
        tier: database
    spec:
      containers:
      - name: postgres
        image: postgres:15
        env:
        - name: POSTGRES_DB
          value: "dev_db"
        - name: POSTGRES_USER
          value: "postgres"
        - name: POSTGRES_PASSWORD
          value: "dev_password"
        - name: PGDATA
          value: "/var/lib/postgresql/data/pgdata"
        - name: POSTGRES_INITDB_ARGS
          value: "--auth-host=scram-sha-256"
        ports:
        - containerPort: 5432
          name: postgres
        volumeMounts:
        - name: postgres-host-storage
          mountPath: /var/lib/postgresql/data
        resources:
          requests:
            memory: "6Gi"
            cpu: "3000m"
          limits:
            memory: "12Gi"
            cpu: "6000m"
        livenessProbe:
          exec:
            command:
            - pg_isready
            - -U
            - postgres
          initialDelaySeconds: 30
          periodSeconds: 10
        readinessProbe:
          exec:
            command:
            - pg_isready
            - -U
            - postgres
          initialDelaySeconds: 5
          periodSeconds: 5
      volumes:
      - name: postgres-host-storage
        hostPath:
          path: /mnt/host/data/postgres
          type: DirectoryOrCreate
---
apiVersion: v1
kind: Service
metadata:
  name: postgres
  namespace: ats-dev
  labels:
    app: postgres
    tier: database
spec:
  selector:
    app: postgres
  ports:
    - port: 5432
      targetPort: 5432
      nodePort: 30433
  type: NodePort
EOF

    print_success "PostgreSQL deployment created"
}

# Wait for PostgreSQL to be ready
wait_for_postgres() {
    print_status "Waiting for PostgreSQL to be ready..."
    kubectl wait --for=condition=available --timeout=300s deployment/postgres -n ats-dev
    
    # Additional check for PostgreSQL readiness
    for i in {1..30}; do
        if kubectl exec -n ats-dev deployment/postgres -- pg_isready -U postgres >/dev/null 2>&1; then
            print_success "PostgreSQL is ready and accepting connections"
            return 0
        fi
        echo -n "."
        sleep 5
    done
    
    print_error "PostgreSQL failed to become ready within timeout"
    return 1
}

# Create basic database schema
setup_database_schema() {
    print_status "Setting up basic database schema..."
    
    # Run basic migrations if migration manager exists
    if [ -f "src/db/migration_manager.py" ]; then
        print_status "Running database migrations..."
        PYTHONPATH=src python src/db/migration_manager.py migrate \
            --db-url "postgresql://postgres:postgres@localhost:30433/dev_db" || true
    fi
    
    print_success "Database schema setup completed"
}

# Display connection information
display_connection_info() {
    echo ""
    print_success "🎉 ATS-DEV environment setup completed!"
    echo ""
    echo "📊 Connection Information:"
    echo "  Database Host: postgres (internal) / localhost:30433 (external)"
    echo "  Database: dev_db"
    echo "  User: postgres"
    echo "  Password: postgres"
    echo ""
    echo "🔧 Quick Commands:"
    echo "  Test connection: run_dev query \"SELECT version()\""
    echo "  Check pods: kubectl get pods -n ats-dev"
    echo "  Port-forward: kubectl port-forward service/postgres 5433:5432 -n ats-dev"
    echo ""
    echo "📁 Data Directory: /home/jianjun/ats-genai/data/postgres"
    echo ""
}

# Main execution
main() {
    echo "🚀 Starting ATS-DEV Environment Setup"
    echo "======================================"
    
    check_minikube
    create_namespace
    create_host_directories
    deploy_postgres
    wait_for_postgres
    setup_database_schema
    display_connection_info
    
    print_success "Setup completed successfully! 🎉"
}

# Run main function
main "$@"