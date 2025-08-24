#!/bin/bash
# Automated ATS All Environments Setup
# Sets up ats-dev, ats-intg, and ats-prod namespaces with PostgreSQL instances

set -e

echo "🚀 Setting up ALL ATS environments (dev, intg, prod)..."

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
PURPLE='\033[0;35m'
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

print_env() {
    echo -e "${PURPLE}[ENV-$1]${NC} $2"
}

# Environment configuration
ENVIRONMENTS=("dev" "intg" "prod")
declare -A DB_PASSWORDS=( 
    ["dev"]="postgres" 
    ["intg"]="intg_password" 
    ["prod"]="prod_secure_password_2024" 
)
declare -A NODE_PORTS=(
    ["dev"]="30433"
    ["intg"]="30434" 
    ["prod"]="30435"
)

# Check if minikube is running
check_minikube() {
    print_status "Checking minikube status..."
    if ! minikube status >/dev/null 2>&1; then
        print_warning "Minikube is not running. Starting minikube with high resources..."
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

# Create all namespaces
create_namespaces() {
    print_status "Creating ATS namespaces..."
    for env in "${ENVIRONMENTS[@]}"; do
        if kubectl get namespace "ats-$env" >/dev/null 2>&1; then
            print_warning "Namespace ats-$env already exists"
        else
            kubectl create namespace "ats-$env"
            print_success "Namespace ats-$env created"
        fi
    done
}

# Create host data directories
create_host_directories() {
    print_status "Creating host data directories for all environments..."
    for env in "${ENVIRONMENTS[@]}"; do
        mkdir -p "/home/jianjun/ats-genai/data/postgres-$env"
        mkdir -p "/home/jianjun/ats-genai/data/logs-$env"
        mkdir -p "/home/jianjun/ats-genai/data/backups-$env"
        print_env "$env" "Host directories created"
    done
    print_success "All host directories created"
}

# Deploy PostgreSQL for specific environment
deploy_postgres_for_env() {
    local env=$1
    local db_password=${DB_PASSWORDS[$env]}
    local node_port=${NODE_PORTS[$env]}
    
    print_env "$env" "Deploying PostgreSQL with high resources..."
    
    kubectl apply -n "ats-$env" -f - << EOF
apiVersion: apps/v1
kind: Deployment
metadata:
  name: postgres
  namespace: ats-$env
  labels:
    app: postgres
    tier: database
    environment: $env
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
        environment: $env
    spec:
      containers:
      - name: postgres
        image: postgres:15
        env:
        - name: POSTGRES_DB
          value: "${env}_db"
        - name: POSTGRES_USER
          value: "postgres"
        - name: POSTGRES_PASSWORD
          value: "$db_password"
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
            memory: "4Gi"
            cpu: "2000m"
          limits:
            memory: "8Gi"
            cpu: "4000m"
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
          path: /mnt/host/data/postgres-$env
          type: DirectoryOrCreate
---
apiVersion: v1
kind: Service
metadata:
  name: postgres
  namespace: ats-$env
  labels:
    app: postgres
    tier: database
    environment: $env
spec:
  selector:
    app: postgres
  ports:
    - port: 5432
      targetPort: 5432
      nodePort: $node_port
  type: NodePort
EOF

    print_env "$env" "PostgreSQL deployment created (NodePort: $node_port)"
}

# Deploy PostgreSQL for all environments
deploy_all_postgres() {
    print_status "Deploying PostgreSQL for all environments..."
    for env in "${ENVIRONMENTS[@]}"; do
        deploy_postgres_for_env "$env"
    done
}

# Wait for PostgreSQL to be ready in specific environment
wait_for_postgres_env() {
    local env=$1
    print_env "$env" "Waiting for PostgreSQL to be ready..."
    
    kubectl wait --for=condition=available --timeout=300s deployment/postgres -n "ats-$env" || {
        print_error "PostgreSQL in ats-$env failed to become ready"
        return 1
    }
    
    # Additional health check
    for i in {1..30}; do
        if kubectl exec -n "ats-$env" deployment/postgres -- pg_isready -U postgres >/dev/null 2>&1; then
            print_env "$env" "PostgreSQL is ready and accepting connections"
            return 0
        fi
        echo -n "."
        sleep 5
    done
    
    print_error "PostgreSQL in ats-$env failed health check"
    return 1
}

# Wait for all PostgreSQL instances
wait_for_all_postgres() {
    print_status "Waiting for all PostgreSQL instances to be ready..."
    for env in "${ENVIRONMENTS[@]}"; do
        wait_for_postgres_env "$env"
    done
    print_success "All PostgreSQL instances are ready!"
}

# Create connection test script
create_connection_test() {
    print_status "Creating connection test scripts..."
    
    cat > /tmp/test_all_connections.sh << 'EOF'
#!/bin/bash
echo "🔗 Testing all database connections..."

# Test dev
echo "Testing ats-dev (localhost:30433)..."
kubectl port-forward service/postgres 5433:5432 -n ats-dev &
DEV_PF_PID=$!
sleep 3
python -c "import psycopg2; conn = psycopg2.connect('postgresql://postgres:postgres@localhost:5433/dev_db'); print('✅ ats-dev connection successful'); conn.close()" 2>/dev/null || echo "❌ ats-dev connection failed"
kill $DEV_PF_PID 2>/dev/null || true

# Test intg
echo "Testing ats-intg (localhost:30434)..."
kubectl port-forward service/postgres 5434:5432 -n ats-intg &
INTG_PF_PID=$!
sleep 3
python -c "import psycopg2; conn = psycopg2.connect('postgresql://postgres:intg_password@localhost:5434/intg_db'); print('✅ ats-intg connection successful'); conn.close()" 2>/dev/null || echo "❌ ats-intg connection failed"
kill $INTG_PF_PID 2>/dev/null || true

# Test prod
echo "Testing ats-prod (localhost:30435)..."
kubectl port-forward service/postgres 5435:5432 -n ats-prod &
PROD_PF_PID=$!
sleep 3
python -c "import psycopg2; conn = psycopg2.connect('postgresql://postgres:prod_secure_password_2024@localhost:5435/prod_db'); print('✅ ats-prod connection successful'); conn.close()" 2>/dev/null || echo "❌ ats-prod connection failed"
kill $PROD_PF_PID 2>/dev/null || true

echo "🎉 Connection tests completed!"
EOF

    chmod +x /tmp/test_all_connections.sh
    print_success "Connection test script created at /tmp/test_all_connections.sh"
}

# Display comprehensive connection information
display_connection_info() {
    echo ""
    print_success "🎉 ALL ATS environments setup completed!"
    echo ""
    echo "📊 Environment Summary:"
    echo "┌─────────────┬─────────────────┬─────────────────┬─────────────────┐"
    echo "│ Environment │ Database        │ NodePort        │ Password        │"
    echo "├─────────────┼─────────────────┼─────────────────┼─────────────────┤"
    echo "│ ats-dev     │ dev_db          │ 30433          │ postgres        │"
    echo "│ ats-intg    │ intg_db         │ 30434          │ intg_password   │"
    echo "│ ats-prod    │ prod_db         │ 30435          │ prod_secure_... │"
    echo "└─────────────┴─────────────────┴─────────────────┴─────────────────┘"
    echo ""
    echo "🔧 Quick Commands:"
    echo "  Test all connections: /tmp/test_all_connections.sh"
    echo "  Check all pods: kubectl get pods --all-namespaces | grep postgres"
    echo "  Port-forward dev: kubectl port-forward service/postgres 5433:5432 -n ats-dev"
    echo "  Port-forward intg: kubectl port-forward service/postgres 5434:5432 -n ats-intg" 
    echo "  Port-forward prod: kubectl port-forward service/postgres 5435:5432 -n ats-prod"
    echo ""
    echo "📁 Data Directories:"
    echo "  Dev:  /home/jianjun/ats-genai/data/postgres-dev"
    echo "  Intg: /home/jianjun/ats-genai/data/postgres-intg"
    echo "  Prod: /home/jianjun/ats-genai/data/postgres-prod"
    echo ""
    echo "🔐 Security Note: Production uses secure password. Dev/Intg use simple passwords for development."
    echo ""
}

# Health check for all environments
health_check_all() {
    print_status "Running comprehensive health check..."
    
    echo ""
    echo "🏥 Health Check Results:"
    for env in "${ENVIRONMENTS[@]}"; do
        local node_port=${NODE_PORTS[$env]}
        echo -n "  ats-$env: "
        
        if kubectl get deployment postgres -n "ats-$env" >/dev/null 2>&1 && \
           kubectl get pods -n "ats-$env" | grep postgres | grep -q "Running"; then
            echo -e "${GREEN}✅ Healthy${NC} (NodePort: $node_port)"
        else
            echo -e "${RED}❌ Unhealthy${NC}"
        fi
    done
    echo ""
}

# Main execution
main() {
    echo "🚀 Starting ALL ATS Environments Setup"
    echo "====================================="
    echo "This will create: ats-dev, ats-intg, ats-prod"
    echo "Each with dedicated PostgreSQL instance"
    echo ""
    
    check_minikube
    create_namespaces
    create_host_directories
    deploy_all_postgres
    wait_for_all_postgres
    create_connection_test
    health_check_all
    display_connection_info
    
    print_success "🎉 All environments setup completed successfully!"
    echo ""
    print_warning "⚠️  Remember to run migrations for each environment separately"
    print_status "Next steps: Run /tmp/test_all_connections.sh to verify connectivity"
}

# Run main function
main "$@"