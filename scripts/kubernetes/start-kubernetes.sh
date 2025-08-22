#!/bin/bash

# Configuration - Edit these values as needed
LOG_FILE="/home/jianjun/ats-genai/logs/kubernetes-startup.log"
CONFIG_FILE="/home/jianjun/ats-genai/scripts/k8s-config.yaml"
CLUSTER_NAME="ats-dev"
NAMESPACE="market-data"
DEPLOY_DATA_AGENT=true
MAX_RETRIES=3
DOCKER_STARTUP_TIMEOUT=30
NOTIFY_DESKTOP=true

# Create logs directory if it doesn't exist
mkdir -p "$(dirname "$LOG_FILE")"

# Function for logging
log() {
    local message="$1"
    local timestamp=$(date "+%Y-%m-%d %H:%M:%S")
    echo "[$timestamp] $message" | tee -a "$LOG_FILE"
    
    # Desktop notification if enabled
    if [[ "$NOTIFY_DESKTOP" == true ]] && command -v notify-send &> /dev/null; then
        notify-send "Kubernetes Startup" "$message" --icon=dialog-information
    fi
}

# Function for error handling
handle_error() {
    local error_message="$1"
    local exit_code="$2"
    
    log "ERROR: $error_message"
    
    if [[ "$NOTIFY_DESKTOP" == true ]] && command -v notify-send &> /dev/null; then
        notify-send "Kubernetes Startup Error" "$error_message" --icon=dialog-error
    fi
    
    if [[ -n "$exit_code" ]]; then
        exit "$exit_code"
    fi
}

# Create default config file if it doesn't exist
if [[ ! -f "$CONFIG_FILE" ]]; then
    log "Creating default config file at $CONFIG_FILE"
    cat > "$CONFIG_FILE" << EOF
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
name: $CLUSTER_NAME
nodes:
- role: control-plane
  kubeadmConfigPatches:
  - |
    kind: InitConfiguration
    nodeRegistration:
      kubeletExtraArgs:
        node-labels: "ingress-ready=true"
  extraPortMappings:
  - containerPort: 80
    hostPort: 80
    protocol: TCP
  - containerPort: 443
    hostPort: 443
    protocol: TCP
networking:
  apiServerAddress: "127.0.0.1"
  apiServerPort: 6443
EOF
fi

log "Starting Kubernetes environment setup..."

# Start Docker service with retry logic
retry_count=0
docker_started=false

while [[ $retry_count -lt $MAX_RETRIES && "$docker_started" == false ]]; do
    if service docker status > /dev/null 2>&1; then
        log "Docker service is already running"
        docker_started=true
    else
        log "Attempting to start Docker service (attempt $((retry_count + 1))/$MAX_RETRIES)..."
        sudo service docker start
        
        # Wait for Docker to initialize with timeout
        timeout_count=0
        while [[ $timeout_count -lt $DOCKER_STARTUP_TIMEOUT ]]; do
            if docker info &> /dev/null; then
                log "Docker service started successfully"
                docker_started=true
                break
            fi
            sleep 1
            ((timeout_count++))
        done
        
        if [[ "$docker_started" == false ]]; then
            log "Docker service failed to start within $DOCKER_STARTUP_TIMEOUT seconds"
            ((retry_count++))
        fi
    fi
done

if [[ "$docker_started" == false ]]; then
    handle_error "Failed to start Docker service after $MAX_RETRIES attempts" 1
fi

# Check Docker system resources
log "Docker system resources:"
docker system df | tee -a "$LOG_FILE"

# Check if our KinD cluster exists
if ! kind get clusters 2>/dev/null | grep -q "$CLUSTER_NAME"; then
    log "Creating KinD cluster '$CLUSTER_NAME' with custom configuration..."
    if ! kind create cluster --config "$CONFIG_FILE"; then
        handle_error "Failed to create KinD cluster" 2
    fi
    log "KinD cluster created successfully"
else
    # Check if the cluster is running by attempting to get nodes
    if ! kubectl get nodes --context "kind-$CLUSTER_NAME" &> /dev/null; then
        log "KinD cluster exists but appears to be down. Recreating..."
        kind delete cluster --name "$CLUSTER_NAME"
        if ! kind create cluster --config "$CONFIG_FILE"; then
            handle_error "Failed to recreate KinD cluster" 3
        fi
        log "KinD cluster recreated successfully"
        
        # Update kubeconfig to ensure correct API server address and port
        log "Updating kubeconfig with correct cluster configuration..."
        kind get kubeconfig --name="$CLUSTER_NAME" > ~/.kube/config
        log "Kubeconfig updated successfully"
    else
        log "KinD cluster '$CLUSTER_NAME' is already running"
    fi
fi

# Create namespace if it doesn't exist
if ! kubectl get namespace "$NAMESPACE" &> /dev/null; then
    log "Creating namespace '$NAMESPACE'..."
    if ! kubectl create namespace "$NAMESPACE"; then
        handle_error "Failed to create namespace '$NAMESPACE'" 4
    fi
    log "Namespace created successfully"
else
    log "Namespace '$NAMESPACE' already exists"
fi

# Deploy NGINX Ingress Controller for external access
log "Setting up NGINX Ingress Controller..."
if ! kubectl get deployment -n ingress-nginx ingress-nginx-controller &> /dev/null; then
    kubectl apply -f https://raw.githubusercontent.com/kubernetes/ingress-nginx/main/deploy/static/provider/kind/deploy.yaml
    log "Waiting for NGINX Ingress Controller to be ready..."
    kubectl wait --namespace ingress-nginx \
      --for=condition=ready pod \
      --selector=app.kubernetes.io/component=controller \
      --timeout=90s
fi

# Deploy metrics server for resource monitoring
log "Setting up Kubernetes Metrics Server..."
if ! kubectl get deployment -n kube-system metrics-server &> /dev/null; then
    kubectl apply -f https://github.com/kubernetes-sigs/metrics-server/releases/latest/download/components.yaml
    # Patch metrics-server to work with self-signed certificates
    kubectl patch deployment metrics-server -n kube-system --type=json \
      -p='[{"op":"add","path":"/spec/template/spec/containers/0/args/-","value":"--kubelet-insecure-tls"}]'
fi

# Deploy Data Agent if enabled
if [[ "$DEPLOY_DATA_AGENT" == true ]]; then
    log "Checking for Data Agent deployment..."
    
    # Check for required secrets
    if ! kubectl get secret -n "$NAMESPACE" data-agent-secrets &> /dev/null; then
        log "Warning: data-agent-secrets not found. Creating placeholder secrets..."
        kubectl create secret generic data-agent-secrets \
          --namespace "$NAMESPACE" \
          --from-literal=polygon-api-key="PLACEHOLDER" \
          --from-literal=tiingo-api-key="PLACEHOLDER" \
          --from-literal=openai-api-key="PLACEHOLDER" \
          --from-literal=database-url="PLACEHOLDER" \
          --from-literal=slack-webhook-url="PLACEHOLDER"
        log "Created placeholder secrets. Please update with real values."
    fi
    
    if ! kubectl get secret -n "$NAMESPACE" grafana-secrets &> /dev/null; then
        log "Creating Grafana secrets..."
        kubectl create secret generic grafana-secrets \
          --namespace "$NAMESPACE" \
          --from-literal=admin-password="admin"
    fi
    
    # Deploy data agent components
    log "Deploying Data Agent components..."
    DATA_AGENT_PATH="$SCRIPT_DIR/../k8s/data-agent/data-agent-deployment.yaml"
    GRAFANA_PATH="$SCRIPT_DIR/../k8s/data-agent/grafana-deployment.yaml"
    
    log "Using data agent path: $DATA_AGENT_PATH"
    log "Using grafana path: $GRAFANA_PATH"
    
    if [ -f "$DATA_AGENT_PATH" ]; then
        kubectl apply -f "$DATA_AGENT_PATH" || log "Error applying data agent deployment"
    else
        log "WARNING: Data agent deployment file not found at $DATA_AGENT_PATH"
    fi
    
    if [ -f "$GRAFANA_PATH" ]; then
        kubectl apply -f "$GRAFANA_PATH" || log "Error applying grafana deployment"
    else
        log "WARNING: Grafana deployment file not found at $GRAFANA_PATH"
    fi
    
    log "Data Agent deployment initiated"
    
    # Create services and ingress for data agent and Grafana
    log "Creating services and ingress for Data Agent and Grafana..."
    
    # Create a temporary services file if it doesn't exist
    SERVICES_FILE="$SCRIPT_DIR/k8s-services.yaml"
    if [ ! -f "$SERVICES_FILE" ]; then
        cat > "$SERVICES_FILE" << 'EOF'
apiVersion: v1
kind: Service
metadata:
  name: data-agent-service
  namespace: market-data
  labels:
    app: data-agent
spec:
  selector:
    app: data-agent
  ports:
  - port: 8080
    targetPort: 8080
    name: http
  - port: 8000
    targetPort: 8000
    name: metrics
---
apiVersion: v1
kind: Service
metadata:
  name: grafana-service
  namespace: market-data
  labels:
    app: grafana
spec:
  selector:
    app: grafana
  ports:
  - port: 3000
    targetPort: 3000
    name: http
---
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata:
  name: market-data-ingress
  namespace: market-data
  annotations:
    nginx.ingress.kubernetes.io/rewrite-target: /$2
    nginx.ingress.kubernetes.io/use-regex: "true"
spec:
  rules:
  - http:
      paths:
      - path: /data-agent(/|$)(.*)
        pathType: ImplementationSpecific
        backend:
          service:
            name: data-agent-service
            port:
              number: 8080
      - path: /grafana(/|$)(.*)
        pathType: ImplementationSpecific
        backend:
          service:
            name: grafana-service
            port:
              number: 3000
      - path: /metrics
        pathType: Exact
        backend:
          service:
            name: data-agent-service
            port:
              number: 8000
EOF
    fi
    
    # Apply the services and ingress
    log "Applying services and ingress from: $SERVICES_FILE"
    cat "$SERVICES_FILE" # Debug: print the file contents
    kubectl apply -f "$SERVICES_FILE" || log "Error applying services and ingress"
    log "Services and ingress created successfully"
fi

# Display cluster information
log "Cluster information:"
kubectl cluster-info | tee -a "$LOG_FILE"
log "Node status:"
kubectl get nodes | tee -a "$LOG_FILE"
log "Pods in $NAMESPACE namespace:"
kubectl get pods -n "$NAMESPACE" | tee -a "$LOG_FILE"

log "Kubernetes environment is ready!"
