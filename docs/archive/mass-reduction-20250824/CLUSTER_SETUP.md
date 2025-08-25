# Kubernetes Cluster Setup Guide

## Overview

This guide covers setting up separate Kubernetes clusters for the ATS GenAI system across development, integration, and production environments.

## Cluster Architecture

### Recommended Setup
- **trading-dev**: Development cluster (can be local or cloud)
- **trading-intg**: Integration cluster (cloud-based)
- **trading-prod**: Production cluster (cloud-based, multi-AZ)

### Cluster Specifications

| Environment | Nodes | Node Type | CPU | Memory | Storage |
|-------------|-------|-----------|-----|--------|---------|
| Dev         | 1-2   | t3.medium | 2   | 4GB    | 50GB    |
| Integration | 2-3   | t3.large  | 2   | 8GB    | 100GB   |
| Production  | 3-5   | t3.xlarge | 4   | 16GB   | 200GB   |

## Cloud Provider Setup

### AWS EKS Setup

#### Prerequisites
```bash
# Install required tools
curl --silent --location "https://github.com/weaveworks/eksctl/releases/latest/download/eksctl_$(uname -s)_amd64.tar.gz" | tar xz -C /tmp
sudo mv /tmp/eksctl /usr/local/bin

# Install AWS CLI v2
curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
unzip awscliv2.zip
sudo ./aws/install

# Configure AWS credentials
aws configure
```

#### Create Development Cluster
```bash
# Create development cluster
eksctl create cluster \
  --name trading-dev \
  --version 1.28 \
  --region us-west-2 \
  --nodegroup-name dev-workers \
  --node-type t3.medium \
  --nodes 2 \
  --nodes-min 1 \
  --nodes-max 3 \
  --managed

# Verify cluster
kubectl get nodes
```

#### Create Integration Cluster
```bash
# Create integration cluster
eksctl create cluster \
  --name trading-intg \
  --version 1.28 \
  --region us-west-2 \
  --nodegroup-name intg-workers \
  --node-type t3.large \
  --nodes 3 \
  --nodes-min 2 \
  --nodes-max 5 \
  --managed \
  --enable-ssm

# Configure cluster autoscaler
kubectl apply -f https://raw.githubusercontent.com/kubernetes/autoscaler/master/cluster-autoscaler/cloudprovider/aws/examples/cluster-autoscaler-autodiscover.yaml
```

#### Create Production Cluster
```bash
# Create production cluster with high availability
eksctl create cluster \
  --name trading-prod \
  --version 1.28 \
  --region us-west-2 \
  --zones us-west-2a,us-west-2b,us-west-2c \
  --nodegroup-name prod-workers \
  --node-type t3.xlarge \
  --nodes 3 \
  --nodes-min 3 \
  --nodes-max 10 \
  --managed \
  --enable-ssm \
  --asg-access \
  --external-dns-access \
  --full-ecr-access

# Enable logging
eksctl utils update-cluster-logging --enable-types=all --region=us-west-2 --cluster=trading-prod
```

### Google GKE Setup

#### Prerequisites
```bash
# Install gcloud CLI
curl https://sdk.cloud.google.com | bash
exec -l $SHELL
gcloud init

# Install kubectl
gcloud components install kubectl
```

#### Create Clusters
```bash
# Development cluster
gcloud container clusters create trading-dev \
  --zone us-central1-a \
  --num-nodes 2 \
  --machine-type e2-medium \
  --enable-autoscaling \
  --min-nodes 1 \
  --max-nodes 3

# Integration cluster
gcloud container clusters create trading-intg \
  --zone us-central1-a \
  --num-nodes 3 \
  --machine-type e2-standard-2 \
  --enable-autoscaling \
  --min-nodes 2 \
  --max-nodes 5

# Production cluster (regional for HA)
gcloud container clusters create trading-prod \
  --region us-central1 \
  --num-nodes 1 \
  --machine-type e2-standard-4 \
  --enable-autoscaling \
  --min-nodes 3 \
  --max-nodes 10 \
  --enable-network-policy
```

## Local Development Setup

### Kind (Kubernetes in Docker)

```bash
# Install Kind
curl -Lo ./kind https://kind.sigs.k8s.io/dl/v0.20.0/kind-linux-amd64
chmod +x ./kind
sudo mv ./kind /usr/local/bin/kind

# Create local cluster
cat <<EOF | kind create cluster --config=-
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
name: trading-dev
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
- role: worker
- role: worker
EOF

# Verify cluster
kubectl cluster-info --context kind-trading-dev
```

### Minikube Alternative

```bash
# Install minikube
curl -LO https://storage.googleapis.com/minikube/releases/latest/minikube-linux-amd64
sudo install minikube-linux-amd64 /usr/local/bin/minikube

# Start cluster
minikube start --profile trading-dev --cpus 4 --memory 8192 --disk-size 50g

# Enable addons
minikube addons enable ingress --profile trading-dev
minikube addons enable metrics-server --profile trading-dev
```

## Cluster Configuration

### 1. Create Namespaces

```bash
# Create namespaces for each environment
kubectl create namespace ats-dev
kubectl create namespace ats-intg
kubectl create namespace ats-prod

# Label namespaces
kubectl label namespace ats-dev environment=dev
kubectl label namespace ats-intg environment=intg
kubectl label namespace ats-prod environment=prod
```

### 2. Install Essential Components

#### Ingress Controller (NGINX)
```bash
# Install NGINX Ingress Controller
kubectl apply -f https://raw.githubusercontent.com/kubernetes/ingress-nginx/controller-v1.8.2/deploy/static/provider/cloud/deploy.yaml

# Verify installation
kubectl get pods -n ingress-nginx
```

#### Cert-Manager (TLS Certificates)
```bash
# Install cert-manager
kubectl apply -f https://github.com/cert-manager/cert-manager/releases/download/v1.13.2/cert-manager.yaml

# Verify installation
kubectl get pods -n cert-manager
```

#### Metrics Server
```bash
# Install metrics server
kubectl apply -f https://github.com/kubernetes-sigs/metrics-server/releases/latest/download/components.yaml

# Verify installation
kubectl top nodes
```

### 3. Configure RBAC

```bash
# Create service account for ATS
kubectl create serviceaccount ats-service-account -n ats-prod

# Create cluster role
cat <<EOF | kubectl apply -f -
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: ats-cluster-role
rules:
- apiGroups: [""]
  resources: ["pods", "services", "configmaps", "secrets"]
  verbs: ["get", "list", "watch"]
- apiGroups: ["apps"]
  resources: ["deployments"]
  verbs: ["get", "list", "watch"]
EOF

# Bind role to service account
kubectl create clusterrolebinding ats-cluster-role-binding \
  --clusterrole=ats-cluster-role \
  --serviceaccount=ats-prod:ats-service-account
```

## Database Setup

### TimescaleDB Installation

```bash
# Add TimescaleDB Helm repository
helm repo add timescale https://charts.timescale.com/
helm repo update

# Install TimescaleDB in each environment
helm install timescaledb timescale/timescaledb-single \
  --namespace ats-prod \
  --set credentials.postgres.password=secure-password \
  --set persistentVolumes.data.size=100Gi \
  --set persistentVolumes.wal.size=20Gi
```

### Database Migration

```bash
# Create migration job
cat <<EOF | kubectl apply -f -
apiVersion: batch/v1
kind: Job
metadata:
  name: db-migration
  namespace: ats-prod
spec:
  template:
    spec:
      containers:
      - name: migration
        image: ghcr.io/jianjun00/ats-genai:latest
        command: ["python", "src/db/setup_trading_db.py"]
        env:
        - name: DATABASE_URL
          valueFrom:
            secretKeyRef:
              name: ats-secrets
              key: database-url
      restartPolicy: OnFailure
EOF
```

## Monitoring Setup

### Prometheus and Grafana

```bash
# Add Prometheus Helm repository
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
helm repo update

# Install Prometheus stack
helm install prometheus prometheus-community/kube-prometheus-stack \
  --namespace monitoring \
  --create-namespace \
  --set grafana.adminPassword=admin123

# Access Grafana
kubectl port-forward svc/prometheus-grafana 3000:80 -n monitoring
```

## Security Hardening

### Network Policies

```bash
# Create network policy for production
cat <<EOF | kubectl apply -f -
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: ats-network-policy
  namespace: ats-prod
spec:
  podSelector:
    matchLabels:
      app: ats-api
  policyTypes:
  - Ingress
  - Egress
  ingress:
  - from:
    - namespaceSelector:
        matchLabels:
          name: ingress-nginx
    ports:
    - protocol: TCP
      port: 8080
  egress:
  - to: []
    ports:
    - protocol: TCP
      port: 5432  # Database
    - protocol: TCP
      port: 443   # HTTPS
EOF
```

### Pod Security Standards

```bash
# Apply pod security standards
kubectl label namespace ats-prod pod-security.kubernetes.io/enforce=restricted
kubectl label namespace ats-prod pod-security.kubernetes.io/audit=restricted
kubectl label namespace ats-prod pod-security.kubernetes.io/warn=restricted
```

## Cluster Validation

### Health Checks

```bash
# Verify cluster health
kubectl get nodes
kubectl get pods --all-namespaces
kubectl top nodes
kubectl top pods --all-namespaces

# Check cluster info
kubectl cluster-info
kubectl version

# Verify DNS
kubectl run test-dns --rm -i --tty --image=busybox -- nslookup kubernetes.default
```

### Performance Testing

```bash
# Install cluster performance testing tool
kubectl apply -f https://raw.githubusercontent.com/kubernetes/perf-tests/master/clusterloader2/testing/density/config/pod-startup-latency.yaml

# Run basic performance test
kubectl run performance-test --rm -i --tty --image=busybox -- /bin/sh
```

This cluster setup guide provides comprehensive instructions for establishing robust Kubernetes environments for the ATS GenAI system across all deployment stages.
