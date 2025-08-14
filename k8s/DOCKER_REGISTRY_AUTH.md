# Docker Desktop Registry Authentication

This document explains how to use Docker Desktop for registry authentication with Kubernetes deployments.

## Prerequisites

1. **Docker Desktop** installed on your Windows machine
2. **Kubernetes** enabled in Docker Desktop settings
3. **kubectl** configured to use the Docker Desktop Kubernetes context

## Setting Up Docker Desktop for Automatic Startup

1. **Open Docker Desktop**
2. **Access Settings**:
   - Click on the gear icon (⚙️) in the top-right corner
3. **Configure Automatic Startup**:
   - Go to "General" settings
   - Check the box for "Start Docker Desktop when you log in"
   - Click "Apply & Restart"
4. **Verify Configuration**:
   - Restart your Windows machine
   - Docker Desktop should start automatically
   - You'll see the Docker icon in your system tray

## Using Docker Desktop for Registry Authentication

### Step 1: Log in to Your Container Registry

```bash
# Log in to Docker Hub
docker login

# Or log in to a specific registry
docker login registry.example.com
```

### Step 2: Create Kubernetes Secret from Docker Credentials

Use the provided script to create a Kubernetes secret from your Docker credentials:

```bash
# For data-agent
./k8s/docker-registry-auth.sh market-data docker.io

# For instrument-agent
./k8s/docker-registry-auth.sh ats-dev docker.io
```

Replace `docker.io` with your specific registry URL if you're using a different registry.

### Step 3: Update Deployment Manifests

Make sure your deployment manifests include the `imagePullSecrets` section:

```yaml
spec:
  imagePullSecrets:
  - name: registry-credentials
  containers:
  - name: your-container
    image: your-registry/your-image:latest
```

## Troubleshooting

1. **Docker Desktop Not Starting Automatically**:
   - Check Windows Task Manager > Startup tab
   - Ensure Docker Desktop is enabled for startup
   - Check Windows Event Viewer for startup errors

2. **Authentication Issues**:
   - Verify Docker login status: `docker login --password-stdin`
   - Check Docker config file: `cat ~/.docker/config.json`
   - Ensure the secret was created: `kubectl get secret registry-credentials -n your-namespace`

3. **Image Pull Errors**:
   - Check pod events: `kubectl describe pod your-pod-name`
   - Verify image name and tag are correct
   - Confirm registry access permissions

## Using with ArgoCD

If you're using ArgoCD for deployment, you'll need to ensure the registry credentials are available to ArgoCD:

1. Create the registry credentials secret in the ArgoCD namespace:
   ```bash
   ./k8s/docker-registry-auth.sh argocd docker.io
   ```

2. Configure ArgoCD to use these credentials for image pulls.
