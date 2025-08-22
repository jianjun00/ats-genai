# WSL Kubernetes Autostart Setup

This document provides comprehensive instructions for setting up automatic Kubernetes (KinD) startup in WSL after a Windows reboot, along with automated deployment of the Data Agent and monitoring components.

## Components

1. **Startup Script (`start-kubernetes.sh`)**
   - Automatically starts Docker service
   - Creates/recreates KinD cluster with custom configuration
   - Updates kubeconfig with correct API server address and port
   - Creates necessary namespaces
   - Deploys NGINX ingress controller and metrics server
   - Deploys Data Agent and Grafana with placeholder secrets
   - Creates services and ingress for Data Agent and Grafana
   - Provides detailed logging and desktop notifications

2. **Shutdown Script (`stop-kubernetes.sh`)**
   - Gracefully shuts down the KinD cluster
   - Logs resource usage before shutdown
   - Provides desktop notifications

3. **Systemd Service (`wsl-kubernetes.service`)**
   - Ensures Kubernetes starts automatically in WSL
   - Handles dependencies and restart policies
   - Integrates with system logging

4. **Windows Integration**
   - Batch script (`start-wsl-kubernetes.bat`) for basic Windows integration
   - PowerShell script (`start-wsl-kubernetes.ps1`) for advanced Windows integration
   - Automatic startup configuration

## Installation Instructions

### 1. WSL Setup

1. Install the systemd service:
   ```bash
   sudo cp /home/jianjun/ats-genai/scripts/wsl-kubernetes.service /etc/systemd/system/
   sudo systemctl daemon-reload
   sudo systemctl enable wsl-kubernetes.service
   ```

2. Make scripts executable:
   ```bash
   chmod +x /home/jianjun/ats-genai/scripts/start-kubernetes.sh
   chmod +x /home/jianjun/ats-genai/scripts/stop-kubernetes.sh
   ```

3. Test the service:
   ```bash
   sudo systemctl start wsl-kubernetes.service
   ```

### 2. Windows Setup

1. **Basic Setup**: Copy the batch script to your Windows Startup folder:
   - Press `Win+R`, type `shell:startup`, and press Enter
   - Copy `start-wsl-kubernetes.bat` to this folder

2. **Advanced Setup**: Run the PowerShell script once to automatically create the startup shortcut:
   - Right-click `start-wsl-kubernetes.ps1` and select "Run with PowerShell"

## Configuration Options

### Startup Script Configuration

The `start-kubernetes.sh` script has several configurable parameters at the top:

```bash
# Configuration
LOG_FILE="/home/jianjun/ats-genai/logs/kubernetes-startup.log"
CLUSTER_NAME="ats-dev"
NAMESPACE="market-data"
NOTIFY_DESKTOP=true
DOCKER_RETRY_COUNT=5
DOCKER_RETRY_INTERVAL=5
```

Adjust these parameters as needed for your environment.

### WSL Configuration

For optimal performance, configure WSL resource allocation in your `.wslconfig` file:

```ini
[wsl2]
memory=8GB
processors=4
swap=2GB
```

## Accessing Services

After startup, the following services will be available:

1. **Data Agent API**: http://localhost/data-agent/health (health check endpoint)
2. **Grafana Dashboard**: http://localhost/grafana (default credentials: admin/admin)
3. **Data Agent Metrics**: http://localhost/metrics (Prometheus metrics)

You can verify the services are working with these commands:

```bash
# Check Data Agent health
curl http://localhost/data-agent/health

# Check Grafana (should redirect to login page)
curl -I http://localhost/grafana
```

## Troubleshooting

### Common Issues

1. **Connection Refused to API Server**
   - Symptom: `kubectl` commands fail with "connection refused"
   - Solution: Update kubeconfig with correct port using:
     ```bash
     kind get kubeconfig --name=ats-dev > ~/.kube/config
     ```

2. **Docker Not Starting**
   - Symptom: "Docker service failed to start" in logs
   - Solution: Check Docker service status and restart manually:
     ```bash
     sudo service docker status
     sudo service docker restart
     ```

3. **Node in NotReady State**
   - Symptom: `kubectl get nodes` shows NotReady status
   - Solution: Check node conditions and kubelet logs:
     ```bash
     kubectl describe node <node-name>
     docker exec <container-name> journalctl -u kubelet
     ```

### Logs

- Kubernetes startup logs: `/home/jianjun/ats-genai/logs/kubernetes-startup.log`
- Windows startup logs: `%USERPROFILE%\wsl-kubernetes-startup.log`
- Systemd service logs: `journalctl -u wsl-kubernetes.service`

## Security Notes

- The startup script creates placeholder secrets for development purposes
- For production use, replace placeholder secrets with real API keys and credentials
- Consider using a secrets management solution for production environments

## Additional Resources

- [KinD Documentation](https://kind.sigs.k8s.io/docs/user/quick-start/)
- [WSL Documentation](https://docs.microsoft.com/en-us/windows/wsl/)
- [Data Agent Documentation](/home/jianjun/ats-genai/src/market_data/agent/README.md)
