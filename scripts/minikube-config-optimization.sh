#!/bin/bash
# Minikube Configuration Optimization for WSL2 Environment
# This script optimizes minikube configuration for better stability

set -euo pipefail

MINIKUBE_PROFILE=${MINIKUBE_PROFILE:-minikube}

log() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') $1"
}

# Optimize minikube configuration
optimize_minikube_config() {
    log "Optimizing minikube configuration..."
    
    # Stop current minikube if running
    minikube stop --profile="$MINIKUBE_PROFILE" 2>/dev/null || true
    
    # Delete existing profile to start fresh
    log "Cleaning up existing profile..."
    minikube delete --profile="$MINIKUBE_PROFILE" 2>/dev/null || true
    
    # Create optimized minikube profile
    log "Creating optimized minikube profile..."
    minikube start --profile="$MINIKUBE_PROFILE" \
        --driver=docker \
        --memory=6144 \
        --cpus=3 \
        --disk-size=30g \
        --kubernetes-version=stable \
        --container-runtime=docker \
        --extra-config=kubelet.housekeeping-interval=10s \
        --extra-config=kubelet.image-gc-high-threshold=85 \
        --extra-config=kubelet.image-gc-low-threshold=80 \
        --extra-config=kubelet.minimum-image-ttl-duration=120s \
        --extra-config=kubelet.max-pods=110 \
        --extra-config=kubeadm.pod-network-cidr=10.244.0.0/16 \
        --extra-config=apiserver.enable-admission-plugins=NamespaceLifecycle,LimitRanger,ServiceAccount,DefaultStorageClass,ResourceQuota \
        --bootstrapper=kubeadm
    
    log "Minikube profile created successfully"
}

# Configure WSL2 optimizations
configure_wsl2_optimizations() {
    log "Applying WSL2-specific optimizations..."
    
    # Create or update .wslconfig
    local wsl_config="$HOME/.wslconfig"
    log "Creating optimized .wslconfig at $wsl_config"
    
    cat > "$wsl_config" << 'EOF'
[wsl2]
# Limits VM memory to use no more than 16 GB
memory=16GB

# Sets the VM to use 6 virtual processors
processors=6

# Specify a custom Linux kernel to use with your installed distros
# kernel=C:\\temp\\myCustomKernel

# Sets additional kernel parameters, in this case enabling older Linux base images such as Centos 6
# kernelCommandLine = vsyscall=emulate

# Sets amount of swap storage space to 8GB
swap=8GB

# Sets swapfile path location, default is %USERPROFILE%\AppData\Local\Temp\swap.vhdx
# swapfile=C:\\temp\\wsl-swap.vhdx

# Disable page reporting so WSL retains all allocated memory claimed from Windows and releases none back when free
# pageReporting=false

# Turn on default connection to bind WSL 2 localhost to Windows localhost
localhostforwarding=true

# Disables nested virtualization
# nestedVirtualization=false

# Turns on output console showing contents of dmesg when opening a WSL 2 distro for debugging
# debugConsole=true
EOF
    
    log "WSL2 configuration updated. Restart WSL2 with: wsl --shutdown"
}

# Configure Docker optimizations
configure_docker_optimizations() {
    log "Configuring Docker optimizations..."
    
    # Create Docker daemon configuration for better resource management
    local docker_config_dir="$HOME/.docker"
    mkdir -p "$docker_config_dir"
    
    cat > "$docker_config_dir/daemon.json" << 'EOF'
{
  "log-driver": "json-file",
  "log-opts": {
    "max-size": "10m",
    "max-file": "3"
  },
  "storage-driver": "overlay2",
  "storage-opts": [
    "overlay2.override_kernel_check=true"
  ],
  "default-address-pools": [
    {
      "base": "172.17.0.0/16",
      "size": 24
    }
  ],
  "max-concurrent-downloads": 3,
  "max-concurrent-uploads": 5,
  "live-restore": true,
  "userland-proxy": false,
  "experimental": false,
  "features": {
    "buildkit": true
  }
}
EOF
    
    log "Docker configuration updated"
}

# Set up automatic cleanup cron job
setup_cleanup_cron() {
    log "Setting up automatic cleanup cron job..."
    
    # Create cleanup script
    cat > "$HOME/minikube-daily-cleanup.sh" << 'EOF'
#!/bin/bash
# Daily minikube cleanup script

# Clean up Docker resources
docker system prune -af --filter "until=24h" 2>/dev/null || true

# Clean up minikube cache
minikube cache delete 2>/dev/null || true

# Log cleanup
echo "$(date): Automatic cleanup completed" >> /tmp/minikube-cleanup.log
EOF
    
    chmod +x "$HOME/minikube-daily-cleanup.sh"
    
    # Add to crontab
    (crontab -l 2>/dev/null || echo "") | grep -v "minikube-daily-cleanup" > /tmp/current_cron
    echo "0 2 * * * $HOME/minikube-daily-cleanup.sh" >> /tmp/current_cron
    crontab /tmp/current_cron
    rm /tmp/current_cron
    
    log "Daily cleanup cron job installed"
}

# Configure resource limits for minikube
configure_resource_limits() {
    log "Configuring resource limits..."
    
    # Create resource quota for ats-dev namespace
    kubectl create namespace ats-dev --dry-run=client -o yaml | kubectl apply -f -
    
    cat << 'EOF' | kubectl apply -f -
apiVersion: v1
kind: ResourceQuota
metadata:
  name: compute-resources
  namespace: ats-dev
spec:
  hard:
    requests.cpu: "4"
    requests.memory: 8Gi
    limits.cpu: "8"
    limits.memory: 16Gi
    persistentvolumeclaims: "10"
---
apiVersion: v1
kind: LimitRange
metadata:
  name: compute-limit-range
  namespace: ats-dev
spec:
  limits:
  - default:
      cpu: 500m
      memory: 1Gi
    defaultRequest:
      cpu: 100m
      memory: 256Mi
    type: Container
EOF
    
    log "Resource limits configured for ats-dev namespace"
}

# Main optimization routine
main() {
    case "${1:-all}" in
        "config")
            optimize_minikube_config
            ;;
        "wsl2")
            configure_wsl2_optimizations
            ;;
        "docker")
            configure_docker_optimizations
            ;;
        "cleanup")
            setup_cleanup_cron
            ;;
        "limits")
            configure_resource_limits
            ;;
        "all")
            log "Running complete minikube optimization..."
            configure_wsl2_optimizations
            configure_docker_optimizations
            optimize_minikube_config
            configure_resource_limits
            setup_cleanup_cron
            log "Optimization complete!"
            echo ""
            echo "Next steps:"
            echo "1. Restart WSL2: wsl --shutdown (from Windows)"
            echo "2. Restart Docker Desktop"
            echo "3. Test minikube: minikube status"
            echo "4. Start monitoring: ./minikube-fault-tolerance.sh monitor"
            ;;
        *)
            echo "Usage: $0 {config|wsl2|docker|cleanup|limits|all}"
            echo ""
            echo "Commands:"
            echo "  config   - Optimize minikube configuration"
            echo "  wsl2     - Configure WSL2 optimizations"
            echo "  docker   - Configure Docker optimizations"
            echo "  cleanup  - Setup automatic cleanup"
            echo "  limits   - Configure resource limits"
            echo "  all      - Run all optimizations"
            exit 1
            ;;
    esac
}

main "$@"