#!/bin/bash
# Automated Minikube GPU Setup for Docker Desktop + WSL2
# This script starts minikube and automatically configures NVIDIA GPU support

set -e

echo "🚀 Starting Minikube with NVIDIA GPU support..."

# Stop existing minikube if running
minikube stop 2>/dev/null || true

# Start minikube with Docker driver
minikube start --driver=docker

# Wait for minikube to be ready
echo "⏳ Waiting for minikube to be ready..."
kubectl wait --for=condition=Ready nodes --all --timeout=300s

# Install NVIDIA Container Toolkit inside minikube
echo "📦 Installing NVIDIA Container Toolkit in minikube..."

# Add NVIDIA package repository
minikube ssh -- 'sudo rm -f /usr/share/keyrings/nvidia-container-toolkit-keyring.gpg'
minikube ssh -- 'curl -fsSL https://nvidia.github.io/libnvidia-container/gpgkey | sudo gpg --dearmor -o /usr/share/keyrings/nvidia-container-toolkit-keyring.gpg'
minikube ssh -- 'curl -s -L https://nvidia.github.io/libnvidia-container/stable/deb/nvidia-container-toolkit.list | sed "s#deb https://#deb [signed-by=/usr/share/keyrings/nvidia-container-toolkit-keyring.gpg] https://#g" | sudo tee /etc/apt/sources.list.d/nvidia-container-toolkit.list'

# Update and install
minikube ssh -- 'sudo DEBIAN_FRONTEND=noninteractive apt-get update'
minikube ssh -- 'sudo DEBIAN_FRONTEND=noninteractive apt-get install -y nvidia-container-toolkit nvidia-container-runtime'

# Configure Docker to use NVIDIA runtime
echo "🔧 Configuring Docker with NVIDIA runtime..."
minikube ssh -- 'sudo nvidia-ctk runtime configure --runtime=docker'

# Restart Docker
echo "🔄 Restarting Docker with NVIDIA runtime..."
minikube ssh -- 'sudo systemctl restart docker'

# Wait for system to stabilize
sleep 10

# Test GPU access inside minikube
echo "🧪 Testing GPU access in minikube..."
minikube ssh -- 'docker run --rm --gpus all ubuntu:20.04 nvidia-smi' || {
    echo "⚠️  Direct GPU test failed, continuing with device plugin setup..."
}

# Now set up the device plugin
echo "🎯 Setting up NVIDIA Device Plugin..."

# Disable the default addon
minikube addons disable nvidia-device-plugin 2>/dev/null || true

# Clean up any existing device plugins
kubectl delete daemonset nvidia-device-plugin-daemonset -n kube-system --ignore-not-found=true
kubectl delete pods -n kube-system -l name=nvidia-device-plugin-ds --ignore-not-found=true

# Wait for cleanup
sleep 10

# Create custom device plugin that should work now
cat <<EOF | kubectl apply -f -
apiVersion: apps/v1
kind: DaemonSet
metadata:
  name: nvidia-device-plugin-daemonset
  namespace: kube-system
spec:
  selector:
    matchLabels:
      name: nvidia-device-plugin-ds
  updateStrategy:
    type: RollingUpdate
  template:
    metadata:
      labels:
        name: nvidia-device-plugin-ds
    spec:
      tolerations:
      - operator: Exists
        effect: NoSchedule
      priorityClassName: "system-node-critical"
      containers:
      - image: nvcr.io/nvidia/k8s-device-plugin:v0.17.2
        name: nvidia-device-plugin-ctr
        args: ["--fail-on-init-error=false", "--mig-strategy=none"]
        securityContext:
          allowPrivilegeEscalation: false
          capabilities:
            drop: ["ALL"]
        volumeMounts:
        - name: device-plugin
          mountPath: /var/lib/kubelet/device-plugins
        env:
        - name: NVIDIA_VISIBLE_DEVICES
          value: "all"
        - name: NVIDIA_DRIVER_CAPABILITIES
          value: "all"
      volumes:
      - name: device-plugin
        hostPath:
          path: /var/lib/kubelet/device-plugins
      hostNetwork: true
      hostPID: true
EOF

echo "⏳ Waiting for NVIDIA device plugin to start..."
kubectl wait --for=condition=ready pod -l name=nvidia-device-plugin-ds -n kube-system --timeout=120s

# Wait for device registration
sleep 15

echo "🔍 Checking GPU availability in Kubernetes..."
GPU_COUNT=$(kubectl get nodes -o yaml | grep 'nvidia.com/gpu:' | awk -F': ' '{sum+=$2} END {print sum+0}')

if [ "$GPU_COUNT" -gt 0 ]; then
    echo "✅ SUCCESS: $GPU_COUNT GPU(s) detected in Kubernetes!"
    
    # Create and run GPU test
    echo "🧪 Testing GPU access in Kubernetes..."
    cat <<EOF | kubectl apply -f -
apiVersion: batch/v1
kind: Job
metadata:
  name: gpu-test-job
spec:
  template:
    spec:
      restartPolicy: Never
      containers:
      - name: gpu-test
        image: ubuntu:20.04
        command: ["/bin/bash", "-c", "nvidia-smi && echo 'GPU test successful!'"]
        resources:
          limits:
            nvidia.com/gpu: 1
EOF
    
    echo "⏳ Waiting for GPU test to complete..."
    kubectl wait --for=condition=complete job/gpu-test-job --timeout=120s
    
    echo "📄 GPU test results:"
    kubectl logs job/gpu-test-job
    
    # Clean up
    kubectl delete job gpu-test-job
    
    echo ""
    echo "🎉 MINIKUBE GPU SETUP COMPLETED SUCCESSFULLY!"
    echo ""
    echo "💡 Usage examples:"
    echo "   # Quick test:"
    echo "   kubectl run gpu-pod --image=ubuntu:20.04 --rm -it --restart=Never --overrides='{\"spec\":{\"containers\":[{\"name\":\"gpu-pod\",\"image\":\"ubuntu:20.04\",\"command\":[\"nvidia-smi\"],\"resources\":{\"limits\":{\"nvidia.com/gpu\":\"1\"}}}]}}'"
    echo ""
    echo "   # YAML deployment:"
    echo "   resources:"
    echo "     limits:"
    echo "       nvidia.com/gpu: 1"
    echo ""
    echo "🔧 To restart with GPU support anytime:"
    echo "   $0"
    
else
    echo "❌ SETUP FAILED: No GPUs detected in Kubernetes"
    echo ""
    echo "📋 Checking device plugin logs:"
    kubectl logs -l name=nvidia-device-plugin-ds -n kube-system --tail=30
    
    echo ""
    echo "🔧 Manual troubleshooting commands:"
    echo "   minikube ssh -- 'docker run --rm --gpus all ubuntu:20.04 nvidia-smi'"
    echo "   kubectl describe node minikube | grep -A 10 -B 10 nvidia"
    echo "   kubectl get pods -n kube-system | grep nvidia"
    
    exit 1
fi