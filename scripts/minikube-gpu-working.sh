#!/bin/bash
# Working Minikube GPU Setup for Docker Desktop + WSL2
# This approach mounts WSL2 NVIDIA libraries directly

set -e

echo "🚀 Starting Minikube with working GPU support..."

# Stop existing minikube
minikube stop 2>/dev/null || true
minikube delete 2>/dev/null || true

# Start minikube with proper mounts for GPU support
echo "🔧 Starting minikube with NVIDIA library mounts..."
minikube start \
  --driver=docker \
  --container-runtime=docker \
  --mount-string="/usr/lib/wsl/lib:/usr/local/cuda/lib64" \
  --mount \
  --extra-config=kubelet.feature-gates=DevicePlugins=true

# Wait for startup
echo "⏳ Waiting for minikube to be ready..."
kubectl wait --for=condition=Ready nodes --all --timeout=300s

# Configure environment in minikube
echo "🔧 Configuring GPU environment..."
minikube ssh -- 'sudo mkdir -p /usr/local/nvidia/bin'
minikube ssh -- 'sudo ln -sf /usr/local/cuda/lib64/nvidia-smi /usr/local/nvidia/bin/nvidia-smi'
minikube ssh -- 'sudo ln -sf /usr/local/cuda/lib64/nvidia-smi /usr/bin/nvidia-smi'

# Test GPU access
echo "🧪 Testing GPU access in minikube..."
minikube ssh -- 'nvidia-smi' || {
    echo "⚠️  nvidia-smi not working, checking libraries..."
    minikube ssh -- 'ls -la /usr/local/cuda/lib64/ | head -10'
}

# Create working device plugin
echo "🎯 Creating NVIDIA Device Plugin with proper configuration..."

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
        args: 
        - --mig-strategy=none
        - --fail-on-init-error=false
        - --device-list-strategy=envvar
        - --nvidia-driver-root=/usr/local/cuda
        securityContext:
          allowPrivilegeEscalation: false
          capabilities:
            drop: ["ALL"]
        volumeMounts:
        - name: device-plugin
          mountPath: /var/lib/kubelet/device-plugins
        - name: nvidia-libs
          mountPath: /usr/local/cuda
          readOnly: true
        env:
        - name: NVIDIA_VISIBLE_DEVICES
          value: "all"
        - name: NVIDIA_DRIVER_CAPABILITIES  
          value: "all"
        - name: NVIDIA_DRIVER_ROOT
          value: "/usr/local/cuda"
      volumes:
      - name: device-plugin
        hostPath:
          path: /var/lib/kubelet/device-plugins
      - name: nvidia-libs
        hostPath:
          path: /usr/local/cuda
      hostNetwork: true
EOF

echo "⏳ Waiting for NVIDIA device plugin to be ready..."
kubectl wait --for=condition=ready pod -l name=nvidia-device-plugin-ds -n kube-system --timeout=120s

# Wait for device registration
sleep 20

echo "🔍 Checking GPU availability..."
kubectl get nodes -o yaml | grep nvidia || echo "No nvidia resources found yet..."

# Final test
echo "🧪 Running final GPU test..."
cat <<EOF | kubectl apply -f -
apiVersion: batch/v1
kind: Job
metadata:
  name: gpu-final-test
spec:
  template:
    spec:
      restartPolicy: Never
      containers:
      - name: gpu-test
        image: ubuntu:20.04
        command: ["/bin/bash", "-c", "ls -la /usr/local/cuda/lib64/ && nvidia-smi"]
        volumeMounts:
        - name: nvidia-libs
          mountPath: /usr/local/cuda
          readOnly: true
        env:
        - name: LD_LIBRARY_PATH
          value: "/usr/local/cuda/lib64"
        - name: PATH
          value: "/usr/local/cuda/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
        resources:
          limits:
            nvidia.com/gpu: 1
      volumes:
      - name: nvidia-libs
        hostPath:
          path: /usr/local/cuda
EOF

echo "⏳ Waiting for test completion..."
kubectl wait --for=condition=complete job/gpu-final-test --timeout=120s 2>/dev/null || {
    echo "Test job taking longer, checking status..."
    kubectl get job gpu-final-test
    kubectl describe job gpu-final-test
}

echo "📄 Test results:"
kubectl logs job/gpu-final-test || echo "Could not get logs"

# Cleanup
kubectl delete job gpu-final-test

echo ""
echo "🎉 Minikube GPU setup attempt completed!"
echo "If successful, you can use GPU in pods with:"
echo "  resources:"
echo "    limits:"
echo "      nvidia.com/gpu: 1"