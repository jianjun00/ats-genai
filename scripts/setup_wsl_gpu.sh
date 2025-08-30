#!/bin/bash
# Setup GPU access in WSL2 for NVIDIA GPUs

echo "Setting up NVIDIA GPU support in WSL2..."

# Remove any existing NVIDIA packages to avoid conflicts
sudo apt-get remove --purge '^nvidia-.*' -y
sudo apt-get remove --purge '^libnvidia-.*' -y
sudo apt-get remove --purge '^cuda-.*' -y

# Update system
sudo apt-get update

# Add NVIDIA package repositories
wget https://developer.download.nvidia.com/compute/cuda/repos/wsl-ubuntu/x86_64/cuda-wsl-ubuntu.pin
sudo mv cuda-wsl-ubuntu.pin /etc/apt/preferences.d/cuda-repository-pin-600

sudo apt-key adv --fetch-keys https://developer.download.nvidia.com/compute/cuda/repos/wsl-ubuntu/x86_64/7fa2af80.pub
echo "deb https://developer.download.nvidia.com/compute/cuda/repos/wsl-ubuntu/x86_64/ /" | sudo tee /etc/apt/sources.list.d/cuda-wsl-ubuntu.list

# Update package list
sudo apt-get update

# Install CUDA toolkit for WSL2 (this is different from Windows CUDA)
sudo apt-get install cuda-toolkit-12-2 -y

# Install NVIDIA container toolkit for Docker
distribution=$(. /etc/os-release;echo $ID$VERSION_ID) \
    && curl -fsSL https://nvidia.github.io/libnvidia-container/gpgkey | sudo gpg --dearmor -o /usr/share/keyrings/nvidia-container-toolkit-keyring.gpg \
    && curl -s -L https://nvidia.github.io/libnvidia-container/experimental/$distribution/libnvidia-container.list | \
        sed 's#deb https://#deb [signed-by=/usr/share/keyrings/nvidia-container-toolkit-keyring.gpg] https://#g' | \
        sudo tee /etc/apt/sources.list.d/nvidia-container-toolkit.list

sudo apt-get update
sudo apt-get install nvidia-container-toolkit -y

# Configure Docker to use NVIDIA runtime
sudo nvidia-ctk runtime configure --runtime=docker
sudo systemctl restart docker

# Add current user to docker group if not already added
sudo usermod -aG docker $USER

echo "GPU setup complete! Please restart WSL2 to apply changes:"
echo "  1. Exit WSL2 completely"
echo "  2. In Windows PowerShell: wsl --shutdown"
echo "  3. Restart WSL2"
echo "  4. Test with: nvidia-smi"

# Test if setup worked (this might fail until restart)
echo "Testing GPU access..."
nvidia-smi || echo "nvidia-smi not available yet - restart WSL2"