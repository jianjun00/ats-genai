#!/bin/bash
# Setup Minikube to start automatically when WSL starts

set -e

# Define colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo -e "${YELLOW}Setting up Minikube autostart for WSL...${NC}"

# Check if running in WSL
if ! grep -q Microsoft /proc/version; then
    echo -e "${RED}This script is intended to be run in WSL. Exiting.${NC}"
    exit 1
fi

# Check if minikube is installed
if ! command -v minikube &> /dev/null; then
    echo -e "${RED}Minikube is not installed. Please install it first.${NC}"
    exit 1
fi

# Get the full path to minikube
MINIKUBE_PATH=$(which minikube)
echo -e "${GREEN}Found minikube at: ${MINIKUBE_PATH}${NC}"

# Create systemd service file
SERVICE_FILE="$HOME/.config/systemd/user/minikube.service"
mkdir -p "$HOME/.config/systemd/user"

cat > "$SERVICE_FILE" << EOL
[Unit]
Description=Minikube Kubernetes Cluster
After=network.target

[Service]
Type=oneshot
ExecStart=${MINIKUBE_PATH} start
ExecStop=${MINIKUBE_PATH} stop
RemainAfterExit=yes

[Install]
WantedBy=default.target
EOL

echo -e "${GREEN}Created systemd user service file at ${SERVICE_FILE}${NC}"

# Enable and start the service
systemctl --user daemon-reload
systemctl --user enable minikube.service
echo -e "${GREEN}Enabled minikube service${NC}"

# Check if we can start the service now
echo -e "${YELLOW}Starting minikube service...${NC}"
if systemctl --user start minikube.service; then
    echo -e "${GREEN}Minikube service started successfully!${NC}"
else
    echo -e "${RED}Failed to start minikube service. You may need to start it manually.${NC}"
fi

# Add lingering for the user to allow services to run without being logged in
echo -e "${YELLOW}Setting up lingering to allow services to run without being logged in...${NC}"
sudo loginctl enable-linger $(whoami)
echo -e "${GREEN}Lingering enabled for user $(whoami)${NC}"

# Create a WSL startup script
STARTUP_SCRIPT="$HOME/.wsl-startup.sh"
cat > "$STARTUP_SCRIPT" << EOL
#!/bin/bash
# WSL startup script
systemctl --user start minikube.service
EOL
chmod +x "$STARTUP_SCRIPT"
echo -e "${GREEN}Created WSL startup script at ${STARTUP_SCRIPT}${NC}"

# Add to .profile to run on login
if ! grep -q ".wsl-startup.sh" "$HOME/.profile"; then
    echo -e "\n# Start WSL services\nif [ -f \"$HOME/.wsl-startup.sh\" ]; then\n    $HOME/.wsl-startup.sh\nfi" >> "$HOME/.profile"
    echo -e "${GREEN}Added startup script to .profile${NC}"
fi

echo -e "${GREEN}Setup complete! Minikube will now start automatically when WSL starts.${NC}"
echo -e "${YELLOW}Note: You may need to restart WSL for all changes to take effect.${NC}"
echo -e "${YELLOW}To restart WSL, run 'wsl --shutdown' in PowerShell/CMD and then restart your WSL terminal.${NC}"
