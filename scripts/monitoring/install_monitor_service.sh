#!/bin/bash
"""
Install WSL System Monitor as a systemd service

This script sets up the WSL system monitor to run automatically as a system service
with automatic restart capabilities and proper logging.
"""

set -e

# Configuration
SERVICE_NAME="ats-wsl-monitor"
SERVICE_USER="jianjun"
SCRIPT_DIR="/home/jianjun/ats-genai-admin/scripts/monitoring"
SLACK_WEBHOOK_URL="${SLACK_WEBHOOK_URL:-}"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}=== Installing ATS WSL System Monitor Service ===${NC}"

# Check if running as root for service installation
if [ "$EUID" -eq 0 ]; then
    echo -e "${RED}Please do not run this script as root. It will use sudo when needed.${NC}"
    exit 1
fi

# Check if Slack webhook URL is provided
if [ -z "$SLACK_WEBHOOK_URL" ]; then
    echo -e "${RED}Error: SLACK_WEBHOOK_URL environment variable is required${NC}"
    echo "Usage: SLACK_WEBHOOK_URL='https://hooks.slack.com/...' $0"
    exit 1
fi

# Validate Slack webhook URL format
if [[ ! "$SLACK_WEBHOOK_URL" =~ ^https://hooks\.slack\.com/services/ ]]; then
    echo -e "${YELLOW}Warning: Slack webhook URL format may be incorrect${NC}"
    echo "Expected format: https://hooks.slack.com/services/..."
fi

# Create systemd service file
echo -e "${YELLOW}Creating systemd service file...${NC}"

sudo tee /etc/systemd/system/${SERVICE_NAME}.service > /dev/null << EOF
[Unit]
Description=ATS WSL System Monitor with Slack Alerts
Documentation=https://github.com/ats-genai-admin/monitoring
After=network-online.target
Wants=network-online.target
StartLimitIntervalSec=0

[Service]
Type=simple
Restart=always
RestartSec=30
User=${SERVICE_USER}
Group=${SERVICE_USER}
WorkingDirectory=${SCRIPT_DIR}

# Environment variables
Environment=PATH=/usr/local/bin:/usr/bin:/bin
Environment=PYTHONPATH=/home/jianjun/ats-genai-admin/src
Environment=SLACK_WEBHOOK_URL=${SLACK_WEBHOOK_URL}

# Command to run
ExecStart=/usr/bin/python3 ${SCRIPT_DIR}/wsl_system_monitor.py \\
    --slack-webhook=\${SLACK_WEBHOOK_URL} \\
    --config-file=${SCRIPT_DIR}/monitor_config.json \\
    --interval=60

# Logging
StandardOutput=journal
StandardError=journal
SyslogIdentifier=${SERVICE_NAME}

# Security settings
NoNewPrivileges=true
PrivateTmp=true
ProtectSystem=strict
ProtectHome=true
ReadWritePaths=/mnt/d/ats-logs

# Resource limits
MemoryMax=256M
CPUQuota=50%

[Install]
WantedBy=multi-user.target
EOF

# Create log rotation configuration
echo -e "${YELLOW}Setting up log rotation...${NC}"

sudo tee /etc/logrotate.d/${SERVICE_NAME} > /dev/null << EOF
/mnt/d/ats-logs/monitoring/*.log {
    daily
    rotate 30
    compress
    delaycompress
    missingok
    notifempty
    create 644 ${SERVICE_USER} ${SERVICE_USER}
    postrotate
        systemctl reload-or-restart ${SERVICE_NAME} || true
    endscript
}
EOF

# Install required Python packages
echo -e "${YELLOW}Installing required Python packages...${NC}"
pip3 install --user psutil requests

# Create monitoring directories
echo -e "${YELLOW}Creating monitoring directories...${NC}"
mkdir -p /mnt/d/ats-logs/monitoring
mkdir -p ${SCRIPT_DIR}

# Set proper permissions
chmod +x ${SCRIPT_DIR}/wsl_system_monitor.py
chown -R ${SERVICE_USER}:${SERVICE_USER} /mnt/d/ats-logs/monitoring

# Reload systemd and enable service
echo -e "${YELLOW}Configuring systemd service...${NC}"
sudo systemctl daemon-reload
sudo systemctl enable ${SERVICE_NAME}

# Test the service
echo -e "${YELLOW}Testing service configuration...${NC}"
sudo systemctl start ${SERVICE_NAME}

# Wait a moment for service to start
sleep 3

# Check service status
if sudo systemctl is-active --quiet ${SERVICE_NAME}; then
    echo -e "${GREEN}✅ Service started successfully!${NC}"
    
    # Send test alert
    echo -e "${YELLOW}Sending test Slack alert...${NC}"
    python3 ${SCRIPT_DIR}/wsl_system_monitor.py \\
        --slack-webhook="${SLACK_WEBHOOK_URL}" \\
        --config-file=${SCRIPT_DIR}/monitor_config.json \\
        --test-alert
    
    echo -e "${GREEN}=== Installation Complete ===${NC}"
    echo -e "Service Status: ${GREEN}ACTIVE${NC}"
    echo -e "View logs: ${YELLOW}journalctl -u ${SERVICE_NAME} -f${NC}"
    echo -e "Stop service: ${YELLOW}sudo systemctl stop ${SERVICE_NAME}${NC}"
    echo -e "Start service: ${YELLOW}sudo systemctl start ${SERVICE_NAME}${NC}"
    echo -e "Restart service: ${YELLOW}sudo systemctl restart ${SERVICE_NAME}${NC}"
    echo -e "Disable service: ${YELLOW}sudo systemctl disable ${SERVICE_NAME}${NC}"
    
else
    echo -e "${RED}❌ Service failed to start!${NC}"
    echo -e "Check logs: ${YELLOW}journalctl -u ${SERVICE_NAME} -n 50${NC}"
    sudo systemctl status ${SERVICE_NAME}
    exit 1
fi

# Show final status
echo -e "\\n${GREEN}=== Service Information ===${NC}"
sudo systemctl status ${SERVICE_NAME} --no-pager -l