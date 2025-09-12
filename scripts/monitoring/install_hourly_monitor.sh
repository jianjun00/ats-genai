#!/bin/bash
# Install WSL System Monitor as systemd service for hourly Slack alerts

set -e

SERVICE_NAME="ats-wsl-hourly-monitor"
SCRIPT_PATH="/home/jianjun/ats-genai-model/scripts/monitoring/simple_wsl_monitor.py"
SERVICE_FILE="/etc/systemd/system/${SERVICE_NAME}.service"

echo "🔧 Installing ATS WSL Hourly Monitor Service..."

# Create systemd service file
sudo tee "$SERVICE_FILE" > /dev/null << EOF
[Unit]
Description=ATS WSL System Monitor - Hourly Slack Alerts
After=network.target
Wants=network.target

[Service]
Type=simple
User=jianjun
Group=jianjun
WorkingDirectory=/home/jianjun/ats-genai-model/scripts/monitoring
ExecStart=/usr/bin/python3 ${SCRIPT_PATH} --hourly
Restart=always
RestartSec=60
StandardOutput=journal
StandardError=journal

# Environment
Environment=PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
Environment=HOME=/home/jianjun

[Install]
WantedBy=multi-user.target
EOF

# Reload systemd and enable service
sudo systemctl daemon-reload
sudo systemctl enable "${SERVICE_NAME}"

echo "✅ Service installed: ${SERVICE_NAME}"
echo ""
echo "📋 Service Management Commands:"
echo "  Start:   sudo systemctl start ${SERVICE_NAME}"
echo "  Stop:    sudo systemctl stop ${SERVICE_NAME}"
echo "  Status:  sudo systemctl status ${SERVICE_NAME}"
echo "  Logs:    journalctl -u ${SERVICE_NAME} -f"
echo ""
echo "🚀 Starting service now..."
sudo systemctl start "${SERVICE_NAME}"

echo "✅ WSL Hourly Monitor is now running!"
echo "📱 You should receive hourly system status updates in Slack"
echo ""
echo "🔍 Check status with:"
echo "  sudo systemctl status ${SERVICE_NAME}"