#!/bin/bash
# Setup script for ATS autostart - requires manual sudo execution

echo "🚀 Setting up ATS autostart service..."

# Install systemd service
echo "Installing systemd service..."
sudo cp /home/jianjun/ats-genai-admin/scripts/ats-autostart.service /etc/systemd/system/

# Reload systemd
echo "Reloading systemd..."
sudo systemctl daemon-reload

# Enable service
echo "Enabling ats-autostart service..."
sudo systemctl enable ats-autostart

echo "✅ Setup complete! Service will start on next boot."
echo "To start manually: sudo systemctl start ats-autostart"
echo "To check status: systemctl status ats-autostart"