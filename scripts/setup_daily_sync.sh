#!/bin/bash
# Setup ATS Daily Prices Sync with Monitoring

echo "🚀 Setting up ATS Daily Prices Sync for INTG environment..."
echo ""

# Check if running as root or with sudo
if [ "$EUID" -ne 0 ]; then
    echo "⚠️  This script needs to be run with sudo to install systemd services."
    echo "   Usage: sudo ./scripts/setup_daily_sync.sh"
    exit 1
fi

# Install systemd service files
echo "📝 Installing systemd service files..."
cp config/systemd/ats-daily-sync.service /etc/systemd/system/
cp config/systemd/ats-daily-sync.timer /etc/systemd/system/

# Set proper permissions
chmod 644 /etc/systemd/system/ats-daily-sync.service
chmod 644 /etc/systemd/system/ats-daily-sync.timer

# Reload systemd
echo "🔄 Reloading systemd daemon..."
systemctl daemon-reload

# Enable the timer (but don't start it yet)
echo "✅ Enabling daily sync timer..."
systemctl enable ats-daily-sync.timer

# Show timer status
echo "📊 Timer status:"
systemctl list-timers ats-daily-sync.timer

echo ""
echo "🎯 ATS Daily Sync Setup Complete!"
echo ""
echo "📋 What was installed:"
echo "   • Service: /etc/systemd/system/ats-daily-sync.service"
echo "   • Timer:   /etc/systemd/system/ats-daily-sync.timer"
echo "   • Schedule: Monday-Friday at 1:00 AM"
echo "   • Logs: /mnt/d/ats-logs/daily-sync.log"
echo ""
echo "🚀 To start the timer immediately:"
echo "   sudo systemctl start ats-daily-sync.timer"
echo ""
echo "🧪 To test the service manually:"
echo "   sudo systemctl start ats-daily-sync.service"
echo ""
echo "📊 To check status:"
echo "   systemctl status ats-daily-sync.timer"
echo "   systemctl status ats-daily-sync.service"
echo ""
echo "📈 Monitoring:"
echo "   Metrics will be pushed to: http://localhost:9091/metrics"
echo "   Grafana dashboard: http://10.0.0.79:4002/d/a94a33f2-aeea-4b56-93c4-4d22a0cf1c2b"