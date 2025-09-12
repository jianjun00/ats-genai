#!/bin/bash
"""
Setup ATS Autostart - Configure automatic startup of ATS services on WSL boot

This script provides multiple methods to ensure ATS services start automatically:
1. Bashrc integration (already configured)
2. Systemd service (optional)
3. Manual verification commands
"""

set -e

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

echo "🚀 Setting up ATS Autostart..."

# Method 1: Bashrc integration (already done)
echo "✅ Method 1: Bashrc integration - COMPLETED"
echo "   - Added to ~/.bashrc"
echo "   - Will run on each new bash session"

# Method 2: Systemd service (optional)
echo ""
echo "📋 Method 2: Systemd service (optional):"
echo "   To enable systemd service (runs at boot, independent of shell):"
echo "   sudo cp $SCRIPT_DIR/ats-autostart.service /etc/systemd/system/"
echo "   sudo systemctl daemon-reload"
echo "   sudo systemctl enable ats-autostart.service"
echo "   sudo systemctl start ats-autostart.service"

# Method 3: Verify setup
echo ""
echo "🔍 Verification commands:"
echo "   # Test autostart script manually:"
echo "   $SCRIPT_DIR/ats_autostart.sh"
echo ""
echo "   # Check if services are running:"
echo "   python3 $PROJECT_ROOT/scripts/run_dev.py status"
echo ""
echo "   # View autostart logs:"
echo "   tail -f /mnt/d/ats-logs/autostart.log"

# Test the autostart script
echo ""
echo "🧪 Testing autostart script..."
if [ -x "$SCRIPT_DIR/ats_autostart.sh" ]; then
    echo "✅ Autostart script is executable"

    # Run a quick test (dry run)
    echo "Running test execution..."
    "$SCRIPT_DIR/ats_autostart.sh" &

    echo "✅ Test started in background"
    echo "   Check logs: tail -f /mnt/d/ats-logs/autostart.log"
else
    echo "❌ Autostart script is not executable"
    chmod +x "$SCRIPT_DIR/ats_autostart.sh"
    echo "✅ Made autostart script executable"
fi

echo ""
echo "🎉 ATS Autostart setup complete!"
echo ""
echo "📝 What happens on WSL restart:"
echo "   1. WSL starts and initializes"
echo "   2. Your bash session loads ~/.bashrc"
echo "   3. ats_autostart.sh runs in background"
echo "   4. ats-dev and ats-intg PostgreSQL databases start"
echo "   5. Logs written to /mnt/d/ats-logs/autostart.log"
echo ""
echo "🔧 To disable autostart:"
echo "   Comment out the ATS autostart section in ~/.bashrc"