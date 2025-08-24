#!/bin/bash
# Configure WSL to auto-start ATS-DEV environment
# This script sets up automatic startup options

set -e

echo "🔧 Configuring WSL auto-start for ATS-DEV environment..."

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m'

print_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_option() {
    echo -e "${YELLOW}[OPTION]${NC} $1"
}

# Option 1: Add to .bashrc (runs on every terminal)
setup_bashrc_autostart() {
    print_info "Setting up .bashrc auto-start (runs on terminal open)..."
    
    # Backup existing .bashrc
    cp ~/.bashrc ~/.bashrc.backup.$(date +%Y%m%d_%H%M%S)
    
    # Add auto-start to .bashrc
    cat >> ~/.bashrc << 'EOF'

# ATS-DEV Auto-start (added by configure_wsl_autostart.sh)
# Uncomment the line below to enable automatic startup
# /home/jianjun/ats-genai/scripts/setup/auto_start_ats_dev.sh >/dev/null 2>&1 &

# Alias for manual startup
alias start-ats-dev="/home/jianjun/ats-genai/scripts/setup/setup_ats_dev_environment.sh"
alias status-ats-dev="kubectl get all -n ats-dev"
alias logs-ats-dev="kubectl logs -f deployment/postgres -n ats-dev"
EOF
    
    print_success ".bashrc configured with ATS-DEV aliases"
    print_info "To enable auto-start, uncomment the line in ~/.bashrc"
}

# Option 2: Create systemd user service (Linux)
setup_systemd_service() {
    print_info "Setting up systemd user service..."
    
    # Create user systemd directory
    mkdir -p ~/.config/systemd/user
    
    # Create service file
    cat > ~/.config/systemd/user/ats-dev-autostart.service << 'EOF'
[Unit]
Description=ATS-DEV Environment Auto-start
After=network.target

[Service]
Type=oneshot
ExecStart=/home/jianjun/ats-genai/scripts/setup/auto_start_ats_dev.sh
WorkingDirectory=/home/jianjun/ats-genai
User=jianjun
Environment=HOME=/home/jianjun
Environment=PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=default.target
EOF
    
    # Enable the service (but don't start it automatically)
    systemctl --user daemon-reload
    
    print_success "Systemd service created: ats-dev-autostart.service"
    print_info "To enable: systemctl --user enable ats-dev-autostart.service"
    print_info "To start: systemctl --user start ats-dev-autostart.service"
}

# Option 3: Create Windows startup script
setup_windows_startup() {
    print_info "Creating Windows startup script..."
    
    # Create Windows batch file
    cat > /mnt/c/Users/$(whoami)/ats-dev-startup.bat << 'EOF' 2>/dev/null || true
@echo off
echo Starting ATS-DEV environment...
wsl -d Ubuntu -u jianjun -- /home/jianjun/ats-genai/scripts/setup/auto_start_ats_dev.sh
echo ATS-DEV startup complete
EOF
    
    if [ -f "/mnt/c/Users/$(whoami)/ats-dev-startup.bat" ]; then
        print_success "Windows startup script created at C:\\Users\\$(whoami)\\ats-dev-startup.bat"
        print_info "Add this to Windows Startup folder for automatic startup"
    else
        print_info "Could not create Windows startup script (WSL mount not available)"
    fi
}

# Display configuration options
display_options() {
    echo ""
    print_success "🎉 Configuration completed!"
    echo ""
    echo "📋 Available startup options:"
    echo ""
    print_option "1. Manual startup:"
    echo "   start-ats-dev"
    echo ""
    print_option "2. Terminal auto-start (edit ~/.bashrc):"
    echo "   Uncomment the auto-start line in ~/.bashrc"
    echo ""
    print_option "3. Systemd service:"
    echo "   systemctl --user enable ats-dev-autostart.service"
    echo "   systemctl --user start ats-dev-autostart.service"
    echo ""
    print_option "4. Test current setup:"
    echo "   ./scripts/setup/setup_ats_dev_environment.sh"
    echo ""
    print_option "5. Check status:"
    echo "   status-ats-dev"
    echo ""
}

# Main execution
main() {
    echo "🚀 Configuring WSL Auto-start for ATS-DEV"
    echo "========================================="
    
    setup_bashrc_autostart
    setup_systemd_service
    setup_windows_startup
    display_options
    
    print_success "Configuration completed! Choose your preferred startup method."
}

# Run main function
main "$@"