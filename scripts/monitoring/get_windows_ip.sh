#!/bin/bash
# Get Windows host IP address from WSL

echo "🔍 Finding Windows host IP address..."

WSL_IP=$(hostname -I | awk '{print $1}')
echo "WSL IP: $WSL_IP"

# Method 1: Default gateway (most reliable)
DEFAULT_GW=$(ip route | grep default | awk '{print $3}' | head -1)
if [ -n "$DEFAULT_GW" ]; then
    echo "Default Gateway: $DEFAULT_GW"
    
    # Test if gateway responds
    if ping -c 1 -W 2 "$DEFAULT_GW" &>/dev/null; then
        echo "✅ Windows IP (via gateway): $DEFAULT_GW"
        WINDOWS_IP="$DEFAULT_GW"
    else
        echo "⚠️  Gateway $DEFAULT_GW not responding"
    fi
fi

# Method 2: Infer from WSL subnet (backup method)
if [ -z "$WINDOWS_IP" ]; then
    WSL_SUBNET=$(echo $WSL_IP | cut -d'.' -f1-3)
    INFERRED_IP="${WSL_SUBNET}.1"
    echo "Inferred Windows IP: $INFERRED_IP"
    
    # Test inferred IP
    if ping -c 1 -W 2 "$INFERRED_IP" &>/dev/null; then
        echo "✅ Windows IP (inferred): $INFERRED_IP"
        WINDOWS_IP="$INFERRED_IP"
    else
        echo "⚠️  Inferred IP $INFERRED_IP not responding"
    fi
fi

# Method 3: Try common gateway IPs
if [ -z "$WINDOWS_IP" ]; then
    echo "Trying common gateway IPs..."
    for ip in "192.168.1.1" "192.168.0.1" "10.0.0.1" "172.16.0.1"; do
        echo "  Testing $ip..."
        if ping -c 1 -W 1 "$ip" &>/dev/null; then
            echo "✅ Windows IP (common gateway): $ip"
            WINDOWS_IP="$ip"
            break
        fi
    done
fi

# Method 4: Use PowerShell via wsl.exe (if available)
if [ -z "$WINDOWS_IP" ] && command -v powershell.exe &>/dev/null; then
    echo "Trying PowerShell method..."
    PS_IP=$(powershell.exe -Command "(Get-NetIPAddress -AddressFamily IPv4 | Where-Object {$_.InterfaceAlias -like '*Wi-Fi*' -or $_.InterfaceAlias -like '*Ethernet*'} | Where-Object {$_.AddressState -eq 'Preferred'} | Select-Object -First 1).IPAddress" 2>/dev/null | tr -d '\r\n')
    if [ -n "$PS_IP" ] && [ "$PS_IP" != "" ]; then
        echo "✅ Windows IP (via PowerShell): $PS_IP"
        WINDOWS_IP="$PS_IP"
    fi
fi

echo ""
echo "🎯 Results:"
echo "==========="
if [ -n "$WINDOWS_IP" ]; then
    echo "✅ Windows Host IP: $WINDOWS_IP"
    echo ""
    echo "🌐 External Access URLs:"
    echo "  Grafana:           http://$WINDOWS_IP:3000"
    echo "  PostgreSQL Metrics: http://$WINDOWS_IP:8001/metrics"
    echo "  Prometheus:        http://$WINDOWS_IP:9090"
    echo "  Data Agent:        http://$WINDOWS_IP:8000/metrics"
    echo ""
    echo "📝 Next step: Run Windows port forwarding script as Administrator:"
    echo "   PowerShell -ExecutionPolicy Bypass -File scripts\\monitoring\\setup_wsl_port_forwarding.ps1"
else
    echo "❌ Could not automatically determine Windows IP"
    echo ""
    echo "🔧 Manual steps:"
    echo "1. On Windows, open Command Prompt and run: ipconfig"
    echo "2. Look for your network adapter (Wi-Fi or Ethernet)"
    echo "3. Use the IPv4 address shown"
    echo "4. Then run the port forwarding script with that IP"
fi

echo ""
echo "🔍 Network debugging info:"
echo "WSL IP: $WSL_IP"
echo "Default route: $(ip route | grep default)"
echo "Network interfaces:"
ip addr show | grep -E "inet.*scope global" | sed 's/^/  /'