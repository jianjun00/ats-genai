# WSL Port Forwarding Setup for ATS Monitoring
# Run this PowerShell script as Administrator on Windows host

Write-Host "Setting up WSL port forwarding for ATS monitoring services..." -ForegroundColor Green

# Get WSL IP address
$wslIP = wsl hostname -I
$wslIP = $wslIP.Trim()
Write-Host "WSL IP Address: $wslIP" -ForegroundColor Yellow

# Monitoring ports to forward
$ports = @(
    @{Port=3000; Service="Grafana"},
    @{Port=8000; Service="Data Agent Metrics"},
    @{Port=8001; Service="PostgreSQL Metrics"},
    @{Port=9090; Service="Prometheus"}
)

# Remove existing port forwarding rules (ignore errors)
Write-Host "Removing existing port forwarding rules..." -ForegroundColor Yellow
foreach ($portInfo in $ports) {
    $port = $portInfo.Port
    try {
        netsh interface portproxy delete v4tov4 listenport=$port | Out-Null
        Write-Host "  Removed existing rule for port $port" -ForegroundColor Gray
    } catch {
        # Ignore errors for non-existing rules
    }
}

# Add new port forwarding rules
Write-Host "Adding new port forwarding rules..." -ForegroundColor Yellow
foreach ($portInfo in $ports) {
    $port = $portInfo.Port
    $service = $portInfo.Service
    
    try {
        netsh interface portproxy add v4tov4 listenport=$port listenaddress=0.0.0.0 connectport=$port connectaddress=$wslIP
        Write-Host "  ✅ Port $port ($service) -> $wslIP" -ForegroundColor Green
    } catch {
        Write-Host "  ❌ Failed to forward port $port ($service)" -ForegroundColor Red
    }
}

# Configure Windows Firewall
Write-Host "Configuring Windows Firewall rules..." -ForegroundColor Yellow
foreach ($portInfo in $ports) {
    $port = $portInfo.Port
    $service = $portInfo.Service
    $ruleName = "WSL ATS $service (Port $port)"
    
    try {
        # Remove existing rule if it exists
        Remove-NetFirewallRule -DisplayName $ruleName -ErrorAction SilentlyContinue
        
        # Add new firewall rule
        New-NetFirewallRule -DisplayName $ruleName -Direction Inbound -Protocol TCP -LocalPort $port -Action Allow | Out-Null
        Write-Host "  ✅ Firewall rule for $service (port $port)" -ForegroundColor Green
    } catch {
        Write-Host "  ❌ Failed to add firewall rule for port $port" -ForegroundColor Red
    }
}

# Show current port forwarding status
Write-Host "`nCurrent port forwarding rules:" -ForegroundColor Cyan
netsh interface portproxy show all

# Get Windows host IP for external access (avoid virtual/cluster IPs)
$hostIP = (Get-NetIPAddress -AddressFamily IPv4 | Where-Object {
    ($_.InterfaceAlias -like "Wi-Fi*" -or $_.InterfaceAlias -like "Ethernet*") -and
    $_.AddressState -eq "Preferred" -and
    $_.IPAddress -notlike "172.*" -and
    $_.IPAddress -notlike "169.254.*"
} | Select-Object -First 1).IPAddress

if (-not $hostIP) {
    # Fallback to any preferred IPv4 address
    $hostIP = (Get-NetIPAddress -AddressFamily IPv4 | Where-Object {
        $_.AddressState -eq "Preferred" -and
        $_.IPAddress -notlike "127.*" -and
        $_.IPAddress -notlike "169.254.*"
    } | Select-Object -First 1).IPAddress
}

Write-Host "`n🎯 ATS Monitoring Access URLs:" -ForegroundColor Green
Write-Host "================================" -ForegroundColor Green
if ($hostIP) {
    Write-Host "External Network Access (from other machines):" -ForegroundColor Cyan
    foreach ($portInfo in $ports) {
        $port = $portInfo.Port
        $service = $portInfo.Service
        Write-Host "  $service : http://$hostIP`:$port" -ForegroundColor White
    }
    Write-Host ""
}

Write-Host "Local WSL Access:" -ForegroundColor Cyan
foreach ($portInfo in $ports) {
    $port = $portInfo.Port
    $service = $portInfo.Service
    Write-Host "  $service : http://localhost:$port" -ForegroundColor White
}

Write-Host "`n📝 Usage Notes:" -ForegroundColor Yellow
Write-Host "- Run this script as Administrator whenever WSL restarts" -ForegroundColor Gray
Write-Host "- WSL IP changes on restart, so port forwarding needs to be reset" -ForegroundColor Gray
Write-Host "- Use 'scripts/monitoring/remove_wsl_port_forwarding.ps1' to clean up" -ForegroundColor Gray

Write-Host "`n✅ WSL port forwarding setup complete!" -ForegroundColor Green