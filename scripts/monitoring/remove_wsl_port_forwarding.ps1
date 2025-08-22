# Remove WSL Port Forwarding for ATS Monitoring
# Run this PowerShell script as Administrator on Windows host

Write-Host "Removing WSL port forwarding for ATS monitoring services..." -ForegroundColor Yellow

# Monitoring ports to remove
$ports = @(3000, 8000, 8001, 9090)

# Remove port forwarding rules
Write-Host "Removing port forwarding rules..." -ForegroundColor Yellow
foreach ($port in $ports) {
    try {
        netsh interface portproxy delete v4tov4 listenport=$port
        Write-Host "  ✅ Removed port forwarding for port $port" -ForegroundColor Green
    } catch {
        Write-Host "  ⚠️  Port $port forwarding rule not found" -ForegroundColor Gray
    }
}

# Remove Windows Firewall rules
Write-Host "Removing Windows Firewall rules..." -ForegroundColor Yellow
$services = @("Grafana", "Data Agent Metrics", "PostgreSQL Metrics", "Prometheus")
foreach ($service in $services) {
    try {
        $ruleName = "WSL ATS $service*"
        Remove-NetFirewallRule -DisplayName $ruleName -ErrorAction SilentlyContinue
        Write-Host "  ✅ Removed firewall rule for $service" -ForegroundColor Green
    } catch {
        Write-Host "  ⚠️  Firewall rule for $service not found" -ForegroundColor Gray
    }
}

# Show remaining port forwarding rules
Write-Host "`nRemaining port forwarding rules:" -ForegroundColor Cyan
$remaining = netsh interface portproxy show all
if ($remaining -match "No entries") {
    Write-Host "  No port forwarding rules remaining" -ForegroundColor Green
} else {
    $remaining
}

Write-Host "`n✅ WSL port forwarding cleanup complete!" -ForegroundColor Green