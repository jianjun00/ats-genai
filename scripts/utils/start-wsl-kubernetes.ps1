# WSL Kubernetes Startup PowerShell Script
# This script provides advanced Windows integration for starting Kubernetes in WSL

# Configuration
$wslDistro = "Ubuntu"
$logFile = "$env:USERPROFILE\wsl-kubernetes-startup.log"
$maxRetries = 3
$startupDir = "$env:APPDATA\Microsoft\Windows\Start Menu\Programs\Startup"
$scriptPath = "$PSScriptRoot\start-wsl-kubernetes.bat"
$autoStartEnabled = $true
$notificationsEnabled = $true
$kubernetesTimeout = 120  # seconds

# Create log directory if it doesn't exist
if (-not (Test-Path (Split-Path $logFile))) {
    New-Item -ItemType Directory -Path (Split-Path $logFile) -Force | Out-Null
}

# Function for logging
function Write-Log {
    param([string]$message)
    $timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    Write-Host "[$timestamp] $message"
    Add-Content -Path $logFile -Value "[$timestamp] $message"
}

# Function to show notifications
function Show-Notification {
    param(
        [string]$title,
        [string]$message,
        [string]$icon = "Information"  # Can be Information, Warning, Error
    )

    if ($notificationsEnabled) {
        $notification = New-Object System.Windows.Forms.NotifyIcon
        $notification.Icon = [System.Drawing.SystemIcons]::$icon
        $notification.BalloonTipTitle = $title
        $notification.BalloonTipText = $message
        $notification.Visible = $true
        $notification.ShowBalloonTip(10000)
    }
}

# Load required assemblies for notifications
Add-Type -AssemblyName System.Windows.Forms
Add-Type -AssemblyName System.Drawing

# Check if WSL is installed and available
Write-Log "Checking WSL installation..."
try {
    $wslStatus = wsl --status 2>&1
    if ($LASTEXITCODE -ne 0) {
        Write-Log "ERROR: WSL is not installed or not available"
        Show-Notification "WSL Kubernetes" "WSL is not installed or not available" "Error"
        exit 1
    }
} catch {
    Write-Log "ERROR: WSL is not installed or not available: $_"
    Show-Notification "WSL Kubernetes" "WSL is not installed or not available" "Error"
    exit 1
}

# Check if the specified distribution exists
Write-Log "Checking if WSL distribution '$wslDistro' exists..."
$wslList = wsl --list
if ($wslList -notmatch $wslDistro) {
    Write-Log "ERROR: WSL distribution '$wslDistro' not found"
    Write-Log "Available distributions:"
    Write-Log $wslList
    Show-Notification "WSL Kubernetes" "WSL distribution '$wslDistro' not found" "Error"
    exit 1
}

# Start WSL if it's not already running
Write-Log "Ensuring WSL is running..."
try {
    $wslRunning = wsl -d $wslDistro -e echo "WSL is running" 2>&1
    if ($LASTEXITCODE -ne 0) {
        Write-Log "Starting WSL distribution '$wslDistro'..."
        wsl --start $wslDistro
        Start-Sleep -Seconds 5
    }
} catch {
    Write-Log "Error checking WSL status: $_"
}

# Check if Docker is running in WSL
Write-Log "Checking Docker status in WSL..."
$dockerStatus = wsl -d $wslDistro -e service docker status 2>&1
if ($LASTEXITCODE -ne 0) {
    Write-Log "Docker is not running. Attempting to start..."
    wsl -d $wslDistro -u root -e service docker start
    Start-Sleep -Seconds 5
}

# Start Kubernetes service with retry logic
$retryCount = 0
$success = $false

while ($retryCount -lt $maxRetries -and -not $success) {
    $retryCount++
    
    Write-Log "Starting Kubernetes in WSL (attempt $retryCount/$maxRetries)..."
    $startResult = wsl -d $wslDistro -u root systemctl start wsl-kubernetes.service 2>&1
    
    if ($LASTEXITCODE -eq 0) {
        $success = $true
        break
    } else {
        Write-Log "Failed to start Kubernetes service (attempt $retryCount/$maxRetries)"
        Write-Log "Error: $startResult"
        Start-Sleep -Seconds 10
    }
}

if (-not $success) {
    Write-Log "ERROR: Failed to start Kubernetes service after $maxRetries attempts"
    Show-Notification "WSL Kubernetes" "Failed to start Kubernetes service" "Error"
    exit 1
}

# Wait for Kubernetes to initialize
Write-Log "Waiting for Kubernetes to initialize..."
$startTime = Get-Date
$kubernetesRunning = $false

while ((New-TimeSpan -Start $startTime -End (Get-Date)).TotalSeconds -lt $kubernetesTimeout) {
    $nodeStatus = wsl -d $wslDistro -e kubectl get nodes 2>&1
    if ($LASTEXITCODE -eq 0) {
        $kubernetesRunning = $true
        break
    }
    Write-Log "Kubernetes not ready yet, waiting..."
    Start-Sleep -Seconds 5
}

# Check if Kubernetes is running
if ($kubernetesRunning) {
    Write-Log "SUCCESS: Kubernetes is running!"
    
    # Display cluster information
    Write-Log "Cluster information:"
    $clusterInfo = wsl -d $wslDistro -e kubectl cluster-info
    Write-Log $clusterInfo
    
    # Display node status
    Write-Log "Node status:"
    $nodeStatus = wsl -d $wslDistro -e kubectl get nodes
    Write-Log $nodeStatus
    
    # Check for data-agent deployment
    $dataAgentStatus = wsl -d $wslDistro -e kubectl get pods -n market-data -l app=data-agent 2>&1
    if ($LASTEXITCODE -eq 0) {
        Write-Log "Data Agent status:"
        Write-Log $dataAgentStatus
    } else {
        Write-Log "Data Agent is not deployed or not running"
    }
    
    Show-Notification "WSL Kubernetes" "Kubernetes has started successfully!" "Information"
} else {
    Write-Log "WARNING: Kubernetes did not start within the timeout period"
    Write-Log "Please check the WSL logs for more information:"
    Write-Log "wsl -d $wslDistro -e cat /home/jianjun/ats-genai/logs/kubernetes-startup.log"
    Show-Notification "WSL Kubernetes" "Kubernetes may not have started properly" "Warning"
}

# Create auto-start shortcut if enabled
if ($autoStartEnabled) {
    $shortcutPath = Join-Path -Path $startupDir -ChildPath "WSL-Kubernetes.lnk"
    
    if (-not (Test-Path $shortcutPath)) {
        Write-Log "Creating auto-start shortcut..."
        
        $WshShell = New-Object -ComObject WScript.Shell
        $Shortcut = $WshShell.CreateShortcut($shortcutPath)
        $Shortcut.TargetPath = "cmd.exe"
        $Shortcut.Arguments = "/c `"$scriptPath`""
        $Shortcut.WorkingDirectory = Split-Path -Parent $scriptPath
        $Shortcut.Description = "Start Kubernetes in WSL"
        $Shortcut.IconLocation = "shell32.dll,21"
        $Shortcut.Save()
        
        Write-Log "Auto-start shortcut created at: $shortcutPath"
    } else {
        Write-Log "Auto-start shortcut already exists at: $shortcutPath"
    }
}

Write-Log "WSL Kubernetes startup process completed"
