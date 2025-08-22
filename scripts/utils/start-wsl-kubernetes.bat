@echo off
setlocal enabledelayedexpansion

title WSL Kubernetes Startup

:: Configuration
set "WSL_DISTRO=Ubuntu"
set "LOG_FILE=%USERPROFILE%\wsl-kubernetes-startup.log"
set "MAX_RETRIES=3"

:: Create log file if it doesn't exist
if not exist "%LOG_FILE%" (
    echo WSL Kubernetes Startup Log > "%LOG_FILE%"
    echo ========================== >> "%LOG_FILE%"
)

:: Log function
:log
set "message=%~1"
set "timestamp=%date% %time%"
echo [!timestamp!] !message!
echo [!timestamp!] !message! >> "%LOG_FILE%"
goto :eof

:: Check if WSL is installed and available
wsl --status > nul 2>&1
if %ERRORLEVEL% neq 0 (
    call :log "ERROR: WSL is not installed or not available"
    call :log "Please install WSL using 'wsl --install' or ensure it's properly configured"
    pause
    exit /b 1
)

:: Check if the specified distribution exists
wsl --list | findstr /C:"%WSL_DISTRO%" > nul 2>&1
if %ERRORLEVEL% neq 0 (
    call :log "ERROR: WSL distribution '%WSL_DISTRO%' not found"
    call :log "Available distributions:"
    wsl --list
    pause
    exit /b 1
)

:: Start WSL if it's not already running
call :log "Ensuring WSL is running..."
wsl -d %WSL_DISTRO% -e echo "WSL is running" > nul 2>&1
if %ERRORLEVEL% neq 0 (
    call :log "Starting WSL distribution '%WSL_DISTRO%'..."
    wsl --start %WSL_DISTRO%
    timeout /t 5 > nul
)

:: Check if Docker is running in WSL
call :log "Checking Docker status in WSL..."
wsl -d %WSL_DISTRO% -e service docker status > nul 2>&1
if %ERRORLEVEL% neq 0 (
    call :log "Docker is not running. Attempting to start..."
    wsl -d %WSL_DISTRO% -u root -e service docker start
    timeout /t 5 > nul
)

:: Start Kubernetes service with retry logic
set "retry_count=0"
set "success=false"

:retry_loop
if !retry_count! geq %MAX_RETRIES% goto :retry_failed
set /a "retry_count+=1"

call :log "Starting Kubernetes in WSL (attempt !retry_count!/%MAX_RETRIES%)..."
wsl -d %WSL_DISTRO% -u root systemctl start wsl-kubernetes.service
if %ERRORLEVEL% equ 0 (
    set "success=true"
    goto :check_status
) else (
    call :log "Failed to start Kubernetes service (attempt !retry_count!/%MAX_RETRIES%)"
    timeout /t 10 > nul
    goto :retry_loop
)

:retry_failed
if "%success%"=="false" (
    call :log "ERROR: Failed to start Kubernetes service after %MAX_RETRIES% attempts"
    pause
    exit /b 1
)

:check_status
call :log "Waiting for Kubernetes to initialize..."
timeout /t 20 > nul

:: Check if Kubernetes is running
call :log "Checking Kubernetes status..."
wsl -d %WSL_DISTRO% -e kubectl get nodes > nul 2>&1
if %ERRORLEVEL% equ 0 (
    call :log "SUCCESS: Kubernetes is running!"
    
    :: Display cluster information
    call :log "Cluster information:"
    wsl -d %WSL_DISTRO% -e kubectl cluster-info
    
    :: Display node status
    call :log "Node status:"
    wsl -d %WSL_DISTRO% -e kubectl get nodes
    
    :: Create a notification
    powershell -Command "[reflection.assembly]::loadwithpartialname('System.Windows.Forms'); [reflection.assembly]::loadwithpartialname('System.Drawing'); $notify = new-object system.windows.forms.notifyicon; $notify.icon = [System.Drawing.SystemIcons]::Information; $notify.visible = $true; $notify.showballoontip(10, 'WSL Kubernetes', 'Kubernetes has started successfully!', [system.windows.forms.tooltipicon]::Info)"
) else (
    call :log "WARNING: Kubernetes may not be running properly"
    call :log "Please check the WSL logs for more information:"
    call :log "wsl -d %WSL_DISTRO% -e cat /home/jianjun/ats-genai/logs/kubernetes-startup.log"
)

call :log "WSL Kubernetes startup process completed"

:: Keep the window open if launched directly
if "%1"=="" pause

exit /b 0
