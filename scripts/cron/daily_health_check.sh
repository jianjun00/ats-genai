#!/bin/bash
# Daily Health Check for ATS Platform
# Monitors critical services, data pipelines, and system health

set -euo pipefail

LOG_PREFIX="[$(date '+%Y-%m-%d %H:%M:%S')] ATS Health Check:"

echo "$LOG_PREFIX Starting daily health check"

# Function to check HTTP endpoint
check_endpoint() {
    local name="$1"
    local url="$2"
    if curl -sf "$url" > /dev/null 2>&1; then
        echo "$LOG_PREFIX ✅ $name is healthy ($url)"
        return 0
    else
        echo "$LOG_PREFIX ❌ $name is down ($url)" >&2
        return 1
    fi
}

# Function to check database connection
check_database() {
    local name="$1"
    local port="$2"
    local password="$3"
    if PGPASSWORD="$password" psql -h localhost -p "$port" -U postgres -d "${name}_db" -c "SELECT 1;" > /dev/null 2>&1; then
        echo "$LOG_PREFIX ✅ $name database is responding (port $port)"
        return 0
    else
        echo "$LOG_PREFIX ❌ $name database is down (port $port)" >&2
        return 1
    fi
}

# Function to check recent file creation
check_recent_files() {
    local path="$1"
    local description="$2"
    local hours_old="${3:-24}"
    
    if find "$path" -type f -mtime -"$(echo "$hours_old/24" | bc -l)" | head -1 | grep -q .; then
        local count=$(find "$path" -type f -mtime -"$(echo "$hours_old/24" | bc -l)" | wc -l)
        echo "$LOG_PREFIX ✅ $description: $count recent files found"
        return 0
    else
        echo "$LOG_PREFIX ⚠️ $description: No recent files in last $hours_old hours" >&2
        return 1
    fi
}

HEALTH_ERRORS=0

# Check ATS-DEV Environment
echo "$LOG_PREFIX Checking ATS-DEV environment..."
check_endpoint "ATS-DEV Analytics" "http://localhost:3000/health" || ((HEALTH_ERRORS++))
check_database "dev" "3432" "dev_password" || ((HEALTH_ERRORS++))

# Check ATS-INTG Environment  
echo "$LOG_PREFIX Checking ATS-INTG environment..."
check_endpoint "ATS-INTG Analytics" "http://localhost:4000/health" || ((HEALTH_ERRORS++))
check_endpoint "ATS-INTG Metrics" "http://localhost:4080/health" || ((HEALTH_ERRORS++))
check_database "intg" "4432" "intg_password" || ((HEALTH_ERRORS++))

# Check FirstRate Data Pipeline
echo "$LOG_PREFIX Checking FirstRate data pipeline..."
TODAY_PATH="/mnt/d/ats-data/firstrate-data/daily/stock"
if [ -d "$TODAY_PATH" ]; then
    check_recent_files "$TODAY_PATH" "FirstRate daily downloads" 36 || ((HEALTH_ERRORS++))
else
    echo "$LOG_PREFIX ❌ FirstRate daily directory not found: $TODAY_PATH" >&2
    ((HEALTH_ERRORS++))
fi

# Check Backup System
echo "$LOG_PREFIX Checking backup system..."
BACKUP_PATH="/mnt/d/ats-backup"
if [ -d "$BACKUP_PATH" ]; then
    check_recent_files "$BACKUP_PATH" "Database backups" 48 || ((HEALTH_ERRORS++))
else
    echo "$LOG_PREFIX ❌ Backup directory not found: $BACKUP_PATH" >&2
    ((HEALTH_ERRORS++))
fi

# Check Disk Space
echo "$LOG_PREFIX Checking disk space..."
DISK_USAGE=$(df /mnt/d | tail -1 | awk '{print $5}' | sed 's/%//')
if [ "$DISK_USAGE" -lt 80 ]; then
    echo "$LOG_PREFIX ✅ Disk usage is healthy: ${DISK_USAGE}%"
else
    echo "$LOG_PREFIX ⚠️ Disk usage is high: ${DISK_USAGE}%" >&2
    ((HEALTH_ERRORS++))
fi

# Check Docker Containers
echo "$LOG_PREFIX Checking Docker containers..."
EXPECTED_CONTAINERS=("ats-dev-postgres" "ats-intg-postgres" "ats-dev-analytics" "ats-intg-analytics")
for container in "${EXPECTED_CONTAINERS[@]}"; do
    if docker ps --format "table {{.Names}}" | grep -q "^${container}$"; then
        echo "$LOG_PREFIX ✅ Container $container is running"
    else
        echo "$LOG_PREFIX ⚠️ Container $container is not running" >&2
        ((HEALTH_ERRORS++))
    fi
done

# Summary
echo "$LOG_PREFIX Health check completed"
if [ $HEALTH_ERRORS -eq 0 ]; then
    echo "$LOG_PREFIX ✅ All systems healthy"
    exit 0
else
    echo "$LOG_PREFIX ❌ Found $HEALTH_ERRORS health issues"
    exit 1
fi