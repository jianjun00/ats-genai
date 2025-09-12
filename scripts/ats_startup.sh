#!/bin/bash
# ATS Complete Startup Script
# Brings up both ATS-DEV and ATS-INTG environments without backup restoration

set -e

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
LOG_FILE="/mnt/d/ats-logs/startup.log"

cd "$PROJECT_ROOT"

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG_FILE"
}

# Ensure log directory exists
mkdir -p "$(dirname "$LOG_FILE")"

log "🚀 Starting ATS complete startup sequence..."

# 1. Stop any existing services to ensure clean state
log "🛑 Stopping any existing ATS services..."
docker-compose -f docker-compose.ats.yml down || true
docker-compose -f docker-compose.intg-jobs.yml down || true

# 2. Start ATS-DEV PostgreSQL (uses postgres-data-new volume)
log "🐘 Starting ATS-DEV PostgreSQL..."
docker-compose -f docker-compose.ats.yml up -d postgres-dev

# 3. Start ATS-INTG PostgreSQL (uses postgres-intg-data volume)
log "🐘 Starting ATS-INTG PostgreSQL..."
docker-compose -f docker-compose.ats.yml up -d postgres-intg

# 4. Wait for both databases to be ready
log "⏳ Waiting for ATS-DEV PostgreSQL to be ready..."
timeout=60
while [ $timeout -gt 0 ]; do
    if docker exec ats-dev-postgres pg_isready -U postgres -d dev_db > /dev/null 2>&1; then
        log "✅ ATS-DEV PostgreSQL is ready"
        break
    fi
    sleep 2
    timeout=$((timeout - 2))
done

if [ $timeout -le 0 ]; then
    log "❌ ATS-DEV PostgreSQL failed to start within 60 seconds"
    exit 1
fi

log "⏳ Waiting for ATS-INTG PostgreSQL to be ready..."
timeout=60
while [ $timeout -gt 0 ]; do
    if docker exec ats-intg-postgres pg_isready -U postgres -d intg_db > /dev/null 2>&1; then
        log "✅ ATS-INTG PostgreSQL is ready"
        break
    fi
    sleep 2
    timeout=$((timeout - 2))
done

if [ $timeout -le 0 ]; then
    log "❌ ATS-INTG PostgreSQL failed to start within 60 seconds"
    exit 1
fi

# 5. Check database state (no restoration - manual initialization required)
log "🔍 Checking ATS-DEV database state..."
dev_table_count=$(docker exec ats-dev-postgres psql -U postgres -d dev_db -t -c "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public';" 2>/dev/null | xargs)
log "📊 ATS-DEV database has $dev_table_count tables"

log "🔍 Checking ATS-INTG database state..."
intg_table_count=$(docker exec ats-intg-postgres psql -U postgres -d intg_db -t -c "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public';" 2>/dev/null | xargs)
log "📊 ATS-INTG database has $intg_table_count tables"

# 6. Start all ATS-DEV and ATS-INTG services
log "🚀 Starting all ATS-DEV services..."
docker-compose -f docker-compose.ats.yml up -d

log "🚀 Starting all ATS-INTG services..."
docker-compose -f docker-compose.intg-jobs.yml up -d

# 7. Wait for analytics services to be ready
log "⏳ Waiting for ATS-DEV analytics service..."
timeout=60
while [ $timeout -gt 0 ]; do
    if curl -s http://localhost:3000/health > /dev/null 2>&1; then
        log "✅ ATS-DEV analytics service is ready"
        break
    fi
    sleep 2
    timeout=$((timeout - 2))
done

log "⏳ Waiting for ATS-INTG analytics service..."
timeout=60
while [ $timeout -gt 0 ]; do
    if curl -s http://localhost:4000/health > /dev/null 2>&1; then
        log "✅ ATS-INTG analytics service is ready"
        break
    fi
    sleep 2
    timeout=$((timeout - 2))
done

# 8. Show final status
log "📊 Final service status:"
echo "ATS-DEV Services:"
docker-compose -f docker-compose.ats.yml ps
echo ""
echo "ATS-INTG Services:"
docker-compose -f docker-compose.intg-jobs.yml ps

log "🌐 Service URLs:"
log "  - ATS-DEV Analytics: http://localhost:3000"
log "  - ATS-DEV EDA: http://localhost:3000/eda"
log "  - ATS-INTG Analytics: http://localhost:4000"
log "  - ATS-INTG EDA: http://localhost:4000/eda"

# 9. Test database connectivity (no restoration warnings)
log "🔍 Testing database connectivity..."
dev_datasets_count=$(docker exec ats-dev-postgres psql -U postgres -d dev_db -t -c "SELECT COUNT(*) FROM dev_training_datasets;" 2>/dev/null | xargs || echo "0")
intg_datasets_count=$(docker exec ats-intg-postgres psql -U postgres -d intg_db -t -c "SELECT COUNT(*) FROM intg_training_datasets;" 2>/dev/null | xargs || echo "0")

log "📊 ATS-DEV training datasets: $dev_datasets_count"
log "📊 ATS-INTG training datasets: $intg_datasets_count"

log "🎉 ATS startup completed successfully!"

echo ""
echo "🚀 ATS Services Ready:"
echo "  - DEV Analytics: http://localhost:3000"
echo "  - INTG Analytics: http://localhost:4000"
echo "📊 Use proper migration/population scripts to initialize data if needed"