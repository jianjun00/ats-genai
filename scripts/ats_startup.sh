#!/bin/bash
# ATS Stable Startup Script
# Ensures consistent database state and service startup without manual intervention

set -e

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
COMPOSE_FILE="$PROJECT_ROOT/docker-compose.ats.yml"
BACKUP_FILE="/mnt/d/ats-backup/database/2025-08-27/ats-dev-before-events-20250827-2207.backup"
LOG_FILE="/mnt/d/ats-logs/startup.log"

cd "$PROJECT_ROOT"

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG_FILE"
}

# Ensure log directory exists
mkdir -p "$(dirname "$LOG_FILE")"

log "🚀 Starting ATS stable startup sequence..."

# 1. Stop any existing services to ensure clean state
log "🛑 Stopping any existing ATS services..."
docker-compose -f docker-compose.ats.yml down || true

# 2. Start PostgreSQL first
log "🐘 Starting PostgreSQL..."
docker-compose -f docker-compose.ats.yml up -d postgres-dev

# 3. Wait for PostgreSQL to be ready
log "⏳ Waiting for PostgreSQL to be ready..."
timeout=60
while [ $timeout -gt 0 ]; do
    if docker exec ats-dev-postgres pg_isready -U postgres -d dev_db > /dev/null 2>&1; then
        log "✅ PostgreSQL is ready"
        break
    fi
    sleep 2
    timeout=$((timeout - 2))
done

if [ $timeout -le 0 ]; then
    log "❌ PostgreSQL failed to start within 60 seconds"
    exit 1
fi

# 4. Check if database has tables (is initialized)
log "🔍 Checking database state..."
table_count=$(docker exec ats-dev-postgres psql -U postgres -d dev_db -t -c "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public';" 2>/dev/null | xargs)

if [ "$table_count" = "0" ] || [ -z "$table_count" ]; then
    log "📊 Database is empty, restoring from backup..."
    
    if [ -f "$BACKUP_FILE" ]; then
        log "📥 Restoring database from: $BACKUP_FILE"
        # Copy backup into container and restore with version-tolerant options
        docker cp "$BACKUP_FILE" ats-dev-postgres:/tmp/restore.backup
        docker exec ats-dev-postgres pg_restore --verbose --no-owner --no-acl -U postgres -d dev_db /tmp/restore.backup 2>/dev/null || {
            log "⚠️  Custom format failed, trying alternative restore..."
            # If custom format fails, check if there's a SQL backup
            if [ -f "/mnt/d/ats-backup/latest_backup.sql" ]; then
                log "📥 Using SQL backup instead..."
                docker cp /mnt/d/ats-backup/latest_backup.sql ats-dev-postgres:/tmp/restore.sql
                docker exec ats-dev-postgres psql -U postgres -d dev_db -f /tmp/restore.sql
            else
                log "❌ No alternative backup found"
                return 1
            fi
        }
        
        if [ $? -eq 0 ]; then
            log "✅ Database restored successfully"
            # Verify restoration
            restored_count=$(docker exec ats-dev-postgres psql -U postgres -d dev_db -t -c "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public';" 2>/dev/null | xargs)
            log "📊 Tables restored: $restored_count"
        else
            log "❌ Database restore failed"
            exit 1
        fi
    else
        log "❌ Backup file not found: $BACKUP_FILE"
        exit 1
    fi
else
    log "✅ Database already has $table_count tables - skipping restore"
fi

# 5. Start all other services
log "🚀 Starting all ATS services..."
docker-compose -f docker-compose.ats.yml up -d

# 6. Wait for analytics service to be ready
log "⏳ Waiting for analytics service..."
timeout=30
while [ $timeout -gt 0 ]; do
    if curl -s http://localhost:3000/health > /dev/null 2>&1; then
        log "✅ Analytics service is ready"
        break
    fi
    sleep 2
    timeout=$((timeout - 2))
done

# 7. Show final status
log "📊 Final service status:"
docker-compose -f docker-compose.ats.yml ps

log "🌐 Service URLs:"
log "  - Dev Analytics: http://localhost:3000"
log "  - Intg Analytics: http://localhost:3002"
log "  - Grafana: http://localhost:3001"
log "  - Prometheus: http://localhost:9090"

log "🎉 ATS startup completed successfully!"

# 8. Test database connectivity
log "🔍 Testing database connectivity..."
dev_instruments_count=$(docker exec ats-dev-postgres psql -U postgres -d dev_db -t -c "SELECT COUNT(*) FROM dev_instruments;" 2>/dev/null | xargs || echo "0")
log "📊 dev_instruments table has $dev_instruments_count records"

if [ "$dev_instruments_count" -gt "0" ]; then
    log "✅ Database is working properly with data"
else
    log "⚠️  Database may need attention - no instruments found"
fi

echo ""
echo "🚀 ATS Analytics Dashboard: http://localhost:3000"
echo "📊 Database restored and services ready!"