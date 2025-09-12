#!/bin/bash
"""
Setup ATS-INTG Environment
Creates host directories and sets proper permissions for PostgreSQL data persistence.
"""

set -e

echo "🚀 Setting up ATS-INTG environment..."

# Define directories
ATS_BASE_DIR="/mnt/d/ats-data/intg"
POSTGRESQL_DATA_DIR="/mnt/d/ats-data/intg/postgresql"
BACKUP_DIR="/mnt/d/ats-backup/intg"
LOGS_DIR="/mnt/d/ats-logs/intg"

# Create directories if they don't exist
echo "📁 Creating host directories..."

directories=(
    "$ATS_BASE_DIR"
    "$POSTGRESQL_DATA_DIR"
    "$BACKUP_DIR"
    "$LOGS_DIR"
)

for dir in "${directories[@]}"; do
    if [ ! -d "$dir" ]; then
        mkdir -p "$dir"
        echo "  ✅ Created: $dir"
    else
        echo "  ℹ️  Exists: $dir"
    fi
done

# Set proper permissions for PostgreSQL
echo "🔐 Setting PostgreSQL permissions..."

# PostgreSQL runs as user postgres (UID 999) inside container
# Need to ensure host directory is accessible
if [ -d "$POSTGRESQL_DATA_DIR" ]; then
    # If directory is empty, set ownership to allow PostgreSQL container to initialize
    if [ -z "$(ls -A "$POSTGRESQL_DATA_DIR")" ]; then
        echo "  📝 Setting permissions for PostgreSQL initialization..."
        chmod 755 "$POSTGRESQL_DATA_DIR"
        echo "  ✅ PostgreSQL data directory ready for initialization"
    else
        echo "  ℹ️  PostgreSQL data directory already contains data"
        # Check if data directory has proper structure
        if [ -f "$POSTGRESQL_DATA_DIR/PG_VERSION" ]; then
            PG_VERSION=$(cat "$POSTGRESQL_DATA_DIR/PG_VERSION")
            echo "  📊 Existing PostgreSQL version: $PG_VERSION"
        fi
    fi
fi

# Set permissions for backup and logs directories
chmod 755 "$BACKUP_DIR" 2>/dev/null || echo "  ⚠️  Could not set backup directory permissions"
chmod 755 "$LOGS_DIR" 2>/dev/null || echo "  ⚠️  Could not set logs directory permissions"

echo "📋 Checking existing data..."

# Check for existing database backups
BACKUP_COUNT=$(ls "$BACKUP_DIR"/*.sql 2>/dev/null | wc -l || echo "0")
echo "  🗄️  Database backups found: $BACKUP_COUNT"

# Check for existing logs
LOG_COUNT=$(ls "$LOGS_DIR"/*.log 2>/dev/null | wc -l || echo "0")
echo "  📝 Log files found: $LOG_COUNT"

echo ""
echo "🎯 Environment Setup Summary:"
echo "  📁 PostgreSQL Data: $POSTGRESQL_DATA_DIR"
echo "  💾 Backups:         $BACKUP_DIR"
echo "  📝 Logs:            $LOGS_DIR"
echo ""

# Validate Docker Compose file
if [ -f "docker-compose.intg-jobs.yml" ]; then
    echo "🐳 Validating Docker Compose configuration..."
    if docker-compose -f docker-compose.intg-jobs.yml config -q; then
        echo "  ✅ Docker Compose file is valid"
    else
        echo "  ❌ Docker Compose file has errors"
        exit 1
    fi
else
    echo "  ⚠️  docker-compose.intg-jobs.yml not found in current directory"
    exit 1
fi

echo ""
echo "🎉 ATS-INTG environment setup complete!"
echo ""
echo "Next steps:"
echo "  1. Start services: docker-compose -f docker-compose.intg-jobs.yml up -d"
echo "  2. Check logs:     docker logs postgres-intg -f"
echo "  3. Test database:  docker exec postgres-intg psql -U postgres -d intg_db -c 'SELECT version()'"
echo "  4. Monitor jobs:   docker logs ats-intg-scheduler -f"
echo ""
echo "🔧 Troubleshooting:"
echo "  - If PostgreSQL fails to start, check: docker logs postgres-intg"
echo "  - If permissions errors occur, run this script with sudo"
echo "  - Database data persists in: $POSTGRESQL_DATA_DIR"
echo ""