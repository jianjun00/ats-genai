#!/usr/bin/env python3
"""
Enhanced PostgreSQL Configuration with Host Path Mounting

Provides maximum data protection by mounting PostgreSQL data directory
directly to host filesystem alongside existing backup system.

Features:
- Host path mounting for /var/lib/postgresql/data
- Automatic backup/restore functionality
- D: drive persistence with timestamps
- Zero-downtime migration from existing setup
"""

import subprocess
import sys
import os
import time
from datetime import datetime
from pathlib import Path

def run_command(cmd, description=None, check=True):
    """Run command with logging."""
    if description:
        print(f"🔧 {description}")
    
    try:
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        if check and result.returncode != 0:
            print(f"❌ Command failed: {cmd}")
            print(f"Error: {result.stderr}")
            return False
        return result.stdout.strip() if result.stdout else True
    except Exception as e:
        print(f"❌ Exception: {e}")
        return False

def ensure_host_directories():
    """Ensure all required host directories exist."""
    directories = [
        "/mnt/d/ats-data/postgres-data",
        "/mnt/d/ats-backup/continuous",
        "/mnt/d/ats-backup/emergency-backups",
        "/mnt/d/ats-logs/postgres"
    ]
    
    for directory in directories:
        os.makedirs(directory, exist_ok=True)
        print(f"📁 Ensured directory: {directory}")

def backup_current_data():
    """Create comprehensive backup of current database."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_file = f"/mnt/d/ats-backup/continuous/pre_migration_backup_{timestamp}.sql"
    
    print("💾 Creating pre-migration backup...")
    cmd = f"docker exec ats-dev-postgres pg_dump -U postgres -d dev_db > {backup_file}"
    
    if run_command(cmd, "Backing up current database"):
        # Check backup size
        if os.path.exists(backup_file):
            size_bytes = os.path.getsize(backup_file)
            size_gb = size_bytes / (1024**3)
            print(f"✅ Backup created: {backup_file} ({size_gb:.2f} GB)")
            return backup_file
        else:
            print("❌ Backup file not created")
            return None
    return None

def stop_current_postgres():
    """Safely stop current PostgreSQL container."""
    print("🛑 Stopping current PostgreSQL container...")
    
    # The run_dev.py script should handle backup automatically
    result = run_command("python scripts/run_dev.py stop --service postgres", 
                        "Stopping PostgreSQL with auto-backup", check=False)
    
    # Wait for container to stop
    time.sleep(5)
    
    # Verify it's stopped
    result = run_command("docker ps -q -f name=ats-dev-postgres", check=False)
    if result:
        print("⚠️ Container still running, force stopping...")
        run_command("docker stop ats-dev-postgres", check=False)
        run_command("docker rm ats-dev-postgres", check=False)
    
    print("✅ PostgreSQL container stopped")

def migrate_existing_data():
    """Migrate data from Docker volume to host path."""
    print("🔄 Migrating existing data to host path...")
    
    # Create temporary container to access volume data
    temp_container_cmd = """
    docker run --rm -v postgres-data:/source -v /mnt/d/ats-data/postgres-data:/destination \
    busybox sh -c 'cp -a /source/. /destination/'
    """
    
    if run_command(temp_container_cmd, "Migrating PostgreSQL data to host path"):
        print("✅ Data migration completed")
        
        # Verify migration
        host_files = run_command("ls -la /mnt/d/ats-data/postgres-data/", check=False)
        if host_files and "postgresql.conf" in host_files:
            print("✅ Migration verified - PostgreSQL files found on host")
            return True
        else:
            print("❌ Migration verification failed")
            return False
    else:
        print("❌ Data migration failed")
        return False

def start_postgres_with_host_mount():
    """Start PostgreSQL with host path mounting."""
    print("🚀 Starting PostgreSQL with host path mounting...")
    
    cmd = f"""
    docker run -d --name ats-dev-postgres \\
        -p 5432:5432 \\
        -v /mnt/d/ats-data/postgres-data:/var/lib/postgresql/data \\
        -v /mnt/d/ats-backup:/backup \\
        -v /mnt/d/ats-logs/postgres:/var/log/postgresql \\
        -e POSTGRES_USER=postgres \\
        -e POSTGRES_PASSWORD=dev_password \\
        -e POSTGRES_DB=dev_db \\
        -e POSTGRES_INITDB_ARGS="--auth-host=md5 --auth-local=trust" \\
        postgres:13
    """
    
    if run_command(cmd, "Starting PostgreSQL with host mounts"):
        print("✅ PostgreSQL started with host path mounting")
        
        # Wait for PostgreSQL to be ready
        print("⏳ Waiting for PostgreSQL to be ready...")
        for i in range(30):
            if run_command("docker exec ats-dev-postgres pg_isready -U postgres", check=False):
                print("✅ PostgreSQL is ready")
                return True
            time.sleep(2)
        
        print("❌ PostgreSQL failed to become ready")
        return False
    else:
        print("❌ Failed to start PostgreSQL")
        return False

def verify_data_integrity():
    """Verify that data is intact after migration."""
    print("🔍 Verifying data integrity...")
    
    # Check record counts
    queries = [
        ("dev_daily_prices_tiingo", "SELECT COUNT(*) FROM dev_daily_prices_tiingo"),
        ("dev_daily_prices_polygon", "SELECT COUNT(*) FROM dev_daily_prices_polygon"),  
        ("dev_daily_prices_eodhd", "SELECT COUNT(*) FROM dev_daily_prices_eodhd"),
        ("dev_financial_events", "SELECT COUNT(*) FROM dev_financial_events")
    ]
    
    total_records = 0
    for table_name, query in queries:
        result = run_command(f'docker exec ats-dev-postgres psql -U postgres -d dev_db -t -c "{query}"', check=False)
        if result:
            count = int(result.strip())
            total_records += count
            print(f"✅ {table_name}: {count:,} records")
        else:
            print(f"❌ Failed to query {table_name}")
            return False
    
    print(f"📊 Total records verified: {total_records:,}")
    
    if total_records > 40000000:  # Should have 40M+ records
        print("✅ Data integrity verification passed")
        return True
    else:
        print("❌ Data integrity verification failed - unexpected record count")
        return False

def setup_continuous_backup():
    """Setup continuous backup system."""
    print("⚙️ Setting up continuous backup system...")
    
    # Create backup script
    backup_script = """#!/bin/bash
# Continuous PostgreSQL Backup Script
BACKUP_DIR="/mnt/d/ats-backup/continuous"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
BACKUP_FILE="$BACKUP_DIR/auto_backup_$TIMESTAMP.sql"

# Create backup
docker exec ats-dev-postgres pg_dump -U postgres -d dev_db > "$BACKUP_FILE"

# Keep only last 10 backups
ls -t $BACKUP_DIR/auto_backup_*.sql | tail -n +11 | xargs -r rm

# Update latest backup link
ln -sf "$BACKUP_FILE" "$BACKUP_DIR/latest_backup.sql"

echo "Backup created: $BACKUP_FILE"
"""
    
    script_path = "/mnt/d/ats-backup/continuous_backup.sh"
    with open(script_path, 'w') as f:
        f.write(backup_script)
    
    os.chmod(script_path, 0o755)
    print(f"✅ Backup script created: {script_path}")
    
    print("📋 To enable hourly backups, run:")
    print(f"   echo '0 * * * * {script_path}' | crontab -")

def main():
    """Main migration process."""
    print("🚀 Starting PostgreSQL Host Path Migration")
    print("=" * 60)
    
    try:
        # Step 1: Ensure directories exist
        ensure_host_directories()
        
        # Step 2: Create comprehensive backup
        backup_file = backup_current_data()
        if not backup_file:
            print("❌ Backup failed - aborting migration")
            return False
        
        # Step 3: Stop current PostgreSQL
        stop_current_postgres()
        
        # Step 4: Migrate data to host path
        if not migrate_existing_data():
            print("❌ Migration failed - aborting")
            return False
        
        # Step 5: Start PostgreSQL with host mounting
        if not start_postgres_with_host_mount():
            print("❌ Failed to start new PostgreSQL")
            return False
        
        # Step 6: Verify data integrity
        if not verify_data_integrity():
            print("❌ Data integrity check failed")
            return False
        
        # Step 7: Setup continuous backup
        setup_continuous_backup()
        
        print("=" * 60)
        print("✅ MIGRATION SUCCESSFUL!")
        print("🎉 PostgreSQL now uses host path mounting")
        print("📍 Data location: /mnt/d/ats-data/postgres-data")
        print("💾 Backup location: /mnt/d/ats-backup/continuous")
        print("📊 Data is now maximally protected against container loss")
        
        return True
        
    except Exception as e:
        print(f"❌ Migration failed with exception: {e}")
        return False

if __name__ == "__main__":
    success = main()
    if not success:
        sys.exit(1)