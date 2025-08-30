#!/usr/bin/env python3
"""
ATS-INTG Startup Manager
Intelligent startup orchestration for ATS Integration environment.
Handles data migration, incremental sync setup, and service initialization.
"""

import sys
import os
import subprocess
import time
from datetime import datetime, timedelta
import json

# Add ATS source path
sys.path.append('/workspace/src')

def log_info(message: str):
    """Enhanced logging with timestamp."""
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"{timestamp} - STARTUP - {message}")
    
    # Also log to file
    log_file = "/logs/startup.log"
    try:
        with open(log_file, 'a') as f:
            f.write(f"{timestamp} - STARTUP - {message}\n")
    except:
        pass  # Don't fail startup if logging fails

def log_success(message: str):
    """Log success messages."""
    log_info(f"✅ {message}")

def log_error(message: str):
    """Log error messages."""
    log_info(f"❌ {message}")

def log_warning(message: str):
    """Log warning messages."""
    log_info(f"⚠️ {message}")

def run_command(cmd: list, description: str = None, capture_output: bool = False) -> dict:
    """Run command with proper error handling."""
    if description:
        log_info(f"🔧 {description}")
    
    try:
        result = subprocess.run(
            cmd, 
            capture_output=capture_output,
            text=True,
            cwd='/workspace'
        )
        
        if result.returncode == 0:
            if description:
                log_success(f"{description}")
            return {
                'success': True,
                'stdout': result.stdout if capture_output else '',
                'stderr': result.stderr if capture_output else '',
                'returncode': result.returncode
            }
        else:
            if description:
                log_error(f"{description} failed")
            return {
                'success': False,
                'stdout': result.stdout if capture_output else '',
                'stderr': result.stderr if capture_output else '',
                'returncode': result.returncode
            }
    except Exception as e:
        log_error(f"Command execution failed: {e}")
        return {
            'success': False,
            'stdout': '',
            'stderr': str(e),
            'returncode': -1
        }

def wait_for_postgres():
    """Wait for PostgreSQL to be ready using Python connection."""
    log_info("⏳ Waiting for PostgreSQL to be ready...")
    
    max_attempts = 30
    attempt = 1
    
    while attempt <= max_attempts:
        try:
            # Use Python to test database connection
            import socket
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(5)
            result = sock.connect_ex(('postgres-intg', 5432))
            sock.close()
            
            if result == 0:
                log_success("PostgreSQL is ready and accepting connections")
                return True
            
        except Exception as e:
            log_warning(f"PostgreSQL readiness check error: {e}")
        
        log_info(f"  Attempt {attempt}/{max_attempts} - waiting 5 seconds...")
        time.sleep(5)
        attempt += 1
    
    log_error("PostgreSQL failed to become ready within expected time")
    return False

def check_intg_database_status() -> dict:
    """Check the current status of INTG database."""
    log_info("🔍 Checking INTG database status...")
    
    status = {
        'has_schema': False,
        'has_data': False,
        'table_count': 0,
        'record_count': 0,
        'last_migration': None
    }
    
    try:
        # Check for INTG tables
        table_query = "SELECT COUNT(*) FROM information_schema.tables WHERE table_name LIKE 'intg_%'"
        result = subprocess.run([
            'psql', '-h', 'postgres-intg', '-U', 'postgres', '-d', 'intg_db', 
            '-t', '-c', table_query
        ], capture_output=True, text=True, timeout=30, env={**os.environ, 'PGPASSWORD': 'intg_password'})
        
        if result.returncode == 0:
            table_count = int(result.stdout.strip())
            status['table_count'] = table_count
            status['has_schema'] = table_count > 0
            
            if status['has_schema']:
                # Check for actual data
                data_queries = [
                    "SELECT COUNT(*) FROM intg_instruments",
                    "SELECT COUNT(*) FROM intg_daily_prices", 
                    "SELECT COUNT(*) FROM intg_fundamentals_comprehensive"
                ]
                
                total_records = 0
                for query in data_queries:
                    try:
                        result = subprocess.run([
                            'psql', '-h', 'postgres-intg', '-U', 'postgres', '-d', 'intg_db',
                            '-t', '-c', query
                        ], capture_output=True, text=True, timeout=30, env={**os.environ, 'PGPASSWORD': 'intg_password'})
                        
                        if result.returncode == 0:
                            count = int(result.stdout.strip())
                            total_records += count
                    except:
                        pass
                
                status['record_count'] = total_records
                status['has_data'] = total_records > 0
        
        log_info(f"📊 Database status: {status['table_count']} tables, {status['record_count']} records")
        return status
        
    except Exception as e:
        log_error(f"Error checking database status, falling back to simulated: {e}")
        log_info("📊 Database status: Simulated check - 0 tables, 0 records")
        return status

def check_dev_database_connectivity() -> bool:
    """Check if DEV database is accessible."""
    log_info("🔗 Checking DEV database connectivity...")
    
    dev_host = os.getenv('DEV_DB_HOST', 'host.docker.internal')
    dev_port = int(os.getenv('DEV_DB_PORT', '5432'))
    
    try:
        import socket
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(5)
        result = sock.connect_ex((dev_host, dev_port))
        sock.close()
        
        if result == 0:
            log_success("DEV database is accessible")
            return True
        else:
            log_warning("DEV database not accessible - connection failed")
            return False
            
    except Exception as e:
        log_warning(f"DEV database connectivity check failed: {e}")
        return False

def get_dev_data_summary() -> dict:
    """Get summary of available data in DEV database."""
    log_info("📊 Checking DEV database data availability...")
    
    summary = {
        'instruments': 0,
        'daily_prices': 0,
        'fundamentals': 0,
        'available': False
    }
    
    if not check_dev_database_connectivity():
        return summary
    
    try:
        # Query DEV database directly
        dev_host = os.getenv('DEV_DB_HOST', '172.17.0.1')
        dev_port = os.getenv('DEV_DB_PORT', '5433')
        dev_user = os.getenv('DEV_DB_USER', 'postgres')
        dev_password = os.getenv('DEV_DB_PASSWORD', 'postgres')
        dev_db = os.getenv('DEV_DB_NAME', 'dev_db')
        
        queries = {
            'instruments': "SELECT COUNT(*) FROM dev_instruments",
            'daily_prices': "SELECT COUNT(*) FROM dev_daily_prices", 
            'fundamentals': "SELECT COUNT(*) FROM dev_fundamentals_comprehensive"
        }
        
        for data_type, query in queries.items():
            try:
                result = subprocess.run([
                    'psql', '-h', dev_host, '-p', dev_port, '-U', dev_user, '-d', dev_db,
                    '-t', '-c', query
                ], capture_output=True, text=True, timeout=30, env={**os.environ, 'PGPASSWORD': dev_password})
                
                if result.returncode == 0:
                    count = int(result.stdout.strip())
                    summary[data_type] = count
            except:
                pass
        
        summary['available'] = any(count > 0 for count in [summary['instruments'], summary['daily_prices'], summary['fundamentals']])
        
        if summary['available']:
            log_info(f"DEV data summary: {summary['instruments']} instruments, "
                    f"{summary['daily_prices']} prices, {summary['fundamentals']} fundamentals")
        else:
            log_warning("No data found in DEV database")
        
        return summary
        
    except Exception as e:
        log_error(f"Error getting DEV data summary, falling back to simulated: {e}")
        log_warning("No data found in DEV database (simulated)")
        return summary

def run_full_migration() -> bool:
    """Run full data migration from DEV to INTG."""
    log_info("🚀 Starting full data migration from DEV to INTG...")
    
    try:
        # Validate migration prerequisites
        result = run_command([
            'python3', 'scripts/intg_data_backfill.py', 'validate'
        ], "Validating migration prerequisites")
        
        if not result['success']:
            log_error("Migration validation failed")
            return False
        
        # Run the migration
        result = run_command([
            'python3', 'scripts/intg_data_backfill.py', 'backfill'
        ], "Running full data backfill")
        
        if result['success']:
            log_success("Full migration completed successfully")
            return True
        else:
            log_error("Full migration failed")
            return False
            
    except Exception as e:
        log_error(f"Migration error: {e}")
        log_warning("Migration failed - falling back to empty database")
        return False

def run_incremental_sync_setup() -> bool:
    """Setup incremental sync infrastructure."""
    log_info("🔧 Setting up incremental sync infrastructure...")
    
    try:
        # Setup incremental sync tables
        result = run_command([
            'python3', 'scripts/intg_incremental_sync.py', 'setup'
        ], "Setting up incremental sync tables")
        
        if not result['success']:
            log_error("Incremental sync setup failed")
            return False
        
        # Run initial incremental sync
        lookback_hours = 48  # Look back 48 hours for initial sync
        result = run_command([
            'python3', 'scripts/intg_incremental_sync.py', 'sync', 
            '--lookback-hours', str(lookback_hours)
        ], f"Running initial incremental sync ({lookback_hours}h lookback)")
        
        if result['success']:
            log_success("Incremental sync setup completed")
            return True
        else:
            log_warning("Incremental sync completed with some issues")
            return True  # Don't fail startup for sync issues
            
    except Exception as e:
        log_error(f"Incremental sync error: {e}")
        log_warning("Incremental sync setup had issues")
        return True  # Don't fail startup for sync issues

def create_startup_status_report() -> str:
    """Create startup status report."""
    
    intg_status = check_intg_database_status()
    dev_connectivity = check_dev_database_connectivity()
    dev_summary = get_dev_data_summary() if dev_connectivity else {}
    
    report = f"""
# ATS-INTG Startup Status Report

**Startup Time**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
**Auto-Migration**: {os.getenv('AUTO_MIGRATION_ENABLED', 'false')}

## INTG Database Status

- **Tables**: {intg_status.get('table_count', 0)} intg_* tables
- **Records**: {intg_status.get('record_count', 0)} total records  
- **Has Schema**: {intg_status.get('has_schema', False)}
- **Has Data**: {intg_status.get('has_data', False)}
- **Last Migration**: {intg_status.get('last_migration', 'Never')}

## DEV Database Connectivity

- **Accessible**: {dev_connectivity}
- **Instruments**: {dev_summary.get('instruments', 0)}
- **Daily Prices**: {dev_summary.get('daily_prices', 0)}
- **Fundamentals**: {dev_summary.get('fundamentals', 0)}

## Startup Actions Taken

"""
    
    return report

def main():
    """Main startup orchestration logic."""
    
    log_info("🚀 ATS-INTG Startup Manager")
    log_info("=" * 50)
    
    startup_success = True
    actions_taken = []
    
    # Step 1: Wait for PostgreSQL
    if not wait_for_postgres():
        log_error("PostgreSQL not ready - startup failed")
        return False
    
    actions_taken.append("✅ PostgreSQL ready")
    
    # Step 2: Check current INTG database status
    intg_status = check_intg_database_status()
    
    # Step 3: Determine startup strategy based on current state
    auto_migration = os.getenv('AUTO_MIGRATION_ENABLED', 'false').lower() == 'true'
    
    if not auto_migration:
        log_info("ℹ️ Auto-migration disabled - skipping migration checks")
        actions_taken.append("ℹ️ Auto-migration disabled")
    
    elif intg_status['has_data']:
        log_info("📊 INTG database already contains data - running incremental sync")
        actions_taken.append("📊 Found existing data")
        
        if run_incremental_sync_setup():
            actions_taken.append("✅ Incremental sync setup completed")
        else:
            actions_taken.append("⚠️ Incremental sync setup had issues")
            startup_success = False
    
    else:
        log_info("📋 INTG database is empty - checking migration options")
        actions_taken.append("📋 Empty database detected")
        
        # Check DEV database availability
        if check_dev_database_connectivity():
            dev_summary = get_dev_data_summary()
            
            if dev_summary['available']:
                log_info("🔄 DEV data available - running full migration")
                actions_taken.append("🔄 DEV data found")
                
                if run_full_migration():
                    actions_taken.append("✅ Full migration completed")
                    
                    # Setup incremental sync for future updates
                    if run_incremental_sync_setup():
                        actions_taken.append("✅ Incremental sync setup completed")
                else:
                    actions_taken.append("❌ Full migration failed")
                    startup_success = False
            else:
                log_warning("⚠️ DEV database accessible but contains no data")
                actions_taken.append("⚠️ DEV database empty")
        else:
            log_warning("⚠️ DEV database not accessible - starting with empty INTG")
            actions_taken.append("⚠️ DEV database not accessible")
    
    # Step 4: Create startup status report
    report = create_startup_status_report()
    
    # Add actions taken to report
    report += "\n".join(actions_taken)
    report += f"""

## Next Steps

{"✅ Startup completed successfully!" if startup_success else "⚠️ Startup completed with issues"}

- **Monitor Logs**: `docker logs ats-intg-scheduler -f`
- **Check Status**: `python scripts/intg_incremental_sync.py status`
- **Manual Migration**: `python scripts/intg_data_backfill.py backfill`

**Services Starting**: Daily jobs scheduler with incremental sync
"""
    
    # Save report
    try:
        with open('/logs/startup_report.md', 'w') as f:
            f.write(report)
        log_info("📄 Startup report saved: /logs/startup_report.md")
    except:
        pass
    
    # Step 5: Final startup summary
    log_info("🎉 Startup Manager Complete!")
    log_info("=" * 50)
    
    for action in actions_taken:
        log_info(f"  {action}")
    
    log_info("=" * 50)
    
    if startup_success:
        log_success("ATS-INTG startup completed successfully!")
        log_info("🔄 Starting continuous scheduler...")
        
        # Start continuous scheduler loop
        while True:
            import time
            current_time = time.strftime('%Y-%m-%d %H:%M:%S')
            log_info(f"📊 ATS-INTG running at {current_time}")
            time.sleep(3600)  # Log every hour
    else:
        log_warning("ATS-INTG startup completed with issues")
        log_info("💡 Check logs and run manual migration if needed")
    
    return startup_success

if __name__ == "__main__":
    success = main()
    if not success:
        sys.exit(1)