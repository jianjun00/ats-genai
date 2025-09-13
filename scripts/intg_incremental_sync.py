#!/usr/bin/env python3
"""
ATS-INTG Incremental Sync from ATS-DEV
Handles ongoing incremental updates from development to integration environment.
"""

import sys
import os
import subprocess
import json
import argparse
from datetime import datetime, timedelta
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
import hashlib

# Add ATS source path
sys.path.append('/workspace/src')

# Configuration
SYNC_BATCH_SIZE = 500
MAX_WORKERS = 2
CHECKPOINT_INTERVAL = 100
DEFAULT_LOOKBACK_HOURS = 25  # Look back 25 hours to catch any delayed updates

# Threading for progress tracking
stats = {
    'tables_processed': 0,
    'records_synced': 0,
    'records_updated': 0,
    'records_inserted': 0,
    'sync_conflicts': 0,
    'start_time': None
}
stats_lock = threading.Lock()

def log_info(message: str):
    """Enhanced logging with timestamp."""
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"{timestamp} - SYNC - {message}")

def log_success(message: str):
    """Log success messages."""
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"{timestamp} - ✅ {message}")

def log_error(message: str):
    """Log error messages."""
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"{timestamp} - ❌ {message}")

def log_warning(message: str):
    """Log warning messages."""
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"{timestamp} - ⚠️ {message}")

def run_dev_query(query: str, description: str = None) -> str:
    """Execute query on dev database."""
    if description:
        log_info(f"🔍 DEV: {description}")

    try:
        result = subprocess.run(
            ['python3', 'scripts/run_dev.py', 'query', '--query', query],
            capture_output=True,
            text=True,
            cwd='/workspace'
        )
        if result.returncode == 0:
            return result.stdout.strip()
        else:
            log_error(f"DEV query failed: {result.stderr}")
            return ""
    except Exception as e:
        log_error(f"DEV query error: {e}")
        return ""

def run_intg_query(query: str, description: str = None) -> str:
    """Execute query on intg database."""
    if description:
        log_info(f"🔧 INTG: {description}")

    try:
        result = subprocess.run(
            ['python3', 'scripts/run_intg.py', 'query', '--query', query],
            capture_output=True,
            text=True,
            cwd='/workspace'
        )
        if result.returncode == 0:
            return result.stdout.strip()
        else:
            log_error(f"INTG query failed: {result.stderr}")
            return ""
    except Exception as e:
        log_error(f"INTG query error: {e}")
        return ""

def create_sync_tracking_tables():
    """Create tables to track incremental sync progress."""
    log_info("🔧 Setting up incremental sync tracking...")

    # Sync checkpoint table
    checkpoint_table_sql = """
    CREATE TABLE IF NOT EXISTS intg_sync_checkpoint (
        id SERIAL PRIMARY KEY,
        table_name VARCHAR(100) NOT NULL UNIQUE,
        last_sync_timestamp TIMESTAMP NOT NULL DEFAULT '1900-01-01'::timestamp,
        last_sync_completed TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        records_synced_last_run INTEGER DEFAULT 0,
        sync_method VARCHAR(50) DEFAULT 'incremental',
        is_active BOOLEAN DEFAULT true,
        error_count INTEGER DEFAULT 0,
        last_error_message TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """

    # Sync history table
    history_table_sql = """
    CREATE TABLE IF NOT EXISTS intg_sync_history (
        id SERIAL PRIMARY KEY,
        table_name VARCHAR(100) NOT NULL,
        sync_date DATE NOT NULL DEFAULT CURRENT_DATE,
        sync_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        records_checked INTEGER DEFAULT 0,
        records_inserted INTEGER DEFAULT 0,
        records_updated INTEGER DEFAULT 0,
        records_deleted INTEGER DEFAULT 0,
        sync_duration_seconds INTEGER,
        sync_method VARCHAR(50) DEFAULT 'incremental',
        status VARCHAR(20) DEFAULT 'completed',
        error_message TEXT,
        lookback_hours INTEGER DEFAULT 24
    );
    """

    # Conflict resolution log
    conflict_table_sql = """
    CREATE TABLE IF NOT EXISTS intg_sync_conflicts (
        id SERIAL PRIMARY KEY,
        table_name VARCHAR(100) NOT NULL,
        conflict_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        conflict_type VARCHAR(50),
        record_id VARCHAR(255),
        dev_value TEXT,
        intg_value TEXT,
        resolution VARCHAR(50),
        resolved_value TEXT,
        notes TEXT
    );
    """

    run_intg_query(checkpoint_table_sql, "Creating sync checkpoint table")
    run_intg_query(history_table_sql, "Creating sync history table")
    run_intg_query(conflict_table_sql, "Creating sync conflicts table")

    # Initialize checkpoint records for core tables
    core_tables = [
        'dev_instrument',
        'dev_daily_price',
        'dev_fundamental_comprehensive',
        'dev_tiingo_daily_prices',
        'dev_polygon_daily_prices',
        'dev_fmp_daily_prices'
    ]

    for table_name in core_tables:
        init_checkpoint = f"""
        INSERT INTO intg_sync_checkpoint (table_name, last_sync_timestamp)
        VALUES ('{table_name}', CURRENT_TIMESTAMP - INTERVAL '24 hours')
        ON CONFLICT (table_name) DO NOTHING
        """
        run_intg_query(init_checkpoint)

    log_success("Sync tracking tables initialized")

def get_table_sync_strategy() -> dict:
    """Define sync strategies for different table types."""
    return {
        'dev_instrument': {
            'target_table': 'intg_instrument',
            'sync_method': 'upsert',
            'timestamp_column': 'updated_at',
            'unique_columns': ['symbol'],
            'conflict_resolution': 'dev_wins',
            'change_detection': 'timestamp'
        },
        'dev_daily_price': {
            'target_table': 'intg_daily_price',
            'sync_method': 'append_only',
            'timestamp_column': 'created_at',
            'unique_columns': ['symbol', 'date'],
            'conflict_resolution': 'skip_duplicate',
            'change_detection': 'timestamp',
            'vendor_field': 'dev_migration'
        },
        'dev_fundamental_comprehensive': {
            'target_table': 'intg_fundamental_comprehensive',
            'sync_method': 'upsert',
            'timestamp_column': 'updated_at',
            'unique_columns': ['symbol', 'date', 'fiscal_period'],
            'conflict_resolution': 'dev_wins',
            'change_detection': 'timestamp'
        },
        'dev_tiingo_daily_prices': {
            'target_table': 'intg_daily_price',
            'sync_method': 'append_only',
            'timestamp_column': 'created_at',
            'unique_columns': ['symbol', 'date'],
            'conflict_resolution': 'skip_duplicate',
            'change_detection': 'timestamp',
            'vendor_field': 'tiingo'
        },
        'dev_polygon_daily_prices': {
            'target_table': 'intg_daily_price',
            'sync_method': 'append_only',
            'timestamp_column': 'created_at',
            'unique_columns': ['symbol', 'date'],
            'conflict_resolution': 'skip_duplicate',
            'change_detection': 'timestamp',
            'vendor_field': 'polygon'
        },
        'dev_fmp_daily_prices': {
            'target_table': 'intg_daily_price',
            'sync_method': 'append_only',
            'timestamp_column': 'created_at',
            'unique_columns': ['symbol', 'date'],
            'conflict_resolution': 'skip_duplicate',
            'change_detection': 'timestamp',
            'vendor_field': 'fmp'
        }
    }

def get_last_sync_checkpoint(table_name: str) -> datetime:
    """Get the last sync timestamp for a table."""
    checkpoint_query = f"""
    SELECT last_sync_timestamp
    FROM intg_sync_checkpoint
    WHERE table_name = '{table_name}' AND is_active = true
    """

    result = run_intg_query(checkpoint_query, f"Getting sync checkpoint for {table_name}")

    if result and '|' in result:
        lines = result.strip().split('\n')
        for line in lines[2:]:  # Skip header and separator
            if '|' in line and not line.strip().startswith('('):
                timestamp_str = line.split('|')[0].strip()
                try:
                    return datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S')
                except ValueError:
                    try:
                        return datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S.%f')
                    except ValueError:
                        pass

    # Default to 24 hours ago
    return datetime.now() - timedelta(hours=24)

def update_sync_checkpoint(table_name: str, new_checkpoint: datetime,
                          records_synced: int, error_message: str = None):
    """Update sync checkpoint for a table."""

    update_query = f"""
    UPDATE intg_sync_checkpoint
    SET last_sync_timestamp = '{new_checkpoint}',
        last_sync_completed = CURRENT_TIMESTAMP,
        records_synced_last_run = {records_synced},
        error_count = {1 if error_message else 0},
        last_error_message = {f"'{error_message}'" if error_message else 'NULL'},
        updated_at = CURRENT_TIMESTAMP
    WHERE table_name = '{table_name}'
    """

    run_intg_query(update_query, f"Updating checkpoint for {table_name}")

def log_sync_history(table_name: str, records_checked: int, records_inserted: int,
                    records_updated: int, duration_seconds: int,
                    sync_method: str = 'incremental', status: str = 'completed',
                    error_message: str = None, lookback_hours: int = 24):
    """Log sync operation to history."""

    history_query = f"""
    INSERT INTO intg_sync_history
    (table_name, records_checked, records_inserted, records_updated,
     sync_duration_seconds, sync_method, status, error_message, lookback_hours)
    VALUES ('{table_name}', {records_checked}, {records_inserted}, {records_updated},
            {duration_seconds}, '{sync_method}', '{status}',
            {f"'{error_message}'" if error_message else 'NULL'}, {lookback_hours})
    """

    run_intg_query(history_query, f"Logging sync history for {table_name}")

def detect_changes_in_dev_table(table_name: str, strategy: dict,
                                lookback_hours: int = None) -> list:
    """Detect changes in DEV table since last sync."""

    if lookback_hours is None:
        lookback_hours = DEFAULT_LOOKBACK_HOURS

    # Get last sync checkpoint
    last_sync = get_last_sync_checkpoint(table_name)

    # Override with lookback hours if specified
    if lookback_hours:
        cutoff_time = datetime.now() - timedelta(hours=lookback_hours)
        if cutoff_time > last_sync:
            last_sync = cutoff_time

    timestamp_column = strategy.get('timestamp_column', 'updated_at')

    # Query for changes since last sync
    changes_query = f"""
    SELECT * FROM {table_name}
    WHERE {timestamp_column} > '{last_sync}'
    ORDER BY {timestamp_column}
    LIMIT 10000
    """

    log_info(f"🔍 Detecting changes in {table_name} since {last_sync}")

    result = run_dev_query(changes_query, f"Detecting changes in {table_name}")

    if not result or 'ERROR' in result.upper():
        log_warning(f"No changes found in {table_name} or query failed")
        return []

    # Parse results into records
    lines = result.strip().split('\n')
    if len(lines) < 3:  # Header, separator, at least one record
        log_info(f"No changes detected in {table_name}")
        return []

    headers = [h.strip() for h in lines[0].split('|') if h.strip()]

    changes = []
    for line in lines[2:]:  # Skip header and separator
        if '|' in line and not line.strip().startswith('('):
            values = [v.strip() for v in line.split('|')]
            if len(values) >= len(headers):
                record = dict(zip(headers, values[:len(headers)]))
                changes.append(record)

    log_info(f"📊 Found {len(changes)} changed records in {table_name}")
    return changes

def transform_record_for_intg(record: dict, source_table: str, strategy: dict) -> dict:
    """Transform a DEV record for INTG insertion."""

    transformed = record.copy()

    # Add vendor field if needed (for daily prices tables)
    if 'vendor_field' in strategy:
        transformed['vendor'] = strategy['vendor_field']

    # Standardize column names
    if 'creation_timestamp' in transformed:
        transformed['created_at'] = transformed.pop('creation_timestamp')

    if 'last_updated' in transformed:
        transformed['updated_at'] = transformed.pop('last_updated')

    # Add sync metadata
    transformed['sync_timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    transformed['sync_source'] = source_table

    return transformed

def upsert_record_to_intg(record: dict, target_table: str, strategy: dict) -> str:
    """Insert or update record in INTG database."""

    # Build column lists
    columns = list(record.keys())
    values = []

    for col in columns:
        value = record[col]
        if value is None or value == 'NULL' or value == '':
            values.append('NULL')
        elif isinstance(value, str):
            escaped_value = value.replace("'", "''")
            values.append(f"'{escaped_value}'")
        else:
            values.append(str(value))

    unique_columns = strategy.get('unique_columns', ['id'])
    conflict_resolution = strategy.get('conflict_resolution', 'dev_wins')

    # Build conflict clause based on strategy
    if conflict_resolution == 'dev_wins':
        # Update all columns with DEV values
        update_clauses = [f"{col} = EXCLUDED.{col}" for col in columns if col not in unique_columns]
        conflict_clause = f"ON CONFLICT ({', '.join(unique_columns)}) DO UPDATE SET {', '.join(update_clauses)}, updated_at = CURRENT_TIMESTAMP"
    elif conflict_resolution == 'skip_duplicate':
        conflict_clause = f"ON CONFLICT ({', '.join(unique_columns)}) DO NOTHING"
    else:
        conflict_clause = ""

    # Build and execute upsert query
    upsert_query = f"""
    INSERT INTO {target_table} ({', '.join(columns)})
    VALUES ({', '.join(values)})
    {conflict_clause}
    """

    result = run_intg_query(upsert_query)

    # Determine operation type from result
    if result:
        if 'INSERT 0 1' in result:
            return 'inserted'
        elif 'UPDATE 1' in result:
            return 'updated'
        elif 'INSERT 0 0' in result:
            return 'skipped'

    return 'unknown'

def sync_table_incrementally(table_name: str, strategy: dict,
                           lookback_hours: int = None, dry_run: bool = False) -> dict:
    """Perform incremental sync for a single table."""

    start_time = datetime.now()
    log_info(f"🔄 Starting incremental sync: {table_name} → {strategy['target_table']}")

    # Detect changes in DEV
    changes = detect_changes_in_dev_table(table_name, strategy, lookback_hours)

    if not changes:
        log_info(f"✅ No changes to sync for {table_name}")
        return {
            'records_checked': 0,
            'records_inserted': 0,
            'records_updated': 0,
            'records_skipped': 0,
            'duration_seconds': (datetime.now() - start_time).seconds,
            'status': 'completed'
        }

    # Process changes
    records_inserted = 0
    records_updated = 0
    records_skipped = 0
    latest_timestamp = start_time

    for record in changes:
        try:
            # Transform record for INTG
            transformed_record = transform_record_for_intg(record, table_name, strategy)

            if dry_run:
                log_info(f"🧪 DRY RUN: Would sync record for {transformed_record.get('symbol', 'unknown')}")
                records_inserted += 1
            else:
                # Upsert to INTG
                operation = upsert_record_to_intg(transformed_record, strategy['target_table'], strategy)

                if operation == 'inserted':
                    records_inserted += 1
                elif operation == 'updated':
                    records_updated += 1
                else:
                    records_skipped += 1

            # Track latest timestamp
            timestamp_column = strategy.get('timestamp_column', 'updated_at')
            if timestamp_column in record:
                try:
                    record_timestamp = datetime.strptime(record[timestamp_column], '%Y-%m-%d %H:%M:%S')
                    if record_timestamp > latest_timestamp:
                        latest_timestamp = record_timestamp
                except ValueError:
                    pass

            with stats_lock:
                stats['records_synced'] += 1
                if operation == 'inserted':
                    stats['records_inserted'] += 1
                elif operation == 'updated':
                    stats['records_updated'] += 1

        except Exception as e:
            log_error(f"Error syncing record from {table_name}: {e}")
            continue

    duration_seconds = (datetime.now() - start_time).seconds

    # Update checkpoint if not dry run
    if not dry_run:
        update_sync_checkpoint(table_name, latest_timestamp, records_inserted + records_updated)

        # Log to history
        log_sync_history(
            table_name, len(changes), records_inserted, records_updated,
            duration_seconds, 'incremental', 'completed',
            lookback_hours=lookback_hours or DEFAULT_LOOKBACK_HOURS
        )

    log_success(f"Sync completed: {table_name} - {records_inserted} inserted, {records_updated} updated, {records_skipped} skipped")

    return {
        'records_checked': len(changes),
        'records_inserted': records_inserted,
        'records_updated': records_updated,
        'records_skipped': records_skipped,
        'duration_seconds': duration_seconds,
        'status': 'completed'
    }

def get_sync_status_report() -> str:
    """Generate comprehensive sync status report."""

    log_info("📊 Generating sync status report...")

    # Get checkpoint status
    checkpoint_query = """
    SELECT
        table_name,
        last_sync_timestamp,
        last_sync_completed,
        records_synced_last_run,
        error_count,
        CASE WHEN is_active THEN 'Active' ELSE 'Inactive' END as status
    FROM intg_sync_checkpoint
    ORDER BY table_name
    """

    checkpoint_result = run_intg_query(checkpoint_query, "Getting checkpoint status")

    # Get recent sync history
    history_query = """
    SELECT
        table_name,
        sync_date,
        records_checked,
        records_inserted,
        records_updated,
        sync_duration_seconds,
        status
    FROM intg_sync_history
    WHERE sync_date >= CURRENT_DATE - INTERVAL '7 days'
    ORDER BY sync_timestamp DESC
    LIMIT 20
    """

    history_result = run_intg_query(history_query, "Getting recent sync history")

    # Build report
    report = f"""
# ATS-INTG Incremental Sync Status Report

**Report Generated**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

## Current Sync Checkpoints

{checkpoint_result if checkpoint_result else 'No checkpoint data available'}

## Recent Sync History (Last 7 Days)

{history_result if history_result else 'No recent sync history'}

## Sync Statistics Summary

"""

    # Get aggregate statistics
    stats_query = """
    SELECT
        COUNT(DISTINCT table_name) as tables_tracked,
        SUM(records_synced_last_run) as total_records_last_sync,
        COUNT(CASE WHEN error_count > 0 THEN 1 END) as tables_with_errors,
        AVG(EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - last_sync_completed))/3600) as avg_hours_since_sync
    FROM intg_sync_checkpoint
    WHERE is_active = true
    """

    stats_result = run_intg_query(stats_query, "Getting sync statistics")

    if stats_result:
        report += f"\n{stats_result}\n"

    report += f"""

## Recommended Actions

**Daily Sync Schedule:**
- **Peak Hours (9 AM - 4 PM ET)**: Every 4 hours for price data
- **Off Hours**: Every 8 hours for comprehensive sync
- **Weekly**: Full reconciliation on Sundays

**Monitoring Commands:**
```bash
# Check sync status
python scripts/intg_incremental_sync.py status

# Run manual incremental sync
python scripts/intg_incremental_sync.py sync --lookback-hours 2

# View detailed logs
tail -f /mnt/d/ats-logs/intg/incremental_sync.log
```

Generated by ATS-INTG Incremental Sync System
"""

    return report

def main():
    """Main incremental sync execution function."""

    parser = argparse.ArgumentParser(description="ATS-INTG Incremental Sync from ATS-DEV")
    parser.add_argument("action", choices=[
        "setup", "sync", "status", "reconcile", "monitor"
    ], help="Sync action to perform")

    parser.add_argument("--tables", nargs="+", help="Specific tables to sync")
    parser.add_argument("--lookback-hours", type=int, default=DEFAULT_LOOKBACK_HOURS,
                       help=f"Hours to look back for changes (default: {DEFAULT_LOOKBACK_HOURS})")
    parser.add_argument("--dry-run", action="store_true", help="Dry run mode - no actual changes")
    parser.add_argument("--continuous", action="store_true", help="Run in continuous monitoring mode")

    args = parser.parse_args()

    with stats_lock:
        stats['start_time'] = datetime.now()

    log_info("🚀 ATS-INTG Incremental Sync Manager")
    log_info("=" * 50)

    if args.action == "setup":
        log_info("🔧 Setting up incremental sync infrastructure...")
        create_sync_tracking_tables()
        log_success("Incremental sync setup completed")
        return True

    elif args.action == "sync":
        log_info("🔄 Starting incremental sync operation...")

        if args.dry_run:
            log_warning("🧪 DRY RUN MODE - No actual changes will be made")

        # Get sync strategies
        sync_strategies = get_table_sync_strategy()

        # Filter to specific tables if requested
        if args.tables:
            filtered_strategies = {k: v for k, v in sync_strategies.items() if k in args.tables}
            sync_strategies = filtered_strategies

        log_info(f"📋 Syncing {len(sync_strategies)} tables")

        # Process each table
        successful_syncs = 0
        failed_syncs = 0
        total_records_synced = 0

        for table_name, strategy in sync_strategies.items():
            log_info(f"🔄 Processing: {table_name}")

            try:
                sync_result = sync_table_incrementally(
                    table_name, strategy, args.lookback_hours, args.dry_run
                )

                if sync_result['status'] == 'completed':
                    successful_syncs += 1
                    total_records_synced += sync_result['records_inserted'] + sync_result['records_updated']

                    log_info(f"  ✅ {sync_result['records_checked']} checked, "
                            f"{sync_result['records_inserted']} inserted, "
                            f"{sync_result['records_updated']} updated")
                else:
                    failed_syncs += 1
                    log_error(f"  ❌ Sync failed for {table_name}")

            except Exception as e:
                log_error(f"Error syncing {table_name}: {e}")
                failed_syncs += 1

        # Final summary
        log_info("🎉 Incremental Sync Complete!")
        log_info("=" * 50)
        log_info(f"✅ Successful table syncs: {successful_syncs}")
        log_info(f"❌ Failed table syncs: {failed_syncs}")
        log_info(f"📊 Total records synced: {total_records_synced}")
        log_info(f"⏱️ Total duration: {(datetime.now() - stats['start_time']).total_seconds() / 60:.1f} minutes")

        return failed_syncs == 0

    elif args.action == "status":
        log_info("📊 Generating sync status report...")

        report = get_sync_status_report()
        print(report)

        # Save report to file
        report_file = f"/workspace/INTG-SYNC-STATUS-{datetime.now().strftime('%Y%m%d_%H%M%S')}.md"
        with open(report_file, 'w') as f:
            f.write(report)

        log_success(f"Sync status report saved: {report_file}")
        return True

    elif args.action == "reconcile":
        log_info("🔍 Starting full reconciliation...")

        # Full reconciliation means sync with longer lookback
        reconcile_hours = args.lookback_hours * 24  # Much longer lookback

        log_info(f"Reconciling with {reconcile_hours} hour lookback")

        # Run sync with extended lookback
        return main_sync_with_args(['sync', '--lookback-hours', str(reconcile_hours)])

    elif args.action == "monitor":
        log_info("👀 Starting continuous monitoring mode...")

        if args.continuous:
            import time

            log_info("🔄 Continuous monitoring enabled - syncing every hour")

            while True:
                try:
                    log_info("⏰ Running scheduled incremental sync...")

                    # Run incremental sync
                    sync_strategies = get_table_sync_strategy()

                    for table_name, strategy in sync_strategies.items():
                        sync_table_incrementally(table_name, strategy, args.lookback_hours)

                    log_success("Scheduled sync completed")

                    # Wait 1 hour before next sync
                    log_info("😴 Sleeping for 1 hour until next sync...")
                    time.sleep(3600)

                except KeyboardInterrupt:
                    log_info("🛑 Continuous monitoring stopped by user")
                    break
                except Exception as e:
                    log_error(f"Error in continuous monitoring: {e}")
                    log_info("😴 Sleeping 5 minutes before retry...")
                    time.sleep(300)
        else:
            # Single monitoring run
            report = get_sync_status_report()
            print(report)

        return True

def main_sync_with_args(args_list):
    """Helper function to call main with specific arguments."""
    import sys
    original_argv = sys.argv
    sys.argv = ['intg_incremental_sync.py'] + args_list
    try:
        return main()
    finally:
        sys.argv = original_argv

if __name__ == "__main__":
    success = main()
    if not success:
        sys.exit(1)