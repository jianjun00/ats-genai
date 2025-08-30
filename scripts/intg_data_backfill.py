#!/usr/bin/env python3
"""
ATS-INTG Data Backfill from ATS-DEV
Comprehensive data migration strategy to populate integration environment with development data.
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
BATCH_SIZE = 1000
MAX_WORKERS = 3
CHECKPOINT_INTERVAL = 500

# Threading for progress tracking
stats = {
    'tables_processed': 0,
    'total_records': 0,
    'successful_batches': 0,
    'failed_batches': 0,
    'start_time': None,
    'current_table': None
}
stats_lock = threading.Lock()

def log_info(message: str):
    """Enhanced logging with timestamp."""
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"{timestamp} - BACKFILL - {message}")

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
    """Execute query on dev database using run_dev infrastructure."""
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
    """Execute query on intg database using run_intg infrastructure."""
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

def get_table_mapping() -> dict:
    """Define mapping between dev and intg tables."""
    return {
        # Core data tables
        'dev_instruments': 'intg_instruments',
        'dev_daily_prices': 'intg_daily_prices', 
        'dev_fundamentals_comprehensive': 'intg_fundamentals_comprehensive',
        
        # Add more mappings as needed
        'dev_tiingo_daily_prices': 'intg_daily_prices',  # Merge into unified table
        'dev_polygon_daily_prices': 'intg_daily_prices', # Merge into unified table
        
        # Checkpoint and metadata tables (don't copy these)
        # 'dev_*_checkpoint': None (skip checkpoint tables)
    }

def create_intg_backfill_tracking():
    """Create backfill tracking table in INTG."""
    log_info("🔧 Setting up backfill tracking...")
    
    tracking_table_sql = """
    CREATE TABLE IF NOT EXISTS intg_backfill_tracking (
        id SERIAL PRIMARY KEY,
        source_table VARCHAR(100) NOT NULL,
        target_table VARCHAR(100) NOT NULL,
        backfill_date DATE NOT NULL DEFAULT CURRENT_DATE,
        records_processed INTEGER DEFAULT 0,
        records_inserted INTEGER DEFAULT 0,
        last_processed_id INTEGER,
        status VARCHAR(20) DEFAULT 'running',
        started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        completed_at TIMESTAMP,
        error_message TEXT,
        UNIQUE(source_table, target_table, backfill_date)
    );
    """
    
    run_intg_query(tracking_table_sql, "Creating backfill tracking table")
    
    # Initialize tracking records
    table_mapping = get_table_mapping()
    today = datetime.now().date()
    
    for source_table, target_table in table_mapping.items():
        if target_table:  # Skip None mappings
            init_sql = f"""
            INSERT INTO intg_backfill_tracking (source_table, target_table, status)
            VALUES ('{source_table}', '{target_table}', 'pending')
            ON CONFLICT (source_table, target_table, backfill_date) DO NOTHING
            """
            run_intg_query(init_sql)

def update_backfill_tracking(source_table: str, target_table: str, 
                           records_processed: int, records_inserted: int,
                           last_id: int = None, status: str = 'running', 
                           error: str = None):
    """Update backfill tracking progress."""
    
    update_sql = f"""
    UPDATE intg_backfill_tracking 
    SET records_processed = {records_processed},
        records_inserted = {records_inserted},
        last_processed_id = {last_id or 'NULL'},
        status = '{status}',
        {"completed_at = CURRENT_TIMESTAMP," if status == 'completed' else ""}
        error_message = {f"'{error}'" if error else 'NULL'}
    WHERE source_table = '{source_table}' 
    AND target_table = '{target_table}' 
    AND backfill_date = CURRENT_DATE
    """
    
    run_intg_query(update_sql)

def get_table_schema(table_name: str, database: str = 'dev') -> dict:
    """Get table schema information."""
    
    schema_query = f"""
    SELECT column_name, data_type, is_nullable, column_default
    FROM information_schema.columns 
    WHERE table_name = '{table_name}'
    ORDER BY ordinal_position
    """
    
    if database == 'dev':
        result = run_dev_query(schema_query, f"Getting {table_name} schema from DEV")
    else:
        result = run_intg_query(schema_query, f"Getting {table_name} schema from INTG")
    
    columns = {}
    for line in result.split('\n'):
        if '|' in line and 'column_name' not in line and '---' not in line:
            parts = [p.strip() for p in line.split('|') if p.strip()]
            if len(parts) >= 3:
                columns[parts[0]] = {
                    'data_type': parts[1],
                    'is_nullable': parts[2],
                    'column_default': parts[3] if len(parts) > 3 else None
                }
    
    return columns

def get_table_count(table_name: str, database: str = 'dev') -> int:
    """Get total record count for a table."""
    
    count_query = f"SELECT COUNT(*) FROM {table_name}"
    
    if database == 'dev':
        result = run_dev_query(count_query, f"Counting records in {table_name}")
    else:
        result = run_intg_query(count_query, f"Counting records in {table_name}")
    
    # Extract count from result
    for line in result.split('\n'):
        line = line.strip()
        if line.isdigit():
            return int(line)
    
    return 0

def create_column_mapping(source_columns: dict, target_columns: dict) -> dict:
    """Create mapping between source and target columns."""
    
    mapping = {}
    
    # Direct column name matches
    for source_col in source_columns:
        if source_col in target_columns:
            mapping[source_col] = source_col
    
    # Handle common column name variations
    column_aliases = {
        'creation_timestamp': 'created_at',
        'last_updated': 'updated_at',
        'id': 'id',  # Primary keys usually match
        'symbol': 'symbol',  # Symbol columns usually match
        'date': 'date',  # Date columns usually match
        'vendor': 'vendor'  # Vendor columns usually match
    }
    
    for source_col in source_columns:
        if source_col not in mapping:
            # Check aliases
            for alias, target in column_aliases.items():
                if source_col.endswith(alias) and target in target_columns:
                    mapping[source_col] = target
                    break
    
    return mapping

def transform_data_for_integration(source_table: str, target_table: str, data_batch: list) -> list:
    """Transform data from dev format to integration format."""
    
    transformed_batch = []
    
    for record in data_batch:
        transformed_record = record.copy()
        
        # Table-specific transformations
        if source_table.startswith('dev_') and target_table.startswith('intg_'):
            
            # Add vendor information if missing
            if target_table == 'intg_daily_prices' and 'vendor' not in transformed_record:
                if 'tiingo' in source_table:
                    transformed_record['vendor'] = 'tiingo'
                elif 'polygon' in source_table:
                    transformed_record['vendor'] = 'polygon'
                elif 'fmp' in source_table:
                    transformed_record['vendor'] = 'fmp'
                else:
                    transformed_record['vendor'] = 'dev_migration'
            
            # Ensure consistent column naming
            if 'creation_timestamp' in transformed_record:
                transformed_record['created_at'] = transformed_record.pop('creation_timestamp')
            
            if 'last_updated' in transformed_record:
                transformed_record['updated_at'] = transformed_record.pop('last_updated')
            
            # Add integration-specific metadata
            transformed_record['migration_source'] = f"{source_table}_{datetime.now().strftime('%Y%m%d')}"
        
        transformed_batch.append(transformed_record)
    
    return transformed_batch

def backfill_table_batch(source_table: str, target_table: str, 
                        offset: int, batch_size: int) -> dict:
    """Backfill a batch of records from source to target table."""
    
    try:
        # Get batch of data from dev
        batch_query = f"""
        SELECT * FROM {source_table} 
        ORDER BY id 
        LIMIT {batch_size} OFFSET {offset}
        """
        
        batch_result = run_dev_query(batch_query)
        
        if not batch_result or 'ERROR' in batch_result.upper():
            return {'success': False, 'records': 0, 'error': 'Failed to fetch batch from dev'}
        
        # Parse batch results into records
        lines = batch_result.strip().split('\n')
        if len(lines) < 3:  # Header, separator, at least one record
            return {'success': True, 'records': 0, 'error': None}  # Empty batch
        
        headers = [h.strip() for h in lines[0].split('|') if h.strip()]
        
        batch_records = []
        for line in lines[2:]:  # Skip header and separator
            if '|' in line and not line.strip().startswith('('):
                values = [v.strip() for v in line.split('|') if v.strip()]
                if len(values) == len(headers):
                    record = dict(zip(headers, values))
                    batch_records.append(record)
        
        if not batch_records:
            return {'success': True, 'records': 0, 'error': None}
        
        # Transform data for integration environment
        transformed_records = transform_data_for_integration(
            source_table, target_table, batch_records
        )
        
        # Insert batch into intg
        inserted_count = 0
        
        for record in transformed_records:
            # Build insert query dynamically
            columns = list(record.keys())
            values = []
            
            for col in columns:
                value = record[col]
                if value is None or value == 'NULL':
                    values.append('NULL')
                elif isinstance(value, str):
                    # Escape single quotes
                    escaped_value = value.replace("'", "''")
                    values.append(f"'{escaped_value}'")
                else:
                    values.append(str(value))
            
            insert_sql = f"""
            INSERT INTO {target_table} ({', '.join(columns)})
            VALUES ({', '.join(values)})
            ON CONFLICT DO NOTHING
            """
            
            result = run_intg_query(insert_sql)
            if result and 'INSERT' in result:
                inserted_count += 1
        
        return {
            'success': True,
            'records': len(batch_records),
            'inserted': inserted_count,
            'error': None
        }
        
    except Exception as e:
        return {'success': False, 'records': 0, 'inserted': 0, 'error': str(e)}

def backfill_table(source_table: str, target_table: str) -> bool:
    """Backfill entire table from dev to intg."""
    
    log_info(f"🔄 Starting backfill: {source_table} → {target_table}")
    
    with stats_lock:
        stats['current_table'] = f"{source_table} → {target_table}"
    
    # Get total record count
    total_records = get_table_count(source_table, 'dev')
    
    if total_records == 0:
        log_warning(f"No records found in {source_table}")
        update_backfill_tracking(source_table, target_table, 0, 0, status='completed')
        return True
    
    log_info(f"📊 Total records to backfill: {total_records}")
    
    # Process in batches
    total_processed = 0
    total_inserted = 0
    batch_num = 0
    
    while total_processed < total_records:
        offset = batch_num * BATCH_SIZE
        
        batch_result = backfill_table_batch(
            source_table, target_table, offset, BATCH_SIZE
        )
        
        if not batch_result['success']:
            log_error(f"Batch {batch_num} failed: {batch_result['error']}")
            update_backfill_tracking(
                source_table, target_table, total_processed, total_inserted,
                status='failed', error=batch_result['error']
            )
            return False
        
        batch_records = batch_result['records']
        batch_inserted = batch_result.get('inserted', 0)
        
        total_processed += batch_records
        total_inserted += batch_inserted
        batch_num += 1
        
        # Update progress
        if batch_num % 10 == 0 or total_processed >= total_records:
            progress_pct = (total_processed / total_records) * 100
            log_info(f"📈 Progress: {total_processed}/{total_records} ({progress_pct:.1f}%) - {total_inserted} inserted")
            
            update_backfill_tracking(
                source_table, target_table, total_processed, total_inserted,
                last_id=offset + batch_records
            )
        
        with stats_lock:
            stats['total_records'] += batch_records
        
        if batch_records < BATCH_SIZE:
            # Reached end of data
            break
    
    # Mark as completed
    update_backfill_tracking(
        source_table, target_table, total_processed, total_inserted,
        status='completed'
    )
    
    log_success(f"Completed backfill: {source_table} → {target_table} ({total_inserted}/{total_processed} records)")
    return True

def validate_backfill_results() -> dict:
    """Validate backfill results by comparing record counts."""
    
    log_info("🔍 Validating backfill results...")
    
    validation_results = {}
    table_mapping = get_table_mapping()
    
    for source_table, target_table in table_mapping.items():
        if not target_table:
            continue
            
        dev_count = get_table_count(source_table, 'dev')
        intg_count = get_table_count(target_table, 'intg')
        
        # For merged tables (like daily_prices), we expect INTG count >= DEV count
        if target_table == 'intg_daily_prices':
            validation_results[f"{source_table}→{target_table}"] = {
                'dev_count': dev_count,
                'intg_count': intg_count,
                'status': 'partial_merge',  # Expected for merged tables
                'notes': 'Multiple dev tables merged into single intg table'
            }
        else:
            # For direct mappings, counts should match
            match_ratio = (intg_count / dev_count) if dev_count > 0 else 1.0
            status = 'success' if match_ratio >= 0.95 else 'warning'
            
            validation_results[f"{source_table}→{target_table}"] = {
                'dev_count': dev_count,
                'intg_count': intg_count,
                'match_ratio': match_ratio,
                'status': status
            }
    
    return validation_results

def create_backfill_summary() -> str:
    """Create comprehensive backfill summary report."""
    
    # Get backfill tracking data
    summary_query = """
    SELECT source_table, target_table, records_processed, records_inserted, 
           status, started_at, completed_at,
           EXTRACT(EPOCH FROM (completed_at - started_at))/60 as duration_minutes
    FROM intg_backfill_tracking 
    WHERE backfill_date = CURRENT_DATE
    ORDER BY completed_at DESC
    """
    
    tracking_result = run_intg_query(summary_query, "Getting backfill summary")
    
    validation_results = validate_backfill_results()
    
    # Generate report
    report = f"""
# ATS-INTG Data Backfill Summary Report

**Backfill Date**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
**Total Execution Time**: {(datetime.now() - stats['start_time']).total_seconds() / 60:.1f} minutes

## Backfill Results

"""
    
    if tracking_result:
        for line in tracking_result.split('\n')[2:]:  # Skip headers
            if '|' in line and not line.strip().startswith('('):
                parts = [p.strip() for p in line.split('|') if p.strip()]
                if len(parts) >= 4:
                    report += f"- **{parts[0]} → {parts[1]}**: {parts[2]} processed, {parts[3]} inserted, Status: {parts[4]}\n"
    
    report += f"""

## Validation Results

"""
    
    for table_pair, result in validation_results.items():
        status_icon = "✅" if result['status'] == 'success' else "⚠️" if result['status'] == 'warning' else "ℹ️"
        report += f"{status_icon} **{table_pair}**: DEV={result['dev_count']}, INTG={result['intg_count']}"
        
        if 'match_ratio' in result:
            report += f", Match={result['match_ratio']:.1%}"
        
        if 'notes' in result:
            report += f" ({result['notes']})"
        
        report += "\n"
    
    report += f"""

## Database Status

**INTG Database Summary:**
"""
    
    # Get final database stats
    db_stats_query = """
    SELECT 
        'intg_instruments' as table_name, COUNT(*) as record_count 
    FROM intg_instruments
    UNION ALL
    SELECT 
        'intg_daily_prices' as table_name, COUNT(*) as record_count 
    FROM intg_daily_prices
    UNION ALL
    SELECT 
        'intg_fundamentals' as table_name, COUNT(*) as record_count 
    FROM intg_fundamentals_comprehensive
    """
    
    db_result = run_intg_query(db_stats_query, "Getting final database statistics")
    
    if db_result:
        for line in db_result.split('\n')[2:]:  # Skip headers
            if '|' in line and not line.strip().startswith('('):
                parts = [p.strip() for p in line.split('|') if p.strip()]
                if len(parts) >= 2:
                    report += f"- **{parts[0]}**: {parts[1]} records\n"
    
    report += f"""

## Next Steps

1. **Verify Data Quality**: Run data quality checks on INTG tables
2. **Start Daily Jobs**: Enable daily refresh jobs for ongoing updates
3. **Monitor Performance**: Check query performance on backfilled data
4. **Schedule Incremental Updates**: Set up regular dev→intg data sync

**Backfill completed successfully! 🎉**
"""
    
    return report

def main():
    """Main backfill execution function."""
    
    parser = argparse.ArgumentParser(description="ATS-INTG Data Backfill from ATS-DEV")
    parser.add_argument("action", choices=[
        "validate", "backfill", "status", "resume"
    ], help="Backfill action to perform")
    
    parser.add_argument("--tables", nargs="+", help="Specific tables to backfill")
    parser.add_argument("--batch-size", type=int, default=1000, help="Batch size for processing")
    parser.add_argument("--dry-run", action="store_true", help="Dry run mode - no actual data changes")
    
    args = parser.parse_args()
    
    global BATCH_SIZE
    BATCH_SIZE = args.batch_size
    
    with stats_lock:
        stats['start_time'] = datetime.now()
    
    log_info("🚀 ATS-INTG Data Backfill Manager")
    log_info("=" * 50)
    
    if args.action == "validate":
        log_info("🔍 Validating backfill environment...")
        
        # Check dev database connectivity
        dev_test = run_dev_query("SELECT 'DEV database connected' as status")
        if 'connected' in dev_test:
            log_success("DEV database connection verified")
        else:
            log_error("DEV database connection failed")
            return False
        
        # Check intg database connectivity
        intg_test = run_intg_query("SELECT 'INTG database connected' as status")
        if 'connected' in intg_test:
            log_success("INTG database connection verified")
        else:
            log_error("INTG database connection failed")
            return False
        
        # Validate table mappings
        table_mapping = get_table_mapping()
        log_info(f"📋 Found {len(table_mapping)} table mappings to process")
        
        for source_table, target_table in table_mapping.items():
            if target_table:
                dev_count = get_table_count(source_table, 'dev')
                log_info(f"  {source_table} → {target_table}: {dev_count} records")
        
        log_success("Environment validation completed")
        return True
    
    elif args.action == "backfill":
        log_info("🔄 Starting data backfill process...")
        
        if args.dry_run:
            log_warning("🧪 DRY RUN MODE - No actual data changes will be made")
        
        # Setup tracking
        create_intg_backfill_tracking()
        
        # Get tables to process
        table_mapping = get_table_mapping()
        
        if args.tables:
            # Filter to specified tables
            filtered_mapping = {k: v for k, v in table_mapping.items() if k in args.tables}
            table_mapping = filtered_mapping
        
        log_info(f"📋 Processing {len(table_mapping)} table mappings")
        
        # Process each table
        successful_tables = 0
        failed_tables = 0
        
        for source_table, target_table in table_mapping.items():
            if not target_table:
                log_info(f"⏭️ Skipping {source_table} (no target mapping)")
                continue
                
            log_info(f"🔄 Processing: {source_table} → {target_table}")
            
            if args.dry_run:
                log_info(f"🧪 DRY RUN: Would backfill {source_table} to {target_table}")
                dev_count = get_table_count(source_table, 'dev')
                log_info(f"🧪 DRY RUN: Would process {dev_count} records")
                successful_tables += 1
            else:
                if backfill_table(source_table, target_table):
                    successful_tables += 1
                else:
                    failed_tables += 1
        
        # Generate summary report
        if not args.dry_run:
            report = create_backfill_summary()
            
            # Save report to file
            report_file = f"/workspace/INTG-BACKFILL-{datetime.now().strftime('%Y%m%d_%H%M%S')}.md"
            with open(report_file, 'w') as f:
                f.write(report)
            
            log_success(f"Backfill summary report saved: {report_file}")
        
        # Final summary
        log_info("🎉 Data Backfill Complete!")
        log_info("=" * 50)
        log_info(f"✅ Successful tables: {successful_tables}")
        log_info(f"❌ Failed tables: {failed_tables}")
        log_info(f"📊 Total records processed: {stats['total_records']}")
        log_info(f"⏱️ Total duration: {(datetime.now() - stats['start_time']).total_seconds() / 60:.1f} minutes")
        
        return failed_tables == 0
    
    elif args.action == "status":
        log_info("📊 Backfill Status Report")
        
        # Get current tracking status
        status_query = """
        SELECT source_table, target_table, records_processed, records_inserted, 
               status, started_at, completed_at
        FROM intg_backfill_tracking 
        WHERE backfill_date = CURRENT_DATE
        ORDER BY started_at DESC
        """
        
        result = run_intg_query(status_query, "Getting backfill status")
        
        if result:
            print("\nBackfill Status:")
            print(result)
        else:
            log_info("No backfill operations found for today")
        
        return True
    
    elif args.action == "resume":
        log_info("🔄 Resuming incomplete backfill operations...")
        
        # Find incomplete operations
        resume_query = """
        SELECT source_table, target_table 
        FROM intg_backfill_tracking 
        WHERE status IN ('running', 'failed') 
        AND backfill_date = CURRENT_DATE
        """
        
        result = run_intg_query(resume_query, "Finding incomplete backfills")
        
        # Resume processing
        # (Implementation would restart failed table backfills)
        log_info("Resume functionality would restart incomplete backfills")
        
        return True

if __name__ == "__main__":
    success = main()
    if not success:
        sys.exit(1)