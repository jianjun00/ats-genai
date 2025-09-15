#!/usr/bin/env python3
"""
Database Migration Runner for ATS Platform

This script applies database migrations from the src/db/migrations directory.
It supports environment-specific table prefixes and tracks migration state.
"""

import os
import sys
import argparse
import psycopg2
from pathlib import Path
from typing import List, Tuple
import logging

# Add src to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class MigrationRunner:
    def __init__(self, environment: str = 'dev'):
        self.environment = environment
        self.table_prefix = f"{environment}_"
        
        # Database configuration
        if environment == 'intg':
            self.db_config = {
                'host': 'localhost',
                'port': 4432,
                'user': 'postgres', 
                'password': 'intg_password',
                'database': 'intg_db'
            }
        else:  # dev
            self.db_config = {
                'host': 'localhost',
                'port': 3432,
                'user': 'postgres',
                'password': 'dev_password', 
                'database': 'dev_db'
            }
        
        self.migrations_dir = Path(__file__).parent / 'migrations'
        
    def get_connection(self):
        """Get database connection."""
        return psycopg2.connect(**self.db_config)
    
    def get_applied_migrations(self) -> List[int]:
        """Get list of applied migration versions."""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    # Check if db_version table exists
                    cur.execute(f"""
                        SELECT EXISTS (
                            SELECT FROM information_schema.tables 
                            WHERE table_name = '{self.table_prefix}db_version'
                        )
                    """)
                    
                    if not cur.fetchone()[0]:
                        return []
                    
                    # Get applied migrations
                    cur.execute(f"SELECT version FROM {self.table_prefix}db_version ORDER BY version")
                    return [row[0] for row in cur.fetchall()]
                    
        except Exception as e:
            logger.warning(f"Could not get applied migrations: {e}")
            return []
    
    def get_available_migrations(self) -> List[Tuple[int, Path]]:
        """Get list of available migration files."""
        migrations = []
        
        for file_path in sorted(self.migrations_dir.glob('*.sql')):
            if file_path.name.startswith('README'):
                continue
                
            try:
                # Extract migration number from filename (e.g., 001_create_tables.sql)
                version = int(file_path.name.split('_')[0])
                migrations.append((version, file_path))
            except (ValueError, IndexError):
                logger.warning(f"Skipping invalid migration file: {file_path}")
                
        return sorted(migrations)
    
    def apply_table_prefix(self, sql: str) -> str:
        """Apply environment-specific table prefix to SQL."""
        if self.environment == 'test':
            # For test environment, don't prefix tables that are already prefixed
            return sql
            
        # List of table names to prefix (based on intg schema)
        tables_to_prefix = [
            'db_version', 'vendors', 'status_code', 'instrument', 'instrument_aliases',
            'instrument_metadata', 'instrument_xrefs', 'universe', 'universe_membership', 
            'universe_membership_changes', 'daily_price_tiingo', 'daily_price_polygon',
            'daily_price_eodhd', 'daily_market_cap', 'fundamental', 'dividend', 'stock_splits',
            'dividend_polygon', 'stock_splits_polygon', 'instrument_interval', 
            'instrument_indicator_interval', 'factor_interval', 'universe_state_interval',
            'runs', 'portfolio_holdings', 'portfolio_performance', 'risk_metrics',
            'comprehensive_backtest_runs', 'backtest_trades', 'economic_event_types',
            'economic_events', 'economic_events_fred', 'economic_events_alpha_vantage',
            'economic_events_polygon', 'economic_events_tiingo', 'economic_events_eodhd',
            'earnings_events', 'financial_events', 'news', 'news_polygon', 'realtime_news',
            'training_dataset', 'monthly_training_data', 'model_comparisons', 'sr_levels',
            'sr_events', 'sr_tests', 'gap_events', 'market_regimes', 'symbol_performance',
            'user_preferences', 'dashboard_configs', 'vendor_api_health', 'api_calls',
            'minute_bar_api_calls', 'news_api_calls', 'data_quality_issues',
            'data_quality_metrics', 'data_quality_agent_operations', 'data_quality_alert_config',
            'minute_bar_collection_metrics', 'news_collection_metrics', 'realtime_minute_bars',
            'one_minute_live_polygon', 'one_minute_live_tiingo', 'one_minute_live_eodhd'
        ]
        
        # Apply prefix to table names
        for table in tables_to_prefix:
            # Handle various SQL patterns
            sql = sql.replace(f"CREATE TABLE IF NOT EXISTS {table}", 
                            f"CREATE TABLE IF NOT EXISTS {self.table_prefix}{table}")
            sql = sql.replace(f"INSERT INTO {table}", 
                            f"INSERT INTO {self.table_prefix}{table}")
            sql = sql.replace(f"REFERENCES {table}(", 
                            f"REFERENCES {self.table_prefix}{table}(")
            sql = sql.replace(f"FROM {table}", 
                            f"FROM {self.table_prefix}{table}")
            sql = sql.replace(f"ON {table}(", 
                            f"ON {self.table_prefix}{table}(")
            
        return sql
    
    def apply_migration(self, version: int, file_path: Path) -> bool:
        """Apply a single migration."""
        logger.info(f"Applying migration {version}: {file_path.name}")
        
        try:
            # Read migration file
            with open(file_path, 'r') as f:
                sql = f.read()
            
            # Apply table prefix
            sql = self.apply_table_prefix(sql)
            
            # Execute migration
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(sql)
                    
            logger.info(f"✅ Migration {version} applied successfully")
            return True
            
        except Exception as e:
            logger.error(f"❌ Migration {version} failed: {e}")
            return False
    
    def migrate(self) -> bool:
        """Apply all pending migrations."""
        applied = set(self.get_applied_migrations())
        available = self.get_available_migrations()
        
        if not available:
            logger.info("No migration files found")
            return True
        
        pending = [(v, f) for v, f in available if v not in applied]
        
        if not pending:
            logger.info("All migrations already applied")
            return True
        
        logger.info(f"Applying {len(pending)} pending migrations...")
        
        success = True
        for version, file_path in pending:
            if not self.apply_migration(version, file_path):
                success = False
                break
        
        if success:
            logger.info("🎉 All migrations applied successfully")
        else:
            logger.error("💥 Migration failed - database may be in inconsistent state")
            
        return success
    
    def status(self):
        """Show migration status."""
        applied = set(self.get_applied_migrations())
        available = self.get_available_migrations()
        
        print(f"\n📊 Migration Status for {self.environment} environment:")
        print(f"Database: {self.db_config['host']}:{self.db_config['port']}/{self.db_config['database']}")
        print(f"Table prefix: {self.table_prefix}")
        print()
        
        if not available:
            print("No migration files found")
            return
        
        for version, file_path in available:
            status = "✅ APPLIED" if version in applied else "⏳ PENDING"
            print(f"Migration {version:03d}: {status} - {file_path.name}")
        
        pending_count = len([v for v, _ in available if v not in applied])
        print(f"\nTotal migrations: {len(available)}")
        print(f"Applied: {len(applied)}")
        print(f"Pending: {pending_count}")

def main():
    parser = argparse.ArgumentParser(description='ATS Database Migration Runner')
    parser.add_argument('--environment', '-e', choices=['dev', 'intg', 'prod', 'test'], 
                       default='dev', help='Target environment')
    parser.add_argument('command', choices=['migrate', 'status'], 
                       help='Command to execute')
    
    args = parser.parse_args()
    
    runner = MigrationRunner(args.environment)
    
    if args.command == 'migrate':
        success = runner.migrate()
        sys.exit(0 if success else 1)
    elif args.command == 'status':
        runner.status()

if __name__ == '__main__':
    main()