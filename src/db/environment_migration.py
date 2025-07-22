"""
Database migration utility for environment-specific table management.

This module provides utilities to create and manage environment-specific database
tables with proper prefixes (test_, intg_, prod_).
"""

import asyncio
import asyncpg
from typing import List, Dict, Any
from config.environment import get_environment, EnvironmentType


class EnvironmentMigration:
    """
    Handles database migrations for environment-specific tables.
    """
    
    def __init__(self, env_type: EnvironmentType = None):
        """
        Initialize migration utility.
        
        Args:
            env_type: Environment type. If None, uses current environment.
        """
        if env_type:
            from config.environment import set_environment
            set_environment(env_type)
        
        self.env = get_environment()
        self.db_config = self.env.get_database_config()
    
    async def create_database_if_not_exists(self):
        """Create the environment-specific database if it doesn't exist."""
        # Connect to postgres database to create the target database
        admin_config = self.db_config.copy()
        target_db = admin_config.pop('database')
        admin_config['database'] = 'postgres'
        
        admin_url = f"postgresql://{admin_config['user']}:{admin_config['password']}@{admin_config['host']}:{admin_config['port']}/postgres"
        
        conn = await asyncpg.connect(admin_url)
        try:
            # Check if database exists
            exists = await conn.fetchval(
                "SELECT 1 FROM pg_database WHERE datname = $1", target_db
            )
            
            if not exists:
                await conn.execute(f'CREATE DATABASE "{target_db}"')
                print(f"Created database: {target_db}")
            else:
                print(f"Database already exists: {target_db}")
                
        finally:
            await conn.close()
    
    async def get_connection_pool(self):
        """Get connection pool for the environment database."""
        return await asyncpg.create_pool(
            host=self.db_config['host'],
            port=self.db_config['port'],
            user=self.db_config['user'],
            password=self.db_config['password'],
            database=self.db_config['database'],
            min_size=self.db_config['min_size'],
            max_size=self.db_config['max_size'],
            command_timeout=self.db_config['command_timeout']
        )
    
    def get_table_definitions(self) -> Dict[str, str]:
        """
        Get table definitions for all required tables.
        
        Returns:
            Dictionary mapping table names to their CREATE TABLE SQL
        """
        return {
            "daily_prices": f"""
                CREATE TABLE IF NOT EXISTS {self.env.get_table_name("daily_prices")} (
                    date DATE NOT NULL,
                    symbol TEXT NOT NULL,
                    open DOUBLE PRECISION,
                    high DOUBLE PRECISION,
                    low DOUBLE PRECISION,
                    close DOUBLE PRECISION,
                    volume BIGINT,
                    source TEXT,
                    status TEXT,
                    note TEXT,
                    PRIMARY KEY (date, symbol)
                )
            """,
            
            "daily_prices_polygon": f"""
                CREATE TABLE IF NOT EXISTS {self.env.get_table_name("daily_prices_polygon")} (
                    date DATE NOT NULL,
                    symbol TEXT NOT NULL,
                    open DOUBLE PRECISION,
                    high DOUBLE PRECISION,
                    low DOUBLE PRECISION,
                    close DOUBLE PRECISION,
                    volume BIGINT,
                    market_cap DOUBLE PRECISION,
                    PRIMARY KEY (date, symbol)
                )
            """,
            
            "daily_prices_tiingo": f"""
                CREATE TABLE IF NOT EXISTS {self.env.get_table_name("daily_prices_tiingo")} (
                    date DATE NOT NULL,
                    symbol TEXT NOT NULL,
                    open DOUBLE PRECISION,
                    high DOUBLE PRECISION,
                    low DOUBLE PRECISION,
                    close DOUBLE PRECISION,
                    adjclose DOUBLE PRECISION,
                    volume BIGINT,
                    status_id INTEGER,
                    PRIMARY KEY (date, symbol)
                )
            """,
            
            "daily_adjusted_prices": f"""
                CREATE TABLE IF NOT EXISTS {self.env.get_table_name("daily_adjusted_prices")} (
                    date DATE NOT NULL,
                    symbol TEXT NOT NULL,
                    open DOUBLE PRECISION,
                    high DOUBLE PRECISION,
                    low DOUBLE PRECISION,
                    close DOUBLE PRECISION,
                    volume BIGINT,
                    market_cap DOUBLE PRECISION,
                    original_open DOUBLE PRECISION,
                    original_high DOUBLE PRECISION,
                    original_low DOUBLE PRECISION,
                    original_close DOUBLE PRECISION,
                    split_numerator DOUBLE PRECISION,
                    split_denominator DOUBLE PRECISION,
                    dividend_amount DOUBLE PRECISION,
                    PRIMARY KEY (date, symbol)
                )
            """,
            
            "instrument_polygon": f"""
                CREATE TABLE IF NOT EXISTS {self.env.get_table_name("instrument_polygon")} (
                    symbol TEXT PRIMARY KEY,
                    name TEXT,
                    exchange TEXT,
                    type TEXT,
                    currency TEXT,
                    figi TEXT,
                    isin TEXT,
                    cusip TEXT,
                    composite_figi TEXT,
                    active BOOLEAN,
                    list_date DATE,
                    delist_date DATE,
                    raw JSONB,
                    created_at TIMESTAMP WITH TIME ZONE,
                    updated_at TIMESTAMP WITH TIME ZONE
                )
            """,
            
            "splits": f"""
                CREATE TABLE IF NOT EXISTS {self.env.get_table_name("splits")} (
                    date DATE NOT NULL,
                    symbol TEXT NOT NULL,
                    numerator DOUBLE PRECISION NOT NULL,
                    denominator DOUBLE PRECISION NOT NULL,
                    PRIMARY KEY (date, symbol)
                )
            """,
            
            "universe": f"""
                CREATE TABLE IF NOT EXISTS {self.env.get_table_name("universe")} (
                    id SERIAL PRIMARY KEY,
                    name TEXT NOT NULL UNIQUE,
                    description TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """,
            
            "universe_membership": f"""
                CREATE TABLE IF NOT EXISTS {self.env.get_table_name("universe_membership")} (
                    id SERIAL PRIMARY KEY,
                    universe_id INTEGER NOT NULL,
                    symbol TEXT NOT NULL,
                    start_at DATE NOT NULL,
                    end_at DATE,
                    meta JSONB,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (universe_id) REFERENCES {self.env.get_table_name("universe")}(id)
                )
            """,
            
            "test_universe_membership": f"""
                CREATE TABLE IF NOT EXISTS {self.env.get_table_name("test_universe_membership")} (
                    date DATE NOT NULL,
                    symbol TEXT NOT NULL,
                    PRIMARY KEY (date, symbol)
                )
            """,
            
            "spy_membership_change": f"""
                CREATE TABLE IF NOT EXISTS {self.env.get_table_name("spy_membership_change")} (
                    id SERIAL PRIMARY KEY,
                    change_date DATE NOT NULL,
                    added TEXT,
                    removed TEXT
                )
            """,
            
            "status_code": f"""
                CREATE TABLE IF NOT EXISTS {self.env.get_table_name("status_code")} (
                    id SERIAL PRIMARY KEY,
                    code TEXT NOT NULL,
                    description TEXT
                )
            """,
            
            "events": f"""
                CREATE TABLE IF NOT EXISTS {self.env.get_table_name("events")} (
                    id SERIAL PRIMARY KEY,
                    date DATE NOT NULL,
                    symbol TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    amount DOUBLE PRECISION,
                    meta JSONB,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """,
            
            "dividends": f"""
                CREATE TABLE IF NOT EXISTS {self.env.get_table_name("dividends")} (
                    date DATE NOT NULL,
                    symbol TEXT NOT NULL,
                    amount DOUBLE PRECISION NOT NULL,
                    PRIMARY KEY (date, symbol)
                )
            """,
        }
    
    async def create_all_tables(self):
        """Create all required tables for the current environment."""
        pool = await self.get_connection_pool()
        
        try:
            async with pool.acquire() as conn:
                table_definitions = self.get_table_definitions()
                
                for table_name, create_sql in table_definitions.items():
                    try:
                        await conn.execute(create_sql)
                        prefixed_name = self.env.get_table_name(table_name)
                        print(f"Created table: {prefixed_name}")
                    except Exception as e:
                        print(f"Error creating table {table_name}: {e}")
                        raise
                        
        finally:
            await pool.close()
    
    async def create_indexes(self):
        """Create indexes for better query performance."""
        pool = await self.get_connection_pool()
        
        try:
            async with pool.acquire() as conn:
                indexes = [
                    f"CREATE INDEX IF NOT EXISTS idx_{self.env.env_type.value}_daily_prices_symbol ON {self.env.get_table_name('daily_prices')} (symbol)",
                    f"CREATE INDEX IF NOT EXISTS idx_{self.env.env_type.value}_daily_prices_date ON {self.env.get_table_name('daily_prices')} (date)",
                    f"CREATE INDEX IF NOT EXISTS idx_{self.env.env_type.value}_daily_prices_polygon_symbol ON {self.env.get_table_name('daily_prices_polygon')} (symbol)",
                    f"CREATE INDEX IF NOT EXISTS idx_{self.env.env_type.value}_daily_prices_polygon_date ON {self.env.get_table_name('daily_prices_polygon')} (date)",
                    f"CREATE INDEX IF NOT EXISTS idx_{self.env.env_type.value}_daily_prices_tiingo_symbol ON {self.env.get_table_name('daily_prices_tiingo')} (symbol)",
                    f"CREATE INDEX IF NOT EXISTS idx_{self.env.env_type.value}_daily_prices_tiingo_date ON {self.env.get_table_name('daily_prices_tiingo')} (date)",
                    f"CREATE INDEX IF NOT EXISTS idx_{self.env.env_type.value}_daily_adjusted_prices_symbol ON {self.env.get_table_name('daily_adjusted_prices')} (symbol)",
                    f"CREATE INDEX IF NOT EXISTS idx_{self.env.env_type.value}_daily_adjusted_prices_date ON {self.env.get_table_name('daily_adjusted_prices')} (date)",
                    f"CREATE INDEX IF NOT EXISTS idx_{self.env.env_type.value}_instrument_polygon_symbol ON {self.env.get_table_name('instrument_polygon')} (symbol)",
                    f"CREATE INDEX IF NOT EXISTS idx_{self.env.env_type.value}_universe_membership_symbol ON {self.env.get_table_name('universe_membership')} (symbol)",
                    f"CREATE INDEX IF NOT EXISTS idx_{self.env.env_type.value}_universe_membership_dates ON {self.env.get_table_name('universe_membership')} (start_at, end_at)",
                    f"CREATE INDEX IF NOT EXISTS idx_{self.env.env_type.value}_universe_membership_universe_id ON {self.env.get_table_name('universe_membership')} (universe_id)",
                    f"CREATE INDEX IF NOT EXISTS idx_{self.env.env_type.value}_spy_membership_change_date ON {self.env.get_table_name('spy_membership_change')} (change_date)",
                    f"CREATE INDEX IF NOT EXISTS idx_{self.env.env_type.value}_events_date ON {self.env.get_table_name('events')} (date)",
                    f"CREATE INDEX IF NOT EXISTS idx_{self.env.env_type.value}_events_symbol ON {self.env.get_table_name('events')} (symbol)",
                ]
                
                for index_sql in indexes:
                    try:
                        await conn.execute(index_sql)
                        print(f"Created index: {index_sql.split()[-1]}")
                    except Exception as e:
                        print(f"Error creating index: {e}")
                        
        finally:
            await pool.close()
    
    async def drop_all_tables(self):
        """Drop all environment-specific tables. USE WITH CAUTION!"""
        if self.env.env_type == EnvironmentType.PRODUCTION:
            raise ValueError("Cannot drop production tables through this utility!")
        
        pool = await self.get_connection_pool()
        
        try:
            async with pool.acquire() as conn:
                table_names = [
                    "universe_membership",  # Drop foreign key tables first
                    "events",
                    "dividends", 
                    "splits",
                    "daily_adjusted_prices",
                    "daily_prices",
                    "universe"
                ]
                
                for table_name in table_names:
                    prefixed_name = self.env.get_table_name(table_name)
                    try:
                        await conn.execute(f"DROP TABLE IF EXISTS {prefixed_name} CASCADE")
                        print(f"Dropped table: {prefixed_name}")
                    except Exception as e:
                        print(f"Error dropping table {prefixed_name}: {e}")
                        
        finally:
            await pool.close()
    
    async def setup_environment(self):
        """Complete environment setup: create database, tables, and indexes."""
        print(f"Setting up {self.env.env_type.value} environment...")
        
        try:
            await self.create_database_if_not_exists()
            await self.create_all_tables()
            await self.create_indexes()
            
            print(f"Environment {self.env.env_type.value} setup complete!")
            return True
        except Exception as e:
            print(f"Error setting up {self.env.env_type.value} environment: {e}")
            return False


async def main():
    """Command-line interface for environment migration."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Environment-specific database migration utility")
    parser.add_argument("--env", choices=["test", "intg", "prod"], default="test",
                       help="Environment to operate on")
    parser.add_argument("--action", choices=["setup", "create-tables", "create-indexes", "drop-tables"],
                       default="setup", help="Action to perform")
    
    args = parser.parse_args()
    
    env_type = EnvironmentType(args.env)
    migration = EnvironmentMigration(env_type)
    
    if args.action == "setup":
        await migration.setup_environment()
    elif args.action == "create-tables":
        await migration.create_all_tables()
    elif args.action == "create-indexes":
        await migration.create_indexes()
    elif args.action == "drop-tables":
        if input(f"Are you sure you want to drop all {args.env} tables? (yes/no): ") == "yes":
            await migration.drop_all_tables()
        else:
            print("Operation cancelled.")


if __name__ == "__main__":
    asyncio.run(main())
