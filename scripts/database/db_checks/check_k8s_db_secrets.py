#!/usr/bin/env python3
"""Script to check database connection using credentials from Kubernetes secrets."""

import os
import sys
import argparse
import asyncio
import asyncpg
import subprocess
from pathlib import Path

# Add project root to Python path
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

async def get_secret_value(namespace, secret_name, key):
    """Get a value from a Kubernetes secret."""
    try:
        cmd = [
            "kubectl", "get", "secret", secret_name, 
            "-n", namespace, 
            "-o", f"jsonpath={{.data.{key}}}"
        ]
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        if not result.stdout:
            print(f"Warning: Key '{key}' not found in secret '{secret_name}'")
            return None
            
        # Decode base64 value
        import base64
        return base64.b64decode(result.stdout).decode('utf-8')
    except subprocess.CalledProcessError as e:
        print(f"Error retrieving secret: {e}")
        print(f"Error output: {e.stderr}")
        return None

async def check_db_connection(namespace, secret_name):
    """Check database connection using credentials from Kubernetes secrets."""
    print(f"Retrieving database credentials from secret '{secret_name}' in namespace '{namespace}'...")
    
    # Get database connection details from Kubernetes secret
    db_host = await get_secret_value(namespace, secret_name, "DB_HOST") or "timescaledb"
    db_port = await get_secret_value(namespace, secret_name, "DB_PORT") or "5432"
    db_user = await get_secret_value(namespace, secret_name, "DB_USER")
    db_password = await get_secret_value(namespace, secret_name, "DB_PASSWORD")
    db_name = await get_secret_value(namespace, secret_name, "DB_NAME")
    tsdb_url = await get_secret_value(namespace, secret_name, "TSDB_URL")
    
    # If TSDB_URL is available, use it directly
    if tsdb_url:
        db_url = tsdb_url
        print(f"Using TSDB_URL from secret: {db_url}")
    else:
        # Otherwise construct URL from individual components
        if not all([db_host, db_port, db_user, db_password, db_name]):
            print("Error: Missing required database credentials in secret")
            print(f"Host: {'Set' if db_host else 'Not set'}")
            print(f"Port: {'Set' if db_port else 'Not set'}")
            print(f"User: {'Set' if db_user else 'Not set'}")
            print(f"Password: {'Set' if db_password else 'Not set'}")
            print(f"Database: {'Set' if db_name else 'Not set'}")
            return
            
        db_url = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
        print(f"Constructed database URL from components")
    
    print(f"Database Host: {db_host}")
    print(f"Database Port: {db_port}")
    print(f"Database User: {db_user}")
    print(f"Database Name: {db_name}")
    print(f"Database Password: {'*' * len(db_password) if db_password else 'Not set'}")
    
    try:
        # Try to connect to the database
        print("\nAttempting to connect to the database...")
        conn = await asyncpg.connect(db_url)
        
        print("✅ Connection successful!")
        
        # List all databases
        print("\n=== Available Databases ===")
        databases = await conn.fetch("SELECT datname FROM pg_database WHERE datistemplate = false")
        for db in databases:
            print(f"- {db['datname']}")
        
        # List all tables in the current database
        print(f"\n=== Tables in current database ===")
        tables = await conn.fetch(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = 'public' ORDER BY table_name"
        )
        
        if tables:
            for table in tables:
                print(f"- {table['table_name']}")
                
            # Check for instrument tables
            instrument_tables = [t['table_name'] for t in tables 
                               if 'instrument' in t['table_name'].lower()]
            
            if instrument_tables:
                print("\n=== Instrument-related tables ===")
                for table in instrument_tables:
                    print(f"- {table}")
                    
                    # Count rows in each instrument table
                    count = await conn.fetchval(f"SELECT COUNT(*) FROM {table}")
                    print(f"  Count: {count} rows")
                    
                    # Show sample data if table has rows
                    if count > 0:
                        sample = await conn.fetch(f"SELECT * FROM {table} LIMIT 3")
                        print(f"  Sample data:")
                        for row in sample:
                            print(f"    {dict(row)}")
            else:
                print("\nNo instrument-related tables found.")
        else:
            print("No tables found in the database.")
        
        await conn.close()
        return True
    except Exception as e:
        print(f"❌ Error connecting to database: {e}")
        
        # Try to connect to postgres database directly
        try:
            print("\nAttempting to connect to postgres database directly...")
            if tsdb_url:
                postgres_url = tsdb_url.rsplit('/', 1)[0] + '/postgres'
            else:
                postgres_url = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/postgres"
                
            conn = await asyncpg.connect(postgres_url)
            print("✅ Connection to postgres database successful!")
            
            # List all databases
            print("\n=== Available Databases ===")
            databases = await conn.fetch("SELECT datname FROM pg_database WHERE datistemplate = false")
            for db in databases:
                print(f"- {db['datname']}")
                
            await conn.close()
            return False
        except Exception as e2:
            print(f"❌ Error connecting to postgres database: {e2}")
            return False

def main():
    parser = argparse.ArgumentParser(description='Check database connection using Kubernetes secrets')
    parser.add_argument('--namespace', '-n', default='ats-dev', help='Kubernetes namespace (default: ats-dev)')
    parser.add_argument('--secret', '-s', default='db-credentials-dev', help='Secret name (default: db-credentials-dev)')
    
    args = parser.parse_args()
    
    # Run the database connection check
    success = asyncio.run(check_db_connection(args.namespace, args.secret))
    
    # Exit with appropriate status code
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()
