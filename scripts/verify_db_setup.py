#!/usr/bin/env python3
"""
Script to verify database setup in Kubernetes.
This script:
1. Port-forwards the database service
2. Tests the database connection
3. Verifies the database tables and schema
"""

import os
import subprocess
import sys
import time
import psycopg2
from psycopg2 import sql
from contextlib import contextmanager

# Database connection parameters
DB_HOST = "localhost"
DB_PORT = 5460  # Port-forwarded port
DB_USER = "postgres"
DB_PASS = "postgres"
DB_NAME = "dev_db"

def run_command(command, check=True):
    """Run a command and return the output."""
    try:
        result = subprocess.run(
            command,
            check=check,
            capture_output=True,
            text=True,
            shell=True
        )
        return True, result.stdout
    except subprocess.CalledProcessError as e:
        return False, f"Error: {e.stderr}"

@contextmanager
def port_forward_database(namespace="ats-dev", local_port=5460):
    """Port forward the database service to a local port."""
    print(f"Setting up port forwarding from localhost:{local_port} to postgres:5432 in namespace {namespace}...")
    
    # Start port-forwarding in the background
    process = subprocess.Popen(
        f"kubectl port-forward service/postgres {local_port}:5432 -n {namespace}",
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )
    
    try:
        # Give it a moment to establish
        time.sleep(3)
        
        # Check if process is still running
        if process.poll() is not None:
            stderr = process.stderr.read().decode('utf-8')
            print(f"Error setting up port-forwarding: {stderr}")
            raise Exception("Port forwarding failed to start")
        
        print(f"Port forwarding established on port {local_port}")
        yield
    finally:
        # Terminate the port-forwarding process
        if process.poll() is None:
            process.terminate()
            process.wait()
            print("Port forwarding stopped")

def test_database_connection():
    """Test connection to the database."""
    print(f"\nTesting database connection to {DB_HOST}:{DB_PORT}/{DB_NAME} as {DB_USER}...")
    
    try:
        # Connect to the database
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            password=DB_PASS,
            dbname=DB_NAME
        )
        
        print("✓ Database connection successful!")
        
        # Create a cursor
        cur = conn.cursor()
        
        # Check tables
        print("\nChecking database tables...")
        cur.execute("""
            SELECT table_name FROM information_schema.tables 
            WHERE table_schema = 'public'
            ORDER BY table_name
        """)
        
        tables = cur.fetchall()
        if tables:
            print(f"Found {len(tables)} tables:")
            for table in tables:
                table_name = table[0]
                cur.execute(sql.SQL("SELECT COUNT(*) FROM {}").format(sql.Identifier(table_name)))
                count = cur.fetchone()[0]
                print(f"- {table_name}: {count} rows")
        else:
            print("No tables found in the database.")
        
        # Check if specific tables exist
        print("\nChecking for dev_ tables...")
        cur.execute("""
            SELECT table_name FROM information_schema.tables 
            WHERE table_schema = 'public' AND table_name LIKE 'dev\\_%'
            ORDER BY table_name
        """)
        
        dev_tables = cur.fetchall()
        if dev_tables:
            print(f"✓ Found {len(dev_tables)} dev_ tables:")
            for table in dev_tables:
                print(f"- {table[0]}")
        else:
            print("✗ No dev_ tables found. This might indicate an issue with database initialization.")
        
        # Check vendors table as a sample
        if any(table[0] == 'dev_vendors' for table in dev_tables):
            print("\nChecking dev_vendors table...")
            # First, get the column names to ensure we query only existing columns
            cur.execute("""
                SELECT column_name FROM information_schema.columns 
                WHERE table_schema = 'public' AND table_name = 'dev_vendors'
            """)
            columns = [col[0] for col in cur.fetchall()]
            print(f"Available columns: {', '.join(columns)}")
            
            # Query the vendors table with the columns that exist
            query = sql.SQL("SELECT {} FROM dev_vendors").format(
                sql.SQL(', ').join(map(sql.Identifier, columns))
            )
            cur.execute(query)
            vendors = cur.fetchall()
            if vendors:
                print(f"✓ Found {len(vendors)} vendors:")
                for vendor in vendors:
                    vendor_info = {columns[i]: vendor[i] for i in range(len(columns))}
                    print(f"- Vendor: {vendor_info}")
            else:
                print("✗ No vendors found in dev_vendors table.")
        
        # Close the connection
        cur.close()
        conn.close()
        print("\n✓ Database verification completed successfully.")
        return True
        
    except Exception as e:
        print(f"✗ Error: {e}")
        return False

def main():
    """Main function."""
    try:
        with port_forward_database():
            success = test_database_connection()
            return 0 if success else 1
    except Exception as e:
        print(f"Error: {e}")
        return 1

if __name__ == "__main__":
    sys.exit(main())
