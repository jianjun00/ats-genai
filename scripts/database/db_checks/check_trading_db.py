#!/usr/bin/env python3
"""Script to check instrument data in the Kubernetes or local trading database."""

import asyncio
import asyncpg
import os
import sys
import subprocess
import time
from dotenv import load_dotenv

async def check_trading_db(use_k8s=False, namespace='ats-dev'):
    """Check the trading database for instrument data.
    
    Args:
        use_k8s: If True, use port-forwarding to connect to the Kubernetes database
        namespace: Kubernetes namespace where the database is deployed
    """
    # Load environment variables from .env.dev
    load_dotenv('.env.dev')
    
    port_forward_process = None
    
    try:
        # Setup database connection details based on environment
        if use_k8s:
            print(f"Setting up port-forwarding to Kubernetes database in {namespace} namespace...")
            # Get the postgres pod name
            pod_cmd = f"kubectl get pods -n {namespace} -l app=postgres -o name"
            pod_result = subprocess.run(pod_cmd, shell=True, capture_output=True, text=True)
            
            if pod_result.returncode != 0 or not pod_result.stdout.strip():
                print(f"Error: Could not find postgres pod in {namespace} namespace")
                return
                
            pod_name = pod_result.stdout.strip().replace('pod/', '')
            print(f"Found postgres pod: {pod_name}")
            
            # Setup port-forwarding
            port_forward_cmd = f"kubectl port-forward -n {namespace} {pod_name} 5433:5432"
            print(f"Starting port-forwarding: {port_forward_cmd}")
            port_forward_process = subprocess.Popen(port_forward_cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            
            # Wait for port-forwarding to establish
            time.sleep(2)
            
            # Check if port-forwarding is working
            if port_forward_process.poll() is not None:
                stderr = port_forward_process.stderr.read().decode('utf-8')
                print(f"Error: Port-forwarding failed: {stderr}")
                return
                
            print("Port-forwarding established successfully")
            db_url = 'postgresql://postgres:postgres@localhost:5433/ats_dev'
        else:
            # Use local database connection
            db_url = os.getenv('TSDB_URL', 'postgresql://postgres:password@localhost:5432/trading_db_dev')
    except Exception as e:
        print(f"Error setting up database connection: {e}")
        return
    
    print(f"Connecting to database: {db_url}")
    
    try:
        # Connect to the database
        conn = await asyncpg.connect(db_url)
        try:
            # List all tables
            tables = await conn.fetch(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = 'public' ORDER BY table_name"
            )
            print("\n=== Tables in database ===")
            for table in tables:
                print(f"- {table['table_name']}")
            
            # Check for instrument tables - adapt to dev_ prefix in Kubernetes
            if use_k8s:
                instrument_tables = [t['table_name'] for t in tables 
                                   if 'instrument' in t['table_name'].lower() or 
                                   ('dev_instrument' in t['table_name'].lower())]
            else:
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
                
            # Check for instrument_xrefs table specifically - adapt to dev_ prefix in Kubernetes
            xref_tables = []
            if use_k8s:
                if any(t['table_name'] == 'dev_instrument_xrefs' for t in tables):
                    xref_tables.append(('dev_instrument_xrefs', 'vendor_id'))
            else:
                if any(t['table_name'] == 'instrument_xrefs' for t in tables):
                    xref_tables.append(('instrument_xrefs', 'provider_id'))
                if any(t['table_name'] == 'intg_instrument_xrefs' for t in tables):
                    xref_tables.append(('intg_instrument_xrefs', 'provider_id'))
            
            for table_name, id_column in xref_tables:
                print(f"\n=== Cross-references in {table_name} by {id_column} ===")
                try:
                    xrefs_by_provider = await conn.fetch(
                        f"SELECT {id_column}, COUNT(*) FROM {table_name} GROUP BY {id_column}"
                    )
                    for row in xrefs_by_provider:
                        print(f"- {id_column} {row[id_column]}: {row['count']} instruments")
                except Exception as e:
                    print(f"Error querying {table_name}: {e}")
                    
        finally:
            await conn.close()
    except Exception as e:
        print(f"Error connecting to database: {e}")
    finally:
        # Clean up port-forwarding process if it exists
        if port_forward_process:
            print("Stopping port-forwarding...")
            port_forward_process.terminate()
            port_forward_process.wait()
            print("Port-forwarding stopped")

if __name__ == "__main__":
    # Check if we should use Kubernetes
    use_k8s = len(sys.argv) > 1 and sys.argv[1] == '--k8s'
    namespace = 'ats-dev'
    
    # If additional arguments are provided, use them as the namespace
    if use_k8s and len(sys.argv) > 2:
        namespace = sys.argv[2]
    
    print(f"Checking trading database {'in Kubernetes' if use_k8s else 'locally'}")
    asyncio.run(check_trading_db(use_k8s, namespace))
    
    print("\nDone!")

