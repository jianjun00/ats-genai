#!/usr/bin/env python3
"""
Script to check instrument counts in the database.

This script can work with both local database and Kubernetes database.
Use --k8s flag to connect to the Kubernetes database.
"""

import asyncio
import sys
import os
import argparse
import subprocess
import time
import asyncpg

async def check_instruments_direct(use_k8s=False, namespace='ats-dev'):
    """Check instruments directly using asyncpg without DAOs."""
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
                return 1
                
            pod_name = pod_result.stdout.strip().replace('pod/', '')
            print(f"Found postgres pod: {pod_name}")
            
            # Setup port-forwarding
            port_forward_cmd = f"kubectl port-forward -n {namespace} {pod_name} 5434:5432"
            print(f"Starting port-forwarding: {port_forward_cmd}")
            port_forward_process = subprocess.Popen(port_forward_cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            
            # Wait for port-forwarding to establish
            time.sleep(2)
            
            # Check if port-forwarding is working
            if port_forward_process.poll() is not None:
                stderr = port_forward_process.stderr.read().decode('utf-8')
                print(f"Error: Port-forwarding failed: {stderr}")
                return 1
                
            print("Port-forwarding established successfully")
            db_url = 'postgresql://postgres:postgres@localhost:5434/ats_dev'
            table_prefix = 'dev_'
        else:
            # Use local database connection
            db_url = os.getenv('TSDB_URL', 'postgresql://postgres:password@localhost:5432/trading_db_dev')
            table_prefix = ''
            
        print(f"Connecting to database: {db_url}")
        
        # Connect to the database
        conn = await asyncpg.connect(db_url)
        try:
            # Count instruments
            instrument_count = await conn.fetchval(f"SELECT COUNT(*) FROM {table_prefix}instruments")
            print(f"Total instruments in database: {instrument_count}")
            
            # Get a sample of instruments if any exist
            if instrument_count > 0:
                instruments = await conn.fetch(
                    f"SELECT id, symbol, name, exchange, is_active FROM {table_prefix}instruments LIMIT 5"
                )
                print(f"\nSample of {len(instruments)} instruments:")
                for i, instrument in enumerate(instruments):
                    print(f"{i+1}. {instrument['symbol']} - {instrument.get('name', 'N/A')} ({instrument.get('exchange', 'N/A')})")
                
                # Count xrefs by vendor/provider
                id_column = 'vendor_id' if use_k8s else 'provider_id'
                xrefs_query = f"SELECT {id_column}, COUNT(*) FROM {table_prefix}instrument_xrefs GROUP BY {id_column}"
                xrefs = await conn.fetch(xrefs_query)
                
                if xrefs:
                    print(f"\nCross-references by {id_column}:")
                    for row in xrefs:
                        print(f"- {id_column} {row[id_column]}: {row['count']} instruments")
                        
                        # Get vendor/provider name if possible
                        try:
                            vendor_table = f"{table_prefix}vendors"
                            vendor_name_col = 'name'
                            vendor = await conn.fetchrow(
                                f"SELECT {vendor_name_col} FROM {vendor_table} WHERE id = $1", 
                                row[id_column]
                            )
                            if vendor:
                                print(f"  Name: {vendor[vendor_name_col]}")
                        except Exception as e:
                            print(f"  Could not get vendor name: {e}")
                else:
                    print("\nNo cross-references found.")
        finally:
            await conn.close()
            
        return 0
    except Exception as e:
        print(f"Error accessing database: {str(e)}")
        return 1
    finally:
        # Clean up port-forwarding process if it exists
        if port_forward_process:
            print("Stopping port-forwarding...")
            port_forward_process.terminate()
            port_forward_process.wait()
            print("Port-forwarding stopped")

async def main_with_dao():
    """Original implementation using DAOs."""
    # Set GIN_CONFIG environment variable to use integration config
    os.environ['GIN_CONFIG'] = 'config/app_intg.gin'
    
    # Initialize environment for integration (dev)
    from src.config.environment import Environment, EnvironmentType
    from src.dao.instruments_dao import InstrumentsDAO
    from src.dao.instrument_xrefs_dao import InstrumentXrefsDAO
    
    env = Environment(env_type=EnvironmentType.INTEGRATION)
    
    # Create DAOs
    instruments_dao = InstrumentsDAO(env)
    xrefs_dao = InstrumentXrefsDAO(env)
    
    # Count instruments
    try:
        instrument_count = await instruments_dao.count_instruments()
        print(f"Total instruments in database: {instrument_count}")
        
        # Get a sample of instruments if any exist
        if instrument_count > 0:
            instruments = await instruments_dao.list_instruments()
            sample_size = min(5, len(instruments))
            print(f"\nSample of {sample_size} instruments:")
            for i, instrument in enumerate(instruments[:sample_size]):
                print(f"{i+1}. {instrument['symbol']} - {instrument.get('name', 'N/A')} ({instrument.get('exchange', 'N/A')})")
            
            # Count xrefs by provider
            xrefs = await xrefs_dao.count_xrefs_by_provider()
            if xrefs:
                print("\nCross-references by provider:")
                for provider, count in xrefs.items():
                    print(f"- {provider}: {count} instruments")
            else:
                print("\nNo cross-references found.")
    except Exception as e:
        print(f"Error accessing database: {str(e)}")
        return 1
    
    return 0

async def main():
    """Main function that parses arguments and calls the appropriate function."""
    parser = argparse.ArgumentParser(description='Check instrument counts in the database')
    parser.add_argument('--k8s', action='store_true', help='Connect to Kubernetes database')
    parser.add_argument('--namespace', default='ats-dev', help='Kubernetes namespace (default: ats-dev)')
    parser.add_argument('--use-dao', action='store_true', help='Use DAOs instead of direct database access')
    args = parser.parse_args()
    
    if args.use_dao:
        print("Using DAOs to access the database...")
        return await main_with_dao()
    else:
        print(f"Using direct database access {'with Kubernetes' if args.k8s else 'locally'}...")
        return await check_instruments_direct(use_k8s=args.k8s, namespace=args.namespace)

if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
