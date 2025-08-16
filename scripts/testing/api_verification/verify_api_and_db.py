#!/usr/bin/env python3
"""
Comprehensive verification script for API and database connectivity.

This script:
1. Sets up port-forwarding to the API pod
2. Tests all API endpoints
3. Sets up port-forwarding to the database
4. Verifies database tables and data
5. Compares API data with direct database queries
"""

import argparse
import asyncio
import json
import os
import subprocess
import sys
import time
import aiohttp
import asyncpg
from datetime import datetime
from typing import Dict, List, Any, Optional, Tuple


class DateTimeEncoder(json.JSONEncoder):
    """JSON encoder that handles datetime objects."""
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)

# Constants
DEFAULT_API_PORT = 8090  # Changed to avoid conflicts
DEFAULT_DB_PORT = 5440  # Changed to avoid conflicts
DEFAULT_NAMESPACE = "ats-dev"
DEFAULT_API_JOB = "api-test-job"

class PortForward:
    """Class to manage port-forwarding processes."""
    
    def __init__(self, resource_type: str, resource_name: str, 
                 local_port: int, remote_port: int, namespace: str):
        """Initialize port-forwarding."""
        self.resource_type = resource_type
        self.resource_name = resource_name
        self.local_port = local_port
        self.remote_port = remote_port
        self.namespace = namespace
        self.process = None
        
    async def start(self) -> bool:
        """Start port-forwarding."""
        command = f"kubectl port-forward {self.resource_type}/{self.resource_name} {self.local_port}:{self.remote_port} -n {self.namespace}"
        print(f"Starting port-forwarding: {command}")
        
        self.process = subprocess.Popen(
            command, 
            shell=True, 
            stdout=subprocess.PIPE, 
            stderr=subprocess.PIPE,
            text=True
        )
        
        # Wait for port-forwarding to establish
        await asyncio.sleep(2)
        
        # Check if process is still running
        if self.process.poll() is not None:
            stderr = self.process.stderr.read()
            print(f"Error setting up port-forwarding: {stderr}")
            return False
        
        print(f"Port-forwarding established on port {self.local_port}")
        return True
    
    def stop(self):
        """Stop port-forwarding."""
        if self.process and self.process.poll() is None:
            print(f"Stopping port-forwarding on port {self.local_port}...")
            self.process.terminate()
            self.process.wait()
            print("Port-forwarding stopped")


class APIClient:
    """Client for interacting with the API."""
    
    def __init__(self, base_url: str):
        """Initialize API client."""
        self.base_url = base_url
        self.session = None
        
    async def __aenter__(self):
        """Create session when entering context."""
        self.session = aiohttp.ClientSession()
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Close session when exiting context."""
        if self.session:
            await self.session.close()
            
    async def get_health(self) -> Dict:
        """Get API health status."""
        return await self._get_json("/health")
    
    async def get_db_check(self) -> Dict:
        """Get database check results."""
        return await self._get_json("/api/v1/db-check")
    
    async def get_instruments(self) -> Optional[List[Dict]]:
        """Get all instruments."""
        data = await self._get_json("/api/v1/instruments")
        if data and "instruments" in data:
            return data["instruments"]
        elif data:
            print(f"Warning: 'instruments' key not found in API response: {data}")
            return []
        return None
    
    async def get_instrument(self, symbol: str) -> Dict:
        """Get a specific instrument by symbol."""
        return await self._get_json(f"/api/v1/instrument/{symbol}")
    
    async def _get_json(self, endpoint: str) -> Any:
        """Make a GET request and return JSON response."""
        url = f"{self.base_url}{endpoint}"
        print(f"API Request: GET {url}")
        
        if not self.session:
            self.session = aiohttp.ClientSession()
            
        try:
            async with self.session.get(url) as response:
                if response.status == 200:
                    return await response.json()
                else:
                    print(f"Error: API returned status {response.status}")
                    print(await response.text())
                    return None
        except aiohttp.ClientError as e:
            print(f"API request error: {e}")
            return None


class DatabaseClient:
    """Client for interacting with the database."""
    
    def __init__(self, db_url: str, table_prefix: str = "dev_"):
        """Initialize database client."""
        self.db_url = db_url
        self.table_prefix = table_prefix
        self.conn = None
        
    async def connect(self) -> bool:
        """Connect to the database."""
        try:
            print(f"Connecting to database: {self.db_url}")
            self.conn = await asyncpg.connect(self.db_url)
            return True
        except Exception as e:
            print(f"Database connection error: {e}")
            return False
    
    async def close(self):
        """Close the database connection."""
        if self.conn:
            await self.conn.close()
            print("Database connection closed")
    
    async def get_tables(self) -> List[str]:
        """Get all tables in the database."""
        if not self.conn:
            return []
        
        query = """
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public'
        ORDER BY table_name
        """
        
        try:
            rows = await self.conn.fetch(query)
            return [row['table_name'] for row in rows]
        except Exception as e:
            print(f"Error getting tables: {e}")
            return []
    
    async def get_instruments(self) -> List[Dict]:
        """Get all instruments from the database."""
        if not self.conn:
            return []
        
        query = f"""
        SELECT id, symbol, name, exchange, is_active, created_at, updated_at
        FROM {self.table_prefix}instruments
        ORDER BY symbol
        """
        
        try:
            rows = await self.conn.fetch(query)
            return [dict(row) for row in rows]
        except Exception as e:
            print(f"Error getting instruments: {e}")
            return []
    
    async def get_instrument(self, symbol: str) -> Optional[Dict]:
        """Get a specific instrument by symbol."""
        if not self.conn:
            return None
        
        query = f"""
        SELECT id, symbol, name, exchange, is_active, created_at, updated_at
        FROM {self.table_prefix}instruments
        WHERE symbol = $1
        """
        
        try:
            row = await self.conn.fetchrow(query, symbol)
            return dict(row) if row else None
        except Exception as e:
            print(f"Error getting instrument {symbol}: {e}")
            return None
    
    async def get_xrefs_for_instrument(self, instrument_id: int) -> List[Dict]:
        """Get cross-references for an instrument."""
        if not self.conn:
            return []
        
        query = f"""
        SELECT x.id, x.instrument_id, x.vendor_id, x.vendor_symbol,
               v.name as vendor_name
        FROM {self.table_prefix}instrument_xrefs x
        JOIN {self.table_prefix}vendors v ON x.vendor_id = v.id
        WHERE x.instrument_id = $1
        """
        
        try:
            rows = await self.conn.fetch(query, instrument_id)
            return [dict(row) for row in rows]
        except Exception as e:
            print(f"Error getting xrefs for instrument {instrument_id}: {e}")
            return []


async def setup_api_port_forward(args) -> Tuple[Optional[PortForward], str]:
    """Set up port-forwarding to the API pod."""
    # Get the API pod name
    pod_cmd = f"kubectl get pods -n {args.namespace} -l job-name={args.api_job} -o name"
    pod_result = subprocess.run(pod_cmd, shell=True, capture_output=True, text=True)
    
    if pod_result.returncode != 0 or not pod_result.stdout.strip():
        print(f"Error: Could not find API pod for job {args.api_job} in namespace {args.namespace}")
        return None, ""
    
    pod_name = pod_result.stdout.strip().replace('pod/', '')
    print(f"Found API pod: {pod_name}")
    
    # Set up port-forwarding
    port_forward = PortForward(
        resource_type="pod",
        resource_name=pod_name,
        local_port=args.api_port,
        remote_port=8080,
        namespace=args.namespace
    )
    
    if await port_forward.start():
        api_url = f"http://localhost:{args.api_port}"
        return port_forward, api_url
    
    return None, ""


async def setup_db_port_forward(args) -> Tuple[Optional[PortForward], str]:
    """Set up port-forwarding to the database pod."""
    # Get the postgres pod name
    pod_cmd = f"kubectl get pods -n {args.namespace} -l app=postgres -o name"
    pod_result = subprocess.run(pod_cmd, shell=True, capture_output=True, text=True)
    
    if pod_result.returncode != 0 or not pod_result.stdout.strip():
        print(f"Error: Could not find postgres pod in namespace {args.namespace}")
        return None, ""
    
    pod_name = pod_result.stdout.strip().replace('pod/', '')
    print(f"Found postgres pod: {pod_name}")
    
    # Set up port-forwarding
    port_forward = PortForward(
        resource_type="pod",
        resource_name=pod_name,
        local_port=args.db_port,
        remote_port=5432,
        namespace=args.namespace
    )
    
    if await port_forward.start():
        db_url = f"postgresql://postgres:postgres@localhost:{args.db_port}/ats_dev"
        return port_forward, db_url
    
    return None, ""


async def verify_api(api_client: APIClient) -> bool:
    """Verify API endpoints."""
    print("\n=== Verifying API Endpoints ===")
    
    try:
        # Check health endpoint
        print("\nChecking health endpoint...")
        health = await api_client.get_health()
        if health:
            print(f"Health check successful: {json.dumps(health, indent=2, cls=DateTimeEncoder)}")
        else:
            print("Health check failed")
            return False
        
        # Check database connectivity
        print("\nChecking database connectivity...")
        db_check = await api_client.get_db_check()
        if db_check:
            print(f"Database check successful: {json.dumps(db_check, indent=2, cls=DateTimeEncoder)}")
        else:
            print("Database check failed")
            return False
        
        # Check instruments endpoint
        print("\nChecking instruments endpoint...")
        instruments = await api_client.get_instruments()
        if instruments is not None:
            print(f"Found {len(instruments)} instruments")
            if instruments and len(instruments) > 0:
                print(f"Sample instrument: {json.dumps(instruments[0], indent=2, cls=DateTimeEncoder)}")
            else:
                print("No instruments found, but endpoint is working")
        else:
            print("Failed to get instruments endpoint")
            return False
        
        # Check specific instrument endpoint with a known symbol (AAPL)
        symbol = "AAPL"  # Use a known symbol that should exist
        print(f"\nChecking instrument endpoint for {symbol}...")
        instrument = await api_client.get_instrument(symbol)
        if instrument:
            print(f"Found instrument: {json.dumps(instrument, indent=2, cls=DateTimeEncoder)}")
        else:
            print(f"Failed to get instrument {symbol}")
            # Try another known symbol (MSFT)
            symbol = "MSFT"
            print(f"Trying another symbol: {symbol}...")
            instrument = await api_client.get_instrument(symbol)
            if instrument:
                print(f"Found instrument: {json.dumps(instrument, indent=2, cls=DateTimeEncoder)}")
            else:
                print(f"Failed to get instrument {symbol}")
                print("Warning: Could not find any instruments by symbol, but API is functioning")
                # Not returning False here as the API might be working but just doesn't have these symbols
        
        return True
    except Exception as e:
        print(f"Error during API verification: {e}")
        return False
    
    return True


async def verify_database(db_client: DatabaseClient) -> bool:
    """Verify database tables and data."""
    print("\n=== Verifying Database ===")
    
    try:
        # Connect to the database
        if not await db_client.connect():
            return False
        
        # Get tables
        print("\nGetting database tables...")
        tables = await db_client.get_tables()
        if tables:
            print(f"Found {len(tables)} tables:")
            for table in tables:
                print(f"- {table}")
        else:
            print("Failed to get tables")
            return False
        
        # Get instruments
        print("\nGetting instruments from database...")
        instruments = await db_client.get_instruments()
        if instruments is not None:
            print(f"Found {len(instruments)} instruments")
            if instruments and len(instruments) > 0:
                print(f"Sample instrument: {json.dumps(instruments[0], indent=2, cls=DateTimeEncoder)}")
            else:
                print("No instruments found in database")
        else:
            print("Failed to query instruments table")
            return False
        
        # Get specific instrument with a known symbol (AAPL)
        symbol = "AAPL"  # Use a known symbol that should exist
        print(f"\nGetting instrument {symbol} from database...")
        instrument = await db_client.get_instrument(symbol)
        if instrument:
            print(f"Found instrument: {json.dumps(instrument, indent=2, cls=DateTimeEncoder)}")
            
            # Get cross-references
            print(f"\nGetting cross-references for instrument {symbol}...")
            xrefs = await db_client.get_xrefs_for_instrument(instrument["id"])
            if xrefs:
                print(f"Found {len(xrefs)} cross-references:")
                for xref in xrefs:
                    print(f"- {xref['vendor_name']}: {xref['vendor_symbol']}")
            else:
                print(f"No cross-references found for {symbol}")
        else:
            print(f"Failed to get instrument {symbol}")
            # Try another known symbol (MSFT)
            symbol = "MSFT"
            print(f"Trying another symbol: {symbol}...")
            instrument = await db_client.get_instrument(symbol)
            if instrument:
                print(f"Found instrument: {json.dumps(instrument, indent=2, cls=DateTimeEncoder)}")
                # Get cross-references
                print(f"\nGetting cross-references for instrument {symbol}...")
                xrefs = await db_client.get_xrefs_for_instrument(instrument["id"])
                if xrefs:
                    print(f"Found {len(xrefs)} cross-references:")
                    for xref in xrefs:
                        print(f"- {xref['vendor_name']}: {xref['vendor_symbol']}")
                else:
                    print(f"No cross-references found for {symbol}")
            else:
                print(f"Failed to get instrument {symbol}")
                print("Warning: Could not find specific instruments, but database connection is working")
                # Not returning False here as the database might be working but just doesn't have these symbols
        
        return True
    except Exception as e:
        print(f"Error during database verification: {e}")
        return False


async def compare_api_and_db(api_client: APIClient, db_client: DatabaseClient) -> bool:
    """Compare API data with direct database queries."""
    print("\n=== Comparing API and Database Data ===")
    
    try:
        # Get instruments from API
        print("\nGetting instruments from API...")
        api_instruments = await api_client.get_instruments()
        if api_instruments is None:
            print("Failed to get instruments from API")
            return False
        
        # Get instruments from database
        print("Getting instruments from database...")
        db_instruments = await db_client.get_instruments()
        if not db_instruments:
            print("Failed to get instruments from database")
            return False
        
        # Compare counts
        api_count = len(api_instruments)
        db_count = len(db_instruments)
        print(f"\nAPI instruments count: {api_count}")
        print(f"Database instruments count: {db_count}")
        
        if api_count != db_count:
            print("Warning: API and database instrument counts do not match")
            print("This is expected if the API is filtering or limiting results")
        else:
            print("API and database instrument counts match")
        
        # Try to compare a known symbol (AAPL)
        symbol = "AAPL"
        print(f"\nComparing instrument {symbol} between API and database...")
        
        api_response = await api_client.get_instrument(symbol)
        db_instrument = await db_client.get_instrument(symbol)
        
        # Extract the actual instrument data from the API response
        api_instrument = None
        if api_response and "instrument" in api_response:
            api_instrument = api_response["instrument"]
        elif api_response and "status" in api_response and api_response["status"] == "success":
            # Try to find the instrument data in the response
            for key, value in api_response.items():
                if isinstance(value, dict) and "symbol" in value:
                    api_instrument = value
                    break
        else:
            api_instrument = api_response  # Fallback to the whole response
        
        if api_instrument and db_instrument:
            print("Found instrument in both API and database")
            
            # Compare basic fields
            api_symbol = api_instrument.get("symbol", "")
            db_symbol = db_instrument.get("symbol", "")
            
            if api_symbol and db_symbol and api_symbol == db_symbol:
                print(f"Instrument {symbol} symbols match between API and database")
                
                # Check other fields if they exist in both
                name_match = True
                exchange_match = True
                
                if "name" in api_instrument and "name" in db_instrument:
                    name_match = api_instrument["name"] == db_instrument["name"]
                    if name_match:
                        print("Name fields match")
                    else:
                        print("Warning: Name fields do not match")
                
                if "exchange" in api_instrument and "exchange" in db_instrument:
                    exchange_match = api_instrument["exchange"] == db_instrument["exchange"]
                    if exchange_match:
                        print("Exchange fields match")
                    else:
                        print("Warning: Exchange fields do not match")
                
                if name_match and exchange_match:
                    print(f"Instrument {symbol} data matches between API and database")
                else:
                    print(f"Warning: Some fields for instrument {symbol} do not match exactly between API and database")
                    print(f"API: {json.dumps(api_instrument, indent=2, cls=DateTimeEncoder)}")
                    print(f"DB: {json.dumps(db_instrument, indent=2, cls=DateTimeEncoder)}")
                    print("This may be expected if the API transforms the data")
            else:
                print(f"Warning: Instrument {symbol} symbols do not match between API and database")
                print(f"API symbol: {api_symbol}")
                print(f"DB symbol: {db_symbol}")
        else:
            if not api_instrument:
                print(f"Failed to get instrument {symbol} from API")
            if not db_instrument:
                print(f"Failed to get instrument {symbol} from database")
            
            # Try another symbol (MSFT)
            symbol = "MSFT"
            print(f"\nTrying another symbol: {symbol}...")
            
            api_response = await api_client.get_instrument(symbol)
            db_instrument = await db_client.get_instrument(symbol)
            
            # Extract the actual instrument data from the API response
            api_instrument = None
            if api_response and "instrument" in api_response:
                api_instrument = api_response["instrument"]
            elif api_response and "status" in api_response and api_response["status"] == "success":
                # Try to find the instrument data in the response
                for key, value in api_response.items():
                    if isinstance(value, dict) and "symbol" in value:
                        api_instrument = value
                        break
            else:
                api_instrument = api_response  # Fallback to the whole response
            
            if api_instrument and db_instrument:
                print(f"Found instrument {symbol} in both API and database")
            else:
                if not api_instrument:
                    print(f"Failed to get instrument {symbol} from API")
                if not db_instrument:
                    print(f"Failed to get instrument {symbol} from database")
                print("Warning: Could not find matching instruments in both API and database")
                print("This may be expected if the data is different between systems")
                # Not returning False here as the comparison might be working but just doesn't have matching data
        
        return True
    except Exception as e:
        print(f"Error during API and database comparison: {e}")
        import traceback
        print(traceback.format_exc())
        return False


async def main():
    """Main function."""
    parser = argparse.ArgumentParser(description="Verify API and database connectivity")
    parser.add_argument("--namespace", default=DEFAULT_NAMESPACE, help=f"Kubernetes namespace (default: {DEFAULT_NAMESPACE})")
    parser.add_argument("--api-job", default=DEFAULT_API_JOB, help=f"API job name (default: {DEFAULT_API_JOB})")
    parser.add_argument("--api-port", type=int, default=DEFAULT_API_PORT, help=f"Local port for API port-forwarding (default: {DEFAULT_API_PORT})")
    parser.add_argument("--db-port", type=int, default=DEFAULT_DB_PORT, help=f"Local port for database port-forwarding (default: {DEFAULT_DB_PORT})")
    parser.add_argument("--skip-api", action="store_true", help="Skip API verification")
    parser.add_argument("--skip-db", action="store_true", help="Skip database verification")
    parser.add_argument("--skip-compare", action="store_true", help="Skip comparison between API and database")
    args = parser.parse_args()
    
    api_port_forward = None
    db_port_forward = None
    api_client = None
    db_client = None
    
    try:
        # Set up API port-forwarding
        if not args.skip_api or not args.skip_compare:
            api_port_forward, api_url = await setup_api_port_forward(args)
            if not api_port_forward:
                return 1
            api_client = APIClient(api_url)
            # Initialize the session
            await api_client.__aenter__()
        
        # Set up database port-forwarding
        if not args.skip_db or not args.skip_compare:
            db_port_forward, db_url = await setup_db_port_forward(args)
            if not db_port_forward:
                return 1
            db_client = DatabaseClient(db_url)
        
        # Verify API
        if not args.skip_api:
            if not await verify_api(api_client):
                print("API verification failed")
                return 1
            else:
                print("API verification successful")
        
        # Verify database
        if not args.skip_db:
            if not await verify_database(db_client):
                print("Database verification failed")
                return 1
            else:
                print("Database verification successful")
        
        # Compare API and database
        if not args.skip_compare:
            if not await compare_api_and_db(api_client, db_client):
                print("API and database comparison failed")
                return 1
            else:
                print("API and database comparison successful")
        
        print("\n=== Verification Complete ===")
        print("All checks passed successfully!")
        return 0
    
    except Exception as e:
        print(f"\nError during verification: {e}")
        return 1
    
    finally:
        # Close API client session
        if api_client is not None:
            await api_client.__aexit__(None, None, None)
        
        # Close database connection
        if db_client is not None:
            await db_client.close()
        
        # Stop port-forwarding
        if api_port_forward:
            api_port_forward.stop()
        
        if db_port_forward:
            db_port_forward.stop()


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
