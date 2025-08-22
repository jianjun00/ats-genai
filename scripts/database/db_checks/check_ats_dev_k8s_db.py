#!/usr/bin/env python3
import asyncio
import asyncpg
import sys
import subprocess
import time

async def check_ats_dev_db():
    """
    Check the ats_dev database in the Kubernetes cluster via port-forwarding.
    Ensures port-forwarding is active before attempting to connect.
    """
    print("Checking port-forwarding status...")
    try:
        # Check if port-forwarding is already active
        result = subprocess.run(
            ["netstat", "-tuln"], 
            capture_output=True, 
            text=True, 
            check=False
        )
        
        print(f"Network status:\n{result.stdout}")
        
        if ":5432" not in result.stdout:
            print("Port-forwarding not active. Please run the following command in a separate terminal:")
            print("kubectl port-forward service/postgres 5432:5432 -n ats-dev")
            print("\nWaiting for port-forwarding to be established...")
            return 1
    
        print("Port-forwarding is active. Connecting to database...")
        
        # Connect to the ats_dev database
        conn = await asyncpg.connect(
            host='localhost',  # Using port-forwarding
            port=5432,
            user='postgres',
            password='postgres',
            database='ats_dev'
        )
        
        print("Successfully connected to ats_dev database!")
        
        # Check for tables with dev_ prefix
        print("\nChecking for tables with dev_ prefix:")
        tables = await conn.fetch(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = 'public' AND table_name LIKE 'dev\\_%' "
            "ORDER BY table_name"
        )
        
        if not tables:
            print("No tables with dev_ prefix found.")
        else:
            print(f"Found {len(tables)} tables with dev_ prefix:")
            for table in tables:
                table_name = table['table_name']
                print(f"\n- {table_name}")
                
                # Count rows in the table
                count = await conn.fetchval(f"SELECT COUNT(*) FROM {table_name}")
                print(f"  Row count: {count}")
                
                # If this is the instruments table, show some sample data
                if table_name == 'dev_instruments' and count > 0:
                    print("\n  Sample instruments:")
                    instruments = await conn.fetch(
                        "SELECT id, symbol, name, exchange, is_active FROM dev_instruments LIMIT 5"
                    )
                    for instrument in instruments:
                        print(f"    ID: {instrument['id']}, Symbol: {instrument['symbol']}, "
                              f"Name: {instrument['name']}, Exchange: {instrument['exchange']}, "
                              f"Active: {instrument['is_active']}")
                
                # If this is the instrument_xrefs table, show some sample data
                if table_name == 'dev_instrument_xrefs' and count > 0:
                    print("\n  Sample instrument xrefs:")
                    xrefs = await conn.fetch(
                        "SELECT ix.id, i.symbol, v.name as vendor, ix.vendor_symbol "
                        "FROM dev_instrument_xrefs ix "
                        "JOIN dev_instruments i ON ix.instrument_id = i.id "
                        "JOIN dev_vendors v ON ix.vendor_id = v.id "
                        "LIMIT 5"
                    )
                    for xref in xrefs:
                        print(f"    ID: {xref['id']}, Symbol: {xref['symbol']}, "
                              f"Vendor: {xref['vendor']}, Vendor Symbol: {xref['vendor_symbol']}")
        
        # Check if we can add another instrument
        print("\nAdding a test instrument (TSLA)...")
        try:
            # Begin transaction
            async with conn.transaction():
                # Insert instrument
                instrument_id = await conn.fetchval(
                    "INSERT INTO dev_instruments (symbol, name, exchange, is_active) "
                    "VALUES ('TSLA', 'Tesla Inc.', 'NASDAQ', true) RETURNING id"
                )
                print(f"  Added instrument TSLA with ID: {instrument_id}")
                
                # Get polygon vendor ID
                vendor_id = await conn.fetchval("SELECT id FROM dev_vendors WHERE name = 'polygon'")
                
                # Insert xref
                xref_id = await conn.fetchval(
                    "INSERT INTO dev_instrument_xrefs (instrument_id, vendor_id, vendor_symbol) "
                    "VALUES ($1, $2, 'TSLA') RETURNING id",
                    instrument_id, vendor_id
                )
                print(f"  Added instrument xref with ID: {xref_id}")
        except Exception as e:
            print(f"  Error adding test instrument: {e}")
        
        # Count instruments after addition
        instrument_count = await conn.fetchval("SELECT COUNT(*) FROM dev_instruments")
        print(f"\nTotal instruments in database: {instrument_count}")
        
        # Close the connection
        await conn.close()
        print("\nDatabase check completed successfully!")
        
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1
    
    return 0

if __name__ == "__main__":
    exit_code = asyncio.run(check_ats_dev_db())
    sys.exit(exit_code)
