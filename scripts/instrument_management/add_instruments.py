#!/usr/bin/env python3
import subprocess
import sys
import json

def run_kubectl_command(command):
    """Run a kubectl command and return the output."""
    try:
        result = subprocess.run(
            command,
            capture_output=True,
            text=True,
            check=True
        )
        return result.stdout.strip()
    except subprocess.CalledProcessError as e:
        print(f"Error executing command: {' '.join(command)}", file=sys.stderr)
        print(f"Error output: {e.stderr}", file=sys.stderr)
        return None

def add_instruments():
    """Add more instruments to the database in the Kubernetes cluster."""
    print("Adding instruments to the database in Kubernetes cluster...")
    
    # Check if db-client pod exists
    pod_check = run_kubectl_command(["kubectl", "get", "pod", "db-client", "-n", "ats-dev", "-o", "name"])
    if not pod_check:
        print("db-client pod not found. Creating it...")
        create_pod = run_kubectl_command([
            "kubectl", "apply", "-f", "k8s/dev/db-client-pod.yaml"
        ])
        if not create_pod:
            print("Failed to create db-client pod.")
            return 1
        
        # Wait for pod to be ready
        print("Waiting for db-client pod to be ready...")
        wait_result = run_kubectl_command([
            "kubectl", "wait", "--for=condition=ready", "pod/db-client", 
            "-n", "ats-dev", "--timeout=30s"
        ])
        if not wait_result:
            print("Timed out waiting for db-client pod to be ready.")
            return 1
    
    # Define instruments to add
    instruments = [
        {"symbol": "MSFT", "name": "Microsoft Corporation", "exchange": "NASDAQ"},
        {"symbol": "AMZN", "name": "Amazon.com Inc.", "exchange": "NASDAQ"},
        {"symbol": "GOOGL", "name": "Alphabet Inc.", "exchange": "NASDAQ"},
        {"symbol": "META", "name": "Meta Platforms Inc.", "exchange": "NASDAQ"},
        {"symbol": "NVDA", "name": "NVIDIA Corporation", "exchange": "NASDAQ"},
        {"symbol": "TSLA", "name": "Tesla Inc.", "exchange": "NASDAQ"},
        {"symbol": "JPM", "name": "JPMorgan Chase & Co.", "exchange": "NYSE"},
        {"symbol": "V", "name": "Visa Inc.", "exchange": "NYSE"},
        {"symbol": "PG", "name": "Procter & Gamble Co.", "exchange": "NYSE"},
        {"symbol": "DIS", "name": "The Walt Disney Company", "exchange": "NYSE"}
    ]
    
    # Add each instrument
    for instrument in instruments:
        print(f"\nAdding instrument: {instrument['symbol']} - {instrument['name']}...")
        
        # Check if instrument already exists
        check_cmd = f"SELECT id FROM dev_instruments WHERE symbol = '{instrument['symbol']}';"
        check_result = run_kubectl_command([
            "kubectl", "exec", "db-client", "-n", "ats-dev", "--", 
            "psql", "-t", "-c", check_cmd
        ])
        
        if check_result and check_result.strip():
            print(f"  Instrument {instrument['symbol']} already exists with ID: {check_result.strip()}")
            instrument_id = check_result.strip()
        else:
            # Insert instrument
            insert_cmd = f"INSERT INTO dev_instruments (symbol, name, exchange, is_active) VALUES ('{instrument['symbol']}', '{instrument['name']}', '{instrument['exchange']}', true) RETURNING id;"
            insert_result = run_kubectl_command([
                "kubectl", "exec", "db-client", "-n", "ats-dev", "--", 
                "psql", "-t", "-c", insert_cmd
            ])
            
            if not insert_result:
                print(f"  Failed to add instrument {instrument['symbol']}.")
                continue
                
            instrument_id = insert_result.strip()
            print(f"  Added instrument {instrument['symbol']} with ID: {instrument_id}")
        
        # Add polygon xref
        polygon_cmd = f"DO $$ \n\
BEGIN \n\
  IF NOT EXISTS (SELECT 1 FROM dev_instrument_xrefs WHERE instrument_id = {instrument_id} AND vendor_id = 1) THEN \n\
    INSERT INTO dev_instrument_xrefs (instrument_id, vendor_id, vendor_symbol) VALUES ({instrument_id}, 1, '{instrument['symbol']}'); \n\
  END IF; \n\
END $$;"
        
        polygon_result = run_kubectl_command([
            "kubectl", "exec", "db-client", "-n", "ats-dev", "--", 
            "psql", "-c", polygon_cmd
        ])
        
        if polygon_result:
            print(f"  Added/verified polygon xref for {instrument['symbol']}")
        
        # Add tiingo xref
        tiingo_cmd = f"DO $$ \n\
BEGIN \n\
  IF NOT EXISTS (SELECT 1 FROM dev_instrument_xrefs WHERE instrument_id = {instrument_id} AND vendor_id = 2) THEN \n\
    INSERT INTO dev_instrument_xrefs (instrument_id, vendor_id, vendor_symbol) VALUES ({instrument_id}, 2, '{instrument['symbol']}'); \n\
  END IF; \n\
END $$;"
        
        tiingo_result = run_kubectl_command([
            "kubectl", "exec", "db-client", "-n", "ats-dev", "--", 
            "psql", "-c", tiingo_cmd
        ])
        
        if tiingo_result:
            print(f"  Added/verified tiingo xref for {instrument['symbol']}")
    
    # Count instruments and xrefs
    count_instruments = run_kubectl_command([
        "kubectl", "exec", "db-client", "-n", "ats-dev", "--", 
        "psql", "-t", "-c", "SELECT COUNT(*) FROM dev_instruments;"
    ])
    
    count_xrefs = run_kubectl_command([
        "kubectl", "exec", "db-client", "-n", "ats-dev", "--", 
        "psql", "-t", "-c", "SELECT COUNT(*) FROM dev_instrument_xrefs;"
    ])
    
    print(f"\nTotal instruments in database: {count_instruments.strip()}")
    print(f"Total instrument xrefs in database: {count_xrefs.strip()}")
    
    print("\nInstrument addition completed successfully!")
    return 0

if __name__ == "__main__":
    exit_code = add_instruments()
    sys.exit(exit_code)
