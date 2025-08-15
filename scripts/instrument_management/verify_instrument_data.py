#!/usr/bin/env python3
import subprocess
import sys
import json
import tabulate

def run_kubectl_command(command):
    """Run a kubectl command and return the output."""
    print(f"Running command: {' '.join(command)}")
    try:
        result = subprocess.run(
            command,
            capture_output=True,
            text=True,
            check=False  # Don't raise exception on non-zero exit
        )
        if result.returncode != 0:
            print(f"Command failed with exit code {result.returncode}")
            print(f"Error output: {result.stderr}")
            return None
        return result.stdout.strip()
    except Exception as e:
        print(f"Exception executing command: {' '.join(command)}")
        print(f"Exception: {str(e)}")
        return None

def verify_instrument_data():
    """Verify instrument data integrity in the Kubernetes cluster."""
    print("Verifying instrument data integrity in the Kubernetes cluster...")
    
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
    
    # Check database schema
    print("\n=== Database Schema ===")
    schema_cmd = "\\dt dev_*"
    schema_result = run_kubectl_command([
        "kubectl", "exec", "db-client", "-n", "ats-dev", "--", 
        "psql", "-c", schema_cmd
    ])
    if schema_result:
        print(schema_result)
    
    # Check vendors
    print("\n=== Vendors ===")
    vendors_cmd = "SELECT * FROM dev_vendors ORDER BY id;"
    vendors_result = run_kubectl_command([
        "kubectl", "exec", "db-client", "-n", "ats-dev", "--", 
        "psql", "-c", vendors_cmd
    ])
    if vendors_result:
        print(vendors_result)
    
    # Check instruments
    print("\n=== Instruments ===")
    instruments_cmd = "SELECT * FROM dev_instruments ORDER BY id;"
    instruments_result = run_kubectl_command([
        "kubectl", "exec", "db-client", "-n", "ats-dev", "--", 
        "psql", "-c", instruments_cmd
    ])
    if instruments_result:
        print(instruments_result)
    
    # Check instrument xrefs with vendor names
    print("\n=== Instrument Xrefs ===")
    xrefs_cmd = """
    SELECT 
        ix.id, 
        i.symbol, 
        v.name as vendor, 
        ix.vendor_symbol,
        ix.created_at,
        ix.updated_at
    FROM 
        dev_instrument_xrefs ix 
    JOIN 
        dev_instruments i ON ix.instrument_id = i.id 
    JOIN 
        dev_vendors v ON ix.vendor_id = v.id
    ORDER BY 
        i.symbol, v.name;
    """
    xrefs_result = run_kubectl_command([
        "kubectl", "exec", "db-client", "-n", "ats-dev", "--", 
        "psql", "-c", xrefs_cmd
    ])
    if xrefs_result:
        print(xrefs_result)
    
    # Verify data integrity
    print("\n=== Data Integrity Checks ===")
    
    # Check 1: All instruments have xrefs for both vendors
    integrity_check1 = """
    SELECT 
        i.symbol,
        COUNT(DISTINCT v.id) as vendor_count,
        string_agg(v.name, ', ') as vendors
    FROM 
        dev_instruments i
    LEFT JOIN 
        dev_instrument_xrefs ix ON i.id = ix.instrument_id
    LEFT JOIN 
        dev_vendors v ON ix.vendor_id = v.id
    GROUP BY 
        i.symbol
    ORDER BY 
        vendor_count ASC;
    """
    integrity_result1 = run_kubectl_command([
        "kubectl", "exec", "db-client", "-n", "ats-dev", "--", 
        "psql", "-c", integrity_check1
    ])
    if integrity_result1:
        print("\nCheck 1: Instruments with vendor xrefs:")
        print(integrity_result1)
    
    # Check 2: Instruments without xrefs for any vendor
    integrity_check2 = """
    SELECT 
        i.id,
        i.symbol,
        i.name
    FROM 
        dev_instruments i
    LEFT JOIN 
        dev_instrument_xrefs ix ON i.id = ix.instrument_id
    WHERE 
        ix.id IS NULL;
    """
    integrity_result2 = run_kubectl_command([
        "kubectl", "exec", "db-client", "-n", "ats-dev", "--", 
        "psql", "-c", integrity_check2
    ])
    if integrity_result2:
        print("\nCheck 2: Instruments without any vendor xrefs:")
        print(integrity_result2)
    
    # Check 3: Instruments with missing vendor xrefs
    integrity_check3 = """
    WITH vendor_counts AS (
        SELECT 
            i.id,
            i.symbol,
            COUNT(DISTINCT v.id) as vendor_count
        FROM 
            dev_instruments i
        LEFT JOIN 
            dev_instrument_xrefs ix ON i.id = ix.instrument_id
        LEFT JOIN 
            dev_vendors v ON ix.vendor_id = v.id
        GROUP BY 
            i.id, i.symbol
    )
    SELECT 
        id,
        symbol,
        vendor_count,
        CASE 
            WHEN vendor_count < 2 THEN 'Missing some vendor xrefs'
            WHEN vendor_count = 2 THEN 'Complete'
            ELSE 'Unknown'
        END as status
    FROM 
        vendor_counts
    WHERE 
        vendor_count < 2
    ORDER BY 
        symbol;
    """
    integrity_result3 = run_kubectl_command([
        "kubectl", "exec", "db-client", "-n", "ats-dev", "--", 
        "psql", "-c", integrity_check3
    ])
    if integrity_result3:
        print("\nCheck 3: Instruments with missing vendor xrefs:")
        print(integrity_result3)
    
    # Summary
    print("\n=== Summary ===")
    summary_cmd = """
    SELECT 
        (SELECT COUNT(*) FROM dev_instruments) as instrument_count,
        (SELECT COUNT(*) FROM dev_instrument_xrefs) as xref_count,
        (SELECT COUNT(*) FROM dev_vendors) as vendor_count,
        (SELECT COUNT(DISTINCT instrument_id) FROM dev_instrument_xrefs) as instruments_with_xrefs;
    """
    summary_result = run_kubectl_command([
        "kubectl", "exec", "db-client", "-n", "ats-dev", "--", 
        "psql", "-t", "-c", summary_cmd
    ])
    if summary_result:
        parts = summary_result.strip().split('|')
        if len(parts) == 4:
            instrument_count = parts[0].strip()
            xref_count = parts[1].strip()
            vendor_count = parts[2].strip()
            instruments_with_xrefs = parts[3].strip()
            
            print(f"Total instruments: {instrument_count}")
            print(f"Total xrefs: {xref_count}")
            print(f"Total vendors: {vendor_count}")
            print(f"Instruments with xrefs: {instruments_with_xrefs}")
            
            if instrument_count == instruments_with_xrefs:
                print("\n✅ All instruments have at least one vendor xref")
            else:
                print(f"\n❌ {int(instrument_count) - int(instruments_with_xrefs)} instruments are missing vendor xrefs")
            
            if int(xref_count) == int(instrument_count) * int(vendor_count):
                print("✅ All instruments have xrefs for all vendors")
            else:
                print(f"❌ Some instruments are missing vendor xrefs (Expected: {int(instrument_count) * int(vendor_count)}, Actual: {xref_count})")
    
    print("\nInstrument data verification completed!")
    return 0

if __name__ == "__main__":
    exit_code = verify_instrument_data()
    sys.exit(exit_code)
