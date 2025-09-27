#!/usr/bin/env python3
import subprocess
import sys

def run_query(host, port, db, query):
    """Run a SQL query and return results"""
    cmd = f'PGPASSWORD=dev_password psql -h {host} -p {port} -U postgres -d {db} -c "{query}"'

    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    if result.returncode == 0:
        return result.stdout
    else:
        return f"Error: {result.stderr}"
def main():
    print("🔍 Checking for additional event data sources...")

    # Check if dev DB is running
    print("\n📊 DEV Environment (localhost:5432):")
    result = run_query("localhost", "5432", "dev_db",
                      "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_name LIKE '%event%'")
    print(result)

    # Check intg environment (we know this works)
    print("\n📊 INTG Environment (localhost:4432):")
    result = subprocess.run('PGPASSWORD=intg_password psql -h localhost -p 4432 -U postgres -d intg_db -c "SELECT table_name FROM information_schema.tables WHERE table_schema = \'public\' AND (table_name LIKE \'%event%\' OR table_name LIKE \'%dividend%\' OR table_name LIKE \'%split%\')"',
                           shell=True, capture_output=True, text=True)
    print(result.stdout)

    # Check for tables that might contain corporate actions
    print("\n📊 Potential Corporate Action Tables (INTG):")
    result = subprocess.run('PGPASSWORD=intg_password psql -h localhost -p 4432 -U postgres -d intg_db -c "SELECT table_name, pg_size_pretty(pg_total_relation_size(schemaname||\'.\' ||tablename)) as size FROM pg_tables WHERE schemaname = \'public\' AND (table_name LIKE \'%event%\' OR table_name LIKE \'%dividend%\' OR table_name LIKE \'%split%\' OR table_name LIKE \'%fundamental%\') ORDER BY pg_total_relation_size(schemaname||\'.\'||tablename) DESC"',
                           shell=True, capture_output=True, text=True)
    print(result.stdout)

if __name__ == "__main__":
    main()