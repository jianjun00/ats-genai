#!/usr/bin/env python3
"""
Simplified migration script for test and integration environments only.
Skips production due to configuration placeholder issues.
"""
import asyncio
import sys
import os

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))
# Add scripts to path
sys.path.insert(0, os.path.dirname(__file__))

from migrate_to_environment_structure import DataMigrator
from config.environment import EnvironmentType


async def main():
    """Main migration function for test and intg environments only."""
    # Source database URL from environment
    source_db_url = os.getenv('TSDB_URL', 'postgresql://postgres:postgres@localhost:5432/trading_db')
    
    print("=" * 80)
    print("DATA MIGRATION TO TEST & INTEGRATION ENVIRONMENTS")
    print("=" * 80)
    print(f"Source Database: {source_db_url}")
    print(f"Target Environments: test_trading_db, intg_trading_db")
    print("Note: Skipping production due to configuration placeholder issues")
    print()
    
    # Confirm migration
    if len(sys.argv) > 1 and sys.argv[1] == "--confirm":
        proceed = True
    else:
        response = input("This will create new databases and migrate all data. Continue? (yes/no): ")
        proceed = response.lower() in ['yes', 'y']
    
    if not proceed:
        print("Migration cancelled.")
        return
    
    # Initialize migrator
    migrator = DataMigrator(source_db_url)
    
    # Migrate to test and integration environments only
    results = {
        "environments": {},
        "overall_success": True
    }
    
    for env_type in [EnvironmentType.TEST, EnvironmentType.INTEGRATION]:
        try:
            print(f"\n{'='*60}")
            print(f"MIGRATING TO {env_type.value.upper()} ENVIRONMENT")
            print(f"{'='*60}")
            
            env_result = await migrator.migrate_to_environment(env_type)
            results["environments"][env_type.value] = env_result
            
            if not env_result["success"]:
                results["overall_success"] = False
                
        except Exception as e:
            migrator.log(f"Critical error migrating to {env_type.value}: {e}", "ERROR")
            results["environments"][env_type.value] = {
                "success": False,
                "error": str(e)
            }
            results["overall_success"] = False
    
    # Print results summary
    print("\n" + "=" * 80)
    print("MIGRATION RESULTS SUMMARY")
    print("=" * 80)
    
    for env_name, env_result in results["environments"].items():
        status = "✅ SUCCESS" if env_result.get("success", False) else "❌ FAILED"
        print(f"{env_name.upper()}: {status}")
        
        if env_result.get("success", False):
            print(f"  - Tables migrated: {env_result.get('tables_migrated', 0)}")
            print(f"  - Rows migrated: {env_result.get('total_rows_migrated', 0)}")
            if env_result.get('failed_tables'):
                print(f"  - Failed tables: {env_result['failed_tables']}")
        else:
            print(f"  - Error: {env_result.get('error', 'Unknown error')}")
        print()
    
    overall_status = "✅ SUCCESS" if results["overall_success"] else "❌ PARTIAL FAILURE"
    print(f"OVERALL STATUS: {overall_status}")


if __name__ == "__main__":
    asyncio.run(main())
