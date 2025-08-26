#!/usr/bin/env python3
"""
Script to run database migrations for ATS integration environment
"""

import sys
import os
sys.path.append('/workspace/src')

from db.migration_manager import MigrationManager
import asyncio

async def main():
    # Use integration database connection with correct password
    db_url = "postgresql://postgres:intg_password@host.docker.internal:5433/intg_db"
    
    migration_manager = MigrationManager(db_url)
    
    print("🚀 Running database migrations for integration environment...")
    try:
        success = await migration_manager.migrate_to_latest()
        if success:
            print("✅ Integration migrations completed successfully")
        else:
            print("❌ Integration migration failed")
            sys.exit(1)
    except Exception as e:
        print(f"❌ Integration migration failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())