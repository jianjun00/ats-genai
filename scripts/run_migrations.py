#!/usr/bin/env python3
"""
Script to run database migrations for ATS platform
"""

import sys
import os
sys.path.append('/workspace/src')

from db.migration_manager import MigrationManager
import asyncio

async def main():
    # Use database connection from environment variables set by run_dev.py
    db_host = os.getenv("DB_HOST", "localhost")
    db_port = os.getenv("DB_PORT", "5432")
    db_user = os.getenv("DB_USER", "postgres")
    db_password = os.getenv("DB_PASSWORD", "dev_password")
    db_name = os.getenv("DB_NAME", "dev_db")

    db_url = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
    print(f"🔗 Connecting to: postgresql://{db_user}:***@{db_host}:{db_port}/{db_name}")

    migration_manager = MigrationManager(db_url)

    print("🚀 Running database migrations...")
    try:
        success = await migration_manager.migrate_to_latest()
        if success:
            print("✅ Migrations completed successfully")
        else:
            print("❌ Migration failed")
            sys.exit(1)
    except Exception as e:
        print(f"❌ Migration failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())