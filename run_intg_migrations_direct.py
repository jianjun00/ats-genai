#!/usr/bin/env python3
"""
Direct migration runner for ATS-INTG using proper connection
"""

import sys
import os
import asyncio
import asyncpg
from pathlib import Path

# Add src to path  
sys.path.append('/home/jianjun/ats-genai-data/src')

from db.migration_manager import MigrationManager

async def run_intg_migrations():
    """Run migrations for ATS-INTG environment."""
    
    # Use correct connection for ATS-INTG database
    # From Docker container, connect to ats-intg-postgres container
    db_url = "postgresql://postgres:intg_password@ats-intg-postgres:5432/intg_db"
    
    print("🚀 Running ATS-INTG database migrations...")
    print(f"📍 Connecting to: {db_url.replace('intg_password', '***')}")
    
    try:
        # Test connection first
        conn = await asyncpg.connect(db_url)
        version_result = await conn.fetchrow("SELECT version()")
        print(f"✅ Connected to: {version_result['version']}")
        await conn.close()
        
        # Run migrations
        migration_manager = MigrationManager(db_url)
        print("📋 Starting migration process...")
        
        success = await migration_manager.migrate_to_latest()
        
        if success:
            print("✅ ATS-INTG migrations completed successfully!")
            
            # Show final version
            conn = await asyncpg.connect(db_url)
            final_version = await conn.fetchrow("SELECT * FROM intg_db_version ORDER BY id DESC LIMIT 1")
            if final_version:
                print(f"📊 Database now at version: {final_version['version']} - {final_version['description']}")
            await conn.close()
            
            return True
        else:
            print("❌ ATS-INTG migrations failed!")
            return False
            
    except Exception as e:
        print(f"❌ Migration error: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = asyncio.run(run_intg_migrations())
    if not success:
        sys.exit(1)