#!/usr/bin/env python3
"""
Run the schema fix migration to resolve production issues.
"""

import os
import sys
import asyncio

# Add src to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from db.migration_manager import MigrationManager


async def main():
    """Run the schema fix migration."""
    print("🔧 Running schema fix migration...")
    
    manager = MigrationManager()
    await manager.migrate_to_latest()
    
    print("✅ Schema fix migration completed successfully!")


if __name__ == "__main__":
    asyncio.run(main())