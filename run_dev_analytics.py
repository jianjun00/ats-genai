#!/usr/bin/env python3
"""
Quick Start: Dev Analytics Platform

One-command startup for the development analytics web interface
with automatic conflict resolution (handles Grafana on port 3000, etc.)
"""

import sys
import asyncio
from pathlib import Path

# Add scripts to path
sys.path.insert(0, str(Path(__file__).parent / "scripts/analytics"))

from setup_analytics_with_conflict_resolution import AnalyticsPlatformSetup

async def main():
    """Quick start the dev analytics platform with conflict resolution"""
    
    print("🚀 Starting Analytics Platform with Conflict Resolution...")
    print("   This will detect and resolve port conflicts (like Grafana)")
    print("   and set up the complete analytics interface on alternative ports.")
    print()
    
    setup = AnalyticsPlatformSetup()
    success = await setup.setup_analytics_platform()
    
    return 0 if success else 1

if __name__ == "__main__":
    sys.exit(asyncio.run(main()))