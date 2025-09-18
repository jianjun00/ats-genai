#!/usr/bin/env python3
"""
Script to update import paths after directory restructuring.
Maps old import paths to new clean architecture paths.
"""

import os
import re
import sys
from pathlib import Path

# Mapping of old import paths to new paths
IMPORT_MAPPINGS = {
    # Shared utilities moved to core/shared
    'from shared.': 'from core.shared.',
    'import shared.': 'import core.shared.',
    
    # Agents moved to domains/data_quality/agents
    'from agents.': 'from domains.data_quality.agents.',
    'import agents.': 'import domains.data_quality.agents.',
    
    # Events moved to domains/analytics/events
    'from events.': 'from domains.analytics.events.',
    'import events.': 'import domains.analytics.events.',
    
    # Signals moved to domains/trading/signals
    'from signals.': 'from domains.trading.signals.',
    'import signals.': 'import domains.trading.signals.',
    
    # Interfaces moved to infrastructure/interfaces
    'from interfaces.': 'from infrastructure.interfaces.',
    'import interfaces.': 'import infrastructure.interfaces.',
    
    # ML moved to domains/ml/legacy
    'from ml.': 'from domains.ml.legacy.',
    'import ml.': 'import domains.ml.legacy.',
    
    # Monitoring moved to infrastructure/monitoring/legacy
    'from monitoring.': 'from infrastructure.monitoring.legacy.',
    'import monitoring.': 'import infrastructure.monitoring.legacy.',
    
    # Observability moved to infrastructure/monitoring/observability
    'from observability.': 'from infrastructure.monitoring.observability.',
    'import observability.': 'import infrastructure.monitoring.observability.',
    
    # Jobs moved to infrastructure/jobs
    'from jobs.': 'from infrastructure.jobs.',
    'import jobs.': 'import infrastructure.jobs.',
    
    # MCP tools moved to infrastructure/tools/mcp
    'from mcp_tools.': 'from infrastructure.tools.mcp.',
    'import mcp_tools.': 'import infrastructure.tools.mcp.',
    
    # Services reorganization
    'from services.analytics_service': 'from domains.analytics.services.analytics_service',
    'import services.analytics_service': 'import domains.analytics.services.analytics_service',
    'from services.data_quality.': 'from domains.data_quality.services.',
    'import services.data_quality.': 'import domains.data_quality.services.',
    'from services.ml_services.': 'from domains.ml.services.',
    'import services.ml_services.': 'import domains.ml.services.',
    'from services.financial_events.': 'from domains.analytics.services.financial_events.',
    'import services.financial_events.': 'import domains.analytics.services.financial_events.',
    'from services.data_services.': 'from infrastructure.data.',
    'import services.data_services.': 'import infrastructure.data.',
    'from services.web_services.': 'from infrastructure.web.',
    'import services.web_services.': 'import infrastructure.web.',
    'from services.prometheus_metrics': 'from infrastructure.monitoring.prometheus.prometheus_metrics',
    'import services.prometheus_metrics': 'import infrastructure.monitoring.prometheus.prometheus_metrics',
    'from services.core.': 'from domains.trading.services.core.',
    'import services.core.': 'import domains.trading.services.core.',
}

def update_file_imports(file_path: Path) -> bool:
    """Update imports in a single file."""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        original_content = content
        
        # Apply import mappings
        for old_import, new_import in IMPORT_MAPPINGS.items():
            content = content.replace(old_import, new_import)
        
        # Write back if changed
        if content != original_content:
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(content)
            print(f"Updated: {file_path}")
            return True
        
        return False
    
    except Exception as e:
        print(f"Error updating {file_path}: {e}")
        return False

def find_python_files(directory: Path) -> list[Path]:
    """Find all Python files in directory recursively."""
    python_files = []
    for path in directory.rglob("*.py"):
        # Skip __pycache__ directories
        if "__pycache__" not in str(path):
            python_files.append(path)
    return python_files

def main():
    """Main function to update all imports."""
    # Get the src directory
    src_dir = Path(__file__).parent / "src"
    tests_dir = Path(__file__).parent / "tests"
    scripts_dir = Path(__file__).parent / "scripts"
    
    if not src_dir.exists():
        print(f"Source directory not found: {src_dir}")
        return 1
    
    updated_count = 0
    total_count = 0
    
    # Update files in src, tests, and scripts directories
    for directory in [src_dir, tests_dir, scripts_dir]:
        if directory.exists():
            print(f"\nProcessing directory: {directory}")
            python_files = find_python_files(directory)
            
            for file_path in python_files:
                total_count += 1
                if update_file_imports(file_path):
                    updated_count += 1
    
    print(f"\nCompleted: {updated_count} files updated out of {total_count} total files")
    
    # Also update root level Python files
    root_dir = Path(__file__).parent
    for file_path in root_dir.glob("*.py"):
        if file_path.name != "update_imports.py":  # Skip this script
            total_count += 1
            if update_file_imports(file_path):
                updated_count += 1
    
    print(f"Final count: {updated_count} files updated out of {total_count} total files")
    return 0

if __name__ == "__main__":
    sys.exit(main())