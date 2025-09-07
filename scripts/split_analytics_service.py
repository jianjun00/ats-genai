#!/usr/bin/env python3
"""
Analytics Service Splitter
Splits the 4,143-line analytics_service.py into 7 focused modules.
"""
import os
from pathlib import Path

def split_analytics_service():
    """Split the large analytics service into focused modules."""

    source_file = Path("/home/jianjun/ats-genai-data/src/services/web_services/analytics_service.py")
    modules_dir = Path("/home/jianjun/ats-genai-data/src/services/web_services/analytics_modules")

    modules_dir.mkdir(exist_ok=True)

    with open(source_file, 'r', encoding='utf-8') as f:
        lines = f.readlines()

    # Define split points based on section analysis
    splits = {
        "analytics_service_core.py": {
            "start": 1,
            "end": 190,
            "description": "Core service initialization and configuration"
        },
        "type_aware_analyzer.py": {
            "start": 111,
            "end": 259,
            "description": "Type-aware analysis and intelligent filters"
        },
        "training_data_manager.py": {
            "start": 260,
            "end": 533,
            "description": "Training dataset management and visualization"
        },
        "data_analysis_engine.py": {
            "start": 534,
            "end": 1418,
            "description": "Data filtering, aggregation, and statistical analysis"
        },
        "news_events_handler.py": {
            "start": 1419,
            "end": 1565,
            "description": "News events and economic data handling"
        },
        "dashboard_generator.py": {
            "start": 1676,
            "end": 3265,
            "description": "EDA dashboard HTML generation and visualization"
        },
        "request_handler.py": {
            "start": 3266,
            "end": -1,
            "description": "HTTP request handling and server management"
        }
    }

    # Common imports for all modules
    common_imports = """#!/usr/bin/env python3
import asyncio
import json
import logging
import os
import sys
import time
import numpy as np
from datetime import datetime, date, timedelta
from pathlib import Path
from typing import Dict, List, Any, Optional
from dataclasses import asdict

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Import core components
from core.database.connection_manager import get_connection_manager
from core.config.settings import get_settings

"""

    for module_name, config in splits.items():
        print(f"Creating {module_name}...")

        module_path = modules_dir / module_name
        start_line = config["start"] - 1  # Convert to 0-based index
        end_line = config["end"] if config["end"] != -1 else len(lines)

        with open(module_path, 'w', encoding='utf-8') as f:
            f.write('"""' + f'\n{config["description"]}\n' + '"""\n\n')
            f.write(common_imports)
            f.write('\n')

            # Write the specific section
            for i in range(start_line, end_line):
                if i < len(lines):
                    f.write(lines[i])

        print(f"  ✅ Created {module_name} ({end_line - start_line} lines)")

    # Create __init__.py for the modules package
    init_path = modules_dir / "__init__.py"
    with open(init_path, 'w', encoding='utf-8') as f:
        f.write('"""Analytics Service Modules Package"""\n')

    print(f"\n🎯 Successfully split analytics_service.py into 7 focused modules")
    print(f"   Original file: {len(lines)} lines")
    print(f"   Modules directory: {modules_dir}")

if __name__ == "__main__":
    split_analytics_service()