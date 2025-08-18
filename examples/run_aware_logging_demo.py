#!/usr/bin/env python3
"""
Run-Aware Logging Demonstration

Shows how logging includes run_id for better traceability.
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from core.run_context import create_run_context
from core.run_aware_logging import setup_run_aware_logging, get_run_aware_logger
import logging


def demonstrate_run_aware_logging():
    """Demonstrate run-aware logging capabilities."""
    
    print("🚀 Run-Aware Logging Demonstration")
    print("=" * 50)
    
    # Create run context
    run_context = create_run_context(metadata={'demo': 'logging_demo'})
    print(f"📋 Created run context: {run_context.run_id}")
    print(f"📁 Log directory: {run_context.logs_dir}")
    
    # Set up run-aware logging
    setup_run_aware_logging(run_context=run_context, detailed_format=True)
    
    # Get loggers
    main_logger = get_run_aware_logger(__name__, run_context)
    test_logger = get_run_aware_logger("test.module", run_context)
    
    print("\n📝 Logging messages with run_id:")
    print("-" * 30)
    
    # Log various messages
    main_logger.info("Starting demonstration")
    main_logger.debug("This is a debug message")
    main_logger.warning("This is a warning message")
    test_logger.info("Message from test module")
    test_logger.error("Simulated error for demo")
    
    # Show log file content
    log_file = run_context.logs_dir / "ats_genai.log"
    if log_file.exists():
        print(f"\n📄 Contents of {log_file}:")
        print("-" * 50)
        with open(log_file, 'r') as f:
            content = f.read()
            print(content)
    
    print("\n✅ Demo complete!")
    print(f"🗂️  All logs saved to: {log_file}")
    
    return run_context


if __name__ == "__main__":
    run_context = demonstrate_run_aware_logging()
    
    # Optional cleanup
    cleanup = input("\n🗑️  Clean up demo files? (y/N): ").lower().strip()
    if cleanup == 'y':
        import shutil
        shutil.rmtree(run_context.base_dir)
        print(f"✅ Cleaned up: {run_context.base_dir}")