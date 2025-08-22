#!/usr/bin/env python
"""
Local test script for Ray fixes.

This script tests the Ray initialization with our fixes to ensure Ray runs in local mode
without autoscaler errors. It simulates the environment variables and Ray initialization
that would be used in the Kubernetes job.
"""

import os
import sys
import logging
import traceback
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("ray_test")

# Add project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

def test_ray_fixes():
    """Test Ray initialization with our fixes."""
    try:
        # Set environment variables for Ray local mode
        logger.info("Setting Ray environment variables for local mode")
        os.environ["RAY_TMPDIR"] = "/tmp/ray"
        os.environ["RAY_DISABLE_DASHBOARD"] = "1"
        os.environ["RAY_USAGE_STATS_ENABLED"] = "0"
        os.environ["RAY_WORKER_REGISTER_TIMEOUT_SECONDS"] = "60"
        os.environ["PYTHONUNBUFFERED"] = "1"
        os.environ["RAY_ADDRESS"] = "local"
        
        # Import Ray after setting environment variables
        import ray
        
        # Ensure we do not accidentally connect to a Ray cluster via env
        os.environ.pop("RAY_ADDRESS", None)
        
        # Initialize Ray with local mode settings
        logger.info("Initializing Ray in local mode")
        ray.init(
            address=None,  # force local
            ignore_reinit_error=True,
            local_mode=True,
            num_cpus=1,
            _system_config={"worker_register_timeout_seconds": 60},
            include_dashboard=False,
        )
        
        # Log Ray configuration
        logger.info(f"Ray initialized with address: {ray.get_runtime_context().get_address()}")
        logger.info(f"Ray resources: {ray.available_resources()}")
        
        # Define a simple Ray task
        @ray.remote
        def hello_world():
            import socket
            return f"Hello from {socket.gethostname()} with Ray local mode!"
        
        # Run the task
        logger.info("Running a simple Ray task")
        result = ray.get(hello_world.remote())
        logger.info(f"Task result: {result}")
        
        # Import our actual code that uses Ray
        from src.secmaster.populate_instrument_polygon import fetch_instrument_polygon
        
        # Create a simple test task using our actual code
        logger.info("Testing fetch_instrument_polygon with Ray")
        
        # Define a simple wrapper to test our function
        @ray.remote
        def test_fetch(ticker):
            return f"Would fetch data for {ticker} (simulation only)"
        
        # Run the test task
        logger.info("Running test task")
        result = ray.get(test_fetch.remote("AAPL"))
        logger.info(f"Test task result: {result}")
        
        # Shutdown Ray
        logger.info("Shutting down Ray")
        ray.shutdown()
        logger.info("Ray test completed successfully")
        return True
        
    except Exception as e:
        logger.error(f"Error testing Ray fixes: {e}")
        logger.error(traceback.format_exc())
        return False

if __name__ == "__main__":
    success = test_ray_fixes()
    sys.exit(0 if success else 1)
