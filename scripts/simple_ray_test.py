#!/usr/bin/env python
"""
Simple Ray test script.

This script tests Ray initialization with local mode settings to verify
that our fixes prevent autoscaler errors.
"""

import os
import sys
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    stream=sys.stdout
)
logger = logging.getLogger("simple_ray_test")

def main():
    """Test Ray initialization with local mode settings."""
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
        logger.info("Importing Ray")
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
        logger.info(f"Ray initialized with GCS address: {ray.get_runtime_context().gcs_address}")
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
        
        # Shutdown Ray
        logger.info("Shutting down Ray")
        ray.shutdown()
        logger.info("Ray test completed successfully")
        return True
        
    except Exception as e:
        logger.error(f"Error testing Ray fixes: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
