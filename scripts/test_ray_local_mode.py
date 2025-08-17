#!/usr/bin/env python
"""
Test script for Ray local mode configuration.

This script tests the Ray initialization with the fixes we've implemented
to ensure Ray runs in local mode without autoscaler errors.
"""

import os
import ray
import logging
import time

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("ray_test")

def test_ray_local_mode():
    """Test Ray initialization with local mode configuration."""
    # Set environment variables for Ray local mode
    logger.info("Setting Ray environment variables for local mode")
    os.environ["RAY_TMPDIR"] = "/tmp/ray"
    os.environ["RAY_DISABLE_DASHBOARD"] = "1"
    os.environ["RAY_USAGE_STATS_ENABLED"] = "0"
    os.environ["RAY_WORKER_REGISTER_TIMEOUT_SECONDS"] = "60"
    os.environ["PYTHONUNBUFFERED"] = "1"
    os.environ["RAY_ADDRESS"] = "local"
    
    # Ensure we do not accidentally connect to a Ray cluster via env
    os.environ.pop("RAY_ADDRESS", None)
    
    # Extra runtime safety knobs
    os.environ.setdefault("RAY_DISABLE_DASHBOARD", "1")
    os.environ.setdefault("RAY_USAGE_STATS_ENABLED", "0")
    os.environ.setdefault("RAY_SCHEDULER_EVENTS", "0")
    
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
    
    # Test CPU resource allocation
    @ray.remote(num_cpus=1)
    def cpu_task():
        time.sleep(1)
        return "CPU task completed"
    
    logger.info("Testing CPU resource allocation")
    result = ray.get(cpu_task.remote())
    logger.info(f"CPU task result: {result}")
    
    # Shutdown Ray
    logger.info("Shutting down Ray")
    ray.shutdown()
    logger.info("Ray test completed successfully")

if __name__ == "__main__":
    test_ray_local_mode()
