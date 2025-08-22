#!/usr/bin/env python3
"""
Flyte Dynamic Analytics API Deployment

This allows rapid deployment of Python changes without Docker rebuilds
by using Flyte's dynamic task execution capabilities.
"""

import asyncio
import os
from pathlib import Path
from flytekit import workflow, task, Resources
from flytekit.types.file import FlyteFile
from flytekit.types.directory import FlyteDirectory
from typing import Dict, Any

# Dynamic task for analytics API deployment
@task(
    requests=Resources(cpu="500m", mem="1Gi"),
    limits=Resources(cpu="1000m", mem="2Gi")
)
def deploy_analytics_api(
    source_code: FlyteDirectory,
    db_config: Dict[str, str]
) -> Dict[str, Any]:
    """
    Deploy analytics API with dynamic Python code injection
    """
    import subprocess
    import sys
    import json
    from pathlib import Path
    
    # Extract source code to container
    source_path = Path(source_code)
    app_path = Path("/app/dynamic_src")
    app_path.mkdir(exist_ok=True)
    
    # Copy source code
    subprocess.run(["cp", "-r", f"{source_path}/.", str(app_path)], check=True)
    
    # Set up Python path
    sys.path.insert(0, str(app_path))
    
    # Set environment variables for database
    for key, value in db_config.items():
        os.environ[key] = value
    
    try:
        # Install dependencies first
        import subprocess
        subprocess.run([
            "pip", "install", "-r", f"{app_path}/requirements.txt"
        ], check=True)
        
        # Import and run analytics API
        from analytics_api_dynamic import create_analytics_app
        
        app = create_analytics_app()
        
        # Start server in background
        import uvicorn
        import threading
        import traceback
        
        def run_server():
            uvicorn.run(app, host="0.0.0.0", port=8000, log_level="info")
        
        server_thread = threading.Thread(target=run_server, daemon=True)
        server_thread.start()
        
        # Wait for startup
        import time
        time.sleep(10)
        
        # Test server health
        import requests
        try:
            response = requests.get("http://localhost:8000/health", timeout=5)
            health_data = response.json() if response.status_code == 200 else {"status": "unhealthy"}
        except Exception as e:
            health_data = {"status": "connection_failed", "error": str(e)}
        
        return {
            "status": "success",
            "api_url": "http://localhost:8000",
            "health_check": health_data,
            "endpoints": [
                "/health",
                "/api/v1/backtests",
                "/api/v1/backtests/{id}/metrics", 
                "/api/v1/backtests/{id}/performance",
                "/api/v1/stats"
            ],
            "deployment_type": "flyte_dynamic",
            "database_config": {k: v for k, v in db_config.items() if k != "DB_PASSWORD"}
        }
        
    except Exception as e:
        import traceback
        return {
            "status": "failed",
            "error": str(e),
            "traceback": traceback.format_exc()
        }

@task
def create_analytics_source_package() -> FlyteDirectory:
    """
    Create dynamic source package for analytics API
    """
    import tempfile
    import shutil
    from pathlib import Path
    
    # Create temporary directory for source code
    temp_dir = Path(tempfile.mkdtemp())
    
    # Copy current analytics source files
    base_path = Path("/home/jianjun/ats-genai/src")  # Local development path
    
    source_files = [
        "analytics_api_dynamic.py",
        "config/environment.py",
        "config/database.py",
        "config/__init__.py"
    ]
    
    # Create __init__.py files for proper module structure
    (temp_dir / "__init__.py").touch()
    (temp_dir / "config").mkdir(exist_ok=True)
    (temp_dir / "config" / "__init__.py").touch()
    
    for file in source_files:
        src_file = base_path / file
        if src_file.exists():
            dest_file = temp_dir / file
            dest_file.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(src_file, dest_file)
        else:
            print(f"Warning: Source file not found: {src_file}")
    
    # Create minimal requirements.txt for dynamic deployment
    requirements_content = """
fastapi==0.68.0
uvicorn[standard]==0.15.0
asyncpg==0.24.0
pandas==1.3.3
numpy==1.21.2
pydantic==1.8.2
"""
    (temp_dir / "requirements.txt").write_text(requirements_content.strip())
    
    return FlyteDirectory(path=str(temp_dir))

@workflow
def deploy_analytics_workflow(
    db_host: str = "postgres",
    db_port: str = "5432", 
    db_user: str = "postgres",
    db_password: str = "dev_password",
    db_name: str = "dev_db"
) -> Dict[str, Any]:
    """
    Complete workflow to deploy analytics API with dynamic code
    """
    
    # Create database configuration
    db_config = {
        "DB_HOST": db_host,
        "DB_PORT": db_port,
        "DB_USER": db_user,
        "DB_PASSWORD": db_password,
        "DB_NAME": db_name,
        "ENVIRONMENT": "dev",
        "PYTHONPATH": "/app/dynamic_src"
    }
    
    # Package source code
    source_package = create_analytics_source_package()
    
    # Deploy API
    deployment_result = deploy_analytics_api(
        source_code=source_package,
        db_config=db_config
    )
    
    return deployment_result

if __name__ == "__main__":
    # Run the workflow locally for testing
    result = deploy_analytics_workflow()
    print(f"Deployment result: {result}")