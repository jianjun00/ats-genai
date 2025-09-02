from fastapi import FastAPI
from typing import Dict, Any
import os
import gin

# Gin configurable parameters
@gin.configurable
class ApiConfig:
    def __init__(self, 
                 title: str = "ATS GenAI API",
                 description: str = "Algorithmic Trading System with GenAI", 
                 version: str = "1.0.0",
                 port: int = 8080,
                 host: str = "0.0.0.0"):
        self.title = title
        self.description = description
        self.version = version
        self.port = port
        self.host = host

# Initialize configuration (will be overridden by gin)
config = ApiConfig()

# Create FastAPI app
app = FastAPI(
    title=config.title,
    description=config.description,
    version=config.version
)

@app.get("/")
async def root() -> Dict[str, str]:
    """Root endpoint"""
    return {
        "message": "ATS GenAI API is running",
        "status": "healthy",
        "environment": os.getenv("ENVIRONMENT", "development")
    }

@app.get("/health")
async def health_check() -> Dict[str, str]:
    """Health check endpoint for Kubernetes probes"""
    return {
        "status": "healthy",
        "service": "ats-genai-api",
        "environment": os.getenv("ENVIRONMENT", "development")
    }

@app.get("/api/v1/status")
async def api_status() -> Dict[str, Any]:
    """API status endpoint"""
    return {
        "api_version": "v1",
        "status": "operational",
        "environment": os.getenv("ENVIRONMENT", "development"),
        "database_url": os.getenv("DATABASE_URL", "not_configured"),
        "features": [
            "health_checks",
            "status_monitoring",
            "environment_config"
        ]
    }

if __name__ == "__main__":
    import uvicorn
    
    # Load gin configuration if available
    gin_config = os.getenv("GIN_CONFIG", "config/hardcoded_values.gin")
    if os.path.exists(gin_config):
        gin.parse_config_file(gin_config)
        # Reinitialize config after gin parsing
        config = ApiConfig()
    
    uvicorn.run(app, host=config.host, port=config.port)
