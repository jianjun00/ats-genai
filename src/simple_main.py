from fastapi import FastAPI
from typing import Dict, Any
import os

# Create FastAPI app
app = FastAPI(
    title="ATS GenAI API",
    description="Algorithmic Trading System with GenAI",
    version="1.0.0"
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
    uvicorn.run(app, host="0.0.0.0", port=8080)
