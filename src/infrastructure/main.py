import logging
import traceback
from dataclasses import dataclass
from typing import List
from fastapi import FastAPI, Request, status
from fastapi.responses import JSONResponse
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from domains.analytics.events.api import router as events_router
from api.model_registry_endpoints import model_registry_bp
from api.datasets_api import datasets_router
import gin

# Import environment-specific configuration system
from core.config.environment_config import load_gin_config, get_current_env, get_env_info
from core.config.validation import validate_current_config

@gin.configurable
@dataclass
class FastAPIConfig:
    """Configuration for FastAPI application"""
    title: str = "ATS GenAI API"
    description: str = "Algorithmic Trading System with GenAI"
    version: str = "0.1.0"

@gin.configurable
@dataclass
class CORSConfig:
    """Configuration for CORS middleware"""
    allow_origins: List[str] = None
    allow_credentials: bool = True
    allow_methods: List[str] = None
    allow_headers: List[str] = None

    def __post_init__(self):
        if self.allow_origins is None:
            self.allow_origins = ["*"]
        if self.allow_methods is None:
            self.allow_methods = ["*"]
        if self.allow_headers is None:
            self.allow_headers = ["*"]

# Load environment-specific configuration
try:
    detected_env = load_gin_config()
    print(f"🚀 ATS GenAI API starting in {detected_env.value} environment")

    # Validate configuration
    validation_result = validate_current_config()
    if not validation_result.is_valid:
        print("⚠️  Configuration validation warnings:")
        for warning in validation_result.warnings:
            print(f"   - {warning}")
        for error in validation_result.errors:
            print(f"   ❌ {error}")
    else:
        print("✅ Configuration validation passed")

except Exception as e:
    print(f"❌ Failed to load environment configuration: {e}")
    print("🔄 Falling back to default configuration...")

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize gin-configured settings
fastapi_config = FastAPIConfig()
cors_config = CORSConfig()

app = FastAPI(
    title=fastapi_config.title,
    description=fastapi_config.description,
    version=fastapi_config.version
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=cors_config.allow_origins,
    allow_credentials=cors_config.allow_credentials,
    allow_methods=cors_config.allow_methods,
    allow_headers=cors_config.allow_headers,
)

# Global exception handler
@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    logger.error(f"Unhandled exception: {str(exc)}")
    logger.error(traceback.format_exc())
    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content={"detail": "Internal server error", "error": str(exc)},
    )

# Validation error handler
@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    logger.error(f"Validation error: {exc.errors()}")
    return JSONResponse(
        status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
        content={"detail": exc.errors(), "body": exc.body},
    )

# Include the events router with a prefix
app.include_router(events_router, prefix="/api/v1", tags=["events"])

# Include model registry endpoints
app.include_router(model_registry_bp, tags=["models"])

# Include dataset service endpoints with feature metadata APIs
app.include_router(datasets_router, prefix="/api/v1", tags=["datasets"])

@app.get("/")
async def root():
    logger.info("Root endpoint accessed")
    return {"message": "ATS GenAI API is running"}

@app.get("/health")
async def health_check():
    logger.info("Health check endpoint accessed")
    try:
        db_connected = await check_db_connection()
        current_env = get_current_env()

        return {
            "status": "healthy",
            "database": "connected" if db_connected else "disconnected",
            "environment": current_env.value if current_env else "unknown",
            "configuration_loaded": current_env is not None
        }
    except Exception as e:
        import traceback
        error_details = {
            "error": str(e),
            "type": type(e).__name__,
            "traceback": traceback.format_exc()
        }
        logger.error(f"Health check failed: {error_details}")
        return {
            "status": "error",
            "database": "error",
            "error": str(e),
            "type": type(e).__name__
        }

@app.get("/config")
async def get_configuration_info():
    """Get current environment configuration information"""
    try:
        env_info = get_env_info()
        current_env = get_current_env()

        # Add FastAPI configuration details
        fastapi_info = {
            "title": fastapi_config.title,
            "description": fastapi_config.description,
            "version": fastapi_config.version
        }

        cors_info = {
            "allow_origins": cors_config.allow_origins,
            "allow_credentials": cors_config.allow_credentials,
            "allow_methods": cors_config.allow_methods,
            "allow_headers": cors_config.allow_headers
        }

        return {
            "current_environment": current_env.value if current_env else None,
            "environment_info": env_info,
            "fastapi_config": fastapi_info,
            "cors_config": cors_info,
            "configuration_status": "loaded" if current_env else "not_loaded"
        }

    except Exception as e:
        logger.error(f"Configuration info retrieval failed: {str(e)}")
        return {
            "error": f"Failed to retrieve configuration info: {str(e)}",
            "configuration_status": "error"
        }

# Helper function to check database connection
async def check_db_connection() -> bool:
    try:
        # Import here to avoid circular imports
        from core.platform.config_env.environment import Environment
        import asyncpg

        # Create a new environment instance which will use the Gin config
        env = Environment()

        # Get the database URL from the environment
        db_url = env.get_database_url()
        if not db_url:
            logger.error("No database URL configured")
            return False

        # Parse the database URL to get connection parameters
        from urllib.parse import urlparse

        parsed = urlparse(db_url)
        db_params = {
            'host': parsed.hostname,
            'port': parsed.port or 5432,
            'user': parsed.username,
            'password': parsed.password,
            'database': parsed.path.lstrip('/')
        }

        # Try to connect to the database
        conn = await asyncpg.connect(**db_params)
        await conn.close()
        return True

    except Exception as e:
        logger.error(f"Database connection check failed: {str(e)}")
        logger.error(traceback.format_exc())
        return False
