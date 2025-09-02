import logging
import traceback
from dataclasses import dataclass
from typing import List
from fastapi import FastAPI, Request, status
from fastapi.responses import JSONResponse
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from events.api import router as events_router
import gin

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

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize configuration
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

@app.get("/")
async def root():
    logger.info("Root endpoint accessed")
    return {"message": "ATS GenAI API is running"}

@app.get("/health")
async def health_check():
    logger.info("Health check endpoint accessed")
    try:
        db_connected = await check_db_connection()
        return {
            "status": "healthy",
            "database": "connected" if db_connected else "disconnected"
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

# Helper function to check database connection
async def check_db_connection() -> bool:
    try:
        # Import here to avoid circular imports
        from config.environment import Environment
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
