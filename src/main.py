import logging
import traceback
from fastapi import FastAPI, Request, status
from fastapi.responses import JSONResponse
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from events.api import router as events_router

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="ATS GenAI API",
    description="Algorithmic Trading System with GenAI",
    version="0.1.0"
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
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
