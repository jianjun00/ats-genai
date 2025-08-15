import gin
import os
import asyncio
import logging
import asyncpg
from .db_retry import retry_async

logger = logging.getLogger(__name__)

@gin.configurable
class Database:
    def __init__(self, host=None, port=None, user=None, password=None, database=None, base_database=None, pool_min_size=1, pool_max_size=10, command_timeout=60):
        # Use environment variables if set, otherwise use provided values or defaults
        env_type = os.getenv("ENVIRONMENT", "").lower()
        
        # Store environment type for later use
        self.env_type = env_type
        
        # Set host based on environment
        db_host = os.getenv("DB_HOST")
        if not db_host:
            # Check if we're in a Kubernetes environment
            if os.getenv("KUBERNETES_SERVICE_HOST"):
                if env_type == "dev":
                    # Use fully qualified service name for Kubernetes
                    db_host = "timescaledb.ats-dev.svc.cluster.local"
                    logger.info(f"Using Kubernetes service host for dev environment: {db_host}")
                elif env_type == "intg":
                    db_host = "timescaledb.ats-intg.svc.cluster.local"
                    logger.info(f"Using Kubernetes service host for intg environment: {db_host}")
                elif env_type == "prod":
                    db_host = "timescaledb.ats-prod.svc.cluster.local"
                    logger.info(f"Using Kubernetes service host for prod environment: {db_host}")
            else:
                # For local development or Docker Compose
                if env_type == "dev" or env_type == "intg":
                    db_host = "timescaledb"
                    logger.info(f"Using Docker service name for {env_type} environment: {db_host}")
        
        self.host = db_host or host or 'localhost'
        self.port = int(os.getenv("DB_PORT") or port or 5432)
        self.user = os.getenv("DB_USER") or user or 'postgres'
        self.password = os.getenv("DB_PASSWORD") or password or 'postgres'
        
        # Set database name based on environment
        db_name = os.getenv("DB_NAME")
        if not db_name:
            if env_type == "dev":
                db_name = "dev_db"
                logger.info(f"Using dev_db for dev environment")
            elif env_type == "intg":
                db_name = "intg_db"
                logger.info(f"Using intg_db for intg environment")
            else:
                db_name = database or 'trading_db'
        
        self.database = db_name
        self.base_database = base_database or 'postgres'
        self.pool_min_size = pool_min_size
        self.pool_max_size = pool_max_size
        self.command_timeout = command_timeout
        
        logger.info(f"Configured database connection to {self.host}:{self.port}/{self.database}")

    def get_database_url(self):
        """Return a DSN string for asyncpg or psycopg2."""
        # Ensure all components are properly set before constructing the URL
        if not all([self.host, self.port, self.user, self.password, self.database]):
            logger.warning(f"Incomplete database configuration: host={self.host}, port={self.port}, user={self.user}, password={'*****' if self.password else None}, database={self.database}")
        
        # Check if this is a Kubernetes environment
        is_kubernetes = os.getenv("KUBERNETES_SERVICE_HOST") is not None
        
        # For Kubernetes environments, use appropriate connection parameters
        if is_kubernetes:
            logger.info(f"Kubernetes environment detected for {self.env_type}, using connection for {self.host}")
            # Use connection timeout and disable SSL for all Kubernetes environments
            return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}?connect_timeout=10&sslmode=disable"
        else:
            # For local development, use standard connection string
            logger.info(f"Local environment detected, using standard connection for {self.host}")
            return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"
            
    async def create_pool_with_retry(self, max_retries=3, initial_delay=1.0, timeout=10.0):
        """Create a connection pool with retry logic.
        
        Args:
            max_retries: Maximum number of retries
            initial_delay: Initial delay between retries in seconds
            timeout: Connection timeout in seconds
            
        Returns:
            asyncpg.pool.Pool: The connection pool
            
        Raises:
            Exception: If all connection attempts fail
        """
        logger.info(f"Creating database connection pool to {self.host}:{self.port}/{self.database} with {max_retries} retries")
        
        # Define the connection function to retry
        async def connect_to_db():
            connection_url = self.get_database_url()
            logger.info(f"Connecting to database with URL: {connection_url.replace(self.password, '******')}")
            return await asyncpg.create_pool(
                connection_url,
                min_size=self.pool_min_size,
                max_size=self.pool_max_size,
                command_timeout=self.command_timeout,
                timeout=timeout
            )
        
        # Use retry logic
        try:
            return await retry_async(
                connect_to_db,
                retries=max_retries,
                delay=initial_delay,
                backoff_factor=2.0,
                exceptions=(asyncio.TimeoutError, ConnectionRefusedError, OSError, asyncpg.exceptions.PostgresError)
            )
        except Exception as e:
            logger.error(f"Failed to connect to database after {max_retries + 1} attempts: {str(e)}")
            raise
            
    @classmethod
    async def create_connection_pool(cls, env=None, max_retries=3, initial_delay=1.0, timeout=10.0):
        """Class method to create a database connection pool.
        
        This is a convenience method that creates a Database instance and then creates a connection pool.
        
        Args:
            env: Optional Environment instance. If not provided, a new Database instance will be created.
            max_retries: Maximum number of retries
            initial_delay: Initial delay between retries in seconds
            timeout: Connection timeout in seconds
            
        Returns:
            asyncpg.pool.Pool: The connection pool
        """
        if env and hasattr(env, 'database'):
            db = env.database
        else:
            db = cls()
            
        logger.info(f"Creating connection pool for {db.host}:{db.port}/{db.database}")
        return await db.create_pool_with_retry(max_retries=max_retries, initial_delay=initial_delay, timeout=timeout)
