import gin
import os
import asyncio
import logging
from .db_retry import retry_async

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
            if env_type == "dev":
                # Try simpler service name first
                db_host = "timescaledb"
                print(f"[DATABASE] Using simplified Kubernetes service host for dev environment: {db_host}")
            elif env_type == "intg":
                db_host = "timescaledb"
                print(f"[DATABASE] Using simplified Kubernetes service host for intg environment: {db_host}")
        
        self.host = db_host or host or 'localhost'
        self.port = int(os.getenv("DB_PORT") or port or 5432)
        self.user = os.getenv("DB_USER") or user or 'postgres'
        self.password = os.getenv("DB_PASSWORD") or password or 'postgres'
        
        # Set database name based on environment
        db_name = os.getenv("DB_NAME")
        if not db_name:
            if env_type == "dev":
                db_name = "dev_db"
                print(f"[DATABASE] Using dev_db for dev environment")
            elif env_type == "intg":
                db_name = "intg_db"
                print(f"[DATABASE] Using intg_db for intg environment")
            else:
                db_name = database or 'trading_db'
        
        self.database = db_name
        self.base_database = base_database or 'postgres'
        self.pool_min_size = pool_min_size
        self.pool_max_size = pool_max_size
        self.command_timeout = command_timeout
        
        print(f"[DATABASE] Configured connection to {self.host}:{self.port}/{self.database}")

    def get_database_url(self):
        """Return a DSN string for asyncpg or psycopg2."""
        # Ensure all components are properly set before constructing the URL
        if not all([self.host, self.port, self.user, self.password, self.database]):
            print(f"[WARNING] Incomplete database configuration: host={self.host}, port={self.port}, user={self.user}, password={'*****' if self.password else None}, database={self.database}")
        
        # Check if this is a Kubernetes environment based on environment type
        is_kubernetes = self.env_type in ["dev", "intg", "prod"] and not self.host.startswith('localhost')
        
        # For Kubernetes environments, use appropriate connection parameters
        if is_kubernetes:
            # For dev/intg environments in Kubernetes, we need special handling
            if self.env_type == "dev":
                print(f"[DATABASE] Kubernetes dev environment detected, using special connection for {self.host}")
                # Try with connection timeout and disable SSL
                return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}?connect_timeout=10&sslmode=disable"
            elif self.env_type == "intg":
                print(f"[DATABASE] Kubernetes intg environment detected, using special connection for {self.host}")
                # Try with connection timeout and disable SSL
                return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}?connect_timeout=10&sslmode=disable"
            else:
                # For other Kubernetes environments
                print(f"[DATABASE] Kubernetes environment detected, using standard connection with SSL disabled for {self.host}")
                return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}?sslmode=disable"
        else:
            # For local development, use standard connection string
            return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"
            
    async def create_pool_with_retry(self, asyncpg, max_retries=3, initial_delay=1.0):
        """Create a connection pool with retry logic.
        
        Args:
            asyncpg: The asyncpg module
            max_retries: Maximum number of retries
            initial_delay: Initial delay between retries in seconds
            
        Returns:
            asyncpg.pool.Pool: The connection pool
            
        Raises:
            Exception: If all connection attempts fail
        """
        logging.info(f"Creating database connection pool to {self.host}:{self.port}/{self.database} with {max_retries} retries")
        
        # Define the connection function to retry
        async def connect_to_db():
            connection_url = self.get_database_url()
            logging.info(f"Connecting to database with URL: {connection_url.replace(self.password, '******')}")
            return await asyncpg.create_pool(
                connection_url,
                min_size=self.pool_min_size,
                max_size=self.pool_max_size,
                command_timeout=self.command_timeout
            )
        
        # Use retry logic
        try:
            return await retry_async(
                connect_to_db,
                retries=max_retries,
                delay=initial_delay,
                backoff_factor=2.0,
                exceptions=(asyncio.TimeoutError, ConnectionRefusedError, OSError)
            )
        except Exception as e:
            logging.error(f"Failed to connect to database after {max_retries + 1} attempts: {str(e)}")
            raise
