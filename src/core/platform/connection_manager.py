"""
Centralized database connection management.

This module provides a unified interface for all database connections
with connection pooling, retry logic, and environment isolation.
"""

import asyncio
import logging
from contextlib import contextmanager, asynccontextmanager
from typing import Optional, Dict, Any, Generator, AsyncGenerator
from sqlalchemy import create_engine, Engine, text
from sqlalchemy.ext.asyncio import create_async_engine, AsyncEngine, AsyncSession
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.pool import QueuePool
import psycopg2
from psycopg2.extras import RealDictCursor

from core.platform.config_core.settings import get_settings
from core.custom_exceptions import DatabaseConnectionError, DatabaseError


logger = logging.getLogger(__name__)


class DatabaseConnectionManager:
    """
    Centralized database connection management with pooling and retry logic.

    Provides both sync and async connections with automatic environment
    detection and connection health monitoring.
    """

    def __init__(self):
        self.settings = get_settings()
        self._engine: Optional[Engine] = None
        self._async_engine: Optional[AsyncEngine] = None
        self._session_factory: Optional[sessionmaker] = None
        self._async_session_factory: Optional[sessionmaker] = None

    @property
    def engine(self) -> Engine:
        """Get or create SQLAlchemy engine."""
        if self._engine is None:
            self._engine = self._create_engine()
        return self._engine

    @property
    def async_engine(self) -> AsyncEngine:
        """Get or create async SQLAlchemy engine."""
        if self._async_engine is None:
            self._async_engine = self._create_async_engine()
        return self._async_engine

    def _create_engine(self) -> Engine:
        """Create SQLAlchemy engine with connection pooling."""
        engine_config = {
            "poolclass": QueuePool,
            "pool_size": self.settings.database_pool_size,
            "max_overflow": self.settings.database_max_overflow,
            "pool_timeout": self.settings.database_pool_timeout,
            "pool_recycle": self.settings.database_pool_recycle,
            "pool_pre_ping": True,  # Validate connections before use
            "echo": self.settings.database_echo,
        }

        # Add SSL configuration for production
        if self.settings.is_production:
            engine_config["connect_args"] = {"sslmode": "require"}

        try:
            engine = create_engine(self.settings.database_url, **engine_config)

            # Test connection
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))

            logger.info(
                f"Database engine created successfully for {self.settings.environment}",
                extra={
                    "database_host": self.settings.database_host,
                    "database_name": self.settings.database_name,
                    "pool_size": self.settings.database_pool_size
                }
            )
            return engine

        except Exception as e:
            logger.error(
                f"Failed to create database engine: {e}",
                extra={"database_url_masked": self._mask_password(self.settings.database_url)}
            )
            raise DatabaseConnectionError(f"Failed to create database engine: {e}")

    def _create_async_engine(self) -> AsyncEngine:
        """Create async SQLAlchemy engine."""
        engine_config = {
            "poolclass": QueuePool,
            "pool_size": self.settings.database_pool_size,
            "max_overflow": self.settings.database_max_overflow,
            "pool_timeout": self.settings.database_pool_timeout,
            "pool_recycle": self.settings.database_pool_recycle,
            "pool_pre_ping": True,
            "echo": self.settings.database_echo,
        }

        if self.settings.is_production:
            engine_config["connect_args"] = {"server_settings": {"sslmode": "require"}}

        try:
            return create_async_engine(self.settings.async_database_url, **engine_config)
        except Exception as e:
            logger.error(f"Failed to create async database engine: {e}")
            raise DatabaseConnectionError(f"Failed to create async database engine: {e}")

    def get_session_factory(self) -> sessionmaker:
        """Get or create session factory."""
        if self._session_factory is None:
            self._session_factory = sessionmaker(
                bind=self.engine,
                autocommit=False,
                autoflush=False,
                expire_on_commit=False
            )
        return self._session_factory

    def get_async_session_factory(self) -> sessionmaker:
        """Get or create async session factory."""
        if self._async_session_factory is None:
            self._async_session_factory = sessionmaker(
                bind=self.async_engine,
                class_=AsyncSession,
                autocommit=False,
                autoflush=False,
                expire_on_commit=False
            )
        return self._async_session_factory

    @contextmanager
    def get_session(self) -> Generator[Session, None, None]:
        """
        Get database session with automatic transaction management.

        Usage:
            with db_manager.get_session() as session:
                # Use session
                pass
        """
        session_factory = self.get_session_factory()
        session = session_factory()
        try:
            yield session
            session.commit()
        except Exception as e:
            session.rollback()
            logger.error(f"Database session error: {e}")
            raise DatabaseError(f"Database operation failed: {e}")
        finally:
            session.close()

    @asynccontextmanager
    async def get_async_session(self) -> AsyncGenerator[AsyncSession, None]:
        """
        Get async database session with automatic transaction management.

        Usage:
            async with db_manager.get_async_session() as session:
                # Use async session
                pass
        """
        session_factory = self.get_async_session_factory()
        session = session_factory()
        try:
            yield session
            await session.commit()
        except Exception as e:
            await session.rollback()
            logger.error(f"Async database session error: {e}")
            raise DatabaseError(f"Async database operation failed: {e}")
        finally:
            await session.close()

    @contextmanager
    def get_raw_connection(self) -> Generator[psycopg2.extensions.connection, None, None]:
        """
        Get raw psycopg2 connection for direct SQL operations with fallback attempts.

        Usage:
            with db_manager.get_raw_connection() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                    cursor.execute("SELECT * FROM table")
                    results = cursor.fetchall()
        """
        connection = None

        # Multiple connection attempts for container environments
        connection_attempts = [
            # Primary: Use configured settings
            {
                'host': self.settings.database_host,
                'port': self.settings.database_port,
                'database': self.settings.database_name,
                'user': self.settings.database_user,
                'password': self.settings.database_password
            }
        ]

        # REMOVED: No fallback connections - fail fast instead of silent fallbacks to wrong databases
        # This prevents network misconfigurations from being masked by connecting to dev database

        # Validate network connectivity before attempting connection
        self._validate_network_connectivity()

        last_exception = None
        for attempt in connection_attempts:
            try:
                connection = psycopg2.connect(
                    host=attempt['host'],
                    port=attempt['port'],
                    database=attempt['database'],
                    user=attempt['user'],
                    password=attempt['password'],
                    cursor_factory=RealDictCursor
                )
                connection.autocommit = False
                logger.debug(f"Connected via {attempt['host']}:{attempt['port']}")
                break
            except Exception as e:
                last_exception = e
                logger.debug(f"Connection attempt failed for {attempt['host']}:{attempt['port']} - {e}")
                continue

        if not connection:
            logger.error(f"All database connection attempts failed. Last error: {last_exception}")
            raise DatabaseError(f"All database connection attempts failed: {last_exception}")

        try:
            yield connection
            connection.commit()
        except Exception as e:
            if connection:
                connection.rollback()
            logger.error(f"Raw database operation error: {e}")
            raise DatabaseError(f"Raw database operation failed: {e}")
        finally:
            if connection:
                connection.close()

    def check_connection(self) -> bool:
        """Check if database connection is healthy."""
        try:
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            return True
        except Exception as e:
            logger.error(f"Database health check failed: {e}")
            return False

    async def check_async_connection(self) -> bool:
        """Check if async database connection is healthy."""
        try:
            async with self.async_engine.begin() as conn:
                await conn.execute(text("SELECT 1"))
            return True
        except Exception as e:
            logger.error(f"Async database health check failed: {e}")
            return False

    def get_connection_stats(self) -> Dict[str, Any]:
        """Get connection pool statistics."""
        pool = self.engine.pool
        return {
            "pool_size": pool.size(),
            "checked_in": pool.checkedin(),
            "checked_out": pool.checkedout(),
            "overflow": pool.overflow(),
            "invalid": pool.invalid(),
        }

    def close_all_connections(self):
        """Close all database connections."""
        if self._engine:
            self._engine.dispose()
            self._engine = None

        if self._async_engine:
            asyncio.create_task(self._async_engine.dispose())
            self._async_engine = None

        self._session_factory = None
        self._async_session_factory = None

        logger.info("All database connections closed")

    @staticmethod
    def _mask_password(url: str) -> str:
        """Mask password in database URL for logging."""
        import re
        return re.sub(r'://([^:]+):([^@]+)@', r'://\1:***@', url)

    def _validate_network_connectivity(self):
        """Validate network connectivity and environment configuration before connection attempts."""
        import socket
        import os

        # Get expected configuration based on environment
        environment = os.getenv('ENVIRONMENT', 'dev')
        expected_host = self.settings.database_host
        expected_db = self.settings.database_name

        # Validate DNS resolution first
        try:
            socket.gethostbyname(expected_host)
            logger.debug(f"✅ DNS resolution successful for {expected_host}")
        except socket.gaierror as e:
            raise DatabaseConnectionError(
                f"❌ NETWORK ERROR: Cannot resolve hostname '{expected_host}'. "
                f"Check Docker network configuration for {environment} environment. "
                f"Container may be on wrong network. Error: {e}"
            )

        # Validate expected database name matches environment
        expected_pattern = f"{environment}_db"
        if expected_db != expected_pattern:
            logger.warning(
                f"⚠️ Database name '{expected_db}' doesn't match expected pattern '{expected_pattern}' "
                f"for environment '{environment}'"
            )

        # Test socket connectivity
        try:
            sock = socket.create_connection((expected_host, self.settings.database_port), timeout=5)
            sock.close()
            logger.debug(f"✅ Socket connectivity successful to {expected_host}:{self.settings.database_port}")
        except (socket.timeout, ConnectionRefusedError, OSError) as e:
            raise DatabaseConnectionError(
                f"❌ NETWORK ERROR: Cannot connect to {expected_host}:{self.settings.database_port}. "
                f"Database server may be down or containers on different networks. "
                f"Environment: {environment}. Error: {e}"
            )


# Global connection manager instance
_connection_manager: Optional[DatabaseConnectionManager] = None


def get_connection_manager() -> DatabaseConnectionManager:
    """Get global database connection manager (singleton pattern)."""
    global _connection_manager
    if _connection_manager is None:
        _connection_manager = DatabaseConnectionManager()
    return _connection_manager


# Convenience functions
def get_engine() -> Engine:
    """Get SQLAlchemy engine."""
    return get_connection_manager().engine


def get_async_engine() -> AsyncEngine:
    """Get async SQLAlchemy engine."""
    return get_connection_manager().async_engine


@contextmanager
def get_session() -> Generator[Session, None, None]:
    """Get database session."""
    with get_connection_manager().get_session() as session:
        yield session


@asynccontextmanager
async def get_async_session() -> AsyncGenerator[AsyncSession, None]:
    """Get async database session."""
    async with get_connection_manager().get_async_session() as session:
        yield session


@contextmanager
def get_raw_connection() -> Generator[psycopg2.extensions.connection, None, None]:
    """Get raw psycopg2 connection."""
    with get_connection_manager().get_raw_connection() as conn:
        yield conn