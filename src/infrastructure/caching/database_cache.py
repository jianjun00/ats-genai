"""
Database Query Caching and Connection Optimization

Advanced database caching with query result caching, connection pooling,
and query optimization patterns.
"""

import asyncio
import hashlib
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple, Union
import logging

import asyncpg
from asyncpg import Pool

from .cache_manager import CacheBackend, MultiLayerCache, CacheConfig, cached

logger = logging.getLogger(__name__)


@dataclass
class QueryCacheEntry:
    """Database query cache entry."""
    query: str
    params: Tuple
    result: Any
    execution_time_ms: float
    cached_at: datetime
    hit_count: int = 0
    last_accessed: datetime = field(default_factory=datetime.utcnow)
    
    def is_expired(self, ttl_seconds: int) -> bool:
        """Check if cache entry is expired."""
        return datetime.utcnow() > self.cached_at + timedelta(seconds=ttl_seconds)


@dataclass
class ConnectionPoolConfig:
    """Database connection pool configuration."""
    host: str
    port: int
    database: str
    user: str
    password: str
    min_connections: int = 5
    max_connections: int = 20
    max_queries: int = 50000
    max_inactive_connection_lifetime: float = 300.0
    timeout: float = 60.0
    command_timeout: float = 30.0
    server_settings: Optional[Dict[str, str]] = None


@dataclass
class QueryMetrics:
    """Database query performance metrics."""
    total_queries: int = 0
    cached_queries: int = 0
    cache_hits: int = 0
    cache_misses: int = 0
    total_execution_time_ms: float = 0.0
    fastest_query_ms: float = float('inf')
    slowest_query_ms: float = 0.0
    average_execution_time_ms: float = 0.0
    
    @property
    def cache_hit_rate(self) -> float:
        """Calculate cache hit rate percentage."""
        if self.cached_queries == 0:
            return 0.0
        return (self.cache_hits / self.cached_queries) * 100.0
    
    def update_execution_time(self, execution_time_ms: float):
        """Update execution time metrics."""
        self.total_execution_time_ms += execution_time_ms
        self.fastest_query_ms = min(self.fastest_query_ms, execution_time_ms)
        self.slowest_query_ms = max(self.slowest_query_ms, execution_time_ms)
        self.total_queries += 1
        
        if self.total_queries > 0:
            self.average_execution_time_ms = self.total_execution_time_ms / self.total_queries


class QueryOptimizer:
    """Database query optimization and analysis."""
    
    def __init__(self):
        self.slow_query_threshold_ms = 1000
        self.slow_queries: List[Dict[str, Any]] = []
        self.query_patterns: Dict[str, int] = {}
    
    def analyze_query(self, query: str, execution_time_ms: float, params: Optional[Tuple] = None):
        """Analyze query performance and patterns."""
        # Track slow queries
        if execution_time_ms > self.slow_query_threshold_ms:
            slow_query_info = {
                'query': query,
                'params': params,
                'execution_time_ms': execution_time_ms,
                'timestamp': datetime.utcnow(),
                'query_hash': hashlib.md5(query.encode()).hexdigest()
            }
            self.slow_queries.append(slow_query_info)
            
            # Keep only recent slow queries
            cutoff_time = datetime.utcnow() - timedelta(hours=24)
            self.slow_queries = [
                q for q in self.slow_queries 
                if q['timestamp'] > cutoff_time
            ]
        
        # Track query patterns
        query_pattern = self._extract_query_pattern(query)
        self.query_patterns[query_pattern] = self.query_patterns.get(query_pattern, 0) + 1
    
    def _extract_query_pattern(self, query: str) -> str:
        """Extract query pattern by removing specific values."""
        # Simple pattern extraction - replace numbers and strings with placeholders
        import re
        
        # Normalize whitespace
        pattern = re.sub(r'\s+', ' ', query.strip().upper())
        
        # Replace string literals
        pattern = re.sub(r"'[^']*'", "'?'", pattern)
        
        # Replace numeric literals
        pattern = re.sub(r'\b\d+\b', '?', pattern)
        
        # Replace IN clauses with placeholder
        pattern = re.sub(r'IN\s*\([^)]+\)', 'IN (?)', pattern)
        
        return pattern
    
    def get_recommendations(self) -> List[Dict[str, Any]]:
        """Get query optimization recommendations."""
        recommendations = []
        
        # Analyze slow queries
        if self.slow_queries:
            avg_slow_time = sum(q['execution_time_ms'] for q in self.slow_queries) / len(self.slow_queries)
            recommendations.append({
                'type': 'slow_queries',
                'severity': 'high' if avg_slow_time > 5000 else 'medium',
                'message': f'Found {len(self.slow_queries)} slow queries (avg: {avg_slow_time:.1f}ms)',
                'details': self.slow_queries[-5:]  # Last 5 slow queries
            })
        
        # Analyze query patterns
        frequent_patterns = sorted(
            self.query_patterns.items(), 
            key=lambda x: x[1], 
            reverse=True
        )[:10]
        
        if frequent_patterns:
            recommendations.append({
                'type': 'frequent_patterns',
                'severity': 'info',
                'message': 'Most frequent query patterns',
                'details': frequent_patterns
            })
        
        return recommendations


class DatabaseCache:
    """Advanced database caching with connection pooling."""
    
    def __init__(
        self, 
        pool_config: ConnectionPoolConfig,
        cache: Optional[Union[CacheBackend, MultiLayerCache]] = None,
        cache_config: Optional[CacheConfig] = None
    ):
        self.pool_config = pool_config
        self.cache = cache
        self.cache_config = cache_config or CacheConfig(ttl_seconds=3600)
        
        self._pool: Optional[Pool] = None
        self.metrics = QueryMetrics()
        self.optimizer = QueryOptimizer()
        
        # Query cache configuration
        self.enable_query_cache = True
        self.cache_read_queries = True
        self.cache_write_queries = False
        self.max_cached_query_size = 10000  # Max query length to cache
        
        # Connection monitoring
        self._connection_stats = {
            'total_connections': 0,
            'active_connections': 0,
            'failed_connections': 0,
            'connection_errors': []
        }
    
    async def initialize(self):
        """Initialize database connection pool."""
        try:
            server_settings = self.pool_config.server_settings or {}
            
            # Add performance-oriented settings
            default_settings = {
                'application_name': 'ats-service',
                'tcp_keepalives_idle': '600',
                'tcp_keepalives_interval': '30',
                'tcp_keepalives_count': '3',
            }
            server_settings = {**default_settings, **server_settings}
            
            self._pool = await asyncpg.create_pool(
                host=self.pool_config.host,
                port=self.pool_config.port,
                database=self.pool_config.database,
                user=self.pool_config.user,
                password=self.pool_config.password,
                min_size=self.pool_config.min_connections,
                max_size=self.pool_config.max_connections,
                max_queries=self.pool_config.max_queries,
                max_inactive_connection_lifetime=self.pool_config.max_inactive_connection_lifetime,
                timeout=self.pool_config.timeout,
                command_timeout=self.pool_config.command_timeout,
                server_settings=server_settings
            )
            
            logger.info(f"Database pool initialized: {self.pool_config.min_connections}-{self.pool_config.max_connections} connections")
            
        except Exception as e:
            logger.error(f"Failed to initialize database pool: {e}")
            self._connection_stats['failed_connections'] += 1
            self._connection_stats['connection_errors'].append({
                'error': str(e),
                'timestamp': datetime.utcnow()
            })
            raise
    
    async def close(self):
        """Close database connection pool."""
        if self._pool:
            await self._pool.close()
            logger.info("Database pool closed")
    
    @asynccontextmanager
    async def get_connection(self):
        """Get database connection from pool."""
        if not self._pool:
            await self.initialize()
        
        connection = None
        try:
            self._connection_stats['total_connections'] += 1
            self._connection_stats['active_connections'] += 1
            
            connection = await self._pool.acquire()
            yield connection
            
        except Exception as e:
            logger.error(f"Database connection error: {e}")
            self._connection_stats['failed_connections'] += 1
            self._connection_stats['connection_errors'].append({
                'error': str(e),
                'timestamp': datetime.utcnow()
            })
            raise
        finally:
            if connection:
                await self._pool.release(connection)
                self._connection_stats['active_connections'] -= 1
    
    async def execute_query(
        self, 
        query: str, 
        params: Optional[Tuple] = None,
        cache_ttl: Optional[int] = None,
        force_cache_refresh: bool = False
    ) -> Any:
        """Execute query with caching support."""
        params = params or ()
        cache_ttl = cache_ttl or self.cache_config.ttl_seconds
        
        # Determine if query should be cached
        should_cache = (
            self.enable_query_cache and 
            self.cache and
            len(query) <= self.max_cached_query_size and
            (self.cache_read_queries and self._is_read_query(query) or
             self.cache_write_queries and not self._is_read_query(query))
        )
        
        # Try cache first if enabled
        cached_result = None
        if should_cache and not force_cache_refresh:
            cache_key = self._generate_cache_key(query, params)
            cached_result = await self.cache.get(cache_key)
            
            if cached_result is not None:
                self.metrics.cache_hits += 1
                self.metrics.cached_queries += 1
                logger.debug(f"Cache hit for query: {query[:100]}...")
                return cached_result
            else:
                self.metrics.cache_misses += 1
                self.metrics.cached_queries += 1
        
        # Execute query
        start_time = time.time()
        try:
            async with self.get_connection() as conn:
                if params:
                    result = await conn.fetch(query, *params)
                else:
                    result = await conn.fetch(query)
                
                # Convert to serializable format
                serializable_result = [dict(record) for record in result]
                
                execution_time_ms = (time.time() - start_time) * 1000
                
                # Update metrics and analyze query
                self.metrics.update_execution_time(execution_time_ms)
                self.optimizer.analyze_query(query, execution_time_ms, params)
                
                # Cache result if enabled
                if should_cache:
                    cache_key = self._generate_cache_key(query, params)
                    await self.cache.set(cache_key, serializable_result, cache_ttl)
                    logger.debug(f"Cached query result: {query[:100]}...")
                
                logger.debug(f"Query executed in {execution_time_ms:.2f}ms: {query[:100]}...")
                return serializable_result
                
        except Exception as e:
            execution_time_ms = (time.time() - start_time) * 1000
            logger.error(f"Query execution failed ({execution_time_ms:.2f}ms): {query[:100]}... - {e}")
            raise
    
    async def execute_transaction(self, queries: List[Tuple[str, Optional[Tuple]]]) -> List[Any]:
        """Execute multiple queries in a transaction."""
        results = []
        start_time = time.time()
        
        async with self.get_connection() as conn:
            async with conn.transaction():
                for query, params in queries:
                    if params:
                        result = await conn.fetch(query, *params)
                    else:
                        result = await conn.fetch(query)
                    
                    results.append([dict(record) for record in result])
        
        execution_time_ms = (time.time() - start_time) * 1000
        self.metrics.update_execution_time(execution_time_ms)
        
        logger.debug(f"Transaction executed in {execution_time_ms:.2f}ms with {len(queries)} queries")
        return results
    
    def _generate_cache_key(self, query: str, params: Tuple) -> str:
        """Generate cache key for query and parameters."""
        key_data = f"{query}:{str(params)}"
        return hashlib.md5(key_data.encode()).hexdigest()
    
    def _is_read_query(self, query: str) -> bool:
        """Check if query is a read operation."""
        query_upper = query.strip().upper()
        read_keywords = ['SELECT', 'WITH']
        write_keywords = ['INSERT', 'UPDATE', 'DELETE', 'CREATE', 'DROP', 'ALTER', 'TRUNCATE']
        
        for keyword in read_keywords:
            if query_upper.startswith(keyword):
                return True
        
        for keyword in write_keywords:
            if query_upper.startswith(keyword):
                return False
        
        # Default to read for safety
        return True
    
    async def invalidate_cache_pattern(self, pattern: str) -> int:
        """Invalidate cached queries matching pattern."""
        if self.cache:
            return await self.cache.clear(pattern)
        return 0
    
    async def get_pool_stats(self) -> Dict[str, Any]:
        """Get connection pool statistics."""
        if not self._pool:
            return {'status': 'not_initialized'}
        
        return {
            'pool_size': len(self._pool._holders),
            'available_connections': len([h for h in self._pool._holders if h.is_available()]),
            'total_connections': self._connection_stats['total_connections'],
            'active_connections': self._connection_stats['active_connections'],
            'failed_connections': self._connection_stats['failed_connections'],
            'recent_errors': self._connection_stats['connection_errors'][-5:],
            'max_connections': self.pool_config.max_connections,
            'min_connections': self.pool_config.min_connections
        }
    
    async def get_query_metrics(self) -> Dict[str, Any]:
        """Get comprehensive query metrics."""
        optimizer_recommendations = self.optimizer.get_recommendations()
        
        return {
            'execution_metrics': {
                'total_queries': self.metrics.total_queries,
                'average_execution_time_ms': self.metrics.average_execution_time_ms,
                'fastest_query_ms': self.metrics.fastest_query_ms if self.metrics.fastest_query_ms != float('inf') else 0,
                'slowest_query_ms': self.metrics.slowest_query_ms,
                'total_execution_time_ms': self.metrics.total_execution_time_ms
            },
            'cache_metrics': {
                'cached_queries': self.metrics.cached_queries,
                'cache_hits': self.metrics.cache_hits,
                'cache_misses': self.metrics.cache_misses,
                'cache_hit_rate': self.metrics.cache_hit_rate
            },
            'optimization': {
                'recommendations': optimizer_recommendations,
                'slow_queries_count': len(self.optimizer.slow_queries),
                'unique_query_patterns': len(self.optimizer.query_patterns)
            }
        }
    
    async def health_check(self) -> Dict[str, Any]:
        """Perform database health check."""
        try:
            start_time = time.time()
            
            async with self.get_connection() as conn:
                await conn.fetchval("SELECT 1")
            
            response_time_ms = (time.time() - start_time) * 1000
            
            pool_stats = await self.get_pool_stats()
            
            # Determine health status
            if response_time_ms < 100 and pool_stats['available_connections'] > 0:
                status = 'healthy'
            elif response_time_ms < 1000 and pool_stats['available_connections'] > 0:
                status = 'degraded'
            else:
                status = 'unhealthy'
            
            return {
                'status': status,
                'response_time_ms': response_time_ms,
                'pool_stats': pool_stats,
                'cache_enabled': self.enable_query_cache and self.cache is not None
            }
            
        except Exception as e:
            return {
                'status': 'unhealthy',
                'error': str(e),
                'pool_stats': await self.get_pool_stats()
            }


# Query result caching decorators
def cached_query(
    ttl: int = 3600, 
    cache_name: str = "default", 
    invalidation_tags: Optional[List[str]] = None
):
    """Decorator for caching database query results."""
    def decorator(func):
        @cached(ttl=ttl, cache_name=cache_name)
        async def wrapper(*args, **kwargs):
            return await func(*args, **kwargs)
        return wrapper
    return decorator


class QueryBuilder:
    """Dynamic query builder with caching awareness."""
    
    def __init__(self, db_cache: DatabaseCache):
        self.db_cache = db_cache
        self._query_parts = []
        self._params = []
        self._cache_tags = []
    
    def select(self, columns: Union[str, List[str]]) -> 'QueryBuilder':
        """Add SELECT clause."""
        if isinstance(columns, list):
            columns = ', '.join(columns)
        self._query_parts.append(f"SELECT {columns}")
        return self
    
    def from_table(self, table: str) -> 'QueryBuilder':
        """Add FROM clause."""
        self._query_parts.append(f"FROM {table}")
        self._cache_tags.append(f"table:{table}")
        return self
    
    def where(self, condition: str, *params) -> 'QueryBuilder':
        """Add WHERE clause."""
        if self._query_parts and "WHERE" not in ' '.join(self._query_parts):
            self._query_parts.append(f"WHERE {condition}")
        else:
            self._query_parts.append(f"AND {condition}")
        
        self._params.extend(params)
        return self
    
    def order_by(self, column: str, direction: str = "ASC") -> 'QueryBuilder':
        """Add ORDER BY clause."""
        self._query_parts.append(f"ORDER BY {column} {direction}")
        return self
    
    def limit(self, count: int) -> 'QueryBuilder':
        """Add LIMIT clause."""
        self._query_parts.append(f"LIMIT {count}")
        return self
    
    async def execute(self, cache_ttl: Optional[int] = None) -> List[Dict[str, Any]]:
        """Execute the built query."""
        query = ' '.join(self._query_parts)
        return await self.db_cache.execute_query(
            query, 
            tuple(self._params) if self._params else None,
            cache_ttl=cache_ttl
        )
    
    def build(self) -> Tuple[str, Tuple]:
        """Build and return query string and parameters."""
        query = ' '.join(self._query_parts)
        return query, tuple(self._params) if self._params else ()


# Global database cache instances
_db_cache_instances: Dict[str, DatabaseCache] = {}


def get_db_cache(name: str = "default") -> Optional[DatabaseCache]:
    """Get named database cache instance."""
    return _db_cache_instances.get(name)


def register_db_cache(name: str, db_cache: DatabaseCache):
    """Register named database cache instance."""
    _db_cache_instances[name] = db_cache