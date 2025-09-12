"""
Caching Infrastructure Module

Comprehensive caching solution with multi-layer caching, intelligent invalidation,
and performance optimization patterns.
"""

from .cache_manager import (
    # Core caching components
    CacheLevel,
    EvictionPolicy,
    CacheConfig,
    CacheMetrics,
    CacheEntry,
    CacheBackend,
    
    # Cache implementations
    MemoryCache,
    RedisCache,
    MultiLayerCache,
    
    # Cache utilities
    CacheDecorator,
    CacheInvalidationManager,
    cached,
    cache_invalidate_tag,
    
    # Global cache management
    get_cache,
    register_cache
)

from .database_cache import (
    # Database caching
    DatabaseCache,
    QueryCacheEntry,
    ConnectionPoolConfig,
    QueryMetrics,
    QueryOptimizer,
    QueryBuilder,
    
    # Database cache utilities
    cached_query,
    get_db_cache,
    register_db_cache
)

from .api_cache import (
    # API response caching
    APICacheManager,
    APICacheMiddleware,
    CacheStrategy,
    CacheHeaders,
    CachedResponse,
    ResponseCacheConfig,
    SmartCacheInvalidator,
    
    # API cache decorators
    cache_response,
    
    # Global API cache management
    get_api_cache_manager,
    register_api_cache_manager
)

__all__ = [
    # Core caching
    'CacheLevel',
    'EvictionPolicy', 
    'CacheConfig',
    'CacheMetrics',
    'CacheEntry',
    'CacheBackend',
    'MemoryCache',
    'RedisCache',
    'MultiLayerCache',
    'CacheDecorator',
    'CacheInvalidationManager',
    'cached',
    'cache_invalidate_tag',
    'get_cache',
    'register_cache',
    
    # Database caching
    'DatabaseCache',
    'QueryCacheEntry',
    'ConnectionPoolConfig',
    'QueryMetrics',
    'QueryOptimizer',
    'QueryBuilder',
    'cached_query',
    'get_db_cache',
    'register_db_cache',
    
    # API caching
    'APICacheManager',
    'APICacheMiddleware',
    'CacheStrategy',
    'CacheHeaders',
    'CachedResponse',
    'ResponseCacheConfig',
    'SmartCacheInvalidator',
    'cache_response',
    'get_api_cache_manager',
    'register_api_cache_manager'
]