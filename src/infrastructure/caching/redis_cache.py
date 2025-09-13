"""
Redis-based caching infrastructure for ATS platform.

Provides high-performance caching with TTL, invalidation patterns,
and distributed cache coherence for service layer.
"""

import redis
import json
import logging
import asyncio
import pickle
from typing import Any, Dict, List, Optional, Union, Callable, TypeVar
from datetime import datetime, timedelta
from dataclasses import asdict
import hashlib
from contextlib import asynccontextmanager

from core.platform.config.environment import Environment

logger = logging.getLogger(__name__)

T = TypeVar('T')


class CacheKeyBuilder:
    """Smart cache key generation with collision avoidance"""
    
    @staticmethod
    def instrument_by_id(instrument_id: int) -> str:
        return f"instrument:id:{instrument_id}"
    
    @staticmethod
    def instrument_by_symbol(symbol: str, vendor: str = "ticker") -> str:
        return f"instrument:symbol:{vendor}:{symbol.upper()}"
    
    @staticmethod
    def instruments_list(criteria_hash: str) -> str:
        return f"instruments:list:{criteria_hash}"
    
    @staticmethod
    def instrument_count() -> str:
        return "instruments:count"
    
    @staticmethod
    def vendor_id(vendor_name: str) -> str:
        return f"vendor:id:{vendor_name}"
    
    @staticmethod
    def cross_references(instrument_id: int) -> str:
        return f"xrefs:instrument:{instrument_id}"
    
    @staticmethod
    def hash_criteria(criteria: Dict[str, Any]) -> str:
        """Create stable hash for complex search criteria"""
        # Sort keys and create deterministic string
        sorted_items = sorted(criteria.items())
        criteria_str = json.dumps(sorted_items, sort_keys=True, default=str)
        return hashlib.md5(criteria_str.encode()).hexdigest()[:16]


class CacheStats:
    """Cache performance metrics"""
    
    def __init__(self):
        self.hits = 0
        self.misses = 0
        self.sets = 0
        self.deletes = 0
        self.errors = 0
    
    @property
    def hit_rate(self) -> float:
        total = self.hits + self.misses
        return self.hits / total if total > 0 else 0.0
    
    def reset(self):
        """Reset all counters"""
        self.hits = self.misses = self.sets = self.deletes = self.errors = 0
    
    def to_dict(self) -> Dict[str, Union[int, float]]:
        return {
            'hits': self.hits,
            'misses': self.misses,
            'sets': self.sets,
            'deletes': self.deletes,
            'errors': self.errors,
            'hit_rate': self.hit_rate
        }


class RedisCache:
    """
    High-performance Redis cache with async support, TTL management,
    and intelligent invalidation patterns.
    """
    
    def __init__(self, environment: Environment, 
                 redis_url: str = None,
                 default_ttl: int = 3600,  # 1 hour
                 key_prefix: str = "ats"):
        self.env = environment
        self.redis_url = redis_url or self._get_redis_url()
        self.default_ttl = default_ttl
        self.key_prefix = key_prefix
        self.stats = CacheStats()
        self._redis_client = None
        self._connection_pool = None
    
    def _get_redis_url(self) -> str:
        """Get Redis URL from environment configuration"""
        # Try environment variables first
        import os
        redis_host = os.getenv('REDIS_HOST', 'localhost')
        redis_port = os.getenv('REDIS_PORT', '6379')
        redis_db = os.getenv('REDIS_DB', '0')
        redis_password = os.getenv('REDIS_PASSWORD', '')
        
        # Build Redis URL
        auth = f":{redis_password}@" if redis_password else ""
        return f"redis://{auth}{redis_host}:{redis_port}/{redis_db}"
    
    def _get_client(self) -> redis.Redis:
        """Get Redis client with connection pooling"""
        if self._redis_client is None:
            try:
                self._connection_pool = redis.ConnectionPool.from_url(
                    self.redis_url,
                    max_connections=20,
                    socket_connect_timeout=5,
                    socket_timeout=5,
                    retry_on_timeout=True
                )
                self._redis_client = redis.Redis(
                    connection_pool=self._connection_pool,
                    decode_responses=False  # Handle encoding manually
                )
                # Test connection
                self._redis_client.ping()
                logger.info(f"Redis cache initialized: {self.redis_url}")
            except Exception as e:
                logger.warning(f"Redis unavailable, using in-memory fallback: {e}")
                self._redis_client = self._create_fallback_cache()
        
        return self._redis_client
    
    def _create_fallback_cache(self) -> 'InMemoryCache':
        """Create in-memory fallback when Redis is unavailable"""
        return InMemoryCache(max_size=10000, default_ttl=self.default_ttl)
    
    def _make_key(self, key: str) -> str:
        """Create prefixed cache key"""
        env_prefix = self.env.env_type.value
        return f"{self.key_prefix}:{env_prefix}:{key}"
    
    def _serialize(self, value: Any) -> bytes:
        """Serialize value for Redis storage"""
        try:
            # Handle dataclasses and DTOs
            if hasattr(value, '__dict__'):
                if hasattr(value, '__dataclass_fields__'):  # dataclass
                    value = asdict(value)
                else:  # DTO or regular object
                    value = value.__dict__
            
            # Use pickle for complex objects, JSON for simple ones
            if isinstance(value, (dict, list, str, int, float, bool, type(None))):
                return json.dumps(value, default=str).encode('utf-8')
            else:
                return pickle.dumps(value)
        except Exception as e:
            logger.error(f"Serialization error: {e}")
            return pickle.dumps(value)  # Fallback to pickle
    
    def _deserialize(self, data: bytes) -> Any:
        """Deserialize value from Redis"""
        try:
            # Try JSON first (more readable)
            try:
                return json.loads(data.decode('utf-8'))
            except (json.JSONDecodeError, UnicodeDecodeError):
                # Fallback to pickle
                return pickle.loads(data)
        except Exception as e:
            logger.error(f"Deserialization error: {e}")
            return None
    
    async def get(self, key: str) -> Optional[Any]:
        """Get value from cache"""
        try:
            client = self._get_client()
            full_key = self._make_key(key)
            
            if hasattr(client, 'get'):  # Redis client
                data = client.get(full_key)
            else:  # Fallback cache
                data = await client.get(key)
            
            if data is not None:
                self.stats.hits += 1
                if isinstance(data, bytes):
                    return self._deserialize(data)
                return data
            else:
                self.stats.misses += 1
                return None
                
        except Exception as e:
            self.stats.errors += 1
            logger.error(f"Cache get error for key {key}: {e}")
            return None
    
    async def set(self, key: str, value: Any, ttl: int = None) -> bool:
        """Set value in cache with TTL"""
        try:
            client = self._get_client()
            full_key = self._make_key(key)
            ttl = ttl or self.default_ttl
            
            if hasattr(client, 'setex'):  # Redis client
                serialized = self._serialize(value)
                result = client.setex(full_key, ttl, serialized)
            else:  # Fallback cache
                result = await client.set(key, value, ttl)
            
            self.stats.sets += 1
            return bool(result)
            
        except Exception as e:
            self.stats.errors += 1
            logger.error(f"Cache set error for key {key}: {e}")
            return False
    
    async def delete(self, key: str) -> bool:
        """Delete specific key"""
        try:
            client = self._get_client()
            full_key = self._make_key(key)
            
            if hasattr(client, 'delete'):  # Redis client
                result = client.delete(full_key)
            else:  # Fallback cache
                result = await client.delete(key)
            
            self.stats.deletes += 1
            return bool(result)
            
        except Exception as e:
            self.stats.errors += 1
            logger.error(f"Cache delete error for key {key}: {e}")
            return False
    
    async def delete_pattern(self, pattern: str) -> int:
        """Delete all keys matching pattern"""
        try:
            client = self._get_client()
            full_pattern = self._make_key(pattern)
            
            if hasattr(client, 'keys'):  # Redis client
                keys = client.keys(full_pattern)
                if keys:
                    deleted = client.delete(*keys)
                    self.stats.deletes += deleted
                    return deleted
            else:  # Fallback cache
                return await client.delete_pattern(pattern)
            
            return 0
            
        except Exception as e:
            self.stats.errors += 1
            logger.error(f"Cache delete pattern error for {pattern}: {e}")
            return 0
    
    async def invalidate_instrument(self, instrument_id: int, symbol: str = None):
        """Invalidate all cache entries related to an instrument"""
        patterns = [
            f"instrument:id:{instrument_id}",
            f"xrefs:instrument:{instrument_id}",
            "instruments:list:*",  # Invalidate all list queries
            "instruments:count"    # Invalidate count
        ]
        
        if symbol:
            patterns.extend([
                f"instrument:symbol:*:{symbol.upper()}",
            ])
        
        for pattern in patterns:
            await self.delete_pattern(pattern)
    
    async def invalidate_vendor(self, vendor_name: str):
        """Invalidate vendor-related cache entries"""
        patterns = [
            f"vendor:id:{vendor_name}",
            f"instrument:symbol:{vendor_name}:*"
        ]
        
        for pattern in patterns:
            await self.delete_pattern(pattern)
    
    def get_stats(self) -> Dict[str, Union[int, float]]:
        """Get cache performance statistics"""
        return self.stats.to_dict()
    
    async def health_check(self) -> Dict[str, Any]:
        """Check cache health and connectivity"""
        try:
            client = self._get_client()
            
            # Test basic operations
            test_key = self._make_key("health_check")
            test_value = {"timestamp": datetime.now().isoformat()}
            
            # Test set
            await self.set("health_check", test_value, 60)
            
            # Test get
            retrieved = await self.get("health_check")
            
            # Test delete
            await self.delete("health_check")
            
            return {
                "status": "healthy",
                "redis_url": self.redis_url.split('@')[-1],  # Hide password
                "stats": self.get_stats(),
                "test_passed": retrieved is not None
            }
            
        except Exception as e:
            return {
                "status": "unhealthy",
                "error": str(e),
                "stats": self.get_stats()
            }
    
    async def close(self):
        """Close Redis connections"""
        try:
            if self._connection_pool:
                self._connection_pool.disconnect()
            if self._redis_client:
                if hasattr(self._redis_client, 'close'):
                    self._redis_client.close()
        except Exception as e:
            logger.error(f"Error closing Redis cache: {e}")


class InMemoryCache:
    """Fallback in-memory cache when Redis is unavailable"""
    
    def __init__(self, max_size: int = 10000, default_ttl: int = 3600):
        self.max_size = max_size
        self.default_ttl = default_ttl
        self._cache = {}
        self._expiry = {}
    
    def _is_expired(self, key: str) -> bool:
        if key in self._expiry:
            return datetime.now() > self._expiry[key]
        return False
    
    def _cleanup_expired(self):
        """Remove expired entries"""
        now = datetime.now()
        expired_keys = [k for k, exp_time in self._expiry.items() if now > exp_time]
        for key in expired_keys:
            self._cache.pop(key, None)
            self._expiry.pop(key, None)
    
    async def get(self, key: str) -> Optional[Any]:
        self._cleanup_expired()
        if key in self._cache and not self._is_expired(key):
            return self._cache[key]
        return None
    
    async def set(self, key: str, value: Any, ttl: int = None) -> bool:
        ttl = ttl or self.default_ttl
        
        # LRU eviction if at capacity
        if len(self._cache) >= self.max_size:
            # Remove oldest entry
            oldest_key = next(iter(self._cache))
            self._cache.pop(oldest_key, None)
            self._expiry.pop(oldest_key, None)
        
        self._cache[key] = value
        self._expiry[key] = datetime.now() + timedelta(seconds=ttl)
        return True
    
    async def delete(self, key: str) -> bool:
        existed = key in self._cache
        self._cache.pop(key, None)
        self._expiry.pop(key, None)
        return existed
    
    async def delete_pattern(self, pattern: str) -> int:
        import fnmatch
        matching_keys = [k for k in self._cache.keys() if fnmatch.fnmatch(k, pattern)]
        for key in matching_keys:
            await self.delete(key)
        return len(matching_keys)


def cache_result(ttl: int = 3600, key_func: Callable = None):
    """
    Decorator for caching function results.
    
    Args:
        ttl: Time to live in seconds
        key_func: Function to generate cache key from arguments
    """
    def decorator(func):
        async def wrapper(self, *args, **kwargs):
            # Generate cache key
            if key_func:
                cache_key = key_func(*args, **kwargs)
            else:
                # Default key generation
                args_str = "_".join(str(arg) for arg in args)
                kwargs_str = "_".join(f"{k}:{v}" for k, v in kwargs.items())
                cache_key = f"{func.__name__}:{args_str}:{kwargs_str}"
            
            # Try to get from cache
            if hasattr(self, 'cache'):
                cached_result = await self.cache.get(cache_key)
                if cached_result is not None:
                    return cached_result
            
            # Execute function
            result = await func(self, *args, **kwargs)
            
            # Cache result
            if hasattr(self, 'cache') and result is not None:
                await self.cache.set(cache_key, result, ttl)
            
            return result
        
        return wrapper
    return decorator