"""
Multi-Layer Cache Manager

Comprehensive caching infrastructure supporting Redis, in-memory caching,
and intelligent cache invalidation strategies.
"""

import asyncio
import hashlib
import json
import pickle
import time
import zlib
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Dict, List, Optional, Union, Callable, Tuple
import logging

import redis.asyncio as redis
from cachetools import TTLCache, LRUCache, LFUCache

logger = logging.getLogger(__name__)


class CacheLevel(Enum):
    """Cache level enumeration."""
    L1_MEMORY = "l1_memory"      # In-memory cache (fastest)
    L2_REDIS = "l2_redis"        # Redis cache (shared across services)
    L3_DATABASE = "l3_database"  # Database cache (slowest but persistent)


class EvictionPolicy(Enum):
    """Cache eviction policy enumeration."""
    LRU = "lru"  # Least Recently Used
    LFU = "lfu"  # Least Frequently Used
    TTL = "ttl"  # Time To Live
    FIFO = "fifo"  # First In First Out


@dataclass
class CacheConfig:
    """Configuration for cache behavior."""
    ttl_seconds: int = 3600  # 1 hour default
    max_size: int = 1000
    eviction_policy: EvictionPolicy = EvictionPolicy.LRU
    compression_enabled: bool = True
    compression_threshold: int = 1024  # Compress if data > 1KB
    serialization_method: str = "json"  # json, pickle, msgpack
    key_prefix: str = ""
    namespace: str = "default"


@dataclass
class CacheMetrics:
    """Cache performance metrics."""
    hits: int = 0
    misses: int = 0
    evictions: int = 0
    total_requests: int = 0
    total_size_bytes: int = 0
    last_updated: datetime = field(default_factory=datetime.utcnow)

    @property
    def hit_rate(self) -> float:
        """Calculate hit rate percentage."""
        if self.total_requests == 0:
            return 0.0
        return (self.hits / self.total_requests) * 100.0

    @property
    def miss_rate(self) -> float:
        """Calculate miss rate percentage."""
        return 100.0 - self.hit_rate


@dataclass
class CacheEntry:
    """Cache entry with metadata."""
    key: str
    value: Any
    created_at: datetime
    expires_at: Optional[datetime] = None
    access_count: int = 0
    last_accessed: datetime = field(default_factory=datetime.utcnow)
    size_bytes: int = 0
    compressed: bool = False

    def is_expired(self) -> bool:
        """Check if cache entry is expired."""
        if self.expires_at is None:
            return False
        return datetime.utcnow() > self.expires_at

    def touch(self):
        """Update last accessed time and increment access count."""
        self.last_accessed = datetime.utcnow()
        self.access_count += 1


class CacheBackend(ABC):
    """Abstract cache backend interface."""

    def __init__(self, config: CacheConfig):
        self.config = config
        self.metrics = CacheMetrics()

    @abstractmethod
    async def get(self, key: str) -> Optional[Any]:
        """Get value from cache."""
        pass

    @abstractmethod
    async def set(self, key: str, value: Any, ttl: Optional[int] = None) -> bool:
        """Set value in cache."""
        pass

    @abstractmethod
    async def delete(self, key: str) -> bool:
        """Delete value from cache."""
        pass

    @abstractmethod
    async def exists(self, key: str) -> bool:
        """Check if key exists in cache."""
        pass

    @abstractmethod
    async def clear(self, pattern: Optional[str] = None) -> int:
        """Clear cache entries matching pattern."""
        pass

    @abstractmethod
    async def get_metrics(self) -> CacheMetrics:
        """Get cache metrics."""
        pass

    def _generate_key(self, key: str) -> str:
        """Generate full cache key with prefix and namespace."""
        parts = []
        if self.config.namespace:
            parts.append(self.config.namespace)
        if self.config.key_prefix:
            parts.append(self.config.key_prefix)
        parts.append(key)
        return ":".join(parts)

    def _serialize(self, value: Any) -> bytes:
        """Serialize value for storage."""
        if self.config.serialization_method == "json":
            data = json.dumps(value, default=str).encode('utf-8')
        elif self.config.serialization_method == "pickle":
            data = pickle.dumps(value)
        else:
            raise ValueError(f"Unsupported serialization method: {self.config.serialization_method}")

        # Compress if enabled and data exceeds threshold
        if (self.config.compression_enabled and
            len(data) > self.config.compression_threshold):
            data = zlib.compress(data)
            return data, True

        return data, False

    def _deserialize(self, data: bytes, compressed: bool = False) -> Any:
        """Deserialize value from storage."""
        if compressed:
            data = zlib.decompress(data)

        if self.config.serialization_method == "json":
            return json.loads(data.decode('utf-8'))
        elif self.config.serialization_method == "pickle":
            return pickle.loads(data)
        else:
            raise ValueError(f"Unsupported serialization method: {self.config.serialization_method}")


class MemoryCache(CacheBackend):
    """In-memory cache backend using cachetools."""

    def __init__(self, config: CacheConfig):
        super().__init__(config)

        # Create appropriate cache based on eviction policy
        if config.eviction_policy == EvictionPolicy.LRU:
            self._cache = LRUCache(maxsize=config.max_size)
        elif config.eviction_policy == EvictionPolicy.LFU:
            self._cache = LFUCache(maxsize=config.max_size)
        elif config.eviction_policy == EvictionPolicy.TTL:
            self._cache = TTLCache(maxsize=config.max_size, ttl=config.ttl_seconds)
        else:
            self._cache = LRUCache(maxsize=config.max_size)  # Default to LRU

        self._lock = asyncio.Lock()

    async def get(self, key: str) -> Optional[Any]:
        """Get value from memory cache."""
        full_key = self._generate_key(key)

        async with self._lock:
            try:
                entry = self._cache[full_key]
                if isinstance(entry, CacheEntry):
                    if entry.is_expired():
                        del self._cache[full_key]
                        self.metrics.misses += 1
                        return None

                    entry.touch()
                    self.metrics.hits += 1
                    return entry.value
                else:
                    # Legacy entry without metadata
                    self.metrics.hits += 1
                    return entry

            except KeyError:
                self.metrics.misses += 1
                return None
            finally:
                self.metrics.total_requests += 1

    async def set(self, key: str, value: Any, ttl: Optional[int] = None) -> bool:
        """Set value in memory cache."""
        full_key = self._generate_key(key)
        ttl = ttl or self.config.ttl_seconds

        async with self._lock:
            try:
                # Create cache entry with metadata
                entry = CacheEntry(
                    key=full_key,
                    value=value,
                    created_at=datetime.utcnow(),
                    expires_at=datetime.utcnow() + timedelta(seconds=ttl) if ttl > 0 else None,
                    size_bytes=len(str(value))  # Rough size estimate
                )

                self._cache[full_key] = entry
                return True

            except Exception as e:
                logger.error(f"Error setting memory cache key {full_key}: {e}")
                return False

    async def delete(self, key: str) -> bool:
        """Delete value from memory cache."""
        full_key = self._generate_key(key)

        async with self._lock:
            try:
                del self._cache[full_key]
                return True
            except KeyError:
                return False

    async def exists(self, key: str) -> bool:
        """Check if key exists in memory cache."""
        full_key = self._generate_key(key)

        async with self._lock:
            return full_key in self._cache

    async def clear(self, pattern: Optional[str] = None) -> int:
        """Clear memory cache entries."""
        async with self._lock:
            if pattern is None:
                count = len(self._cache)
                self._cache.clear()
                return count
            else:
                # Pattern-based clearing
                keys_to_delete = [k for k in self._cache.keys() if pattern in k]
                for key in keys_to_delete:
                    del self._cache[key]
                return len(keys_to_delete)

    async def get_metrics(self) -> CacheMetrics:
        """Get memory cache metrics."""
        async with self._lock:
            self.metrics.total_size_bytes = sum(
                entry.size_bytes for entry in self._cache.values()
                if isinstance(entry, CacheEntry)
            )
            self.metrics.last_updated = datetime.utcnow()
            return self.metrics


class RedisCache(CacheBackend):
    """Redis cache backend."""

    def __init__(self, config: CacheConfig, redis_url: str = "redis://localhost:6379"):
        super().__init__(config)
        self.redis_url = redis_url
        self._redis: Optional[redis.Redis] = None

    async def _get_redis(self) -> redis.Redis:
        """Get Redis connection."""
        if self._redis is None:
            self._redis = redis.from_url(self.redis_url, decode_responses=False)
        return self._redis

    async def get(self, key: str) -> Optional[Any]:
        """Get value from Redis cache."""
        full_key = self._generate_key(key)

        try:
            r = await self._get_redis()
            data = await r.get(full_key)

            if data is None:
                self.metrics.misses += 1
                return None

            # Deserialize data
            try:
                # Check if data has compression flag
                if data.startswith(b'compressed:'):
                    compressed_data = data[11:]  # Remove 'compressed:' prefix
                    value = self._deserialize(compressed_data, compressed=True)
                else:
                    value = self._deserialize(data, compressed=False)

                self.metrics.hits += 1
                return value

            except Exception as e:
                logger.error(f"Error deserializing Redis data for key {full_key}: {e}")
                self.metrics.misses += 1
                return None

        except Exception as e:
            logger.error(f"Redis get error for key {full_key}: {e}")
            self.metrics.misses += 1
            return None
        finally:
            self.metrics.total_requests += 1

    async def set(self, key: str, value: Any, ttl: Optional[int] = None) -> bool:
        """Set value in Redis cache."""
        full_key = self._generate_key(key)
        ttl = ttl or self.config.ttl_seconds

        try:
            r = await self._get_redis()

            # Serialize and potentially compress data
            data, compressed = self._serialize(value)

            if compressed:
                data = b'compressed:' + data

            # Set with TTL
            if ttl > 0:
                await r.setex(full_key, ttl, data)
            else:
                await r.set(full_key, data)

            return True

        except Exception as e:
            logger.error(f"Redis set error for key {full_key}: {e}")
            return False

    async def delete(self, key: str) -> bool:
        """Delete value from Redis cache."""
        full_key = self._generate_key(key)

        try:
            r = await self._get_redis()
            result = await r.delete(full_key)
            return result > 0

        except Exception as e:
            logger.error(f"Redis delete error for key {full_key}: {e}")
            return False

    async def exists(self, key: str) -> bool:
        """Check if key exists in Redis cache."""
        full_key = self._generate_key(key)

        try:
            r = await self._get_redis()
            result = await r.exists(full_key)
            return result > 0

        except Exception as e:
            logger.error(f"Redis exists error for key {full_key}: {e}")
            return False

    async def clear(self, pattern: Optional[str] = None) -> int:
        """Clear Redis cache entries."""
        try:
            r = await self._get_redis()

            if pattern is None:
                # Clear all keys with our namespace
                pattern = f"{self.config.namespace}:*"

            # Use SCAN for safe deletion
            count = 0
            async for key in r.scan_iter(match=pattern):
                await r.delete(key)
                count += 1

            return count

        except Exception as e:
            logger.error(f"Redis clear error: {e}")
            return 0

    async def get_metrics(self) -> CacheMetrics:
        """Get Redis cache metrics."""
        try:
            r = await self._get_redis()
            info = await r.info('memory')

            self.metrics.total_size_bytes = info.get('used_memory', 0)
            self.metrics.last_updated = datetime.utcnow()

        except Exception as e:
            logger.error(f"Error getting Redis metrics: {e}")

        return self.metrics

    async def close(self):
        """Close Redis connection."""
        if self._redis:
            await self._redis.close()


class MultiLayerCache:
    """Multi-layer cache with L1 (memory) and L2 (Redis) support."""

    def __init__(
        self,
        l1_config: CacheConfig,
        l2_config: Optional[CacheConfig] = None,
        redis_url: str = "redis://localhost:6379"
    ):
        self.l1_cache = MemoryCache(l1_config)
        self.l2_cache = RedisCache(l2_config or l1_config, redis_url) if l2_config else None
        self.write_through = True  # Write to all layers simultaneously
        self.read_repair = True   # Populate upper layers on cache miss

    async def get(self, key: str) -> Optional[Any]:
        """Get value with multi-layer fallback."""
        # Try L1 cache first (fastest)
        value = await self.l1_cache.get(key)
        if value is not None:
            return value

        # Try L2 cache (Redis)
        if self.l2_cache:
            value = await self.l2_cache.get(key)
            if value is not None:
                # Read repair: populate L1 cache
                if self.read_repair:
                    await self.l1_cache.set(key, value)
                return value

        return None

    async def set(self, key: str, value: Any, ttl: Optional[int] = None) -> bool:
        """Set value in multi-layer cache."""
        results = []

        # Write to L1 cache
        results.append(await self.l1_cache.set(key, value, ttl))

        # Write to L2 cache if enabled
        if self.l2_cache and self.write_through:
            results.append(await self.l2_cache.set(key, value, ttl))

        return any(results)  # Success if at least one layer succeeded

    async def delete(self, key: str) -> bool:
        """Delete from all cache layers."""
        results = []

        results.append(await self.l1_cache.delete(key))

        if self.l2_cache:
            results.append(await self.l2_cache.delete(key))

        return any(results)

    async def exists(self, key: str) -> bool:
        """Check if key exists in any cache layer."""
        if await self.l1_cache.exists(key):
            return True

        if self.l2_cache and await self.l2_cache.exists(key):
            return True

        return False

    async def clear(self, pattern: Optional[str] = None) -> Dict[str, int]:
        """Clear all cache layers."""
        results = {}

        results["l1_cleared"] = await self.l1_cache.clear(pattern)

        if self.l2_cache:
            results["l2_cleared"] = await self.l2_cache.clear(pattern)

        return results

    async def get_metrics(self) -> Dict[str, CacheMetrics]:
        """Get metrics from all cache layers."""
        metrics = {}

        metrics["l1"] = await self.l1_cache.get_metrics()

        if self.l2_cache:
            metrics["l2"] = await self.l2_cache.get_metrics()

        return metrics

    async def close(self):
        """Close all cache connections."""
        if self.l2_cache:
            await self.l2_cache.close()


class CacheDecorator:
    """Decorator for automatic function result caching."""

    def __init__(
        self,
        cache: Union[CacheBackend, MultiLayerCache],
        ttl: int = 3600,
        key_func: Optional[Callable] = None,
        condition: Optional[Callable] = None
    ):
        self.cache = cache
        self.ttl = ttl
        self.key_func = key_func or self._default_key_func
        self.condition = condition

    def __call__(self, func):
        """Decorator implementation."""
        async def wrapper(*args, **kwargs):
            # Generate cache key
            cache_key = self.key_func(func, args, kwargs)

            # Check condition if provided
            if self.condition and not self.condition(*args, **kwargs):
                return await func(*args, **kwargs)

            # Try to get from cache
            cached_result = await self.cache.get(cache_key)
            if cached_result is not None:
                return cached_result

            # Execute function and cache result
            result = await func(*args, **kwargs)
            await self.cache.set(cache_key, result, self.ttl)

            return result

        return wrapper

    def _default_key_func(self, func, args, kwargs) -> str:
        """Default cache key generation."""
        key_parts = [func.__name__]

        # Add args
        for arg in args:
            key_parts.append(str(arg))

        # Add sorted kwargs
        for k, v in sorted(kwargs.items()):
            key_parts.append(f"{k}:{v}")

        # Generate hash for long keys
        key_string = ":".join(key_parts)
        if len(key_string) > 100:
            key_string = hashlib.md5(key_string.encode()).hexdigest()

        return key_string


# Cache invalidation patterns
class CacheInvalidationManager:
    """Manages cache invalidation patterns and dependencies."""

    def __init__(self, cache: Union[CacheBackend, MultiLayerCache]):
        self.cache = cache
        self.tag_registry: Dict[str, List[str]] = {}  # tag -> [keys]
        self.key_tags: Dict[str, List[str]] = {}      # key -> [tags]

    async def set_with_tags(self, key: str, value: Any, tags: List[str], ttl: Optional[int] = None) -> bool:
        """Set cache value with invalidation tags."""
        # Set the actual cache value
        success = await self.cache.set(key, value, ttl)

        if success:
            # Register tags
            for tag in tags:
                if tag not in self.tag_registry:
                    self.tag_registry[tag] = []

                if key not in self.tag_registry[tag]:
                    self.tag_registry[tag].append(key)

            self.key_tags[key] = tags.copy()

        return success

    async def invalidate_by_tag(self, tag: str) -> int:
        """Invalidate all cache entries with the given tag."""
        if tag not in self.tag_registry:
            return 0

        keys_to_invalidate = self.tag_registry[tag].copy()
        count = 0

        for key in keys_to_invalidate:
            if await self.cache.delete(key):
                count += 1

                # Clean up tag registry
                if key in self.key_tags:
                    for key_tag in self.key_tags[key]:
                        if key_tag in self.tag_registry:
                            self.tag_registry[key_tag] = [
                                k for k in self.tag_registry[key_tag] if k != key
                            ]

                    del self.key_tags[key]

        # Clean up empty tag
        self.tag_registry[tag] = []

        return count

    async def invalidate_by_pattern(self, pattern: str) -> int:
        """Invalidate cache entries matching pattern."""
        return await self.cache.clear(pattern)


# Global cache instances
_cache_instances: Dict[str, Union[CacheBackend, MultiLayerCache]] = {}


def get_cache(name: str = "default") -> Union[CacheBackend, MultiLayerCache]:
    """Get named cache instance."""
    return _cache_instances.get(name)


def register_cache(name: str, cache: Union[CacheBackend, MultiLayerCache]):
    """Register named cache instance."""
    _cache_instances[name] = cache


# Convenience decorators
def cached(ttl: int = 3600, cache_name: str = "default", key_func: Optional[Callable] = None):
    """Convenience decorator for caching function results."""
    def decorator(func):
        cache = get_cache(cache_name)
        if cache is None:
            logger.warning(f"Cache '{cache_name}' not found, function will not be cached")
            return func

        cache_decorator = CacheDecorator(cache, ttl=ttl, key_func=key_func)
        return cache_decorator(func)

    return decorator


def cache_invalidate_tag(tag: str, cache_name: str = "default"):
    """Convenience function to invalidate cache by tag."""
    async def invalidate():
        cache = get_cache(cache_name)
        if cache and hasattr(cache, 'invalidation_manager'):
            return await cache.invalidation_manager.invalidate_by_tag(tag)
        return 0

    return invalidate()