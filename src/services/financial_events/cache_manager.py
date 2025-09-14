"""
Advanced Caching Manager for xAI Financial Event Extractor
Implements multiple caching strategies to minimize API calls
"""

import json
import hashlib
import asyncio
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional, Union
from pathlib import Path
import logging
from dataclasses import dataclass, asdict

# Try to import aiofiles, fallback to regular file operations
try:
    import aiofiles
    AIOFILES_AVAILABLE = True
except ImportError:
    AIOFILES_AVAILABLE = False
    import warnings
    warnings.warn("aiofiles not available, using synchronous file operations")

logger = logging.getLogger(__name__)

@dataclass
class CacheEntry:
    """Cache entry with metadata"""
    key: str
    data: Any
    timestamp: datetime
    expiry: datetime
    hit_count: int = 0
    size_bytes: int = 0
    
    def is_expired(self) -> bool:
        return datetime.now() > self.expiry
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "key": self.key,
            "data": self.data,
            "timestamp": self.timestamp.isoformat(),
            "expiry": self.expiry.isoformat(),
            "hit_count": self.hit_count,
            "size_bytes": self.size_bytes
        }

class SmartCacheManager:
    """
    Multi-tier caching system for xAI API responses
    
    Tier 1: In-memory cache (fastest)
    Tier 2: File-based cache (persistent) 
    Tier 3: Query deduplication (smart caching)
    """
    
    def __init__(
        self, 
        cache_dir: str = "/tmp/xai_event_cache",
        max_memory_size_mb: int = 100,
        default_ttl_hours: int = 24,
        enable_persistent_cache: bool = True
    ):
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(exist_ok=True)
        
        # In-memory cache
        self.memory_cache: Dict[str, CacheEntry] = {}
        self.max_memory_size = max_memory_size_mb * 1024 * 1024  # Convert to bytes
        self.current_memory_usage = 0
        
        # Configuration
        self.default_ttl = timedelta(hours=default_ttl_hours)
        self.enable_persistent = enable_persistent_cache
        
        # Cache statistics
        self.stats = {
            "hits": 0,
            "misses": 0,
            "memory_hits": 0,
            "disk_hits": 0,
            "saves": 0,
            "evictions": 0
        }
        
        logger.info(f"Cache manager initialized: {cache_dir}, TTL: {default_ttl_hours}h")
    
    def _generate_cache_key(
        self, 
        start_date: str, 
        end_date: str, 
        symbols: List[str] = None,
        event_types: List[str] = None,
        prefix: str = "events"
    ) -> str:
        """Generate consistent cache key from parameters"""
        
        # Sort symbols and event types for consistency
        symbols_str = ",".join(sorted(symbols or []))
        event_types_str = ",".join(sorted(event_types or []))
        
        # Create key components
        key_data = f"{prefix}:{start_date}:{end_date}:{symbols_str}:{event_types_str}"
        
        # Hash for consistent, shorter keys
        key_hash = hashlib.md5(key_data.encode()).hexdigest()
        
        return f"{prefix}_{key_hash}_{start_date}_{end_date}"
    
    async def get(
        self, 
        start_date: str, 
        end_date: str, 
        symbols: List[str] = None,
        event_types: List[str] = None
    ) -> Optional[List[Dict[str, Any]]]:
        """
        Get cached events data with multi-tier lookup
        """
        
        cache_key = self._generate_cache_key(start_date, end_date, symbols, event_types)
        
        # Tier 1: Memory cache lookup
        if cache_key in self.memory_cache:
            entry = self.memory_cache[cache_key]
            
            if not entry.is_expired():
                entry.hit_count += 1
                self.stats["hits"] += 1
                self.stats["memory_hits"] += 1
                logger.debug(f"Memory cache HIT: {cache_key}")
                return entry.data
            else:
                # Remove expired entry
                self._evict_memory_entry(cache_key)
        
        # Tier 2: Persistent file cache lookup
        if self.enable_persistent:
            cached_data = await self._load_from_disk(cache_key)
            if cached_data:
                # Promote to memory cache
                await self._store_in_memory(cache_key, cached_data, self.default_ttl)
                self.stats["hits"] += 1
                self.stats["disk_hits"] += 1
                logger.debug(f"Disk cache HIT: {cache_key}")
                return cached_data
        
        # Cache miss
        self.stats["misses"] += 1
        logger.debug(f"Cache MISS: {cache_key}")
        return None
    
    async def set(
        self, 
        start_date: str, 
        end_date: str, 
        data: List[Dict[str, Any]],
        symbols: List[str] = None,
        event_types: List[str] = None,
        ttl: Optional[timedelta] = None
    ) -> str:
        """
        Store data in multi-tier cache
        """
        
        cache_key = self._generate_cache_key(start_date, end_date, symbols, event_types)
        ttl = ttl or self.default_ttl
        
        # Store in memory cache
        await self._store_in_memory(cache_key, data, ttl)
        
        # Store in persistent cache
        if self.enable_persistent:
            await self._store_on_disk(cache_key, data, ttl)
        
        self.stats["saves"] += 1
        logger.debug(f"Cached data: {cache_key} ({len(data)} events)")
        
        return cache_key
    
    async def _store_in_memory(
        self, 
        cache_key: str, 
        data: List[Dict[str, Any]], 
        ttl: timedelta
    ):
        """Store data in memory cache with LRU eviction"""
        
        # Calculate data size
        data_size = len(json.dumps(data).encode('utf-8'))
        
        # Check if we need to make space
        while (self.current_memory_usage + data_size > self.max_memory_size 
               and self.memory_cache):
            self._evict_lru_entry()
        
        # Create cache entry
        entry = CacheEntry(
            key=cache_key,
            data=data,
            timestamp=datetime.now(),
            expiry=datetime.now() + ttl,
            size_bytes=data_size
        )
        
        # Store in memory
        self.memory_cache[cache_key] = entry
        self.current_memory_usage += data_size
    
    async def _store_on_disk(
        self, 
        cache_key: str, 
        data: List[Dict[str, Any]], 
        ttl: timedelta
    ):
        """Store data in persistent file cache"""
        
        cache_file = self.cache_dir / f"{cache_key}.json"
        
        cache_data = {
            "data": data,
            "timestamp": datetime.now().isoformat(),
            "expiry": (datetime.now() + ttl).isoformat(),
            "key": cache_key
        }
        
        try:
            if AIOFILES_AVAILABLE:
                async with aiofiles.open(cache_file, 'w') as f:
                    await f.write(json.dumps(cache_data, indent=2))
            else:
                # Fallback to synchronous file operations
                with open(cache_file, 'w') as f:
                    f.write(json.dumps(cache_data, indent=2))
        except Exception as e:
            logger.error(f"Failed to save cache to disk: {e}")
    
    async def _load_from_disk(self, cache_key: str) -> Optional[List[Dict[str, Any]]]:
        """Load data from persistent file cache"""
        
        cache_file = self.cache_dir / f"{cache_key}.json"
        
        if not cache_file.exists():
            return None
        
        try:
            if AIOFILES_AVAILABLE:
                async with aiofiles.open(cache_file, 'r') as f:
                    content = await f.read()
            else:
                # Fallback to synchronous file operations
                with open(cache_file, 'r') as f:
                    content = f.read()
            
            cache_data = json.loads(content)
            
            # Check expiry
            expiry = datetime.fromisoformat(cache_data["expiry"])
            if datetime.now() > expiry:
                # Remove expired file
                cache_file.unlink(missing_ok=True)
                return None
            
            return cache_data["data"]
            
        except Exception as e:
            logger.error(f"Failed to load cache from disk: {e}")
            # Remove corrupted cache file
            cache_file.unlink(missing_ok=True)
            return None
    
    def _evict_memory_entry(self, cache_key: str):
        """Remove entry from memory cache"""
        if cache_key in self.memory_cache:
            entry = self.memory_cache[cache_key]
            self.current_memory_usage -= entry.size_bytes
            del self.memory_cache[cache_key]
            self.stats["evictions"] += 1
    
    def _evict_lru_entry(self):
        """Evict least recently used entry from memory"""
        if not self.memory_cache:
            return
        
        # Find LRU entry (lowest hit_count, then oldest timestamp)
        lru_key = min(
            self.memory_cache.keys(),
            key=lambda k: (self.memory_cache[k].hit_count, self.memory_cache[k].timestamp)
        )
        
        logger.debug(f"Evicting LRU entry: {lru_key}")
        self._evict_memory_entry(lru_key)
    
    def get_cache_stats(self) -> Dict[str, Any]:
        """Get comprehensive cache statistics"""
        
        hit_rate = (self.stats["hits"] / (self.stats["hits"] + self.stats["misses"]) 
                   if (self.stats["hits"] + self.stats["misses"]) > 0 else 0)
        
        return {
            "hit_rate": f"{hit_rate:.1%}",
            "total_requests": self.stats["hits"] + self.stats["misses"],
            "memory_usage_mb": round(self.current_memory_usage / 1024 / 1024, 2),
            "memory_entries": len(self.memory_cache),
            "disk_cache_files": len(list(self.cache_dir.glob("*.json"))),
            **self.stats
        }
    
    async def cleanup_expired(self):
        """Clean up expired cache entries"""
        
        # Clean memory cache
        expired_keys = [
            key for key, entry in self.memory_cache.items() 
            if entry.is_expired()
        ]
        
        for key in expired_keys:
            self._evict_memory_entry(key)
        
        # Clean disk cache
        if self.enable_persistent:
            cache_files = list(self.cache_dir.glob("*.json"))
            cleaned = 0
            
            for cache_file in cache_files:
                try:
                    if AIOFILES_AVAILABLE:
                        async with aiofiles.open(cache_file, 'r') as f:
                            content = await f.read()
                    else:
                        with open(cache_file, 'r') as f:
                            content = f.read()
                    
                    cache_data = json.loads(content)
                    expiry = datetime.fromisoformat(cache_data["expiry"])
                    if datetime.now() > expiry:
                        cache_file.unlink()
                        cleaned += 1
                        
                except Exception as e:
                    logger.warning(f"Failed to check cache file {cache_file}: {e}")
                    cache_file.unlink(missing_ok=True)
                    cleaned += 1
            
            if cleaned > 0:
                logger.info(f"Cleaned up {cleaned} expired cache files")
    
    def clear_all(self):
        """Clear all cache data"""
        
        # Clear memory cache
        self.memory_cache.clear()
        self.current_memory_usage = 0
        
        # Clear disk cache
        if self.enable_persistent:
            for cache_file in self.cache_dir.glob("*.json"):
                cache_file.unlink()
        
        # Reset stats
        self.stats = {key: 0 for key in self.stats.keys()}
        
        logger.info("All cache data cleared")

class QueryDeduplicator:
    """
    Smart query deduplication to avoid redundant API calls
    """
    
    def __init__(self):
        self.pending_queries: Dict[str, asyncio.Future] = {}
    
    def get_query_key(
        self, 
        start_date: str, 
        end_date: str, 
        symbols: List[str] = None
    ) -> str:
        """Generate key for query deduplication"""
        symbols_str = ",".join(sorted(symbols or []))
        return f"query:{start_date}:{end_date}:{symbols_str}"
    
    async def deduplicate_query(
        self, 
        query_key: str, 
        query_func,
        *args, 
        **kwargs
    ) -> Any:
        """
        Deduplicate concurrent queries - if same query is running, wait for it
        """
        
        # Check if query is already running
        if query_key in self.pending_queries:
            logger.debug(f"Query deduplication: Waiting for existing query {query_key}")
            return await self.pending_queries[query_key]
        
        # Create future for this query
        future = asyncio.Future()
        self.pending_queries[query_key] = future
        
        try:
            # Execute query
            result = await query_func(*args, **kwargs)
            future.set_result(result)
            return result
            
        except Exception as e:
            future.set_exception(e)
            raise
            
        finally:
            # Clean up
            if query_key in self.pending_queries:
                del self.pending_queries[query_key]

# Usage example and testing
async def test_cache_manager():
    """Test the cache manager functionality"""
    
    print("🧪 Testing Cache Manager")
    print("=" * 50)
    
    # Initialize cache manager
    cache_manager = SmartCacheManager(
        cache_dir="/tmp/test_xai_cache",
        max_memory_size_mb=10,
        default_ttl_hours=1
    )
    
    # Test data
    test_events = [
        {
            "event_type": "earnings",
            "company_symbol": "AAPL",
            "details": "Test earnings announcement",
            "event_date": "2025-09-13",
            "impact_level": "high"
        }
    ]
    
    # Test cache miss
    print("🔍 Testing cache miss...")
    result = await cache_manager.get("2025-09-01", "2025-09-13", ["AAPL"])
    print(f"Cache miss result: {result}")
    
    # Test cache set
    print("💾 Testing cache set...")
    cache_key = await cache_manager.set(
        "2025-09-01", "2025-09-13", test_events, ["AAPL"]
    )
    print(f"Cache key: {cache_key}")
    
    # Test cache hit (memory)
    print("⚡ Testing memory cache hit...")
    result = await cache_manager.get("2025-09-01", "2025-09-13", ["AAPL"])
    print(f"Cache hit result: {len(result) if result else None} events")
    
    # Clear memory and test disk cache
    print("💽 Testing disk cache hit...")
    cache_manager.memory_cache.clear()
    result = await cache_manager.get("2025-09-01", "2025-09-13", ["AAPL"])
    print(f"Disk cache hit result: {len(result) if result else None} events")
    
    # Print statistics
    print("📊 Cache Statistics:")
    stats = cache_manager.get_cache_stats()
    for key, value in stats.items():
        print(f"  {key}: {value}")

if __name__ == "__main__":
    asyncio.run(test_cache_manager())