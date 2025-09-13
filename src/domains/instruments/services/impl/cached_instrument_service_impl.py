"""
Cached implementation of InstrumentService using Redis.

This implementation wraps the base InstrumentServiceImpl with
intelligent caching, cache invalidation, and performance optimization.
"""

import logging
from typing import List, Optional, Dict, Any
from datetime import datetime

from domains.instruments.services.interfaces.instrument_service_interface import (
    InstrumentServiceInterface,
    InstrumentDTO,
    InstrumentXrefDTO,
    InstrumentSearchCriteria,
    InstrumentOperationResult
)
from domains.instruments.services.impl.instrument_service_impl import InstrumentServiceImpl
from infrastructure.caching.redis_cache import RedisCache, CacheKeyBuilder, cache_result
from core.platform.config.environment import Environment

logger = logging.getLogger(__name__)


class CachedInstrumentServiceImpl(InstrumentServiceInterface):
    """
    High-performance cached instrument service implementation.
    
    Features:
    - Redis-based caching with intelligent TTL
    - Automatic cache invalidation on mutations
    - Cache warming strategies
    - Performance metrics and monitoring
    - Graceful fallback when cache is unavailable
    """
    
    def __init__(self, base_service: InstrumentServiceImpl, environment: Environment):
        self.base_service = base_service
        self.cache = RedisCache(
            environment=environment,
            default_ttl=3600,  # 1 hour for instrument data
            key_prefix="instruments"
        )
        self._cache_stats = {"warm_cache_calls": 0, "invalidation_calls": 0}
    
    # READ OPERATIONS (Cached)
    
    async def get_instrument_by_id(self, instrument_id: int) -> Optional[InstrumentDTO]:
        """Get instrument by ID with caching"""
        cache_key = CacheKeyBuilder.instrument_by_id(instrument_id)
        
        # Try cache first
        cached_result = await self.cache.get(cache_key)
        if cached_result:
            # Convert dict back to DTO if needed
            if isinstance(cached_result, dict):
                return InstrumentDTO(**cached_result)
            return cached_result
        
        # Cache miss - get from base service
        result = await self.base_service.get_instrument_by_id(instrument_id)
        
        # Cache the result
        if result:
            await self.cache.set(cache_key, result, ttl=3600)
        
        return result
    
    async def get_instrument_by_symbol(self, symbol: str, vendor: str = "ticker") -> Optional[InstrumentDTO]:
        """Get instrument by symbol with caching"""
        cache_key = CacheKeyBuilder.instrument_by_symbol(symbol, vendor)
        
        # Try cache first
        cached_result = await self.cache.get(cache_key)
        if cached_result:
            if isinstance(cached_result, dict):
                return InstrumentDTO(**cached_result)
            return cached_result
        
        # Cache miss - get from base service
        result = await self.base_service.get_instrument_by_symbol(symbol, vendor)
        
        # Cache the result
        if result:
            await self.cache.set(cache_key, result, ttl=3600)
            # Also cache by ID for consistency
            id_key = CacheKeyBuilder.instrument_by_id(result.id)
            await self.cache.set(id_key, result, ttl=3600)
        
        return result
    
    async def list_instruments(self, criteria: InstrumentSearchCriteria) -> List[InstrumentDTO]:
        """List instruments with caching"""
        criteria_dict = {
            'symbols': criteria.symbols,
            'exchanges': criteria.exchanges,
            'instrument_types': criteria.instrument_types,
            'currencies': criteria.currencies,
            'active_only': criteria.active_only,
            'limit': criteria.limit,
            'offset': criteria.offset
        }
        
        criteria_hash = CacheKeyBuilder.hash_criteria(criteria_dict)
        cache_key = CacheKeyBuilder.instruments_list(criteria_hash)
        
        # Try cache first
        cached_result = await self.cache.get(cache_key)
        if cached_result:
            # Convert dicts back to DTOs
            if isinstance(cached_result, list) and cached_result:
                if isinstance(cached_result[0], dict):
                    return [InstrumentDTO(**item) for item in cached_result]
            return cached_result or []
        
        # Cache miss - get from base service
        result = await self.base_service.list_instruments(criteria)
        
        # Cache the result (shorter TTL for lists as they change more frequently)
        await self.cache.set(cache_key, result, ttl=1800)  # 30 minutes
        
        return result
    
    async def get_instrument_count(self) -> int:
        """Get total instrument count with caching"""
        cache_key = CacheKeyBuilder.instrument_count()
        
        # Try cache first
        cached_result = await self.cache.get(cache_key)
        if cached_result is not None:
            return int(cached_result)
        
        # Cache miss - get from base service
        result = await self.base_service.get_instrument_count()
        
        # Cache the result (shorter TTL as count changes frequently)
        await self.cache.set(cache_key, result, ttl=600)  # 10 minutes
        
        return result
    
    async def validate_symbol(self, symbol: str, vendor: str = "ticker") -> bool:
        """Validate symbol with caching"""
        # Use get_instrument_by_symbol which is already cached
        instrument = await self.get_instrument_by_symbol(symbol, vendor)
        return instrument is not None
    
    async def get_cross_references(self, instrument_id: int) -> List[InstrumentXrefDTO]:
        """Get cross-references with caching"""
        cache_key = CacheKeyBuilder.cross_references(instrument_id)
        
        # Try cache first
        cached_result = await self.cache.get(cache_key)
        if cached_result:
            if isinstance(cached_result, list) and cached_result:
                if isinstance(cached_result[0], dict):
                    return [InstrumentXrefDTO(**item) for item in cached_result]
            return cached_result or []
        
        # Cache miss - get from base service
        result = await self.base_service.get_cross_references(instrument_id)
        
        # Cache the result
        await self.cache.set(cache_key, result, ttl=3600)
        
        return result
    
    # WRITE OPERATIONS (Cache Invalidation)
    
    async def create_instrument(self, instrument: InstrumentDTO) -> InstrumentOperationResult:
        """Create instrument and invalidate relevant caches"""
        result = await self.base_service.create_instrument(instrument)
        
        if result.success and result.instrument_id:
            # Invalidate list and count caches
            await self.cache.delete_pattern("instruments:list:*")
            await self.cache.delete_pattern("instruments:count")
            
            # Pre-warm cache with new instrument
            if instrument.symbol:
                cache_key = CacheKeyBuilder.instrument_by_symbol(instrument.symbol)
                new_instrument = await self.base_service.get_instrument_by_id(result.instrument_id)
                if new_instrument:
                    await self.cache.set(cache_key, new_instrument, ttl=3600)
        
        return result
    
    async def create_instruments_batch(self, instruments: List[InstrumentDTO]) -> InstrumentOperationResult:
        """Create instruments batch and invalidate caches"""
        result = await self.base_service.create_instruments_batch(instruments)
        
        if result.success and result.created_count > 0:
            # Invalidate all list and count caches
            await self.cache.delete_pattern("instruments:list:*")
            await self.cache.delete_pattern("instruments:count")
            
            logger.info(f"Cache invalidated after creating {result.created_count} instruments")
        
        return result
    
    async def create_cross_reference(self, xref: InstrumentXrefDTO) -> InstrumentOperationResult:
        """Create cross-reference and invalidate caches"""
        result = await self.base_service.create_cross_reference(xref)
        
        if result.success:
            # Invalidate specific instrument caches
            if xref.instrument_id:
                await self.cache.invalidate_instrument(xref.instrument_id, xref.vendor_symbol)
        
        return result
    
    async def create_cross_references_batch(self, xrefs: List[InstrumentXrefDTO]) -> InstrumentOperationResult:
        """Create cross-references batch and invalidate caches"""
        result = await self.base_service.create_cross_references_batch(xrefs)
        
        if result.success and result.created_count > 0:
            # Invalidate affected instruments
            instrument_ids = {xref.instrument_id for xref in xrefs if xref.instrument_id}
            for instrument_id in instrument_ids:
                relevant_xref = next(x for x in xrefs if x.instrument_id == instrument_id)
                await self.cache.invalidate_instrument(instrument_id, relevant_xref.vendor_symbol)
            
            logger.info(f"Cache invalidated for {len(instrument_ids)} instruments after xref batch")
        
        return result
    
    # PERFORMANCE AND MONITORING
    
    async def warm_cache(self, symbols: List[str] = None, limit: int = 1000):
        """Pre-warm cache with commonly accessed instruments"""
        self._cache_stats["warm_cache_calls"] += 1
        
        try:
            if symbols:
                # Warm specific symbols
                for symbol in symbols:
                    instrument = await self.get_instrument_by_symbol(symbol)
                    if instrument:
                        await self.get_cross_references(instrument.id)
                logger.info(f"Cache warmed for {len(symbols)} specific symbols")
            else:
                # Warm most commonly accessed instruments
                criteria = InstrumentSearchCriteria(active_only=True, limit=limit)
                instruments = await self.list_instruments(criteria)
                
                # Pre-load cross-references for active instruments
                for instrument in instruments[:100]:  # Limit to prevent overload
                    await self.get_cross_references(instrument.id)
                
                logger.info(f"Cache warmed for {len(instruments)} active instruments")
                
        except Exception as e:
            logger.error(f"Cache warming failed: {e}")
    
    async def get_cache_stats(self) -> Dict[str, Any]:
        """Get comprehensive cache performance statistics"""
        cache_health = await self.cache.health_check()
        cache_stats = self.cache.get_stats()
        
        return {
            **cache_stats,
            **self._cache_stats,
            "cache_health": cache_health,
            "cache_efficiency": {
                "hit_rate": cache_stats.get("hit_rate", 0.0),
                "total_operations": cache_stats.get("hits", 0) + cache_stats.get("misses", 0),
                "error_rate": cache_stats.get("errors", 0) / max(cache_stats.get("hits", 0) + cache_stats.get("misses", 0), 1)
            }
        }
    
    async def invalidate_all_cache(self):
        """Clear all instrument-related cache entries"""
        self._cache_stats["invalidation_calls"] += 1
        
        patterns = [
            "instrument:*",
            "instruments:*", 
            "xrefs:*",
            "vendor:*"
        ]
        
        total_deleted = 0
        for pattern in patterns:
            deleted = await self.cache.delete_pattern(pattern)
            total_deleted += deleted
        
        logger.info(f"Cache invalidation complete: {total_deleted} keys removed")
        return total_deleted
    
    # CACHE-OPTIMIZED BULK OPERATIONS
    
    async def get_instruments_by_ids(self, instrument_ids: List[int]) -> List[InstrumentDTO]:
        """Get multiple instruments with batch caching optimization"""
        results = []
        cache_misses = []
        
        # Check cache for each ID
        for instrument_id in instrument_ids:
            cache_key = CacheKeyBuilder.instrument_by_id(instrument_id)
            cached_result = await self.cache.get(cache_key)
            
            if cached_result:
                if isinstance(cached_result, dict):
                    results.append(InstrumentDTO(**cached_result))
                else:
                    results.append(cached_result)
            else:
                cache_misses.append(instrument_id)
        
        # Batch load cache misses
        if cache_misses:
            # Get missing instruments from base service
            for instrument_id in cache_misses:
                instrument = await self.base_service.get_instrument_by_id(instrument_id)
                if instrument:
                    results.append(instrument)
                    # Cache the result
                    cache_key = CacheKeyBuilder.instrument_by_id(instrument_id)
                    await self.cache.set(cache_key, instrument, ttl=3600)
        
        # Sort results to match input order
        id_to_instrument = {inst.id: inst for inst in results if inst}
        return [id_to_instrument.get(inst_id) for inst_id in instrument_ids if inst_id in id_to_instrument]
    
    async def get_instruments_by_symbols(self, symbols: List[str], vendor: str = "ticker") -> List[InstrumentDTO]:
        """Get multiple instruments by symbols with batch caching"""
        results = []
        cache_misses = []
        
        # Check cache for each symbol
        for symbol in symbols:
            cache_key = CacheKeyBuilder.instrument_by_symbol(symbol, vendor)
            cached_result = await self.cache.get(cache_key)
            
            if cached_result:
                if isinstance(cached_result, dict):
                    results.append(InstrumentDTO(**cached_result))
                else:
                    results.append(cached_result)
            else:
                cache_misses.append(symbol)
        
        # Batch load cache misses
        if cache_misses:
            for symbol in cache_misses:
                instrument = await self.base_service.get_instrument_by_symbol(symbol, vendor)
                if instrument:
                    results.append(instrument)
                    # Cache the result
                    cache_key = CacheKeyBuilder.instrument_by_symbol(symbol, vendor)
                    await self.cache.set(cache_key, instrument, ttl=3600)
        
        return results
    
    async def close(self):
        """Close cache connections and cleanup resources"""
        await self.cache.close()
        if hasattr(self.base_service, 'close'):
            await self.base_service.close()
        
        logger.info("Cached instrument service closed successfully")