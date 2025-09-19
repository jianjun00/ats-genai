"""
Cached Instrument Service Implementation

Enhanced instrument service with comprehensive caching for optimal performance.
Integrates multi-layer caching, intelligent invalidation, and performance monitoring.
"""

import asyncio
from typing import List, Optional, Dict, Any
import logging

from infrastructure.caching import (
    MultiLayerCache,
    cached,
    CacheInvalidationManager,
    DatabaseCache
)
from ..interfaces.instrument_service_interface import (
    InstrumentServiceInterface,
    VendorInstrumentDTO,
    InstrumentXrefDTO,
    UnifiedInstrumentDTO,
    SearchCriteria,
    OperationResult,
    BulkOperationResult
)
from ..impl.instrument_service_impl import InstrumentServiceImpl

logger = logging.getLogger(__name__)


class CachedInstrumentService(InstrumentServiceInterface):
    """Instrument service with advanced caching capabilities."""

    def __init__(
        self,
        base_service: InstrumentServiceImpl,
        cache: MultiLayerCache,
        db_cache: DatabaseCache
    ):
        self.base_service = base_service
        self.cache = cache
        self.db_cache = db_cache

        # Cache invalidation manager
        self.invalidation_manager = CacheInvalidationManager(cache)

        # Cache configuration for different operations
        self.cache_configs = {
            'vendor_instruments': {'ttl': 3600, 'tags': ['vendor_instruments']},
            'instrument_xrefs': {'ttl': 1800, 'tags': ['instrument_xrefs']},
            'unified_instruments': {'ttl': 7200, 'tags': ['unified_instruments']},
            'symbol_lookups': {'ttl': 14400, 'tags': ['symbol_lookups']},
            'search_results': {'ttl': 600, 'tags': ['search_results']}
        }

        # Performance metrics
        self.performance_metrics = {
            'cache_hits': 0,
            'cache_misses': 0,
            'invalidations': 0,
            'total_requests': 0
        }

    # === Cached CRUD Operations ===

    @cached(ttl=3600, cache_name="instruments")
    async def get_vendor_instrument_by_id(self, instrument_id: int) -> Optional[VendorInstrumentDTO]:
        """Get vendor instrument by ID with caching."""
        logger.debug(f"Cache miss for vendor instrument ID: {instrument_id}")
        return await self.base_service.get_vendor_instrument_by_id(instrument_id)

    @cached(ttl=1800, cache_name="instruments")
    async def get_instrument_xref_by_id(self, xref_id: int) -> Optional[InstrumentXrefDTO]:
        """Get instrument cross-reference by ID with caching."""
        logger.debug(f"Cache miss for instrument xref ID: {xref_id}")
        return await self.base_service.get_instrument_xref_by_id(xref_id)

    @cached(ttl=7200, cache_name="instruments")
    async def get_unified_instrument_by_id(self, instrument_id: int) -> Optional[UnifiedInstrumentDTO]:
        """Get unified instrument by ID with caching."""
        logger.debug(f"Cache miss for unified instrument ID: {instrument_id}")
        return await self.base_service.get_unified_instrument_by_id(instrument_id)

    # === Cached Search Operations ===

    async def list_vendor_instruments(self, criteria: SearchCriteria) -> List[VendorInstrumentDTO]:
        """List vendor instruments with intelligent caching."""
        # Generate cache key based on criteria
        cache_key = self._generate_search_cache_key("vendor_instruments", criteria)

        # Try cache first
        cached_result = await self.cache.get(cache_key)
        if cached_result is not None:
            self.performance_metrics['cache_hits'] += 1
            logger.debug(f"Cache hit for vendor instruments search")
            return cached_result

        # Cache miss - fetch from base service
        self.performance_metrics['cache_misses'] += 1
        logger.debug(f"Cache miss for vendor instruments search")

        result = await self.base_service.list_vendor_instruments(criteria)

        # Cache with appropriate TTL based on query complexity
        ttl = self._calculate_dynamic_ttl(criteria, base_ttl=600)
        await self.invalidation_manager.set_with_tags(
            cache_key,
            result,
            self.cache_configs['vendor_instruments']['tags'],
            ttl=ttl
        )

        self.performance_metrics['total_requests'] += 1
        return result

    async def list_instrument_xrefs(self, criteria: SearchCriteria) -> List[InstrumentXrefDTO]:
        """List instrument cross-references with intelligent caching."""
        cache_key = self._generate_search_cache_key("instrument_xrefs", criteria)

        cached_result = await self.cache.get(cache_key)
        if cached_result is not None:
            self.performance_metrics['cache_hits'] += 1
            return cached_result

        self.performance_metrics['cache_misses'] += 1
        result = await self.base_service.list_instrument_xrefs(criteria)

        ttl = self._calculate_dynamic_ttl(criteria, base_ttl=900)
        await self.invalidation_manager.set_with_tags(
            cache_key,
            result,
            self.cache_configs['instrument_xrefs']['tags'],
            ttl=ttl
        )

        self.performance_metrics['total_requests'] += 1
        return result

    async def list_unified_instruments(self, criteria: SearchCriteria) -> List[UnifiedInstrumentDTO]:
        """List unified instruments with intelligent caching."""
        cache_key = self._generate_search_cache_key("unified_instruments", criteria)

        cached_result = await self.cache.get(cache_key)
        if cached_result is not None:
            self.performance_metrics['cache_hits'] += 1
            return cached_result

        self.performance_metrics['cache_misses'] += 1
        result = await self.base_service.list_unified_instruments(criteria)

        ttl = self._calculate_dynamic_ttl(criteria, base_ttl=1200)
        await self.invalidation_manager.set_with_tags(
            cache_key,
            result,
            self.cache_configs['unified_instruments']['tags'],
            ttl=ttl
        )

        self.performance_metrics['total_requests'] += 1
        return result

    # === Cached Lookup Operations ===

    @cached(ttl=14400, cache_name="instruments")
    async def get_vendor_instrument_by_symbol(
        self,
        vendor_symbol: str,
        vendor_name: str
    ) -> Optional[VendorInstrumentDTO]:
        """Get vendor instrument by symbol with long-term caching."""
        logger.debug(f"Cache miss for vendor instrument: {vendor_name}:{vendor_symbol}")
        return await self.base_service.get_vendor_instrument_by_symbol(vendor_symbol, vendor_name)

    @cached(ttl=7200, cache_name="instruments")
    async def resolve_unified_symbol(self, vendor_symbol: str, vendor_name: str) -> Optional[str]:
        """Resolve unified symbol with caching."""
        logger.debug(f"Cache miss for symbol resolution: {vendor_name}:{vendor_symbol}")
        return await self.base_service.resolve_unified_symbol(vendor_symbol, vendor_name)

    @cached(ttl=10800, cache_name="instruments")
    async def get_vendor_symbols_for_unified(self, unified_symbol: str) -> List[Dict[str, str]]:
        """Get vendor symbols for unified symbol with caching."""
        logger.debug(f"Cache miss for vendor symbols: {unified_symbol}")
        return await self.base_service.get_vendor_symbols_for_unified(unified_symbol)

    # === Write Operations with Cache Invalidation ===

    async def create_vendor_instrument(self, instrument: VendorInstrumentDTO) -> OperationResult:
        """Create vendor instrument and invalidate related caches."""
        result = await self.base_service.create_vendor_instrument(instrument)

        if result.success:
            # Invalidate vendor instrument caches
            await self._invalidate_vendor_instrument_caches(instrument.vendor_name, instrument.vendor_symbol)
            self.performance_metrics['invalidations'] += 1

        return result

    async def update_vendor_instrument(self, instrument: VendorInstrumentDTO) -> OperationResult:
        """Update vendor instrument and invalidate related caches."""
        result = await self.base_service.update_vendor_instrument(instrument)

        if result.success:
            await self._invalidate_vendor_instrument_caches(instrument.vendor_name, instrument.vendor_symbol)
            # Also invalidate by ID if available
            if instrument.id:
                await self.cache.delete(f"get_vendor_instrument_by_id:{instrument.id}")
            self.performance_metrics['invalidations'] += 1

        return result

    async def create_instrument_xref(self, xref: InstrumentXrefDTO) -> OperationResult:
        """Create instrument cross-reference and invalidate related caches."""
        result = await self.base_service.create_instrument_xref(xref)

        if result.success:
            await self._invalidate_xref_caches(xref.vendor_symbol, xref.vendor_name, xref.unified_symbol)
            self.performance_metrics['invalidations'] += 1

        return result

    async def update_instrument_xref(self, xref: InstrumentXrefDTO) -> OperationResult:
        """Update instrument cross-reference and invalidate related caches."""
        result = await self.base_service.update_instrument_xref(xref)

        if result.success:
            await self._invalidate_xref_caches(xref.vendor_symbol, xref.vendor_name, xref.unified_symbol)
            if xref.id:
                await self.cache.delete(f"get_instrument_xref_by_id:{xref.id}")
            self.performance_metrics['invalidations'] += 1

        return result

    async def create_unified_instrument(self, instrument: UnifiedInstrumentDTO) -> OperationResult:
        """Create unified instrument and invalidate related caches."""
        result = await self.base_service.create_unified_instrument(instrument)

        if result.success:
            await self._invalidate_unified_instrument_caches(instrument.unified_symbol)
            self.performance_metrics['invalidations'] += 1

        return result

    # === Batch Operations with Optimized Caching ===

    async def create_vendor_instruments_batch(
        self,
        instruments: List[VendorInstrumentDTO]
    ) -> BulkOperationResult:
        """Create vendor instruments in batch and invalidate caches efficiently."""
        result = await self.base_service.create_vendor_instruments_batch(instruments)

        if result.success:
            # Batch invalidation for performance
            unique_vendors = set((inst.vendor_name, inst.vendor_symbol) for inst in instruments)

            invalidation_tasks = [
                self._invalidate_vendor_instrument_caches(vendor_name, vendor_symbol)
                for vendor_name, vendor_symbol in unique_vendors
            ]

            await asyncio.gather(*invalidation_tasks)
            self.performance_metrics['invalidations'] += len(unique_vendors)

        return result

    # === Cache Management Methods ===

    def _generate_search_cache_key(self, operation: str, criteria: SearchCriteria) -> str:
        """Generate cache key for search operations."""
        key_parts = [operation]

        if criteria.symbols:
            key_parts.append(f"symbols:{','.join(sorted(criteria.symbols))}")

        if criteria.vendor_name:
            key_parts.append(f"vendor:{criteria.vendor_name}")

        if criteria.exchanges:
            key_parts.append(f"exchanges:{','.join(sorted(criteria.exchanges))}")

        if criteria.instrument_types:
            key_parts.append(f"types:{','.join(sorted(criteria.instrument_types))}")

        if criteria.limit:
            key_parts.append(f"limit:{criteria.limit}")

        if criteria.offset:
            key_parts.append(f"offset:{criteria.offset}")

        return ":".join(key_parts)

    def _calculate_dynamic_ttl(self, criteria: SearchCriteria, base_ttl: int) -> int:
        """Calculate dynamic TTL based on query specificity."""
        # More specific queries get longer TTL
        specificity_score = 0

        if criteria.symbols and len(criteria.symbols) <= 5:
            specificity_score += 2
        elif criteria.symbols:
            specificity_score += 1

        if criteria.vendor_name:
            specificity_score += 2

        if criteria.exchanges and len(criteria.exchanges) <= 3:
            specificity_score += 1

        # Adjust TTL based on specificity
        ttl_multiplier = min(3.0, 1.0 + (specificity_score * 0.3))
        return int(base_ttl * ttl_multiplier)

    async def _invalidate_vendor_instrument_caches(self, vendor_name: str, vendor_symbol: str):
        """Invalidate caches related to vendor instrument."""
        patterns_to_invalidate = [
            f"*vendor_instruments*vendor:{vendor_name}*",
            f"*vendor_instruments*symbols:*{vendor_symbol}*",
            f"get_vendor_instrument_by_symbol:{vendor_symbol}:{vendor_name}",
            "*search_results*"
        ]

        for pattern in patterns_to_invalidate:
            await self.cache.clear(pattern)

    async def _invalidate_xref_caches(self, vendor_symbol: str, vendor_name: str, unified_symbol: str):
        """Invalidate caches related to instrument cross-references."""
        patterns_to_invalidate = [
            f"*instrument_xrefs*vendor:{vendor_name}*",
            f"*instrument_xrefs*symbols:*{vendor_symbol}*",
            f"resolve_unified_symbol:{vendor_symbol}:{vendor_name}",
            f"get_vendor_symbols_for_unified:{unified_symbol}",
            "*search_results*"
        ]

        for pattern in patterns_to_invalidate:
            await self.cache.clear(pattern)

    async def _invalidate_unified_instrument_caches(self, unified_symbol: str):
        """Invalidate caches related to unified instrument."""
        patterns_to_invalidate = [
            f"*unified_instruments*symbols:*{unified_symbol}*",
            f"get_vendor_symbols_for_unified:{unified_symbol}",
            "*search_results*"
        ]

        for pattern in patterns_to_invalidate:
            await self.cache.clear(pattern)

    # === Cache Statistics and Management ===

    async def get_cache_statistics(self) -> Dict[str, Any]:
        """Get comprehensive cache statistics."""
        cache_metrics = await self.cache.get_metrics()
        db_cache_metrics = await self.db_cache.get_query_metrics()

        total_requests = self.performance_metrics['cache_hits'] + self.performance_metrics['cache_misses']
        hit_rate = (self.performance_metrics['cache_hits'] / total_requests * 100) if total_requests > 0 else 0

        return {
            'service_metrics': {
                **self.performance_metrics,
                'hit_rate_percent': round(hit_rate, 2),
                'total_requests': total_requests
            },
            'multi_layer_cache': cache_metrics,
            'database_cache': db_cache_metrics,
            'cache_configurations': self.cache_configs
        }

    async def warm_cache(self, symbols: Optional[List[str]] = None) -> Dict[str, int]:
        """Warm cache with commonly accessed data."""
        logger.info("Starting cache warm-up process...")

        warm_up_stats = {
            'vendor_instruments_loaded': 0,
            'instrument_xrefs_loaded': 0,
            'unified_instruments_loaded': 0
        }

        # Warm up with most common symbols if not specified
        if symbols is None:
            symbols = ['AAPL', 'GOOGL', 'MSFT', 'TSLA', 'AMZN', 'META', 'NVDA', 'NFLX', 'TWTR', 'UBER']

        # Warm up vendor instruments
        for symbol in symbols:
            try:
                criteria = SearchCriteria(symbols=[symbol], limit=10)
                await self.list_vendor_instruments(criteria)
                warm_up_stats['vendor_instruments_loaded'] += 1
            except Exception as e:
                logger.warning(f"Failed to warm cache for vendor instrument {symbol}: {e}")

        # Warm up cross-references
        for symbol in symbols:
            try:
                criteria = SearchCriteria(symbols=[symbol], limit=10)
                await self.list_instrument_xrefs(criteria)
                warm_up_stats['instrument_xrefs_loaded'] += 1
            except Exception as e:
                logger.warning(f"Failed to warm cache for xrefs {symbol}: {e}")

        logger.info(f"Cache warm-up completed: {warm_up_stats}")
        return warm_up_stats

    async def clear_all_caches(self) -> Dict[str, int]:
        """Clear all caches for this service."""
        results = {}

        # Clear multi-layer cache
        results.update(await self.cache.clear())

        # Clear database query cache
        db_cleared = await self.db_cache.invalidate_cache_pattern("*")
        results['database_cache_cleared'] = db_cleared

        # Reset metrics
        self.performance_metrics = {
            'cache_hits': 0,
            'cache_misses': 0,
            'invalidations': 0,
            'total_requests': 0
        }

        logger.info(f"Cleared all caches: {results}")
        return results

    # === Health Check ===

    async def cache_health_check(self) -> Dict[str, Any]:
        """Check cache system health."""
        health_status = {
            'multi_layer_cache': 'unknown',
            'database_cache': 'unknown',
            'overall_status': 'unknown'
        }

        try:
            # Test multi-layer cache
            test_key = "health_check_test"
            test_value = "test_data"

            await self.cache.set(test_key, test_value, ttl=10)
            retrieved_value = await self.cache.get(test_key)
            await self.cache.delete(test_key)

            if retrieved_value == test_value:
                health_status['multi_layer_cache'] = 'healthy'
            else:
                health_status['multi_layer_cache'] = 'unhealthy'

        except Exception as e:
            logger.error(f"Multi-layer cache health check failed: {e}")
            health_status['multi_layer_cache'] = 'unhealthy'

        try:
            # Test database cache
            db_health = await self.db_cache.health_check()
            health_status['database_cache'] = db_health['status']

        except Exception as e:
            logger.error(f"Database cache health check failed: {e}")
            health_status['database_cache'] = 'unhealthy'

        # Overall status
        if all(status == 'healthy' for status in [health_status['multi_layer_cache'], health_status['database_cache']]):
            health_status['overall_status'] = 'healthy'
        elif any(status == 'healthy' for status in [health_status['multi_layer_cache'], health_status['database_cache']]):
            health_status['overall_status'] = 'degraded'
        else:
            health_status['overall_status'] = 'unhealthy'

        return health_status

    # === Delegate remaining methods to base service ===

    async def delete_vendor_instrument(self, instrument_id: int) -> OperationResult:
        """Delete vendor instrument and invalidate caches."""
        # Get instrument details first for cache invalidation
        instrument = await self.get_vendor_instrument_by_id(instrument_id)

        result = await self.base_service.delete_vendor_instrument(instrument_id)

        if result.success and instrument:
            await self._invalidate_vendor_instrument_caches(instrument.vendor_name, instrument.vendor_symbol)
            await self.cache.delete(f"get_vendor_instrument_by_id:{instrument_id}")
            self.performance_metrics['invalidations'] += 1

        return result

    async def validate_instrument_data(self, instrument: VendorInstrumentDTO) -> OperationResult:
        """Delegate validation to base service (no caching needed)."""
        return await self.base_service.validate_instrument_data(instrument)

    async def get_instrument_metadata(self) -> Dict[str, Any]:
        """Get instrument metadata with light caching."""
        cache_key = "instrument_metadata"

        cached_result = await self.cache.get(cache_key)
        if cached_result is not None:
            return cached_result

        result = await self.base_service.get_instrument_metadata()
        await self.cache.set(cache_key, result, ttl=1800)  # 30 minutes

        return result

    async def health_check(self) -> Dict[str, Any]:
        """Enhanced health check including cache status."""
        base_health = await self.base_service.health_check()
        cache_health = await self.cache_health_check()
        cache_stats = await self.get_cache_statistics()

        return {
            'base_service': base_health,
            'cache_system': cache_health,
            'cache_statistics': cache_stats
        }