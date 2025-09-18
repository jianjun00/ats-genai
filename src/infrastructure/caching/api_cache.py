"""
API Response Caching

Advanced HTTP response caching with intelligent invalidation,
conditional requests, and cache-aware middleware.
"""

import hashlib
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Dict, List, Optional, Union, Callable
import logging

from fastapi import Request, Response
from fastapi.middleware.base import BaseHTTPMiddleware

from .cache_manager import CacheBackend, MultiLayerCache

logger = logging.getLogger(__name__)


class CacheStrategy(Enum):
    """HTTP caching strategy enumeration."""
    CACHE_FIRST = "cache_first"          # Try cache first, then origin on miss
    NETWORK_FIRST = "network_first"      # Try network first, then cache on failure
    CACHE_ONLY = "cache_only"            # Only use cache, fail if not found
    NETWORK_ONLY = "network_only"        # Only use network, never cache
    STALE_WHILE_REVALIDATE = "swr"       # Return stale while updating in background


@dataclass
class CacheHeaders:
    """HTTP cache headers configuration."""
    cache_control: Optional[str] = None
    expires: Optional[datetime] = None
    etag: Optional[str] = None
    last_modified: Optional[datetime] = None
    vary: Optional[str] = None

    def to_dict(self) -> Dict[str, str]:
        """Convert to HTTP headers dictionary."""
        headers = {}

        if self.cache_control:
            headers["Cache-Control"] = self.cache_control

        if self.expires:
            headers["Expires"] = self.expires.strftime("%a, %d %b %Y %H:%M:%S GMT")

        if self.etag:
            headers["ETag"] = f'"{self.etag}"'

        if self.last_modified:
            headers["Last-Modified"] = self.last_modified.strftime("%a, %d %b %Y %H:%M:%S GMT")

        if self.vary:
            headers["Vary"] = self.vary

        return headers


@dataclass
class CachedResponse:
    """Cached HTTP response."""
    status_code: int
    headers: Dict[str, str]
    body: bytes
    content_type: str
    cached_at: datetime
    expires_at: Optional[datetime] = None
    etag: Optional[str] = None
    size_bytes: int = 0
    hit_count: int = 0

    def __post_init__(self):
        self.size_bytes = len(self.body)

    def is_expired(self) -> bool:
        """Check if cached response is expired."""
        if self.expires_at is None:
            return False
        return datetime.utcnow() > self.expires_at

    def is_stale(self, max_age_seconds: int) -> bool:
        """Check if cached response is stale."""
        age = datetime.utcnow() - self.cached_at
        return age.total_seconds() > max_age_seconds

    def touch(self):
        """Update hit count and access time."""
        self.hit_count += 1

    def to_response(self) -> Response:
        """Convert to FastAPI Response object."""
        headers = self.headers.copy()

        # Add cache-related headers
        age_seconds = int((datetime.utcnow() - self.cached_at).total_seconds())
        headers["Age"] = str(age_seconds)
        headers["X-Cache"] = "HIT"
        headers["X-Cache-Hits"] = str(self.hit_count)

        return Response(
            content=self.body,
            status_code=self.status_code,
            headers=headers,
            media_type=self.content_type
        )


class ResponseCacheConfig:
    """Configuration for API response caching."""

    def __init__(
        self,
        default_ttl: int = 3600,
        max_response_size: int = 1024 * 1024,  # 1MB
        cache_private_responses: bool = False,
        cache_error_responses: bool = False,
        vary_headers: Optional[List[str]] = None,
        ignore_query_params: Optional[List[str]] = None,
        cache_key_headers: Optional[List[str]] = None
    ):
        self.default_ttl = default_ttl
        self.max_response_size = max_response_size
        self.cache_private_responses = cache_private_responses
        self.cache_error_responses = cache_error_responses
        self.vary_headers = vary_headers or []
        self.ignore_query_params = ignore_query_params or []
        self.cache_key_headers = cache_key_headers or []


class APICacheManager:
    """Manages API response caching with intelligent invalidation."""

    def __init__(
        self,
        cache: Union[CacheBackend, MultiLayerCache],
        config: ResponseCacheConfig
    ):
        self.cache = cache
        self.config = config
        self.cache_rules: Dict[str, Dict[str, Any]] = {}

        # Statistics
        self.stats = {
            'hits': 0,
            'misses': 0,
            'stores': 0,
            'invalidations': 0,
            'bypasses': 0,
            'errors': 0
        }

    def add_cache_rule(
        self,
        path_pattern: str,
        methods: Optional[List[str]] = None,
        ttl: Optional[int] = None,
        strategy: CacheStrategy = CacheStrategy.CACHE_FIRST,
        vary_headers: Optional[List[str]] = None,
        invalidation_tags: Optional[List[str]] = None,
        condition: Optional[Callable] = None
    ):
        """Add caching rule for specific endpoints."""
        self.cache_rules[path_pattern] = {
            'methods': methods or ['GET', 'HEAD'],
            'ttl': ttl or self.config.default_ttl,
            'strategy': strategy,
            'vary_headers': vary_headers or [],
            'invalidation_tags': invalidation_tags or [],
            'condition': condition
        }

        logger.info(f"Added cache rule for {path_pattern}: {strategy.value}, TTL={ttl}")

    def generate_cache_key(self, request: Request) -> str:
        """Generate cache key for HTTP request."""
        key_parts = [
            request.method,
            request.url.path,
        ]

        # Add query parameters (excluding ignored ones)
        if request.query_params:
            filtered_params = {
                k: v for k, v in request.query_params.items()
                if k not in self.config.ignore_query_params
            }
            if filtered_params:
                sorted_params = sorted(filtered_params.items())
                query_string = '&'.join(f"{k}={v}" for k, v in sorted_params)
                key_parts.append(query_string)

        # Add relevant headers
        for header_name in self.config.cache_key_headers:
            header_value = request.headers.get(header_name)
            if header_value:
                key_parts.append(f"{header_name}:{header_value}")

        # Generate hash for long keys
        key_string = '|'.join(key_parts)
        if len(key_string) > 200:
            key_string = hashlib.sha256(key_string.encode()).hexdigest()

        return f"api_cache:{key_string}"

    async def get_cached_response(self, request: Request) -> Optional[CachedResponse]:
        """Get cached response for request."""
        try:
            cache_key = self.generate_cache_key(request)
            cached_data = await self.cache.get(cache_key)

            if cached_data is None:
                self.stats['misses'] += 1
                return None

            cached_response = CachedResponse(**cached_data)

            # Check if response is expired
            if cached_response.is_expired():
                await self.cache.delete(cache_key)
                self.stats['misses'] += 1
                return None

            # Update hit count
            cached_response.touch()
            self.stats['hits'] += 1

            logger.debug(f"Cache hit for {request.method} {request.url.path}")
            return cached_response

        # Let all cache access exceptions propagate - fail fast on cache infrastructure issues

    async def store_response(
        self,
        request: Request,
        response: Response,
        ttl: Optional[int] = None
    ) -> bool:
        """Store response in cache."""
        try:
            # Check if response should be cached
            if not self._should_cache_response(request, response):
                self.stats['bypasses'] += 1
                return False

            # Check response size
            response_body = getattr(response, 'body', b'')
            if len(response_body) > self.config.max_response_size:
                logger.debug(f"Response too large to cache: {len(response_body)} bytes")
                self.stats['bypasses'] += 1
                return False

            cache_key = self.generate_cache_key(request)
            ttl = ttl or self.config.default_ttl

            # Generate ETag
            etag = hashlib.md5(response_body).hexdigest()

            # Create cached response
            cached_response = CachedResponse(
                status_code=response.status_code,
                headers=dict(response.headers),
                body=response_body,
                content_type=response.media_type or 'application/json',
                cached_at=datetime.utcnow(),
                expires_at=datetime.utcnow() + timedelta(seconds=ttl),
                etag=etag
            )

            # Store in cache
            success = await self.cache.set(
                cache_key,
                cached_response.__dict__,
                ttl
            )

            if success:
                self.stats['stores'] += 1
                logger.debug(f"Cached response for {request.method} {request.url.path}")

            return success

        except Exception as e:
            logger.error(f"Error storing response in cache: {e}")
            self.stats['errors'] += 1
            return False

    def _should_cache_response(self, request: Request, response: Response) -> bool:
        """Determine if response should be cached."""
        # Only cache successful responses by default
        if not self.config.cache_error_responses and response.status_code >= 400:
            return False

        # Check cache-control headers
        cache_control = response.headers.get('cache-control', '').lower()
        if 'no-cache' in cache_control or 'no-store' in cache_control:
            return False

        # Check if it's a private response
        if not self.config.cache_private_responses and 'private' in cache_control:
            return False

        return True

    def _find_matching_rule(self, request: Request) -> Optional[Dict[str, Any]]:
        """Find matching cache rule for request."""
        for pattern, rule in self.cache_rules.items():
            if request.method in rule['methods']:
                # Simple pattern matching (can be enhanced with regex)
                if pattern in request.url.path or pattern == '*':
                    # Check condition if provided
                    if rule.get('condition') and not rule['condition'](request):
                        continue
                    return rule
        return None

    async def invalidate_by_tags(self, tags: List[str]) -> int:
        """Invalidate cache entries by tags."""
        total_invalidated = 0

        for tag in tags:
            try:
                count = await self.cache.clear(f"*{tag}*")
                total_invalidated += count
                self.stats['invalidations'] += count
                logger.info(f"Invalidated {count} cache entries for tag: {tag}")
            except Exception as e:
                logger.error(f"Error invalidating cache by tag {tag}: {e}")

        return total_invalidated

    async def invalidate_by_pattern(self, pattern: str) -> int:
        """Invalidate cache entries matching pattern."""
        try:
            count = await self.cache.clear(pattern)
            self.stats['invalidations'] += count
            logger.info(f"Invalidated {count} cache entries for pattern: {pattern}")
            return count
        # Let all cache invalidation exceptions propagate - fail fast on cache operation errors

    def get_stats(self) -> Dict[str, Any]:
        """Get cache statistics."""
        total_requests = self.stats['hits'] + self.stats['misses']
        hit_rate = (self.stats['hits'] / total_requests * 100) if total_requests > 0 else 0

        return {
            **self.stats,
            'total_requests': total_requests,
            'hit_rate_percent': round(hit_rate, 2),
            'cache_rules_count': len(self.cache_rules)
        }


class APICacheMiddleware(BaseHTTPMiddleware):
    """FastAPI middleware for automatic response caching."""

    def __init__(self, app, cache_manager: APICacheManager):
        super().__init__(app)
        self.cache_manager = cache_manager

    async def dispatch(self, request: Request, call_next):
        """Process request through cache layer."""
        start_time = time.time()

        # Only cache GET and HEAD requests by default
        if request.method not in ['GET', 'HEAD']:
            response = await call_next(request)
            return self._add_cache_headers(response, cache_status="BYPASS")

        # Check for cache-control headers in request
        if 'no-cache' in request.headers.get('cache-control', '').lower():
            response = await call_next(request)
            return self._add_cache_headers(response, cache_status="BYPASS")

        # Try to get cached response
        cached_response = await self.cache_manager.get_cached_response(request)

        if cached_response:
            # Handle conditional requests
            if self._handle_conditional_request(request, cached_response):
                # Return 304 Not Modified
                return Response(
                    status_code=304,
                    headers={"ETag": f'"{cached_response.etag}"'}
                )

            # Return cached response
            response = cached_response.to_response()
            processing_time = (time.time() - start_time) * 1000
            response.headers["X-Processing-Time-Ms"] = f"{processing_time:.2f}"
            return response

        # Cache miss - call next middleware/endpoint
        response = await call_next(request)

        # Store response in cache
        await self.cache_manager.store_response(request, response)

        # Add cache headers
        response = self._add_cache_headers(response, cache_status="MISS")

        processing_time = (time.time() - start_time) * 1000
        response.headers["X-Processing-Time-Ms"] = f"{processing_time:.2f}"

        return response

    def _handle_conditional_request(self, request: Request, cached_response: CachedResponse) -> bool:
        """Handle If-None-Match and If-Modified-Since headers."""
        # Handle If-None-Match (ETag)
        if_none_match = request.headers.get('if-none-match')
        if if_none_match and cached_response.etag:
            # Remove quotes from ETag
            client_etag = if_none_match.strip('"')
            if client_etag == cached_response.etag:
                return True

        # Handle If-Modified-Since
        if_modified_since = request.headers.get('if-modified-since')
        if if_modified_since:
            try:
                client_date = datetime.strptime(
                    if_modified_since,
                    "%a, %d %b %Y %H:%M:%S GMT"
                )
                if cached_response.cached_at <= client_date:
                    return True
            except ValueError:
                pass

        return False

    def _add_cache_headers(self, response: Response, cache_status: str) -> Response:
        """Add cache-related headers to response."""
        response.headers["X-Cache"] = cache_status
        response.headers["X-Cache-Date"] = datetime.utcnow().strftime("%a, %d %b %Y %H:%M:%S GMT")

        return response


# Decorator for endpoint-specific caching
def cache_response(
    ttl: int = 3600,
    strategy: CacheStrategy = CacheStrategy.CACHE_FIRST,
    vary_headers: Optional[List[str]] = None,
    invalidation_tags: Optional[List[str]] = None,
    condition: Optional[Callable] = None
):
    """Decorator for caching API endpoint responses."""
    def decorator(func):
        # Add cache metadata to function
        func._cache_config = {
            'ttl': ttl,
            'strategy': strategy,
            'vary_headers': vary_headers or [],
            'invalidation_tags': invalidation_tags or [],
            'condition': condition
        }
        return func
    return decorator


class SmartCacheInvalidator:
    """Intelligent cache invalidation based on data changes."""

    def __init__(self, cache_manager: APICacheManager):
        self.cache_manager = cache_manager
        self.entity_patterns: Dict[str, List[str]] = {}

    def register_entity_patterns(self, entity_type: str, cache_patterns: List[str]):
        """Register cache patterns for entity type."""
        self.entity_patterns[entity_type] = cache_patterns
        logger.info(f"Registered {len(cache_patterns)} cache patterns for {entity_type}")

    async def invalidate_entity(self, entity_type: str, entity_id: Optional[str] = None) -> int:
        """Invalidate cache for specific entity."""
        if entity_type not in self.entity_patterns:
            logger.warning(f"No cache patterns registered for entity type: {entity_type}")
            return 0

        total_invalidated = 0
        patterns = self.entity_patterns[entity_type]

        for pattern in patterns:
            if entity_id:
                # Replace {id} placeholder with actual ID
                specific_pattern = pattern.replace('{id}', str(entity_id))
            else:
                # Use pattern as-is for broader invalidation
                specific_pattern = pattern

            count = await self.cache_manager.invalidate_by_pattern(specific_pattern)
            total_invalidated += count

        logger.info(f"Invalidated {total_invalidated} cache entries for {entity_type}:{entity_id}")
        return total_invalidated


# Global cache managers
_api_cache_managers: Dict[str, APICacheManager] = {}


def get_api_cache_manager(name: str = "default") -> Optional[APICacheManager]:
    """Get named API cache manager."""
    return _api_cache_managers.get(name)


def register_api_cache_manager(name: str, cache_manager: APICacheManager):
    """Register named API cache manager."""
    _api_cache_managers[name] = cache_manager