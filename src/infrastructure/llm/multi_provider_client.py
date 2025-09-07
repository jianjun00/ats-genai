"""
Multi-Provider LLM Client Infrastructure

This module provides a unified interface for accessing multiple LLM providers
with automatic failover, rate limiting, cost optimization, and performance monitoring.

Supports: OpenAI (GPT-4o), Anthropic (Claude), Google (Gemini), Hugging Face models
"""

import logging
import asyncio
import aiohttp
import json
import time
from typing import Dict, List, Optional, Any, Union
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
import hashlib
import redis.asyncio as redis

from core.platform.config.environment import Environment
from core.security.exceptions.custom_exceptions import ATSBaseException


class LLMProvider(Enum):
    OPENAI = "openai"
    ANTHROPIC = "anthropic"
    GOOGLE = "google"
    HUGGINGFACE = "huggingface"


class LLMModelType(Enum):
    TEXT_GENERATION = "text_generation"
    EMBEDDING = "embedding"
    SENTIMENT = "sentiment"
    NER = "ner"


@dataclass
class LLMRequest:
    """LLM request configuration"""
    prompt: str
    model: str
    provider: LLMProvider
    model_type: LLMModelType = LLMModelType.TEXT_GENERATION
    temperature: float = 0.1
    max_tokens: int = 2000
    top_p: float = 0.9
    frequency_penalty: float = 0.0
    presence_penalty: float = 0.0
    timeout: int = 30
    cache_ttl: int = 3600  # 1 hour cache
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class LLMResponse:
    """LLM response with metadata"""
    content: str
    provider: LLMProvider
    model: str
    tokens_used: int
    cost: float
    latency_ms: float
    cached: bool = False
    timestamp: datetime = field(default_factory=datetime.now)
    metadata: Dict[str, Any] = field(default_factory=dict)


class LLMProviderError(ATSBaseException):
    """LLM provider specific error"""
    pass


class RateLimiter:
    """Rate limiter for LLM API calls"""

    def __init__(self, requests_per_minute: int, tokens_per_minute: int):
        self.requests_per_minute = requests_per_minute
        self.tokens_per_minute = tokens_per_minute
        self.request_times = []
        self.token_usage = []
        self._lock = asyncio.Lock()

    async def can_proceed(self, estimated_tokens: int) -> bool:
        """Check if request can proceed within rate limits"""
        async with self._lock:
            now = time.time()
            minute_ago = now - 60

            # Clean old entries
            self.request_times = [t for t in self.request_times if t > minute_ago]
            self.token_usage = [(t, tokens) for t, tokens in self.token_usage if t > minute_ago]

            # Check request rate limit
            if len(self.request_times) >= self.requests_per_minute:
                return False

            # Check token rate limit
            current_token_usage = sum(tokens for _, tokens in self.token_usage)
            if current_token_usage + estimated_tokens > self.tokens_per_minute:
                return False

            return True

    async def record_request(self, tokens_used: int):
        """Record a completed request"""
        async with self._lock:
            now = time.time()
            self.request_times.append(now)
            self.token_usage.append((now, tokens_used))


class CircuitBreaker:
    """Circuit breaker for LLM provider reliability"""

    def __init__(self, failure_threshold: int = 5, recovery_timeout: int = 300):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.failure_count = 0
        self.last_failure_time = None
        self.state = "closed"  # closed, open, half_open
        self._lock = asyncio.Lock()

    async def can_proceed(self) -> bool:
        """Check if requests can proceed through circuit breaker"""
        async with self._lock:
            if self.state == "closed":
                return True

            if self.state == "open":
                if self.last_failure_time and (
                    time.time() - self.last_failure_time > self.recovery_timeout
                ):
                    self.state = "half_open"
                    return True
                return False

            if self.state == "half_open":
                return True

            return False

    async def record_success(self):
        """Record successful request"""
        async with self._lock:
            self.failure_count = 0
            self.state = "closed"

    async def record_failure(self):
        """Record failed request"""
        async with self._lock:
            self.failure_count += 1
            self.last_failure_time = time.time()

            if self.failure_count >= self.failure_threshold:
                self.state = "open"


class OpenAIProvider:
    """OpenAI API provider implementation"""

    def __init__(self, api_key: str, base_url: str = "https://api.openai.com/v1"):
        self.api_key = api_key
        self.base_url = base_url
        self.session = None

        # Model pricing (per 1K tokens)
        self.pricing = {
            "gpt-4o": {"input": 0.005, "output": 0.015},
            "gpt-4o-mini": {"input": 0.00015, "output": 0.0006},
            "gpt-3.5-turbo": {"input": 0.0005, "output": 0.0015}
        }

    async def initialize(self):
        """Initialize HTTP session"""
        if not self.session:
            self.session = aiohttp.ClientSession(
                headers={
                    "Authorization": f"Bearer {self.api_key}",
                    "Content-Type": "application/json"
                },
                timeout=aiohttp.ClientTimeout(total=60)
            )

    async def complete(self, request: LLMRequest) -> LLMResponse:
        """Complete text generation request"""
        await self.initialize()

        start_time = time.time()

        payload = {
            "model": request.model,
            "messages": [{"role": "user", "content": request.prompt}],
            "temperature": request.temperature,
            "max_tokens": request.max_tokens,
            "top_p": request.top_p,
            "frequency_penalty": request.frequency_penalty,
            "presence_penalty": request.presence_penalty
        }

        try:
            async with self.session.post(
                f"{self.base_url}/chat/completions",
                json=payload,
                timeout=aiohttp.ClientTimeout(total=request.timeout)
            ) as response:

                if response.status != 200:
                    error_text = await response.text()
                    raise LLMProviderError(f"OpenAI API error {response.status}: {error_text}")

                data = await response.json()

                # Extract response content
                content = data["choices"][0]["message"]["content"]
                tokens_used = data["usage"]["total_tokens"]

                # Calculate cost
                input_tokens = data["usage"]["prompt_tokens"]
                output_tokens = data["usage"]["completion_tokens"]
                cost = self._calculate_cost(request.model, input_tokens, output_tokens)

                latency_ms = (time.time() - start_time) * 1000

                return LLMResponse(
                    content=content,
                    provider=LLMProvider.OPENAI,
                    model=request.model,
                    tokens_used=tokens_used,
                    cost=cost,
                    latency_ms=latency_ms,
                    metadata={
                        "input_tokens": input_tokens,
                        "output_tokens": output_tokens,
                        "finish_reason": data["choices"][0]["finish_reason"]
                    }
                )

        except asyncio.TimeoutError:
            raise LLMProviderError(f"OpenAI request timeout after {request.timeout}s")
        except Exception as e:
            raise LLMProviderError(f"OpenAI request failed: {str(e)}")

    def _calculate_cost(self, model: str, input_tokens: int, output_tokens: int) -> float:
        """Calculate request cost based on token usage"""
        if model not in self.pricing:
            return 0.0

        input_cost = (input_tokens / 1000) * self.pricing[model]["input"]
        output_cost = (output_tokens / 1000) * self.pricing[model]["output"]

        return input_cost + output_cost

    async def close(self):
        """Close HTTP session"""
        if self.session:
            await self.session.close()


class AnthropicProvider:
    """Anthropic Claude API provider implementation"""

    def __init__(self, api_key: str, base_url: str = "https://api.anthropic.com"):
        self.api_key = api_key
        self.base_url = base_url
        self.session = None

        # Model pricing (per 1K tokens)
        self.pricing = {
            "claude-3-5-sonnet-20241022": {"input": 0.003, "output": 0.015},
            "claude-3-haiku-20240307": {"input": 0.00025, "output": 0.00125}
        }

    async def initialize(self):
        """Initialize HTTP session"""
        if not self.session:
            self.session = aiohttp.ClientSession(
                headers={
                    "x-api-key": self.api_key,
                    "Content-Type": "application/json",
                    "anthropic-version": "2023-06-01"
                },
                timeout=aiohttp.ClientTimeout(total=60)
            )

    async def complete(self, request: LLMRequest) -> LLMResponse:
        """Complete text generation request"""
        await self.initialize()

        start_time = time.time()

        payload = {
            "model": request.model,
            "max_tokens": request.max_tokens,
            "messages": [{"role": "user", "content": request.prompt}],
            "temperature": request.temperature,
            "top_p": request.top_p
        }

        try:
            async with self.session.post(
                f"{self.base_url}/v1/messages",
                json=payload,
                timeout=aiohttp.ClientTimeout(total=request.timeout)
            ) as response:

                if response.status != 200:
                    error_text = await response.text()
                    raise LLMProviderError(f"Anthropic API error {response.status}: {error_text}")

                data = await response.json()

                # Extract response content
                content = data["content"][0]["text"]
                input_tokens = data["usage"]["input_tokens"]
                output_tokens = data["usage"]["output_tokens"]
                tokens_used = input_tokens + output_tokens

                # Calculate cost
                cost = self._calculate_cost(request.model, input_tokens, output_tokens)

                latency_ms = (time.time() - start_time) * 1000

                return LLMResponse(
                    content=content,
                    provider=LLMProvider.ANTHROPIC,
                    model=request.model,
                    tokens_used=tokens_used,
                    cost=cost,
                    latency_ms=latency_ms,
                    metadata={
                        "input_tokens": input_tokens,
                        "output_tokens": output_tokens,
                        "stop_reason": data.get("stop_reason")
                    }
                )

        except asyncio.TimeoutError:
            raise LLMProviderError(f"Anthropic request timeout after {request.timeout}s")
        except Exception as e:
            raise LLMProviderError(f"Anthropic request failed: {str(e)}")

    def _calculate_cost(self, model: str, input_tokens: int, output_tokens: int) -> float:
        """Calculate request cost based on token usage"""
        if model not in self.pricing:
            return 0.0

        input_cost = (input_tokens / 1000) * self.pricing[model]["input"]
        output_cost = (output_tokens / 1000) * self.pricing[model]["output"]

        return input_cost + output_cost

    async def close(self):
        """Close HTTP session"""
        if self.session:
            await self.session.close()


class GoogleProvider:
    """Google Gemini API provider implementation"""

    def __init__(self, api_key: str, base_url: str = "https://generativelanguage.googleapis.com"):
        self.api_key = api_key
        self.base_url = base_url
        self.session = None

        # Model pricing (per 1K tokens) - approximate
        self.pricing = {
            "gemini-1.5-pro": {"input": 0.0035, "output": 0.0105},
            "gemini-1.5-flash": {"input": 0.00035, "output": 0.00105}
        }

    async def initialize(self):
        """Initialize HTTP session"""
        if not self.session:
            self.session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=60)
            )

    async def complete(self, request: LLMRequest) -> LLMResponse:
        """Complete text generation request"""
        await self.initialize()

        start_time = time.time()

        payload = {
            "contents": [{
                "parts": [{"text": request.prompt}]
            }],
            "generationConfig": {
                "temperature": request.temperature,
                "maxOutputTokens": request.max_tokens,
                "topP": request.top_p
            }
        }

        try:
            async with self.session.post(
                f"{self.base_url}/v1beta/models/{request.model}:generateContent?key={self.api_key}",
                json=payload,
                timeout=aiohttp.ClientTimeout(total=request.timeout)
            ) as response:

                if response.status != 200:
                    error_text = await response.text()
                    raise LLMProviderError(f"Google API error {response.status}: {error_text}")

                data = await response.json()

                if "candidates" not in data or not data["candidates"]:
                    raise LLMProviderError("No candidates in Google response")

                # Extract response content
                content = data["candidates"][0]["content"]["parts"][0]["text"]

                # Estimate tokens (Google doesn't always provide exact counts)
                estimated_tokens = len(request.prompt.split()) + len(content.split())
                cost = self._estimate_cost(request.model, estimated_tokens)

                latency_ms = (time.time() - start_time) * 1000

                return LLMResponse(
                    content=content,
                    provider=LLMProvider.GOOGLE,
                    model=request.model,
                    tokens_used=estimated_tokens,
                    cost=cost,
                    latency_ms=latency_ms,
                    metadata={
                        "finish_reason": data["candidates"][0].get("finishReason"),
                        "estimated_tokens": True
                    }
                )

        except asyncio.TimeoutError:
            raise LLMProviderError(f"Google request timeout after {request.timeout}s")
        except Exception as e:
            raise LLMProviderError(f"Google request failed: {str(e)}")

    def _estimate_cost(self, model: str, estimated_tokens: int) -> float:
        """Estimate request cost"""
        if model not in self.pricing:
            return 0.0

        # Rough estimation assuming 70% input, 30% output
        input_tokens = int(estimated_tokens * 0.7)
        output_tokens = int(estimated_tokens * 0.3)

        input_cost = (input_tokens / 1000) * self.pricing[model]["input"]
        output_cost = (output_tokens / 1000) * self.pricing[model]["output"]

        return input_cost + output_cost

    async def close(self):
        """Close HTTP session"""
        if self.session:
            await self.session.close()


class ResponseCache:
    """Redis-based response caching"""

    def __init__(self, redis_url: str = "redis://localhost:6379"):
        self.redis_url = redis_url
        self.redis_client = None

    async def initialize(self):
        """Initialize Redis connection"""
        if not self.redis_client:
            self.redis_client = redis.from_url(self.redis_url, decode_responses=True)

    def _generate_cache_key(self, request: LLMRequest) -> str:
        """Generate cache key for request"""
        # Create hash of request parameters
        request_str = f"{request.provider.value}:{request.model}:{request.prompt}:{request.temperature}:{request.max_tokens}"
        return f"llm_cache:{hashlib.md5(request_str.encode()).hexdigest()}"

    async def get(self, request: LLMRequest) -> Optional[LLMResponse]:
        """Get cached response"""
        await self.initialize()

        try:
            cache_key = self._generate_cache_key(request)
            cached_data = await self.redis_client.get(cache_key)

            if cached_data:
                data = json.loads(cached_data)
                response = LLMResponse(**data)
                response.cached = True
                return response

            return None

        except Exception as e:
            logging.warning(f"Cache get failed: {e}")
            return None

    async def set(self, request: LLMRequest, response: LLMResponse):
        """Cache response"""
        await self.initialize()

        try:
            cache_key = self._generate_cache_key(request)

            # Convert response to dict for JSON serialization
            response_dict = {
                'content': response.content,
                'provider': response.provider.value,
                'model': response.model,
                'tokens_used': response.tokens_used,
                'cost': response.cost,
                'latency_ms': response.latency_ms,
                'timestamp': response.timestamp.isoformat(),
                'metadata': response.metadata
            }

            await self.redis_client.setex(
                cache_key,
                request.cache_ttl,
                json.dumps(response_dict, default=str)
            )

        except Exception as e:
            logging.warning(f"Cache set failed: {e}")

    async def close(self):
        """Close Redis connection"""
        if self.redis_client:
            await self.redis_client.close()


class MultiProviderLLMClient:
    """
    Unified multi-provider LLM client with failover, rate limiting, and optimization
    """

    def __init__(self, env: Environment):
        self.env = env

        # Initialize providers
        self.providers = {}
        self._initialize_providers()

        # Rate limiters for each provider
        self.rate_limiters = {
            LLMProvider.OPENAI: RateLimiter(requests_per_minute=500, tokens_per_minute=80000),
            LLMProvider.ANTHROPIC: RateLimiter(requests_per_minute=100, tokens_per_minute=40000),
            LLMProvider.GOOGLE: RateLimiter(requests_per_minute=100, tokens_per_minute=32000),
            LLMProvider.HUGGINGFACE: RateLimiter(requests_per_minute=60, tokens_per_minute=20000)
        }

        # Circuit breakers for each provider
        self.circuit_breakers = {
            provider: CircuitBreaker() for provider in LLMProvider
        }

        # Response cache
        self.cache = ResponseCache()

        # Performance tracking
        self.stats = {
            'requests_total': 0,
            'requests_cached': 0,
            'requests_failed': 0,
            'total_cost': 0.0,
            'total_tokens': 0,
            'provider_stats': {provider.value: {'requests': 0, 'failures': 0, 'cost': 0.0}
                             for provider in LLMProvider}
        }

        # Model routing configuration
        self.model_routing = {
            'gpt-4o': LLMProvider.OPENAI,
            'gpt-4o-mini': LLMProvider.OPENAI,
            'gpt-3.5-turbo': LLMProvider.OPENAI,
            'claude-3-5-sonnet-20241022': LLMProvider.ANTHROPIC,
            'claude-3-haiku-20240307': LLMProvider.ANTHROPIC,
            'gemini-1.5-pro': LLMProvider.GOOGLE,
            'gemini-1.5-flash': LLMProvider.GOOGLE
        }

    def _initialize_providers(self):
        """Initialize all LLM providers"""
        # OpenAI
        openai_key = self.env.get('OPENAI_API_KEY')
        if openai_key:
            self.providers[LLMProvider.OPENAI] = OpenAIProvider(openai_key)

        # Anthropic
        anthropic_key = self.env.get('ANTHROPIC_API_KEY')
        if anthropic_key:
            self.providers[LLMProvider.ANTHROPIC] = AnthropicProvider(anthropic_key)

        # Google
        google_key = self.env.get('GOOGLE_API_KEY')
        if google_key:
            self.providers[LLMProvider.GOOGLE] = GoogleProvider(google_key)

        logging.info(f"Initialized {len(self.providers)} LLM providers")

    async def complete(self,
                      model: str,
                      prompt: str,
                      temperature: float = 0.1,
                      max_tokens: int = 2000,
                      **kwargs) -> LLMResponse:
        """
        Complete text generation request with automatic provider routing and failover
        """
        # Determine provider for model
        provider = self.model_routing.get(model)
        if not provider:
            raise LLMProviderError(f"No provider configured for model: {model}")

        # Create request
        request = LLMRequest(
            prompt=prompt,
            model=model,
            provider=provider,
            temperature=temperature,
            max_tokens=max_tokens,
            **kwargs
        )

        # Try cache first
        cached_response = await self.cache.get(request)
        if cached_response:
            self.stats['requests_cached'] += 1
            return cached_response

        self.stats['requests_total'] += 1

        # Try primary provider first, then failover
        providers_to_try = [provider]

        # Add fallback providers for critical requests
        if model.startswith('gpt-4') and LLMProvider.ANTHROPIC in self.providers:
            providers_to_try.append(LLMProvider.ANTHROPIC)
        elif model.startswith('claude') and LLMProvider.OPENAI in self.providers:
            providers_to_try.append(LLMProvider.OPENAI)

        last_error = None

        for provider_to_use in providers_to_try:
            try:
                # Check circuit breaker
                if not await self.circuit_breakers[provider_to_use].can_proceed():
                    continue

                # Check rate limits
                estimated_tokens = len(prompt.split()) * 2  # Rough estimate
                if not await self.rate_limiters[provider_to_use].can_proceed(estimated_tokens):
                    await asyncio.sleep(1)  # Brief wait before trying next provider
                    continue

                # Make request
                provider_instance = self.providers[provider_to_use]

                # Update request with correct provider
                request.provider = provider_to_use

                # Get response
                response = await provider_instance.complete(request)

                # Record success
                await self.circuit_breakers[provider_to_use].record_success()
                await self.rate_limiters[provider_to_use].record_request(response.tokens_used)

                # Update stats
                self.stats['total_cost'] += response.cost
                self.stats['total_tokens'] += response.tokens_used
                self.stats['provider_stats'][provider_to_use.value]['requests'] += 1
                self.stats['provider_stats'][provider_to_use.value]['cost'] += response.cost

                # Cache response
                await self.cache.set(request, response)

                return response

            except Exception as e:
                last_error = e
                logging.warning(f"Provider {provider_to_use.value} failed: {e}")

                # Record failure
                await self.circuit_breakers[provider_to_use].record_failure()
                self.stats['provider_stats'][provider_to_use.value]['failures'] += 1

                continue

        # All providers failed
        self.stats['requests_failed'] += 1
        raise LLMProviderError(f"All providers failed. Last error: {last_error}")

    async def complete_batch(self, requests: List[Dict[str, Any]]) -> List[LLMResponse]:
        """Complete multiple requests in parallel"""
        tasks = []
        for req in requests:
            task = asyncio.create_task(self.complete(**req))
            tasks.append(task)

        results = await asyncio.gather(*tasks, return_exceptions=True)

        responses = []
        for result in results:
            if isinstance(result, Exception):
                logging.error(f"Batch request failed: {result}")
                # Return error response
                responses.append(LLMResponse(
                    content=f"Error: {result}",
                    provider=LLMProvider.OPENAI,  # Default
                    model="error",
                    tokens_used=0,
                    cost=0.0,
                    latency_ms=0.0
                ))
            else:
                responses.append(result)

        return responses

    def get_stats(self) -> Dict[str, Any]:
        """Get performance statistics"""
        total_requests = max(self.stats['requests_total'], 1)

        return {
            'total_requests': self.stats['requests_total'],
            'cache_hit_rate': self.stats['requests_cached'] / total_requests,
            'error_rate': self.stats['requests_failed'] / total_requests,
            'total_cost': self.stats['total_cost'],
            'total_tokens': self.stats['total_tokens'],
            'avg_cost_per_request': self.stats['total_cost'] / total_requests,
            'provider_stats': self.stats['provider_stats'],
            'active_providers': list(self.providers.keys())
        }

    async def health_check(self) -> Dict[str, Any]:
        """Check health of all providers"""
        health_status = {}

        for provider, provider_instance in self.providers.items():
            try:
                # Simple test request
                test_request = LLMRequest(
                    prompt="Test",
                    model=list(self.model_routing.keys())[0],  # Use first available model
                    provider=provider,
                    max_tokens=5
                )

                start_time = time.time()
                await provider_instance.complete(test_request)
                latency = (time.time() - start_time) * 1000

                health_status[provider.value] = {
                    'status': 'healthy',
                    'latency_ms': latency,
                    'circuit_breaker_state': self.circuit_breakers[provider].state
                }

            except Exception as e:
                health_status[provider.value] = {
                    'status': 'unhealthy',
                    'error': str(e),
                    'circuit_breaker_state': self.circuit_breakers[provider].state
                }

        return health_status

    async def close(self):
        """Close all provider connections"""
        for provider_instance in self.providers.values():
            await provider_instance.close()

        await self.cache.close()


# Convenience function for creating client
async def create_llm_client(env: Environment) -> MultiProviderLLMClient:
    """Create and initialize LLM client"""
    client = MultiProviderLLMClient(env)
    await client.cache.initialize()

    for provider in client.providers.values():
        await provider.initialize()

    return client