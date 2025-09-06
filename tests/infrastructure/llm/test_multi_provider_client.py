#!/usr/bin/env python3
"""
Comprehensive tests for Multi-Provider LLM Client

This test suite covers:
- Provider initialization and configuration
- Circuit breaker functionality
- Rate limiting mechanisms
- Failover and fallback behavior
- Cost tracking and monitoring
- Performance metrics
- Error handling and recovery
"""

import pytest
import asyncio
import time
from unittest.mock import AsyncMock, MagicMock, patch
from datetime import datetime, timedelta

import aiohttp
import redis.asyncio as redis

from infrastructure.llm.multi_provider_client import (
    MultiProviderLLMClient, 
    LLMProvider,
    OpenAIProvider,
    AnthropicProvider,
    GoogleProvider,
    LLMResponse,
    CircuitBreakerError,
    RateLimitError
)


class TestLLMProviderBase:
    """Tests for the base LLM provider functionality."""
    
    @pytest.fixture
    def mock_session(self):
        """Mock aiohttp session."""
        session = AsyncMock()
        return session
    
    @pytest.fixture
    def openai_config(self):
        """OpenAI provider configuration."""
        return {
            'api_key': 'test_openai_key',
            'model': 'gpt-4o-mini',
            'max_tokens': 1000,
            'temperature': 0.1
        }
    
    @pytest.fixture
    def anthropic_config(self):
        """Anthropic provider configuration."""
        return {
            'api_key': 'test_anthropic_key', 
            'model': 'claude-3-haiku-20240307',
            'max_tokens': 1000,
            'temperature': 0.1
        }
    
    def test_openai_provider_initialization(self, openai_config):
        """Test OpenAI provider initialization."""
        provider = OpenAIProvider(openai_config)
        
        assert provider.name == 'openai'
        assert provider.api_key == 'test_openai_key'
        assert provider.model == 'gpt-4o-mini'
        assert provider.max_tokens == 1000
        assert provider.temperature == 0.1
        assert provider.base_url == 'https://api.openai.com/v1/chat/completions'
    
    def test_anthropic_provider_initialization(self, anthropic_config):
        """Test Anthropic provider initialization."""
        provider = AnthropicProvider(anthropic_config)
        
        assert provider.name == 'anthropic'
        assert provider.api_key == 'test_anthropic_key'
        assert provider.model == 'claude-3-haiku-20240307'
        assert provider.max_tokens == 1000
        assert provider.temperature == 0.1
        assert provider.base_url == 'https://api.anthropic.com/v1/messages'
    
    @pytest.mark.asyncio
    async def test_openai_request_formation(self, mock_session, openai_config):
        """Test OpenAI request payload formation."""
        provider = OpenAIProvider(openai_config)
        provider.session = mock_session
        
        # Mock successful response
        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.json = AsyncMock(return_value={
            'choices': [{'message': {'content': 'Test response'}}],
            'usage': {'total_tokens': 50}
        })
        mock_session.post.return_value.__aenter__.return_value = mock_response
        
        response = await provider.generate_response(
            prompt="Test prompt",
            system_prompt="Test system",
            temperature=0.2,
            max_tokens=500
        )
        
        # Verify request was made with correct payload
        mock_session.post.assert_called_once()
        call_args = mock_session.post.call_args
        
        assert call_args[0][0] == provider.base_url
        assert 'json' in call_args[1]
        
        payload = call_args[1]['json']
        assert payload['model'] == 'gpt-4o-mini'
        assert payload['temperature'] == 0.2
        assert payload['max_tokens'] == 500
        assert len(payload['messages']) == 2
        assert payload['messages'][0]['role'] == 'system'
        assert payload['messages'][1]['role'] == 'user'
        
        assert response.content == 'Test response'
        assert response.model_used == 'gpt-4o-mini'
        assert response.provider == 'openai'
        assert response.tokens_used == 50
    
    @pytest.mark.asyncio
    async def test_anthropic_request_formation(self, mock_session, anthropic_config):
        """Test Anthropic request payload formation."""
        provider = AnthropicProvider(anthropic_config)
        provider.session = mock_session
        
        # Mock successful response
        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.json = AsyncMock(return_value={
            'content': [{'text': 'Test response'}],
            'usage': {'output_tokens': 30}
        })
        mock_session.post.return_value.__aenter__.return_value = mock_response
        
        response = await provider.generate_response(
            prompt="Test prompt",
            system_prompt="Test system",
            temperature=0.3,
            max_tokens=600
        )
        
        # Verify request was made with correct payload
        call_args = mock_session.post.call_args
        payload = call_args[1]['json']
        
        assert payload['model'] == 'claude-3-haiku-20240307'
        assert payload['temperature'] == 0.3
        assert payload['max_tokens'] == 600
        assert payload['system'] == 'Test system'
        assert len(payload['messages']) == 1
        assert payload['messages'][0]['role'] == 'user'
        
        assert response.content == 'Test response'
        assert response.model_used == 'claude-3-haiku-20240307'
        assert response.provider == 'anthropic'
        assert response.tokens_used == 30
    
    @pytest.mark.asyncio
    async def test_provider_error_handling(self, mock_session, openai_config):
        """Test provider error handling for various HTTP status codes."""
        provider = OpenAIProvider(openai_config)
        provider.session = mock_session
        
        # Test rate limit error (429)
        mock_response = AsyncMock()
        mock_response.status = 429
        mock_response.json = AsyncMock(return_value={'error': {'message': 'Rate limit exceeded'}})
        mock_session.post.return_value.__aenter__.return_value = mock_response
        
        with pytest.raises(RateLimitError):
            await provider.generate_response("Test prompt")
        
        # Test general API error (500)
        mock_response.status = 500
        mock_response.json = AsyncMock(return_value={'error': {'message': 'Internal server error'}})
        
        with pytest.raises(Exception) as exc_info:
            await provider.generate_response("Test prompt")
        assert "HTTP 500" in str(exc_info.value)
    
    @pytest.mark.asyncio
    async def test_provider_timeout_handling(self, mock_session, openai_config):
        """Test provider timeout handling."""
        provider = OpenAIProvider(openai_config)
        provider.session = mock_session
        
        # Mock timeout
        mock_session.post.side_effect = asyncio.TimeoutError()
        
        with pytest.raises(Exception) as exc_info:
            await provider.generate_response("Test prompt")
        assert "timeout" in str(exc_info.value).lower()


class TestCircuitBreaker:
    """Tests for circuit breaker functionality."""
    
    @pytest.fixture
    def provider_config(self):
        """Test provider configuration."""
        return {
            'api_key': 'test_key',
            'model': 'test_model',
            'max_tokens': 1000
        }
    
    @pytest.mark.asyncio
    async def test_circuit_breaker_closed_state(self, provider_config):
        """Test circuit breaker in closed state (normal operation)."""
        provider = OpenAIProvider(provider_config)
        
        # Initial state should be closed
        assert provider.circuit_breaker['state'] == 'closed'
        assert provider.circuit_breaker['failures'] == 0
        
        # Should allow requests
        assert provider._can_make_request()
    
    @pytest.mark.asyncio
    async def test_circuit_breaker_failure_tracking(self, provider_config):
        """Test circuit breaker failure tracking."""
        provider = OpenAIProvider(provider_config)
        
        # Record failures
        for i in range(3):
            provider._record_failure(Exception(f"Test error {i}"))
        
        assert provider.circuit_breaker['failures'] == 3
        assert provider.circuit_breaker['state'] == 'closed'  # Still closed
        
        # Record enough failures to open circuit
        for i in range(2):  # Total will be 5, which should trigger opening
            provider._record_failure(Exception(f"Test error {i+3}"))
        
        assert provider.circuit_breaker['state'] == 'open'
        assert not provider._can_make_request()
    
    @pytest.mark.asyncio
    async def test_circuit_breaker_half_open_transition(self, provider_config):
        """Test circuit breaker transition to half-open state."""
        provider = OpenAIProvider(provider_config)
        
        # Force circuit to open
        provider.circuit_breaker['state'] = 'open'
        provider.circuit_breaker['last_failure'] = datetime.now() - timedelta(minutes=6)
        
        # After timeout, should transition to half-open
        assert provider._can_make_request()  # This should transition to half-open
        assert provider.circuit_breaker['state'] == 'half_open'
    
    @pytest.mark.asyncio
    async def test_circuit_breaker_recovery(self, provider_config):
        """Test circuit breaker recovery after successful request."""
        provider = OpenAIProvider(provider_config)
        
        # Set to half-open state
        provider.circuit_breaker['state'] = 'half_open'
        provider.circuit_breaker['failures'] = 3
        
        # Record successful request
        provider._record_success()
        
        assert provider.circuit_breaker['state'] == 'closed'
        assert provider.circuit_breaker['failures'] == 0


class TestRateLimiting:
    """Tests for rate limiting functionality."""
    
    @pytest.fixture
    def provider_config(self):
        return {
            'api_key': 'test_key',
            'model': 'test_model',
            'rate_limit_rpm': 60,  # 1 request per second
            'rate_limit_window': 60
        }
    
    @pytest.mark.asyncio
    async def test_rate_limit_tracking(self, provider_config):
        """Test rate limit request tracking."""
        provider = OpenAIProvider(provider_config)
        
        # Initially should allow requests
        assert await provider._check_rate_limit()
        
        # Add request to tracking
        current_time = time.time()
        provider.rate_limit_requests.append(current_time)
        
        # Should still allow (under limit)
        assert await provider._check_rate_limit()
    
    @pytest.mark.asyncio
    async def test_rate_limit_enforcement(self, provider_config):
        """Test rate limit enforcement when limit is exceeded."""
        provider = OpenAIProvider(provider_config)
        
        # Fill up rate limit window
        current_time = time.time()
        for i in range(61):  # Exceed the 60 RPM limit
            provider.rate_limit_requests.append(current_time - i)
        
        # Should not allow new requests
        with pytest.raises(RateLimitError):
            await provider._check_rate_limit()
    
    @pytest.mark.asyncio
    async def test_rate_limit_window_cleanup(self, provider_config):
        """Test rate limit window cleanup of old requests."""
        provider = OpenAIProvider(provider_config)
        
        # Add old requests (outside window)
        old_time = time.time() - 120  # 2 minutes ago
        for i in range(10):
            provider.rate_limit_requests.append(old_time - i)
        
        # Add recent requests
        current_time = time.time()
        for i in range(5):
            provider.rate_limit_requests.append(current_time - i)
        
        # Check rate limit (should clean up old requests)
        assert await provider._check_rate_limit()
        
        # Should only have recent requests left
        valid_requests = [
            req for req in provider.rate_limit_requests 
            if req > current_time - 60
        ]
        assert len(provider.rate_limit_requests) == len(valid_requests)


class TestMultiProviderClient:
    """Tests for the multi-provider LLM client."""
    
    @pytest.fixture
    def provider_configs(self):
        """Configuration for multiple providers."""
        return {
            'openai': {
                'api_key': 'test_openai_key',
                'model': 'gpt-4o-mini',
                'max_tokens': 1000,
                'temperature': 0.1
            },
            'anthropic': {
                'api_key': 'test_anthropic_key',
                'model': 'claude-3-haiku-20240307', 
                'max_tokens': 1000,
                'temperature': 0.1
            }
        }
    
    @pytest.fixture
    def mock_redis(self):
        """Mock Redis client."""
        redis_mock = AsyncMock()
        return redis_mock
    
    @pytest.mark.asyncio
    async def test_client_initialization(self, provider_configs):
        """Test multi-provider client initialization."""
        client = MultiProviderLLMClient(provider_configs)
        await client.initialize()
        
        assert len(client.providers) == 2
        assert 'openai' in client.providers
        assert 'anthropic' in client.providers
        
        assert client.primary_provider == 'openai'  # First provider becomes primary
        assert isinstance(client.providers['openai'], OpenAIProvider)
        assert isinstance(client.providers['anthropic'], AnthropicProvider)
    
    @pytest.mark.asyncio
    async def test_successful_request_primary_provider(self, provider_configs):
        """Test successful request using primary provider."""
        client = MultiProviderLLMClient(provider_configs)
        await client.initialize()
        
        # Mock successful response from primary provider
        with patch.object(client.providers['openai'], 'generate_response') as mock_response:
            mock_response.return_value = LLMResponse(
                content="Test response",
                model_used="gpt-4o-mini",
                provider="openai",
                tokens_used=50,
                processing_time_ms=1500,
                cost_estimate=0.001
            )
            
            response = await client.generate_response("Test prompt")
            
            assert response.content == "Test response"
            assert response.provider == "openai"
            assert client.metrics['total_requests'] == 1
            assert client.metrics['successful_requests'] == 1
            assert client.metrics['provider_usage']['openai'] == 1
    
    @pytest.mark.asyncio
    async def test_failover_to_secondary_provider(self, provider_configs):
        """Test failover when primary provider fails."""
        client = MultiProviderLLMClient(provider_configs)
        await client.initialize()
        
        # Mock primary provider failure
        with patch.object(client.providers['openai'], 'generate_response') as mock_primary:
            mock_primary.side_effect = CircuitBreakerError("Primary provider down")
            
            # Mock successful secondary provider
            with patch.object(client.providers['anthropic'], 'generate_response') as mock_secondary:
                mock_secondary.return_value = LLMResponse(
                    content="Fallback response",
                    model_used="claude-3-haiku-20240307", 
                    provider="anthropic",
                    tokens_used=45,
                    processing_time_ms=1800,
                    cost_estimate=0.002
                )
                
                response = await client.generate_response("Test prompt")
                
                assert response.content == "Fallback response"
                assert response.provider == "anthropic"
                assert client.metrics['total_requests'] == 1
                assert client.metrics['successful_requests'] == 1
                assert client.metrics['failed_requests'] == 0  # Overall request succeeded
                assert client.metrics['provider_usage']['anthropic'] == 1
    
    @pytest.mark.asyncio
    async def test_all_providers_fail(self, provider_configs):
        """Test behavior when all providers fail."""
        client = MultiProviderLLMClient(provider_configs)
        await client.initialize()
        
        # Mock all providers failing
        with patch.object(client.providers['openai'], 'generate_response') as mock_openai:
            mock_openai.side_effect = CircuitBreakerError("OpenAI down")
            
            with patch.object(client.providers['anthropic'], 'generate_response') as mock_anthropic:
                mock_anthropic.side_effect = CircuitBreakerError("Anthropic down")
                
                with pytest.raises(Exception) as exc_info:
                    await client.generate_response("Test prompt")
                
                assert "All providers failed" in str(exc_info.value)
                assert client.metrics['total_requests'] == 1
                assert client.metrics['successful_requests'] == 0
                assert client.metrics['failed_requests'] == 1
    
    @pytest.mark.asyncio 
    async def test_provider_preference_override(self, provider_configs):
        """Test overriding provider preference for specific request."""
        client = MultiProviderLLMClient(provider_configs)
        await client.initialize()
        
        # Mock anthropic provider response
        with patch.object(client.providers['anthropic'], 'generate_response') as mock_anthropic:
            mock_anthropic.return_value = LLMResponse(
                content="Anthropic response",
                model_used="claude-3-haiku-20240307",
                provider="anthropic", 
                tokens_used=40,
                processing_time_ms=1200,
                cost_estimate=0.0015
            )
            
            response = await client.generate_response(
                "Test prompt", 
                model_preference="anthropic"
            )
            
            assert response.content == "Anthropic response"
            assert response.provider == "anthropic"
            mock_anthropic.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_caching_functionality(self, provider_configs, mock_redis):
        """Test response caching functionality."""
        client = MultiProviderLLMClient(provider_configs, redis_client=mock_redis)
        await client.initialize()
        
        # Mock cache miss, then hit
        mock_redis.get.side_effect = [None, b'{"content": "Cached response", "provider": "cache"}']
        
        with patch.object(client.providers['openai'], 'generate_response') as mock_provider:
            mock_provider.return_value = LLMResponse(
                content="Original response",
                model_used="gpt-4o-mini",
                provider="openai",
                tokens_used=50,
                processing_time_ms=1500,
                cost_estimate=0.001
            )
            
            # First request - cache miss
            response1 = await client.generate_response("Test prompt")
            assert response1.content == "Original response"
            mock_redis.setex.assert_called_once()  # Should cache the response
            
            # Second request - cache hit
            response2 = await client.generate_response("Test prompt")
            assert response2.content == "Cached response"
            assert response2.provider == "cache"
    
    @pytest.mark.asyncio
    async def test_cost_tracking(self, provider_configs):
        """Test cost tracking across providers."""
        client = MultiProviderLLMClient(provider_configs)
        await client.initialize()
        
        with patch.object(client.providers['openai'], 'generate_response') as mock_openai:
            mock_openai.return_value = LLMResponse(
                content="Response 1",
                model_used="gpt-4o-mini",
                provider="openai",
                tokens_used=100,
                processing_time_ms=1000,
                cost_estimate=0.005
            )
            
            # Make multiple requests
            await client.generate_response("Prompt 1")
            await client.generate_response("Prompt 2") 
            await client.generate_response("Prompt 3")
            
            assert client.metrics['total_cost'] == 0.015  # 3 * 0.005
            assert client.metrics['total_tokens'] == 300   # 3 * 100
            assert client.metrics['provider_costs']['openai'] == 0.015
    
    @pytest.mark.asyncio
    async def test_performance_metrics(self, provider_configs):
        """Test performance metrics collection."""
        client = MultiProviderLLMClient(provider_configs)
        await client.initialize()
        
        with patch.object(client.providers['openai'], 'generate_response') as mock_openai:
            mock_openai.return_value = LLMResponse(
                content="Response",
                model_used="gpt-4o-mini", 
                provider="openai",
                tokens_used=75,
                processing_time_ms=2000,
                cost_estimate=0.003
            )
            
            await client.generate_response("Test prompt")
            
            metrics = client.get_metrics()
            
            assert metrics['total_requests'] == 1
            assert metrics['successful_requests'] == 1
            assert metrics['avg_response_time_ms'] == 2000
            assert metrics['provider_usage']['openai'] == 1
            assert 'uptime_seconds' in metrics
            assert metrics['cache_hit_rate'] == 0.0  # No cache hits
    
    @pytest.mark.asyncio
    async def test_health_check(self, provider_configs):
        """Test health check functionality."""
        client = MultiProviderLLMClient(provider_configs)
        await client.initialize()
        
        # Mock healthy providers
        with patch.object(client.providers['openai'], '_can_make_request', return_value=True):
            with patch.object(client.providers['anthropic'], '_can_make_request', return_value=True):
                
                health = await client.health_check()
                assert health is True
        
        # Mock unhealthy primary provider
        with patch.object(client.providers['openai'], '_can_make_request', return_value=False):
            with patch.object(client.providers['anthropic'], '_can_make_request', return_value=True):
                
                health = await client.health_check()
                assert health is True  # Should still be healthy with backup
        
        # Mock all providers unhealthy
        with patch.object(client.providers['openai'], '_can_make_request', return_value=False):
            with patch.object(client.providers['anthropic'], '_can_make_request', return_value=False):
                
                health = await client.health_check()
                assert health is False


class TestProviderSpecificBehavior:
    """Tests for provider-specific behavior and edge cases."""
    
    @pytest.mark.asyncio
    async def test_openai_token_counting(self):
        """Test OpenAI token counting from response."""
        config = {
            'api_key': 'test_key',
            'model': 'gpt-4o-mini',
            'max_tokens': 1000
        }
        
        provider = OpenAIProvider(config)
        provider.session = AsyncMock()
        
        # Mock response with token usage
        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.json = AsyncMock(return_value={
            'choices': [{'message': {'content': 'Test response'}}],
            'usage': {
                'prompt_tokens': 20,
                'completion_tokens': 30,
                'total_tokens': 50
            }
        })
        provider.session.post.return_value.__aenter__.return_value = mock_response
        
        response = await provider.generate_response("Test prompt")
        
        assert response.tokens_used == 50
        assert 'prompt_tokens' in response.usage_breakdown
        assert response.usage_breakdown['prompt_tokens'] == 20
        assert response.usage_breakdown['completion_tokens'] == 30
    
    @pytest.mark.asyncio
    async def test_anthropic_content_extraction(self):
        """Test Anthropic content extraction from response."""
        config = {
            'api_key': 'test_key',
            'model': 'claude-3-haiku-20240307',
            'max_tokens': 1000
        }
        
        provider = AnthropicProvider(config)
        provider.session = AsyncMock()
        
        # Mock Anthropic response format
        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.json = AsyncMock(return_value={
            'content': [
                {'type': 'text', 'text': 'Part 1'},
                {'type': 'text', 'text': ' Part 2'}
            ],
            'usage': {'output_tokens': 25}
        })
        provider.session.post.return_value.__aenter__.return_value = mock_response
        
        response = await provider.generate_response("Test prompt")
        
        assert response.content == 'Part 1 Part 2'
        assert response.tokens_used == 25
    
    @pytest.mark.asyncio
    async def test_google_response_parsing(self):
        """Test Google/Gemini response parsing."""
        config = {
            'api_key': 'test_key',
            'model': 'gemini-1.5-flash',
            'max_tokens': 1000
        }
        
        provider = GoogleProvider(config)
        provider.session = AsyncMock()
        
        # Mock Gemini response format
        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.json = AsyncMock(return_value={
            'candidates': [
                {
                    'content': {
                        'parts': [{'text': 'Google response'}]
                    }
                }
            ],
            'usageMetadata': {'totalTokenCount': 35}
        })
        provider.session.post.return_value.__aenter__.return_value = mock_response
        
        response = await provider.generate_response("Test prompt")
        
        assert response.content == 'Google response'
        assert response.tokens_used == 35
        assert response.provider == 'google'
    
    @pytest.mark.asyncio
    async def test_cost_calculation_by_provider(self):
        """Test cost calculation varies by provider."""
        # OpenAI cost calculation
        openai_config = {'api_key': 'test', 'model': 'gpt-4o-mini'}
        openai_provider = OpenAIProvider(openai_config)
        
        openai_cost = openai_provider._calculate_cost(100, 'gpt-4o-mini')
        assert openai_cost > 0
        
        # Anthropic cost calculation
        anthropic_config = {'api_key': 'test', 'model': 'claude-3-haiku-20240307'}
        anthropic_provider = AnthropicProvider(anthropic_config)
        
        anthropic_cost = anthropic_provider._calculate_cost(100, 'claude-3-haiku-20240307')
        assert anthropic_cost > 0
        
        # Costs should be different for different providers
        assert openai_cost != anthropic_cost


@pytest.mark.integration
class TestLLMClientIntegration:
    """Integration tests for LLM client with external dependencies."""
    
    @pytest.fixture
    def redis_client(self):
        """Real Redis client for integration testing."""
        return redis.from_url("redis://localhost:6379/0")
    
    @pytest.mark.asyncio
    @pytest.mark.skipif(not pytest.config.getoption("--integration"), 
                       reason="Integration tests require --integration flag")
    async def test_redis_caching_integration(self, redis_client):
        """Integration test with real Redis caching."""
        provider_configs = {
            'openai': {
                'api_key': 'test_key',
                'model': 'gpt-4o-mini',
                'max_tokens': 100
            }
        }
        
        client = MultiProviderLLMClient(provider_configs, redis_client=redis_client)
        await client.initialize()
        
        # Clear any existing cache
        await redis_client.flushdb()
        
        with patch.object(client.providers['openai'], 'generate_response') as mock_provider:
            mock_provider.return_value = LLMResponse(
                content="Cached test response",
                model_used="gpt-4o-mini",
                provider="openai",
                tokens_used=20,
                processing_time_ms=1000,
                cost_estimate=0.001
            )
            
            # First request should hit the provider
            response1 = await client.generate_response("Integration test prompt")
            assert mock_provider.call_count == 1
            
            # Second request should hit cache
            response2 = await client.generate_response("Integration test prompt")
            assert mock_provider.call_count == 1  # No additional calls
            assert response2.provider == "cache"
        
        await redis_client.close()


# Performance and Load Testing
@pytest.mark.performance
class TestLLMClientPerformance:
    """Performance tests for LLM client."""
    
    @pytest.mark.asyncio
    async def test_concurrent_request_handling(self):
        """Test handling of concurrent requests."""
        provider_configs = {
            'openai': {
                'api_key': 'test_key',
                'model': 'gpt-4o-mini',
                'max_tokens': 100,
                'rate_limit_rpm': 100
            }
        }
        
        client = MultiProviderLLMClient(provider_configs)
        await client.initialize()
        
        with patch.object(client.providers['openai'], 'generate_response') as mock_provider:
            # Simulate varying response times
            async def mock_response(*args, **kwargs):
                await asyncio.sleep(0.1)  # 100ms simulated processing
                return LLMResponse(
                    content="Concurrent response",
                    model_used="gpt-4o-mini",
                    provider="openai",
                    tokens_used=25,
                    processing_time_ms=100,
                    cost_estimate=0.001
                )
            
            mock_provider.side_effect = mock_response
            
            # Launch 20 concurrent requests
            start_time = time.time()
            tasks = [
                client.generate_response(f"Concurrent prompt {i}") 
                for i in range(20)
            ]
            responses = await asyncio.gather(*tasks)
            end_time = time.time()
            
            # All responses should succeed
            assert len(responses) == 20
            assert all(r.content == "Concurrent response" for r in responses)
            
            # Should complete in reasonable time (less than 2 seconds for 20 concurrent requests)
            total_time = end_time - start_time
            assert total_time < 2.0
            
            # Should track all requests in metrics
            assert client.metrics['total_requests'] == 20
            assert client.metrics['successful_requests'] == 20
    
    @pytest.mark.asyncio
    async def test_memory_usage_stability(self):
        """Test that memory usage remains stable under load."""
        import psutil
        import os
        
        provider_configs = {
            'openai': {
                'api_key': 'test_key',
                'model': 'gpt-4o-mini',
                'max_tokens': 100
            }
        }
        
        client = MultiProviderLLMClient(provider_configs)
        await client.initialize()
        
        process = psutil.Process(os.getpid())
        initial_memory = process.memory_info().rss
        
        with patch.object(client.providers['openai'], 'generate_response') as mock_provider:
            mock_provider.return_value = LLMResponse(
                content="Memory test response" * 100,  # Larger response
                model_used="gpt-4o-mini",
                provider="openai",
                tokens_used=500,
                processing_time_ms=100,
                cost_estimate=0.005
            )
            
            # Make 100 requests
            for i in range(100):
                await client.generate_response(f"Memory test prompt {i}")
                
                # Check memory every 20 requests
                if i % 20 == 0:
                    current_memory = process.memory_info().rss
                    memory_increase = current_memory - initial_memory
                    
                    # Memory increase should be reasonable (less than 50MB)
                    assert memory_increase < 50 * 1024 * 1024, f"Memory increased by {memory_increase / 1024 / 1024:.2f}MB"
        
        final_memory = process.memory_info().rss
        total_increase = final_memory - initial_memory
        
        # Total memory increase should be reasonable
        assert total_increase < 100 * 1024 * 1024, f"Total memory increase: {total_increase / 1024 / 1024:.2f}MB"


# Fixtures and utilities
@pytest.fixture(scope="session")
def event_loop():
    """Create an instance of the default event loop for the test session."""
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


def pytest_addoption(parser):
    """Add custom pytest command line options."""
    parser.addoption(
        "--integration", action="store_true", default=False,
        help="run integration tests that require external services"
    )


def pytest_configure(config):
    """Configure pytest with custom markers."""
    config.addinivalue_line(
        "markers", "integration: mark test as integration test requiring external services"
    )
    config.addinivalue_line(
        "markers", "performance: mark test as performance/load test"
    )


def pytest_collection_modifyitems(config, items):
    """Modify test collection based on command line options."""
    if config.getoption("--integration"):
        # Run integration tests
        return
    
    # Skip integration tests by default
    skip_integration = pytest.mark.skip(reason="need --integration option to run")
    for item in items:
        if "integration" in item.keywords:
            item.add_marker(skip_integration)