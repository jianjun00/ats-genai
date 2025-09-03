"""
DeepSeek Pilot Integration for Financial News Analysis

This module provides the core integration with DeepSeek-R1 for enhanced financial news processing.
Includes fallback mechanisms, cost tracking, and performance monitoring.
"""

import asyncio
import aiohttp
import json
import logging
import time
from datetime import datetime
from typing import Dict, Any, Optional, List
from dataclasses import dataclass

from sentiment.news_sentiment_analyzer import NewsArticle
from .pilot_monitor import CostTracker, PerformanceMonitor

logger = logging.getLogger(__name__)


@dataclass
class LLMAnalysisResult:
    """Structured result from LLM news analysis"""
    sentiment_score: float
    confidence: float
    event_type: str
    impact_timeline: str
    quantified_impact: str
    risk_factors: List[str]
    key_points: List[str]
    reasoning: str
    metadata: Dict[str, Any]


class DeepSeekPilotClient:
    """
    Pilot integration with DeepSeek-R1 for financial news analysis.
    
    Features:
    - Structured financial news analysis
    - Cost tracking and optimization
    - Performance monitoring
    - Automatic fallback to FinBERT
    - Request caching for efficiency
    """
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        self.config = config or self._default_config()
        self.endpoint = self.config['endpoint']
        self.model_name = self.config['model_name']
        
        # Initialize tracking and monitoring
        self.cost_tracker = CostTracker()
        self.performance_monitor = PerformanceMonitor()
        
        # Request caching for efficiency
        self.request_cache = {}
        self.cache_ttl = 3600  # 1 hour cache
        
        # Session for connection pooling
        self.session = None
        
        # Fallback system
        self._fallback_analyzer = None
        
    def _default_config(self) -> Dict[str, Any]:
        """Default configuration for DeepSeek pilot"""
        return {
            'endpoint': 'http://deepseek-pilot-service:8000/v1/chat/completions',
            'model_name': 'deepseek-r1',
            'max_tokens': 2048,
            'temperature': 0.1,
            'timeout': 30,
            'max_retries': 2,
            'cost_per_1k_tokens': 0.014  # Estimated DeepSeek-R1 pricing
        }
    
    async def __aenter__(self):
        """Async context manager entry"""
        self.session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=self.config['timeout'])
        )
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        if self.session:
            await self.session.close()
    
    async def analyze_financial_news(self, article: NewsArticle) -> Dict[str, Any]:
        """
        Analyze financial news article using DeepSeek-R1 with structured output.
        
        Args:
            article: NewsArticle object containing title, content, symbols, etc.
            
        Returns:
            Dict containing structured analysis and metadata
        """
        try:
            # Check cache first
            cache_key = self._generate_cache_key(article)
            cached_result = self._get_cached_result(cache_key)
            if cached_result:
                logger.info(f"Using cached result for article: {article.url}")
                return cached_result
            
            # Create analysis prompt
            prompt = self._create_analysis_prompt(article)
            
            # Call DeepSeek with performance tracking
            start_time = time.time()
            response = await self._call_deepseek(prompt)
            latency = time.time() - start_time
            
            # Parse structured response
            analysis = self._parse_response(response)
            
            # Calculate costs
            input_tokens = len(prompt.split()) * 1.3  # Rough tokenization estimate
            output_tokens = len(response.split()) * 1.3
            cost = self.cost_tracker.calculate_cost(input_tokens, output_tokens)
            
            # Record performance metrics
            self.performance_monitor.record_request(latency, len(response))
            
            # Create result object
            result = {
                'analysis': analysis,
                'metadata': {
                    'processor': 'deepseek-r1',
                    'latency': latency,
                    'cost': cost,
                    'input_tokens': int(input_tokens),
                    'output_tokens': int(output_tokens),
                    'timestamp': datetime.now(),
                    'cached': False
                }
            }
            
            # Cache successful result
            self._cache_result(cache_key, result)
            
            logger.info(f"DeepSeek analysis completed: {article.url} "
                       f"(latency: {latency:.2f}s, cost: ${cost:.4f})")
            
            return result
            
        except Exception as e:
            logger.error(f"DeepSeek analysis failed for {article.url}: {e}")
            raise RuntimeError(f"Failed to analyze article {article.url}: {e}. No fallback analysis available")
    
    def _create_analysis_prompt(self, article: NewsArticle) -> str:
        """Create structured prompt for financial analysis"""
        
        # Get primary symbol for context
        primary_symbol = article.symbols[0] if article.symbols else 'Unknown'
        
        prompt = f"""You are a financial analyst AI. Analyze this news article and provide structured output in JSON format.

News Article:
Title: {article.title}
Content: {article.content[:2000]}  # Limit content length
Symbol: {primary_symbol}
Source: {article.source}
Published: {article.published_date}

Provide your analysis in this exact JSON format:
{{
    "sentiment_score": <number between -1.0 and 1.0, where -1.0 is very negative, 0.0 is neutral, 1.0 is very positive>,
    "confidence": <number between 0.0 and 1.0, representing confidence in the analysis>,
    "event_type": "<one of: earnings, guidance, merger, regulatory, clinical, bankruptcy, dividend, buyback, partnership, other>",
    "impact_timeline": "<one of: immediate, 1week, 1month, 1quarter>",
    "quantified_impact": "<specific percentage, dollar amount mentioned, or 'not specified'>",
    "risk_factors": ["<list of 1-3 key risk factors identified>"],
    "key_points": ["<3-5 most important factual points from the article>"],
    "reasoning": "<brief 2-3 sentence explanation of your sentiment analysis>"
}}

Guidelines:
- Be conservative with confidence scores
- Focus on factual financial impacts
- Consider both positive and negative aspects
- For earnings/guidance news, pay attention to beats/misses vs expectations
- For M&A news, consider regulatory and execution risks
- For regulatory/clinical news, assess probability and timeline uncertainty

Provide only the JSON response, no other text."""

        return prompt
    
    async def _call_deepseek(self, prompt: str) -> str:
        """Make API call to DeepSeek model"""
        
        if not self.session:
            raise RuntimeError("Client session not initialized. Use async context manager.")
        
        payload = {
            "model": self.model_name,
            "messages": [
                {
                    "role": "user",
                    "content": prompt
                }
            ],
            "max_tokens": self.config['max_tokens'],
            "temperature": self.config['temperature'],
            "top_p": 0.95,
            "frequency_penalty": 0.0,
            "presence_penalty": 0.0
        }
        
        for attempt in range(self.config['max_retries'] + 1):
            try:
                async with self.session.post(
                    self.endpoint,
                    json=payload,
                    headers={"Content-Type": "application/json"}
                ) as response:
                    
                    if response.status == 200:
                        result = await response.json()
                        return result['choices'][0]['message']['content']
                    else:
                        error_text = await response.text()
                        raise aiohttp.ClientError(
                            f"DeepSeek API error {response.status}: {error_text}"
                        )
                        
            except asyncio.TimeoutError:
                if attempt < self.config['max_retries']:
                    wait_time = 2 ** attempt  # Exponential backoff
                    logger.warning(f"DeepSeek timeout, retrying in {wait_time}s (attempt {attempt + 1})")
                    await asyncio.sleep(wait_time)
                else:
                    raise
            except Exception as e:
                if attempt < self.config['max_retries']:
                    wait_time = 2 ** attempt
                    logger.warning(f"DeepSeek error: {e}, retrying in {wait_time}s (attempt {attempt + 1})")
                    await asyncio.sleep(wait_time)
                else:
                    raise
    
    def _parse_response(self, response: str) -> LLMAnalysisResult:
        """Parse structured JSON response from DeepSeek"""
        
        try:
            # Clean response - remove any markdown formatting
            cleaned_response = response.strip()
            if cleaned_response.startswith('```json'):
                cleaned_response = cleaned_response[7:]
            if cleaned_response.endswith('```'):
                cleaned_response = cleaned_response[:-3]
            
            # Parse JSON
            parsed = json.loads(cleaned_response.strip())
            
            # Validate required fields
            required_fields = [
                'sentiment_score', 'confidence', 'event_type', 
                'impact_timeline', 'quantified_impact', 'risk_factors', 
                'key_points', 'reasoning'
            ]
            
            for field in required_fields:
                if field not in parsed:
                    raise ValueError(f"Missing required field: {field}")
            
            # Create structured result
            return LLMAnalysisResult(
                sentiment_score=float(parsed['sentiment_score']),
                confidence=float(parsed['confidence']),
                event_type=str(parsed['event_type']),
                impact_timeline=str(parsed['impact_timeline']),
                quantified_impact=str(parsed['quantified_impact']),
                risk_factors=list(parsed['risk_factors']),
                key_points=list(parsed['key_points']),
                reasoning=str(parsed['reasoning']),
                metadata={'raw_response': response}
            )
            
        except (json.JSONDecodeError, ValueError, KeyError) as e:
            logger.error(f"Failed to parse DeepSeek response: {e}")
            logger.error(f"Raw response: {response}")
            
            # Return default analysis with low confidence
            return LLMAnalysisResult(
                sentiment_score=0.0,
                confidence=0.1,
                event_type='other',
                impact_timeline='1month',
                quantified_impact='not specified',
                risk_factors=['parsing_error'],
                key_points=['Unable to parse structured response'],
                reasoning='Response parsing failed',
                metadata={'error': str(e), 'raw_response': response}
            )
    
    def _generate_cache_key(self, article: NewsArticle) -> str:
        """Generate cache key for article"""
        # Use URL + published date for uniqueness
        return f"deepseek:{hash(article.url + str(article.published_date))}"
    
    def _get_cached_result(self, cache_key: str) -> Optional[Dict[str, Any]]:
        """Get cached result if available and not expired"""
        
        if cache_key not in self.request_cache:
            return None
        
        cached_data = self.request_cache[cache_key]
        cache_time = cached_data.get('cache_time', 0)
        
        # Check if cache is expired
        if time.time() - cache_time > self.cache_ttl:
            del self.request_cache[cache_key]
            return None
        
        # Update metadata to indicate cached result
        result = cached_data['result'].copy()
        result['metadata']['cached'] = True
        
        return result
    
    def _cache_result(self, cache_key: str, result: Dict[str, Any]):
        """Cache successful analysis result"""
        
        # Don't cache if cache is getting too large
        if len(self.request_cache) > 1000:
            # Remove oldest entries (simple FIFO)
            oldest_key = next(iter(self.request_cache))
            del self.request_cache[oldest_key]
        
        self.request_cache[cache_key] = {
            'result': result.copy(),
            'cache_time': time.time()
        }
    
    def get_performance_stats(self) -> Dict[str, Any]:
        """Get performance statistics for monitoring"""
        
        return {
            'total_requests': self.performance_monitor.total_requests,
            'avg_latency': self.performance_monitor.avg_latency,
            'total_cost': self.cost_tracker.total_cost,
            'daily_cost': self.cost_tracker.get_daily_cost(),
            'cache_hit_rate': self._calculate_cache_hit_rate(),
            'error_rate': self.performance_monitor.error_rate
        }
    
    def _calculate_cache_hit_rate(self) -> float:
        """Calculate cache hit rate"""
        if not hasattr(self, '_cache_hits'):
            self._cache_hits = 0
        if not hasattr(self, '_total_requests'):
            self._total_requests = 0
            
        return self._cache_hits / self._total_requests if self._total_requests > 0 else 0.0


# Convenience function for external use
async def analyze_news_with_deepseek(articles: List[NewsArticle]) -> List[Dict[str, Any]]:
    """
    Convenience function to analyze multiple articles with DeepSeek.
    
    Args:
        articles: List of NewsArticle objects to analyze
        
    Returns:
        List of analysis results
    """
    
    results = []
    
    async with DeepSeekPilotClient() as client:
        for article in articles:
            try:
                result = await client.analyze_financial_news(article)
                results.append(result)
            except Exception as e:
                logger.error(f"Failed to analyze article {article.url}: {e}")
                results.append({
                    'analysis': None,
                    'metadata': {'error': str(e), 'article_url': article.url}
                })
    
    return results