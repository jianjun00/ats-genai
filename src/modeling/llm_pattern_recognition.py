"""
LLM-Enhanced Pattern Recognition for Financial Data.
Uses DeepSeek/OpenAI to identify technical patterns, generate features, and create synthetic data.
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple, Any, Union
from datetime import datetime, timedelta
from dataclasses import dataclass
import logging
import json
import asyncio
import aiohttp
from enum import Enum

logger = logging.getLogger(__name__)


class LLMProvider(Enum):
    DEEPSEEK = "deepseek"
    OPENAI = "openai"
    LOCAL = "local"


@dataclass
class PatternAnalysis:
    """LLM analysis of price pattern."""
    pattern_type: str
    confidence: float
    description: str
    technical_indicators: Dict[str, Any]
    predicted_direction: str  # "bullish", "bearish", "neutral"
    support_resistance: Dict[str, float]
    volume_analysis: str
    risk_assessment: str
    timeframe_relevance: List[str]


@dataclass
class MarketRegimeAnalysis:
    """LLM analysis of market regime."""
    regime_type: str  # "trending", "ranging", "volatile", "low_vol"
    confidence: float
    characteristics: List[str]
    typical_duration: str
    trading_implications: List[str]
    risk_factors: List[str]


class LLMPatternRecognizer:
    """Enhanced pattern recognition using Large Language Models."""
    
    def __init__(self, 
                 provider: LLMProvider = LLMProvider.DEEPSEEK,
                 api_key: Optional[str] = None,
                 model_name: Optional[str] = None,
                 max_concurrent_requests: int = 5):
        self.provider = provider
        self.api_key = api_key
        self.max_concurrent_requests = max_concurrent_requests
        
        # Model configuration
        if provider == LLMProvider.DEEPSEEK:
            self.model_name = model_name or "deepseek-chat"
            self.base_url = "https://api.deepseek.com/v1"
        elif provider == LLMProvider.OPENAI:
            self.model_name = model_name or "gpt-4-turbo-preview"
            self.base_url = "https://api.openai.com/v1"
        else:
            self.model_name = model_name or "local-model"
            self.base_url = "http://localhost:8000/v1"
        
        # Semaphore for rate limiting
        self._semaphore = asyncio.Semaphore(max_concurrent_requests)
        
        # Cache for repeated patterns
        self._pattern_cache = {}
    
    async def analyze_price_pattern(self, 
                                  price_data: pd.DataFrame,
                                  symbol: str = "STOCK",
                                  timeframe: str = "daily") -> PatternAnalysis:
        """
        Analyze price pattern using LLM.
        
        Args:
            price_data: DataFrame with OHLCV data
            symbol: Stock symbol for context
            timeframe: Time frame (daily, hourly, etc.)
            
        Returns:
            PatternAnalysis with LLM insights
        """
        try:
            # Create pattern signature for caching
            pattern_sig = self._create_pattern_signature(price_data)
            cache_key = f"{pattern_sig}_{timeframe}"
            
            if cache_key in self._pattern_cache:
                return self._pattern_cache[cache_key]
            
            # Prepare price sequence for LLM
            price_sequence = self._format_price_sequence(price_data)
            
            # Create prompt
            prompt = self._create_pattern_analysis_prompt(
                price_sequence, symbol, timeframe
            )
            
            # Get LLM response
            response = await self._call_llm(prompt)
            
            # Parse response
            analysis = self._parse_pattern_analysis(response)
            
            # Cache result
            self._pattern_cache[cache_key] = analysis
            
            return analysis
            
        except Exception as e:
            logger.warning(f"Failed to analyze pattern with LLM: {e}")
            return self._create_fallback_analysis()
    
    async def identify_market_regime(self,
                                   market_data: pd.DataFrame,
                                   lookback_days: int = 60) -> MarketRegimeAnalysis:
        """
        Identify current market regime using LLM analysis.
        
        Args:
            market_data: Market index data (SPY, QQQ, etc.)
            lookback_days: Days to analyze for regime identification
            
        Returns:
            MarketRegimeAnalysis with regime insights
        """
        try:
            # Prepare market data summary
            recent_data = market_data.tail(lookback_days)
            market_summary = self._create_market_summary(recent_data)
            
            # Create regime analysis prompt
            prompt = self._create_regime_analysis_prompt(market_summary)
            
            # Get LLM response
            response = await self._call_llm(prompt)
            
            # Parse response
            regime_analysis = self._parse_regime_analysis(response)
            
            return regime_analysis
            
        except Exception as e:
            logger.warning(f"Failed to analyze market regime: {e}")
            return self._create_fallback_regime()
    
    async def generate_synthetic_patterns(self,
                                        base_patterns: List[pd.DataFrame],
                                        target_count: int = 100,
                                        pattern_type: str = "bullish_reversal") -> List[pd.DataFrame]:
        """
        Generate synthetic price patterns using LLM guidance.
        
        Args:
            base_patterns: Existing patterns to learn from
            target_count: Number of synthetic patterns to generate
            pattern_type: Type of pattern to generate
            
        Returns:
            List of synthetic price DataFrames
        """
        synthetic_patterns = []
        
        try:
            # Analyze base patterns to understand characteristics
            pattern_characteristics = await self._analyze_pattern_characteristics(
                base_patterns, pattern_type
            )
            
            # Generate synthetic patterns in batches
            batch_size = 10
            for i in range(0, target_count, batch_size):
                batch_patterns = await self._generate_pattern_batch(
                    pattern_characteristics, 
                    min(batch_size, target_count - i),
                    pattern_type
                )
                synthetic_patterns.extend(batch_patterns)
            
            logger.info(f"Generated {len(synthetic_patterns)} synthetic {pattern_type} patterns")
            return synthetic_patterns
            
        except Exception as e:
            logger.error(f"Failed to generate synthetic patterns: {e}")
            return []
    
    async def enhance_feature_engineering(self,
                                        price_data: pd.DataFrame,
                                        existing_features: Dict[str, float]) -> Dict[str, float]:
        """
        Use LLM to suggest and calculate additional features.
        
        Args:
            price_data: Historical price data
            existing_features: Currently calculated features
            
        Returns:
            Enhanced feature dictionary
        """
        try:
            # Get pattern analysis
            pattern_analysis = await self.analyze_price_pattern(price_data)
            
            # Create enhanced features based on LLM insights
            enhanced_features = existing_features.copy()
            
            # Pattern-based features
            enhanced_features.update({
                f'llm_pattern_confidence': pattern_analysis.confidence,
                f'llm_bullish_score': 1.0 if pattern_analysis.predicted_direction == "bullish" else 0.0,
                f'llm_bearish_score': 1.0 if pattern_analysis.predicted_direction == "bearish" else 0.0,
                f'llm_pattern_strength': self._calculate_pattern_strength(pattern_analysis),
            })
            
            # Support/resistance features
            if pattern_analysis.support_resistance:
                current_price = price_data['close'].iloc[-1] if 'close' in price_data.columns else price_data['high'].iloc[-1]
                
                if 'support' in pattern_analysis.support_resistance:
                    support_level = pattern_analysis.support_resistance['support']
                    enhanced_features['llm_distance_to_support'] = (current_price - support_level) / current_price
                
                if 'resistance' in pattern_analysis.support_resistance:
                    resistance_level = pattern_analysis.support_resistance['resistance']
                    enhanced_features['llm_distance_to_resistance'] = (resistance_level - current_price) / current_price
            
            # Generate additional mathematical features suggested by LLM
            llm_math_features = await self._get_llm_mathematical_features(price_data)
            enhanced_features.update(llm_math_features)
            
            return enhanced_features
            
        except Exception as e:
            logger.warning(f"Failed to enhance features with LLM: {e}")
            return existing_features
    
    def _create_pattern_signature(self, price_data: pd.DataFrame) -> str:
        """Create a signature for pattern caching."""
        # Use price ratios for pattern recognition (scale-invariant)
        if 'close' in price_data.columns:
            prices = price_data['close'].tail(20)
        else:
            prices = price_data['high'].tail(20)
        
        # Normalize to first price
        normalized = prices / prices.iloc[0]
        
        # Create signature from key points
        signature_points = [
            normalized.iloc[0], normalized.iloc[len(normalized)//4],
            normalized.iloc[len(normalized)//2], normalized.iloc[3*len(normalized)//4],
            normalized.iloc[-1]
        ]
        
        # Round to create stable signature
        signature = "_".join([f"{p:.3f}" for p in signature_points])
        return signature
    
    def _format_price_sequence(self, price_data: pd.DataFrame) -> str:
        """Format price data for LLM consumption."""
        # Limit to recent data to fit in context
        recent_data = price_data.tail(30)
        
        sequence_parts = []
        sequence_parts.append("Price sequence (Date: Open, High, Low, Close, Volume):")
        
        for idx, row in recent_data.iterrows():
            date_str = str(idx) if hasattr(idx, 'strftime') else str(idx)
            
            if 'open' in row:
                line = f"{date_str}: {row.get('open', 0):.2f}, {row.get('high', 0):.2f}, {row.get('low', 0):.2f}, {row.get('close', 0):.2f}"
            else:
                line = f"{date_str}: -, {row.get('high', 0):.2f}, {row.get('low', 0):.2f}, -"
            
            if 'volume' in row and pd.notna(row['volume']):
                line += f", {int(row['volume'])}"
            
            sequence_parts.append(line)
        
        return "\n".join(sequence_parts)
    
    def _create_pattern_analysis_prompt(self, price_sequence: str, symbol: str, timeframe: str) -> str:
        """Create prompt for pattern analysis."""
        return f"""As an expert technical analyst, analyze this {timeframe} price sequence for {symbol}:

{price_sequence}

Please provide a comprehensive analysis in JSON format with the following structure:
{{
    "pattern_type": "head_and_shoulders|double_top|ascending_triangle|flag|wedge|channel|other",
    "confidence": 0.85,
    "description": "Detailed description of the pattern",
    "technical_indicators": {{
        "trend_direction": "up|down|sideways",
        "momentum": "strong|weak|neutral",
        "volume_confirmation": "confirmed|unconfirmed|neutral"
    }},
    "predicted_direction": "bullish|bearish|neutral",
    "support_resistance": {{
        "support": 150.25,
        "resistance": 165.80
    }},
    "volume_analysis": "Volume analysis comments",
    "risk_assessment": "Risk level and factors",
    "timeframe_relevance": ["short_term", "medium_term", "long_term"]
}}

Focus on:
1. Classical chart patterns (triangles, flags, head & shoulders, etc.)
2. Support and resistance levels
3. Volume confirmation
4. Momentum indicators
5. Risk assessment
6. Directional bias

Provide specific numerical levels where possible and ensure confidence score reflects pattern clarity."""
    
    def _create_regime_analysis_prompt(self, market_summary: str) -> str:
        """Create prompt for market regime analysis."""
        return f"""As a market regime specialist, analyze the current market environment:

{market_summary}

Provide analysis in JSON format:
{{
    "regime_type": "trending_bull|trending_bear|ranging|high_volatility|low_volatility|transition",
    "confidence": 0.80,
    "characteristics": ["characteristic1", "characteristic2"],
    "typical_duration": "2-4 weeks",
    "trading_implications": ["implication1", "implication2"],
    "risk_factors": ["risk1", "risk2"]
}}

Consider:
1. Volatility patterns
2. Trend persistence
3. Correlation structures
4. Volume patterns
5. Sector rotation
6. Economic environment"""
    
    def _create_market_summary(self, market_data: pd.DataFrame) -> str:
        """Create market summary for regime analysis."""
        summary_parts = []
        
        # Calculate key metrics
        returns = market_data['close'].pct_change().dropna()
        
        summary_parts.append("Market Summary:")
        summary_parts.append(f"Period: {len(market_data)} trading days")
        summary_parts.append(f"Total return: {(market_data['close'].iloc[-1] / market_data['close'].iloc[0] - 1):.2%}")
        summary_parts.append(f"Volatility (annualized): {returns.std() * np.sqrt(252):.2%}")
        summary_parts.append(f"Max drawdown: {self._calculate_max_drawdown(market_data['close']):.2%}")
        
        # Recent performance
        recent_returns = returns.tail(10)
        summary_parts.append(f"Recent 10-day return: {recent_returns.sum():.2%}")
        summary_parts.append(f"Positive days: {(returns > 0).sum()}/{len(returns)}")
        
        # Trend analysis
        ma_20 = market_data['close'].rolling(20).mean().iloc[-1]
        ma_50 = market_data['close'].rolling(50).mean().iloc[-1] if len(market_data) >= 50 else ma_20
        
        summary_parts.append(f"Price vs 20-day MA: {(market_data['close'].iloc[-1] / ma_20 - 1):.2%}")
        summary_parts.append(f"20-day MA vs 50-day MA: {(ma_20 / ma_50 - 1):.2%}")
        
        return "\n".join(summary_parts)
    
    def _calculate_max_drawdown(self, prices: pd.Series) -> float:
        """Calculate maximum drawdown."""
        cumulative = (1 + prices.pct_change()).cumprod()
        running_max = cumulative.expanding().max()
        drawdown = (cumulative - running_max) / running_max
        return drawdown.min()
    
    async def _call_llm(self, prompt: str) -> str:
        """Make API call to LLM with rate limiting."""
        async with self._semaphore:
            try:
                async with aiohttp.ClientSession() as session:
                    headers = {
                        "Authorization": f"Bearer {self.api_key}",
                        "Content-Type": "application/json"
                    }
                    
                    payload = {
                        "model": self.model_name,
                        "messages": [
                            {"role": "system", "content": "You are an expert quantitative analyst and technical analyst with deep knowledge of financial markets, chart patterns, and statistical analysis."},
                            {"role": "user", "content": prompt}
                        ],
                        "temperature": 0.3,
                        "max_tokens": 2000
                    }
                    
                    async with session.post(
                        f"{self.base_url}/chat/completions",
                        headers=headers,
                        json=payload,
                        timeout=aiohttp.ClientTimeout(total=60)
                    ) as response:
                        if response.status == 200:
                            result = await response.json()
                            return result["choices"][0]["message"]["content"]
                        else:
                            logger.error(f"LLM API error: {response.status}")
                            return ""
                            
            except Exception as e:
                logger.error(f"LLM API call failed: {e}")
                return ""
    
    def _parse_pattern_analysis(self, response: str) -> PatternAnalysis:
        """Parse LLM response into PatternAnalysis object."""
        try:
            # Try to extract JSON from response
            json_start = response.find('{')
            json_end = response.rfind('}') + 1
            
            if json_start >= 0 and json_end > json_start:
                json_str = response[json_start:json_end]
                data = json.loads(json_str)
                
                return PatternAnalysis(
                    pattern_type=data.get("pattern_type", "unknown"),
                    confidence=float(data.get("confidence", 0.5)),
                    description=data.get("description", ""),
                    technical_indicators=data.get("technical_indicators", {}),
                    predicted_direction=data.get("predicted_direction", "neutral"),
                    support_resistance=data.get("support_resistance", {}),
                    volume_analysis=data.get("volume_analysis", ""),
                    risk_assessment=data.get("risk_assessment", ""),
                    timeframe_relevance=data.get("timeframe_relevance", [])
                )
            
        except Exception as e:
            logger.warning(f"Failed to parse LLM response: {e}")
        
        return self._create_fallback_analysis()
    
    def _parse_regime_analysis(self, response: str) -> MarketRegimeAnalysis:
        """Parse LLM response into MarketRegimeAnalysis object."""
        try:
            json_start = response.find('{')
            json_end = response.rfind('}') + 1
            
            if json_start >= 0 and json_end > json_start:
                json_str = response[json_start:json_end]
                data = json.loads(json_str)
                
                return MarketRegimeAnalysis(
                    regime_type=data.get("regime_type", "unknown"),
                    confidence=float(data.get("confidence", 0.5)),
                    characteristics=data.get("characteristics", []),
                    typical_duration=data.get("typical_duration", "unknown"),
                    trading_implications=data.get("trading_implications", []),
                    risk_factors=data.get("risk_factors", [])
                )
            
        except Exception as e:
            logger.warning(f"Failed to parse regime analysis: {e}")
        
        return self._create_fallback_regime()
    
    def _create_fallback_analysis(self) -> PatternAnalysis:
        """Create fallback analysis when LLM fails."""
        return PatternAnalysis(
            pattern_type="unknown",
            confidence=0.5,
            description="Pattern analysis unavailable",
            technical_indicators={},
            predicted_direction="neutral",
            support_resistance={},
            volume_analysis="",
            risk_assessment="",
            timeframe_relevance=[]
        )
    
    def _create_fallback_regime(self) -> MarketRegimeAnalysis:
        """Create fallback regime analysis when LLM fails."""
        return MarketRegimeAnalysis(
            regime_type="unknown",
            confidence=0.5,
            characteristics=[],
            typical_duration="unknown",
            trading_implications=[],
            risk_factors=[]
        )
    
    def _calculate_pattern_strength(self, analysis: PatternAnalysis) -> float:
        """Calculate pattern strength score from analysis."""
        strength = analysis.confidence
        
        # Adjust based on technical indicators
        if analysis.technical_indicators:
            momentum = analysis.technical_indicators.get("momentum", "neutral")
            if momentum == "strong":
                strength *= 1.2
            elif momentum == "weak":
                strength *= 0.8
            
            volume = analysis.technical_indicators.get("volume_confirmation", "neutral")
            if volume == "confirmed":
                strength *= 1.1
        
        return min(strength, 1.0)
    
    async def _get_llm_mathematical_features(self, price_data: pd.DataFrame) -> Dict[str, float]:
        """Get additional mathematical features suggested by LLM."""
        # This could be expanded to ask LLM for feature suggestions
        # For now, implement some advanced mathematical features
        
        features = {}
        
        if 'close' in price_data.columns:
            prices = price_data['close']
            
            # Fractal dimension (complexity measure)
            features['llm_fractal_dimension'] = self._calculate_fractal_dimension(prices)
            
            # Hurst exponent (trend persistence)
            features['llm_hurst_exponent'] = self._calculate_hurst_exponent(prices)
            
            # Price acceleration
            returns = prices.pct_change()
            features['llm_price_acceleration'] = returns.diff().iloc[-1] if len(returns) > 1 else 0
        
        return features
    
    def _calculate_fractal_dimension(self, prices: pd.Series) -> float:
        """Calculate fractal dimension using box-counting method."""
        try:
            if len(prices) < 10:
                return 1.5
            
            # Simplified fractal dimension calculation
            returns = prices.pct_change().dropna()
            
            # Calculate relative range
            cumulative_returns = returns.cumsum()
            range_returns = cumulative_returns.max() - cumulative_returns.min()
            std_returns = returns.std()
            
            if std_returns == 0:
                return 1.5
            
            rescaled_range = range_returns / std_returns
            n = len(returns)
            
            # Hurst exponent approximation
            hurst = np.log(rescaled_range) / np.log(n) if n > 1 else 0.5
            
            # Fractal dimension = 2 - Hurst exponent
            fractal_dim = 2 - hurst
            
            return max(1.0, min(2.0, fractal_dim))
            
        except Exception:
            return 1.5
    
    def _calculate_hurst_exponent(self, prices: pd.Series) -> float:
        """Calculate Hurst exponent for trend persistence."""
        try:
            if len(prices) < 10:
                return 0.5
            
            returns = prices.pct_change().dropna()
            
            # R/S analysis
            mean_return = returns.mean()
            cumulative_deviations = (returns - mean_return).cumsum()
            
            range_rs = cumulative_deviations.max() - cumulative_deviations.min()
            std_rs = returns.std()
            
            if std_rs == 0:
                return 0.5
            
            rs_ratio = range_rs / std_rs
            n = len(returns)
            
            hurst = np.log(rs_ratio) / np.log(n) if n > 1 else 0.5
            
            return max(0.0, min(1.0, hurst))
            
        except Exception:
            return 0.5
    
    async def _analyze_pattern_characteristics(self, 
                                             patterns: List[pd.DataFrame],
                                             pattern_type: str) -> Dict[str, Any]:
        """Analyze characteristics of base patterns using LLM."""
        if not patterns:
            return {}
        
        # Sample a few patterns for analysis
        sample_patterns = patterns[:5]
        
        # Create summary of pattern characteristics
        characteristics_prompt = f"""Analyze these {pattern_type} patterns and identify key mathematical and statistical characteristics:

Pattern Data Summary:
"""
        
        for i, pattern in enumerate(sample_patterns):
            if 'close' in pattern.columns:
                prices = pattern['close']
                characteristics_prompt += f"""
Pattern {i+1}:
- Duration: {len(pattern)} periods
- Total return: {(prices.iloc[-1] / prices.iloc[0] - 1):.2%}
- Volatility: {prices.pct_change().std():.4f}
- Max drawdown: {self._calculate_max_drawdown(prices):.2%}
"""
        
        characteristics_prompt += f"""
Identify the key characteristics that define a {pattern_type} pattern in JSON format:
{{
    "typical_duration_range": [min_periods, max_periods],
    "return_characteristics": {{"min": -0.05, "max": 0.15, "typical": 0.03}},
    "volatility_profile": "description",
    "key_mathematical_features": ["feature1", "feature2"],
    "generation_parameters": {{"parameter1": value1, "parameter2": value2}}
}}"""
        
        try:
            response = await self._call_llm(characteristics_prompt)
            json_start = response.find('{')
            json_end = response.rfind('}') + 1
            
            if json_start >= 0 and json_end > json_start:
                return json.loads(response[json_start:json_end])
        except Exception as e:
            logger.warning(f"Failed to analyze pattern characteristics: {e}")
        
        return {}
    
    async def _generate_pattern_batch(self,
                                    characteristics: Dict[str, Any],
                                    batch_size: int,
                                    pattern_type: str) -> List[pd.DataFrame]:
        """Generate a batch of synthetic patterns."""
        patterns = []
        
        # Use characteristics to generate synthetic patterns
        # This is a simplified implementation - in practice, you'd use the LLM
        # to guide the generation process more sophisticatedly
        
        for _ in range(batch_size):
            try:
                # Generate synthetic pattern based on characteristics
                synthetic_pattern = self._generate_single_synthetic_pattern(
                    characteristics, pattern_type
                )
                if synthetic_pattern is not None:
                    patterns.append(synthetic_pattern)
            except Exception as e:
                logger.warning(f"Failed to generate synthetic pattern: {e}")
        
        return patterns
    
    def _generate_single_synthetic_pattern(self,
                                         characteristics: Dict[str, Any],
                                         pattern_type: str) -> Optional[pd.DataFrame]:
        """Generate a single synthetic pattern."""
        try:
            # Simplified synthetic pattern generation
            # In practice, this would use more sophisticated methods guided by LLM analysis
            
            duration = np.random.randint(10, 30)  # Random duration
            
            # Generate base price movement
            returns = np.random.normal(0, 0.02, duration)  # Random walk base
            
            # Add pattern-specific characteristics
            if pattern_type == "bullish_reversal":
                # Add initial decline followed by recovery
                returns[:duration//3] *= -1.5  # Initial decline
                returns[duration//3:] *= 1.5   # Recovery
            elif pattern_type == "bearish_reversal":
                returns[:duration//3] *= 1.5   # Initial rise
                returns[duration//3:] *= -1.5  # Decline
            
            # Generate prices
            prices = 100 * (1 + returns).cumprod()
            
            # Create DataFrame
            dates = pd.date_range('2024-01-01', periods=duration)
            
            # Generate OHLCV data
            synthetic_data = pd.DataFrame({
                'open': prices * (1 + np.random.normal(0, 0.001, duration)),
                'high': prices * (1 + np.abs(np.random.normal(0, 0.005, duration))),
                'low': prices * (1 - np.abs(np.random.normal(0, 0.005, duration))),
                'close': prices,
                'volume': np.random.lognormal(10, 0.5, duration).astype(int)
            }, index=dates)
            
            # Ensure OHLC consistency
            synthetic_data['high'] = np.maximum(synthetic_data['high'], 
                                              np.maximum(synthetic_data['open'], synthetic_data['close']))
            synthetic_data['low'] = np.minimum(synthetic_data['low'], 
                                             np.minimum(synthetic_data['open'], synthetic_data['close']))
            
            return synthetic_data
            
        except Exception as e:
            logger.warning(f"Failed to generate single synthetic pattern: {e}")
            return None


# Convenience functions for easy integration

async def analyze_stock_pattern(price_data: pd.DataFrame, 
                              symbol: str,
                              api_key: str,
                              provider: LLMProvider = LLMProvider.DEEPSEEK) -> PatternAnalysis:
    """Convenience function to analyze a stock pattern."""
    recognizer = LLMPatternRecognizer(provider=provider, api_key=api_key)
    return await recognizer.analyze_price_pattern(price_data, symbol)


async def enhance_features_with_llm(price_data: pd.DataFrame,
                                  existing_features: Dict[str, float],
                                  api_key: str,
                                  provider: LLMProvider = LLMProvider.DEEPSEEK) -> Dict[str, float]:
    """Convenience function to enhance features with LLM."""
    recognizer = LLMPatternRecognizer(provider=provider, api_key=api_key)
    return await recognizer.enhance_feature_engineering(price_data, existing_features)


async def generate_training_data_with_llm(existing_patterns: List[pd.DataFrame],
                                        target_count: int,
                                        pattern_type: str,
                                        api_key: str,
                                        provider: LLMProvider = LLMProvider.DEEPSEEK) -> List[pd.DataFrame]:
    """Generate synthetic training data using LLM guidance."""
    recognizer = LLMPatternRecognizer(provider=provider, api_key=api_key)
    return await recognizer.generate_synthetic_patterns(
        existing_patterns, target_count, pattern_type
    )