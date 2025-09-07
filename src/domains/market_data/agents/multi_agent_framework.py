#!/usr/bin/env python3
"""
Multi-Agent Analysis Framework

This framework implements specialized LLM agents for comprehensive financial news analysis.
Each agent focuses on a specific domain of expertise, working together to provide
comprehensive market intelligence and signal generation.

Agent Types:
1. Sentiment Agent - Advanced sentiment analysis with market context
2. Entity Recognition Agent - Financial entity extraction and classification
3. Event Detection Agent - Corporate actions and market-moving events
4. Risk Assessment Agent - Risk evaluation and uncertainty quantification
5. Market Impact Agent - Price/volume impact prediction
6. Signal Generation Agent - Trading signal synthesis and validation

The framework uses ensemble methods to combine agent outputs for robust signal generation.
"""

import asyncio
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional, Union, Tuple
from dataclasses import dataclass, field
from enum import Enum
import json
from abc import ABC, abstractmethod

from infrastructure.llm.multi_provider_client import MultiProviderLLMClient, LLMResponse
from domains.market_data.services.llm.news_llm_processor import NewsArticle, NewsAnalysisResult

logger = logging.getLogger(__name__)


class AgentType(Enum):
    """Types of specialized analysis agents."""
    SENTIMENT = "sentiment"
    ENTITY_RECOGNITION = "entity_recognition"
    EVENT_DETECTION = "event_detection"
    RISK_ASSESSMENT = "risk_assessment"
    MARKET_IMPACT = "market_impact"
    SIGNAL_GENERATION = "signal_generation"


class ConfidenceLevel(Enum):
    """Confidence levels for agent analyses."""
    VERY_HIGH = "very_high"  # 0.9-1.0
    HIGH = "high"           # 0.7-0.9
    MEDIUM = "medium"       # 0.5-0.7
    LOW = "low"            # 0.3-0.5
    VERY_LOW = "very_low"  # 0.0-0.3


@dataclass
class AgentAnalysis:
    """Base class for agent analysis results."""
    agent_type: AgentType
    confidence: float
    processing_time_ms: int
    model_used: str
    timestamp: datetime = field(default_factory=datetime.now)

    # Analysis-specific data (subclasses will extend this)
    analysis_data: Dict[str, Any] = field(default_factory=dict)
    reasoning: str = ""
    warnings: List[str] = field(default_factory=list)


@dataclass
class SentimentAnalysis(AgentAnalysis):
    """Sentiment analysis results."""
    sentiment_score: float  # -1.0 (very bearish) to 1.0 (very bullish)
    sentiment_label: str    # "very_bearish", "bearish", "neutral", "bullish", "very_bullish"
    emotional_indicators: Dict[str, float]  # fear, greed, uncertainty, etc.
    sentiment_strength: float  # 0.0 to 1.0
    market_sentiment_context: str  # How it relates to broader market sentiment


@dataclass
class EntityRecognitionAnalysis(AgentAnalysis):
    """Entity recognition analysis results."""
    companies: List[Dict[str, Any]]  # Company entities with metadata
    people: List[Dict[str, Any]]     # Person entities (executives, analysts)
    financial_products: List[Dict[str, Any]]  # Bonds, derivatives, etc.
    geographic_locations: List[Dict[str, Any]]  # Countries, regions
    regulatory_bodies: List[Dict[str, Any]]     # SEC, FDA, etc.
    entity_relationships: List[Dict[str, Any]]  # Relationships between entities


@dataclass
class EventDetectionAnalysis(AgentAnalysis):
    """Event detection analysis results."""
    events: List[Dict[str, Any]]  # Detected events with classification
    event_categories: List[str]   # earnings, M&A, regulatory, etc.
    event_timeline: List[Dict[str, Any]]  # Temporal sequence of events
    event_importance: Dict[str, float]    # Importance score per event type
    causal_relationships: List[Dict[str, Any]]  # Event causality chains


@dataclass
class RiskAssessmentAnalysis(AgentAnalysis):
    """Risk assessment analysis results."""
    overall_risk_score: float  # 0.0 (low risk) to 1.0 (high risk)
    risk_categories: Dict[str, float]  # market, credit, operational, etc.
    uncertainty_factors: List[Dict[str, Any]]  # Sources of uncertainty
    black_swan_probability: float  # Probability of extreme event
    risk_mitigation_suggestions: List[str]


@dataclass
class MarketImpactAnalysis(AgentAnalysis):
    """Market impact prediction analysis."""
    price_impact_prediction: Dict[str, float]  # 1h, 1d, 5d, 20d predictions
    volatility_impact: float  # Expected volatility increase
    volume_impact: float      # Expected volume increase
    sector_spillover: Dict[str, float]  # Impact on related sectors
    market_timing: str        # immediate, short_term, medium_term, long_term


@dataclass
class SignalGenerationAnalysis(AgentAnalysis):
    """Signal generation analysis results."""
    signal_strength: float    # -1.0 (strong sell) to 1.0 (strong buy)
    signal_direction: str     # buy, sell, hold, hedge
    urgency_level: int       # 1 (low) to 10 (critical)
    time_horizon: str        # intraday, short, medium, long
    position_sizing: float   # 0.0 to 1.0 (fraction of portfolio)
    stop_loss: Optional[float]
    take_profit: Optional[float]
    supporting_factors: List[str]
    risk_factors: List[str]


class BaseFinancialAgent(ABC):
    """Base class for all financial analysis agents."""

    def __init__(self, agent_type: AgentType, llm_client: MultiProviderLLMClient,
                 model_preference: Optional[str] = None):
        self.agent_type = agent_type
        self.llm_client = llm_client
        self.model_preference = model_preference

        # Performance tracking
        self.analysis_count = 0
        self.total_processing_time_ms = 0
        self.error_count = 0

    @abstractmethod
    def get_system_prompt(self) -> str:
        """Get the system prompt for this agent."""
        pass

    @abstractmethod
    def create_analysis_prompt(self, article: NewsArticle, context: Dict[str, Any] = None) -> str:
        """Create analysis prompt for the given article."""
        pass

    @abstractmethod
    def parse_llm_response(self, response: LLMResponse, processing_time_ms: int) -> AgentAnalysis:
        """Parse LLM response into structured analysis."""
        pass

    async def analyze(self, article: NewsArticle, context: Dict[str, Any] = None) -> AgentAnalysis:
        """Perform analysis on the given article."""
        start_time = asyncio.get_event_loop().time()

        try:
            # Create prompts
            system_prompt = self.get_system_prompt()
            analysis_prompt = self.create_analysis_prompt(article, context)

            # Get LLM response
            response = await self.llm_client.generate_response(
                prompt=analysis_prompt,
                system_prompt=system_prompt,
                model_preference=self.model_preference,
                temperature=0.1,  # Low temperature for consistent analysis
                max_tokens=1500
            )

            processing_time_ms = int((asyncio.get_event_loop().time() - start_time) * 1000)

            # Parse response
            analysis = self.parse_llm_response(response, processing_time_ms)

            # Update metrics
            self.analysis_count += 1
            self.total_processing_time_ms += processing_time_ms

            logger.debug(f"{self.agent_type.value} analysis completed in {processing_time_ms}ms")

            return analysis

        except Exception as e:
            self.error_count += 1
            logger.error(f"Error in {self.agent_type.value} agent: {e}")

            # Return error analysis
            processing_time_ms = int((asyncio.get_event_loop().time() - start_time) * 1000)
            return AgentAnalysis(
                agent_type=self.agent_type,
                confidence=0.0,
                processing_time_ms=processing_time_ms,
                model_used="error",
                reasoning=f"Analysis failed: {str(e)}",
                warnings=[f"Agent processing error: {str(e)}"]
            )

    def get_performance_metrics(self) -> Dict[str, Any]:
        """Get agent performance metrics."""
        avg_processing_time = (
            self.total_processing_time_ms / self.analysis_count
            if self.analysis_count > 0 else 0
        )

        return {
            'agent_type': self.agent_type.value,
            'analysis_count': self.analysis_count,
            'error_count': self.error_count,
            'error_rate': self.error_count / max(self.analysis_count, 1),
            'avg_processing_time_ms': avg_processing_time,
            'total_processing_time_ms': self.total_processing_time_ms
        }


class SentimentAgent(BaseFinancialAgent):
    """Specialized agent for sentiment analysis."""

    def __init__(self, llm_client: MultiProviderLLMClient):
        super().__init__(AgentType.SENTIMENT, llm_client, model_preference="openai")

    def get_system_prompt(self) -> str:
        return """You are a specialized financial sentiment analysis agent. Your expertise includes:

        - Advanced sentiment analysis beyond basic positive/negative classification
        - Understanding market psychology and behavioral finance principles
        - Contextualizing news sentiment within current market conditions
        - Identifying emotional indicators (fear, greed, uncertainty, optimism)
        - Recognizing sentiment patterns that drive market movements

        Analyze financial news with precision, providing:
        1. Numerical sentiment score (-1.0 to 1.0)
        2. Sentiment strength (how confident the sentiment is)
        3. Emotional indicators and their intensities
        4. Market psychology context
        5. Potential sentiment-driven price impacts

        Be objective and evidence-based. Consider market context and timing."""

    def create_analysis_prompt(self, article: NewsArticle, context: Dict[str, Any] = None) -> str:
        market_context = ""
        if context and context.get('market_session'):
            market_context = f"Market Session: {context['market_session']}\n"

        return f"""Analyze the sentiment of this financial news article:

{market_context}
Title: {article.title}
Content: {article.content}
Tickers: {', '.join(article.tickers) if article.tickers else 'None'}

Provide your analysis in JSON format:
{{
    "sentiment_score": <float from -1.0 to 1.0>,
    "sentiment_label": "<very_bearish|bearish|neutral|bullish|very_bullish>",
    "sentiment_strength": <float from 0.0 to 1.0>,
    "emotional_indicators": {{
        "fear": <0.0 to 1.0>,
        "greed": <0.0 to 1.0>,
        "uncertainty": <0.0 to 1.0>,
        "optimism": <0.0 to 1.0>,
        "panic": <0.0 to 1.0>
    }},
    "market_sentiment_context": "<explanation of how this fits into current market sentiment>",
    "reasoning": "<detailed explanation of sentiment analysis>",
    "confidence": <float from 0.0 to 1.0>
}}"""

    def parse_llm_response(self, response: LLMResponse, processing_time_ms: int) -> SentimentAnalysis:
        try:
            data = json.loads(response.content)

            return SentimentAnalysis(
                agent_type=self.agent_type,
                confidence=data.get('confidence', 0.5),
                processing_time_ms=processing_time_ms,
                model_used=response.model_used,
                sentiment_score=data.get('sentiment_score', 0.0),
                sentiment_label=data.get('sentiment_label', 'neutral'),
                sentiment_strength=data.get('sentiment_strength', 0.5),
                emotional_indicators=data.get('emotional_indicators', {}),
                market_sentiment_context=data.get('market_sentiment_context', ''),
                reasoning=data.get('reasoning', '')
            )
        except Exception as e:
            return SentimentAnalysis(
                agent_type=self.agent_type,
                confidence=0.0,
                processing_time_ms=processing_time_ms,
                model_used=response.model_used,
                sentiment_score=0.0,
                sentiment_label='neutral',
                sentiment_strength=0.0,
                emotional_indicators={},
                market_sentiment_context='',
                reasoning=f"Failed to parse response: {str(e)}",
                warnings=[f"Response parsing error: {str(e)}"]
            )


class EntityRecognitionAgent(BaseFinancialAgent):
    """Specialized agent for financial entity recognition."""

    def __init__(self, llm_client: MultiProviderLLMClient):
        super().__init__(AgentType.ENTITY_RECOGNITION, llm_client, model_preference="anthropic")

    def get_system_prompt(self) -> str:
        return """You are a specialized financial entity recognition agent. Your expertise includes:

        - Identifying and classifying financial entities (companies, people, products, locations)
        - Understanding corporate hierarchies and ownership structures
        - Recognizing regulatory bodies and their jurisdictions
        - Mapping entity relationships and dependencies
        - Extracting key financial metrics and ratios mentioned

        Extract and classify entities with high precision, providing:
        1. Complete entity identification with proper names and identifiers
        2. Entity classification and sub-classification
        3. Relationship mapping between entities
        4. Confidence scores for each entity extraction
        5. Context around why entities are relevant to the news

        Focus on accuracy over completeness. High-confidence extractions only."""

    def create_analysis_prompt(self, article: NewsArticle, context: Dict[str, Any] = None) -> str:
        return f"""Extract and classify financial entities from this news article:

Title: {article.title}
Content: {article.content}
Known Tickers: {', '.join(article.tickers) if article.tickers else 'None'}

Identify entities in JSON format:
{{
    "companies": [
        {{
            "name": "<company name>",
            "ticker": "<stock symbol if mentioned>",
            "entity_type": "<public|private|subsidiary|parent>",
            "industry": "<industry sector>",
            "confidence": <0.0 to 1.0>
        }}
    ],
    "people": [
        {{
            "name": "<person name>",
            "role": "<title/position>",
            "organization": "<company/organization>",
            "confidence": <0.0 to 1.0>
        }}
    ],
    "financial_products": [
        {{
            "name": "<product name>",
            "type": "<stock|bond|derivative|fund|etc>",
            "description": "<brief description>",
            "confidence": <0.0 to 1.0>
        }}
    ],
    "geographic_locations": [
        {{
            "name": "<location name>",
            "type": "<country|state|city|region>",
            "relevance": "<why this location matters>",
            "confidence": <0.0 to 1.0>
        }}
    ],
    "regulatory_bodies": [
        {{
            "name": "<organization name>",
            "jurisdiction": "<regulatory scope>",
            "relevance": "<why mentioned>",
            "confidence": <0.0 to 1.0>
        }}
    ],
    "entity_relationships": [
        {{
            "entity1": "<entity name>",
            "relationship": "<owns|subsidiary|partner|competitor|etc>",
            "entity2": "<entity name>",
            "confidence": <0.0 to 1.0>
        }}
    ],
    "overall_confidence": <0.0 to 1.0>
}}"""

    def parse_llm_response(self, response: LLMResponse, processing_time_ms: int) -> EntityRecognitionAnalysis:
        try:
            data = json.loads(response.content)

            return EntityRecognitionAnalysis(
                agent_type=self.agent_type,
                confidence=data.get('overall_confidence', 0.5),
                processing_time_ms=processing_time_ms,
                model_used=response.model_used,
                companies=data.get('companies', []),
                people=data.get('people', []),
                financial_products=data.get('financial_products', []),
                geographic_locations=data.get('geographic_locations', []),
                regulatory_bodies=data.get('regulatory_bodies', []),
                entity_relationships=data.get('entity_relationships', [])
            )
        except Exception as e:
            return EntityRecognitionAnalysis(
                agent_type=self.agent_type,
                confidence=0.0,
                processing_time_ms=processing_time_ms,
                model_used=response.model_used,
                companies=[], people=[], financial_products=[],
                geographic_locations=[], regulatory_bodies=[],
                entity_relationships=[],
                warnings=[f"Response parsing error: {str(e)}"]
            )


class EventDetectionAgent(BaseFinancialAgent):
    """Specialized agent for detecting financial events."""

    def __init__(self, llm_client: MultiProviderLLMClient):
        super().__init__(AgentType.EVENT_DETECTION, llm_client, model_preference="google")

    def get_system_prompt(self) -> str:
        return """You are a specialized financial event detection agent. Your expertise includes:

        - Identifying market-moving events (earnings, M&A, regulatory changes, etc.)
        - Classifying event types and their typical market impacts
        - Understanding event timing and sequencing
        - Detecting causal relationships between events
        - Assessing event importance and market significance

        Detect and classify events with precision, providing:
        1. Complete event identification with proper classification
        2. Event timing and expected duration
        3. Importance scoring based on historical market impact
        4. Causal event chains and dependencies
        5. Affected market participants and sectors

        Focus on events that drive significant market movements."""

    def create_analysis_prompt(self, article: NewsArticle, context: Dict[str, Any] = None) -> str:
        return f"""Detect and classify financial events in this news article:

Title: {article.title}
Content: {article.content}
Published: {article.published_date}
Tickers: {', '.join(article.tickers) if article.tickers else 'None'}

Extract events in JSON format:
{{
    "events": [
        {{
            "event_type": "<earnings|merger|acquisition|ipo|bankruptcy|regulatory|fda_approval|etc>",
            "event_name": "<descriptive name>",
            "description": "<detailed description>",
            "affected_entities": ["<entity1>", "<entity2>"],
            "event_timing": "<past|present|future|ongoing>",
            "expected_date": "<date if mentioned>",
            "importance_score": <0.0 to 1.0>,
            "market_impact_expectation": "<positive|negative|neutral|mixed>",
            "confidence": <0.0 to 1.0>
        }}
    ],
    "event_categories": ["<primary categories of events detected>"],
    "event_timeline": [
        {{
            "sequence": <order number>,
            "event": "<event name>",
            "timing": "<when this occurs in the timeline>"
        }}
    ],
    "causal_relationships": [
        {{
            "cause_event": "<event that causes>",
            "effect_event": "<event that is caused>",
            "causality_strength": <0.0 to 1.0>,
            "explanation": "<how one leads to another>"
        }}
    ],
    "overall_confidence": <0.0 to 1.0>
}}"""

    def parse_llm_response(self, response: LLMResponse, processing_time_ms: int) -> EventDetectionAnalysis:
        try:
            data = json.loads(response.content)

            # Calculate event importance scores
            event_importance = {}
            for event in data.get('events', []):
                event_type = event.get('event_type', 'unknown')
                importance = event.get('importance_score', 0.5)
                event_importance[event_type] = max(event_importance.get(event_type, 0.0), importance)

            return EventDetectionAnalysis(
                agent_type=self.agent_type,
                confidence=data.get('overall_confidence', 0.5),
                processing_time_ms=processing_time_ms,
                model_used=response.model_used,
                events=data.get('events', []),
                event_categories=data.get('event_categories', []),
                event_timeline=data.get('event_timeline', []),
                event_importance=event_importance,
                causal_relationships=data.get('causal_relationships', [])
            )
        except Exception as e:
            return EventDetectionAnalysis(
                agent_type=self.agent_type,
                confidence=0.0,
                processing_time_ms=processing_time_ms,
                model_used=response.model_used,
                events=[], event_categories=[], event_timeline=[],
                event_importance={}, causal_relationships=[],
                warnings=[f"Response parsing error: {str(e)}"]
            )


class MultiAgentAnalysisOrchestrator:
    """Orchestrates analysis across multiple specialized agents."""

    def __init__(self, llm_client: MultiProviderLLMClient):
        self.llm_client = llm_client

        # Initialize specialized agents
        self.agents = {
            AgentType.SENTIMENT: SentimentAgent(llm_client),
            AgentType.ENTITY_RECOGNITION: EntityRecognitionAgent(llm_client),
            AgentType.EVENT_DETECTION: EventDetectionAgent(llm_client),
            # Additional agents will be added here
        }

        # Analysis coordination settings
        self.parallel_execution = True
        self.timeout_seconds = 30

        # Performance tracking
        self.orchestration_count = 0
        self.total_orchestration_time_ms = 0

    async def analyze_article(self, article: NewsArticle,
                            context: Dict[str, Any] = None) -> Dict[AgentType, AgentAnalysis]:
        """Orchestrate comprehensive analysis across all agents."""
        start_time = asyncio.get_event_loop().time()

        try:
            if self.parallel_execution:
                # Run all agents in parallel for speed
                tasks = []
                for agent_type, agent in self.agents.items():
                    task = asyncio.create_task(agent.analyze(article, context))
                    tasks.append((agent_type, task))

                # Wait for all analyses with timeout
                results = {}
                done_tasks = await asyncio.wait_for(
                    asyncio.gather(*[task for _, task in tasks], return_exceptions=True),
                    timeout=self.timeout_seconds
                )

                # Collect results
                for i, (agent_type, _) in enumerate(tasks):
                    result = done_tasks[i]
                    if isinstance(result, Exception):
                        logger.error(f"Agent {agent_type.value} failed: {result}")
                        # Create error analysis
                        results[agent_type] = AgentAnalysis(
                            agent_type=agent_type,
                            confidence=0.0,
                            processing_time_ms=int(self.timeout_seconds * 1000),
                            model_used="error",
                            reasoning=f"Agent timeout or error: {str(result)}"
                        )
                    else:
                        results[agent_type] = result

            else:
                # Run agents sequentially
                results = {}
                for agent_type, agent in self.agents.items():
                    try:
                        analysis = await asyncio.wait_for(
                            agent.analyze(article, context),
                            timeout=self.timeout_seconds
                        )
                        results[agent_type] = analysis
                    except Exception as e:
                        logger.error(f"Agent {agent_type.value} failed: {e}")
                        results[agent_type] = AgentAnalysis(
                            agent_type=agent_type,
                            confidence=0.0,
                            processing_time_ms=int(self.timeout_seconds * 1000),
                            model_used="error",
                            reasoning=f"Agent error: {str(e)}"
                        )

            # Update metrics
            orchestration_time_ms = int((asyncio.get_event_loop().time() - start_time) * 1000)
            self.orchestration_count += 1
            self.total_orchestration_time_ms += orchestration_time_ms

            logger.info(f"Multi-agent analysis completed in {orchestration_time_ms}ms")
            logger.debug(f"Agent results: {[f'{k.value}({v.confidence:.2f})' for k, v in results.items()]}")

            return results

        except Exception as e:
            logger.error(f"Multi-agent orchestration failed: {e}")

            # Return error results for all agents
            error_results = {}
            for agent_type in self.agents.keys():
                error_results[agent_type] = AgentAnalysis(
                    agent_type=agent_type,
                    confidence=0.0,
                    processing_time_ms=0,
                    model_used="error",
                    reasoning=f"Orchestration failed: {str(e)}"
                )

            return error_results

    def get_orchestration_metrics(self) -> Dict[str, Any]:
        """Get orchestration performance metrics."""
        avg_orchestration_time = (
            self.total_orchestration_time_ms / self.orchestration_count
            if self.orchestration_count > 0 else 0
        )

        agent_metrics = {}
        for agent_type, agent in self.agents.items():
            agent_metrics[agent_type.value] = agent.get_performance_metrics()

        return {
            'orchestration_count': self.orchestration_count,
            'avg_orchestration_time_ms': avg_orchestration_time,
            'parallel_execution': self.parallel_execution,
            'timeout_seconds': self.timeout_seconds,
            'agent_metrics': agent_metrics
        }

    def get_ensemble_confidence(self, analyses: Dict[AgentType, AgentAnalysis]) -> float:
        """Calculate ensemble confidence across all agent analyses."""
        if not analyses:
            return 0.0

        # Weight confidence by agent reliability and consistency
        total_confidence = 0.0
        total_weight = 0.0

        for agent_type, analysis in analyses.items():
            # Base weight for each agent type
            agent_weight = 1.0

            # Adjust weight based on historical performance
            agent_metrics = self.agents[agent_type].get_performance_metrics()
            error_rate = agent_metrics.get('error_rate', 0.0)
            reliability_factor = max(0.1, 1.0 - error_rate)

            weighted_confidence = analysis.confidence * agent_weight * reliability_factor
            total_confidence += weighted_confidence
            total_weight += agent_weight * reliability_factor

        return total_confidence / total_weight if total_weight > 0 else 0.0


# Factory function to create orchestrator
async def create_multi_agent_orchestrator(llm_client: MultiProviderLLMClient) -> MultiAgentAnalysisOrchestrator:
    """Create and initialize multi-agent analysis orchestrator."""
    orchestrator = MultiAgentAnalysisOrchestrator(llm_client)

    logger.info(f"Multi-agent orchestrator created with {len(orchestrator.agents)} agents")

    return orchestrator