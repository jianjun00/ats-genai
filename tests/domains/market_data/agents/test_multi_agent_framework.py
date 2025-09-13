#!/usr/bin/env python3
"""
Comprehensive tests for Multi-Agent Analysis Framework

This test suite covers:
- Individual agent functionality and prompt generation
- Agent response parsing and error handling
- Multi-agent orchestration and coordination
- Ensemble confidence calculation
- Performance tracking and metrics
- Agent specialization and accuracy
"""

import pytest
import asyncio
import json
from unittest.mock import AsyncMock, MagicMock
from datetime import datetime, timedelta

from domains.market_data.agents.multi_agent_framework import (
    AgentType, AgentAnalysis, SentimentAgent, EntityRecognitionAgent,
    EventDetectionAgent, SentimentAnalysis, EntityRecognitionAnalysis,
    EventDetectionAnalysis, MultiAgentAnalysisOrchestrator
)
from domains.market_data.agents.specialized_agents import (
    RiskAssessmentAgent, MarketImpactAgent, SignalGenerationAgent,
    RiskAssessmentAnalysis, MarketImpactAnalysis, SignalGenerationAnalysis,
    EnhancedMultiAgentOrchestrator
)
from domains.market_data.services.llm.news_llm_processor import NewsArticle
from infrastructure.llm.multi_provider_client import MultiProviderLLMClient, LLMResponse


class TestBaseFinancialAgent:
    """Tests for the base financial agent functionality."""

    @pytest.fixture
    def mock_llm_client(self):
        """Mock LLM client for testing."""
        client = AsyncMock(spec=MultiProviderLLMClient)
        return client

    @pytest.fixture
    def sample_news_article(self):
        """Sample news article for testing."""
        return NewsArticle(
            id="test_article_123",
            title="Apple Reports Strong Q4 Earnings with Revenue Beat",
            content="Apple Inc. reported quarterly earnings that exceeded analyst expectations, with revenue of $85.2 billion compared to expected $82.1 billion. The company saw strong iPhone sales and services growth. CEO Tim Cook highlighted strong performance in emerging markets and continued growth in services revenue, which now represents 23% of total revenue. However, the company noted supply chain challenges that may impact Q1 2025 production.",
            summary="Apple beats Q4 earnings expectations with strong iPhone sales",
            url="https://example.com/news/apple-earnings",
            source="Reuters",
            published_date=datetime.now(),
            tickers=["AAPL"],
            raw_data={"category": "earnings", "importance": "high"}
        )

    def test_base_agent_initialization(self, mock_llm_client):
        """Test base agent initialization."""
        agent = SentimentAgent(mock_llm_client)

        assert agent.agent_type == AgentType.SENTIMENT
        assert agent.llm_client == mock_llm_client
        assert agent.analysis_count == 0
        assert agent.total_processing_time_ms == 0
        assert agent.error_count == 0

    def test_agent_performance_metrics(self, mock_llm_client):
        """Test agent performance metrics tracking."""
        agent = SentimentAgent(mock_llm_client)

        # Simulate some processing
        agent.analysis_count = 10
        agent.total_processing_time_ms = 15000
        agent.error_count = 2

        metrics = agent.get_performance_metrics()

        assert metrics['agent_type'] == 'sentiment'
        assert metrics['analysis_count'] == 10
        assert metrics['error_count'] == 2
        assert metrics['error_rate'] == 0.2
        assert metrics['avg_processing_time_ms'] == 1500
        assert metrics['total_processing_time_ms'] == 15000


class TestSentimentAgent:
    """Tests for the sentiment analysis agent."""

    @pytest.fixture
    def mock_llm_client(self):
        """Mock LLM client with sentiment response."""
        client = AsyncMock(spec=MultiProviderLLMClient)
        client.generate_response.return_value = LLMResponse(
            content=json.dumps({
                "sentiment_score": 0.8,
                "sentiment_label": "bullish",
                "sentiment_strength": 0.9,
                "emotional_indicators": {
                    "fear": 0.1,
                    "greed": 0.7,
                    "uncertainty": 0.2,
                    "optimism": 0.8,
                    "panic": 0.0
                },
                "market_sentiment_context": "Strong earnings beat drives positive sentiment",
                "reasoning": "Revenue and earnings exceeded expectations significantly",
                "confidence": 0.85
            }),
            model_used="gpt-4o-mini",
            provider="openai",
            tokens_used=250,
            processing_time_ms=1200,
            cost_estimate=0.003
        )
        return client

    @pytest.fixture
    def sample_article(self):
        """Sample article for sentiment analysis."""
        return NewsArticle(
            id="sentiment_test",
            title="Tesla Stock Surges on Strong Delivery Numbers",
            content="Tesla delivered 450,000 vehicles in Q3, beating analyst estimates of 430,000. The company also announced plans for expanded production capacity.",
            tickers=["TSLA"],
            published_date=datetime.now()
        )

    def test_sentiment_agent_system_prompt(self, mock_llm_client):
        """Test sentiment agent system prompt generation."""
        agent = SentimentAgent(mock_llm_client)

        system_prompt = agent.get_system_prompt()

        assert "sentiment analysis" in system_prompt.lower()
        assert "market psychology" in system_prompt.lower()
        assert "emotional indicators" in system_prompt.lower()
        assert "behavioral finance" in system_prompt.lower()

    def test_sentiment_analysis_prompt_creation(self, mock_llm_client, sample_article):
        """Test sentiment analysis prompt creation."""
        agent = SentimentAgent(mock_llm_client)

        prompt = agent.create_analysis_prompt(sample_article)

        assert sample_article.title in prompt
        assert sample_article.content in prompt
        assert "TSLA" in prompt
        assert "sentiment_score" in prompt
        assert "JSON format" in prompt
        assert "-1.0 to 1.0" in prompt

    def test_sentiment_analysis_prompt_with_context(self, mock_llm_client, sample_article):
        """Test sentiment analysis prompt with market context."""
        agent = SentimentAgent(mock_llm_client)
        context = {"market_session": "market_hours"}

        prompt = agent.create_analysis_prompt(sample_article, context)

        assert "Market Session: market_hours" in prompt

    @pytest.mark.asyncio
    async def test_sentiment_analysis_execution(self, mock_llm_client, sample_article):
        """Test sentiment analysis execution."""
        agent = SentimentAgent(mock_llm_client)

        result = await agent.analyze(sample_article)

        assert isinstance(result, SentimentAnalysis)
        assert result.sentiment_score == 0.8
        assert result.sentiment_label == "bullish"
        assert result.sentiment_strength == 0.9
        assert result.confidence == 0.85
        assert result.model_used == "gpt-4o-mini"
        assert result.processing_time_ms > 0

        # Check emotional indicators
        assert "fear" in result.emotional_indicators
        assert result.emotional_indicators["optimism"] == 0.8
        assert result.emotional_indicators["panic"] == 0.0

        assert "Strong earnings" in result.market_sentiment_context

    @pytest.mark.asyncio
    async def test_sentiment_analysis_error_handling(self, mock_llm_client, sample_article):
        """Test sentiment analysis error handling."""
        # Mock LLM client to raise exception
        mock_llm_client.generate_response.side_effect = Exception("API Error")

        agent = SentimentAgent(mock_llm_client)
        result = await agent.analyze(sample_article)

        assert isinstance(result, AgentAnalysis)
        assert result.confidence == 0.0
        assert result.model_used == "error"
        assert "Analysis failed" in result.reasoning
        assert len(result.warnings) > 0
        assert agent.error_count == 1

    @pytest.mark.asyncio
    async def test_sentiment_analysis_invalid_json(self, mock_llm_client, sample_article):
        """Test handling of invalid JSON response."""
        # Mock invalid JSON response
        mock_llm_client.generate_response.return_value = LLMResponse(
            content="Invalid JSON response",
            model_used="gpt-4o-mini",
            provider="openai",
            tokens_used=100,
            processing_time_ms=1000,
            cost_estimate=0.001
        )

        agent = SentimentAgent(mock_llm_client)
        result = await agent.analyze(sample_article)

        assert isinstance(result, SentimentAnalysis)
        assert result.sentiment_score == 0.0
        assert result.sentiment_label == 'neutral'
        assert len(result.warnings) > 0


class TestEntityRecognitionAgent:
    """Tests for the entity recognition agent."""

    @pytest.fixture
    def mock_llm_client(self):
        """Mock LLM client with entity response."""
        client = AsyncMock(spec=MultiProviderLLMClient)
        client.generate_response.return_value = LLMResponse(
            content=json.dumps({
                "companies": [
                    {
                        "name": "Apple Inc.",
                        "ticker": "AAPL",
                        "entity_type": "public",
                        "industry": "Technology",
                        "confidence": 0.95
                    }
                ],
                "people": [
                    {
                        "name": "Tim Cook",
                        "role": "CEO",
                        "organization": "Apple Inc.",
                        "confidence": 0.90
                    }
                ],
                "financial_products": [
                    {
                        "name": "iPhone",
                        "type": "product",
                        "description": "Smartphone product line",
                        "confidence": 0.85
                    }
                ],
                "geographic_locations": [
                    {
                        "name": "Cupertino",
                        "type": "city",
                        "relevance": "Company headquarters",
                        "confidence": 0.80
                    }
                ],
                "regulatory_bodies": [],
                "entity_relationships": [
                    {
                        "entity1": "Tim Cook",
                        "relationship": "leads",
                        "entity2": "Apple Inc.",
                        "confidence": 0.95
                    }
                ],
                "overall_confidence": 0.89
            }),
            model_used="claude-3-haiku-20240307",
            provider="anthropic",
            tokens_used=300,
            processing_time_ms=1800,
            cost_estimate=0.005
        )
        return client

    def test_entity_agent_system_prompt(self, mock_llm_client):
        """Test entity recognition agent system prompt."""
        agent = EntityRecognitionAgent(mock_llm_client)

        system_prompt = agent.get_system_prompt()

        assert "entity recognition" in system_prompt.lower()
        assert "financial entities" in system_prompt.lower()
        assert "corporate hierarchies" in system_prompt.lower()
        assert "regulatory bodies" in system_prompt.lower()

    @pytest.mark.asyncio
    async def test_entity_recognition_execution(self, mock_llm_client):
        """Test entity recognition execution."""
        agent = EntityRecognitionAgent(mock_llm_client)

        article = NewsArticle(
            id="entity_test",
            title="Apple CEO Tim Cook Announces New iPhone at Cupertino Event",
            content="At Apple's headquarters in Cupertino, CEO Tim Cook unveiled the latest iPhone model with enhanced AI capabilities.",
            tickers=["AAPL"],
            published_date=datetime.now()
        )

        result = await agent.analyze(article)

        assert isinstance(result, EntityRecognitionAnalysis)
        assert result.confidence == 0.89

        # Check companies
        assert len(result.companies) == 1
        company = result.companies[0]
        assert company["name"] == "Apple Inc."
        assert company["ticker"] == "AAPL"
        assert company["confidence"] == 0.95

        # Check people
        assert len(result.people) == 1
        person = result.people[0]
        assert person["name"] == "Tim Cook"
        assert person["role"] == "CEO"
        assert person["organization"] == "Apple Inc."

        # Check relationships
        assert len(result.entity_relationships) == 1
        relationship = result.entity_relationships[0]
        assert relationship["entity1"] == "Tim Cook"
        assert relationship["relationship"] == "leads"
        assert relationship["entity2"] == "Apple Inc."


class TestEventDetectionAgent:
    """Tests for the event detection agent."""

    @pytest.fixture
    def mock_llm_client(self):
        """Mock LLM client with event response."""
        client = AsyncMock(spec=MultiProviderLLMClient)
        client.generate_response.return_value = LLMResponse(
            content=json.dumps({
                "events": [
                    {
                        "event_type": "earnings_announcement",
                        "event_name": "Q4 Earnings Release",
                        "description": "Company reported quarterly earnings exceeding expectations",
                        "affected_entities": ["Apple Inc."],
                        "event_timing": "past",
                        "expected_date": "2024-01-25",
                        "importance_score": 0.9,
                        "market_impact_expectation": "positive",
                        "confidence": 0.92
                    }
                ],
                "event_categories": ["earnings"],
                "event_timeline": [
                    {
                        "sequence": 1,
                        "event": "Q4 Earnings Release",
                        "timing": "January 25, 2024"
                    }
                ],
                "causal_relationships": [],
                "overall_confidence": 0.88
            }),
            model_used="gemini-1.5-flash",
            provider="google",
            tokens_used=400,
            processing_time_ms=2200,
            cost_estimate=0.006
        )
        return client

    def test_event_agent_system_prompt(self, mock_llm_client):
        """Test event detection agent system prompt."""
        agent = EventDetectionAgent(mock_llm_client)

        system_prompt = agent.get_system_prompt()

        assert "event detection" in system_prompt.lower()
        assert "market-moving events" in system_prompt.lower()
        assert "causal relationships" in system_prompt.lower()
        assert "importance scoring" in system_prompt.lower()

    @pytest.mark.asyncio
    async def test_event_detection_execution(self, mock_llm_client):
        """Test event detection execution."""
        agent = EventDetectionAgent(mock_llm_client)

        article = NewsArticle(
            id="event_test",
            title="Apple Reports Record Q4 Earnings",
            content="Apple announced record-breaking Q4 earnings with revenue of $89.5 billion, surpassing Wall Street expectations of $85.2 billion.",
            tickers=["AAPL"],
            published_date=datetime.now()
        )

        result = await agent.analyze(article)

        assert isinstance(result, EventDetectionAnalysis)
        assert result.confidence == 0.88

        # Check detected events
        assert len(result.events) == 1
        event = result.events[0]
        assert event["event_type"] == "earnings_announcement"
        assert event["event_name"] == "Q4 Earnings Release"
        assert event["importance_score"] == 0.9
        assert event["market_impact_expectation"] == "positive"
        assert "Apple Inc." in event["affected_entities"]

        # Check event categories
        assert "earnings" in result.event_categories

        # Check event importance scoring
        assert "earnings_announcement" in result.event_importance
        assert result.event_importance["earnings_announcement"] == 0.9


class TestSpecializedAgents:
    """Tests for specialized agents (Risk, Market Impact, Signal Generation)."""

    @pytest.fixture
    def mock_llm_client(self):
        """Mock LLM client for specialized agents."""
        client = AsyncMock(spec=MultiProviderLLMClient)
        return client

    @pytest.fixture
    def risk_response(self):
        """Mock risk assessment response."""
        return json.dumps({
            "overall_risk_score": 0.3,
            "risk_categories": {
                "market_risk": 0.4,
                "credit_risk": 0.1,
                "operational_risk": 0.2,
                "regulatory_risk": 0.1,
                "liquidity_risk": 0.2,
                "reputational_risk": 0.3
            },
            "uncertainty_factors": [
                {
                    "factor": "market_volatility",
                    "impact_score": 0.6,
                    "probability": 0.4,
                    "description": "Increased market volatility following earnings"
                }
            ],
            "black_swan_probability": 0.05,
            "risk_horizon": "short_term",
            "risk_mitigation_suggestions": [
                "Consider position sizing limits",
                "Monitor for unusual volume patterns"
            ],
            "reasoning": "Low to moderate risk given strong earnings performance",
            "confidence": 0.82
        })

    @pytest.fixture
    def market_impact_response(self):
        """Mock market impact response."""
        return json.dumps({
            "price_impact_prediction": {
                "1h": 2.5,
                "1d": 4.2,
                "5d": 3.8,
                "20d": 2.1
            },
            "price_impact_confidence": {
                "1h": 0.7,
                "1d": 0.8,
                "5d": 0.6,
                "20d": 0.4
            },
            "volatility_impact": 1.3,
            "volume_impact": 2.1,
            "impact_timing": "immediate",
            "sector_spillover": {
                "technology": 0.6,
                "consumer_electronics": 0.8
            },
            "market_timing": "market_hours",
            "reasoning": "Strong earnings beat likely to drive immediate positive price action",
            "confidence": 0.79
        })

    @pytest.fixture
    def signal_response(self):
        """Mock signal generation response."""
        return json.dumps({
            "signal_strength": 0.7,
            "signal_direction": "buy",
            "signal_confidence": 0.8,
            "urgency_level": 7,
            "time_horizon": "short_term",
            "position_sizing": 0.15,
            "entry_strategy": {
                "timing": "immediate",
                "entry_price_target": "current_market",
                "execution_notes": "Execute during market hours for best liquidity"
            },
            "risk_management": {
                "stop_loss": 0.08,
                "take_profit": 0.12,
                "risk_reward_ratio": 1.5,
                "max_loss_tolerance": 0.05
            },
            "supporting_factors": [
                "Strong earnings beat",
                "Revenue growth acceleration",
                "Positive guidance"
            ],
            "risk_factors": [
                "Market volatility",
                "Sector rotation risk"
            ],
            "reasoning": "Strong fundamental catalyst with favorable risk/reward profile",
            "confidence": 0.85
        })

    @pytest.mark.asyncio
    async def test_risk_assessment_agent(self, mock_llm_client, risk_response):
        """Test risk assessment agent execution."""
        mock_llm_client.generate_response.return_value = LLMResponse(
            content=risk_response,
            model_used="claude-3-haiku-20240307",
            provider="anthropic",
            tokens_used=350,
            processing_time_ms=1900,
            cost_estimate=0.004
        )

        agent = RiskAssessmentAgent(mock_llm_client)

        article = NewsArticle(
            id="risk_test",
            title="Tech Stocks Rally Following Strong Earnings",
            content="Technology sector sees broad gains after several companies report better-than-expected earnings.",
            tickers=["AAPL", "MSFT"],
            published_date=datetime.now()
        )

        result = await agent.analyze(article)

        assert isinstance(result, RiskAssessmentAnalysis)
        assert result.overall_risk_score == 0.3
        assert result.confidence == 0.82

        # Check risk categories
        assert result.risk_categories["market_risk"] == 0.4
        assert result.risk_categories["credit_risk"] == 0.1

        # Check uncertainty factors
        assert len(result.uncertainty_factors) == 1
        factor = result.uncertainty_factors[0]
        assert factor["factor"] == "market_volatility"
        assert factor["impact_score"] == 0.6

        assert result.black_swan_probability == 0.05
        assert len(result.risk_mitigation_suggestions) == 2

    @pytest.mark.asyncio
    async def test_market_impact_agent(self, mock_llm_client, market_impact_response):
        """Test market impact agent execution."""
        mock_llm_client.generate_response.return_value = LLMResponse(
            content=market_impact_response,
            model_used="gpt-4o-mini",
            provider="openai",
            tokens_used=450,
            processing_time_ms=2100,
            cost_estimate=0.007
        )

        agent = MarketImpactAgent(mock_llm_client)

        article = NewsArticle(
            id="impact_test",
            title="Major Tech Acquisition Announcement",
            content="Large technology company announces strategic acquisition of AI startup for $2.5 billion.",
            tickers=["GOOGL"],
            published_date=datetime.now()
        )

        result = await agent.analyze(article)

        assert isinstance(result, MarketImpactAnalysis)
        assert result.confidence == 0.79

        # Check price impact predictions
        assert result.price_impact_prediction["1h"] == 2.5
        assert result.price_impact_prediction["1d"] == 4.2
        assert result.price_impact_prediction["5d"] == 3.8

        assert result.volatility_impact == 1.3
        assert result.volume_impact == 2.1
        assert result.market_timing == "market_hours"

        # Check sector spillover
        assert result.sector_spillover["technology"] == 0.6
        assert result.sector_spillover["consumer_electronics"] == 0.8

    @pytest.mark.asyncio
    async def test_signal_generation_agent(self, mock_llm_client, signal_response):
        """Test signal generation agent execution."""
        mock_llm_client.generate_response.return_value = LLMResponse(
            content=signal_response,
            model_used="claude-3-haiku-20240307",
            provider="anthropic",
            tokens_used=500,
            processing_time_ms=2400,
            cost_estimate=0.008
        )

        agent = SignalGenerationAgent(mock_llm_client)

        article = NewsArticle(
            id="signal_test",
            title="Biotech Company Receives FDA Approval",
            content="Pharmaceutical company receives FDA approval for breakthrough cancer treatment, opening path to commercialization.",
            tickers=["BIIB"],
            published_date=datetime.now()
        )

        # Mock context from other agents
        context = {
            'sentiment_analysis': MagicMock(sentiment_score=0.8, confidence=0.9),
            'risk_analysis': MagicMock(overall_risk_score=0.2, confidence=0.85),
            'market_impact': MagicMock(price_impact_prediction={'1d': 5.2}, confidence=0.8)
        }

        result = await agent.analyze(article, context)

        assert isinstance(result, SignalGenerationAnalysis)
        assert result.signal_strength == 0.7
        assert result.signal_direction == "buy"
        assert result.urgency_level == 7
        assert result.position_sizing == 0.15
        assert result.confidence == 0.85

        # Check supporting and risk factors
        assert "Strong earnings beat" in result.supporting_factors
        assert "Market volatility" in result.risk_factors

        # Check risk management
        assert result.stop_loss == 0.08
        assert result.take_profit == 0.12


class TestMultiAgentOrchestration:
    """Tests for multi-agent orchestration and coordination."""

    @pytest.fixture
    def mock_llm_client(self):
        """Mock LLM client for orchestration testing."""
        return AsyncMock(spec=MultiProviderLLMClient)

    @pytest.fixture
    def orchestrator(self, mock_llm_client):
        """Multi-agent orchestrator for testing."""
        return MultiAgentAnalysisOrchestrator(mock_llm_client)

    def test_orchestrator_initialization(self, orchestrator):
        """Test orchestrator initialization with agents."""
        assert len(orchestrator.agents) == 3  # Sentiment, Entity, Event
        assert AgentType.SENTIMENT in orchestrator.agents
        assert AgentType.ENTITY_RECOGNITION in orchestrator.agents
        assert AgentType.EVENT_DETECTION in orchestrator.agents

        assert orchestrator.parallel_execution is True
        assert orchestrator.timeout_seconds == 30

    @pytest.mark.asyncio
    async def test_parallel_agent_execution(self, orchestrator):
        """Test parallel execution of multiple agents."""

        # Mock agent responses
        sentiment_result = SentimentAnalysis(
            agent_type=AgentType.SENTIMENT,
            confidence=0.8,
            processing_time_ms=1200,
            model_used="gpt-4o-mini",
            sentiment_score=0.7,
            sentiment_label="bullish",
            sentiment_strength=0.8,
            emotional_indicators={},
            market_sentiment_context="Positive market sentiment"
        )

        entity_result = EntityRecognitionAnalysis(
            agent_type=AgentType.ENTITY_RECOGNITION,
            confidence=0.9,
            processing_time_ms=1500,
            model_used="claude-3-haiku-20240307",
            companies=[], people=[], financial_products=[],
            geographic_locations=[], regulatory_bodies=[],
            entity_relationships=[]
        )

        event_result = EventDetectionAnalysis(
            agent_type=AgentType.EVENT_DETECTION,
            confidence=0.85,
            processing_time_ms=1800,
            model_used="gemini-1.5-flash",
            events=[], event_categories=[], event_timeline=[],
            event_importance={}, causal_relationships=[]
        )

        # Mock agent analyze methods
        orchestrator.agents[AgentType.SENTIMENT].analyze = AsyncMock(return_value=sentiment_result)
        orchestrator.agents[AgentType.ENTITY_RECOGNITION].analyze = AsyncMock(return_value=entity_result)
        orchestrator.agents[AgentType.EVENT_DETECTION].analyze = AsyncMock(return_value=event_result)

        article = NewsArticle(
            id="orchestration_test",
            title="Test Article for Orchestration",
            content="Test content for multi-agent analysis",
            tickers=["TEST"],
            published_date=datetime.now()
        )

        results = await orchestrator.analyze_article(article)

        # All agents should have been called
        assert len(results) == 3
        assert AgentType.SENTIMENT in results
        assert AgentType.ENTITY_RECOGNITION in results
        assert AgentType.EVENT_DETECTION in results

        # Check results
        assert results[AgentType.SENTIMENT] == sentiment_result
        assert results[AgentType.ENTITY_RECOGNITION] == entity_result
        assert results[AgentType.EVENT_DETECTION] == event_result

        # Verify all agents were called
        orchestrator.agents[AgentType.SENTIMENT].analyze.assert_called_once()
        orchestrator.agents[AgentType.ENTITY_RECOGNITION].analyze.assert_called_once()
        orchestrator.agents[AgentType.EVENT_DETECTION].analyze.assert_called_once()

    @pytest.mark.asyncio
    async def test_agent_failure_handling(self, orchestrator):
        """Test handling of individual agent failures."""

        # Mock one agent to succeed, one to fail
        sentiment_result = SentimentAnalysis(
            agent_type=AgentType.SENTIMENT,
            confidence=0.8,
            processing_time_ms=1200,
            model_used="gpt-4o-mini",
            sentiment_score=0.7,
            sentiment_label="bullish",
            sentiment_strength=0.8,
            emotional_indicators={},
            market_sentiment_context="Positive"
        )

        orchestrator.agents[AgentType.SENTIMENT].analyze = AsyncMock(return_value=sentiment_result)
        orchestrator.agents[AgentType.ENTITY_RECOGNITION].analyze = AsyncMock(side_effect=Exception("Agent failed"))
        orchestrator.agents[AgentType.EVENT_DETECTION].analyze = AsyncMock(return_value=MagicMock())

        article = NewsArticle(
            id="failure_test",
            title="Test Failure Handling",
            content="Test content",
            tickers=["FAIL"],
            published_date=datetime.now()
        )

        results = await orchestrator.analyze_article(article)

        # Should still get results for all agents (failed ones get error analysis)
        assert len(results) == 3
        assert results[AgentType.SENTIMENT] == sentiment_result

        # Failed agent should have error result
        failed_result = results[AgentType.ENTITY_RECOGNITION]
        assert failed_result.confidence == 0.0
        assert failed_result.model_used == "error"
        assert "Agent timeout or error" in failed_result.reasoning

    @pytest.mark.asyncio
    async def test_orchestration_timeout(self, orchestrator):
        """Test orchestration timeout handling."""

        # Set very short timeout
        orchestrator.timeout_seconds = 0.1

        # Mock agents to take longer than timeout
        async def slow_analysis(*args, **kwargs):
            await asyncio.sleep(0.2)  # Longer than timeout
            return MagicMock()

        for agent in orchestrator.agents.values():
            agent.analyze = AsyncMock(side_effect=slow_analysis)

        article = NewsArticle(
            id="timeout_test",
            title="Test Timeout",
            content="Test content",
            tickers=["TIMEOUT"],
            published_date=datetime.now()
        )

        results = await orchestrator.analyze_article(article)

        # Should get error results for all agents due to timeout
        for result in results.values():
            assert result.confidence == 0.0
            assert result.model_used == "error"

    def test_ensemble_confidence_calculation(self, orchestrator):
        """Test ensemble confidence calculation across agents."""

        # Mock analysis results with different confidences
        analyses = {
            AgentType.SENTIMENT: MagicMock(confidence=0.9),
            AgentType.ENTITY_RECOGNITION: MagicMock(confidence=0.7),
            AgentType.EVENT_DETECTION: MagicMock(confidence=0.8)
        }

        # Mock agent performance metrics
        for agent_type, agent in orchestrator.agents.items():
            agent.get_performance_metrics.return_value = {'error_rate': 0.1}

        ensemble_confidence = orchestrator.get_ensemble_confidence(analyses)

        # Should be weighted average adjusted for error rates
        assert 0.7 <= ensemble_confidence <= 0.9
        assert isinstance(ensemble_confidence, float)

    def test_orchestration_metrics(self, orchestrator):
        """Test orchestration performance metrics."""

        # Simulate some orchestration
        orchestrator.orchestration_count = 5
        orchestrator.total_orchestration_time_ms = 10000

        # Mock agent metrics
        for agent in orchestrator.agents.values():
            agent.get_performance_metrics.return_value = {
                'agent_type': 'test',
                'analysis_count': 5,
                'error_count': 1,
                'avg_processing_time_ms': 2000
            }

        metrics = orchestrator.get_orchestration_metrics()

        # Verify metrics structure
        assert 'orchestration_count' in metrics
        assert 'avg_orchestration_time_ms' in metrics
        assert 'agent_metrics' in metrics
        assert len(metrics['agent_metrics']) == len(orchestrator.agents)


class TestEnhancedMultiAgentIntegration:
    """Integration tests for the enhanced multi-agent orchestrator."""

    @pytest.fixture
    def mock_llm_client(self):
        """Mock LLM client with realistic responses."""
        client = AsyncMock(spec=MultiProviderLLMClient)

        def create_mock_response(content):
            response = LLMResponse(
                content=content,
                model="gpt-4o-mini",
                provider="openai",
                tokens_used=100,
                cost_usd=0.001,
                latency_ms=500
            )
            return response

        # Configure different responses for different agent types
        client.generate_response = AsyncMock()

        def side_effect(*args, **kwargs):
            prompt = args[0] if args else kwargs.get('prompt', '')

            if 'sentiment' in prompt.lower():
                return create_mock_response(json.dumps({
                    "sentiment": "positive",
                    "sentiment_score": 0.8,
                    "confidence": 0.85,
                    "key_phrases": ["strong earnings", "revenue beat", "exceeded expectations"],
                    "explanation": "Very positive sentiment due to earnings beat"
                }))
            elif 'entity' in prompt.lower():
                return create_mock_response(json.dumps({
                    "entities": [
                        {"name": "Apple Inc.", "type": "company", "ticker": "AAPL", "relevance": 1.0},
                        {"name": "iPhone", "type": "product", "ticker": "AAPL", "relevance": 0.9},
                        {"name": "Tim Cook", "type": "person", "ticker": "AAPL", "relevance": 0.7}
                    ],
                    "confidence": 0.9,
                    "explanation": "Clear entity identification from earnings report"
                }))
            elif 'event' in prompt.lower():
                return create_mock_response(json.dumps({
                    "events": [
                        {
                            "type": "earnings_announcement",
                            "description": "Q4 earnings beat expectations",
                            "importance": "high",
                            "market_impact": "positive",
                            "affected_tickers": ["AAPL"]
                        }
                    ],
                    "confidence": 0.88,
                    "explanation": "Major earnings event detected"
                }))
            elif 'risk' in prompt.lower():
                return create_mock_response(json.dumps({
                    "risk_level": "medium",
                    "risk_score": 0.4,
                    "risk_factors": [
                        {"factor": "supply_chain_challenges", "severity": "medium", "probability": 0.6}
                    ],
                    "confidence": 0.75,
                    "explanation": "Supply chain risks noted but offset by strong performance"
                }))
            elif 'impact' in prompt.lower():
                return create_mock_response(json.dumps({
                    "market_impact": "positive",
                    "impact_score": 0.7,
                    "expected_price_movement": "up_5_to_10_percent",
                    "timeframe": "1_to_3_days",
                    "confidence": 0.82,
                    "explanation": "Strong earnings should drive positive price movement"
                }))
            elif 'signal' in prompt.lower():
                return create_mock_response(json.dumps({
                    "signal": "buy",
                    "signal_strength": 0.8,
                    "signal_confidence": 0.85,
                    "signal_horizon": "short_term",
                    "key_catalysts": ["earnings_beat", "strong_guidance"],
                    "explanation": "Strong buy signal based on comprehensive analysis"
                }))
            else:
                return create_mock_response('{"confidence": 0.5, "explanation": "Generic response"}')

        client.generate_response.side_effect = side_effect
        return client

    @pytest.fixture
    def enhanced_orchestrator(self, mock_llm_client):
        """Enhanced orchestrator with all agents."""
        return EnhancedMultiAgentOrchestrator(mock_llm_client)

    @pytest.fixture
    def comprehensive_news_article(self):
        """Complex news article for comprehensive testing."""
        return NewsArticle(
            id="comprehensive_test_456",
            title="Apple Stock Soars After Earnings Beat, But Supply Chain Concerns Linger",
            content="""
            Apple Inc. (NASDAQ: AAPL) reported blockbuster fourth-quarter results that crushed Wall Street expectations,
            sending shares up 8% in after-hours trading. The tech giant posted revenue of $89.5 billion versus
            analyst estimates of $84.3 billion, while earnings per share came in at $1.64 compared to the expected $1.52.

            CEO Tim Cook highlighted the company's strong performance across all product categories, with iPhone revenue
            growing 15% year-over-year to $43.2 billion. The Services segment, which includes the App Store, Apple Pay,
            and iCloud, continued its impressive growth trajectory with revenue of $22.3 billion, up 16% from last year.

            However, Cook also addressed ongoing supply chain challenges, particularly in Asia, which could impact
            production in the first quarter of 2025. "While we're pleased with our Q4 performance, we're closely
            monitoring supply chain dynamics that may affect our ability to meet demand," Cook stated during the
            earnings call.

            Wall Street analysts are upgrading their price targets, with Morgan Stanley raising its target to $250
            from $220, citing strong fundamentals and market share gains. However, some analysts expressed caution
            about the supply chain headwinds and their potential impact on margin expansion.

            The earnings beat comes at a critical time for Apple as it faces increased competition in the smartphone
            market and regulatory scrutiny in multiple jurisdictions. Despite these challenges, the company's strong
            financial performance and robust cash generation continue to attract investor interest.
            """,
            summary="Apple reports strong Q4 earnings beat but faces supply chain challenges",
            url="https://example.com/comprehensive-apple-news",
            source="Financial Times",
            published_date=datetime.now(),
            tickers=["AAPL"],
            language="en"
        )

    @pytest.mark.asyncio
    async def test_comprehensive_analysis_workflow(self, enhanced_orchestrator, comprehensive_news_article):
        """Test complete analysis workflow with all agents."""

        # Perform comprehensive analysis
        results = await enhanced_orchestrator.perform_comprehensive_analysis(comprehensive_news_article)

        # Verify all agent types analyzed
        expected_agents = {
            AgentType.SENTIMENT,
            AgentType.ENTITY_RECOGNITION,
            AgentType.EVENT_DETECTION,
            AgentType.RISK_ASSESSMENT,
            AgentType.MARKET_IMPACT,
            AgentType.SIGNAL_GENERATION
        }
        assert set(results.keys()) == expected_agents

        # Verify sentiment analysis
        sentiment_result = results[AgentType.SENTIMENT]
        assert isinstance(sentiment_result, SentimentAnalysis)
        assert sentiment_result.sentiment == "positive"
        assert 0.8 <= sentiment_result.sentiment_score <= 1.0
        assert sentiment_result.confidence >= 0.8

        # Verify entity recognition
        entity_result = results[AgentType.ENTITY_RECOGNITION]
        assert isinstance(entity_result, EntityRecognitionAnalysis)
        assert len(entity_result.entities) >= 2
        apple_entity = next((e for e in entity_result.entities if e['name'] == 'Apple Inc.'), None)
        assert apple_entity is not None
        assert apple_entity['ticker'] == 'AAPL'

        # Verify event detection
        event_result = results[AgentType.EVENT_DETECTION]
        assert isinstance(event_result, EventDetectionAnalysis)
        assert len(event_result.events) >= 1
        earnings_event = next((e for e in event_result.events if 'earnings' in e['type']), None)
        assert earnings_event is not None

        # Verify risk assessment
        risk_result = results[AgentType.RISK_ASSESSMENT]
        assert isinstance(risk_result, RiskAssessmentAnalysis)
        assert risk_result.risk_level in ['low', 'medium', 'high']
        assert 0.0 <= risk_result.risk_score <= 1.0

        # Verify market impact
        impact_result = results[AgentType.MARKET_IMPACT]
        assert isinstance(impact_result, MarketImpactAnalysis)
        assert impact_result.market_impact in ['negative', 'neutral', 'positive']
        assert impact_result.timeframe in ['immediate', '1_to_3_days', '1_to_2_weeks', 'long_term']

        # Verify signal generation
        signal_result = results[AgentType.SIGNAL_GENERATION]
        assert isinstance(signal_result, SignalGenerationAnalysis)
        assert signal_result.signal in ['strong_sell', 'sell', 'hold', 'buy', 'strong_buy']
        assert 0.0 <= signal_result.signal_strength <= 1.0

    @pytest.mark.asyncio
    async def test_ensemble_confidence_integration(self, enhanced_orchestrator, comprehensive_news_article):
        """Test ensemble confidence calculation with real analysis results."""

        # Perform analysis
        results = await enhanced_orchestrator.perform_comprehensive_analysis(comprehensive_news_article)

        # Calculate ensemble confidence
        ensemble_confidence = enhanced_orchestrator.get_ensemble_confidence(results)

        # Should be reasonable confidence based on our mock responses
        assert 0.7 <= ensemble_confidence <= 0.9

        # Verify confidence is influenced by all agents
        individual_confidences = [result.confidence for result in results.values()]
        min_confidence = min(individual_confidences)
        max_confidence = max(individual_confidences)

        # Ensemble should be within range but not just average
        assert min_confidence <= ensemble_confidence <= max_confidence

    @pytest.mark.asyncio
    async def test_error_handling_in_workflow(self, enhanced_orchestrator, comprehensive_news_article):
        """Test error handling during comprehensive analysis."""

        # Mock one agent to fail
        enhanced_orchestrator.agents[AgentType.SENTIMENT].analyze_article = AsyncMock(
            side_effect=Exception("Sentiment analysis failed")
        )

        # Analysis should still complete with other agents
        results = await enhanced_orchestrator.perform_comprehensive_analysis(comprehensive_news_article)

        # Should have results from other agents
        assert len(results) >= 4  # At least 4 agents should succeed

        # Failed agent should have error result
        sentiment_result = results.get(AgentType.SENTIMENT)
        if sentiment_result:
            assert sentiment_result.confidence == 0.0
            assert sentiment_result.model_used == "error"

    @pytest.mark.asyncio
    async def test_performance_tracking_integration(self, enhanced_orchestrator, comprehensive_news_article):
        """Test performance tracking across comprehensive analysis."""

        # Perform multiple analyses
        for i in range(3):
            await enhanced_orchestrator.perform_comprehensive_analysis(comprehensive_news_article)

        # Check orchestration metrics
        metrics = enhanced_orchestrator.get_orchestration_metrics()

        assert metrics['orchestration_count'] >= 3
        assert metrics['avg_orchestration_time_ms'] > 0
        assert len(metrics['agent_metrics']) == 6  # All six agent types

        # Check individual agent metrics
        for agent_metrics in metrics['agent_metrics']:
            assert agent_metrics['analysis_count'] >= 3
            assert agent_metrics['avg_processing_time_ms'] > 0

    @pytest.mark.asyncio
    async def test_complex_article_analysis_accuracy(self, enhanced_orchestrator):
        """Test analysis accuracy on complex, multi-faceted articles."""

        # Create an article with mixed sentiment and multiple events
        complex_article = NewsArticle(
            id="complex_test_789",
            title="Tesla Stock Drops Despite Record Deliveries as Regulatory Concerns Mount",
            content="""
            Tesla Inc. reported record quarterly vehicle deliveries of 484,507 units, beating analyst
            expectations by 12%. However, shares fell 5% in pre-market trading as investors focused
            on mounting regulatory challenges and CEO Elon Musk's ongoing legal battles.

            The delivery numbers represent a 35% increase from the same quarter last year, driven by
            strong Model Y demand and improved production efficiency at the Fremont and Shanghai facilities.
            Despite the positive operational metrics, several factors are weighing on investor sentiment.

            The National Highway Traffic Safety Administration (NHTSA) announced a formal investigation
            into Tesla's Autopilot system following several accidents. Additionally, the SEC is reportedly
            examining the company's self-driving car claims and marketing practices.
            """,
            tickers=["TSLA"],
            published_date=datetime.now()
        )

        # Configure mixed responses for complex scenario
        def complex_side_effect(*args, **kwargs):
            prompt = args[0] if args else kwargs.get('prompt', '')

            if 'sentiment' in prompt.lower():
                return LLMResponse(
                    content=json.dumps({
                        "sentiment": "mixed",
                        "sentiment_score": 0.3,  # Slightly negative due to stock drop
                        "confidence": 0.75,
                        "key_phrases": ["record deliveries", "stock drops", "regulatory concerns"],
                        "explanation": "Mixed sentiment with positive operational news offset by regulatory concerns"
                    }),
                    model="gpt-4o-mini", provider="openai", tokens_used=120, cost_usd=0.0012, latency_ms=450
                )
            elif 'risk' in prompt.lower():
                return LLMResponse(
                    content=json.dumps({
                        "risk_level": "high",
                        "risk_score": 0.7,
                        "risk_factors": [
                            {"factor": "regulatory_investigation", "severity": "high", "probability": 0.8},
                            {"factor": "legal_challenges", "severity": "medium", "probability": 0.6}
                        ],
                        "confidence": 0.85,
                        "explanation": "High regulatory and legal risks despite operational strength"
                    }),
                    model="gpt-4o-mini", provider="openai", tokens_used=100, cost_usd=0.001, latency_ms=500
                )
            else:
                # Use default responses for other agents
                return LLMResponse(
                    content='{"confidence": 0.6, "explanation": "Standard analysis"}',
                    model="gpt-4o-mini", provider="openai", tokens_used=80, cost_usd=0.0008, latency_ms=400
                )

        enhanced_orchestrator.llm_client.generate_response.side_effect = complex_side_effect

        # Perform analysis
        results = await enhanced_orchestrator.perform_comprehensive_analysis(complex_article)

        # Verify complex analysis captures nuances
        sentiment_result = results[AgentType.SENTIMENT]
        assert sentiment_result.sentiment == "mixed"
        assert sentiment_result.sentiment_score < 0.5  # Should reflect negative sentiment

        risk_result = results[AgentType.RISK_ASSESSMENT]
        assert risk_result.risk_level == "high"
        assert risk_result.risk_score >= 0.6

        # Ensemble confidence should be moderate due to mixed signals
        ensemble_confidence = enhanced_orchestrator.get_ensemble_confidence(results)
        assert 0.4 <= ensemble_confidence <= 0.8

        assert metrics['orchestration_count'] == 5
        assert metrics['avg_orchestration_time_ms'] == 2000
        assert metrics['parallel_execution'] is True
        assert 'agent_metrics' in metrics
        assert len(metrics['agent_metrics']) == 3


class TestEnhancedMultiAgentOrchestrator:
    """Tests for enhanced orchestrator with all specialized agents."""

    @pytest.fixture
    def mock_llm_client(self):
        """Mock LLM client for enhanced orchestration."""
        return AsyncMock(spec=MultiProviderLLMClient)

    @pytest.fixture
    def enhanced_orchestrator(self, mock_llm_client):
        """Enhanced multi-agent orchestrator."""
        return EnhancedMultiAgentOrchestrator(mock_llm_client)

    def test_enhanced_orchestrator_initialization(self, enhanced_orchestrator):
        """Test enhanced orchestrator includes all agent types."""
        assert len(enhanced_orchestrator.agents) == 6
        assert AgentType.SENTIMENT in enhanced_orchestrator.agents
        assert AgentType.ENTITY_RECOGNITION in enhanced_orchestrator.agents
        assert AgentType.EVENT_DETECTION in enhanced_orchestrator.agents
        assert AgentType.RISK_ASSESSMENT in enhanced_orchestrator.agents
        assert AgentType.MARKET_IMPACT in enhanced_orchestrator.agents
        assert AgentType.SIGNAL_GENERATION in enhanced_orchestrator.agents

    @pytest.mark.asyncio
    async def test_comprehensive_analysis_workflow(self, enhanced_orchestrator):
        """Test the complete comprehensive analysis workflow."""

        # Mock all agent responses
        sentiment_result = MagicMock(confidence=0.8, sentiment_score=0.7)
        entity_result = MagicMock(confidence=0.9)
        event_result = MagicMock(confidence=0.85)
        risk_result = MagicMock(confidence=0.82, overall_risk_score=0.3)
        market_result = MagicMock(confidence=0.79, price_impact_prediction={'1d': 3.5})
        signal_result = MagicMock(confidence=0.85, signal_strength=0.7, signal_direction='buy')

        # Mock agent analyze methods
        enhanced_orchestrator.agents[AgentType.SENTIMENT].analyze = AsyncMock(return_value=sentiment_result)
        enhanced_orchestrator.agents[AgentType.ENTITY_RECOGNITION].analyze = AsyncMock(return_value=entity_result)
        enhanced_orchestrator.agents[AgentType.EVENT_DETECTION].analyze = AsyncMock(return_value=event_result)
        enhanced_orchestrator.agents[AgentType.RISK_ASSESSMENT].analyze = AsyncMock(return_value=risk_result)
        enhanced_orchestrator.agents[AgentType.MARKET_IMPACT].analyze = AsyncMock(return_value=market_result)
        enhanced_orchestrator.agents[AgentType.SIGNAL_GENERATION].analyze = AsyncMock(return_value=signal_result)

        article = NewsArticle(
            id="comprehensive_test",
            title="Comprehensive Analysis Test",
            content="Test content for comprehensive analysis",
            tickers=["COMP"],
            published_date=datetime.now()
        )

        result = await enhanced_orchestrator.run_comprehensive_analysis(article)

        # Check comprehensive result structure
        assert result['article_id'] == 'comprehensive_test'
        assert 'analysis_timestamp' in result
        assert 'analysis_time_ms' in result
        assert 'ensemble_confidence' in result
        assert 'agent_results' in result
        assert 'signal_generated' in result
        assert 'actionable_signal' in result

        # Check that all agents were called
        agent_results = result['agent_results']
        assert 'sentiment' in agent_results
        assert 'entity_recognition' in agent_results
        assert 'event_detection' in agent_results
        assert 'risk_assessment' in agent_results
        assert 'market_impact' in agent_results
        assert 'signal_generation' in agent_results

        # Signal generation agent should have been called with context
        signal_call = enhanced_orchestrator.agents[AgentType.SIGNAL_GENERATION].analyze.call_args
        assert signal_call is not None
        context = signal_call[0][1]  # Second argument should be context
        assert 'sentiment' in context
        assert 'risk_assessment' in context

    def test_actionable_signal_determination(self, enhanced_orchestrator):
        """Test determination of actionable signals."""

        # Test actionable signal
        actionable_signal = MagicMock(
            confidence=0.9,
            signal_direction='buy',
            urgency_level=8,
            signal_strength=0.8
        )
        assert enhanced_orchestrator._is_actionable_signal(actionable_signal) is True

        # Test non-actionable signal (low confidence)
        low_confidence_signal = MagicMock(
            confidence=0.4,
            signal_direction='buy',
            urgency_level=8,
            signal_strength=0.8
        )
        assert enhanced_orchestrator._is_actionable_signal(low_confidence_signal) is False

        # Test non-actionable signal (hold direction)
        hold_signal = MagicMock(
            confidence=0.9,
            signal_direction='hold',
            urgency_level=8,
            signal_strength=0.8
        )
        assert enhanced_orchestrator._is_actionable_signal(hold_signal) is False

    def test_comprehensive_metrics(self, enhanced_orchestrator):
        """Test comprehensive metrics collection."""

        # Simulate some analysis
        enhanced_orchestrator.full_analysis_count = 3
        enhanced_orchestrator.total_analysis_time_ms = 15000

        # Mock agent metrics
        for agent in enhanced_orchestrator.agents.values():
            agent.get_performance_metrics.return_value = {
                'analysis_count': 3,
                'error_count': 0,
                'avg_processing_time_ms': 1500
            }

        metrics = enhanced_orchestrator.get_comprehensive_metrics()

        assert metrics['comprehensive_analysis_count'] == 3
        assert metrics['avg_comprehensive_analysis_time_ms'] == 5000
        assert metrics['total_agents'] == 6
        assert 'agent_metrics' in metrics
        assert len(metrics['agent_metrics']) == 6


# Performance and load testing for multi-agent system
@pytest.mark.performance
class TestMultiAgentPerformance:
    """Performance tests for multi-agent framework."""

    @pytest.mark.asyncio
    async def test_concurrent_agent_analysis(self):
        """Test concurrent analysis performance."""
        mock_llm_client = AsyncMock(spec=MultiProviderLLMClient)
        orchestrator = MultiAgentAnalysisOrchestrator(mock_llm_client)

        # Mock fast agent responses
        async def fast_analysis(*args, **kwargs):
            await asyncio.sleep(0.1)  # 100ms processing time
            return MagicMock(confidence=0.8, processing_time_ms=100)

        for agent in orchestrator.agents.values():
            agent.analyze = AsyncMock(side_effect=fast_analysis)

        # Create multiple articles for concurrent processing
        articles = [
            NewsArticle(
                id=f"perf_test_{i}",
                title=f"Performance Test Article {i}",
                content=f"Test content for performance testing {i}",
                tickers=[f"TEST{i}"],
                published_date=datetime.now()
            )
            for i in range(10)
        ]

        # Process articles concurrently
        start_time = time.time()
        tasks = [orchestrator.analyze_article(article) for article in articles]
        results = await asyncio.gather(*tasks)
        end_time = time.time()

        # Check results
        assert len(results) == 10
        total_time = end_time - start_time

        # Should complete in reasonable time (less than 2 seconds for 10 concurrent analyses)
        assert total_time < 2.0

        # Each result should have all agent types
        for result in results:
            assert len(result) == 3  # All three agent types

    @pytest.mark.asyncio
    async def test_agent_memory_usage(self):
        """Test agent memory usage remains stable."""
        import psutil
        import os

        mock_llm_client = AsyncMock(spec=MultiProviderLLMClient)
        orchestrator = EnhancedMultiAgentOrchestrator(mock_llm_client)

        process = psutil.Process(os.getpid())
        initial_memory = process.memory_info().rss

        # Mock agent responses
        for agent in orchestrator.agents.values():
            agent.analyze = AsyncMock(return_value=MagicMock(confidence=0.8))

        # Run many analyses
        for i in range(50):
            article = NewsArticle(
                id=f"memory_test_{i}",
                title=f"Memory Test {i}",
                content=f"Content for memory test {i}" * 100,  # Larger content
                tickers=["MEM"],
                published_date=datetime.now()
            )

            await orchestrator.run_comprehensive_analysis(article)

            # Check memory every 10 iterations
            if i % 10 == 0:
                current_memory = process.memory_info().rss
                memory_increase = current_memory - initial_memory

                # Memory increase should be reasonable (less than 100MB)
                assert memory_increase < 100 * 1024 * 1024

        final_memory = process.memory_info().rss
        total_increase = final_memory - initial_memory

        # Total memory increase should be reasonable
        assert total_increase < 200 * 1024 * 1024  # Less than 200MB


# Integration tests
@pytest.mark.integration
class TestMultiAgentIntegration:
    """Integration tests for multi-agent framework."""

    @pytest.mark.asyncio
    @pytest.mark.skipif(not pytest.config.getoption("--integration"),
                       reason="Integration tests require --integration flag")
    async def test_real_llm_integration(self):
        """Integration test with real LLM client (if available)."""

        # This would require real API keys and would be slow
        # Implementation would depend on having test API keys available
        pytest.skip("Real LLM integration test requires API keys")

    @pytest.mark.asyncio
    async def test_database_integration(self):
        """Test integration with database storage."""

        # This would test storing agent results in database
        # Would require test database setup
        pytest.skip("Database integration test requires test database")


# Fixtures for complex test scenarios
@pytest.fixture
def complex_news_article():
    """Complex news article for comprehensive testing."""
    return NewsArticle(
        id="complex_test_article",
        title="Apple Announces $50B Share Buyback Program Following Record Q2 Earnings Beat",
        content="""
        Apple Inc. (AAPL) reported record second-quarter earnings that significantly exceeded Wall Street expectations,
        prompting the technology giant to announce a massive $50 billion share buyback program.

        The company posted earnings per share of $2.34, well above the consensus estimate of $2.10. Revenue climbed
        to $94.8 billion from $89.5 billion in the same period last year, representing a 6% year-over-year increase.

        CEO Tim Cook attributed the strong performance to robust iPhone sales, particularly in China, and continued
        growth in the services segment. "We're seeing unprecedented demand for our latest iPhone models and our
        services business continues to be a growth engine," Cook stated during the earnings call.

        The company also announced plans to increase its quarterly dividend by 5% to $0.25 per share. Chief Financial
        Officer Luca Maestri noted that the company's strong cash position of $165 billion enables both the buyback
        program and dividend increase while maintaining investment in innovation.

        Apple's stock surged 8% in after-hours trading following the announcement. Analysts at Goldman Sachs upgraded
        their price target to $180 from $165, citing strong fundamentals and shareholder-friendly capital allocation.

        The buyback program represents one of the largest in corporate history and underscores Apple's commitment
        to returning capital to shareholders. The program is expected to be completed over the next 18 months.
        """,
        summary="Apple beats Q2 earnings expectations and announces $50B share buyback program",
        url="https://example.com/apple-earnings-buyback",
        source="Financial Times",
        published_date=datetime.now() - timedelta(minutes=30),
        tickers=["AAPL"],
        raw_data={
            "category": "earnings",
            "importance": "high",
            "market_cap": "2.8T",
            "sector": "Technology"
        }
    )


@pytest.fixture
def market_context():
    """Market context for testing."""
    return {
        "market_session": "after_hours",
        "market_volatility": "moderate",
        "sector_performance": "positive",
        "overall_market_trend": "bullish"
    }