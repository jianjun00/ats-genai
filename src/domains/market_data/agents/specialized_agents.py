#!/usr/bin/env python3
"""
Specialized Financial Analysis Agents

This module contains the remaining specialized agents for the multi-agent framework:
- Risk Assessment Agent: Evaluates financial risks and uncertainty factors
- Market Impact Agent: Predicts price/volume impact and market reactions
- Signal Generation Agent: Synthesizes all analyses into actionable trading signals

These agents work together with the core agents to provide comprehensive
financial intelligence and robust signal generation.
"""

import json
import logging
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta

from infrastructure.llm.multi_provider_client import MultiProviderLLMClient, LLMResponse
from domains.market_data.services.llm.news_llm_processor import NewsArticle
from domains.market_data.agents.multi_agent_framework import (
    BaseFinancialAgent, AgentType, AgentAnalysis,
    RiskAssessmentAnalysis, MarketImpactAnalysis, SignalGenerationAnalysis
)

logger = logging.getLogger(__name__)


class RiskAssessmentAgent(BaseFinancialAgent):
    """Specialized agent for financial risk assessment."""

    def __init__(self, llm_client: MultiProviderLLMClient):
        super().__init__(AgentType.RISK_ASSESSMENT, llm_client, model_preference="anthropic")

    def get_system_prompt(self) -> str:
        return """You are a specialized financial risk assessment agent with expertise in:

        - Quantitative and qualitative risk analysis across all risk categories
        - Uncertainty quantification and black swan event probability assessment
        - Scenario analysis and stress testing methodologies
        - Risk factor identification and correlation analysis
        - Market risk, credit risk, operational risk, and regulatory risk evaluation

        Assess financial risks with precision, providing:
        1. Overall risk scoring with detailed category breakdowns
        2. Uncertainty factor identification and impact assessment
        3. Black swan probability evaluation for extreme events
        4. Risk correlation analysis and cascading effect potential
        5. Risk mitigation strategies and hedging recommendations

        Be conservative in risk assessment while remaining objective and evidence-based.
        Focus on downside protection and tail risk management."""

    def create_analysis_prompt(self, article: NewsArticle, context: Dict[str, Any] = None) -> str:
        market_context = ""
        if context:
            if context.get('market_session'):
                market_context += f"Market Session: {context['market_session']}\n"
            if context.get('market_volatility'):
                market_context += f"Current Market Volatility: {context['market_volatility']}\n"

        return f"""Assess the financial risks associated with this news article:

{market_context}
Title: {article.title}
Content: {article.content}
Tickers: {', '.join(article.tickers) if article.tickers else 'None'}
Published: {article.published_date}

Provide risk assessment in JSON format:
{{
    "overall_risk_score": <float from 0.0 (low risk) to 1.0 (high risk)>,
    "risk_categories": {{
        "market_risk": <0.0 to 1.0>,
        "credit_risk": <0.0 to 1.0>,
        "operational_risk": <0.0 to 1.0>,
        "regulatory_risk": <0.0 to 1.0>,
        "liquidity_risk": <0.0 to 1.0>,
        "reputational_risk": <0.0 to 1.0>
    }},
    "uncertainty_factors": [
        {{
            "factor": "<uncertainty source>",
            "impact_score": <0.0 to 1.0>,
            "probability": <0.0 to 1.0>,
            "description": "<detailed explanation>"
        }}
    ],
    "black_swan_probability": <0.0 to 1.0>,
    "risk_horizon": "<immediate|short_term|medium_term|long_term>",
    "risk_mitigation_suggestions": [
        "<specific hedging or mitigation strategy>"
    ],
    "stress_scenarios": [
        {{
            "scenario": "<worst case scenario description>",
            "probability": <0.0 to 1.0>,
            "potential_impact": "<description of impact>"
        }}
    ],
    "risk_correlations": [
        {{
            "risk1": "<first risk factor>",
            "risk2": "<second risk factor>",
            "correlation_strength": <0.0 to 1.0>,
            "explanation": "<how risks are related>"
        }}
    ],
    "reasoning": "<detailed risk assessment reasoning>",
    "confidence": <0.0 to 1.0>
}}"""

    def parse_llm_response(self, response: LLMResponse, processing_time_ms: int) -> RiskAssessmentAnalysis:
        try:
            data = json.loads(response.content)

            return RiskAssessmentAnalysis(
                agent_type=self.agent_type,
                confidence=data.get('confidence', 0.5),
                processing_time_ms=processing_time_ms,
                model_used=response.model_used,
                overall_risk_score=data.get('overall_risk_score', 0.5),
                risk_categories=data.get('risk_categories', {}),
                uncertainty_factors=data.get('uncertainty_factors', []),
                black_swan_probability=data.get('black_swan_probability', 0.0),
                risk_mitigation_suggestions=data.get('risk_mitigation_suggestions', []),
                reasoning=data.get('reasoning', ''),
                analysis_data={
                    'risk_horizon': data.get('risk_horizon', 'medium_term'),
                    'stress_scenarios': data.get('stress_scenarios', []),
                    'risk_correlations': data.get('risk_correlations', [])
                }
            )
        except Exception as e:
            return RiskAssessmentAnalysis(
                agent_type=self.agent_type,
                confidence=0.0,
                processing_time_ms=processing_time_ms,
                model_used=response.model_used,
                overall_risk_score=0.5,
                risk_categories={},
                uncertainty_factors=[],
                black_swan_probability=0.0,
                risk_mitigation_suggestions=[],
                reasoning=f"Failed to parse response: {str(e)}",
                warnings=[f"Response parsing error: {str(e)}"]
            )


class MarketImpactAgent(BaseFinancialAgent):
    """Specialized agent for market impact prediction."""

    def __init__(self, llm_client: MultiProviderLLMClient):
        super().__init__(AgentType.MARKET_IMPACT, llm_client, model_preference="openai")

    def get_system_prompt(self) -> str:
        return """You are a specialized market impact prediction agent with expertise in:

        - Price impact modeling across different time horizons (1h, 1d, 5d, 20d)
        - Volatility forecasting and impact on options markets
        - Volume prediction and liquidity analysis
        - Sector spillover effects and correlation analysis
        - Market microstructure and order flow dynamics

        Predict market impacts with precision, providing:
        1. Multi-horizon price impact predictions with confidence intervals
        2. Volatility impact assessment and term structure effects
        3. Volume impact and liquidity considerations
        4. Sector and market spillover analysis
        5. Timing analysis for optimal execution strategies

        Base predictions on historical patterns, market structure, and current conditions.
        Consider both direct and indirect market effects."""

    def create_analysis_prompt(self, article: NewsArticle, context: Dict[str, Any] = None) -> str:
        market_context = ""
        if context:
            if context.get('market_session'):
                market_context += f"Market Session: {context['market_session']}\n"
            if context.get('current_volatility'):
                market_context += f"Current Market Volatility: {context['current_volatility']}\n"
            if context.get('average_volume'):
                market_context += f"Recent Average Volume: {context['average_volume']}\n"

        return f"""Predict the market impact of this financial news:

{market_context}
Title: {article.title}
Content: {article.content}
Tickers: {', '.join(article.tickers) if article.tickers else 'None'}
Published: {article.published_date}

Provide market impact prediction in JSON format:
{{
    "price_impact_prediction": {{
        "1h": <expected price change % in 1 hour>,
        "1d": <expected price change % in 1 day>,
        "5d": <expected price change % in 5 days>,
        "20d": <expected price change % in 20 days>
    }},
    "price_impact_confidence": {{
        "1h": <confidence 0.0 to 1.0>,
        "1d": <confidence 0.0 to 1.0>,
        "5d": <confidence 0.0 to 1.0>,
        "20d": <confidence 0.0 to 1.0>
    }},
    "volatility_impact": <expected volatility increase factor (e.g., 1.5 = 50% increase)>,
    "volume_impact": <expected volume increase factor (e.g., 2.0 = 100% increase)>,
    "impact_timing": "<immediate|delayed|gradual|uncertain>",
    "sector_spillover": {{
        "<related_sector>": <spillover impact factor -1.0 to 1.0>
    }},
    "market_timing": "<pre_market|market_open|intraday|after_hours|multi_day>",
    "liquidity_impact": {{
        "bid_ask_spread_widening": <factor 1.0+ for spread increase>,
        "market_depth_reduction": <0.0 to 1.0 for depth decrease>,
        "trading_halt_probability": <0.0 to 1.0>
    }},
    "technical_factors": [
        {{
            "factor": "<technical factor name>",
            "impact": "<positive|negative|neutral>",
            "strength": <0.0 to 1.0>,
            "description": "<how this factor affects price>"
        }}
    ],
    "historical_comparisons": [
        {{
            "similar_event": "<description of similar past event>",
            "historical_impact": "<what happened historically>",
            "similarity_score": <0.0 to 1.0>
        }}
    ],
    "reasoning": "<detailed market impact analysis>",
    "confidence": <0.0 to 1.0>
}}"""

    def parse_llm_response(self, response: LLMResponse, processing_time_ms: int) -> MarketImpactAnalysis:
        try:
            data = json.loads(response.content)

            return MarketImpactAnalysis(
                agent_type=self.agent_type,
                confidence=data.get('confidence', 0.5),
                processing_time_ms=processing_time_ms,
                model_used=response.model_used,
                price_impact_prediction=data.get('price_impact_prediction', {}),
                volatility_impact=data.get('volatility_impact', 1.0),
                volume_impact=data.get('volume_impact', 1.0),
                sector_spillover=data.get('sector_spillover', {}),
                market_timing=data.get('market_timing', 'uncertain'),
                reasoning=data.get('reasoning', ''),
                analysis_data={
                    'price_impact_confidence': data.get('price_impact_confidence', {}),
                    'impact_timing': data.get('impact_timing', 'uncertain'),
                    'liquidity_impact': data.get('liquidity_impact', {}),
                    'technical_factors': data.get('technical_factors', []),
                    'historical_comparisons': data.get('historical_comparisons', [])
                }
            )
        except Exception as e:
            return MarketImpactAnalysis(
                agent_type=self.agent_type,
                confidence=0.0,
                processing_time_ms=processing_time_ms,
                model_used=response.model_used,
                price_impact_prediction={},
                volatility_impact=1.0,
                volume_impact=1.0,
                sector_spillover={},
                market_timing='uncertain',
                reasoning=f"Failed to parse response: {str(e)}",
                warnings=[f"Response parsing error: {str(e)}"]
            )


class SignalGenerationAgent(BaseFinancialAgent):
    """Specialized agent for trading signal generation."""

    def __init__(self, llm_client: MultiProviderLLMClient):
        super().__init__(AgentType.SIGNAL_GENERATION, llm_client, model_preference="anthropic")

    def get_system_prompt(self) -> str:
        return """You are a specialized trading signal generation agent with expertise in:

        - Synthesizing multi-dimensional analysis into actionable trading signals
        - Risk-adjusted position sizing and portfolio management
        - Signal timing and execution strategy optimization
        - Stop-loss and take-profit level determination
        - Signal validation and confidence calibration

        Generate trading signals with precision, providing:
        1. Clear directional signals with strength quantification
        2. Optimal position sizing based on risk-reward analysis
        3. Entry/exit timing and execution strategies
        4. Risk management parameters (stop-loss, take-profit)
        5. Signal validation against historical patterns

        Prioritize capital preservation and risk-adjusted returns.
        Only generate high-confidence signals with clear risk management."""

    def create_analysis_prompt(self, article: NewsArticle, context: Dict[str, Any] = None) -> str:
        # Context should include outputs from other agents
        agent_context = ""
        if context:
            if 'sentiment_analysis' in context:
                sentiment = context['sentiment_analysis']
                agent_context += f"Sentiment Score: {sentiment.sentiment_score:.2f}, Strength: {sentiment.sentiment_strength:.2f}\n"

            if 'risk_analysis' in context:
                risk = context['risk_analysis']
                agent_context += f"Risk Score: {risk.overall_risk_score:.2f}\n"

            if 'market_impact' in context:
                impact = context['market_impact']
                agent_context += f"1d Price Impact: {impact.price_impact_prediction.get('1d', 0.0):.2f}%\n"

            if 'events' in context:
                events = context['events']
                if events.events:
                    agent_context += f"Key Events: {[e.get('event_type') for e in events.events[:3]]}\n"

        return f"""Generate a trading signal based on comprehensive analysis of this news:

ARTICLE:
Title: {article.title}
Content: {article.content}
Tickers: {', '.join(article.tickers) if article.tickers else 'None'}
Published: {article.published_date}

ANALYSIS CONTEXT:
{agent_context}

Generate trading signal in JSON format:
{{
    "signal_strength": <float from -1.0 (strong sell) to 1.0 (strong buy)>,
    "signal_direction": "<strong_buy|buy|hold|sell|strong_sell|hedge>",
    "signal_confidence": <0.0 to 1.0>,
    "urgency_level": <integer from 1 (low) to 10 (critical)>,
    "time_horizon": "<intraday|short_term|medium_term|long_term>",
    "position_sizing": <0.0 to 1.0 (fraction of portfolio)>,
    "entry_strategy": {{
        "timing": "<immediate|wait_for_dip|scale_in|wait_for_confirmation>",
        "entry_price_target": <target entry price or percentage>,
        "execution_notes": "<specific execution recommendations>"
    }},
    "risk_management": {{
        "stop_loss": <stop loss percentage from entry>,
        "take_profit": <take profit percentage from entry>,
        "risk_reward_ratio": <expected reward to risk ratio>,
        "max_loss_tolerance": <maximum acceptable loss percentage>
    }},
    "supporting_factors": [
        "<factor 1 supporting the signal>",
        "<factor 2 supporting the signal>"
    ],
    "risk_factors": [
        "<risk factor 1 that could invalidate signal>",
        "<risk factor 2 that could invalidate signal>"
    ],
    "signal_invalidation_conditions": [
        "<condition that would invalidate this signal>"
    ],
    "expected_catalysts": [
        {{
            "catalyst": "<what could drive the expected move>",
            "timing": "<when this catalyst might occur>",
            "probability": <0.0 to 1.0>
        }}
    ],
    "historical_precedent": {{
        "similar_situations": "<description of similar past situations>",
        "historical_success_rate": <0.0 to 1.0>,
        "average_historical_return": <percentage return from similar signals>
    }},
    "reasoning": "<comprehensive reasoning for the signal>",
    "confidence": <0.0 to 1.0>
}}"""

    def parse_llm_response(self, response: LLMResponse, processing_time_ms: int) -> SignalGenerationAnalysis:
        try:
            data = json.loads(response.content)

            return SignalGenerationAnalysis(
                agent_type=self.agent_type,
                confidence=data.get('confidence', 0.5),
                processing_time_ms=processing_time_ms,
                model_used=response.model_used,
                signal_strength=data.get('signal_strength', 0.0),
                signal_direction=data.get('signal_direction', 'hold'),
                urgency_level=data.get('urgency_level', 5),
                time_horizon=data.get('time_horizon', 'medium_term'),
                position_sizing=data.get('position_sizing', 0.0),
                stop_loss=data.get('risk_management', {}).get('stop_loss'),
                take_profit=data.get('risk_management', {}).get('take_profit'),
                supporting_factors=data.get('supporting_factors', []),
                risk_factors=data.get('risk_factors', []),
                reasoning=data.get('reasoning', ''),
                analysis_data={
                    'signal_confidence': data.get('signal_confidence', 0.5),
                    'entry_strategy': data.get('entry_strategy', {}),
                    'risk_management': data.get('risk_management', {}),
                    'signal_invalidation_conditions': data.get('signal_invalidation_conditions', []),
                    'expected_catalysts': data.get('expected_catalysts', []),
                    'historical_precedent': data.get('historical_precedent', {})
                }
            )
        except Exception as e:
            return SignalGenerationAnalysis(
                agent_type=self.agent_type,
                confidence=0.0,
                processing_time_ms=processing_time_ms,
                model_used=response.model_used,
                signal_strength=0.0,
                signal_direction='hold',
                urgency_level=1,
                time_horizon='medium_term',
                position_sizing=0.0,
                stop_loss=None,
                take_profit=None,
                supporting_factors=[],
                risk_factors=[],
                reasoning=f"Failed to parse response: {str(e)}",
                warnings=[f"Response parsing error: {str(e)}"]
            )


# Enhanced orchestrator that includes all agents
class EnhancedMultiAgentOrchestrator:
    """Enhanced orchestrator with all specialized agents."""

    def __init__(self, llm_client: MultiProviderLLMClient):
        self.llm_client = llm_client

        # Initialize all specialized agents
        from domains.market_data.agents.multi_agent_framework import (
            SentimentAgent, EntityRecognitionAgent, EventDetectionAgent
        )

        self.agents = {
            AgentType.SENTIMENT: SentimentAgent(llm_client),
            AgentType.ENTITY_RECOGNITION: EntityRecognitionAgent(llm_client),
            AgentType.EVENT_DETECTION: EventDetectionAgent(llm_client),
            AgentType.RISK_ASSESSMENT: RiskAssessmentAgent(llm_client),
            AgentType.MARKET_IMPACT: MarketImpactAgent(llm_client),
            AgentType.SIGNAL_GENERATION: SignalGenerationAgent(llm_client)
        }

        # Analysis settings
        self.parallel_execution = True
        self.timeout_seconds = 45  # Longer timeout for full analysis

        # Performance tracking
        self.full_analysis_count = 0
        self.total_analysis_time_ms = 0

    async def run_comprehensive_analysis(self, article: NewsArticle,
                                       context: Dict[str, Any] = None) -> Dict[str, Any]:
        """Run comprehensive analysis with all agents and generate final signal."""

        start_time = datetime.now()

        # Phase 1: Run core analysis agents (sentiment, entities, events, risk)
        core_agents = [
            AgentType.SENTIMENT,
            AgentType.ENTITY_RECOGNITION,
            AgentType.EVENT_DETECTION,
            AgentType.RISK_ASSESSMENT
        ]

        logger.info(f"Starting Phase 1: Core analysis for article '{article.title[:50]}...'")

        core_results = {}
        if self.parallel_execution:
            tasks = [self.agents[agent_type].analyze(article, context) for agent_type in core_agents]
            core_analyses = await asyncio.gather(*tasks, return_exceptions=True)

            for i, agent_type in enumerate(core_agents):
                if isinstance(core_analyses[i], Exception):
                    logger.error(f"Core agent {agent_type.value} failed: {core_analyses[i]}")
                    core_results[agent_type.value] = None
                else:
                    core_results[agent_type.value] = core_analyses[i]
        else:
            for agent_type in core_agents:
                try:
                    analysis = await self.agents[agent_type].analyze(article, context)
                    core_results[agent_type.value] = analysis
                except Exception as e:
                    logger.error(f"Core agent {agent_type.value} failed: {e}")
                    core_results[agent_type.value] = None

        # Phase 2: Market impact analysis (uses core results)
        logger.debug("Starting Phase 2: Market impact analysis")

        market_context = dict(context or {})
        if core_results.get('sentiment'):
            market_context['sentiment_analysis'] = core_results['sentiment']
        if core_results.get('risk_assessment'):
            market_context['risk_analysis'] = core_results['risk_assessment']

        try:
            market_impact_analysis = await self.agents[AgentType.MARKET_IMPACT].analyze(
                article, market_context
            )
            core_results['market_impact'] = market_impact_analysis
        except Exception as e:
            logger.error(f"Market impact agent failed: {e}")
            core_results['market_impact'] = None

        # Phase 3: Signal generation (uses all previous results)
        logger.debug("Starting Phase 3: Signal generation")

        signal_context = dict(market_context)
        signal_context.update(core_results)

        try:
            signal_analysis = await self.agents[AgentType.SIGNAL_GENERATION].analyze(
                article, signal_context
            )
            core_results['signal_generation'] = signal_analysis
        except Exception as e:
            logger.error(f"Signal generation agent failed: {e}")
            core_results['signal_generation'] = None

        # Calculate ensemble metrics
        analysis_time_ms = int((datetime.now() - start_time).total_seconds() * 1000)
        ensemble_confidence = self._calculate_ensemble_confidence(core_results)

        # Update performance metrics
        self.full_analysis_count += 1
        self.total_analysis_time_ms += analysis_time_ms

        # Create comprehensive analysis result
        comprehensive_result = {
            'article_id': article.id,
            'article_title': article.title,
            'analysis_timestamp': start_time.isoformat(),
            'analysis_time_ms': analysis_time_ms,
            'ensemble_confidence': ensemble_confidence,
            'agent_results': core_results,
            'signal_generated': core_results.get('signal_generation') is not None,
            'actionable_signal': self._is_actionable_signal(core_results.get('signal_generation')),
        }

        # Add summary metrics
        if core_results.get('signal_generation'):
            signal = core_results['signal_generation']
            comprehensive_result['signal_summary'] = {
                'direction': signal.signal_direction,
                'strength': signal.signal_strength,
                'urgency': signal.urgency_level,
                'confidence': signal.confidence,
                'position_size': signal.position_sizing
            }

        logger.info(f"Comprehensive analysis completed in {analysis_time_ms}ms "
                   f"(confidence: {ensemble_confidence:.2f})")

        return comprehensive_result

    def _calculate_ensemble_confidence(self, results: Dict[str, Any]) -> float:
        """Calculate ensemble confidence across all agent results."""
        confidences = []
        weights = {
            'sentiment': 0.15,
            'entity_recognition': 0.10,
            'event_detection': 0.20,
            'risk_assessment': 0.20,
            'market_impact': 0.15,
            'signal_generation': 0.20
        }

        total_weight = 0.0
        weighted_confidence = 0.0

        for agent_type, result in results.items():
            if result and hasattr(result, 'confidence'):
                weight = weights.get(agent_type, 0.1)
                weighted_confidence += result.confidence * weight
                total_weight += weight

        return weighted_confidence / total_weight if total_weight > 0 else 0.0

    def _is_actionable_signal(self, signal_analysis) -> bool:
        """Determine if the signal is actionable."""
        if not signal_analysis:
            return False

        # Signal must have high confidence and clear direction
        if (signal_analysis.confidence >= 0.7 and
            signal_analysis.signal_direction not in ['hold'] and
            signal_analysis.urgency_level >= 6 and
            abs(signal_analysis.signal_strength) >= 0.5):
            return True

        return False

    def get_comprehensive_metrics(self) -> Dict[str, Any]:
        """Get comprehensive performance metrics."""
        avg_analysis_time = (
            self.total_analysis_time_ms / self.full_analysis_count
            if self.full_analysis_count > 0 else 0
        )

        agent_metrics = {}
        for agent_type, agent in self.agents.items():
            agent_metrics[agent_type.value] = agent.get_performance_metrics()

        return {
            'comprehensive_analysis_count': self.full_analysis_count,
            'avg_comprehensive_analysis_time_ms': avg_analysis_time,
            'parallel_execution': self.parallel_execution,
            'timeout_seconds': self.timeout_seconds,
            'total_agents': len(self.agents),
            'agent_metrics': agent_metrics
        }