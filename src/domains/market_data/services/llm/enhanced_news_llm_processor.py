#!/usr/bin/env python3
"""
Enhanced LLM News Processing Service

This module implements the enhanced LLM processing pipeline that integrates with the
multi-agent framework for comprehensive financial news analysis and signal generation.

Features:
- Multi-agent analysis coordination
- Database integration for storing analysis results
- Performance tracking and metrics
- Error handling and fallback mechanisms
- Signal generation and validation
"""

import logging
import asyncio
from datetime import datetime
from typing import Dict, List, Optional, Any, Tuple
import json
import time

import asyncpg
from dataclasses import dataclass, asdict

from infrastructure.llm.multi_provider_client import MultiProviderLLMClient
from domains.market_data.services.llm.news_llm_processor import NewsArticle
from domains.market_data.agents.specialized_agents import EnhancedMultiAgentOrchestrator
from core.config.environment import Environment

logger = logging.getLogger(__name__)


@dataclass
class NewsAnalysisResult:
    """Comprehensive news analysis result from multi-agent framework."""
    article_id: str
    analysis_timestamp: datetime
    processing_time_ms: int
    
    # Agent analysis results
    sentiment_analysis: Optional[Any] = None
    entity_analysis: Optional[Any] = None
    event_analysis: Optional[Any] = None
    risk_analysis: Optional[Any] = None
    market_impact_analysis: Optional[Any] = None
    signal_analysis: Optional[Any] = None
    
    # Ensemble metrics
    ensemble_confidence: float = 0.0
    signal_generated: bool = False
    actionable_signal: bool = False
    
    # Performance metrics
    agent_performance: Dict[str, Any] = None
    error_details: List[str] = None
    
    def __post_init__(self):
        if self.agent_performance is None:
            self.agent_performance = {}
        if self.error_details is None:
            self.error_details = []


class LLMProcessingError(Exception):
    """Custom exception for LLM processing errors."""
    pass


class EnhancedLLMNewsProcessor:
    """Enhanced LLM news processor with multi-agent integration."""
    
    def __init__(self, llm_client: MultiProviderLLMClient, db_pool: Optional[asyncpg.Pool] = None,
                 env: Optional[Environment] = None):
        self.llm_client = llm_client
        self.db_pool = db_pool
        self.env = env or Environment()
        
        # Initialize multi-agent orchestrator
        self.orchestrator = EnhancedMultiAgentOrchestrator(llm_client)
        
        # Performance tracking
        self.processing_stats = {
            'total_articles_processed': 0,
            'successful_analyses': 0,
            'failed_analyses': 0,
            'signals_generated': 0,
            'actionable_signals': 0,
            'total_processing_time_ms': 0,
            'avg_processing_time_ms': 0.0,
            'start_time': datetime.now()
        }
        
        # Error tracking
        self.error_stats = {
            'llm_errors': 0,
            'database_errors': 0,
            'orchestration_errors': 0,
            'parsing_errors': 0
        }
    
    async def process_article(self, article: NewsArticle, 
                            context: Dict[str, Any] = None) -> NewsAnalysisResult:
        """Process a news article through the enhanced multi-agent pipeline."""
        
        start_time = datetime.now()
        
        try:
            logger.info(f"Starting enhanced LLM processing for article: {article.id}")
            
            # Run comprehensive multi-agent analysis
            comprehensive_analysis = await self.orchestrator.run_comprehensive_analysis(
                article, context
            )
            
            # Extract individual agent results
            agent_results = comprehensive_analysis.get('agent_results', {})
            
            # Create analysis result
            analysis_result = NewsAnalysisResult(
                article_id=article.id,
                analysis_timestamp=start_time,
                processing_time_ms=comprehensive_analysis.get('analysis_time_ms', 0),
                sentiment_analysis=agent_results.get('sentiment'),
                entity_analysis=agent_results.get('entity_recognition'),
                event_analysis=agent_results.get('event_detection'),
                risk_analysis=agent_results.get('risk_assessment'),
                market_impact_analysis=agent_results.get('market_impact'),
                signal_analysis=agent_results.get('signal_generation'),
                ensemble_confidence=comprehensive_analysis.get('ensemble_confidence', 0.0),
                signal_generated=comprehensive_analysis.get('signal_generated', False),
                actionable_signal=comprehensive_analysis.get('actionable_signal', False),
                agent_performance=self.orchestrator.get_comprehensive_metrics()
            )
            
            # Store analysis in database if available
            if self.db_pool:
                await self._store_analysis_result(analysis_result, article)
            
            # Update statistics
            self._update_processing_stats(analysis_result, success=True)
            
            logger.info(f"Enhanced LLM processing completed for article {article.id} "
                       f"(confidence: {analysis_result.ensemble_confidence:.2f}, "
                       f"signal: {'Yes' if analysis_result.signal_generated else 'No'})")
            
            return analysis_result
            
        except Exception as e:
            # Handle processing errors
            processing_time_ms = int((datetime.now() - start_time).total_seconds() * 1000)
            
            error_result = NewsAnalysisResult(
                article_id=article.id,
                analysis_timestamp=start_time,
                processing_time_ms=processing_time_ms,
                ensemble_confidence=0.0,
                signal_generated=False,
                actionable_signal=False,
                error_details=[f"Processing failed: {str(e)}"]
            )
            
            self._update_processing_stats(error_result, success=False)
            self._record_error(e, 'orchestration_errors')
            
            logger.error(f"Enhanced LLM processing failed for article {article.id}: {e}")
            
            return error_result
    
    async def _store_analysis_result(self, result: NewsAnalysisResult, article: NewsArticle):
        """Store analysis result in the database."""
        try:
            async with self.db_pool.acquire() as conn:
                # Store in dev_news_llm_analysis table
                analysis_id = await conn.fetchval("""
                    INSERT INTO dev_news_llm_analysis 
                    (article_id, article_title, article_content, processing_timestamp,
                     processing_time_ms, llm_model_used, extracted_entities, detected_events,
                     sentiment_ensemble, sentiment_confidence, market_impact_prediction,
                     risk_assessment, rag_context, quality_score, confidence_score,
                     signal_generated, analysis_metadata)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17)
                    RETURNING id
                """,
                result.article_id,
                article.title,
                article.content,
                result.analysis_timestamp,
                result.processing_time_ms,
                self._get_primary_model_used(result),
                self._serialize_entity_analysis(result.entity_analysis),
                self._serialize_event_analysis(result.event_analysis),
                self._get_sentiment_score(result.sentiment_analysis),
                self._get_sentiment_confidence(result.sentiment_analysis),
                self._serialize_market_impact(result.market_impact_analysis),
                self._serialize_risk_assessment(result.risk_analysis),
                {},  # RAG context - placeholder
                self._calculate_quality_score(result),
                result.ensemble_confidence,
                result.signal_generated,
                self._serialize_analysis_metadata(result)
                )
                
                # If a signal was generated, also store it in the signals table
                if result.signal_generated and result.signal_analysis:
                    await self._store_generated_signal(conn, analysis_id, result.signal_analysis, article)
                
                logger.debug(f"Analysis result stored with ID: {analysis_id}")
                
        except Exception as e:
            self._record_error(e, 'database_errors')
            logger.error(f"Failed to store analysis result: {e}")
            # Don't re-raise to avoid breaking the processing pipeline
    
    async def _store_generated_signal(self, conn: asyncpg.Connection, analysis_id: int,
                                    signal_analysis: Any, article: NewsArticle):
        """Store generated trading signal in the signals table."""
        try:
            # Determine market session
            market_session = self._determine_market_session()
            
            signal_id = await conn.fetchval("""
                INSERT INTO dev_critical_news_signals
                (symbol, signal_type, signal_category, urgency_level, market_session,
                 signal_strength, signal_confidence, news_llm_analysis_ids,
                 multi_agent_analysis_ids, predicted_price_impact_1h, predicted_price_impact_1d,
                 predicted_price_impact_5d, predicted_price_impact_20d, risk_score,
                 recommended_action, position_sizing_recommendation, time_horizon,
                 key_entities, key_themes, contributing_factors, model_attribution)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21)
                RETURNING id
            """,
            article.tickers[0] if article.tickers else "UNKNOWN",  # primary symbol
            self._classify_signal_type(signal_analysis),
            self._get_signal_category(signal_analysis),
            signal_analysis.urgency_level,
            market_session,
            signal_analysis.signal_strength,
            signal_analysis.confidence,
            [analysis_id],  # news_llm_analysis_ids
            [],  # multi_agent_analysis_ids (placeholder)
            self._get_price_impact(signal_analysis, '1h'),
            self._get_price_impact(signal_analysis, '1d'),
            self._get_price_impact(signal_analysis, '5d'),
            self._get_price_impact(signal_analysis, '20d'),
            self._get_risk_score_from_signal(signal_analysis),
            signal_analysis.signal_direction,
            signal_analysis.position_sizing,
            signal_analysis.time_horizon,
            self._extract_key_entities(signal_analysis),
            signal_analysis.supporting_factors,
            self._create_contributing_factors(signal_analysis),
            self._create_model_attribution(signal_analysis)
            )
            
            logger.info(f"Trading signal stored with ID: {signal_id} for article {article.id}")
            
        except Exception as e:
            logger.error(f"Failed to store trading signal: {e}")
            raise
    
    def _update_processing_stats(self, result: NewsAnalysisResult, success: bool):
        """Update processing statistics."""
        self.processing_stats['total_articles_processed'] += 1
        self.processing_stats['total_processing_time_ms'] += result.processing_time_ms
        
        if success:
            self.processing_stats['successful_analyses'] += 1
            if result.signal_generated:
                self.processing_stats['signals_generated'] += 1
            if result.actionable_signal:
                self.processing_stats['actionable_signals'] += 1
        else:
            self.processing_stats['failed_analyses'] += 1
        
        # Update average processing time
        self.processing_stats['avg_processing_time_ms'] = (
            self.processing_stats['total_processing_time_ms'] / 
            self.processing_stats['total_articles_processed']
        )
    
    def _record_error(self, error: Exception, error_type: str):
        """Record error statistics."""
        if error_type in self.error_stats:
            self.error_stats[error_type] += 1
        logger.debug(f"Recorded error: {error_type} - {str(error)}")
    
    def _get_primary_model_used(self, result: NewsAnalysisResult) -> str:
        """Extract primary model used from analysis result."""
        if result.signal_analysis and hasattr(result.signal_analysis, 'model_used'):
            return result.signal_analysis.model_used
        elif result.sentiment_analysis and hasattr(result.sentiment_analysis, 'model_used'):
            return result.sentiment_analysis.model_used
        return "unknown"
    
    def _serialize_entity_analysis(self, entity_analysis) -> Dict[str, Any]:
        """Serialize entity analysis for database storage."""
        if not entity_analysis:
            return {}
        
        return {
            'companies': getattr(entity_analysis, 'companies', []),
            'people': getattr(entity_analysis, 'people', []),
            'financial_products': getattr(entity_analysis, 'financial_products', []),
            'confidence': getattr(entity_analysis, 'confidence', 0.0)
        }
    
    def _serialize_event_analysis(self, event_analysis) -> Dict[str, Any]:
        """Serialize event analysis for database storage."""
        if not event_analysis:
            return {}
        
        return {
            'events': getattr(event_analysis, 'events', []),
            'event_categories': getattr(event_analysis, 'event_categories', []),
            'event_importance': getattr(event_analysis, 'event_importance', {}),
            'confidence': getattr(event_analysis, 'confidence', 0.0)
        }
    
    def _get_sentiment_score(self, sentiment_analysis) -> float:
        """Extract sentiment score from analysis."""
        if sentiment_analysis and hasattr(sentiment_analysis, 'sentiment_score'):
            return sentiment_analysis.sentiment_score
        return 0.0
    
    def _get_sentiment_confidence(self, sentiment_analysis) -> float:
        """Extract sentiment confidence from analysis."""
        if sentiment_analysis and hasattr(sentiment_analysis, 'confidence'):
            return sentiment_analysis.confidence
        return 0.0
    
    def _serialize_market_impact(self, market_impact_analysis) -> Dict[str, Any]:
        """Serialize market impact analysis for database storage."""
        if not market_impact_analysis:
            return {}
        
        return {
            'price_impact_prediction': getattr(market_impact_analysis, 'price_impact_prediction', {}),
            'volatility_impact': getattr(market_impact_analysis, 'volatility_impact', 1.0),
            'volume_impact': getattr(market_impact_analysis, 'volume_impact', 1.0),
            'market_timing': getattr(market_impact_analysis, 'market_timing', 'uncertain'),
            'confidence': getattr(market_impact_analysis, 'confidence', 0.0)
        }
    
    def _serialize_risk_assessment(self, risk_analysis) -> Dict[str, Any]:
        """Serialize risk assessment for database storage."""
        if not risk_analysis:
            return {}
        
        return {
            'overall_risk_score': getattr(risk_analysis, 'overall_risk_score', 0.5),
            'risk_categories': getattr(risk_analysis, 'risk_categories', {}),
            'uncertainty_factors': getattr(risk_analysis, 'uncertainty_factors', []),
            'black_swan_probability': getattr(risk_analysis, 'black_swan_probability', 0.0),
            'confidence': getattr(risk_analysis, 'confidence', 0.0)
        }
    
    def _calculate_quality_score(self, result: NewsAnalysisResult) -> float:
        """Calculate overall quality score for the analysis."""
        confidence_scores = []
        
        if result.sentiment_analysis and hasattr(result.sentiment_analysis, 'confidence'):
            confidence_scores.append(result.sentiment_analysis.confidence)
        if result.entity_analysis and hasattr(result.entity_analysis, 'confidence'):
            confidence_scores.append(result.entity_analysis.confidence)
        if result.event_analysis and hasattr(result.event_analysis, 'confidence'):
            confidence_scores.append(result.event_analysis.confidence)
        if result.risk_analysis and hasattr(result.risk_analysis, 'confidence'):
            confidence_scores.append(result.risk_analysis.confidence)
        if result.market_impact_analysis and hasattr(result.market_impact_analysis, 'confidence'):
            confidence_scores.append(result.market_impact_analysis.confidence)
        if result.signal_analysis and hasattr(result.signal_analysis, 'confidence'):
            confidence_scores.append(result.signal_analysis.confidence)
        
        return sum(confidence_scores) / len(confidence_scores) if confidence_scores else 0.0
    
    def _serialize_analysis_metadata(self, result: NewsAnalysisResult) -> Dict[str, Any]:
        """Create analysis metadata for database storage."""
        return {
            'ensemble_confidence': result.ensemble_confidence,
            'signal_generated': result.signal_generated,
            'actionable_signal': result.actionable_signal,
            'processing_time_ms': result.processing_time_ms,
            'agent_performance': result.agent_performance,
            'error_details': result.error_details
        }
    
    def _determine_market_session(self) -> str:
        """Determine current market session."""
        now = datetime.now()
        hour = now.hour
        weekday = now.weekday()
        
        if weekday >= 5:  # Weekend
            return 'closed'
        elif 4 <= hour < 9:
            return 'pre_market'
        elif 9 <= hour < 16:
            return 'market_hours'
        elif 16 <= hour < 20:
            return 'after_hours'
        else:
            return 'closed'
    
    def _classify_signal_type(self, signal_analysis) -> str:
        """Classify the signal type based on analysis."""
        if not signal_analysis:
            return 'general'
        
        # Check supporting factors for signal classification
        supporting_factors = getattr(signal_analysis, 'supporting_factors', [])
        
        for factor in supporting_factors:
            factor_lower = factor.lower()
            if 'earnings' in factor_lower:
                return 'earnings_signal'
            elif any(word in factor_lower for word in ['merger', 'acquisition', 'm&a']):
                return 'ma_signal'
            elif 'regulatory' in factor_lower or 'fda' in factor_lower:
                return 'regulatory_signal'
            elif 'analyst' in factor_lower or 'rating' in factor_lower:
                return 'analyst_signal'
        
        return 'news_signal'
    
    def _get_signal_category(self, signal_analysis) -> str:
        """Get signal category based on direction and strength."""
        if not signal_analysis:
            return 'neutral'
        
        strength = getattr(signal_analysis, 'signal_strength', 0.0)
        
        if strength >= 0.3:
            return 'bullish'
        elif strength <= -0.3:
            return 'bearish'
        else:
            return 'neutral'
    
    def _get_price_impact(self, signal_analysis, horizon: str) -> Optional[float]:
        """Extract price impact prediction for specific horizon."""
        if not signal_analysis or not hasattr(signal_analysis, 'analysis_data'):
            return None
        
        analysis_data = signal_analysis.analysis_data or {}
        market_impact = analysis_data.get('market_impact', {})
        price_impacts = market_impact.get('price_impact_prediction', {})
        
        return price_impacts.get(horizon)
    
    def _get_risk_score_from_signal(self, signal_analysis) -> float:
        """Extract risk score from signal analysis."""
        if not signal_analysis:
            return 0.5
        
        # Use inverse of confidence as risk approximation
        confidence = getattr(signal_analysis, 'confidence', 0.5)
        return max(0.0, min(1.0, 1.0 - confidence))
    
    def _extract_key_entities(self, signal_analysis) -> Dict[str, Any]:
        """Extract key entities from signal analysis."""
        if not signal_analysis:
            return {}
        
        # This would extract entities from the analysis context
        return {}  # Placeholder
    
    def _create_contributing_factors(self, signal_analysis) -> Dict[str, Any]:
        """Create contributing factors dictionary."""
        if not signal_analysis:
            return {}
        
        supporting = getattr(signal_analysis, 'supporting_factors', [])
        risk_factors = getattr(signal_analysis, 'risk_factors', [])
        
        return {
            'supporting_factors': supporting,
            'risk_factors': risk_factors,
            'signal_strength': getattr(signal_analysis, 'signal_strength', 0.0)
        }
    
    def _create_model_attribution(self, signal_analysis) -> Dict[str, Any]:
        """Create model attribution dictionary."""
        if not signal_analysis:
            return {}
        
        return {
            'primary_model': getattr(signal_analysis, 'model_used', 'unknown'),
            'confidence': getattr(signal_analysis, 'confidence', 0.0),
            'processing_time_ms': getattr(signal_analysis, 'processing_time_ms', 0)
        }
    
    def get_processing_metrics(self) -> Dict[str, Any]:
        """Get comprehensive processing metrics."""
        uptime = (datetime.now() - self.processing_stats['start_time']).total_seconds()
        
        return {
            'processing_stats': dict(self.processing_stats),
            'error_stats': dict(self.error_stats),
            'uptime_seconds': uptime,
            'success_rate': (
                self.processing_stats['successful_analyses'] / 
                max(1, self.processing_stats['total_articles_processed'])
            ),
            'signal_generation_rate': (
                self.processing_stats['signals_generated'] /
                max(1, self.processing_stats['successful_analyses'])
            ),
            'actionable_signal_rate': (
                self.processing_stats['actionable_signals'] /
                max(1, self.processing_stats['signals_generated'])
            ),
            'orchestrator_metrics': self.orchestrator.get_comprehensive_metrics()
        }
    
    async def health_check(self) -> Dict[str, Any]:
        """Perform health check on the processing system."""
        health_status = {
            'status': 'healthy',
            'timestamp': datetime.now().isoformat(),
            'components': {}
        }
        
        try:
            # Check LLM client
            llm_healthy = await self.llm_client.health_check()
            health_status['components']['llm_client'] = 'healthy' if llm_healthy else 'degraded'
            
            # Check database
            if self.db_pool:
                try:
                    async with self.db_pool.acquire() as conn:
                        await conn.fetchval("SELECT 1")
                    health_status['components']['database'] = 'healthy'
                except Exception:
                    health_status['components']['database'] = 'degraded'
            
            # Check orchestrator
            health_status['components']['orchestrator'] = 'healthy'
            
            # Overall status
            if any(status == 'degraded' for status in health_status['components'].values()):
                health_status['status'] = 'degraded'
            
            health_status['metrics'] = self.get_processing_metrics()
            
        except Exception as e:
            health_status['status'] = 'error'
            health_status['error'] = str(e)
        
        return health_status


# Factory function
async def create_enhanced_llm_processor(
    llm_client: MultiProviderLLMClient,
    db_pool: Optional[asyncpg.Pool] = None,
    env: Optional[Environment] = None
) -> EnhancedLLMNewsProcessor:
    """Create and initialize enhanced LLM news processor."""
    
    processor = EnhancedLLMNewsProcessor(llm_client, db_pool, env)
    
    logger.info("Enhanced LLM News Processor initialized with multi-agent framework")
    
    return processor