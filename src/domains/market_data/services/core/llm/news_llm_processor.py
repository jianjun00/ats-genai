"""
LLM-Powered News Processing Service

This module implements the core LLM processing pipeline for financial news analysis,
integrating with the multi-agent framework for comprehensive analysis and signal generation.

Enhanced with 2025 state-of-the-art multi-agent architecture and optimized for the ATS platform.
"""

import logging
import asyncio
from datetime import datetime
from typing import Dict, List, Optional, Any
import json
import time

import asyncpg
from dataclasses import dataclass
import numpy as np

from shared.exceptions.base import ATSBaseException
from core.platform.config.environment import Environment


@dataclass
class NewsArticle:
    """News article data structure"""
    id: str  # Changed from int to str for flexibility
    title: str
    content: str
    summary: Optional[str] = None  # Add summary field
    url: str = ""
    source: str = ""
    published_date: Optional[datetime] = None
    tickers: List[str] = None  # Changed from symbols to tickers for consistency
    raw_data: Dict[str, Any] = None

    def __post_init__(self):
        if self.tickers is None:
            self.tickers = []
        if self.raw_data is None:
            self.raw_data = {}
        if self.published_date is None:
            self.published_date = datetime.now()


@dataclass
class FinancialEntity:
    """Financial entity extracted from news"""
    text: str
    entity_type: str  # COMPANY, PERSON, FINANCIAL_METRIC, etc.
    confidence: float
    normalized_value: str = None
    metadata: Dict[str, Any] = None

    def __post_init__(self):
        if self.metadata is None:
            self.metadata = {}


@dataclass
class FinancialEvent:
    """Structured financial event"""
    event_type: str
    event_subtype: str
    entities: List[FinancialEntity]
    confidence: float
    timeline: Dict[str, Any]
    impact_assessment: Dict[str, Any]
    metadata: Dict[str, Any] = None

    def __post_init__(self):
        if self.metadata is None:
            self.metadata = {}


@dataclass
class SentimentScore:
    """Sentiment analysis result"""
    compound_score: float  # -1.0 to 1.0
    positive: float
    negative: float
    neutral: float
    confidence: float
    uncertainty: float
    model_scores: Dict[str, float] = None
    explanations: List[str] = None

    def __post_init__(self):
        if self.model_scores is None:
            self.model_scores = {}
        if self.explanations is None:
            self.explanations = []


@dataclass
class RAGContext:
    """RAG-based contextual analysis"""
    historical_precedents: List[Dict[str, Any]]
    market_context: Dict[str, Any]
    company_context: Dict[str, Any]
    sector_context: Dict[str, Any]
    confidence: float
    retrieval_quality: float


@dataclass
class NewsAnalysisResult:
    """Complete analysis result for a news article"""
    article: NewsArticle
    entities: List[FinancialEntity]
    events: List[FinancialEvent]
    sentiment: SentimentScore
    rag_context: RAGContext
    processing_metadata: Dict[str, Any]
    quality_score: float
    completeness_score: float


class LLMProcessingError(ATSBaseException):
    """Exception raised during LLM processing"""


class FinancialNERExtractor:
    """
    Financial Named Entity Recognition using state-of-the-art LLM techniques
    """

    def __init__(self, llm_client):
        self.llm_client = llm_client
        self.entity_types = {
            'COMPANY': ['public_company', 'private_company', 'subsidiary'],
            'PERSON': ['ceo', 'cfo', 'analyst', 'official', 'executive'],
            'FINANCIAL_METRIC': ['revenue', 'profit', 'eps', 'guidance', 'valuation'],
            'EVENT': ['earnings', 'merger', 'acquisition', 'ipo', 'spinoff'],
            'AMOUNT': ['dollar_amount', 'percentage', 'quantity', 'market_cap'],
            'DATE': ['announcement_date', 'deadline', 'fiscal_period'],
            'INSTRUMENT': ['stock', 'bond', 'option', 'future', 'etf'],
            'LOCATION': ['country', 'exchange', 'market', 'region']
        }

    async def extract_entities(self, text: str) -> List[FinancialEntity]:
        """Extract financial entities from text using advanced LLM techniques"""
        try:
            prompt = self._build_ner_prompt(text)

            # Use GPT-4o for best financial NER performance
            response = await self.llm_client.complete(
                model="gpt-4o",
                prompt=prompt,
                temperature=0.1,  # Low temperature for consistency
                max_tokens=2000
            )

            # Parse structured response
            entities_data = json.loads(response)
            entities = []

            for entity_type, entity_list in entities_data.items():
                if entity_type in self.entity_types:
                    for entity_info in entity_list:
                        if isinstance(entity_info, dict):
                            entity = FinancialEntity(
                                text=entity_info.get('text', ''),
                                entity_type=entity_type,
                                confidence=entity_info.get('confidence', 0.0),
                                normalized_value=entity_info.get('normalized', None),
                                metadata=entity_info.get('metadata', {})
                            )
                            entities.append(entity)

            return entities

        except Exception as e:
            logging.error(f"NER extraction failed: {e}")
            raise LLMProcessingError(f"Failed to extract entities: {e}")

    def _build_ner_prompt(self, text: str) -> str:
        """Build NER extraction prompt optimized for financial content"""
        return f"""Extract financial entities from the following news article. Focus on entities that are relevant to trading and investment decisions.

Article: {text}

Return a JSON object with the following structure:
{{
    "COMPANY": [
        {{"text": "Apple Inc.", "confidence": 0.95, "normalized": "AAPL", "metadata": {{"exchange": "NASDAQ"}}}},
        ...
    ],
    "PERSON": [
        {{"text": "Tim Cook", "confidence": 0.90, "normalized": "CEO_AAPL", "metadata": {{"role": "CEO", "company": "AAPL"}}}},
        ...
    ],
    "FINANCIAL_METRIC": [
        {{"text": "$1.25 earnings per share", "confidence": 0.88, "normalized": "1.25_EPS", "metadata": {{"metric_type": "eps", "value": 1.25}}}},
        ...
    ],
    "EVENT": [
        {{"text": "quarterly earnings", "confidence": 0.92, "normalized": "Q4_EARNINGS", "metadata": {{"event_type": "earnings", "period": "Q4"}}}},
        ...
    ],
    "AMOUNT": [
        {{"text": "$50 billion revenue", "confidence": 0.94, "normalized": "50000000000_USD", "metadata": {{"amount": 50000000000, "currency": "USD"}}}},
        ...
    ],
    "DATE": [
        {{"text": "January 25, 2024", "confidence": 0.96, "normalized": "2024-01-25", "metadata": {{"date_type": "announcement"}}}},
        ...
    ],
    "INSTRUMENT": [
        {{"text": "common stock", "confidence": 0.85, "normalized": "EQUITY", "metadata": {{"instrument_type": "equity"}}}},
        ...
    ],
    "LOCATION": [
        {{"text": "NYSE", "confidence": 0.98, "normalized": "NYSE", "metadata": {{"location_type": "exchange"}}}},
        ...
    ]
}}

Requirements:
1. Include confidence scores (0.0-1.0) for each entity
2. Normalize entities where possible (ticker symbols, standard formats)
3. Include relevant metadata for context
4. Focus on entities that could impact trading decisions
5. Handle variations and abbreviations common in financial news
"""


class FinancialEventExtractor:
    """
    Extract structured financial events and analyze causal relationships
    """

    def __init__(self, llm_client):
        self.llm_client = llm_client
        self.event_types = {
            'earnings': ['earnings_announcement', 'earnings_guidance', 'earnings_surprise'],
            'corporate_action': ['merger', 'acquisition', 'spinoff', 'dividend', 'stock_split'],
            'regulatory': ['fda_approval', 'regulatory_filing', 'investigation', 'fine'],
            'management': ['ceo_change', 'executive_hire', 'board_change'],
            'market': ['ipo', 'delisting', 'index_inclusion', 'rating_change'],
            'operational': ['product_launch', 'factory_closure', 'layoffs', 'expansion']
        }

    async def extract_events(self, text: str, entities: List[FinancialEntity]) -> List[FinancialEvent]:
        """Extract financial events and analyze causal relationships"""
        try:
            # Build context from extracted entities
            entity_context = self._build_entity_context(entities)

            prompt = self._build_event_extraction_prompt(text, entity_context)

            response = await self.llm_client.complete(
                model="gpt-4o",
                prompt=prompt,
                temperature=0.2,
                max_tokens=3000
            )

            events_data = json.loads(response)
            events = []

            for event_data in events_data.get('events', []):
                event = FinancialEvent(
                    event_type=event_data.get('event_type'),
                    event_subtype=event_data.get('event_subtype'),
                    entities=[self._parse_event_entity(e) for e in event_data.get('entities', [])],
                    confidence=event_data.get('confidence', 0.0),
                    timeline=event_data.get('timeline', {}),
                    impact_assessment=event_data.get('impact_assessment', {}),
                    metadata=event_data.get('metadata', {})
                )
                events.append(event)

            return events

        except Exception as e:
            logging.error(f"Event extraction failed: {e}")
            raise LLMProcessingError(f"Failed to extract events: {e}")

    def _build_entity_context(self, entities: List[FinancialEntity]) -> str:
        """Build context string from extracted entities"""
        context_parts = []
        for entity in entities:
            context_parts.append(f"{entity.entity_type}: {entity.text} (confidence: {entity.confidence})")
        return "\n".join(context_parts)

    def _parse_event_entity(self, entity_data: Dict) -> FinancialEntity:
        """Parse entity data from event extraction"""
        return FinancialEntity(
            text=entity_data.get('text', ''),
            entity_type=entity_data.get('type', ''),
            confidence=entity_data.get('confidence', 0.0),
            normalized_value=entity_data.get('normalized', None),
            metadata=entity_data.get('metadata', {})
        )

    def _build_event_extraction_prompt(self, text: str, entity_context: str) -> str:
        """Build event extraction prompt"""
        return f"""Analyze the following news article and extract structured financial events with their causal relationships.

Article: {text}

Extracted Entities:
{entity_context}

Return a JSON object with the following structure:
{{
    "events": [
        {{
            "event_type": "earnings",
            "event_subtype": "earnings_surprise",
            "confidence": 0.92,
            "entities": [
                {{"text": "Apple Inc.", "type": "COMPANY", "confidence": 0.95, "role": "subject"}},
                {{"text": "$1.25 EPS", "type": "FINANCIAL_METRIC", "confidence": 0.90, "role": "metric"}}
            ],
            "timeline": {{
                "announcement_date": "2024-01-25",
                "effective_date": "2024-01-25",
                "impact_timeline": "immediate"
            }},
            "impact_assessment": {{
                "market_impact": "high",
                "price_impact_direction": "positive",
                "volatility_impact": "medium",
                "sector_impact": "technology"
            }},
            "causal_relationships": [
                {{
                    "cause": "earnings beat expectations",
                    "effect": "positive market reaction",
                    "confidence": 0.85
                }}
            ],
            "metadata": {{
                "severity": 8,
                "urgency": 9,
                "market_session": "after_hours"
            }}
        }}
    ],
    "global_causal_chains": [
        {{
            "chain": ["earnings_beat", "analyst_upgrades", "price_increase"],
            "confidence": 0.78,
            "timeline": "24_hours"
        }}
    ]
}}

Requirements:
1. Identify all significant financial events in the article
2. For each event, specify type, subtype, and confidence
3. Map relevant entities to their roles in each event
4. Analyze causal relationships between events
5. Assess market impact potential
6. Provide timeline information where available
7. Include severity (1-10) and urgency (1-10) ratings
"""


class EnsembleSentimentAnalyzer:
    """
    Multi-model ensemble sentiment analyzer with uncertainty quantification
    """

    def __init__(self, llm_client):
        self.llm_client = llm_client
        self.models = {
            'finbert': {'weight': 0.4, 'model': 'finbert-sentiment'},
            'finllama': {'weight': 0.3, 'model': 'finllama-7b'},
            'bloomberg_gpt': {'weight': 0.2, 'model': 'bloomberg-gpt'},
            'gpt4_financial': {'weight': 0.1, 'model': 'gpt-4o'}
        }

    async def analyze_sentiment(self, text: str, context: Dict[str, Any] = None) -> SentimentScore:
        """Analyze sentiment using ensemble of models with uncertainty quantification"""
        try:
            # Get predictions from all models
            model_predictions = await self._get_model_predictions(text, context or {})

            # Calculate ensemble prediction
            ensemble_score = self._calculate_ensemble_score(model_predictions)

            # Calculate uncertainty
            uncertainty = self._calculate_uncertainty(model_predictions)

            # Generate explanations
            explanations = self._generate_explanations(model_predictions, text)

            return SentimentScore(
                compound_score=ensemble_score['compound'],
                positive=ensemble_score['positive'],
                negative=ensemble_score['negative'],
                neutral=ensemble_score['neutral'],
                confidence=ensemble_score['confidence'],
                uncertainty=uncertainty,
                model_scores={name: pred['compound'] for name, pred in model_predictions.items()},
                explanations=explanations
            )

        except Exception as e:
            logging.error(f"Sentiment analysis failed: {e}")
            raise LLMProcessingError(f"Failed to analyze sentiment: {e}")

    async def _get_model_predictions(self, text: str, context: Dict) -> Dict[str, Dict]:
        """Get predictions from all models in parallel"""
        tasks = []
        for model_name, model_config in self.models.items():
            if model_name == 'gpt4_financial':
                # Use GPT-4o with financial sentiment prompt
                task = self._get_gpt4_sentiment(text, context)
            else:
                # Use specialized financial models (placeholder for actual implementation)
                task = self._get_specialized_model_sentiment(model_name, text, context)
            tasks.append(task)

        results = await asyncio.gather(*tasks, return_exceptions=True)

        predictions = {}
        for i, (model_name, result) in enumerate(zip(self.models.keys(), results)):
            if not isinstance(result, Exception):
                predictions[model_name] = result
            else:
                logging.warning(f"Model {model_name} failed: {result}")
                # Use neutral prediction as fallback
                predictions[model_name] = {
                    'compound': 0.0, 'positive': 0.0, 'negative': 0.0, 'neutral': 1.0, 'confidence': 0.0
                }

        return predictions

    async def _get_gpt4_sentiment(self, text: str, context: Dict) -> Dict:
        """Get sentiment from GPT-4o with financial prompt"""
        prompt = f"""Analyze the sentiment of the following financial news text. Consider the context of financial markets and trading implications.

Text: {text}

Context: {json.dumps(context, indent=2)}

Return a JSON object with the following structure:
{{
    "compound": 0.65,
    "positive": 0.7,
    "negative": 0.1,
    "neutral": 0.2,
    "confidence": 0.85,
    "reasoning": "The article discusses strong quarterly results and positive guidance, indicating bullish sentiment for the stock.",
    "key_phrases": ["beat expectations", "strong guidance", "revenue growth"],
    "financial_context": "earnings_positive"
}}

Requirements:
1. compound: Overall sentiment from -1.0 (very negative) to 1.0 (very positive)
2. positive/negative/neutral: Component scores (sum should be close to 1.0)
3. confidence: How confident you are in the analysis (0.0 to 1.0)
4. Consider financial market context and trading implications
5. Focus on actionable sentiment for trading decisions
"""

        response = await self.llm_client.complete(
            model="gpt-4o",
            prompt=prompt,
            temperature=0.1,
            max_tokens=800
        )

        return json.loads(response)

    async def _get_specialized_model_sentiment(self, model_name: str, text: str, context: Dict) -> Dict:
        """Get sentiment from specialized financial models (placeholder)"""
        # This would integrate with actual FinBERT, FinLlama, etc.
        # For now, return a placeholder structure
        return {
            'compound': 0.0,
            'positive': 0.33,
            'negative': 0.33,
            'neutral': 0.34,
            'confidence': 0.5,
            'reasoning': f"Placeholder for {model_name} analysis",
            'model': model_name
        }

    def _calculate_ensemble_score(self, model_predictions: Dict) -> Dict:
        """Calculate weighted ensemble score"""
        weighted_scores = {'compound': 0, 'positive': 0, 'negative': 0, 'neutral': 0}
        total_weight = 0
        confidence_sum = 0

        for model_name, prediction in model_predictions.items():
            if model_name in self.models:
                weight = self.models[model_name]['weight'] * prediction.get('confidence', 0.5)

                for score_type in weighted_scores.keys():
                    weighted_scores[score_type] += prediction.get(score_type, 0) * weight

                total_weight += weight
                confidence_sum += prediction.get('confidence', 0.5)

        # Normalize scores
        if total_weight > 0:
            for score_type in weighted_scores.keys():
                weighted_scores[score_type] /= total_weight

        # Average confidence
        ensemble_confidence = confidence_sum / len(model_predictions) if model_predictions else 0

        return {
            'compound': weighted_scores['compound'],
            'positive': weighted_scores['positive'],
            'negative': weighted_scores['negative'],
            'neutral': weighted_scores['neutral'],
            'confidence': ensemble_confidence
        }

    def _calculate_uncertainty(self, model_predictions: Dict) -> float:
        """Calculate uncertainty based on model disagreement"""
        if len(model_predictions) < 2:
            return 1.0

        compound_scores = [pred.get('compound', 0) for pred in model_predictions.values()]
        std_dev = np.std(compound_scores)

        # Normalize standard deviation to 0-1 scale
        # Higher disagreement = higher uncertainty
        uncertainty = min(std_dev * 2, 1.0)  # Scale factor of 2

        return uncertainty

    def _generate_explanations(self, model_predictions: Dict, text: str) -> List[str]:
        """Generate explanations for sentiment analysis"""
        explanations = []

        for model_name, prediction in model_predictions.items():
            if 'reasoning' in prediction:
                explanations.append(f"{model_name}: {prediction['reasoning']}")

        # Add ensemble explanation
        compound_scores = [pred.get('compound', 0) for pred in model_predictions.values()]
        avg_score = np.mean(compound_scores)

        if avg_score > 0.3:
            explanations.append("Ensemble: Overall positive sentiment detected across models")
        elif avg_score < -0.3:
            explanations.append("Ensemble: Overall negative sentiment detected across models")
        else:
            explanations.append("Ensemble: Mixed or neutral sentiment detected")

        return explanations


class FinancialRAGProcessor:
    """
    Retrieval-Augmented Generation for contextual financial news analysis
    """

    def __init__(self, llm_client, vector_db_client, market_data_client):
        self.llm_client = llm_client
        self.vector_db = vector_db_client
        self.market_data = market_data_client

    async def get_contextual_analysis(self, article: NewsArticle, entities: List[FinancialEntity]) -> RAGContext:
        """Get contextual analysis using RAG approach"""
        try:
            # Create embedding for article
            article_embedding = await self._create_embedding(article.content)

            # Retrieve similar historical events
            historical_precedents = await self._retrieve_historical_precedents(
                article_embedding, entities
            )

            # Get current market context
            market_context = await self._get_market_context(entities)

            # Get company-specific context
            company_context = await self._get_company_context(entities)

            # Get sector context
            sector_context = await self._get_sector_context(entities)

            # Calculate retrieval quality
            retrieval_quality = self._assess_retrieval_quality(
                historical_precedents, market_context, company_context
            )

            return RAGContext(
                historical_precedents=historical_precedents,
                market_context=market_context,
                company_context=company_context,
                sector_context=sector_context,
                confidence=self._calculate_rag_confidence(retrieval_quality),
                retrieval_quality=retrieval_quality
            )

        except Exception as e:
            logging.error(f"RAG processing failed: {e}")
            raise LLMProcessingError(f"Failed to process RAG context: {e}")

    async def _create_embedding(self, text: str) -> List[float]:
        """Create embedding for text using appropriate model"""
        # This would use a financial-domain embedding model
        # Placeholder for actual implementation
        return [0.0] * 384  # Typical embedding dimension

    async def _retrieve_historical_precedents(self, embedding: List[float], entities: List[FinancialEntity]) -> List[Dict]:
        """Retrieve similar historical events from vector database"""
        # This would query the vector database for similar events
        # Placeholder for actual implementation
        return [
            {
                "event": "Similar earnings announcement",
                "date": "2023-10-25",
                "outcome": "10% price increase",
                "similarity": 0.85,
                "context": "Technology sector earnings beat"
            }
        ]

    async def _get_market_context(self, entities: List[FinancialEntity]) -> Dict:
        """Get current market context"""
        # Get relevant market indicators, VIX, sector performance, etc.
        return {
            "market_regime": "bull_market",
            "vix_level": 18.5,
            "sector_performance": {
                "technology": 0.025,  # 2.5% recent performance
                "healthcare": -0.010
            },
            "market_session": "after_hours"
        }

    async def _get_company_context(self, entities: List[FinancialEntity]) -> Dict:
        """Get company-specific context"""
        # Get company financials, recent performance, analyst ratings
        company_entities = [e for e in entities if e.entity_type == 'COMPANY']

        context = {}
        for entity in company_entities[:3]:  # Limit to top 3 companies
            context[entity.normalized_value or entity.text] = {
                "recent_performance": 0.05,  # 5% recent performance
                "analyst_rating": "BUY",
                "pe_ratio": 25.6,
                "market_cap": 2800000000000,  # $2.8T
                "volatility": 0.28
            }

        return context

    async def _get_sector_context(self, entities: List[FinancialEntity]) -> Dict:
        """Get sector-specific context"""
        return {
            "technology": {
                "recent_performance": 0.032,
                "pe_ratio": 28.5,
                "outlook": "positive"
            }
        }

    def _assess_retrieval_quality(self, precedents: List, market_ctx: Dict, company_ctx: Dict) -> float:
        """Assess quality of retrieved context"""
        quality_score = 0.8  # Base score

        # Adjust based on data availability
        if len(precedents) > 3:
            quality_score += 0.1
        if market_ctx and len(market_ctx) > 3:
            quality_score += 0.1

        return min(quality_score, 1.0)

    def _calculate_rag_confidence(self, retrieval_quality: float) -> float:
        """Calculate confidence in RAG analysis"""
        return retrieval_quality * 0.9  # Slightly lower than retrieval quality


class LLMNewsProcessor:
    """
    Main LLM news processing pipeline coordinator
    """

    def __init__(self, pool: asyncpg.Pool, env: Environment):
        self.pool = pool
        self.env = env
        self.llm_client = self._initialize_llm_client()

        # Initialize processing components
        self.ner_extractor = FinancialNERExtractor(self.llm_client)
        self.event_extractor = FinancialEventExtractor(self.llm_client)
        self.sentiment_analyzer = EnsembleSentimentAnalyzer(self.llm_client)
        self.rag_processor = FinancialRAGProcessor(
            self.llm_client, None, None  # TODO: Initialize vector DB and market data clients
        )

        self.processing_stats = {
            'articles_processed': 0,
            'processing_times': [],
            'error_count': 0
        }

    def _initialize_llm_client(self):
        """Initialize LLM client with multiple providers"""
        # This would initialize the actual LLM client
        # Placeholder for implementation
        class MockLLMClient:
            async def complete(self, model: str, prompt: str, **kwargs) -> str:
                # Placeholder response
                if "extract financial entities" in prompt.lower():
                    return '{"COMPANY": [{"text": "Apple Inc.", "confidence": 0.95, "normalized": "AAPL"}]}'
                elif "extract structured financial events" in prompt.lower():
                    return '{"events": [{"event_type": "earnings", "confidence": 0.9}]}'
                elif "analyze the sentiment" in prompt.lower():
                    return '{"compound": 0.5, "positive": 0.6, "negative": 0.2, "neutral": 0.2, "confidence": 0.8}'
                return "{}"

        return MockLLMClient()

    async def process_article(self, article: NewsArticle) -> NewsAnalysisResult:
        """Process a single news article through the complete LLM pipeline"""
        start_time = time.time()

        try:
            # Step 1: Named Entity Recognition
            entities = await self.ner_extractor.extract_entities(article.content)

            # Step 2: Event Extraction
            events = await self.event_extractor.extract_events(article.content, entities)

            # Step 3: Sentiment Analysis
            sentiment = await self.sentiment_analyzer.analyze_sentiment(article.content)

            # Step 4: RAG-based Contextual Analysis
            rag_context = await self.rag_processor.get_contextual_analysis(article, entities)

            # Calculate quality and completeness scores
            quality_score = self._calculate_quality_score(entities, events, sentiment, rag_context)
            completeness_score = self._calculate_completeness_score(entities, events, sentiment)

            # Create processing metadata
            processing_time = (time.time() - start_time) * 1000  # milliseconds
            processing_metadata = {
                'processing_time_ms': processing_time,
                'model_versions': {
                    'ner': '1.0',
                    'event_extraction': '1.0',
                    'sentiment': '1.0',
                    'rag': '1.0'
                },
                'processing_timestamp': datetime.now().isoformat(),
                'processing_node': 'node_1'  # Would be actual node ID
            }

            # Store results in database
            analysis_id = await self._store_analysis_results(
                article, entities, events, sentiment, rag_context,
                processing_metadata, quality_score, completeness_score
            )

            # Update stats
            self.processing_stats['articles_processed'] += 1
            self.processing_stats['processing_times'].append(processing_time)

            return NewsAnalysisResult(
                article=article,
                entities=entities,
                events=events,
                sentiment=sentiment,
                rag_context=rag_context,
                processing_metadata=processing_metadata,
                quality_score=quality_score,
                completeness_score=completeness_score
            )

        except Exception as e:
            self.processing_stats['error_count'] += 1
            logging.error(f"Failed to process article {article.id}: {e}")
            raise LLMProcessingError(f"Article processing failed: {e}")

    def _calculate_quality_score(self, entities: List[FinancialEntity],
                                events: List[FinancialEvent],
                                sentiment: SentimentScore,
                                rag_context: RAGContext) -> float:
        """Calculate overall quality score for the analysis"""
        scores = []

        # Entity extraction quality
        if entities:
            avg_entity_confidence = np.mean([e.confidence for e in entities])
            scores.append(avg_entity_confidence)

        # Event extraction quality
        if events:
            avg_event_confidence = np.mean([e.confidence for e in events])
            scores.append(avg_event_confidence)

        # Sentiment confidence
        scores.append(sentiment.confidence)

        # RAG context quality
        scores.append(rag_context.confidence)

        return np.mean(scores) if scores else 0.5

    def _calculate_completeness_score(self, entities: List[FinancialEntity],
                                    events: List[FinancialEvent],
                                    sentiment: SentimentScore) -> float:
        """Calculate completeness score based on extracted information"""
        completeness = 0.0

        # Check if we extracted entities
        if entities:
            completeness += 0.4

        # Check if we extracted events
        if events:
            completeness += 0.3

        # Sentiment is always present
        completeness += 0.3

        return completeness

    async def _store_analysis_results(self, article: NewsArticle,
                                    entities: List[FinancialEntity],
                                    events: List[FinancialEvent],
                                    sentiment: SentimentScore,
                                    rag_context: RAGContext,
                                    processing_metadata: Dict,
                                    quality_score: float,
                                    completeness_score: float) -> int:
        """Store analysis results in the database"""
        try:
            async with self.pool.acquire() as conn:
                # Prepare data for storage
                entities_json = {
                    'extracted_entities': {
                        entity.entity_type: [
                            {
                                'text': e.text,
                                'confidence': e.confidence,
                                'normalized': e.normalized_value,
                                'metadata': e.metadata
                            }
                            for e in entities if e.entity_type == entity.entity_type
                        ]
                        for entity in entities
                    }
                }

                events_json = [
                    {
                        'event_type': event.event_type,
                        'event_subtype': event.event_subtype,
                        'confidence': event.confidence,
                        'timeline': event.timeline,
                        'impact_assessment': event.impact_assessment,
                        'entities': [
                            {
                                'text': e.text,
                                'type': e.entity_type,
                                'confidence': e.confidence
                            }
                            for e in event.entities
                        ]
                    }
                    for event in events
                ]

                sentiment_json = {
                    'ensemble': sentiment.compound_score,
                    'positive': sentiment.positive,
                    'negative': sentiment.negative,
                    'neutral': sentiment.neutral,
                    'confidence': sentiment.confidence,
                    'uncertainty': sentiment.uncertainty,
                    'model_scores': sentiment.model_scores,
                    'explanations': sentiment.explanations
                }

                rag_json = {
                    'historical_precedents': rag_context.historical_precedents,
                    'market_context': rag_context.market_context,
                    'company_context': rag_context.company_context,
                    'sector_context': rag_context.sector_context,
                    'confidence': rag_context.confidence,
                    'retrieval_quality': rag_context.retrieval_quality
                }

                # Insert into dev_news_llm_analysis
                result = await conn.fetchrow("""
                    INSERT INTO dev_news_llm_analysis (
                        news_id, news_source, extracted_entities, detected_events,
                        sentiment_scores, sentiment_ensemble, sentiment_confidence,
                        sentiment_uncertainty, historical_precedents, market_context,
                        company_context, sector_context, rag_confidence,
                        processing_latency_ms, model_versions, processing_node,
                        data_quality_score, analysis_completeness
                    ) VALUES (
                        $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13,
                        $14, $15, $16, $17, $18
                    ) RETURNING id
                """,
                article.id, article.source, entities_json, events_json,
                sentiment_json, sentiment.compound_score, sentiment.confidence,
                sentiment.uncertainty, rag_context.historical_precedents,
                rag_context.market_context, rag_context.company_context,
                rag_context.sector_context, rag_context.confidence,
                processing_metadata['processing_time_ms'],
                processing_metadata['model_versions'],
                processing_metadata['processing_node'],
                quality_score, completeness_score
                )

                return result['id']

        except Exception as e:
            logging.error(f"Failed to store analysis results: {e}")
            raise LLMProcessingError(f"Database storage failed: {e}")

    async def get_processing_stats(self) -> Dict[str, Any]:
        """Get current processing statistics"""
        avg_processing_time = (
            np.mean(self.processing_stats['processing_times'])
            if self.processing_stats['processing_times'] else 0
        )

        return {
            'articles_processed': self.processing_stats['articles_processed'],
            'average_processing_time_ms': avg_processing_time,
            'error_count': self.processing_stats['error_count'],
            'error_rate': (
                self.processing_stats['error_count'] /
                max(self.processing_stats['articles_processed'], 1)
            )
        }