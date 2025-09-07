#!/usr/bin/env python3
"""
Historic News Signal Extraction Service

This service processes existing historic news data and extracts enhanced trading signals
using our local LLM infrastructure. It builds upon the existing insights in the database
and creates structured trading signals for backtesting and analysis.

Key Features:
- Processes 104K+ historic news records from 2016-2025
- Enhanced signal extraction using local FinBERT + GPT-2/Llama models  
- Creates structured signal database for backtesting
- Batch processing with checkpoints and error handling
- Integration with existing news infrastructure
"""

import asyncio
import logging
import json
import time
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime, timedelta
from dataclasses import dataclass, asdict
import traceback

import asyncpg
from core.platform.config.environment import Environment
from infrastructure.llm.hybrid_llm_client import HybridLLMClient
from infrastructure.llm.local_model_client import LocalModelClient

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)


@dataclass
class TradingSignal:
    """Structured trading signal extracted from news."""
    news_id: str
    ticker: str
    signal_type: str  # 'BUY', 'SELL', 'HOLD', 'WATCH'
    confidence: float  # 0.0 to 1.0
    sentiment: str    # 'positive', 'negative', 'neutral'
    sentiment_score: float  # -1.0 to 1.0
    impact_timeframe: str   # 'immediate', 'short_term', 'medium_term', 'long_term'
    key_factors: List[str]  # Main reasons for the signal
    risk_level: str   # 'low', 'medium', 'high'
    target_price_change: Optional[float]  # Expected price change percentage
    published_utc: datetime
    processed_at: datetime
    model_version: str
    

@dataclass
class EnhancedNewsAnalysis:
    """Enhanced analysis of news article using local LLMs."""
    news_id: str
    title: str
    description: str
    original_insights: List[Dict]
    enhanced_signals: List[TradingSignal]
    market_impact_score: float  # 0.0 to 1.0
    volatility_indicator: float  # 0.0 to 1.0  
    sector_impact: List[str]
    entity_mentions: Dict[str, List[str]]  # companies, people, events
    processing_time_seconds: float
    model_performance_metrics: Dict[str, Any]


class HistoricNewsSignalExtractor:
    """Main service for extracting trading signals from historic news data."""
    
    def __init__(self):
        self.env: Optional[Environment] = None
        self.db_pool: Optional[asyncpg.Pool] = None
        self.llm_client: Optional[HybridLLMClient] = None
        self.local_client: Optional[LocalModelClient] = None
        
        # Processing metrics
        self.processed_count = 0
        self.success_count = 0
        self.error_count = 0
        self.start_time = None
        
        # Configuration
        self.batch_size = 100
        self.checkpoint_frequency = 1000
        self.max_retries = 3
        
    async def initialize(self):
        """Initialize all service components."""
        logger.info("Initializing Historic News Signal Extraction Service")
        
        try:
            # Initialize environment
            self.env = Environment()
            
            # Initialize database connection
            await self._initialize_database()
            
            # Initialize local LLM client for fast sentiment analysis
            await self._initialize_local_llm()
            
            # Initialize hybrid LLM client for complex analysis
            await self._initialize_hybrid_llm()
            
            logger.info("Historic News Signal Extractor initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize signal extractor: {e}")
            raise
    
    async def _initialize_database(self):
        """Initialize database connection pool."""
        try:
            database_url = self.env.get_database_url()
            
            self.db_pool = await asyncpg.create_pool(
                database_url,
                min_size=5,
                max_size=20,
                command_timeout=60,
                server_settings={
                    'jit': 'off',
                    'application_name': 'historic_news_signal_extractor'
                }
            )
            
            # Test connection and create tables
            async with self.db_pool.acquire() as conn:
                await conn.fetchval("SELECT 1")
                await self._create_signal_tables(conn)
            
            logger.info("Database connection pool initialized")
            
        except Exception as e:
            logger.error(f"Failed to initialize database: {e}")
            raise
    
    async def _create_signal_tables(self, conn):
        """Create necessary tables for signal storage."""
        
        # Trading signals table
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS dev_trading_signals (
                id SERIAL PRIMARY KEY,
                news_id VARCHAR(255) NOT NULL,
                ticker VARCHAR(10) NOT NULL,
                signal_type VARCHAR(10) NOT NULL,
                confidence DECIMAL(4,3) NOT NULL,
                sentiment VARCHAR(20) NOT NULL,
                sentiment_score DECIMAL(4,3) NOT NULL,
                impact_timeframe VARCHAR(20) NOT NULL,
                key_factors JSONB,
                risk_level VARCHAR(10) NOT NULL,
                target_price_change DECIMAL(6,3),
                published_utc TIMESTAMPTZ NOT NULL,
                processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                model_version VARCHAR(50) NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                UNIQUE(news_id, ticker)
            )
        """)
        
        # Enhanced news analysis table  
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS dev_enhanced_news_analysis (
                id SERIAL PRIMARY KEY,
                news_id VARCHAR(255) NOT NULL UNIQUE,
                title TEXT NOT NULL,
                description TEXT,
                original_insights JSONB,
                market_impact_score DECIMAL(4,3) NOT NULL,
                volatility_indicator DECIMAL(4,3) NOT NULL,
                sector_impact JSONB,
                entity_mentions JSONB,
                processing_time_seconds DECIMAL(8,3) NOT NULL,
                model_performance_metrics JSONB,
                processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
        """)
        
        # Processing checkpoint table
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS dev_news_processing_checkpoints (
                id SERIAL PRIMARY KEY,
                batch_id VARCHAR(50) NOT NULL,
                last_processed_news_id VARCHAR(255),
                processed_count INTEGER NOT NULL DEFAULT 0,
                success_count INTEGER NOT NULL DEFAULT 0,
                error_count INTEGER NOT NULL DEFAULT 0,
                started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                completed_at TIMESTAMPTZ
            )
        """)
        
        # Create indexes for performance
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_trading_signals_ticker_date ON dev_trading_signals(ticker, published_utc)")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_trading_signals_signal_type ON dev_trading_signals(signal_type)")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_enhanced_analysis_impact ON dev_enhanced_news_analysis(market_impact_score)")
        
        logger.info("Signal database tables created successfully")
    
    async def _initialize_local_llm(self):
        """Initialize local model client for fast sentiment analysis."""
        try:
            self.local_client = LocalModelClient()
            
            # Configure with FinBERT for sentiment analysis
            model_config = {
                'sentiment_model': 'ProsusAI/finbert',
                'device': 'cuda' if self.env.environment != 'test' else 'cpu',
                'quantization': True
            }
            
            await self.local_client.initialize(model_config)
            logger.info("Local LLM client initialized with FinBERT")
            
        except Exception as e:
            logger.error(f"Failed to initialize local LLM: {e}")
            # Continue without local LLM if it fails
            self.local_client = None
    
    async def _initialize_hybrid_llm(self):
        """Initialize hybrid LLM client for complex analysis."""
        try:
            # Configure with available providers
            provider_configs = {}
            
            # Try to use local models first, then fallback to APIs
            if self.local_client:
                provider_configs['local'] = {
                    'client': self.local_client,
                    'priority': 1
                }
            
            # Add API providers as fallback
            import os
            if os.getenv('OPENAI_API_KEY'):
                provider_configs['openai'] = {
                    'api_key': os.getenv('OPENAI_API_KEY'),
                    'model': 'gpt-4o-mini',
                    'priority': 2
                }
            
            if provider_configs:
                self.llm_client = HybridLLMClient(provider_configs)
                await self.llm_client.initialize()
                logger.info(f"Hybrid LLM client initialized with providers: {list(provider_configs.keys())}")
            else:
                logger.warning("No LLM providers available - will use basic processing only")
                
        except Exception as e:
            logger.error(f"Failed to initialize hybrid LLM: {e}")
            self.llm_client = None
    
    async def process_historic_news(self, 
                                  start_date: Optional[datetime] = None,
                                  end_date: Optional[datetime] = None,
                                  limit: Optional[int] = None,
                                  checkpoint_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Process historic news data and extract enhanced trading signals.
        
        Args:
            start_date: Start date for processing (default: earliest news)
            end_date: End date for processing (default: latest news)  
            limit: Maximum number of records to process
            checkpoint_id: Resume from specific checkpoint
            
        Returns:
            Processing statistics and results
        """
        self.start_time = time.time()
        batch_id = checkpoint_id or f"historic_backfill_{int(self.start_time)}"
        
        logger.info(f"Starting historic news signal extraction (batch: {batch_id})")
        logger.info(f"Date range: {start_date} to {end_date}")
        logger.info(f"Limit: {limit}")
        
        try:
            # Create checkpoint record
            await self._create_checkpoint(batch_id)
            
            # Get news records to process
            news_records = await self._get_news_records(start_date, end_date, limit, checkpoint_id)
            logger.info(f"Found {len(news_records)} news records to process")
            
            if not news_records:
                logger.info("No news records to process")
                return self._get_processing_stats(batch_id)
            
            # Process in batches
            batch_results = []
            for i in range(0, len(news_records), self.batch_size):
                batch = news_records[i:i + self.batch_size]
                
                logger.info(f"Processing batch {i//self.batch_size + 1}/{(len(news_records)-1)//self.batch_size + 1} ({len(batch)} records)")
                
                batch_result = await self._process_news_batch(batch, batch_id)
                batch_results.append(batch_result)
                
                # Update checkpoint
                if self.processed_count % self.checkpoint_frequency == 0:
                    await self._update_checkpoint(batch_id, batch[-1]['id'])
                
                # Brief pause to avoid overwhelming the system
                await asyncio.sleep(0.1)
            
            # Mark checkpoint as completed
            await self._complete_checkpoint(batch_id)
            
            # Generate final statistics
            stats = self._get_processing_stats(batch_id)
            logger.info(f"Historic news processing completed: {stats}")
            
            return stats
            
        except Exception as e:
            logger.error(f"Historic news processing failed: {e}")
            logger.error(traceback.format_exc())
            await self._update_checkpoint(batch_id, error=str(e))
            raise
    
    async def _get_news_records(self, start_date, end_date, limit, checkpoint_id):
        """Get news records to process based on criteria."""
        
        conditions = []
        params = []
        
        if start_date:
            conditions.append(f"published_utc >= ${len(params) + 1}")
            params.append(start_date)
            
        if end_date:
            conditions.append(f"published_utc <= ${len(params) + 1}")
            params.append(end_date)
        
        # If resuming from checkpoint, start after last processed record
        if checkpoint_id:
            checkpoint_info = await self._get_checkpoint_info(checkpoint_id)
            if checkpoint_info and checkpoint_info.get('last_processed_news_id'):
                conditions.append(f"id > ${len(params) + 1}")
                params.append(checkpoint_info['last_processed_news_id'])
        
        where_clause = " AND ".join(conditions) if conditions else ""
        if where_clause:
            where_clause = f" WHERE {where_clause}"
        
        limit_clause = f" LIMIT {limit}" if limit else ""
        
        query = f"""
            SELECT id, vendor_id, title, description, author, published_utc, 
                   tickers, insights, keywords, publisher_name
            FROM dev_news_polygon 
            {where_clause}
            ORDER BY published_utc ASC, id ASC
            {limit_clause}
        """
        
        async with self.db_pool.acquire() as conn:
            records = await conn.fetch(query, *params)
            return [dict(record) for record in records]
    
    async def _process_news_batch(self, batch: List[Dict], batch_id: str) -> Dict[str, Any]:
        """Process a batch of news records."""
        
        batch_start = time.time()
        batch_results = {
            'processed': 0,
            'successful': 0,
            'errors': 0,
            'signals_created': 0
        }
        
        async with self.db_pool.acquire() as conn:
            async with conn.transaction():
                
                for news_record in batch:
                    try:
                        # Extract enhanced signals from news
                        analysis = await self._extract_enhanced_signals(news_record)
                        
                        if analysis:
                            # Store enhanced analysis
                            await self._store_enhanced_analysis(conn, analysis)
                            
                            # Store individual trading signals
                            for signal in analysis.enhanced_signals:
                                await self._store_trading_signal(conn, signal)
                                batch_results['signals_created'] += 1
                            
                            batch_results['successful'] += 1
                        
                        batch_results['processed'] += 1
                        self.processed_count += 1
                        
                    except Exception as e:
                        logger.error(f"Failed to process news {news_record.get('id', 'unknown')}: {e}")
                        batch_results['errors'] += 1
                        self.error_count += 1
        
        batch_time = time.time() - batch_start
        batch_results['processing_time'] = batch_time
        
        # Update global counters
        self.success_count += batch_results['successful']
        
        logger.info(f"Batch completed: {batch_results}")
        return batch_results
    
    async def _extract_enhanced_signals(self, news_record: Dict) -> Optional[EnhancedNewsAnalysis]:
        """Extract enhanced trading signals from a news record using local LLMs."""
        
        start_time = time.time()
        news_id = news_record.get('vendor_id', news_record.get('id'))
        title = news_record.get('title', '')
        description = news_record.get('description', '')
        original_insights = news_record.get('insights', [])
        tickers = news_record.get('tickers', [])
        
        if not title and not description:
            return None
        
        try:
            enhanced_signals = []
            model_metrics = {}
            
            # Process each ticker mentioned in the news
            if isinstance(tickers, str):
                tickers = json.loads(tickers) if tickers.startswith('[') else [tickers]
            elif not isinstance(tickers, list):
                tickers = []
            
            # Extract signals using local sentiment analysis
            if self.local_client:
                sentiment_result = await self._analyze_sentiment_local(title + " " + description)
                model_metrics['sentiment_analysis'] = sentiment_result.get('metrics', {})
            else:
                sentiment_result = {'sentiment': 'neutral', 'confidence': 0.5}
            
            # Enhanced signal generation for each ticker
            for ticker in tickers[:10]:  # Limit to 10 tickers per article
                if not ticker or len(ticker) > 10:
                    continue
                    
                # Find original insight for this ticker
                original_insight = None
                if isinstance(original_insights, list):
                    for insight in original_insights:
                        if isinstance(insight, dict) and insight.get('ticker') == ticker:
                            original_insight = insight
                            break
                
                # Generate enhanced signal
                signal = await self._generate_trading_signal(
                    news_record, ticker, original_insight, sentiment_result
                )
                
                if signal:
                    enhanced_signals.append(signal)
            
            # Calculate market impact and volatility indicators
            market_impact = self._calculate_market_impact(news_record, enhanced_signals)
            volatility_indicator = self._calculate_volatility_indicator(news_record, enhanced_signals)
            
            # Extract entities and sector impact
            entities = await self._extract_entities(title, description)
            sector_impact = self._identify_sector_impact(news_record, enhanced_signals)
            
            processing_time = time.time() - start_time
            model_metrics['total_processing_time'] = processing_time
            
            return EnhancedNewsAnalysis(
                news_id=news_id,
                title=title,
                description=description,
                original_insights=original_insights,
                enhanced_signals=enhanced_signals,
                market_impact_score=market_impact,
                volatility_indicator=volatility_indicator,
                sector_impact=sector_impact,
                entity_mentions=entities,
                processing_time_seconds=processing_time,
                model_performance_metrics=model_metrics
            )
            
        except Exception as e:
            logger.error(f"Failed to extract signals from news {news_id}: {e}")
            return None
    
    async def _analyze_sentiment_local(self, text: str) -> Dict[str, Any]:
        """Analyze sentiment using local FinBERT model."""
        if not self.local_client:
            return {'sentiment': 'neutral', 'confidence': 0.5}
        
        try:
            # Use local sentiment analysis
            result = await self.local_client.analyze_sentiment(text)
            return {
                'sentiment': result.get('label', 'neutral').lower(),
                'confidence': result.get('score', 0.5),
                'metrics': {'model': 'finbert_local', 'processing_time': result.get('processing_time', 0)}
            }
        except Exception as e:
            logger.error(f"Local sentiment analysis failed: {e}")
            return {'sentiment': 'neutral', 'confidence': 0.5}
    
    async def _generate_trading_signal(self, news_record: Dict, ticker: str, 
                                     original_insight: Optional[Dict], 
                                     sentiment_result: Dict) -> Optional[TradingSignal]:
        """Generate an enhanced trading signal for a specific ticker."""
        
        try:
            # Base signal from sentiment
            sentiment = sentiment_result.get('sentiment', 'neutral')
            confidence = sentiment_result.get('confidence', 0.5)
            
            # Enhance with original insight if available
            if original_insight:
                original_sentiment = original_insight.get('sentiment', 'neutral')
                if original_sentiment in ['positive', 'negative']:
                    sentiment = original_sentiment
                    confidence = max(confidence, 0.7)  # Boost confidence if we have original insight
            
            # Convert sentiment to signal type
            signal_type = self._sentiment_to_signal_type(sentiment, confidence)
            
            # Determine impact timeframe based on news characteristics
            impact_timeframe = self._determine_impact_timeframe(news_record)
            
            # Extract key factors
            key_factors = self._extract_key_factors(news_record, original_insight)
            
            # Assess risk level
            risk_level = self._assess_risk_level(sentiment, confidence, news_record)
            
            # Estimate target price change
            target_price_change = self._estimate_price_change(sentiment, confidence, impact_timeframe)
            
            return TradingSignal(
                news_id=news_record.get('vendor_id', news_record.get('id')),
                ticker=ticker,
                signal_type=signal_type,
                confidence=confidence,
                sentiment=sentiment,
                sentiment_score=self._sentiment_to_score(sentiment, confidence),
                impact_timeframe=impact_timeframe,
                key_factors=key_factors,
                risk_level=risk_level,
                target_price_change=target_price_change,
                published_utc=news_record.get('published_utc'),
                processed_at=datetime.now(),
                model_version='local_finbert_v1.0'
            )
            
        except Exception as e:
            logger.error(f"Failed to generate trading signal for {ticker}: {e}")
            return None
    
    def _sentiment_to_signal_type(self, sentiment: str, confidence: float) -> str:
        """Convert sentiment to trading signal type."""
        if confidence < 0.6:
            return 'HOLD'
        elif sentiment == 'positive':
            return 'BUY' if confidence > 0.8 else 'WATCH'
        elif sentiment == 'negative':
            return 'SELL' if confidence > 0.8 else 'WATCH' 
        else:
            return 'HOLD'
    
    def _sentiment_to_score(self, sentiment: str, confidence: float) -> float:
        """Convert sentiment to numerical score (-1 to 1)."""
        base_score = {'positive': 1.0, 'negative': -1.0, 'neutral': 0.0}.get(sentiment, 0.0)
        return base_score * confidence
    
    def _determine_impact_timeframe(self, news_record: Dict) -> str:
        """Determine impact timeframe based on news characteristics."""
        title = news_record.get('title', '').lower()
        description = news_record.get('description', '').lower()
        
        # Immediate impact keywords
        if any(word in title for word in ['earnings', 'halt', 'crash', 'surge', 'breaking']):
            return 'immediate'
        
        # Long term impact keywords  
        if any(word in title for word in ['acquisition', 'merger', 'partnership', 'strategy']):
            return 'long_term'
        
        # Medium term by default
        return 'medium_term'
    
    def _extract_key_factors(self, news_record: Dict, original_insight: Optional[Dict]) -> List[str]:
        """Extract key factors driving the signal."""
        factors = []
        
        # From original insight
        if original_insight and original_insight.get('sentiment_reasoning'):
            factors.append(original_insight['sentiment_reasoning'][:100])
        
        # From keywords
        keywords = news_record.get('keywords', [])
        if isinstance(keywords, list):
            factors.extend(keywords[:3])
        
        # From title analysis
        title = news_record.get('title', '')
        if 'earnings' in title.lower():
            factors.append('earnings_report')
        if 'acquisition' in title.lower():
            factors.append('acquisition_news')
        if 'partnership' in title.lower():
            factors.append('strategic_partnership')
        
        return factors[:5]  # Limit to 5 key factors
    
    def _assess_risk_level(self, sentiment: str, confidence: float, news_record: Dict) -> str:
        """Assess risk level of the trading signal."""
        if confidence < 0.6:
            return 'high'
        elif sentiment == 'neutral' or confidence < 0.75:
            return 'medium'
        else:
            return 'low'
    
    def _estimate_price_change(self, sentiment: str, confidence: float, timeframe: str) -> Optional[float]:
        """Estimate expected price change percentage."""
        if sentiment == 'neutral' or confidence < 0.6:
            return None
        
        base_change = 0.05 if sentiment == 'positive' else -0.05  # 5% base change
        
        # Adjust by confidence
        change = base_change * confidence
        
        # Adjust by timeframe
        if timeframe == 'immediate':
            change *= 2.0
        elif timeframe == 'long_term':
            change *= 3.0
        
        return round(change, 3)
    
    def _calculate_market_impact(self, news_record: Dict, signals: List[TradingSignal]) -> float:
        """Calculate overall market impact score."""
        if not signals:
            return 0.0
        
        # Average confidence across all signals
        avg_confidence = sum(s.confidence for s in signals) / len(signals)
        
        # Boost for high-profile publishers
        publisher = news_record.get('publisher_name', '').lower()
        if any(pub in publisher for pub in ['reuters', 'bloomberg', 'wall street journal']):
            avg_confidence *= 1.2
        
        return min(1.0, avg_confidence)
    
    def _calculate_volatility_indicator(self, news_record: Dict, signals: List[TradingSignal]) -> float:
        """Calculate volatility indicator."""
        if not signals:
            return 0.0
        
        # Higher volatility for immediate impact signals
        immediate_signals = [s for s in signals if s.impact_timeframe == 'immediate']
        if immediate_signals:
            return min(1.0, len(immediate_signals) / len(signals) + 0.3)
        
        return 0.3  # Base volatility
    
    async def _extract_entities(self, title: str, description: str) -> Dict[str, List[str]]:
        """Extract entities from news text."""
        # Simple entity extraction for now
        entities = {
            'companies': [],
            'people': [],
            'events': []
        }
        
        text = (title + " " + description).lower()
        
        # Common financial events
        if 'earnings' in text:
            entities['events'].append('earnings_announcement')
        if 'acquisition' in text:
            entities['events'].append('acquisition')
        if 'merger' in text:
            entities['events'].append('merger')
        
        return entities
    
    def _identify_sector_impact(self, news_record: Dict, signals: List[TradingSignal]) -> List[str]:
        """Identify sectors impacted by the news."""
        sectors = []
        
        title_lower = news_record.get('title', '').lower()
        keywords = news_record.get('keywords', [])
        
        # Technology sector
        if any(word in title_lower for word in ['ai', 'artificial intelligence', 'tech', 'software']):
            sectors.append('technology')
        
        # Healthcare/Biotech
        if any(word in title_lower for word in ['drug', 'biotech', 'pharmaceutical', 'fda']):
            sectors.append('healthcare')
        
        # Energy
        if any(word in title_lower for word in ['oil', 'energy', 'renewable', 'solar']):
            sectors.append('energy')
        
        # From keywords
        if isinstance(keywords, list):
            for keyword in keywords:
                if isinstance(keyword, str):
                    keyword_lower = keyword.lower()
                    if 'fintech' in keyword_lower:
                        sectors.append('financial')
                    elif 'crypto' in keyword_lower:
                        sectors.append('cryptocurrency')
        
        return list(set(sectors))  # Remove duplicates
    
    async def _store_enhanced_analysis(self, conn, analysis: EnhancedNewsAnalysis):
        """Store enhanced news analysis in database."""
        await conn.execute("""
            INSERT INTO dev_enhanced_news_analysis 
            (news_id, title, description, original_insights, market_impact_score, 
             volatility_indicator, sector_impact, entity_mentions, processing_time_seconds, 
             model_performance_metrics)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            ON CONFLICT (news_id) DO UPDATE SET
                market_impact_score = EXCLUDED.market_impact_score,
                volatility_indicator = EXCLUDED.volatility_indicator,
                sector_impact = EXCLUDED.sector_impact,
                entity_mentions = EXCLUDED.entity_mentions,
                processing_time_seconds = EXCLUDED.processing_time_seconds,
                model_performance_metrics = EXCLUDED.model_performance_metrics,
                processed_at = NOW()
        """, analysis.news_id, analysis.title, analysis.description,
            json.dumps(analysis.original_insights), analysis.market_impact_score,
            analysis.volatility_indicator, json.dumps(analysis.sector_impact),
            json.dumps(analysis.entity_mentions), analysis.processing_time_seconds,
            json.dumps(analysis.model_performance_metrics))
    
    async def _store_trading_signal(self, conn, signal: TradingSignal):
        """Store trading signal in database."""
        await conn.execute("""
            INSERT INTO dev_trading_signals 
            (news_id, ticker, signal_type, confidence, sentiment, sentiment_score,
             impact_timeframe, key_factors, risk_level, target_price_change,
             published_utc, processed_at, model_version)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
            ON CONFLICT (news_id, ticker) DO UPDATE SET
                signal_type = EXCLUDED.signal_type,
                confidence = EXCLUDED.confidence,
                sentiment = EXCLUDED.sentiment,
                sentiment_score = EXCLUDED.sentiment_score,
                impact_timeframe = EXCLUDED.impact_timeframe,
                key_factors = EXCLUDED.key_factors,
                risk_level = EXCLUDED.risk_level,
                target_price_change = EXCLUDED.target_price_change,
                processed_at = EXCLUDED.processed_at,
                model_version = EXCLUDED.model_version
        """, signal.news_id, signal.ticker, signal.signal_type, signal.confidence,
            signal.sentiment, signal.sentiment_score, signal.impact_timeframe,
            json.dumps(signal.key_factors), signal.risk_level, signal.target_price_change,
            signal.published_utc, signal.processed_at, signal.model_version)
    
    async def _create_checkpoint(self, batch_id: str):
        """Create processing checkpoint."""
        async with self.db_pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO dev_news_processing_checkpoints (batch_id)
                VALUES ($1)
                ON CONFLICT (batch_id) DO NOTHING
            """, batch_id)
    
    async def _update_checkpoint(self, batch_id: str, last_processed_id: Optional[str] = None, error: Optional[str] = None):
        """Update processing checkpoint."""
        async with self.db_pool.acquire() as conn:
            if error:
                await conn.execute("""
                    UPDATE dev_news_processing_checkpoints 
                    SET processed_count = $2, success_count = $3, error_count = $4,
                        updated_at = NOW()
                    WHERE batch_id = $1
                """, batch_id, self.processed_count, self.success_count, self.error_count)
            else:
                await conn.execute("""
                    UPDATE dev_news_processing_checkpoints 
                    SET last_processed_news_id = $2, processed_count = $3, 
                        success_count = $4, error_count = $5, updated_at = NOW()
                    WHERE batch_id = $1
                """, batch_id, last_processed_id, self.processed_count, self.success_count, self.error_count)
    
    async def _complete_checkpoint(self, batch_id: str):
        """Mark checkpoint as completed."""
        async with self.db_pool.acquire() as conn:
            await conn.execute("""
                UPDATE dev_news_processing_checkpoints 
                SET completed_at = NOW(), updated_at = NOW()
                WHERE batch_id = $1
            """, batch_id)
    
    async def _get_checkpoint_info(self, checkpoint_id: str) -> Optional[Dict]:
        """Get checkpoint information."""
        async with self.db_pool.acquire() as conn:
            record = await conn.fetchrow("""
                SELECT * FROM dev_news_processing_checkpoints 
                WHERE batch_id = $1
            """, checkpoint_id)
            return dict(record) if record else None
    
    def _get_processing_stats(self, batch_id: str) -> Dict[str, Any]:
        """Get processing statistics."""
        total_time = time.time() - self.start_time if self.start_time else 0
        
        return {
            'batch_id': batch_id,
            'processed_count': self.processed_count,
            'success_count': self.success_count,
            'error_count': self.error_count,
            'success_rate': self.success_count / max(1, self.processed_count),
            'total_processing_time_seconds': total_time,
            'avg_processing_time_per_record': total_time / max(1, self.processed_count),
            'records_per_second': self.processed_count / max(1, total_time)
        }
    
    async def get_processing_status(self) -> Dict[str, Any]:
        """Get current processing status."""
        return {
            'service': 'historic_news_signal_extractor',
            'status': 'running' if self.start_time else 'idle',
            'stats': self._get_processing_stats('current'),
            'components': {
                'database': 'connected' if self.db_pool else 'disconnected',
                'local_llm': 'available' if self.local_client else 'unavailable',
                'hybrid_llm': 'available' if self.llm_client else 'unavailable'
            }
        }
    
    async def close(self):
        """Clean up resources."""
        if self.llm_client:
            await self.llm_client.close()
        if self.local_client:
            await self.local_client.close()
        if self.db_pool:
            await self.db_pool.close()


async def main():
    """Main entry point for running the historic news signal extractor."""
    
    logger = logging.getLogger(__name__)
    
    logger.info("🚀 Starting Historic News Signal Extraction Service")
    logger.info("=" * 60)
    
    extractor = HistoricNewsSignalExtractor()
    
    try:
        # Initialize service
        await extractor.initialize()
        
        # Process historic news data
        # Start with a small batch for testing
        results = await extractor.process_historic_news(
            start_date=datetime(2024, 1, 1),  # Start with 2024 data
            end_date=datetime(2025, 12, 31),
            limit=1000  # Process 1000 records for initial testing
        )
        
        logger.info("🎉 Historic news signal extraction completed successfully!")
        logger.info(f"Results: {results}")
        
    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt")
    except Exception as e:
        logger.error(f"Service error: {e}")
        logger.error(traceback.format_exc())
    finally:
        await extractor.close()
        logger.info("Service shutdown complete")


if __name__ == "__main__":
    asyncio.run(main())