#!/usr/bin/env python3
"""
Economic Events Classification System
Analyzes news articles to identify and classify economic events with market impact scoring.
Part of the Multi-Modal News Prediction System.
"""

import asyncio
import logging
import re
import json
from datetime import datetime
from typing import List, Dict, Any, Optional, Tuple
from dataclasses import dataclass
from enum import Enum

import asyncpg

logger = logging.getLogger(__name__)

class EventCategory(Enum):
    """Economic event categories"""
    EARNINGS = "earnings"
    FEDERAL_RESERVE = "fed"
    EMPLOYMENT = "employment"
    INFLATION = "inflation"
    GDP_GROWTH = "growth"
    CORPORATE = "corporate"
    MACRO = "macro"

class EventSeverity(Enum):
    """Event severity levels (1-10 scale)"""
    LOW = 1
    MEDIUM_LOW = 3
    MEDIUM = 5
    MEDIUM_HIGH = 7
    HIGH = 9
    CRITICAL = 10

@dataclass
class EconomicEvent:
    """Structured economic event data"""
    event_type: str
    event_subtype: str
    event_category: EventCategory
    severity: EventSeverity
    confidence_score: float
    affected_symbols: List[str]
    affected_sectors: List[str]
    affected_regions: List[str]
    event_date: datetime
    announcement_date: Optional[datetime]
    predicted_impact_score: float
    title: str
    description: str
    source_url: str
    data_vendor: str
    raw_data: Dict[str, Any]

class EconomicEventsClassifier:
    """
    Advanced economic events classification system
    Uses pattern matching, keyword analysis, and ML-based classification
    """

    # Federal Reserve event patterns
    FED_PATTERNS = {
        'rate_decision': [
            r'federal reserve.*interest rate',
            r'fed.*rate decision',
            r'fomc.*meeting',
            r'federal open market committee',
            r'fed chair.*powell',
            r'central bank.*rate',
            r'monetary policy'
        ],
        'rate_hike': [
            r'rate hike',
            r'raises.*interest rate',
            r'increases.*fed funds',
            r'tightening.*monetary policy'
        ],
        'rate_cut': [
            r'rate cut',
            r'lowers.*interest rate',
            r'cuts.*fed funds',
            r'easing.*monetary policy'
        ],
        'qe_announcement': [
            r'quantitative easing',
            r'bond buying',
            r'asset purchases',
            r'balance sheet'
        ]
    }

    # Employment event patterns
    EMPLOYMENT_PATTERNS = {
        'jobs_report': [
            r'jobs report',
            r'employment.*data',
            r'unemployment rate',
            r'nonfarm payrolls',
            r'jobless claims',
            r'labor market'
        ],
        'unemployment': [
            r'unemployment.*rate',
            r'jobless.*rate',
            r'unemployment.*rises',
            r'unemployment.*falls'
        ]
    }

    # Inflation event patterns
    INFLATION_PATTERNS = {
        'cpi_release': [
            r'consumer price index',
            r'cpi.*data',
            r'inflation.*rate',
            r'core.*inflation'
        ],
        'ppi_release': [
            r'producer price index',
            r'ppi.*data',
            r'wholesale.*inflation'
        ]
    }

    # GDP and growth patterns
    GDP_PATTERNS = {
        'gdp_release': [
            r'gdp.*growth',
            r'gross domestic product',
            r'economic growth',
            r'quarterly.*gdp'
        ],
        'recession': [
            r'recession',
            r'economic contraction',
            r'negative growth'
        ]
    }

    # Corporate event patterns
    CORPORATE_PATTERNS = {
        'earnings_announcement': [
            r'earnings.*report',
            r'quarterly.*results',
            r'earnings.*call',
            r'eps.*beats',
            r'eps.*misses',
            r'revenue.*beats',
            r'revenue.*misses'
        ],
        'merger_acquisition': [
            r'merger.*announced',
            r'acquisition.*deal',
            r'takeover.*bid',
            r'buyout.*offer'
        ],
        'dividend': [
            r'dividend.*announcement',
            r'dividend.*increase',
            r'dividend.*cut',
            r'special.*dividend'
        ],
        'guidance': [
            r'guidance.*raised',
            r'guidance.*lowered',
            r'outlook.*improved',
            r'forecast.*updated'
        ]
    }

    # Sector mappings
    SECTOR_KEYWORDS = {
        'technology': ['tech', 'software', 'semiconductor', 'cloud', 'ai', 'artificial intelligence'],
        'financial': ['bank', 'finance', 'insurance', 'fintech', 'credit'],
        'healthcare': ['health', 'pharma', 'biotech', 'medical', 'drug'],
        'energy': ['oil', 'gas', 'energy', 'renewable', 'petroleum'],
        'consumer': ['retail', 'consumer', 'ecommerce', 'shopping'],
        'industrial': ['manufacturing', 'aerospace', 'defense', 'construction'],
        'materials': ['mining', 'metals', 'commodities', 'steel'],
        'utilities': ['utility', 'electric', 'power', 'water'],
        'real_estate': ['real estate', 'reit', 'property', 'housing'],
        'communications': ['telecom', 'media', 'communications', 'wireless']
    }

    def __init__(self):
        """Initialize the economic events classifier"""
        # Compile regex patterns for performance
        self._compiled_patterns = {}
        self._compile_patterns()

    def _compile_patterns(self):
        """Compile all regex patterns for better performance"""
        pattern_groups = [
            ('fed', self.FED_PATTERNS),
            ('employment', self.EMPLOYMENT_PATTERNS),
            ('inflation', self.INFLATION_PATTERNS),
            ('gdp', self.GDP_PATTERNS),
            ('corporate', self.CORPORATE_PATTERNS)
        ]

        for group_name, patterns in pattern_groups:
            self._compiled_patterns[group_name] = {}
            for event_type, pattern_list in patterns.items():
                compiled_list = []
                for pattern in pattern_list:
                    compiled_list.append(re.compile(pattern, re.IGNORECASE))
                self._compiled_patterns[group_name][event_type] = compiled_list

    def classify_news_article(self, title: str, description: str, content: str = "",
                            symbols: List[str] = None, published_date: datetime = None) -> Optional[EconomicEvent]:
        """
        Classify a news article as an economic event

        Args:
            title: Article title
            description: Article description/summary
            content: Full article content (optional)
            symbols: Associated stock symbols
            published_date: Article publication date

        Returns:
            EconomicEvent if classification successful, None otherwise
        """
        symbols = symbols or []
        published_date = published_date or datetime.now()

        # Combine all text for analysis
        full_text = f"{title} {description} {content}".lower()

        # Try to classify the event
        classification_result = self._analyze_text_patterns(full_text, title)

        if not classification_result:
            return None

        event_type, event_subtype, category, confidence = classification_result

        # Calculate severity based on keywords and context
        severity = self._calculate_severity(full_text, event_type, symbols)

        # Identify affected sectors
        affected_sectors = self._identify_affected_sectors(full_text, symbols)

        # Calculate predicted market impact
        impact_score = self._calculate_impact_score(event_type, severity, len(symbols), affected_sectors)

        return EconomicEvent(
            event_type=event_type,
            event_subtype=event_subtype,
            event_category=category,
            severity=severity,
            confidence_score=confidence,
            affected_symbols=symbols,
            affected_sectors=affected_sectors,
            affected_regions=['US'],  # Default to US, could be enhanced
            event_date=published_date,
            announcement_date=published_date,
            predicted_impact_score=impact_score,
            title=title,
            description=description,
            source_url="",  # Will be filled by calling code
            data_vendor="news_classifier",
            raw_data={
                'title': title,
                'description': description,
                'content': content,
                'symbols': symbols
            }
        )

    def _analyze_text_patterns(self, text: str, title: str) -> Optional[Tuple[str, str, EventCategory, float]]:
        """Analyze text using compiled regex patterns"""

        # Federal Reserve events (highest priority)
        for event_type, patterns in self._compiled_patterns['fed'].items():
            if any(pattern.search(text) for pattern in patterns):
                confidence = 0.9 if any(pattern.search(title) for pattern in patterns) else 0.7
                return event_type, event_type, EventCategory.FEDERAL_RESERVE, confidence

        # Employment events
        for event_type, patterns in self._compiled_patterns['employment'].items():
            if any(pattern.search(text) for pattern in patterns):
                confidence = 0.85 if any(pattern.search(title) for pattern in patterns) else 0.6
                return event_type, event_type, EventCategory.EMPLOYMENT, confidence

        # Inflation events
        for event_type, patterns in self._compiled_patterns['inflation'].items():
            if any(pattern.search(text) for pattern in patterns):
                confidence = 0.8 if any(pattern.search(title) for pattern in patterns) else 0.6
                return event_type, event_type, EventCategory.INFLATION, confidence

        # GDP events
        for event_type, patterns in self._compiled_patterns['gdp'].items():
            if any(pattern.search(text) for pattern in patterns):
                confidence = 0.75 if any(pattern.search(title) for pattern in patterns) else 0.5
                return event_type, event_type, EventCategory.GDP_GROWTH, confidence

        # Corporate events
        for event_type, patterns in self._compiled_patterns['corporate'].items():
            if any(pattern.search(text) for pattern in patterns):
                confidence = 0.7 if any(pattern.search(title) for pattern in patterns) else 0.4
                return event_type, event_type, EventCategory.CORPORATE, confidence

        return None

    def _calculate_severity(self, text: str, event_type: str, symbols: List[str]) -> EventSeverity:
        """Calculate event severity based on context and keywords"""

        # Base severity by event type
        base_severity = {
            'rate_hike': EventSeverity.HIGH,
            'rate_cut': EventSeverity.HIGH,
            'rate_decision': EventSeverity.MEDIUM_HIGH,
            'qe_announcement': EventSeverity.HIGH,
            'jobs_report': EventSeverity.MEDIUM_HIGH,
            'unemployment': EventSeverity.MEDIUM,
            'cpi_release': EventSeverity.MEDIUM_HIGH,
            'ppi_release': EventSeverity.MEDIUM,
            'gdp_release': EventSeverity.MEDIUM_HIGH,
            'recession': EventSeverity.CRITICAL,
            'earnings_announcement': EventSeverity.MEDIUM,
            'merger_acquisition': EventSeverity.MEDIUM_HIGH,
            'dividend': EventSeverity.LOW,
            'guidance': EventSeverity.MEDIUM
        }.get(event_type, EventSeverity.MEDIUM)

        # Adjust severity based on keywords
        high_impact_keywords = ['surprise', 'unexpected', 'shock', 'dramatic', 'significant', 'major']
        if any(keyword in text for keyword in high_impact_keywords):
            if base_severity.value < 8:
                base_severity = EventSeverity(min(10, base_severity.value + 2))

        # Adjust for number of affected symbols
        if len(symbols) > 10:  # Market-wide impact
            base_severity = EventSeverity(min(10, base_severity.value + 1))

        return base_severity

    def _identify_affected_sectors(self, text: str, symbols: List[str]) -> List[str]:
        """Identify sectors affected by the event"""
        affected_sectors = set()

        # Check for sector keywords in text
        for sector, keywords in self.SECTOR_KEYWORDS.items():
            if any(keyword in text for keyword in keywords):
                affected_sectors.add(sector)

        # If specific symbols are mentioned, add their sectors
        # This would require a symbol-to-sector mapping (could be enhanced)
        if symbols:
            # For now, mark as general market impact
            affected_sectors.add('market')

        return list(affected_sectors)

    def _calculate_impact_score(self, event_type: str, severity: EventSeverity,
                              num_symbols: int, affected_sectors: List[str]) -> float:
        """Calculate predicted market impact score (-1 to 1)"""

        # Base impact by event type
        base_impact = {
            'rate_hike': -0.3,      # Generally negative for stocks
            'rate_cut': 0.4,        # Generally positive for stocks
            'rate_decision': 0.0,   # Neutral until specifics known
            'qe_announcement': 0.5, # Very positive for stocks
            'jobs_report': 0.1,     # Slightly positive if good
            'unemployment': -0.2,   # Negative if unemployment rises
            'cpi_release': -0.1,    # Slight negative (inflation concerns)
            'gdp_release': 0.2,     # Positive if growth
            'recession': -0.8,      # Very negative
            'earnings_announcement': 0.0,  # Neutral (depends on results)
            'merger_acquisition': 0.3,     # Generally positive
            'dividend': 0.1,        # Slightly positive
            'guidance': 0.0         # Neutral (depends on direction)
        }.get(event_type, 0.0)

        # Adjust for severity
        severity_multiplier = severity.value / 5.0  # Convert 1-10 to 0.2-2.0
        adjusted_impact = base_impact * severity_multiplier

        # Adjust for market breadth
        if num_symbols > 10:
            adjusted_impact *= 1.2  # Broader impact

        # Ensure within bounds
        return max(-1.0, min(1.0, adjusted_impact))

class EconomicEventsProcessor:
    """Process news articles and extract economic events for database storage"""

    def __init__(self, db_config: Dict[str, Any]):
        self.db_config = db_config
        self.classifier = EconomicEventsClassifier()
        self.db_pool = None

    async def __aenter__(self):
        """Initialize database connection pool"""
        self.db_pool = await asyncpg.create_pool(**self.db_config)
        await self._ensure_tables_exist()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Cleanup database connection pool"""
        if self.db_pool:
            await self.db_pool.close()

    async def _ensure_tables_exist(self):
        """Ensure economic events tables exist"""
        async with self.db_pool.acquire() as conn:
            # Create economic events table if it doesn't exist
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS dev_economic_events (
                    id BIGSERIAL PRIMARY KEY,
                    event_type VARCHAR(50) NOT NULL,
                    event_subtype VARCHAR(50),
                    event_category VARCHAR(30) NOT NULL,
                    severity INTEGER NOT NULL CHECK (severity BETWEEN 1 AND 10),
                    confidence_score DECIMAL(5,3) NOT NULL CHECK (confidence_score BETWEEN 0 AND 1),

                    -- Affected entities
                    affected_symbols TEXT[] DEFAULT '{}',
                    affected_sectors TEXT[] DEFAULT '{}',
                    affected_regions TEXT[] DEFAULT '{}',

                    -- Timing
                    event_date TIMESTAMP WITH TIME ZONE NOT NULL,
                    announcement_date TIMESTAMP WITH TIME ZONE,
                    market_open_date TIMESTAMP WITH TIME ZONE,

                    -- Impact analysis
                    predicted_impact_score DECIMAL(7,4),
                    actual_impact_score DECIMAL(7,4),
                    impact_duration_days INTEGER,

                    -- Event details
                    title TEXT NOT NULL,
                    description TEXT,
                    source_url TEXT,
                    data JSONB NOT NULL,

                    -- Metadata
                    data_vendor VARCHAR(30) NOT NULL,
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
                )
            """)

            # Create news-events mapping table
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS dev_news_economic_events (
                    id BIGSERIAL PRIMARY KEY,
                    news_id BIGINT NOT NULL,
                    news_source VARCHAR(20) NOT NULL,
                    event_id BIGINT NOT NULL REFERENCES dev_economic_events(id),
                    relevance_score DECIMAL(5,3) NOT NULL CHECK (relevance_score BETWEEN 0 AND 1),
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                    UNIQUE(news_id, news_source, event_id)
                )
            """)

            # Create indexes
            await conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_economic_events_event_date
                ON dev_economic_events(event_date DESC)
            """)
            await conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_economic_events_category_severity
                ON dev_economic_events(event_category, severity DESC)
            """)

    async def process_news_articles(self, source_table: str, limit: Optional[int] = None) -> int:
        """
        Process news articles from a source table and extract economic events

        Args:
            source_table: Name of news table ('news_polygon', 'news_tiingo', etc.)
            limit: Maximum number of articles to process

        Returns:
            Number of economic events created
        """
        events_created = 0

        async with self.db_pool.acquire() as conn:
            # Get unprocessed news articles
            query = f"""
                SELECT id, title, description, tickers, published_utc as published_date
                FROM {source_table}
                WHERE id NOT IN (
                    SELECT news_id
                    FROM dev_news_economic_events
                    WHERE news_source = $1
                )
                ORDER BY published_utc DESC
            """

            if limit:
                query += f" LIMIT {limit}"

            # Extract source name from table name
            source_name = source_table.replace('news_', '')

            articles = await conn.fetch(query, source_name)
            logger.info(f"Processing {len(articles)} articles from {source_table}")

            for article in articles:
                try:
                    # Classify the article
                    event = self.classifier.classify_news_article(
                        title=article['title'] or '',
                        description=article['description'] or '',
                        symbols=article['tickers'] or [],
                        published_date=article['published_date']
                    )

                    if event and event.confidence_score >= 0.5:  # Minimum confidence threshold
                        # Insert economic event
                        event_id = await conn.fetchval("""
                            INSERT INTO dev_economic_events (
                                event_type, event_subtype, event_category, severity, confidence_score,
                                affected_symbols, affected_sectors, affected_regions,
                                event_date, announcement_date, predicted_impact_score,
                                title, description, data, data_vendor
                            )
                            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
                            RETURNING id
                        """,
                        event.event_type,
                        event.event_subtype,
                        event.event_category.value,
                        event.severity.value,
                        event.confidence_score,
                        event.affected_symbols,
                        event.affected_sectors,
                        event.affected_regions,
                        event.event_date,
                        event.announcement_date,
                        event.predicted_impact_score,
                        event.title,
                        event.description,
                        json.dumps(event.raw_data),
                        event.data_vendor
                        )

                        # Create news-event mapping
                        await conn.execute("""
                            INSERT INTO dev_news_economic_events (
                                news_id, news_source, event_id, relevance_score
                            )
                            VALUES ($1, $2, $3, $4)
                            ON CONFLICT (news_id, news_source, event_id) DO NOTHING
                        """, article['id'], source_name, event_id, event.confidence_score)

                        events_created += 1

                        if events_created % 100 == 0:
                            logger.info(f"Created {events_created} economic events...")

                except Exception as e:
                    logger.warning(f"Error processing article {article['id']}: {e}")
                    continue

        logger.info(f"✅ Created {events_created} economic events from {source_table}")
        return events_created

    async def get_event_summary(self) -> Dict[str, Any]:
        """Get summary statistics of economic events"""
        async with self.db_pool.acquire() as conn:
            summary = {}

            # Total events
            summary['total_events'] = await conn.fetchval(
                "SELECT COUNT(*) FROM dev_economic_events"
            )

            # Events by category
            category_counts = await conn.fetch("""
                SELECT event_category, COUNT(*) as count
                FROM dev_economic_events
                GROUP BY event_category
                ORDER BY count DESC
            """)
            summary['by_category'] = dict(category_counts)

            # Recent high-impact events
            recent_events = await conn.fetch("""
                SELECT title, event_category, severity, predicted_impact_score, event_date
                FROM dev_economic_events
                WHERE event_date >= CURRENT_DATE - INTERVAL '30 days'
                    AND severity >= 7
                ORDER BY event_date DESC, severity DESC
                LIMIT 10
            """)
            summary['recent_high_impact'] = [dict(row) for row in recent_events]

            return summary


async def main():
    """Test the economic events classification system"""
    import os

    # Database configuration
    db_config = {
        'host': os.getenv("DB_HOST", "localhost"),
        'port': int(os.getenv("DB_PORT", "5433")),
        'user': os.getenv("DB_USER", "postgres"),
        'password': os.getenv("DB_PASSWORD", "postgres"),
        'database': os.getenv("DB_NAME", "dev_db")
    }

    async with EconomicEventsProcessor(db_config) as processor:
        # Process news articles from both sources
        polygon_events = await processor.process_news_articles('news_polygon', limit=1000)
        tiingo_events = await processor.process_news_articles('news_tiingo', limit=1000)

        # Get summary
        summary = await processor.get_event_summary()

        print(f"📊 Economic Events Processing Complete:")
        print(f"   Polygon Events: {polygon_events}")
        print(f"   Tiingo Events: {tiingo_events}")
        print(f"   Total Events: {summary['total_events']}")
        print(f"   By Category: {summary['by_category']}")


if __name__ == "__main__":
    asyncio.run(main())