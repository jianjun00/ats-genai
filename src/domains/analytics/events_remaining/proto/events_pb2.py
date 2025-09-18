"""
Generated Protocol Buffer code for ATS Events.
This is a Python implementation of the event schema.
"""

from enum import IntEnum
from dataclasses import dataclass, field
from typing import Optional, List
from datetime import datetime
import json
import uuid
import hashlib

class EventType(IntEnum):
    EVENT_TYPE_UNSPECIFIED = 0
    EVENT_TYPE_NEWS = 1
    EVENT_TYPE_EARNINGS = 2
    EVENT_TYPE_CORPORATE_ACTION = 3
    EVENT_TYPE_ECONOMIC_INDICATOR = 4
    EVENT_TYPE_ANALYST_RECOMMENDATION = 5
    EVENT_TYPE_REGULATORY_FILING = 6
    EVENT_TYPE_PRICE_GAP = 7
    EVENT_TYPE_SUPPORT_RESISTANCE_BREAK = 8
    EVENT_TYPE_VOLUME_ANOMALY = 9
    EVENT_TYPE_TECHNICAL_SIGNAL = 10
    EVENT_TYPE_RISK_ALERT = 11

class Priority(IntEnum):
    PRIORITY_UNSPECIFIED = 0
    PRIORITY_LOW = 1
    PRIORITY_MEDIUM = 2
    PRIORITY_HIGH = 3
    PRIORITY_CRITICAL = 4

class Classification(IntEnum):
    CLASSIFICATION_UNSPECIFIED = 0
    CLASSIFICATION_PUBLIC = 1
    CLASSIFICATION_INTERNAL = 2
    CLASSIFICATION_CONFIDENTIAL = 3
    CLASSIFICATION_RESTRICTED = 4

class EntityType(IntEnum):
    ENTITY_TYPE_UNSPECIFIED = 0
    ENTITY_TYPE_PERSON = 1
    ENTITY_TYPE_ORGANIZATION = 2
    ENTITY_TYPE_LOCATION = 3
    ENTITY_TYPE_INSTRUMENT = 4

class ImpactDirection(IntEnum):
    IMPACT_DIRECTION_UNSPECIFIED = 0
    IMPACT_DIRECTION_POSITIVE = 1
    IMPACT_DIRECTION_NEGATIVE = 2
    IMPACT_DIRECTION_NEUTRAL = 3

class ImpactMagnitude(IntEnum):
    IMPACT_MAGNITUDE_UNSPECIFIED = 0
    IMPACT_MAGNITUDE_LOW = 1
    IMPACT_MAGNITUDE_MEDIUM = 2
    IMPACT_MAGNITUDE_HIGH = 3

class TimeHorizon(IntEnum):
    TIME_HORIZON_UNSPECIFIED = 0
    TIME_HORIZON_IMMEDIATE = 1
    TIME_HORIZON_SHORT_TERM = 2
    TIME_HORIZON_LONG_TERM = 3

class SignalDirection(IntEnum):
    SIGNAL_DIRECTION_UNSPECIFIED = 0
    SIGNAL_DIRECTION_BULLISH = 1
    SIGNAL_DIRECTION_BEARISH = 2
    SIGNAL_DIRECTION_NEUTRAL = 3

class SignalType(IntEnum):
    SIGNAL_TYPE_UNSPECIFIED = 0
    SIGNAL_TYPE_BREAKOUT = 1
    SIGNAL_TYPE_BREAKDOWN = 2
    SIGNAL_TYPE_REVERSAL = 3
    SIGNAL_TYPE_CONTINUATION = 4
    SIGNAL_TYPE_DIVERGENCE = 5

@dataclass
class EventSubject:
    instrument_id: str = ""
    symbol: str = ""
    isin: str = ""
    cusip: str = ""
    exchange: str = ""
    asset_class: str = ""
    sector: str = ""
    industry: str = ""
    country: str = ""
    currency: str = ""

    def to_dict(self) -> dict:
        return {k: v for k, v in self.__dict__.items() if v}

@dataclass
class EventMetadata:
    priority: Priority = Priority.PRIORITY_MEDIUM
    classification: Classification = Classification.CLASSIFICATION_PUBLIC
    tags: List[str] = field(default_factory=list)
    processed_by: str = ""
    processing_time_ms: int = 0
    retry_count: int = 0
    checksum: str = ""

    def to_dict(self) -> dict:
        result = {}
        for k, v in self.__dict__.items():
            if isinstance(v, IntEnum):
                result[k] = v.name
            elif v or isinstance(v, (int, float)):  # Include numeric values even if 0
                result[k] = v
        return result

@dataclass
class AspectSentiment:
    aspect: str = ""
    sentiment: float = 0.0
    confidence: float = 0.0

@dataclass
class SentimentAnalysis:
    overall: float = 0.0
    confidence: float = 0.0
    aspects: List[AspectSentiment] = field(default_factory=list)

@dataclass
class Entity:
    type: EntityType = EntityType.ENTITY_TYPE_UNSPECIFIED
    name: str = ""
    relevance: float = 0.0
    identifier: str = ""

@dataclass
class MarketImpact:
    expected: ImpactDirection = ImpactDirection.IMPACT_DIRECTION_UNSPECIFIED
    magnitude: ImpactMagnitude = ImpactMagnitude.IMPACT_MAGNITUDE_UNSPECIFIED
    time_horizon: TimeHorizon = TimeHorizon.TIME_HORIZON_UNSPECIFIED

@dataclass
class NewsEventData:
    headline: str = ""
    summary: str = ""
    full_text: str = ""
    url: str = ""
    author: str = ""
    publisher: str = ""
    language: str = "en"
    sentiment: Optional[SentimentAnalysis] = None
    entities: List[Entity] = field(default_factory=list)
    categories: List[str] = field(default_factory=list)
    importance: float = 0.0
    market_impact: Optional[MarketImpact] = None

    def to_dict(self) -> dict:
        result = {k: v for k, v in self.__dict__.items() if v and k not in ['sentiment', 'market_impact']}
        if self.sentiment:
            result['sentiment'] = {
                'overall': self.sentiment.overall,
                'confidence': self.sentiment.confidence,
                'aspects': [{'aspect': a.aspect, 'sentiment': a.sentiment, 'confidence': a.confidence}
                           for a in self.sentiment.aspects]
            }
        if self.market_impact:
            result['market_impact'] = {
                'expected': self.market_impact.expected.name,
                'magnitude': self.market_impact.magnitude.name,
                'time_horizon': self.market_impact.time_horizon.name
            }
        return result

@dataclass
class EpsEstimate:
    actual: float = 0.0
    consensus: float = 0.0
    surprise: float = 0.0
    surprise_percent: float = 0.0

@dataclass
class RevenueEstimate:
    actual: float = 0.0
    consensus: float = 0.0
    surprise: float = 0.0
    surprise_percent: float = 0.0

@dataclass
class EarningsEstimates:
    eps: Optional[EpsEstimate] = None
    revenue: Optional[RevenueEstimate] = None

@dataclass
class EarningsEventData:
    report_type: str = ""
    year: int = 0
    quarter: int = 0
    estimates: Optional[EarningsEstimates] = None

    def to_dict(self) -> dict:
        result = {k: v for k, v in self.__dict__.items() if v and k != 'estimates'}
        if self.estimates:
            estimates_dict = {}
            if self.estimates.eps:
                estimates_dict['eps'] = self.estimates.eps.__dict__
            if self.estimates.revenue:
                estimates_dict['revenue'] = self.estimates.revenue.__dict__
            result['estimates'] = estimates_dict
        return result

@dataclass
class Signal:
    direction: SignalDirection = SignalDirection.SIGNAL_DIRECTION_UNSPECIFIED
    strength: float = 0.0
    confidence: float = 0.0

@dataclass
class PriceContext:
    current_price: float = 0.0
    signal_price: float = 0.0
    support_level: float = 0.0
    resistance_level: float = 0.0
    volume: float = 0.0
    average_volume: float = 0.0

@dataclass
class TechnicalSignalEventData:
    signal_type: SignalType = SignalType.SIGNAL_TYPE_UNSPECIFIED
    indicator: str = ""
    timeframe: str = ""
    signal: Optional[Signal] = None
    price_context: Optional[PriceContext] = None

    def to_dict(self) -> dict:
        result = {}
        result['signal_type'] = self.signal_type.name
        result['indicator'] = self.indicator
        result['timeframe'] = self.timeframe
        if self.signal:
            result['signal'] = {
                'direction': self.signal.direction.name,
                'strength': self.signal.strength,
                'confidence': self.signal.confidence
            }
        if self.price_context:
            result['price_context'] = self.price_context.__dict__
        return result

@dataclass
class GapFillData:
    fill_date: Optional[datetime] = None
    days_to_fill: int = 0
    fill_percentage: float = 0.0
    fill_type: str = ""  # full, partial, none

    def to_dict(self) -> dict:
        return {
            'fill_date': self.fill_date.isoformat() if self.fill_date else None,
            'days_to_fill': self.days_to_fill,
            'fill_percentage': self.fill_percentage,
            'fill_type': self.fill_type
        }

@dataclass
class GapEventData:
    gap_points: float = 0.0
    gap_percentage: float = 0.0
    gap_size_class: str = ""        # micro, small, medium, large, extreme
    direction: str = ""             # gap_up, gap_down
    prev_close: float = 0.0
    open_price: float = 0.0
    volume: int = 0
    avg_volume: float = 0.0
    volume_confirmed: bool = False
    significance_score: float = 0.0
    gap_context: str = ""           # earnings, news, market, continuation, reversal
    fill_data: Optional[GapFillData] = None

    def to_dict(self) -> dict:
        result = {
            'gap_points': self.gap_points,
            'gap_percentage': self.gap_percentage,
            'gap_size_class': self.gap_size_class,
            'direction': self.direction,
            'prev_close': self.prev_close,
            'open_price': self.open_price,
            'volume': self.volume,
            'avg_volume': self.avg_volume,
            'volume_confirmed': self.volume_confirmed,
            'significance_score': self.significance_score,
            'gap_context': self.gap_context
        }
        if self.fill_data:
            result['fill_data'] = self.fill_data.to_dict()
        return result

@dataclass
class Event:
    event_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    event_type: EventType = EventType.EVENT_TYPE_UNSPECIFIED
    event_version: str = "1.0.0"
    timestamp: Optional[datetime] = None
    time_zone: str = "UTC"
    valid_from: Optional[datetime] = None
    valid_to: Optional[datetime] = None
    source: str = ""
    source_id: str = ""
    ingestion_time: Optional[datetime] = None
    causation_id: str = ""
    correlation_id: str = ""
    parent_event_id: str = ""
    root_event_id: str = ""
    subject: EventSubject = field(default_factory=EventSubject)
    metadata: EventMetadata = field(default_factory=EventMetadata)
    confidence: float = 0.0
    reliability: str = ""

    # Event-specific data (oneof in proto)
    news_data: Optional[NewsEventData] = None
    earnings_data: Optional[EarningsEventData] = None
    technical_data: Optional[TechnicalSignalEventData] = None
    gap_data: Optional[GapEventData] = None

    def __post_init__(self):
        if not self.timestamp:
            self.timestamp = datetime.utcnow()
        if not self.ingestion_time:
            self.ingestion_time = datetime.utcnow()

        # Generate checksum if not provided
        if not self.metadata.checksum:
            content = f"{self.event_id}{self.event_type}{self.timestamp}"
            self.metadata.checksum = hashlib.md5(content.encode()).hexdigest()[:8]

    def SerializeToString(self) -> bytes:
        """Serialize event to JSON bytes (protobuf-like interface)"""
        return json.dumps(self.to_dict(), default=str).encode('utf-8')

    def ParseFromString(self, data: bytes):
        """Parse event from JSON bytes (protobuf-like interface)"""
        event_dict = json.loads(data.decode('utf-8'))
        self._from_dict(event_dict)

    def to_dict(self) -> dict:
        """Convert event to dictionary for JSON serialization"""
        result = {
            'event_id': self.event_id,
            'event_type': self.event_type.name,
            'event_version': self.event_version,
            'timestamp': self.timestamp.isoformat() if self.timestamp else None,
            'time_zone': self.time_zone,
            'source': self.source,
            'source_id': self.source_id,
            'ingestion_time': self.ingestion_time.isoformat() if self.ingestion_time else None,
            'subject': self.subject.to_dict(),
            'metadata': self.metadata.to_dict(),
            'confidence': self.confidence,
            'reliability': self.reliability
        }

        # Add optional fields
        for field in ['causation_id', 'correlation_id', 'parent_event_id', 'root_event_id']:
            value = getattr(self, field)
            if value:
                result[field] = value

        # Add event-specific data based on type
        if self.news_data:
            result['news_data'] = self.news_data.to_dict()
        elif self.earnings_data:
            result['earnings_data'] = self.earnings_data.to_dict()
        elif self.technical_data:
            result['technical_data'] = self.technical_data.to_dict()
        elif self.gap_data:
            result['gap_data'] = self.gap_data.to_dict()

        return result

    def _from_dict(self, data: dict):
        """Load event from dictionary"""
        self.event_id = data.get('event_id', str(uuid.uuid4()))
        self.event_type = EventType[data.get('event_type', 'EVENT_TYPE_UNSPECIFIED')]
        self.event_version = data.get('event_version', '1.0.0')

        if data.get('timestamp'):
            ts = data['timestamp']
            if isinstance(ts, str):
                # Handle ISO format
                ts = ts.replace('Z', '+00:00')
                if '.' not in ts and '+' not in ts:
                    ts += '+00:00'
                self.timestamp = datetime.fromisoformat(ts)

        if data.get('ingestion_time'):
            ts = data['ingestion_time']
            if isinstance(ts, str):
                ts = ts.replace('Z', '+00:00')
                if '.' not in ts and '+' not in ts:
                    ts += '+00:00'
                self.ingestion_time = datetime.fromisoformat(ts)

        self.time_zone = data.get('time_zone', 'UTC')
        self.source = data.get('source', '')
        self.source_id = data.get('source_id', '')

        # Load subject
        subject_data = data.get('subject', {})
        self.subject = EventSubject(**subject_data)

        # Load metadata
        metadata_data = data.get('metadata', {})
        if 'priority' in metadata_data:
            if isinstance(metadata_data['priority'], str):
                metadata_data['priority'] = Priority[metadata_data['priority']]
        if 'classification' in metadata_data:
            if isinstance(metadata_data['classification'], str):
                metadata_data['classification'] = Classification[metadata_data['classification']]
        self.metadata = EventMetadata(**{k: v for k, v in metadata_data.items()
                                       if k in ['priority', 'classification', 'tags', 'processed_by',
                                               'processing_time_ms', 'retry_count', 'checksum']})

        self.confidence = data.get('confidence', 0.0)
        self.reliability = data.get('reliability', '')

        # Load event-specific data
        if 'news_data' in data and self.event_type == EventType.EVENT_TYPE_NEWS:
            news_dict = data['news_data']
            self.news_data = NewsEventData(
                headline=news_dict.get('headline', ''),
                summary=news_dict.get('summary', ''),
                publisher=news_dict.get('publisher', ''),
                url=news_dict.get('url', ''),
                importance=news_dict.get('importance', 0.0)
            )

            # Load sentiment if present
            if 'sentiment' in news_dict:
                sent_data = news_dict['sentiment']
                self.news_data.sentiment = SentimentAnalysis(
                    overall=sent_data.get('overall', 0.0),
                    confidence=sent_data.get('confidence', 0.0)
                )

def MessageToDict(event: Event, preserving_proto_field_name: bool = False) -> dict:
    """Convert protobuf-like message to dict (compatibility function)"""
    return event.to_dict()

def create_news_event(headline: str, symbol: str, sentiment: float = 0.0,
                     publisher: str = "unknown", url: str = "") -> Event:
    """Factory function to create news events"""
    event = Event()
    event.event_type = EventType.EVENT_TYPE_NEWS
    event.source = "polygon"

    # Subject
    event.subject.symbol = symbol
    event.subject.exchange = "NASDAQ"  # Could be determined from symbol lookup

    # News-specific data
    event.news_data = NewsEventData(
        headline=headline,
        publisher=publisher,
        url=url,
        importance=abs(sentiment) if sentiment else 0.5
    )

    if sentiment != 0.0:
        event.news_data.sentiment = SentimentAnalysis(
            overall=sentiment,
            confidence=0.8
        )

    # Metadata
    event.metadata.priority = Priority.PRIORITY_HIGH if abs(sentiment) > 0.5 else Priority.PRIORITY_MEDIUM
    event.metadata.classification = Classification.CLASSIFICATION_PUBLIC
    event.metadata.tags = ["automated", "sentiment-analyzed"]

    return event

def create_earnings_event(symbol: str, eps_actual: float, eps_consensus: float,
                         year: int, quarter: int) -> Event:
    """Factory function to create earnings events"""
    event = Event()
    event.event_type = EventType.EVENT_TYPE_EARNINGS
    event.source = "polygon"

    # Subject
    event.subject.symbol = symbol

    # Earnings-specific data
    eps_estimate = EpsEstimate(
        actual=eps_actual,
        consensus=eps_consensus,
        surprise=eps_actual - eps_consensus,
        surprise_percent=((eps_actual - eps_consensus) / eps_consensus * 100) if eps_consensus != 0 else 0
    )

    event.earnings_data = EarningsEventData(
        report_type="official",
        year=year,
        quarter=quarter,
        estimates=EarningsEstimates(eps=eps_estimate)
    )

    # Set priority based on surprise magnitude
    surprise_pct = abs(eps_estimate.surprise_percent)
    event.metadata.priority = (Priority.PRIORITY_CRITICAL if surprise_pct > 20
                              else Priority.PRIORITY_HIGH if surprise_pct > 10
                              else Priority.PRIORITY_MEDIUM)

    event.metadata.tags = ["earnings", "automated"]

    return event

def create_technical_signal_event(symbol: str, signal_type: SignalType,
                                 direction: SignalDirection, strength: float,
                                 current_price: float, indicator: str = "RSI") -> Event:
    """Factory function to create technical signal events"""
    event = Event()
    event.event_type = EventType.EVENT_TYPE_TECHNICAL_SIGNAL
    event.source = "ats-internal"

    # Subject
    event.subject.symbol = symbol

    # Technical signal data
    signal = Signal(
        direction=direction,
        strength=strength,
        confidence=min(strength + 0.2, 1.0)  # Confidence slightly higher than strength
    )

    price_context = PriceContext(
        current_price=current_price,
        signal_price=current_price
    )

    event.technical_data = TechnicalSignalEventData(
        signal_type=signal_type,
        indicator=indicator,
        timeframe="1h",
        signal=signal,
        price_context=price_context
    )

    # Set priority based on signal strength
    event.metadata.priority = (Priority.PRIORITY_HIGH if strength > 0.7
                              else Priority.PRIORITY_MEDIUM)
    event.metadata.tags = ["technical-analysis", indicator.lower()]

    return event

def create_gap_event(symbol: str, gap_points: float, gap_percentage: float,
                    direction: str, prev_close: float, open_price: float,
                    volume: int, significance: float, gap_context: str = "market",
                    fill_date: Optional[datetime] = None, days_to_fill: int = 0,
                    fill_percentage: float = 0.0, fill_type: str = "") -> Event:
    """Factory function to create price gap events"""
    event = Event()
    event.event_type = EventType.EVENT_TYPE_PRICE_GAP
    event.source = "ats-internal"

    # Subject
    event.subject.symbol = symbol

    # Classify gap size
    gap_size = abs(gap_percentage)
    if gap_size >= 5.0:
        gap_size_class = "extreme"
    elif gap_size >= 2.5:
        gap_size_class = "large"
    elif gap_size >= 1.0:
        gap_size_class = "medium"
    elif gap_size >= 0.5:
        gap_size_class = "small"
    else:
        gap_size_class = "micro"

    # Gap fill data if provided
    fill_data = None
    if fill_date:
        fill_data = GapFillData(
            fill_date=fill_date,
            days_to_fill=days_to_fill,
            fill_percentage=fill_percentage,
            fill_type=fill_type
        )

    # Gap-specific data
    event.gap_data = GapEventData(
        gap_points=gap_points,
        gap_percentage=gap_percentage,
        gap_size_class=gap_size_class,
        direction=direction,
        prev_close=prev_close,
        open_price=open_price,
        volume=volume,
        avg_volume=volume,  # Will be calculated properly in backfill
        volume_confirmed=False,  # Will be calculated properly
        significance_score=significance,
        gap_context=gap_context,
        fill_data=fill_data
    )

    # Set priority based on gap size and significance
    if gap_size_class == "extreme" or significance > 10.0:
        event.metadata.priority = Priority.PRIORITY_CRITICAL
    elif gap_size_class == "large" or significance > 5.0:
        event.metadata.priority = Priority.PRIORITY_HIGH
    elif gap_size_class == "medium" or significance > 2.0:
        event.metadata.priority = Priority.PRIORITY_MEDIUM
    else:
        event.metadata.priority = Priority.PRIORITY_LOW

    event.metadata.classification = Classification.CLASSIFICATION_INTERNAL
    event.metadata.tags = ["gap-detection", direction, gap_size_class, gap_context]

    return event