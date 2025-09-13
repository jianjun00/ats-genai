"""
Real-time Market Data Processing Service Interface

High-performance market data ingestion, processing, and distribution service.
"""

from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Dict, List, Optional, Any, AsyncIterator, Callable
from dataclasses import dataclass
from enum import Enum


class MarketDataType(Enum):
    """Types of market data."""
    TRADE = "trade"
    QUOTE = "quote"
    LEVEL2 = "level2"
    ORDER_BOOK = "order_book"
    AGGREGATED_TRADE = "aggregated_trade"
    MINUTE_BAR = "minute_bar"
    NEWS = "news"
    CORPORATE_ACTION = "corporate_action"


class DataQuality(Enum):
    """Data quality levels."""
    EXCELLENT = "excellent"
    GOOD = "good"
    FAIR = "fair"
    POOR = "poor"
    INVALID = "invalid"


class ProcessingStatus(Enum):
    """Data processing status."""
    RECEIVED = "received"
    VALIDATED = "validated"
    PROCESSED = "processed"
    ENRICHED = "enriched"
    DISTRIBUTED = "distributed"
    FAILED = "failed"


@dataclass
class MarketDataMessage:
    """Base market data message structure."""
    message_id: str
    symbol: str
    data_type: MarketDataType
    timestamp: datetime
    source: str
    sequence_number: int
    quality_score: float
    raw_data: Dict[str, Any]
    processed_data: Optional[Dict[str, Any]] = None
    processing_status: ProcessingStatus = ProcessingStatus.RECEIVED
    processing_latency_ms: Optional[float] = None


@dataclass
class TradeMessage:
    """Trade execution message."""
    message_id: str
    symbol: str
    data_type: MarketDataType
    timestamp: datetime
    source: str
    sequence_number: int
    quality_score: float
    raw_data: Dict[str, Any]
    price: Decimal
    size: Decimal
    trade_id: str
    conditions: List[str]
    exchange: str
    is_regular_hours: bool
    processed_data: Optional[Dict[str, Any]] = None
    processing_status: ProcessingStatus = ProcessingStatus.RECEIVED
    processing_latency_ms: Optional[float] = None


@dataclass
class QuoteMessage:
    """Bid/ask quote message."""
    message_id: str
    symbol: str
    data_type: MarketDataType
    timestamp: datetime
    source: str
    sequence_number: int
    quality_score: float
    raw_data: Dict[str, Any]
    bid_price: Decimal
    bid_size: Decimal
    ask_price: Decimal
    ask_size: Decimal
    bid_exchange: str
    ask_exchange: str
    spread: Decimal
    processed_data: Optional[Dict[str, Any]] = None
    processing_status: ProcessingStatus = ProcessingStatus.RECEIVED
    processing_latency_ms: Optional[float] = None


@dataclass
class Level2Message:
    """Level 2 order book data."""
    message_id: str
    symbol: str
    data_type: MarketDataType
    timestamp: datetime
    source: str
    sequence_number: int
    quality_score: float
    raw_data: Dict[str, Any]
    bids: List[Dict[str, Decimal]]  # [{"price": Decimal, "size": Decimal, "orders": int}]
    asks: List[Dict[str, Decimal]]
    total_bid_volume: Decimal
    total_ask_volume: Decimal
    order_book_depth: int
    processed_data: Optional[Dict[str, Any]] = None
    processing_status: ProcessingStatus = ProcessingStatus.RECEIVED
    processing_latency_ms: Optional[float] = None


@dataclass
class MinuteBar:
    """Aggregated minute bar data."""
    symbol: str
    timestamp: datetime
    open_price: Decimal
    high_price: Decimal
    low_price: Decimal
    close_price: Decimal
    volume: Decimal
    vwap: Decimal
    trade_count: int
    quality_score: float


@dataclass
class DataSubscription:
    """Market data subscription configuration."""
    subscription_id: str
    symbols: List[str]
    data_types: List[MarketDataType]
    filters: Dict[str, Any]
    callback: Optional[Callable]
    is_active: bool
    created_at: datetime
    last_message_at: Optional[datetime]
    message_count: int


@dataclass
class ProcessingMetrics:
    """Real-time processing performance metrics."""
    messages_per_second: float
    average_latency_ms: float
    p95_latency_ms: float
    p99_latency_ms: float
    error_rate: float
    queue_depth: int
    memory_usage_mb: float
    cpu_usage_percent: float
    timestamp: datetime


@dataclass
class DataValidationRule:
    """Data validation rule configuration."""
    rule_id: str
    rule_name: str
    data_type: MarketDataType
    validation_function: str
    parameters: Dict[str, Any]
    severity: str  # "error", "warning", "info"
    is_active: bool


@dataclass
class ValidationResult:
    """Data validation result."""
    rule_id: str
    message_id: str
    is_valid: bool
    severity: str
    error_message: Optional[str]
    corrected_value: Optional[Any]
    validation_timestamp: datetime


@dataclass
class DataEnrichment:
    """Data enrichment configuration."""
    enrichment_id: str
    name: str
    description: str
    data_sources: List[str]
    enrichment_fields: List[str]
    cache_ttl_seconds: int
    is_active: bool


class RealtimeMarketServiceInterface(ABC):
    """
    Real-time Market Data Processing Service Interface

    Provides high-performance market data ingestion, processing, validation,
    enrichment, and distribution capabilities for financial trading systems.
    """

    # Data Ingestion

    @abstractmethod
    async def start_data_ingestion(
        self,
        sources: List[str],
        buffer_size: int = 10000,
        batch_size: int = 100
    ) -> str:
        """
        Start real-time data ingestion from multiple sources.

        Args:
            sources: List of data source identifiers
            buffer_size: Internal buffer size for messages
            batch_size: Batch size for processing

        Returns:
            Ingestion session ID
        """

    @abstractmethod
    async def stop_data_ingestion(self, session_id: str) -> bool:
        """
        Stop data ingestion session.

        Args:
            session_id: Ingestion session identifier

        Returns:
            Success status
        """

    @abstractmethod
    async def ingest_message(self, message: MarketDataMessage) -> bool:
        """
        Ingest individual market data message.

        Args:
            message: Market data message to ingest

        Returns:
            Success status
        """

    @abstractmethod
    async def ingest_batch(self, messages: List[MarketDataMessage]) -> Dict[str, bool]:
        """
        Ingest batch of market data messages.

        Args:
            messages: List of messages to ingest

        Returns:
            Success status for each message by message_id
        """

    # Data Processing & Validation

    @abstractmethod
    async def add_validation_rule(self, rule: DataValidationRule) -> bool:
        """
        Add data validation rule.

        Args:
            rule: Validation rule configuration

        Returns:
            Success status
        """

    @abstractmethod
    async def validate_message(self, message: MarketDataMessage) -> List[ValidationResult]:
        """
        Validate market data message.

        Args:
            message: Message to validate

        Returns:
            Validation results for all applicable rules
        """

    @abstractmethod
    async def process_message(self, message: MarketDataMessage) -> MarketDataMessage:
        """
        Process and enrich market data message.

        Args:
            message: Raw message to process

        Returns:
            Processed and enriched message
        """

    # Real-time Aggregation

    @abstractmethod
    async def start_minute_bar_aggregation(
        self,
        symbols: List[str],
        output_callback: Optional[Callable[[MinuteBar], None]] = None
    ) -> str:
        """
        Start real-time minute bar aggregation.

        Args:
            symbols: Symbols to aggregate
            output_callback: Callback for completed bars

        Returns:
            Aggregation session ID
        """

    @abstractmethod
    async def get_current_minute_bar(self, symbol: str) -> Optional[MinuteBar]:
        """
        Get current (in-progress) minute bar for symbol.

        Args:
            symbol: Symbol to get bar for

        Returns:
            Current minute bar or None
        """

    @abstractmethod
    async def get_completed_minute_bars(
        self,
        symbol: str,
        start_time: datetime,
        end_time: datetime
    ) -> List[MinuteBar]:
        """
        Get completed minute bars for time range.

        Args:
            symbol: Symbol to get bars for
            start_time: Start of time range
            end_time: End of time range

        Returns:
            List of completed minute bars
        """

    # Data Distribution & Subscriptions

    @abstractmethod
    async def subscribe(
        self,
        symbols: List[str],
        data_types: List[MarketDataType],
        callback: Callable[[MarketDataMessage], None],
        filters: Optional[Dict[str, Any]] = None
    ) -> str:
        """
        Subscribe to real-time market data.

        Args:
            symbols: Symbols to subscribe to
            data_types: Types of data to receive
            callback: Function to call with new messages
            filters: Optional message filters

        Returns:
            Subscription ID
        """

    @abstractmethod
    async def unsubscribe(self, subscription_id: str) -> bool:
        """
        Unsubscribe from market data.

        Args:
            subscription_id: Subscription to cancel

        Returns:
            Success status
        """

    @abstractmethod
    async def get_active_subscriptions(self) -> List[DataSubscription]:
        """
        Get all active data subscriptions.

        Returns:
            List of active subscriptions
        """

    # Data Quality & Monitoring

    @abstractmethod
    async def assess_data_quality(
        self,
        symbol: str,
        data_type: MarketDataType,
        time_window: timedelta
    ) -> Dict[str, Any]:
        """
        Assess data quality for symbol and type.

        Args:
            symbol: Symbol to assess
            data_type: Type of data to assess
            time_window: Time window for assessment

        Returns:
            Data quality metrics and assessment
        """

    @abstractmethod
    async def get_processing_metrics(self) -> ProcessingMetrics:
        """
        Get real-time processing performance metrics.

        Returns:
            Current processing metrics
        """

    @abstractmethod
    async def get_latency_percentiles(
        self,
        time_window: timedelta
    ) -> Dict[str, float]:
        """
        Get processing latency percentiles.

        Args:
            time_window: Time window for calculation

        Returns:
            Latency percentiles (p50, p90, p95, p99)
        """

    # Market Data Queries

    @abstractmethod
    async def get_latest_trade(self, symbol: str) -> Optional[TradeMessage]:
        """
        Get latest trade for symbol.

        Args:
            symbol: Symbol to get trade for

        Returns:
            Latest trade message or None
        """

    @abstractmethod
    async def get_latest_quote(self, symbol: str) -> Optional[QuoteMessage]:
        """
        Get latest quote for symbol.

        Args:
            symbol: Symbol to get quote for

        Returns:
            Latest quote message or None
        """

    @abstractmethod
    async def get_order_book(
        self,
        symbol: str,
        depth: int = 10
    ) -> Optional[Level2Message]:
        """
        Get current order book for symbol.

        Args:
            symbol: Symbol to get order book for
            depth: Number of price levels to return

        Returns:
            Current order book or None
        """

    @abstractmethod
    async def get_trade_history(
        self,
        symbol: str,
        start_time: datetime,
        end_time: datetime,
        limit: Optional[int] = None
    ) -> List[TradeMessage]:
        """
        Get historical trades for symbol.

        Args:
            symbol: Symbol to get trades for
            start_time: Start of time range
            end_time: End of time range
            limit: Maximum number of trades to return

        Returns:
            List of historical trades
        """

    # Data Recovery & Replay

    @abstractmethod
    async def start_data_replay(
        self,
        symbols: List[str],
        start_time: datetime,
        end_time: datetime,
        speed_multiplier: float = 1.0,
        callback: Optional[Callable[[MarketDataMessage], None]] = None
    ) -> str:
        """
        Start historical data replay.

        Args:
            symbols: Symbols to replay
            start_time: Replay start time
            end_time: Replay end time
            speed_multiplier: Replay speed (1.0 = real-time)
            callback: Optional callback for each message

        Returns:
            Replay session ID
        """

    @abstractmethod
    async def stop_data_replay(self, session_id: str) -> bool:
        """
        Stop data replay session.

        Args:
            session_id: Replay session to stop

        Returns:
            Success status
        """

    # Configuration & Administration

    @abstractmethod
    async def configure_source(
        self,
        source_id: str,
        connection_params: Dict[str, Any],
        data_mappings: Dict[str, str],
        quality_thresholds: Dict[str, float]
    ) -> bool:
        """
        Configure market data source.

        Args:
            source_id: Source identifier
            connection_params: Connection configuration
            data_mappings: Field mappings from source to internal format
            quality_thresholds: Data quality thresholds

        Returns:
            Success status
        """

    @abstractmethod
    async def get_source_status(self, source_id: str) -> Dict[str, Any]:
        """
        Get market data source status.

        Args:
            source_id: Source to check

        Returns:
            Source status and metrics
        """

    @abstractmethod
    async def configure_enrichment(self, enrichment: DataEnrichment) -> bool:
        """
        Configure data enrichment.

        Args:
            enrichment: Enrichment configuration

        Returns:
            Success status
        """

    # Stream Processing

    @abstractmethod
    async def create_data_stream(
        self,
        stream_name: str,
        symbols: List[str],
        data_types: List[MarketDataType],
        processing_pipeline: List[str]
    ) -> str:
        """
        Create processed data stream.

        Args:
            stream_name: Name for the stream
            symbols: Symbols to include
            data_types: Data types to include
            processing_pipeline: Processing steps to apply

        Returns:
            Stream ID
        """

    @abstractmethod
    async def get_stream_data(
        self,
        stream_id: str,
        max_messages: int = 100
    ) -> AsyncIterator[MarketDataMessage]:
        """
        Get data from processed stream.

        Args:
            stream_id: Stream to read from
            max_messages: Maximum messages to return

        Yields:
            Processed market data messages
        """

    @abstractmethod
    async def get_processing_pipeline_status(
        self,
        stream_id: str
    ) -> Dict[str, Any]:
        """
        Get processing pipeline status.

        Args:
            stream_id: Stream to check

        Returns:
            Pipeline status and performance metrics
        """
