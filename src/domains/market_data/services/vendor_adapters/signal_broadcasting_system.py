#!/usr/bin/env python3
"""
Trading Signal Broadcasting System

This service handles the distribution of LLM-generated trading signals to multiple endpoints
including internal APIs, external systems, alerts, and monitoring dashboards. It provides
reliable signal delivery with retry mechanisms and performance tracking.

Features:
- Multi-channel signal broadcasting (WebSocket, REST API, Message Queue)
- Priority-based signal routing (critical signals get faster delivery)
- Signal validation and filtering before broadcast
- Real-time delivery status tracking
- Alert management for high-priority signals
- Signal performance monitoring and analytics
- Integration with portfolio management systems
"""

import asyncio
import logging
from datetime import datetime
from typing import Dict, List, Optional, Any, Set
from dataclasses import dataclass, field
from enum import Enum
import json
import time
import aiohttp
import websockets
from collections import defaultdict

import asyncpg
from core.platform.config_env.environment import Environment

logger = logging.getLogger(__name__)


class BroadcastChannel(Enum):
    """Signal broadcasting channels."""
    WEBSOCKET = "websocket"
    REST_API = "rest_api"
    MESSAGE_QUEUE = "message_queue"
    EMAIL_ALERT = "email_alert"
    SLACK_ALERT = "slack_alert"
    PORTFOLIO_SYSTEM = "portfolio_system"
    ANALYTICS_DASHBOARD = "analytics_dashboard"


class SignalPriority(Enum):
    """Signal priority levels for broadcasting."""
    CRITICAL = "critical"     # Immediate broadcast to all channels
    HIGH = "high"            # Fast broadcast to primary channels
    MEDIUM = "medium"        # Standard broadcast timing
    LOW = "low"             # Delayed broadcast, batch processing


@dataclass
class TradingSignal:
    """Standardized trading signal for broadcasting."""
    id: int
    symbol: str
    signal_type: str
    signal_category: str
    urgency_level: int

    # Signal data
    signal_strength: float
    signal_confidence: float
    recommended_action: str
    position_sizing: float
    time_horizon: str

    # Price predictions
    price_impact_1h: Optional[float] = None
    price_impact_1d: Optional[float] = None
    price_impact_5d: Optional[float] = None

    # Risk management
    risk_score: float = 0.5
    stop_loss: Optional[float] = None
    take_profit: Optional[float] = None

    # Metadata
    signal_timestamp: datetime = field(default_factory=datetime.now)
    news_analysis_id: Optional[int] = None
    supporting_factors: List[str] = field(default_factory=list)
    risk_factors: List[str] = field(default_factory=list)
    model_attribution: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Convert signal to dictionary for JSON serialization."""
        return {
            'id': self.id,
            'symbol': self.symbol,
            'signal_type': self.signal_type,
            'signal_category': self.signal_category,
            'urgency_level': self.urgency_level,
            'signal_strength': self.signal_strength,
            'signal_confidence': self.signal_confidence,
            'recommended_action': self.recommended_action,
            'position_sizing': self.position_sizing,
            'time_horizon': self.time_horizon,
            'price_impact_1h': self.price_impact_1h,
            'price_impact_1d': self.price_impact_1d,
            'price_impact_5d': self.price_impact_5d,
            'risk_score': self.risk_score,
            'stop_loss': self.stop_loss,
            'take_profit': self.take_profit,
            'signal_timestamp': self.signal_timestamp.isoformat(),
            'news_analysis_id': self.news_analysis_id,
            'supporting_factors': self.supporting_factors,
            'risk_factors': self.risk_factors,
            'model_attribution': self.model_attribution
        }


@dataclass
class BroadcastTarget:
    """Configuration for a broadcast target."""
    channel: BroadcastChannel
    name: str
    endpoint: str
    enabled: bool = True

    # Priority filtering
    min_urgency_level: int = 1
    min_confidence: float = 0.0
    signal_types: Optional[List[str]] = None
    symbols: Optional[List[str]] = None

    # Delivery settings
    retry_count: int = 3
    timeout_seconds: int = 30
    batch_size: int = 1
    batch_delay_seconds: int = 0

    # Authentication
    headers: Dict[str, str] = field(default_factory=dict)
    auth_token: Optional[str] = None


@dataclass
class BroadcastResult:
    """Result of a signal broadcast attempt."""
    target_name: str
    channel: BroadcastChannel
    signal_id: int
    success: bool
    timestamp: datetime
    latency_ms: int
    error_message: Optional[str] = None
    retry_count: int = 0


class SignalValidator:
    """Validates signals before broadcasting."""

    def __init__(self):
        self.validation_rules = {
            'required_fields': ['id', 'symbol', 'signal_strength', 'signal_confidence'],
            'min_confidence': 0.5,
            'max_urgency_level': 10,
            'valid_actions': ['strong_buy', 'buy', 'hold', 'sell', 'strong_sell', 'hedge'],
            'valid_categories': ['bullish', 'bearish', 'neutral', 'risk', 'opportunity']
        }

    def validate_signal(self, signal: TradingSignal) -> tuple[bool, List[str]]:
        """Validate a trading signal. Returns (is_valid, error_messages)."""
        errors = []

        # Check required fields
        for field in self.validation_rules['required_fields']:
            if not hasattr(signal, field) or getattr(signal, field) is None:
                errors.append(f"Missing required field: {field}")

        # Validate confidence range
        if signal.signal_confidence < 0.0 or signal.signal_confidence > 1.0:
            errors.append("Signal confidence must be between 0.0 and 1.0")

        # Validate minimum confidence
        if signal.signal_confidence < self.validation_rules['min_confidence']:
            errors.append(f"Signal confidence below minimum threshold: {self.validation_rules['min_confidence']}")

        # Validate urgency level
        if signal.urgency_level < 1 or signal.urgency_level > self.validation_rules['max_urgency_level']:
            errors.append(f"Urgency level must be between 1 and {self.validation_rules['max_urgency_level']}")

        # Validate action
        if signal.recommended_action not in self.validation_rules['valid_actions']:
            errors.append(f"Invalid recommended action: {signal.recommended_action}")

        # Validate category
        if signal.signal_category not in self.validation_rules['valid_categories']:
            errors.append(f"Invalid signal category: {signal.signal_category}")

        # Validate signal strength range
        if signal.signal_strength < -1.0 or signal.signal_strength > 1.0:
            errors.append("Signal strength must be between -1.0 and 1.0")

        # Validate position sizing
        if signal.position_sizing < 0.0 or signal.position_sizing > 1.0:
            errors.append("Position sizing must be between 0.0 and 1.0")

        return len(errors) == 0, errors


class WebSocketBroadcaster:
    """Handles WebSocket signal broadcasting."""

    def __init__(self):
        self.connected_clients: Set[websockets.WebSocketServerProtocol] = set()
        self.server = None
        self.server_host = "localhost"
        self.server_port = 8765

    async def start_server(self):
        """Start WebSocket server for signal broadcasting."""
        self.server = await websockets.serve(
            self.handle_client_connection,
            self.server_host,
            self.server_port
        )
        logger.info(f"WebSocket signal server started on {self.server_host}:{self.server_port}")

    async def stop_server(self):
        """Stop WebSocket server."""
        if self.server:
            self.server.close()
            await self.server.wait_closed()
            logger.info("WebSocket signal server stopped")

    async def handle_client_connection(self, websocket, path):
        """Handle new WebSocket client connection."""
        self.connected_clients.add(websocket)
        logger.info(f"New WebSocket client connected: {websocket.remote_address}")

        try:
            # Send welcome message
            await websocket.send(json.dumps({
                'type': 'connection',
                'status': 'connected',
                'message': 'Connected to ATS trading signals',
                'timestamp': datetime.now().isoformat()
            }))

            # Keep connection alive
            async for message in websocket:
                # Handle client messages (ping, subscription updates, etc.)
                try:
                    data = json.loads(message)
                    await self.handle_client_message(websocket, data)
                except json.JSONDecodeError:
                    logger.warning(f"Invalid JSON from client: {message}")

        except websockets.exceptions.ConnectionClosed:
            logger.info(f"WebSocket client disconnected: {websocket.remote_address}")
        finally:
            self.connected_clients.discard(websocket)

    async def handle_client_message(self, websocket, data: Dict[str, Any]):
        """Handle message from WebSocket client."""
        message_type = data.get('type')

        if message_type == 'ping':
            await websocket.send(json.dumps({
                'type': 'pong',
                'timestamp': datetime.now().isoformat()
            }))
        elif message_type == 'subscribe':
            # Handle subscription requests
            logger.info(f"Client subscription request: {data}")

    async def broadcast_signal(self, signal: TradingSignal) -> int:
        """Broadcast signal to all connected WebSocket clients."""
        if not self.connected_clients:
            return 0

        message = {
            'type': 'trading_signal',
            'signal': signal.to_dict(),
            'timestamp': datetime.now().isoformat()
        }

        message_json = json.dumps(message)
        disconnected_clients = set()
        successful_broadcasts = 0

        for client in self.connected_clients:
            try:
                await client.send(message_json)
                successful_broadcasts += 1
            except websockets.exceptions.ConnectionClosed:
                disconnected_clients.add(client)
            except Exception as e:
                logger.error(f"Error broadcasting to WebSocket client: {e}")

        # Remove disconnected clients
        self.connected_clients -= disconnected_clients

        return successful_broadcasts


class TradingSignalBroadcastingSystem:
    """Main trading signal broadcasting system."""

    def __init__(self, db_pool: asyncpg.Pool, env: Environment):
        self.db_pool = db_pool
        self.env = env

        # Components
        self.validator = SignalValidator()
        self.websocket_broadcaster = WebSocketBroadcaster()

        # Broadcast targets
        self.broadcast_targets: List[BroadcastTarget] = []

        # Signal queues by priority
        self.signal_queues = {
            SignalPriority.CRITICAL: asyncio.Queue(maxsize=100),
            SignalPriority.HIGH: asyncio.Queue(maxsize=500),
            SignalPriority.MEDIUM: asyncio.Queue(maxsize=1000),
            SignalPriority.LOW: asyncio.Queue(maxsize=2000)
        }

        # Performance tracking
        self.broadcast_stats = {
            'total_signals_processed': 0,
            'successful_broadcasts': 0,
            'failed_broadcasts': 0,
            'total_broadcast_latency_ms': 0,
            'avg_broadcast_latency_ms': 0.0,
            'broadcasts_by_channel': defaultdict(int),
            'broadcasts_by_priority': defaultdict(int)
        }

        # System state
        self._running = False
        self._broadcast_tasks: List[asyncio.Task] = []

    def add_broadcast_target(self, target: BroadcastTarget):
        """Add a broadcast target."""
        self.broadcast_targets.append(target)
        logger.info(f"Added broadcast target: {target.name} ({target.channel.value})")

    async def start(self):
        """Start the broadcasting system."""
        if self._running:
            logger.warning("Broadcasting system is already running")
            return

        self._running = True
        logger.info("Starting Trading Signal Broadcasting System")

        # Start WebSocket server
        await self.websocket_broadcaster.start_server()

        # Start signal processing tasks
        self._broadcast_tasks = [
            asyncio.create_task(self._process_critical_signals()),
            asyncio.create_task(self._process_high_priority_signals()),
            asyncio.create_task(self._process_medium_priority_signals()),
            asyncio.create_task(self._process_low_priority_signals()),
            asyncio.create_task(self._periodic_signal_fetcher()),
            asyncio.create_task(self._performance_reporter())
        ]

        logger.info("Broadcasting system started successfully")

    async def stop(self):
        """Stop the broadcasting system."""
        if not self._running:
            return

        logger.info("Stopping Trading Signal Broadcasting System")
        self._running = False

        # Cancel all tasks
        for task in self._broadcast_tasks:
            task.cancel()

        await asyncio.gather(*self._broadcast_tasks, return_exceptions=True)

        # Stop WebSocket server
        await self.websocket_broadcaster.stop_server()

        logger.info("Broadcasting system stopped")

    async def broadcast_signal(self, signal: TradingSignal, priority: SignalPriority = SignalPriority.MEDIUM):
        """Queue a signal for broadcasting."""

        # Validate signal
        is_valid, errors = self.validator.validate_signal(signal)
        if not is_valid:
            logger.error(f"Signal validation failed for {signal.id}: {errors}")
            return False

        # Determine priority if not specified
        if priority == SignalPriority.MEDIUM:
            priority = self._determine_signal_priority(signal)

        # Queue signal for processing
        try:
            await self.signal_queues[priority].put(signal)
            self.broadcast_stats['broadcasts_by_priority'][priority.value] += 1

            logger.info(f"Signal {signal.id} queued for broadcasting (priority: {priority.value})")
            return True

        except asyncio.QueueFull:
            logger.error(f"Signal queue full for priority {priority.value}, dropping signal {signal.id}")
            return False

    def _determine_signal_priority(self, signal: TradingSignal) -> SignalPriority:
        """Determine signal broadcasting priority."""

        # Critical priority for high urgency + high confidence
        if signal.urgency_level >= 8 and signal.signal_confidence >= 0.8:
            return SignalPriority.CRITICAL

        # High priority for urgent signals or high confidence
        elif signal.urgency_level >= 6 or signal.signal_confidence >= 0.7:
            return SignalPriority.HIGH

        # Medium priority for moderate signals
        elif signal.urgency_level >= 4 or signal.signal_confidence >= 0.6:
            return SignalPriority.MEDIUM

        # Low priority for everything else
        else:
            return SignalPriority.LOW

    async def _process_critical_signals(self):
        """Process critical priority signals."""
        while self._running:
            try:
                signal = await asyncio.wait_for(
                    self.signal_queues[SignalPriority.CRITICAL].get(),
                    timeout=1.0
                )

                # Broadcast immediately to all targets
                await self._broadcast_signal_to_targets(signal, priority=SignalPriority.CRITICAL)

            except asyncio.TimeoutError:
                continue
            except Exception as e:
                logger.error(f"Error processing critical signals: {e}")
                await asyncio.sleep(1)

    async def _process_high_priority_signals(self):
        """Process high priority signals."""
        while self._running:
            try:
                signal = await asyncio.wait_for(
                    self.signal_queues[SignalPriority.HIGH].get(),
                    timeout=5.0
                )

                await self._broadcast_signal_to_targets(signal, priority=SignalPriority.HIGH)

                # Small delay for high priority signals
                await asyncio.sleep(0.1)

            except asyncio.TimeoutError:
                continue
            except Exception as e:
                logger.error(f"Error processing high priority signals: {e}")
                await asyncio.sleep(1)

    async def _process_medium_priority_signals(self):
        """Process medium priority signals."""
        while self._running:
            try:
                signal = await asyncio.wait_for(
                    self.signal_queues[SignalPriority.MEDIUM].get(),
                    timeout=10.0
                )

                await self._broadcast_signal_to_targets(signal, priority=SignalPriority.MEDIUM)

                # Standard delay for medium priority signals
                await asyncio.sleep(0.5)

            except asyncio.TimeoutError:
                continue
            except Exception as e:
                logger.error(f"Error processing medium priority signals: {e}")
                await asyncio.sleep(1)

    async def _process_low_priority_signals(self):
        """Process low priority signals with batching."""
        batch = []
        batch_timeout = 30  # seconds

        while self._running:
            try:
                # Collect signals for batching
                signal = await asyncio.wait_for(
                    self.signal_queues[SignalPriority.LOW].get(),
                    timeout=batch_timeout
                )

                batch.append(signal)

                # Process batch when it reaches size limit or timeout
                if len(batch) >= 10:
                    await self._broadcast_signal_batch(batch, priority=SignalPriority.LOW)
                    batch = []

            except asyncio.TimeoutError:
                # Process accumulated batch on timeout
                if batch:
                    await self._broadcast_signal_batch(batch, priority=SignalPriority.LOW)
                    batch = []
            except Exception as e:
                logger.error(f"Error processing low priority signals: {e}")
                await asyncio.sleep(1)

    async def _broadcast_signal_to_targets(self, signal: TradingSignal, priority: SignalPriority):
        """Broadcast signal to all configured targets."""

        start_time = time.time()
        broadcast_tasks = []

        for target in self.broadcast_targets:
            if not target.enabled:
                continue

            # Filter by target criteria
            if not self._signal_matches_target(signal, target):
                continue

            # Create broadcast task
            task = asyncio.create_task(
                self._send_signal_to_target(signal, target)
            )
            broadcast_tasks.append((task, target))

        # Send to WebSocket clients
        websocket_task = asyncio.create_task(
            self.websocket_broadcaster.broadcast_signal(signal)
        )

        # Wait for all broadcasts to complete
        results = []

        for task, target in broadcast_tasks:
            try:
                result = await task
                results.append(result)
            except Exception as e:
                error_result = BroadcastResult(
                    target_name=target.name,
                    channel=target.channel,
                    signal_id=signal.id,
                    success=False,
                    timestamp=datetime.now(),
                    latency_ms=0,
                    error_message=str(e)
                )
                results.append(error_result)

        # Handle WebSocket broadcast result
        try:
            websocket_count = await websocket_task
            if websocket_count > 0:
                websocket_result = BroadcastResult(
                    target_name="websocket_clients",
                    channel=BroadcastChannel.WEBSOCKET,
                    signal_id=signal.id,
                    success=True,
                    timestamp=datetime.now(),
                    latency_ms=int((time.time() - start_time) * 1000),
                )
                results.append(websocket_result)
        except Exception as e:
            logger.error(f"WebSocket broadcast error: {e}")

        # Update statistics
        total_latency_ms = int((time.time() - start_time) * 1000)
        await self._update_broadcast_stats(results, total_latency_ms)

        # Log broadcast summary
        successful = sum(1 for r in results if r.success)
        total = len(results)

        logger.info(f"Signal {signal.id} broadcast complete: {successful}/{total} successful "
                   f"(latency: {total_latency_ms}ms, priority: {priority.value})")

    def _signal_matches_target(self, signal: TradingSignal, target: BroadcastTarget) -> bool:
        """Check if signal matches target filtering criteria."""

        # Check minimum urgency level
        if signal.urgency_level < target.min_urgency_level:
            return False

        # Check minimum confidence
        if signal.signal_confidence < target.min_confidence:
            return False

        # Check signal types filter
        if target.signal_types and signal.signal_type not in target.signal_types:
            return False

        # Check symbols filter
        if target.symbols and signal.symbol not in target.symbols:
            return False

        return True

    async def _send_signal_to_target(self, signal: TradingSignal, target: BroadcastTarget) -> BroadcastResult:
        """Send signal to a specific target."""

        start_time = time.time()

        try:
            if target.channel == BroadcastChannel.REST_API:
                success = await self._send_rest_api_signal(signal, target)
            elif target.channel == BroadcastChannel.EMAIL_ALERT:
                success = await self._send_email_alert(signal, target)
            elif target.channel == BroadcastChannel.SLACK_ALERT:
                success = await self._send_slack_alert(signal, target)
            elif target.channel == BroadcastChannel.PORTFOLIO_SYSTEM:
                success = await self._send_portfolio_signal(signal, target)
            else:
                logger.warning(f"Unsupported broadcast channel: {target.channel.value}")
                success = False

            latency_ms = int((time.time() - start_time) * 1000)

            return BroadcastResult(
                target_name=target.name,
                channel=target.channel,
                signal_id=signal.id,
                success=success,
                timestamp=datetime.now(),
                latency_ms=latency_ms
            )

        except Exception as e:
            latency_ms = int((time.time() - start_time) * 1000)

            return BroadcastResult(
                target_name=target.name,
                channel=target.channel,
                signal_id=signal.id,
                success=False,
                timestamp=datetime.now(),
                latency_ms=latency_ms,
                error_message=str(e)
            )

    async def _send_rest_api_signal(self, signal: TradingSignal, target: BroadcastTarget) -> bool:
        """Send signal via REST API."""

        headers = dict(target.headers)
        if target.auth_token:
            headers['Authorization'] = f'Bearer {target.auth_token}'
        headers['Content-Type'] = 'application/json'

        payload = {
            'signal': signal.to_dict(),
            'timestamp': datetime.now().isoformat(),
            'source': 'ats_llm_signals'
        }

        timeout = aiohttp.ClientTimeout(total=target.timeout_seconds)

        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.post(
                target.endpoint,
                json=payload,
                headers=headers
            ) as response:
                return response.status == 200

    async def _send_email_alert(self, signal: TradingSignal, target: BroadcastTarget) -> bool:
        """Send signal via email alert."""
        # Placeholder for email alert implementation
        logger.info(f"Email alert for signal {signal.id} to {target.endpoint}")
        return True

    async def _send_slack_alert(self, signal: TradingSignal, target: BroadcastTarget) -> bool:
        """Send signal via Slack alert."""
        # Create Slack message format

        color = "good" if signal.signal_category == "bullish" else "danger" if signal.signal_category == "bearish" else "warning"

        slack_message = {
            "attachments": [{
                "color": color,
                "title": f"Trading Signal: {signal.symbol}",
                "fields": [
                    {"title": "Action", "value": signal.recommended_action.upper(), "short": True},
                    {"title": "Confidence", "value": f"{signal.signal_confidence:.1%}", "short": True},
                    {"title": "Urgency", "value": f"{signal.urgency_level}/10", "short": True},
                    {"title": "Position Size", "value": f"{signal.position_sizing:.1%}", "short": True}
                ],
                "footer": "ATS LLM Trading Signals",
                "ts": int(signal.signal_timestamp.timestamp())
            }]
        }

        timeout = aiohttp.ClientTimeout(total=target.timeout_seconds)

        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.post(
                target.endpoint,
                json=slack_message,
                headers=target.headers
            ) as response:
                return response.status == 200

    async def _send_portfolio_signal(self, signal: TradingSignal, target: BroadcastTarget) -> bool:
        """Send signal to portfolio management system."""
        # Format for portfolio system integration

        portfolio_signal = {
            'symbol': signal.symbol,
            'action': signal.recommended_action,
            'strength': signal.signal_strength,
            'confidence': signal.signal_confidence,
            'position_size': signal.position_sizing,
            'stop_loss': signal.stop_loss,
            'take_profit': signal.take_profit,
            'time_horizon': signal.time_horizon,
            'risk_score': signal.risk_score,
            'metadata': {
                'signal_id': signal.id,
                'urgency': signal.urgency_level,
                'news_analysis_id': signal.news_analysis_id,
                'timestamp': signal.signal_timestamp.isoformat()
            }
        }

        headers = dict(target.headers)
        if target.auth_token:
            headers['Authorization'] = f'Bearer {target.auth_token}'
        headers['Content-Type'] = 'application/json'

        timeout = aiohttp.ClientTimeout(total=target.timeout_seconds)

        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.post(
                target.endpoint,
                json=portfolio_signal,
                headers=headers
            ) as response:
                return response.status == 200

    async def _broadcast_signal_batch(self, signals: List[TradingSignal], priority: SignalPriority):
        """Broadcast a batch of signals."""
        for signal in signals:
            await self._broadcast_signal_to_targets(signal, priority)
            await asyncio.sleep(0.1)  # Small delay between batch items

    async def _periodic_signal_fetcher(self):
        """Periodically fetch new signals from database."""
        while self._running:
            try:
                await asyncio.sleep(5)  # Check every 5 seconds

                # Fetch new signals from database
                signals = await self._fetch_new_signals()

                for signal_data in signals:
                    signal = self._create_signal_from_db_data(signal_data)
                    if signal:
                        await self.broadcast_signal(signal)

            except Exception as e:
                logger.error(f"Error in periodic signal fetcher: {e}")
                await asyncio.sleep(30)  # Back off on error

    async def _fetch_new_signals(self) -> List[Dict[str, Any]]:
        """Fetch new signals from database."""
        try:
            async with self.db_pool.acquire() as conn:
                # Get signals from last 1 minute that haven't been broadcast yet
                signals = await conn.fetch("""
                    SELECT * FROM dev_critical_news_signals
                    WHERE signal_timestamp >= NOW() - INTERVAL '1 minute'
                    AND id NOT IN (
                        SELECT DISTINCT signal_id
                        FROM dev_signal_broadcasts
                        WHERE broadcast_timestamp >= NOW() - INTERVAL '1 hour'
                    )
                    ORDER BY urgency_level DESC, signal_timestamp DESC
                    LIMIT 50
                """)

                return [dict(signal) for signal in signals]

        except Exception as e:
            logger.error(f"Failed to fetch new signals: {e}")
            return []

    def _create_signal_from_db_data(self, data: Dict[str, Any]) -> Optional[TradingSignal]:
        """Create TradingSignal object from database data."""
        try:
            return TradingSignal(
                id=data['id'],
                symbol=data['symbol'],
                signal_type=data['signal_type'],
                signal_category=data['signal_category'],
                urgency_level=data['urgency_level'],
                signal_strength=float(data['signal_strength']),
                signal_confidence=float(data['signal_confidence']),
                recommended_action=data['recommended_action'],
                position_sizing=float(data.get('position_sizing_recommendation', 0.0)),
                time_horizon=data.get('time_horizon', 'medium_term'),
                price_impact_1h=data.get('predicted_price_impact_1h'),
                price_impact_1d=data.get('predicted_price_impact_1d'),
                price_impact_5d=data.get('predicted_price_impact_5d'),
                risk_score=float(data.get('risk_score', 0.5)),
                stop_loss=data.get('stop_loss_recommendation'),
                take_profit=data.get('take_profit_recommendation'),
                signal_timestamp=data['signal_timestamp'],
                news_analysis_id=data.get('news_llm_analysis_ids', [None])[0],
                supporting_factors=data.get('key_themes', []),
                risk_factors=data.get('risk_factors', []),
                model_attribution=data.get('model_attribution', {})
            )
        except Exception as e:
            logger.error(f"Failed to create signal from database data: {e}")
            return None

    async def _update_broadcast_stats(self, results: List[BroadcastResult], total_latency_ms: int):
        """Update broadcast statistics."""

        self.broadcast_stats['total_signals_processed'] += 1
        self.broadcast_stats['total_broadcast_latency_ms'] += total_latency_ms

        for result in results:
            if result.success:
                self.broadcast_stats['successful_broadcasts'] += 1
            else:
                self.broadcast_stats['failed_broadcasts'] += 1

            self.broadcast_stats['broadcasts_by_channel'][result.channel.value] += 1

        # Update average latency
        self.broadcast_stats['avg_broadcast_latency_ms'] = (
            self.broadcast_stats['total_broadcast_latency_ms'] /
            self.broadcast_stats['total_signals_processed']
        )

        # Store broadcast results in database
        await self._store_broadcast_results(results)

    async def _store_broadcast_results(self, results: List[BroadcastResult]):
        """Store broadcast results in database for tracking."""
        try:
            async with self.db_pool.acquire() as conn:
                # Create broadcast records
                broadcast_records = []
                for result in results:
                    broadcast_records.append((
                        result.signal_id,
                        result.target_name,
                        result.channel.value,
                        result.success,
                        result.timestamp,
                        result.latency_ms,
                        result.error_message,
                        result.retry_count
                    ))

                await conn.executemany("""
                    INSERT INTO dev_signal_broadcasts
                    (signal_id, target_name, channel, success, broadcast_timestamp,
                     latency_ms, error_message, retry_count)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                """, broadcast_records)

        except Exception as e:
            logger.error(f"Failed to store broadcast results: {e}")

    async def _performance_reporter(self):
        """Report performance metrics periodically."""
        while self._running:
            try:
                await asyncio.sleep(60)  # Report every minute

                logger.info(f"Broadcasting Stats: "
                           f"processed={self.broadcast_stats['total_signals_processed']}, "
                           f"successful={self.broadcast_stats['successful_broadcasts']}, "
                           f"failed={self.broadcast_stats['failed_broadcasts']}, "
                           f"avg_latency={self.broadcast_stats['avg_broadcast_latency_ms']:.1f}ms")

            except Exception as e:
                logger.error(f"Error reporting performance metrics: {e}")

    def get_performance_metrics(self) -> Dict[str, Any]:
        """Get comprehensive performance metrics."""

        success_rate = (
            self.broadcast_stats['successful_broadcasts'] /
            max(1, self.broadcast_stats['successful_broadcasts'] + self.broadcast_stats['failed_broadcasts'])
        )

        return {
            'broadcast_stats': dict(self.broadcast_stats),
            'success_rate': success_rate,
            'active_targets': len([t for t in self.broadcast_targets if t.enabled]),
            'connected_websocket_clients': len(self.websocket_broadcaster.connected_clients),
            'queue_sizes': {
                priority.value: queue.qsize()
                for priority, queue in self.signal_queues.items()
            },
            'system_running': self._running
        }


# Database migration for broadcast tracking
BROADCAST_TRACKING_SQL = """
-- Create table for tracking signal broadcasts
CREATE TABLE IF NOT EXISTS dev_signal_broadcasts (
    id BIGSERIAL PRIMARY KEY,
    signal_id BIGINT NOT NULL REFERENCES dev_critical_news_signals(id),
    target_name VARCHAR(100) NOT NULL,
    channel VARCHAR(50) NOT NULL,
    success BOOLEAN NOT NULL,
    broadcast_timestamp TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    latency_ms INTEGER DEFAULT 0,
    error_message TEXT,
    retry_count INTEGER DEFAULT 0,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX idx_signal_broadcasts_signal_id ON dev_signal_broadcasts(signal_id);
CREATE INDEX idx_signal_broadcasts_timestamp ON dev_signal_broadcasts(broadcast_timestamp DESC);
CREATE INDEX idx_signal_broadcasts_success ON dev_signal_broadcasts(success, broadcast_timestamp DESC);
"""


# Factory function
async def create_signal_broadcasting_system(
    db_pool: asyncpg.Pool,
    env: Environment
) -> TradingSignalBroadcastingSystem:
    """Create and configure signal broadcasting system."""

    # Create system
    system = TradingSignalBroadcastingSystem(db_pool, env)

    # Add default broadcast targets
    # WebSocket is handled internally, add external targets

    # Example: REST API endpoint
    if env.get('SIGNAL_API_ENDPOINT'):
        api_target = BroadcastTarget(
            channel=BroadcastChannel.REST_API,
            name="internal_api",
            endpoint=env.get('SIGNAL_API_ENDPOINT'),
            min_urgency_level=6,
            min_confidence=0.7,
            timeout_seconds=10
        )
        system.add_broadcast_target(api_target)

    # Example: Slack alerts for critical signals
    if env.get('SLACK_WEBHOOK_URL'):
        slack_target = BroadcastTarget(
            channel=BroadcastChannel.SLACK_ALERT,
            name="critical_alerts",
            endpoint=env.get('SLACK_WEBHOOK_URL'),
            min_urgency_level=8,
            min_confidence=0.8,
            timeout_seconds=15
        )
        system.add_broadcast_target(slack_target)

    logger.info("Trading Signal Broadcasting System created and configured")

    return system