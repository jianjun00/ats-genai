"""
Event Correlation Engine - Detect relationships between financial events
"""

import logging
from datetime import datetime, timedelta
from typing import List, Dict, Any
from dataclasses import dataclass

from src.domains.analytics.events.proto.events_pb2 import Event, EventType

logger = logging.getLogger(__name__)

@dataclass
class CorrelationRule:
    """Rule for detecting event correlations"""
    name: str
    primary_type: EventType
    related_type: EventType
    time_window_minutes: int
    base_score: float
    description: str

class CorrelationEngine:
    """Engine for detecting correlations between financial events"""

    def __init__(self, event_storage):
        """
        Initialize correlation engine

        Args:
            event_storage: EventStorage instance for querying events
        """
        self.storage = event_storage
        self.correlation_rules = self._define_correlation_rules()

    def _define_correlation_rules(self) -> List[CorrelationRule]:
        """Define correlation detection rules"""
        return [
            # News -> Price Movement
            CorrelationRule(
                name="news_price_impact",
                primary_type=EventType.EVENT_TYPE_NEWS,
                related_type=EventType.EVENT_TYPE_PRICE_GAP,
                time_window_minutes=30,
                base_score=0.7,
                description="News events followed by significant price movements"
            ),

            # News -> Technical Signals
            CorrelationRule(
                name="news_technical_signal",
                primary_type=EventType.EVENT_TYPE_NEWS,
                related_type=EventType.EVENT_TYPE_TECHNICAL_SIGNAL,
                time_window_minutes=60,
                base_score=0.6,
                description="News events triggering technical signals"
            ),

            # Earnings -> Price Gap
            CorrelationRule(
                name="earnings_price_impact",
                primary_type=EventType.EVENT_TYPE_EARNINGS,
                related_type=EventType.EVENT_TYPE_PRICE_GAP,
                time_window_minutes=15,
                base_score=0.8,
                description="Earnings reports causing immediate price gaps"
            ),

            # News -> Volume Anomaly
            CorrelationRule(
                name="news_volume_spike",
                primary_type=EventType.EVENT_TYPE_NEWS,
                related_type=EventType.EVENT_TYPE_VOLUME_ANOMALY,
                time_window_minutes=30,
                base_score=0.5,
                description="News causing abnormal trading volume"
            ),

            # Economic Indicator -> Market Movement
            CorrelationRule(
                name="economic_market_impact",
                primary_type=EventType.EVENT_TYPE_ECONOMIC_INDICATOR,
                related_type=EventType.EVENT_TYPE_TECHNICAL_SIGNAL,
                time_window_minutes=60,
                base_score=0.4,
                description="Economic indicators affecting market technical signals"
            ),

            # Corporate Action -> Price Adjustment
            CorrelationRule(
                name="corporate_action_price",
                primary_type=EventType.EVENT_TYPE_CORPORATE_ACTION,
                related_type=EventType.EVENT_TYPE_PRICE_GAP,
                time_window_minutes=1440,  # 24 hours
                base_score=0.9,
                description="Corporate actions causing price adjustments"
            )
        ]

    def find_correlations(self, event: Event) -> List[Dict[str, Any]]:
        """
        Find correlations for a given event

        Args:
            event: Event to find correlations for

        Returns:
            List of correlation dictionaries
        """
        if not event.subject.symbol:
            return []

        correlations = []

        try:
            # Apply each correlation rule
            for rule in self.correlation_rules:
                # Check if this event matches the rule's primary type
                if event.event_type == rule.primary_type:
                    related_correlations = self._find_correlations_by_rule(event, rule, forward=True)
                    correlations.extend(related_correlations)

                # Check if this event matches the rule's related type (reverse correlation)
                elif event.event_type == rule.related_type:
                    primary_correlations = self._find_correlations_by_rule(event, rule, forward=False)
                    correlations.extend(primary_correlations)

            # Also find same-symbol temporal correlations
            temporal_correlations = self._find_temporal_correlations(event)
            correlations.extend(temporal_correlations)

            logger.info(f"🔍 Found {len(correlations)} potential correlations for event {event.event_id}")

            return correlations

        except Exception as e:
            logger.error(f"❌ Error finding correlations for event {event.event_id}: {e}")
            return []

    def _find_correlations_by_rule(self, event: Event, rule: CorrelationRule,
                                  forward: bool = True) -> List[Dict[str, Any]]:
        """
        Find correlations using a specific rule

        Args:
            event: Current event
            rule: Correlation rule to apply
            forward: If True, look for related events after this event
                    If False, look for primary events before this event
        """
        correlations = []

        try:
            # Define time window for searching
            time_window = timedelta(minutes=rule.time_window_minutes)

            if forward:
                # Look for related events after this event
                after_time = event.timestamp
                before_time = event.timestamp + time_window
                target_type = rule.related_type.name.replace('EVENT_TYPE_', '').lower()
            else:
                # Look for primary events before this event
                after_time = event.timestamp - time_window
                before_time = event.timestamp
                target_type = rule.primary_type.name.replace('EVENT_TYPE_', '').lower()

            # Query for potential correlated events
            related_events = self.storage.query_events(
                symbol=event.subject.symbol,
                event_type=target_type,
                after_timestamp=after_time,
                before_timestamp=before_time,
                limit=50
            )

            for related_event in related_events:
                if related_event['event_id'] == event.event_id:
                    continue

                # Calculate correlation score
                correlation_score = self._calculate_correlation_score(
                    event, related_event, rule
                )

                if correlation_score >= 0.3:  # Minimum threshold
                    # Calculate time lag
                    event_ts = event.timestamp
                    related_ts = self._parse_timestamp(related_event['timestamp'])
                    time_lag = int((related_ts - event_ts).total_seconds())

                    correlation = {
                        'primary_event_id': event.event_id if forward else related_event['event_id'],
                        'related_event_id': related_event['event_id'] if forward else event.event_id,
                        'correlation_type': rule.name,
                        'correlation_score': correlation_score,
                        'time_lag_seconds': abs(time_lag),
                        'rule_description': rule.description
                    }

                    correlations.append(correlation)

        except Exception as e:
            logger.error(f"❌ Error applying correlation rule {rule.name}: {e}")

        return correlations

    def _find_temporal_correlations(self, event: Event) -> List[Dict[str, Any]]:
        """Find simple temporal correlations (events close in time)"""
        correlations = []

        try:
            # Look for any events within 1 hour for the same symbol
            time_window = timedelta(hours=1)
            after_time = event.timestamp - time_window
            before_time = event.timestamp + time_window

            nearby_events = self.storage.query_events(
                symbol=event.subject.symbol,
                after_timestamp=after_time,
                before_timestamp=before_time,
                limit=20
            )

            for nearby_event in nearby_events:
                if nearby_event['event_id'] == event.event_id:
                    continue

                # Calculate temporal proximity score
                event_ts = event.timestamp
                nearby_ts = self._parse_timestamp(nearby_event['timestamp'])
                time_diff = abs((nearby_ts - event_ts).total_seconds())

                # Score based on time proximity (closer = higher score)
                if time_diff <= 300:  # Within 5 minutes
                    proximity_score = 0.8
                elif time_diff <= 900:  # Within 15 minutes
                    proximity_score = 0.6
                elif time_diff <= 1800:  # Within 30 minutes
                    proximity_score = 0.4
                else:
                    proximity_score = 0.2

                # Apply additional scoring factors
                correlation_score = self._calculate_temporal_score(event, nearby_event, proximity_score)

                if correlation_score >= 0.4:
                    correlation = {
                        'primary_event_id': event.event_id,
                        'related_event_id': nearby_event['event_id'],
                        'correlation_type': 'temporal_proximity',
                        'correlation_score': correlation_score,
                        'time_lag_seconds': int(time_diff),
                        'rule_description': 'Events occurring close in time for same symbol'
                    }

                    correlations.append(correlation)

        except Exception as e:
            logger.error(f"❌ Error finding temporal correlations: {e}")

        return correlations

    def _calculate_correlation_score(self, event: Event, related_event: Dict[str, Any],
                                   rule: CorrelationRule) -> float:
        """Calculate correlation score between two events"""
        score = rule.base_score

        try:
            # Time proximity bonus (closer in time = higher score)
            event_ts = event.timestamp
            related_ts = self._parse_timestamp(related_event['timestamp'])
            time_diff_minutes = abs((related_ts - event_ts).total_seconds()) / 60

            if time_diff_minutes <= 5:
                score += 0.2
            elif time_diff_minutes <= 15:
                score += 0.1
            elif time_diff_minutes <= 30:
                score += 0.05

            # Priority alignment bonus
            event_priority = event.metadata.priority.name if hasattr(event.metadata.priority, 'name') else str(event.metadata.priority)
            related_priority = related_event.get('priority', 'medium')

            if 'high' in event_priority.lower() and 'high' in related_priority.lower():
                score += 0.1
            elif 'critical' in event_priority.lower() or 'critical' in related_priority.lower():
                score += 0.15

            # Event-specific scoring
            if event.event_type == EventType.EVENT_TYPE_NEWS:
                score = self._apply_news_specific_scoring(event, score)
            elif event.event_type == EventType.EVENT_TYPE_EARNINGS:
                score = self._apply_earnings_specific_scoring(event, score)

            # Cap the score at 1.0
            return min(score, 1.0)

        except Exception as e:
            logger.warning(f"⚠️ Error calculating correlation score: {e}")
            return rule.base_score

    def _calculate_temporal_score(self, event: Event, nearby_event: Dict[str, Any],
                                 base_score: float) -> float:
        """Calculate temporal correlation score"""
        score = base_score

        try:
            # Same event type bonus
            event_type = event.event_type.name.replace('EVENT_TYPE_', '').lower()
            nearby_type = nearby_event.get('event_type', '')

            if event_type == nearby_type:
                score += 0.1

            # Different event type combinations
            elif (event_type == 'news' and nearby_type in ['technical_signal', 'volume_anomaly']):
                score += 0.2
            elif (event_type == 'earnings' and nearby_type in ['price_gap', 'volume_anomaly']):
                score += 0.2

            # Source diversity bonus (events from different sources are more significant)
            if event.source != nearby_event.get('source', ''):
                score += 0.05

            return min(score, 1.0)

        except Exception as e:
            logger.warning(f"⚠️ Error calculating temporal score: {e}")
            return base_score

    def _apply_news_specific_scoring(self, event: Event, base_score: float) -> float:
        """Apply news-specific correlation scoring"""
        if not event.news_data:
            return base_score

        score = base_score

        try:
            # Sentiment magnitude bonus
            if event.news_data.sentiment:
                sentiment_magnitude = abs(event.news_data.sentiment.overall)
                if sentiment_magnitude > 0.8:
                    score += 0.15
                elif sentiment_magnitude > 0.5:
                    score += 0.1
                elif sentiment_magnitude > 0.3:
                    score += 0.05

            # Importance bonus
            if event.news_data.importance > 0.7:
                score += 0.1
            elif event.news_data.importance > 0.5:
                score += 0.05

            # Publisher credibility (simple heuristic)
            publisher = event.news_data.publisher.lower()
            if any(trusted in publisher for trusted in ['reuters', 'bloomberg', 'wsj', 'cnbc']):
                score += 0.05

            # Headline keywords that suggest market impact
            headline = event.news_data.headline.lower()
            impact_keywords = ['earnings', 'profit', 'revenue', 'guidance', 'forecast',
                             'merger', 'acquisition', 'fda', 'approval', 'lawsuit']

            keyword_count = sum(1 for keyword in impact_keywords if keyword in headline)
            score += min(keyword_count * 0.02, 0.1)

        except Exception as e:
            logger.warning(f"⚠️ Error in news-specific scoring: {e}")

        return score

    def _apply_earnings_specific_scoring(self, event: Event, base_score: float) -> float:
        """Apply earnings-specific correlation scoring"""
        if not event.earnings_data:
            return base_score

        score = base_score

        try:
            # EPS surprise magnitude bonus
            if event.earnings_data.estimates and event.earnings_data.estimates.eps:
                eps = event.earnings_data.estimates.eps
                surprise_pct = abs(eps.surprise_percent)

                if surprise_pct > 20:
                    score += 0.2
                elif surprise_pct > 10:
                    score += 0.15
                elif surprise_pct > 5:
                    score += 0.1
                elif surprise_pct > 2:
                    score += 0.05

            # Revenue surprise bonus
            if event.earnings_data.estimates and event.earnings_data.estimates.revenue:
                revenue = event.earnings_data.estimates.revenue
                surprise_pct = abs(revenue.surprise_percent)

                if surprise_pct > 15:
                    score += 0.1
                elif surprise_pct > 5:
                    score += 0.05

        except Exception as e:
            logger.warning(f"⚠️ Error in earnings-specific scoring: {e}")

        return score

    def get_correlation_stats(self, symbol: str = None, hours: int = 24) -> Dict[str, Any]:
        """Get correlation statistics"""
        try:
            # This would query the event_correlations table
            # For now, return a placeholder
            cutoff_time = datetime.utcnow() - timedelta(hours=hours)

            # In a real implementation, we would query the database
            return {
                'total_correlations': 0,  # Placeholder
                'correlation_types': {},
                'average_score': 0.0,
                'symbol_filter': symbol,
                'time_window_hours': hours,
                'timestamp': datetime.utcnow().isoformat()
            }

        except Exception as e:
            logger.error(f"❌ Error getting correlation stats: {e}")
            return {'error': str(e)}

    def _parse_timestamp(self, timestamp_str: Any) -> datetime:
        """Parse timestamp string to datetime object"""
        if isinstance(timestamp_str, datetime):
            return timestamp_str

        if isinstance(timestamp_str, str):
            try:
                # Handle ISO format
                timestamp_str = timestamp_str.replace('Z', '+00:00')
                if '.' not in timestamp_str and '+' not in timestamp_str:
                    timestamp_str += '+00:00'
                return datetime.fromisoformat(timestamp_str)
            except:
                pass

        return datetime.utcnow()

# Utility functions for correlation analysis
def analyze_event_patterns(events: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Analyze patterns in a list of events"""
    if not events:
        return {'error': 'No events provided'}

    # Group by symbol
    by_symbol = {}
    for event in events:
        symbol = event.get('symbol', 'UNKNOWN')
        if symbol not in by_symbol:
            by_symbol[symbol] = []
        by_symbol[symbol].append(event)

    # Analyze patterns
    patterns = {
        'total_events': len(events),
        'unique_symbols': len(by_symbol),
        'events_by_symbol': {k: len(v) for k, v in by_symbol.items()},
        'event_types': {},
        'time_distribution': {}
    }

    # Count event types
    for event in events:
        event_type = event.get('event_type', 'unknown')
        patterns['event_types'][event_type] = patterns['event_types'].get(event_type, 0) + 1

    return patterns

# CLI interface for testing
if __name__ == "__main__":
    import sys

    # Configure logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    if len(sys.argv) > 1:
        command = sys.argv[1]

        if command == "test":
            # Test correlation engine
            from src.domains.analytics.events.database import EventStorage
            from src.domains.analytics.events.proto.events_pb2 import create_news_event

            storage = EventStorage()
            engine = CorrelationEngine(storage)

            # Create a test event
            test_event = create_news_event(
                headline="Apple beats earnings expectations",
                symbol="AAPL",
                sentiment=0.8
            )

            correlations = engine.find_correlations(test_event)
            print(f"Found correlations: {len(correlations)}")

            for correlation in correlations:
                print(f"  - {correlation['correlation_type']}: {correlation['correlation_score']:.2f}")

        else:
            print("Unknown command. Available: test")
    else:
        print("Event Correlation Engine")
        print("Usage: python correlation.py [test]")