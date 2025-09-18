"""
Training Dataset Integration - Connect events to training data pipeline
"""

import json
import logging
import numpy as np
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Any, Iterator
from pathlib import Path

from src.domains.analytics.events.proto.events_pb2 import Event, EventType, MessageToDict
from src.domains.analytics.events.database import EventStorage

logger = logging.getLogger(__name__)

class TrainingDatasetEventWriter:
    """Writer for integrating events into training datasets"""

    def __init__(self, storage_path: str = "/mnt/d/ats-data/training"):
        """
        Initialize training dataset event writer

        Args:
            storage_path: Base path for training data storage
        """
        self.storage_path = Path(storage_path)
        self.storage_path.mkdir(parents=True, exist_ok=True)

        # Create events subdirectory
        self.events_path = self.storage_path / "events"
        self.events_path.mkdir(exist_ok=True)

        logger.info(f"✅ Training dataset event writer initialized: {self.storage_path}")

    def append_event_to_training_data(self, event: Event, run_id: Optional[str] = None) -> bool:
        """
        Append event to training dataset files

        Args:
            event: Event to append
            run_id: Training run ID (optional)

        Returns:
            bool: Success status
        """
        try:
            # Determine run directory
            if run_id:
                run_dir = self.events_path / run_id
            else:
                # Use current date as run ID
                run_id = datetime.utcnow().strftime("%Y%m%d")
                run_dir = self.events_path / run_id

            run_dir.mkdir(exist_ok=True)

            # Convert event to dict for JSON serialization
            event_dict = MessageToDict(event, preserving_proto_field_name=True)

            # Append to daily event file
            event_file = run_dir / f"events_{datetime.utcnow().strftime('%Y%m%d')}.jsonl"

            with open(event_file, 'a') as f:
                json.dump(event_dict, f)
                f.write('\n')

            # Also create symbol-specific file if symbol exists
            if event.subject.symbol:
                symbol_dir = run_dir / "by_symbol"
                symbol_dir.mkdir(exist_ok=True)

                symbol_file = symbol_dir / f"{event.subject.symbol}_{datetime.utcnow().strftime('%Y%m%d')}.jsonl"

                with open(symbol_file, 'a') as f:
                    json.dump(event_dict, f)
                    f.write('\n')

            logger.debug(f"📝 Appended event {event.event_id} to training data")
            return True

        except Exception as e:
            logger.error(f"❌ Failed to append event to training data: {e}")
            return False

    def create_event_features_array(self, events: List[Event]) -> np.ndarray:
        """
        Create numpy array of event features for training

        Args:
            events: List of events to convert

        Returns:
            np.ndarray: Feature array
        """
        if not events:
            return np.array([])

        features = []

        for event in events:
            feature_vector = self._extract_event_features(event)
            features.append(feature_vector)

        return np.array(features, dtype=np.float32)

    def _extract_event_features(self, event: Event) -> List[float]:
        """Extract numerical features from event for ML training"""
        features = []

        # Basic event features
        features.append(float(event.event_type))  # Event type as number
        features.append(event.confidence)
        features.append(float(event.metadata.priority))

        # Temporal features
        timestamp = event.timestamp
        features.append(timestamp.hour)  # Hour of day
        features.append(timestamp.weekday())  # Day of week
        features.append(timestamp.day)  # Day of month

        # Event-specific features based on type
        if event.event_type == EventType.EVENT_TYPE_NEWS and event.news_data:
            # News-specific features
            features.extend(self._extract_news_features(event.news_data))
        elif event.event_type == EventType.EVENT_TYPE_EARNINGS and event.earnings_data:
            # Earnings-specific features
            features.extend(self._extract_earnings_features(event.earnings_data))
        elif event.event_type == EventType.EVENT_TYPE_TECHNICAL_SIGNAL and event.technical_data:
            # Technical signal features
            features.extend(self._extract_technical_features(event.technical_data))
        else:
            # Pad with zeros for other event types
            features.extend([0.0] * 10)  # Standard padding

        return features

    def _extract_news_features(self, news_data) -> List[float]:
        """Extract features from news event data"""
        features = []

        # Sentiment features
        if news_data.sentiment:
            features.append(news_data.sentiment.overall)
            features.append(news_data.sentiment.confidence)
        else:
            features.extend([0.0, 0.0])

        # Importance and text length features
        features.append(news_data.importance)
        features.append(len(news_data.headline) / 100.0)  # Normalized headline length
        features.append(len(news_data.categories))  # Number of categories

        # Publisher features (simple encoding)
        publisher_score = self._encode_publisher(news_data.publisher)
        features.append(publisher_score)

        # Market impact features
        if news_data.market_impact:
            features.append(float(news_data.market_impact.expected))
            features.append(float(news_data.market_impact.magnitude))
            features.append(float(news_data.market_impact.time_horizon))
        else:
            features.extend([0.0, 0.0, 0.0])

        # Pad to 10 features
        while len(features) < 10:
            features.append(0.0)

        return features[:10]

    def _extract_earnings_features(self, earnings_data) -> List[float]:
        """Extract features from earnings event data"""
        features = []

        # Basic earnings info
        features.append(earnings_data.year - 2020)  # Normalized year
        features.append(earnings_data.quarter if earnings_data.quarter else 0)

        # EPS features
        if earnings_data.estimates and earnings_data.estimates.eps:
            eps = earnings_data.estimates.eps
            features.append(eps.actual)
            features.append(eps.consensus)
            features.append(eps.surprise)
            features.append(eps.surprise_percent / 100.0)  # Normalized
        else:
            features.extend([0.0, 0.0, 0.0, 0.0])

        # Revenue features
        if earnings_data.estimates and earnings_data.estimates.revenue:
            revenue = earnings_data.estimates.revenue
            features.append(revenue.surprise_percent / 100.0)  # Normalized
        else:
            features.append(0.0)

        # Report type encoding
        report_type_score = 1.0 if earnings_data.report_type == "official" else 0.5
        features.append(report_type_score)

        # Pad to 10 features
        while len(features) < 10:
            features.append(0.0)

        return features[:10]

    def _extract_technical_features(self, technical_data) -> List[float]:
        """Extract features from technical signal data"""
        features = []

        # Signal type and direction
        features.append(float(technical_data.signal_type))

        if technical_data.signal:
            features.append(float(technical_data.signal.direction))
            features.append(technical_data.signal.strength)
            features.append(technical_data.signal.confidence)
        else:
            features.extend([0.0, 0.0, 0.0])

        # Price context
        if technical_data.price_context:
            pc = technical_data.price_context
            features.append(pc.current_price / 1000.0)  # Normalized price
            features.append(pc.volume / 1000000.0 if pc.volume else 0.0)  # Normalized volume
        else:
            features.extend([0.0, 0.0])

        # Indicator encoding
        indicator_score = self._encode_indicator(technical_data.indicator)
        features.append(indicator_score)

        # Timeframe encoding
        timeframe_score = self._encode_timeframe(technical_data.timeframe)
        features.append(timeframe_score)

        # Pad to 10 features
        while len(features) < 10:
            features.append(0.0)

        return features[:10]

    def _encode_publisher(self, publisher: str) -> float:
        """Simple publisher encoding"""
        publisher = publisher.lower()
        if any(trusted in publisher for trusted in ['reuters', 'bloomberg', 'wsj']):
            return 1.0
        elif any(major in publisher for major in ['cnbc', 'marketwatch', 'yahoo']):
            return 0.7
        else:
            return 0.3

    def _encode_indicator(self, indicator: str) -> float:
        """Simple technical indicator encoding"""
        indicator_map = {
            'rsi': 0.1,
            'macd': 0.2,
            'sma': 0.3,
            'ema': 0.4,
            'bollinger': 0.5,
            'stochastic': 0.6
        }

        indicator_lower = indicator.lower()
        for key, value in indicator_map.items():
            if key in indicator_lower:
                return value
        return 0.0

    def _encode_timeframe(self, timeframe: str) -> float:
        """Simple timeframe encoding"""
        timeframe_map = {
            '1m': 0.1,
            '5m': 0.2,
            '15m': 0.3,
            '1h': 0.4,
            '4h': 0.5,
            '1d': 0.6
        }
        return timeframe_map.get(timeframe.lower(), 0.0)

    def save_events_as_numpy(self, events: List[Event], filename: str,
                            run_id: Optional[str] = None) -> bool:
        """
        Save events as numpy array file

        Args:
            events: Events to save
            filename: Output filename
            run_id: Training run ID

        Returns:
            bool: Success status
        """
        try:
            if not events:
                logger.warning("No events to save")
                return False

            # Convert events to feature array
            feature_array = self.create_event_features_array(events)

            # Determine output path
            if run_id:
                output_dir = self.events_path / run_id
            else:
                output_dir = self.events_path / datetime.utcnow().strftime("%Y%m%d")

            output_dir.mkdir(exist_ok=True)
            output_path = output_dir / f"{filename}.npy"

            # Save as numpy array
            np.save(output_path, feature_array)

            logger.info(f"💾 Saved {len(events)} events to {output_path}")
            return True

        except Exception as e:
            logger.error(f"❌ Failed to save events as numpy: {e}")
            return False

    def load_events_from_training_run(self, run_id: str) -> Iterator[Dict[str, Any]]:
        """
        Load events from a specific training run

        Args:
            run_id: Training run identifier

        Yields:
            Dict: Event data
        """
        run_dir = self.events_path / run_id

        if not run_dir.exists():
            logger.warning(f"Training run directory not found: {run_dir}")
            return

        # Load from daily event files
        for event_file in run_dir.glob("events_*.jsonl"):
            try:
                with open(event_file, 'r') as f:
                    for line in f:
                        if line.strip():
                            event_data = json.loads(line)
                            yield event_data
            except Exception as e:
                logger.error(f"❌ Error reading event file {event_file}: {e}")

    def get_event_summary_for_training(self, symbol: str, days_back: int = 30) -> Dict[str, Any]:
        """
        Get event summary for training data preparation

        Args:
            symbol: Stock symbol
            days_back: Days to look back

        Returns:
            Dict with event summary
        """
        try:
            cutoff_date = datetime.utcnow() - timedelta(days=days_back)

            # Query events from storage
            event_storage = EventStorage()
            events = event_storage.query_events(
                symbol=symbol,
                after_timestamp=cutoff_date,
                limit=10000
            )

            # Analyze events
            summary = {
                'symbol': symbol,
                'total_events': len(events),
                'date_range': {
                    'from': cutoff_date.isoformat(),
                    'to': datetime.utcnow().isoformat()
                },
                'events_by_type': {},
                'events_by_source': {},
                'high_impact_events': 0,
                'average_confidence': 0.0
            }

            if not events:
                return summary

            # Count by type and source
            total_confidence = 0
            for event in events:
                event_type = event.get('event_type', 'unknown')
                summary['events_by_type'][event_type] = summary['events_by_type'].get(event_type, 0) + 1

                source = event.get('source', 'unknown')
                summary['events_by_source'][source] = summary['events_by_source'].get(source, 0) + 1

                confidence = float(event.get('confidence', 0))
                total_confidence += confidence

                # Count high-impact events (high confidence + high priority)
                priority = event.get('priority', 'low')
                if confidence > 0.7 or priority in ['high', 'critical']:
                    summary['high_impact_events'] += 1

            summary['average_confidence'] = total_confidence / len(events)

            return summary

        except Exception as e:
            logger.error(f"❌ Error getting event summary for {symbol}: {e}")
            return {'error': str(e)}

    def create_event_timeline(self, symbol: str, start_date: datetime,
                             end_date: datetime) -> List[Dict[str, Any]]:
        """
        Create event timeline for a symbol within date range

        Args:
            symbol: Stock symbol
            start_date: Start date
            end_date: End date

        Returns:
            List of events in chronological order
        """
        try:
            event_storage = EventStorage()
            events = event_storage.query_events(
                symbol=symbol,
                after_timestamp=start_date,
                before_timestamp=end_date,
                limit=5000
            )

            # Sort by timestamp
            timeline = sorted(events, key=lambda x: x['timestamp'])

            logger.info(f"📈 Created timeline for {symbol}: {len(timeline)} events")
            return timeline

        except Exception as e:
            logger.error(f"❌ Error creating event timeline for {symbol}: {e}")
            return []

class EventBasedFeatureExtractor:
    """Extract features from events for ML training"""

    def __init__(self, event_storage: EventStorage):
        self.storage = event_storage

    def extract_features_for_symbol_period(self, symbol: str, start_date: datetime,
                                         end_date: datetime) -> Dict[str, np.ndarray]:
        """
        Extract event-based features for a symbol and time period

        Args:
            symbol: Stock symbol
            start_date: Start date
            end_date: End date

        Returns:
            Dict with feature arrays
        """
        try:
            # Get all events for symbol in period
            events = self.storage.query_events(
                symbol=symbol,
                after_timestamp=start_date,
                before_timestamp=end_date,
                limit=10000
            )

            if not events:
                return {}

            # Extract different types of features
            features = {
                'news_sentiment': self._extract_news_sentiment_series(events, start_date, end_date),
                'earnings_surprises': self._extract_earnings_surprise_series(events, start_date, end_date),
                'technical_signals': self._extract_technical_signal_series(events, start_date, end_date),
                'event_frequency': self._extract_event_frequency_series(events, start_date, end_date),
                'event_importance': self._extract_importance_series(events, start_date, end_date)
            }

            return features

        except Exception as e:
            logger.error(f"❌ Error extracting features for {symbol}: {e}")
            return {}

    def _extract_news_sentiment_series(self, events: List[Dict], start_date: datetime,
                                     end_date: datetime) -> np.ndarray:
        """Extract news sentiment time series"""
        # This is a simplified implementation
        # In practice, you'd want to create a proper time series with regular intervals
        news_events = [e for e in events if e['event_type'] == 'news']

        if not news_events:
            return np.array([])

        sentiments = []
        for event in news_events:
            event_data = event.get('event_data', {})
            news_data = event_data.get('news_data', {})
            sentiment_data = news_data.get('sentiment', {})

            sentiment = sentiment_data.get('overall', 0.0)
            sentiments.append(sentiment)

        return np.array(sentiments)

    def _extract_earnings_surprise_series(self, events: List[Dict], start_date: datetime,
                                        end_date: datetime) -> np.ndarray:
        """Extract earnings surprise time series"""
        earnings_events = [e for e in events if e['event_type'] == 'earnings']

        if not earnings_events:
            return np.array([])

        surprises = []
        for event in earnings_events:
            event_data = event.get('event_data', {})
            earnings_data = event_data.get('earnings_data', {})
            estimates = earnings_data.get('estimates', {})
            eps = estimates.get('eps', {})

            surprise_pct = eps.get('surprise_percent', 0.0)
            surprises.append(surprise_pct)

        return np.array(surprises)

    def _extract_technical_signal_series(self, events: List[Dict], start_date: datetime,
                                       end_date: datetime) -> np.ndarray:
        """Extract technical signal strength series"""
        tech_events = [e for e in events if e['event_type'] == 'technical_signal']

        if not tech_events:
            return np.array([])

        signals = []
        for event in tech_events:
            event_data = event.get('event_data', {})
            tech_data = event_data.get('technical_data', {})
            signal = tech_data.get('signal', {})

            strength = signal.get('strength', 0.0)
            direction = signal.get('direction', 'SIGNAL_DIRECTION_NEUTRAL')

            # Convert to signed strength
            if 'BULLISH' in direction:
                signals.append(strength)
            elif 'BEARISH' in direction:
                signals.append(-strength)
            else:
                signals.append(0.0)

        return np.array(signals)

    def _extract_event_frequency_series(self, events: List[Dict], start_date: datetime,
                                      end_date: datetime) -> np.ndarray:
        """Extract event frequency by day"""
        # Group events by day
        from collections import defaultdict

        events_by_day = defaultdict(int)
        for event in events:
            event_date = event['timestamp'].date()
            events_by_day[event_date] += 1

        # Create array of daily counts
        current_date = start_date.date()
        end_date_only = end_date.date()
        daily_counts = []

        while current_date <= end_date_only:
            count = events_by_day.get(current_date, 0)
            daily_counts.append(count)
            current_date += timedelta(days=1)

        return np.array(daily_counts)

    def _extract_importance_series(self, events: List[Dict], start_date: datetime,
                                 end_date: datetime) -> np.ndarray:
        """Extract event importance scores"""
        importance_scores = []

        for event in events:
            confidence = float(event.get('confidence', 0))
            priority = event.get('priority', 'medium')

            # Combine confidence and priority into importance score
            priority_weight = {'low': 0.2, 'medium': 0.5, 'high': 0.8, 'critical': 1.0}
            priority_score = priority_weight.get(priority, 0.5)

            importance = (confidence + priority_score) / 2.0
            importance_scores.append(importance)

        return np.array(importance_scores)

# CLI interface for testing
if __name__ == "__main__":
    import sys

    # Configure logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    writer = TrainingDatasetEventWriter()

    if len(sys.argv) > 1:
        command = sys.argv[1]

        if command == "summary" and len(sys.argv) > 2:
            symbol = sys.argv[2]
            summary = writer.get_event_summary_for_training(symbol)
            print(f"Event summary for {symbol}: {json.dumps(summary, indent=2)}")

        else:
            print("Unknown command. Available: summary <SYMBOL>")
    else:
        print("Training Dataset Integration")
        print("Usage: python training_integration.py [summary SYMBOL]")