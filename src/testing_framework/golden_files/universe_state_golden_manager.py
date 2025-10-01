"""
UniverseStateInterval Golden File Manager

Concrete implementation of golden file testing for UniverseStateInterval objects.
Handles complex nested business object serialization with deterministic output.
"""

from typing import Dict, Any, List, Optional
from pathlib import Path
from datetime import datetime
import logging

from .base_golden_test_framework import BaseGoldenFileManager, BaseGoldenTestCase
from domains.trading.services.state.universe_state import UniverseStateInterval
from domains.trading.services.state.instrument_interval import InstrumentInterval
from domains.trading.services.state.indicator_interval import IndicatorInterval
from domains.trading.services.state.factor_interval import FactorInterval


class UniverseStateGoldenFileManager(BaseGoldenFileManager[UniverseStateInterval]):
    """Golden file manager for UniverseStateInterval objects."""
    
    def __init__(self, test_dir: Path):
        super().__init__(test_dir, "universe_state_interval")
        
    def get_object_schema_version(self) -> str:
        return "1.0.0"
    
    def serialize_object(self, universe_state: UniverseStateInterval) -> Dict[str, Any]:
        """Convert UniverseStateInterval to deterministic dictionary."""
        return {
            "duration": {
                "duration_string": universe_state.duration.get_duration_string(),
                "duration_minutes": universe_state.duration.get_duration_minutes()
            },
            "start_timestamp_utc_micros": int(universe_state.start_date_time.timestamp() * 1_000_000),
            "end_timestamp_utc_micros": int(universe_state.end_date_time.timestamp() * 1_000_000),
            "universe_id": universe_state.universe_id,
            "factor_intervals": [
                self._serialize_factor_interval(factor) for factor in universe_state.factor_intervals
            ],
            "instrument_intervals": {
                str(inst_id): self._serialize_instrument_interval(interval)
                for inst_id, interval in sorted(universe_state.instrument_intervals.items())
            },
            "instrument_indicator_intervals": {
                indicator_type: {
                    str(inst_id): self._serialize_indicator_interval(interval)
                    for inst_id, interval in sorted(indicator_intervals.items())
                }
                for indicator_type, indicator_intervals in sorted(universe_state.instrument_indicator_intervals.items())
            },
            "instrument_forecast_intervals": {
                str(inst_id): self._serialize_forecast_interval(forecast)
                for inst_id, forecast in sorted(universe_state.instrument_forecast_intervals.items())
            }
        }
    
    def _serialize_instrument_interval(self, interval: InstrumentInterval) -> Dict[str, Any]:
        """Serialize InstrumentInterval to deterministic format."""
        return {
            "instrument_id": interval.instrument_id,
            "start_timestamp_utc_micros": int(interval.start_date_time.timestamp() * 1_000_000),
            "end_timestamp_utc_micros": int(interval.end_date_time.timestamp() * 1_000_000),
            "open_micro_dollars": int(round(interval.open * 1_000_000)),
            "high_micro_dollars": int(round(interval.high * 1_000_000)),
            "low_micro_dollars": int(round(interval.low * 1_000_000)),
            "close_micro_dollars": int(round(interval.close * 1_000_000)),
            "traded_volume": int(interval.traded_volume),
            "traded_dollar_micro": int(round(interval.traded_dollar * 1_000_000)),
            "status": interval.status,
            "market_cap_micro_dollars": int(round(interval.market_cap * 1_000_000)) if interval.market_cap else None
        }
    
    def _serialize_indicator_interval(self, interval: IndicatorInterval) -> Dict[str, Any]:
        """Serialize IndicatorInterval to deterministic format."""
        return {
            "instrument_id": interval.instrument_id,
            "start_timestamp_utc_micros": int(interval.start_date_time.timestamp() * 1_000_000),
            "end_timestamp_utc_micros": int(interval.end_date_time.timestamp() * 1_000_000),
            "indicators": {
                indicator_name: {
                    "value": indicator_data.get("value"),
                    "status": indicator_data.get("status"),
                    "update_timestamp_utc_micros": int(indicator_data["update_at"].timestamp() * 1_000_000) if indicator_data.get("update_at") else None
                }
                for indicator_name, indicator_data in sorted(interval.indicators.items())
            }
        }
    
    def _serialize_factor_interval(self, interval) -> Dict[str, Any]:
        """Serialize FactorInterval to deterministic format."""
        # Handle FactorInterval structure (may vary based on implementation)
        return {
            "start_timestamp_utc_micros": int(interval.start_date_time.timestamp() * 1_000_000) if hasattr(interval, 'start_date_time') else None,
            "end_timestamp_utc_micros": int(interval.end_date_time.timestamp() * 1_000_000) if hasattr(interval, 'end_date_time') else None,
            "factor_type": getattr(interval, 'factor_type', 'unknown'),
            "properties": dict(getattr(interval, 'properties', {}))
        }
    
    def _serialize_forecast_interval(self, interval) -> Dict[str, Any]:
        """Serialize ForecastInterval to deterministic format."""
        # Handle ForecastInterval structure (may vary based on implementation)
        return {
            "instrument_id": getattr(interval, 'instrument_id', 0),
            "start_timestamp_utc_micros": int(interval.start_date_time.timestamp() * 1_000_000) if hasattr(interval, 'start_date_time') else None,
            "end_timestamp_utc_micros": int(interval.end_date_time.timestamp() * 1_000_000) if hasattr(interval, 'end_date_time') else None,
            "forecasts": dict(getattr(interval, 'forecasts', {}))
        }
    
    def deserialize_object(self, data: Dict[str, Any]) -> UniverseStateInterval:
        """Convert golden file dictionary back to UniverseStateInterval."""
        # This is primarily for comparison purposes
        # We don't need full object reconstruction for golden file testing
        # The comparison happens at the serialized data level
        raise NotImplementedError("Deserialization not required for golden file testing")


class UniverseStateGoldenTestCase(BaseGoldenTestCase):
    """Test case for UniverseStateInterval golden file testing."""
    
    def __init__(self, test_dir: Path):
        super().__init__(test_dir)
        self._golden_manager = UniverseStateGoldenFileManager(test_dir)
        
    def get_golden_file_manager(self) -> UniverseStateGoldenFileManager:
        return self._golden_manager
        
    def run_universe_state_golden_test(self, test_method_name: str, universe_state: UniverseStateInterval,
                                     symbols: List[str], timeframe: str, start_time: datetime, end_time: datetime,
                                     update_golden: bool = False, **test_metadata) -> bool:
        """Run golden file test specifically for UniverseStateInterval."""
        from .base_golden_test_framework import TestDataHelper
        
        metadata = TestDataHelper.create_test_metadata(
            symbols=symbols,
            timeframe=timeframe, 
            start_time=start_time,
            end_time=end_time,
            universe_id=universe_state.universe_id,
            duration_string=universe_state.duration.get_duration_string(),
            instrument_count=len(universe_state.instrument_intervals),
            indicator_types=list(universe_state.instrument_indicator_intervals.keys()),
            **test_metadata
        )
        
        return self.run_golden_test(test_method_name, universe_state, update_golden, metadata)


class UniverseStateCollectionGoldenManager:
    """Manager for testing collections of UniverseStateInterval objects (rolling windows, etc.)."""
    
    def __init__(self, test_dir: Path):
        self.test_dir = test_dir
        self.single_manager = UniverseStateGoldenFileManager(test_dir)
        self.logger = logging.getLogger(f"{__name__}.collection")
        
    def save_collection_golden_file(self, test_method_name: str, 
                                  universe_states: List[UniverseStateInterval],
                                  collection_metadata: Dict[str, Any]):
        """Save collection of universe states as golden file."""
        golden_dir = self.test_dir / "golden_files" / "universe_state_collection"
        golden_dir.mkdir(parents=True, exist_ok=True)
        golden_path = golden_dir / f"{test_method_name}.json"
        
        # Serialize collection
        collection_data = {
            "schema_version": "1.0.0",
            "test_name": test_method_name,
            "generated_at_utc_micros": int(datetime.utcnow().timestamp() * 1_000_000),
            "total_intervals": len(universe_states),
            "collection_metadata": collection_metadata,
            "universe_states": [
                {
                    "index": i,
                    "universe_state": self.single_manager.serialize_object(state)
                }
                for i, state in enumerate(universe_states)
            ]
        }
        
        # Save with deterministic formatting
        import json
        with open(golden_path, 'w') as f:
            json.dump(collection_data, f, indent=2, sort_keys=True, separators=(',', ': '))
            
        self.logger.info(f"✅ Saved collection golden file: {golden_path} ({len(universe_states)} states)")
        
    def load_and_compare_collection(self, test_method_name: str, 
                                  actual_states: List[UniverseStateInterval]) -> List[str]:
        """Load golden collection and compare with actual states."""
        golden_dir = self.test_dir / "golden_files" / "universe_state_collection"
        golden_path = golden_dir / f"{test_method_name}.json"
        
        if not golden_path.exists():
            return [f"Golden file not found: {golden_path}"]
            
        import json
        with open(golden_path, 'r') as f:
            golden_data = json.load(f)
            
        expected_states = golden_data["universe_states"]
        
        if len(actual_states) != len(expected_states):
            return [f"Collection length mismatch: actual={len(actual_states)}, expected={len(expected_states)}"]
            
        differences = []
        for i, (actual_state, expected_state_data) in enumerate(zip(actual_states, expected_states)):
            actual_serialized = self.single_manager.serialize_object(actual_state)
            expected_serialized = expected_state_data["universe_state"]
            
            state_diffs = self.single_manager._deep_compare_dicts(
                actual_serialized, expected_serialized, f"state[{i}]"
            )
            differences.extend(state_diffs)
            
        return differences