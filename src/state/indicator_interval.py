from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional, Any


@dataclass
class IndicatorInterval:
    """
    Represents computed indicator values for a specific instrument and time period.
    Contains a dictionary mapping indicator names to their computed values and status.
    """
    instrument_id: int
    start_date_time: datetime
    end_date_time: datetime
    indicators: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    
    def add_indicator(self, name: str, value: Optional[float], status: str, update_at: Optional[datetime] = None):
        """
        Add an indicator result to this interval.
        
        Args:
            name: Name of the indicator (e.g., 'PL', 'OneOneLow')
            value: Computed indicator value (None if invalid)
            status: Status of the indicator ('ok' or 'invalid')
            update_at: When the indicator was computed
        """
        self.indicators[name] = {
            'value': value,
            'status': status,
            'update_at': update_at or datetime.now()
        }
    
    def get_indicator_value(self, name: str) -> Optional[float]:
        """Get the value of a specific indicator."""
        indicator_data = self.indicators.get(name)
        return indicator_data['value'] if indicator_data else None
    
    def get_indicator_status(self, name: str) -> Optional[str]:
        """Get the status of a specific indicator."""
        indicator_data = self.indicators.get(name)
        return indicator_data['status'] if indicator_data else None
    
    def has_indicator(self, name: str) -> bool:
        """Check if this interval has a specific indicator computed."""
        return name in self.indicators
    
    def get_indicator_names(self) -> list:
        """Get list of all computed indicator names."""
        return list(self.indicators.keys())
    
    def is_indicator_valid(self, name: str) -> bool:
        """Check if a specific indicator is valid (status == 'ok')."""
        return self.get_indicator_status(name) == 'ok'
