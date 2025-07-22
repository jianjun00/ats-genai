from typing import List, Optional, Dict, Set
from datetime import datetime, date
from dataclasses import dataclass


@dataclass
class Universe:
    """
    Manages the list of instrument_ids that are active in the universe for a given date.
    The instrument_ids can change as the current date advances (e.g., due to additions/removals 
    from indices, delistings, new listings, etc.).
    """
    current_date: date
    instrument_ids: List[int]
    
    def __post_init__(self):
        # Ensure instrument_ids is a list and remove duplicates while preserving order
        if self.instrument_ids is None:
            self.instrument_ids = []
        elif isinstance(self.instrument_ids, set):
            self.instrument_ids = list(self.instrument_ids)
        elif self.instrument_ids:
            # Remove duplicates while preserving order
            seen = set()
            unique_ids = []
            for id_ in self.instrument_ids:
                if id_ not in seen:
                    seen.add(id_)
                    unique_ids.append(id_)
            self.instrument_ids = unique_ids
    
    def update_date(self, new_date: date, new_instrument_ids: Optional[List[int]] = None):
        """
        Update the universe to a new date, optionally updating the instrument list.
        
        Args:
            new_date: The new date for the universe
            new_instrument_ids: Optional new list of instrument IDs. If None, keeps current instruments.
        """
        self.current_date = new_date
        if new_instrument_ids is not None:
            self.instrument_ids = new_instrument_ids
            self.__post_init__()  # Remove duplicates
    
    def add_instrument(self, instrument_id: int):
        """Add an instrument to the universe if it's not already present."""
        if instrument_id not in self.instrument_ids:
            self.instrument_ids.append(instrument_id)
    
    def remove_instrument(self, instrument_id: int):
        """Remove an instrument from the universe if it exists."""
        if instrument_id in self.instrument_ids:
            self.instrument_ids.remove(instrument_id)
    
    def has_instrument(self, instrument_id: int) -> bool:
        """Check if an instrument is in the universe."""
        return instrument_id in self.instrument_ids
    
    def get_instrument_count(self) -> int:
        """Get the number of instruments in the universe."""
        return len(self.instrument_ids)
    
    def copy(self) -> 'Universe':
        """Create a copy of the universe."""
        return Universe(
            current_date=self.current_date,
            instrument_ids=self.instrument_ids.copy()
        )
    
    def __len__(self) -> int:
        return len(self.instrument_ids)
    
    def __contains__(self, instrument_id: int) -> bool:
        return instrument_id in self.instrument_ids
    
    def __iter__(self):
        return iter(self.instrument_ids)
