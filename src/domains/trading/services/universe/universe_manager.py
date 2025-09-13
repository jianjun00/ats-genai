"""
Stub UniverseManager class for training data generation.
This is a minimal implementation to resolve import issues.
"""

from shared.utils.environment import Environment


class UniverseManager:
    """Minimal UniverseManager stub for training data generation."""

    def __init__(self, env: Environment, universe_id: int = None):
        self.env = env
        self.universe_id = universe_id
        # For training data generation, hardcode TSLA instrument_id mapping
        # TODO: Replace with proper universe/instrument resolution
        self._instrument_ids = [9034]  # TSLA instrument_id from database

    @property
    def instrument_ids(self):
        """Return instrument IDs for the universe."""
        return self._instrument_ids

    async def get_symbols(self, universe_id: int = None):
        """Get symbols for the universe - minimal implementation for training data."""
        return ['TSLA']  # Hardcode TSLA for training data generation
    def initialize(self):
        """Initialize the universe manager."""

    def cleanup(self):
        """Clean up resources."""
