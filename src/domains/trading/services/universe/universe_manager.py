"""
UniverseManager class for training data generation with proper database lookup.
Replaces hardcoded mappings with dynamic instrument resolution.
"""

from core.platform.config.environment import Environment
from core.dao.instruments.instrument_xrefs_dao import InstrumentXrefsDAO


class UniverseManager:
    """UniverseManager with proper database lookup for training data generation."""

    def __init__(self, env: Environment, universe_id: int = None, symbols: list = None):
        self.env = env
        self.universe_id = universe_id
        self.symbols = symbols or ['TSLA']  # Default to TSLA for backward compatibility
        self._instrument_ids = None  # Will be populated dynamically
        self._instrument_xrefs_dao = InstrumentXrefsDAO(env)
    
    @property
    def instrument_ids(self):
        """Return instrument IDs for the universe."""
        if self._instrument_ids is None:
            raise RuntimeError("UniverseManager not initialized. Call initialize() first.")
        return self._instrument_ids

    async def get_symbols(self, universe_id: int = None):
        """Get symbols for the universe - returns configured symbols."""
        return self.symbols

    async def initialize(self):
        """Initialize the universe manager with proper database lookups."""
        print(f"[UniverseManager] Initializing with symbols: {self.symbols}")
        
        # Resolve symbols to instrument_ids using database lookup
        instrument_ids = []
        for symbol in self.symbols:
            instrument_id = await self._instrument_xrefs_dao.resolve_instrument_id_by_symbol(symbol)
            if instrument_id:
                instrument_ids.append(instrument_id)
                print(f"[UniverseManager] Resolved {symbol} → instrument_id {instrument_id}")
            else:
                print(f"[UniverseManager] ⚠️  WARNING: Could not resolve symbol {symbol} to instrument_id")
        
        if not instrument_ids:
            raise ValueError(f"No valid instrument_ids found for symbols: {self.symbols}")
        
        self._instrument_ids = instrument_ids
        print(f"[UniverseManager] ✅ Initialized with instrument_ids: {self._instrument_ids}")

    def cleanup(self):
        """Clean up resources."""
