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
    
    def initialize(self):
        """Initialize the universe manager."""
        pass
    
    def cleanup(self):
        """Clean up resources."""
        pass