"""Universe State Modules Package"""

# Import the UniverseStateInterval class from the universe_state module in the parent directory
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

try:
    from universe_state import UniverseStateInterval
except ImportError:
    # Fallback - create a minimal placeholder
    class UniverseStateInterval:
        def __init__(self, *args, **kwargs):
            pass