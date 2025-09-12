"""Universe State Modules Package"""

# Import the UniverseStateInterval class directly from the .py file in parent directory
import sys
import os

# Add parent directory to path to access universe_state.py
parent_dir = os.path.join(os.path.dirname(__file__), '..')
if parent_dir not in sys.path:
    sys.path.insert(0, parent_dir)

# Import directly from the universe_state.py file (not this package)
import importlib.util
universe_state_file = os.path.join(parent_dir, 'universe_state.py')
spec = importlib.util.spec_from_file_location("universe_state_module", universe_state_file)
universe_state_module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(universe_state_module)

# Get the dataclass from the module
UniverseStateInterval = universe_state_module.UniverseStateInterval