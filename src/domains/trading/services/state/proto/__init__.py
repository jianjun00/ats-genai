"""
Protobuf module initialization - Import dependencies in correct order
"""

# Import protobuf modules in dependency order to avoid descriptor pool issues
try:
    # Import basic dependencies first (no dependencies)
    from . import time_duration_pb2
    from . import factor_interval_pb2
    from . import indicator_interval_pb2
    from . import instrument_interval_pb2
    
    # Import complex dependencies last (depends on above)
    from . import universe_state_interval_pb2
    
except ImportError as e:
    # Graceful fallback if protobuf dependencies are missing
    import warnings
    warnings.warn(f"Protobuf initialization failed: {e}. Some features may not work.", ImportWarning)
