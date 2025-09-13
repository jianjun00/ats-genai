"""
Protobuf module initialization - Import dependencies in correct order
"""

# Import protobuf modules in dependency order to avoid descriptor pool issues
try:
    # Import basic dependencies first (no dependencies)
    pass

    # Import complex dependencies last (depends on above)

except ImportError as e:
    # Graceful fallback if protobuf dependencies are missing
    import warnings
    warnings.warn(f"Protobuf initialization failed: {e}. Some features may not work.", ImportWarning)
