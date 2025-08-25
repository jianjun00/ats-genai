import gin
import os

TIINGO_API_KEY = None

@gin.configurable
def set_tiingo_api_key(tiingo_api_key=None):
    global TIINGO_API_KEY
    # Use provided key, or fallback to environment variable
    TIINGO_API_KEY = tiingo_api_key or os.getenv('TIINGO_API_KEY')