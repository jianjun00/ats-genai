import gin
import os

EODHD_API_KEY = None

@gin.configurable
def set_eodhd_api_key(eodhd_api_key=None):
    global EODHD_API_KEY
    # Use provided key, or fallback to environment variable
    EODHD_API_KEY = eodhd_api_key or os.getenv('EODHD_API_KEY')