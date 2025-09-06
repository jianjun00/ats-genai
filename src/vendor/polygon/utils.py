import gin
import os

POLYGON_API_KEY = None

@gin.configurable
def set_polygon_api_key(polygon_api_key=None):
    global POLYGON_API_KEY
    # Use provided key, or fallback to environment variable
    POLYGON_API_KEY = polygon_api_key or os.getenv('POLYGON_API_KEY')
