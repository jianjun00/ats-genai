import gin

POLYGON_API_KEY = None

@gin.configurable
def set_polygon_api_key(polygon_api_key=None):
    global POLYGON_API_KEY
    POLYGON_API_KEY = polygon_api_key
