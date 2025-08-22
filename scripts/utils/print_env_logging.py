import sys
import gin

# Parse --gin_config if provided (before any Gin-configured imports)
from config.logging_config import LoggingConfig

for arg in sys.argv:
    if arg.startswith("--gin_config="):
        gin_config_path = arg.split("=", 1)[1]
        gin.clear_config()
        gin.parse_config_file(gin_config_path)

from config.environment import Environment

env = Environment()
print(env.logging_config.log_level)
print(env.logging_config.log_format)
