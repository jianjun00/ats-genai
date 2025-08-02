import gin

@gin.configurable
class LoggingConfig:
    def __init__(self, level="INFO", format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"):
        self.level = level
        self.format = format
