import gin

from dataclasses import dataclass

@gin.configurable
@dataclass
class LoggingConfig:
    log_level: str = "INFO"
    log_format: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
