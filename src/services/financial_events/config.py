"""
Configuration for xAI Financial Event Extractor
"""

import os
from typing import List, Dict, Any
from dataclasses import dataclass

@dataclass
class XAIConfig:
    """xAI API Configuration"""
    api_key: str
    base_url: str = "https://api.x.ai/v1"
    model: str = "grok-4"
    max_retries: int = 3
    timeout: int = 30
    rate_limit_delay: float = 0.5  # seconds between calls

@dataclass
class ExtractionConfig:
    """Event extraction configuration"""
    max_events_per_call: int = 50
    max_search_results: int = 50
    temperature: float = 0.1
    max_tokens: int = 4000
    default_symbols: List[str] = None

    def __post_init__(self):
        if self.default_symbols is None:
            self.default_symbols = [
                # Major tech stocks
                "AAPL", "MSFT", "GOOGL", "AMZN", "TSLA", "NVDA", "META",
                # Major banks
                "JPM", "BAC", "WFC", "GS", "MS",
                # Major indices ETFs
                "SPY", "QQQ", "IWM", "VTI",
                # Key economic sectors
                "XLF", "XLE", "XLK", "XLV", "XLI"
            ]

@dataclass
class CostOptimizationConfig:
    """Cost optimization settings"""
    use_cached_prompts: bool = True
    batch_size_days: int = 7  # Weekly batching
    max_symbols_per_call: int = 20
    enable_smart_chunking: bool = True

def load_config() -> tuple[XAIConfig, ExtractionConfig, CostOptimizationConfig]:
    """Load configuration from environment variables"""

    # xAI API config
    xai_config = XAIConfig(
        api_key=os.getenv("XAI_API_KEY", ""),
        base_url=os.getenv("XAI_BASE_URL", "https://api.x.ai/v1"),
        model=os.getenv("XAI_MODEL", "grok-4")
    )

    if not xai_config.api_key:
        raise ValueError("XAI_API_KEY environment variable is required")

    # Extraction config
    extraction_config = ExtractionConfig(
        max_events_per_call=int(os.getenv("MAX_EVENTS_PER_CALL", "50")),
        temperature=float(os.getenv("EXTRACTION_TEMPERATURE", "0.1"))
    )

    # Optimization config
    optimization_config = CostOptimizationConfig(
        batch_size_days=int(os.getenv("BATCH_SIZE_DAYS", "7")),
        max_symbols_per_call=int(os.getenv("MAX_SYMBOLS_PER_CALL", "20"))
    )

    return xai_config, extraction_config, optimization_config

# Event type priorities for filtering
EVENT_PRIORITIES = {
    "earnings": 10,           # Highest priority
    "fed_announcement": 10,
    "economic_indicator": 9,
    "m_a": 8,
    "analyst_rating": 7,
    "stock_event": 6,
    "corporate": 5            # Lowest priority
}

# Sample symbols by market cap for testing
SAMPLE_SYMBOLS = {
    "mega_cap": ["AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", "TSLA", "META"],
    "large_cap": ["V", "JNJ", "WMT", "PG", "JPM", "UNH", "MA", "HD"],
    "etfs": ["SPY", "QQQ", "IWM", "VTI", "VEA", "VWO", "AGG", "LQD"],
    "sectors": ["XLF", "XLE", "XLK", "XLV", "XLI", "XLU", "XLY", "XLP"]
}

# Cost estimates (per million tokens)
COST_STRUCTURE = {
    "input_tokens": 3.00,      # $3.00 per 1M input tokens
    "cached_input": 0.75,      # $0.75 per 1M cached tokens (75% savings)
    "output_tokens": 15.00,    # $15.00 per 1M output tokens
    "live_search": 0.025       # $0.025 per search source
}