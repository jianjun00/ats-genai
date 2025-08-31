#!/usr/bin/env python3
"""
LLM-Based Analysis for Financial Events

Advanced event analysis using Large Language Models with self-reflection,
contextual understanding, and adaptive model selection.

Key Components:
- LLMEventAnalyzer: Core event analysis with reflection
- AdaptiveModelSelector: Automatic model selection based on complexity
- EventAnalysisRequest/Result: Data structures for analysis
- LLMInterface: Abstract interface for different LLM providers

All components are feature-flag controlled for safe deployment.
"""

from config.feature_flags import is_enabled

# Conditionally import based on feature flags
if is_enabled("enable_llm_events"):
    from .event_analysis import (
        LLMEventAnalyzer,
        EventAnalysisRequest,
        EventAnalysisResult,
        LLMInterface,
        MockLLMInterface,
        OpenAIInterface,
        EventAnalysisCache,
        create_event_analyzer,
        quick_event_analysis,
        deep_event_analysis
    )
    
    __all__ = [
        'LLMEventAnalyzer',
        'EventAnalysisRequest',
        'EventAnalysisResult',
        'LLMInterface',
        'MockLLMInterface', 
        'OpenAIInterface',
        'EventAnalysisCache',
        'create_event_analyzer',
        'quick_event_analysis',
        'deep_event_analysis'
    ]
    
    if is_enabled("enable_adaptive_selection"):
        from .event_analysis import (
            AdaptiveModelSelector,
            create_adaptive_analyzer
        )
        __all__.extend([
            'AdaptiveModelSelector',
            'create_adaptive_analyzer'
        ])

else:
    __all__ = []
    
    # Provide stubs when features are disabled
    def create_event_analyzer(*args, **kwargs):
        return None
    
    def create_adaptive_analyzer(*args, **kwargs):
        return None
    
    async def quick_event_analysis(*args, **kwargs):
        return None
    
    async def deep_event_analysis(*args, **kwargs):
        return None