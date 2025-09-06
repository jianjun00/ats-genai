"""
LLM-Powered News Processing Services

This package contains the core LLM processing services for financial news analysis,
implementing state-of-the-art techniques for named entity recognition, event extraction,
sentiment analysis, and contextual analysis using RAG.
"""

from .news_llm_processor import (
    LLMNewsProcessor,
    NewsAnalysisResult,
    FinancialEntity,
    FinancialEvent,
    SentimentScore,
    RAGContext,
    LLMProcessingError
)

__all__ = [
    'LLMNewsProcessor',
    'NewsAnalysisResult',
    'FinancialEntity', 
    'FinancialEvent',
    'SentimentScore',
    'RAGContext',
    'LLMProcessingError'
]