"""
Analytics core module for ATS-GenAI platform.

Provides consolidated analytics functionality split from the monolithic service.
"""

try:
    from core.analytics.service import AnalyticsService
except ImportError:
    AnalyticsService = None

from .template_engine import DashboardTemplateEngine

__all__ = [
    'AnalyticsService',
    'DashboardTemplateEngine'
]