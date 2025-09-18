"""
Analytics core module for ATS-GenAI platform.

Provides consolidated analytics functionality split from the monolithic service.
"""

try:
    from src.core.analytics.service import AnalyticsService
except ImportError:
    AnalyticsService = None

from .dashboard.template_engine import DashboardTemplateEngine

__all__ = [
    'AnalyticsService',
    'DashboardTemplateEngine'
]