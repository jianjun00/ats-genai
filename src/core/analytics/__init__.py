"""
Analytics core module for ATS-GenAI platform.

Provides consolidated analytics functionality split from the monolithic service.
"""

from .service.analytics_service import AnalyticsService
from .dashboard.template_engine import DashboardTemplateEngine

__all__ = [
    'AnalyticsService',
    'DashboardTemplateEngine'
]