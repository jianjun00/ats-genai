#!/usr/bin/env python3
"""
Unified Analytics Service - Modular Architecture
Coordinates 7 focused analytics modules for maintainable codebase.
"""

import logging
from typing import Dict, Any, Optional

# Import analytics modules
from .analytics_modules.analytics_service_core import UnifiedAnalyticsService
from .analytics_modules.request_handler import UnifiedAnalyticsRequestHandler
from .analytics_modules.dashboard_generator import generate_eda_dashboard_html
from .analytics_modules.type_aware_analyzer import get_intelligent_filters
from .analytics_modules.training_data_manager import TrainingDataManager
from .analytics_modules.data_analysis_engine import DataAnalysisEngine
from .analytics_modules.news_events_handler import NewsEventsHandler

# Import server components
from http.server import ThreadingHTTPServer
import argparse

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def run_analytics_server(port: int = 3001, host: str = "localhost"):
    """Run the analytics server with modular architecture."""
    
    logger.info(f"🚀 Starting Unified Analytics Server on {host}:{port}")
    logger.info("📦 Modular architecture loaded:")
    logger.info("   ✅ Core Service")
    logger.info("   ✅ Type-Aware Analyzer")
    logger.info("   ✅ Training Data Manager")
    logger.info("   ✅ Data Analysis Engine")
    logger.info("   ✅ News Events Handler")
    logger.info("   ✅ Dashboard Generator")
    logger.info("   ✅ Request Handler")
    
    try:
        server = ThreadingHTTPServer((host, port), UnifiedAnalyticsRequestHandler)
        logger.info(f"✅ Analytics server running at http://{host}:{port}")
        logger.info("🎯 Available endpoints:")
        logger.info("   /dashboard - Main analytics dashboard")
        logger.info("   /api/datasets - Training dataset management")
        logger.info("   /api/analysis - Data analysis engine")
        logger.info("   /api/news - News events and economic data")
        logger.info("   /api/health - Service health check")
        
        server.serve_forever()
        
    except KeyboardInterrupt:
        logger.info("👋 Analytics server stopped by user")
    except Exception as e:
        logger.error(f"❌ Server error: {e}")
        raise

def main():
    """Main entry point for analytics service."""
    parser = argparse.ArgumentParser(description="Unified Analytics Service - Modular Architecture")
    parser.add_argument("--port", type=int, default=3001, help="Server port (default: 3001)")
    parser.add_argument("--host", type=str, default="localhost", help="Server host (default: localhost)")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")
    
    args = parser.parse_args()
    
    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)
        logger.debug("🐛 Debug logging enabled")
    
    run_analytics_server(port=args.port, host=args.host)

if __name__ == "__main__":
    main()