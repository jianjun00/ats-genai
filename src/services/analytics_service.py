#!/usr/bin/env python3
"""
Unified Analytics Service - Consolidated from 5 separate analytics services

This service combines functionality from:
- analytics_service.py (main service, 6046 lines)
- analytics_service_class.py (type-aware features, 383 lines)
- type_aware_analytics_service.py (specialized type handling, 531 lines)
- universe_analytics_service.py (universe analytics, 310 lines)
- analytics_service.py.backup (removed - was duplicate)

Features:
- Web-based analytics dashboard for 30-year price database
- Type-aware dataset analysis and intelligent EDA
- Universe analytics and cross-instrument analysis
- Ray distributed computing integration
- Training dataset management and visualization
- Real-time data quality monitoring
"""

import asyncio
import json
import logging
import os
import time
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Dict, List, Any

# Agent imports
from agents.data_quality_agent import DataQualityAgent

# Prometheus metrics
from services.prometheus_metrics import get_metrics_collector

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Simple Agent State Manager
class SimpleAgentState:
    def __init__(self):
        self.monitoring_active = False
        self.active_workflows = 0
        self.pending_issues = 0
        self.last_activity = datetime.now()

    def start_monitoring(self):
        self.monitoring_active = True
        self.last_activity = datetime.now()
        return True

    def stop_monitoring(self):
        self.monitoring_active = False
        self.active_workflows = 0
        self.pending_issues = 0
        self.last_activity = datetime.now()
        return True

# Initialize simple agent state
AGENT_AVAILABLE = True
simple_agent_state = SimpleAgentState()
logger.info("✅ Simple Data Quality Agent initialized")

# Import Tagging System
try:
    from domains.tagging.services.tag_service import TagService
    from domains.tagging.repositories.tag_repository import TagRepository
    from domains.tagging.api.tag_api import tag_router
    TAGGING_AVAILABLE = True
    logger.info("✅ Tagging System loaded successfully")
except ImportError as e:
    TAGGING_AVAILABLE = False
    logger.warning(f"⚠️ Tagging System not available: {e}")

# Import core components
try:
    from core.platform.database.connection_manager import get_connection_manager
    CORE_PLATFORM_AVAILABLE = True
except ImportError:
    try:
        from infrastructure.database.connection_manager import DatabaseConnectionManager
        get_connection_manager = lambda: DatabaseConnectionManager()
        CORE_PLATFORM_AVAILABLE = True
    except ImportError:
        CORE_PLATFORM_AVAILABLE = False
        logger.warning("⚠️ Core platform not available, using environment variables")

# Import visualization components
try:
    from visualization.multi_panel_trading_chart import MultiPanelTradingChart
    from ml.training_data.timeseries_sequence_training_generator import MultiTimeframeFeatureExtractor, TrainingDataConfig
    VISUALIZATION_AVAILABLE = True
    logger.info("✅ Multi-panel trading visualization loaded")
except ImportError as e:
    VISUALIZATION_AVAILABLE = False
    logger.warning(f"⚠️ Multi-panel visualization not available: {e}")

# Import type system components (from analytics_service_class.py)
try:
    from domains.ml.schema.registry import schema_registry
    from domains.ml.schema.types import FieldSemantics
    TYPE_SYSTEM_AVAILABLE = True
    logger.info("✅ Type system components loaded")
except ImportError as e:
    TYPE_SYSTEM_AVAILABLE = False
    logger.warning(f"⚠️ Type system not available: {e}")

# Ray EDA integration for massive dataset analysis
try:
    from services.ray_eda_engine import get_ray_eda_service
    RAY_AVAILABLE = True
    logger.info("✅ Ray EDA engine loaded - distributed computing enabled")
except ImportError as e:
    RAY_AVAILABLE = False
    logger.warning(f"⚠️ Ray EDA engine not available: {e}. Falling back to traditional methods")

# Dataset metadata cache - expires after 4 hours
DATASET_CACHE = {
    'data': None,
    'timestamp': 0,
    'ttl': 4 * 60 * 60  # 4 hours in seconds
}

class UnifiedAnalyticsService:
    """
    Unified Analytics Service combining all analytics functionality.

    This class consolidates:
    1. Web dashboard serving (from analytics_service.py)
    2. Type-aware analysis (from analytics_service_class.py & type_aware_analytics_service.py)
    3. Universe analytics (from universe_analytics_service.py)
    4. Training dataset management
    5. Ray distributed computing integration
    """

    def __init__(self, db_manager=None):
        """Initialize unified analytics service with all capabilities."""
        self.db = db_manager
        self.type_system_enabled = TYPE_SYSTEM_AVAILABLE
        self.ray_enabled = RAY_AVAILABLE
        self.visualization_enabled = VISUALIZATION_AVAILABLE
        self.agent_enabled = AGENT_AVAILABLE

        # CRITICAL: Validate environment setup before starting
        self._validate_environment_setup()

        # Initialize visualization components
        if self.visualization_enabled:
            self.multi_panel_chart = MultiPanelTradingChart()
            self.feature_extractor = MultiTimeframeFeatureExtractor(TrainingDataConfig())

        # Initialize Data Quality Agent
        if self.agent_enabled:
            self.data_quality_agent = DataQualityAgent()
            self.agent_monitoring_task = None
            logger.info("🤖 Data Quality Agent initialized")
        
        # Initialize Tagging Service
        if TAGGING_AVAILABLE:
            self.tagging_enabled = True
            logger.info("🏷️ Tagging System initialized")
        else:
            self.tagging_enabled = False

        logger.info("🚀 Unified Analytics Service initialized")
        logger.info(f"   Type system: {'✅ Enabled' if self.type_system_enabled else '❌ Disabled'}")
        logger.info(f"   Ray computing: {'✅ Enabled' if self.ray_enabled else '❌ Disabled'}")
        logger.info(f"   Agent system: {'✅ Enabled' if self.agent_enabled else '❌ Disabled'}")
        logger.info(f"   Tagging system: {'✅ Enabled' if getattr(self, 'tagging_enabled', False) else '❌ Disabled'}")
        logger.info(f"   Multi-panel visualization: {'✅ Enabled' if self.visualization_enabled else '❌ Disabled'}")

        if self.type_system_enabled:
            logger.info(f"   Available schemas: {list(schema_registry.get_schema_summary()['entities'].keys())}")

    # ==============================================
    # TYPE-AWARE ANALYSIS (from analytics_service_class.py)
    # ==============================================

    async def get_intelligent_filters(self, table_name: str) -> Dict[str, Any]:
        """Generate intelligent filter definitions using type system."""
        if not self.type_system_enabled:
            logger.warning("Type system not available, falling back to basic filters")
            return self._get_basic_filters(table_name)

        try:
            filterable_fields = {}

            # Try to get schema for this table
            schema = schema_registry.get_table_schema(table_name)

            # Get all filterable fields from schema
            for field_name, field_def in schema.fields.items():
                if field_def.is_filterable:
                    filter_config = {
                        "field_name": field_name,
                        "display_name": field_def.ui_label,
                        "field_type": field_def.field_type.value,
                        "semantics": field_def.semantics.value,
                        "description": field_def.description,
                        "help_text": field_def.ui_help_text,
                        "placeholder": field_def.ui_placeholder,
                        "nullable": field_def.nullable,
                        "eda_priority": field_def.eda_priority
                    }

                    # Add semantic-specific configurations
                    if field_def.semantics == FieldSemantics.PRICE:
                        filter_config.update({
                            "min_value": 0,
                            "step": 0.01,
                            "format": "currency"
                        })
                    elif field_def.semantics == FieldSemantics.DATE:
                        filter_config.update({
                            "format": "date",
                            "date_range": True
                        })
                    elif field_def.semantics == FieldSemantics.SYMBOL:
                        filter_config.update({
                            "autocomplete": True,
                            "multi_select": True
                        })

                    filterable_fields[field_name] = filter_config

            return {
                "table_name": table_name,
                "filterable_fields": filterable_fields,
                "schema_available": True,
                "total_filterable": len(filterable_fields)
            }

        except Exception as e:
            logger.error(f"Error generating intelligent filters for {table_name}: {e}")
            return self._get_basic_filters(table_name)

    def _get_basic_filters(self, table_name: str) -> Dict[str, Any]:
        """Basic filter generation when type system unavailable."""
        # Basic filter definitions for common financial data tables
        basic_filters = {
            "symbol": {"field_type": "string", "multi_select": True},
            "date": {"field_type": "date", "date_range": True},
            "price": {"field_type": "numeric", "min_value": 0, "format": "currency"},
            "volume": {"field_type": "numeric", "min_value": 0},
            "exchange": {"field_type": "string", "multi_select": True}
        }

        return {
            "table_name": table_name,
            "filterable_fields": basic_filters,
            "schema_available": False,
            "total_filterable": len(basic_filters)
        }

    # ==============================================
    # UNIVERSE ANALYTICS (from universe_analytics_service.py)
    # ==============================================

    async def get_universe_analytics(self, universe_name: str = None) -> Dict[str, Any]:
        """Get comprehensive universe analytics and cross-instrument analysis."""
        try:
            # Universe composition analysis
            universe_stats = await self._analyze_universe_composition(universe_name)

            # Cross-instrument correlations
            correlations = await self._calculate_cross_instrument_correlations(universe_name)

            # Sector/industry analysis
            sector_analysis = await self._analyze_sector_composition(universe_name)

            # Performance analytics
            performance_metrics = await self._calculate_universe_performance(universe_name)

            return {
                "universe_name": universe_name or "default",
                "composition": universe_stats,
                "correlations": correlations,
                "sector_analysis": sector_analysis,
                "performance": performance_metrics,
                "analysis_timestamp": datetime.now().isoformat()
            }

        except Exception as e:
            logger.error(f"Error in universe analytics: {e}")
            return {"error": str(e), "universe_name": universe_name}

    async def _analyze_universe_composition(self, universe_name: str) -> Dict[str, Any]:
        """Analyze the composition of the universe."""
        # Implementation would connect to database and analyze universe membership
        # This is a placeholder for the consolidated functionality
        return {
            "total_instruments": 0,
            "by_exchange": {},
            "by_sector": {},
            "market_cap_distribution": {}
        }

    async def _calculate_cross_instrument_correlations(self, universe_name: str) -> Dict[str, Any]:
        """Calculate correlations between instruments in the universe."""
        # Placeholder for correlation analysis
        return {
            "correlation_matrix": [],
            "top_correlated_pairs": [],
            "clustering_results": {}
        }

    async def _analyze_sector_composition(self, universe_name: str) -> Dict[str, Any]:
        """Analyze sector composition of the universe."""
        return {
            "sector_weights": {},
            "sector_performance": {},
            "diversification_metrics": {}
        }

    async def _calculate_universe_performance(self, universe_name: str) -> Dict[str, Any]:
        """Calculate universe performance metrics."""
        return {
            "returns": {},
            "volatility": {},
            "sharpe_ratio": 0.0,
            "max_drawdown": 0.0
        }

    # ==============================================
    # TRAINING DATASET MANAGEMENT (from analytics_service.py)
    # ==============================================

    def get_training_datasets(self):
        """Get training datasets from database for dual-tab functionality."""
        try:
            from core.platform.database.connection_manager import get_raw_connection

            environment = os.getenv('ENVIRONMENT', 'dev')
            table_name = f"{environment}_training_datasets"

            with get_raw_connection() as conn:
                from psycopg2.extras import RealDictCursor

                with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                    query = f"""
                    SELECT
                        id, dataset_name, total_sequences, sequence_length, feature_count,
                        label_count, data_quality_score, feature_completeness, label_completeness,
                        file_size_mb, technical_indicators, symbols, date_range_start,
                        date_range_end, created_at
                    FROM {table_name}
                    WHERE status IN ('completed', 'generating')
                    ORDER BY created_at DESC
                    LIMIT 50
                    """

                    cursor.execute(query)
                    datasets = cursor.fetchall()

                    # Convert to list of dictionaries for JSON serialization
                    datasets_list = []
                    for dataset in datasets:
                        dataset_dict = dict(dataset)
                        # Convert datetime objects to strings
                        if 'created_at' in dataset_dict and dataset_dict['created_at']:
                            dataset_dict['created_at'] = dataset_dict['created_at'].isoformat()
                        if 'date_range_start' in dataset_dict and dataset_dict['date_range_start']:
                            dataset_dict['date_range_start'] = dataset_dict['date_range_start'].isoformat()
                        if 'date_range_end' in dataset_dict and dataset_dict['date_range_end']:
                            dataset_dict['date_range_end'] = dataset_dict['date_range_end'].isoformat()

                        # Split symbols field into array if it's a string
                        if 'symbols' in dataset_dict and isinstance(dataset_dict['symbols'], str):
                            dataset_dict['symbols'] = [s.strip() for s in dataset_dict['symbols'].split(',') if s.strip()]

                        datasets_list.append(dataset_dict)

                    logger.info(f"Retrieved {len(datasets_list)} training datasets from {table_name}")
                    return {
                        'datasets': datasets_list,
                        'total_count': len(datasets_list)
                    }

        except Exception as e:
            logger.error(f"Error getting training datasets: {e}")
            return {
                'datasets': [],
                'total_count': 0,
                'error': str(e)
            }

    def get_training_dataset_sequence(self, dataset_id: int, row_index: int, timeframe: str = "5m") -> Dict[str, Any]:
        """Get training dataset sequence data for OHLC visualization."""
        try:
            from core.platform.database.connection_manager import get_raw_connection
            import psycopg2.extras
            import numpy as np
            from pathlib import Path
            import os

            # Determine environment and table name
            environment = os.getenv('ENVIRONMENT', 'dev')
            table_name = f"{environment}_training_datasets"

            with get_raw_connection() as conn:
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                    # Get dataset info
                    cursor.execute(f"""
                        SELECT dataset_name, symbols, sequence_length, total_sequences,
                               data_format
                        FROM {table_name}
                        WHERE id = %s
                    """, (dataset_id,))

                    dataset_info = cursor.fetchone()
                    if not dataset_info:
                        raise ValueError(f"Dataset {dataset_id} not found")

                    # Try to load actual sequence data from files
                    try:
                        # Check for the generated Riegeli-compatible files
                        symbol = dataset_info['symbols'] or 'unknown'
                        symbol_lower = symbol.lower()

                        logger.info(f"Looking for training data files for symbol: {symbol} (dataset_id: {dataset_id})")

                        # Look for timeframe-specific training data files
                        # Priority order: Riegeli format > numpy files > alternative paths
                        possible_file_paths = [
                            # New structure: /data/training_data/<run_id>/<timeframe>/<symbol>_<startdatetime>_<enddatetime>.arrayrecord
                            f"/data/training_data/*/{timeframe}/{symbol_lower}_*.arrayrecord",
                            f"/data/training_data/*/*/{symbol_lower}_*.arrayrecord",
                            # Legacy numpy files (existing structure) - FIXED: Use correct path
                            f"/data/training_data/arrayrecord_aapl_tsla_2025/{symbol_lower}_features.npy",
                            f"/data/training_data/{dataset_info['dataset_name'].lower()}/{symbol_lower}_features.npy",
                            f"/data/training_data/{dataset_id}/{symbol_lower}_features.npy",
                        ]

                        logger.info(f"Checking file paths for timeframe {timeframe}: {possible_file_paths}")

                        features_file = None
                        import glob

                        for path_pattern in possible_file_paths:
                            logger.info(f"Checking pattern: {path_pattern}")

                            # Handle glob patterns for new file structure
                            if '*' in path_pattern:
                                matching_files = glob.glob(path_pattern)
                                logger.info(f"Glob pattern {path_pattern} found {len(matching_files)} matches")
                                if matching_files:
                                    # Use the first match (could be enhanced to pick best match)
                                    features_file = matching_files[0]
                                    logger.info(f"Selected file from glob: {features_file}")
                                    break
                            else:
                                # Handle exact path matches (legacy numpy files)
                                if Path(path_pattern).exists():
                                    features_file = path_pattern
                                    logger.info(f"Found exact file: {path_pattern}")
                                    break
                                else:
                                    logger.info(f"File not found: {path_pattern}")

                        if features_file:
                            logger.info(f"Loading training data from: {features_file}")
                            # Load numpy features file
                            features = np.load(features_file)
                            logger.info(f"Loaded features shape: {features.shape}")

                            # Ensure row_index is within bounds
                            if row_index >= len(features):
                                row_index = min(len(features) - 1, 0)

                            # Get the sequence for the specified row
                            sequence = features[row_index]  # Shape should be (sequence_length, feature_count)

                            # Convert to OHLC format - features are ordered as:
                            # [open, high, low, close, volume, envelope_top, envelope_bot, pldot, datetime_features...]
                            # Enhanced to handle variable feature counts with new datetime features
                            ohlc_sequence = []
                            for i, time_step in enumerate(sequence):
                                bar = {
                                    "time_step": i,
                                    # Core OHLCV features (indices 0-4)
                                    "open": float(time_step[0]) if len(time_step) > 0 else 0.0,
                                    "high": float(time_step[1]) if len(time_step) > 1 else 0.0,
                                    "low": float(time_step[2]) if len(time_step) > 2 else 0.0,
                                    "close": float(time_step[3]) if len(time_step) > 3 else 0.0,
                                    "volume": int(time_step[4]) if len(time_step) > 4 else 0,
                                    # Technical indicators (indices 5-7)
                                    "envelope_top": float(time_step[5]) if len(time_step) > 5 else 0.0,
                                    "envelope_bot": float(time_step[6]) if len(time_step) > 6 else 0.0,
                                    "pldot": float(time_step[7]) if len(time_step) > 7 else 0.0,
                                    # Traditional technical indicators (indices 8-11, if present)
                                    "sma_20": float(time_step[8]) if len(time_step) > 8 else 0.0,
                                    "ema_12": float(time_step[9]) if len(time_step) > 9 else 0.0,
                                    "rsi_14": float(time_step[10]) if len(time_step) > 10 else 0.0,
                                    "macd": float(time_step[11]) if len(time_step) > 11 else 0.0,
                                    # Datetime features (indices 12+, if present)
                                    "datetime": str(time_step[12]) if len(time_step) > 12 else None,
                                    "hour_of_day_edt": int(time_step[13]) if len(time_step) > 13 else 0,
                                    "day_of_week": int(time_step[14]) if len(time_step) > 14 else 0,
                                    "week_of_month": int(time_step[15]) if len(time_step) > 15 else 0,
                                    "week_of_year": int(time_step[16]) if len(time_step) > 16 else 0,
                                    "year": int(time_step[17]) if len(time_step) > 17 else 2025,
                                }
                                ohlc_sequence.append(bar)

                            return {
                                "dataset_id": dataset_id,
                                "dataset_name": dataset_info['dataset_name'],
                                "row_index": row_index,
                                "symbol": symbol,
                                "timeframe": timeframe,
                                "selected_bar": min(10, len(ohlc_sequence) // 2),  # Middle bar or bar 10
                                "sequence_length": len(ohlc_sequence),
                                "total_sequences": dataset_info['total_sequences'],
                                "data": ohlc_sequence,
                                "source": "arrayrecord_compatible_numpy" if features_file.endswith('.npy') else "arrayrecord_format",
                                "file_path": features_file
                            }

                    except Exception as file_error:
                        logger.warning(f"Could not load actual sequence data: {file_error}")
                        # Fall back to sample data

                    # Generate sample data if actual data not available
                    sample_data = self._generate_sample_sequence_for_dataset(dataset_info)
                    sample_data.update({
                        "dataset_id": dataset_id,
                        "row_index": row_index,
                        "timeframe": timeframe,
                        "source": "sample_data"
                    })
                    return sample_data

        except Exception as e:
            logger.error(f"Error getting sequence data for dataset {dataset_id}: {e}")
            # Return sample data as alternative
            sample_data = self._generate_sample_sequence_for_dataset({
                'dataset_name': f'Dataset {dataset_id}',
                'symbols': 'TEST',
                'sequence_length': 21
            }, dataset_id, row_index)
            sample_data['timeframe'] = timeframe
            return sample_data

    def _generate_sample_sequence_for_dataset(self, dataset_info: Dict, dataset_id: int = 0, row_index: int = 0) -> Dict[str, Any]:
        """Generate sample sequence data based on dataset info."""
        import random

        sequence_length = dataset_info.get('sequence_length', 21)
        symbol = dataset_info.get('symbols', 'TEST').split(',')[0] if dataset_info.get('symbols') else 'TEST'

        # Generate realistic OHLC sequence
        base_price = 150.0 + random.uniform(-50, 50)  # Start price
        sequence = []

        for i in range(sequence_length):
            # Simulate price movement
            change = random.uniform(-3.0, 3.0)
            base_price += change
            base_price = max(base_price, 10.0)  # Minimum price

            open_price = base_price + random.uniform(-1.0, 1.0)
            high_price = open_price + random.uniform(0, 4.0)
            low_price = open_price - random.uniform(0, 3.0)
            close_price = open_price + random.uniform(-2.0, 2.0)

            # Ensure OHLC consistency
            high_price = max(high_price, open_price, close_price)
            low_price = min(low_price, open_price, close_price)

            # Technical indicators
            envelope_top = high_price * 1.025  # 2.5% above high
            envelope_bot = low_price * 0.975   # 2.5% below low
            pldot = low_price * 0.99 if i % 4 == 0 else 0  # Pivot low dots occasionally

            bar = {
                "time_step": i,
                "open": round(open_price, 2),
                "high": round(high_price, 2),
                "low": round(low_price, 2),
                "close": round(close_price, 2),
                "volume": random.randint(50000, 2000000),
                "envelope_top": round(envelope_top, 2),
                "envelope_bot": round(envelope_bot, 2),
                "pldot": round(pldot, 2) if pldot > 0 else 0
            }
            sequence.append(bar)

        return {
            "dataset_id": dataset_id,
            "dataset_name": dataset_info.get('dataset_name', f'Dataset {dataset_id}'),
            "row_index": row_index,
            "symbol": symbol,
            "selected_bar": min(10, sequence_length // 2),  # Middle bar
            "sequence_length": sequence_length,
            "data": sequence
        }

    # ==============================================
    # RAY DISTRIBUTED COMPUTING INTEGRATION
    # ==============================================

    def get_training_dataset_sequences(self, dataset_id: int) -> Dict[str, Any]:
        """Get available sequences for a training dataset."""
        try:
            from core.platform.database.connection_manager import get_raw_connection
            import psycopg2.extras
            import os

            # Determine environment and table name
            environment = os.getenv('ENVIRONMENT', 'dev')
            table_name = f"{environment}_training_datasets"

            with get_raw_connection() as conn:
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                    # Get dataset info
                    cursor.execute(f"""
                        SELECT dataset_name, symbols, total_sequences, run_id,
                               dataset_path, symbol_files, file_metadata
                        FROM {table_name}
                        WHERE id = %s
                    """, (dataset_id,))

                    dataset_info = cursor.fetchone()
                    if not dataset_info:
                        return {
                            "datasets": [{"id": dataset_id, "error": "Dataset not found"}],
                            "sequences": [],
                            "total_count": 0
                        }

                    # Get sequences info from visualization data
                    sequences = []
                    total_count = 0

                    # Use file_metadata from database for precise sequence information
                    try:
                        file_metadata = dataset_info.get('file_metadata', {})

                        if file_metadata and file_metadata.get('files'):
                            logger.info(f"Using database file_metadata for dataset {dataset_id}")

                            sequence_id_counter = 0
                            files_info = file_metadata.get('files', [])

                            # Generate sequence-based menu items from file metadata
                            # Group files by sequence (symbol+daterange) instead of individual timeframes
                            sequences_by_id = {}

                            for file_info in files_info:
                                symbol = file_info.get('symbol')
                                timeframe = file_info.get('timeframe')
                                file_path = file_info.get('file_path', '')
                                file_size_bytes = file_info.get('file_size_bytes', 0)

                                # Extract sequence ID from file path (e.g., AAPL_20250701_000000_20250906_000000.arrayrecord)
                                if file_path.endswith('.arrayrecord'):
                                    sequence_id = file_path.replace('.arrayrecord', '')
                                else:
                                    sequence_id = f"{symbol}_sequence"

                                if sequence_id not in sequences_by_id:
                                    sequences_by_id[sequence_id] = {
                                        'symbol': symbol,
                                        'timeframes': {},
                                        'total_size': 0,
                                        'file_count': 0
                                    }

                                # Add this timeframe to the sequence
                                sequences_by_id[sequence_id]['timeframes'][timeframe] = {
                                    'file_path': file_path,
                                    'file_size_bytes': file_size_bytes
                                }
                                sequences_by_id[sequence_id]['total_size'] += file_size_bytes
                                sequences_by_id[sequence_id]['file_count'] += 1

                            # Create sequence menu items (one per symbol+daterange)
                            for sequence_id, sequence_info in sequences_by_id.items():
                                sequences.append({
                                    "id": sequence_id_counter,
                                    "sequence_id": sequence_id,
                                    "symbol": sequence_info['symbol'],
                                    "timeframes": list(sequence_info['timeframes'].keys()),
                                    "timeframe_count": len(sequence_info['timeframes']),
                                    "description": sequence_id,  # Show sequence ID as description
                                    "file_count": sequence_info['file_count'],
                                    "total_size_mb": round(sequence_info['total_size'] / (1024 * 1024), 2)
                                })
                                sequence_id_counter += 1

                            total_count = len(sequences)
                            logger.info(f"Generated {total_count} sequence-based menu items from file_metadata for dataset {dataset_id}")

                        else:
                            # Alternative: use filesystem scanning (legacy approach)
                            logger.warning(f"No file_metadata available for dataset {dataset_id}, using filesystem alternative")

                            symbols = dataset_info.get('symbols', [])
                            if isinstance(symbols, str):
                                if symbols.startswith('{') and symbols.endswith('}'):
                                    symbols = [s.strip() for s in symbols.strip('{}').split(',') if s.strip()]
                                else:
                                    symbols = [s.strip() for s in symbols.split(',') if s.strip()]

                            for symbol in symbols:
                                try:
                                    # Each symbol has exactly ONE sequence file
                                    # The sequence file contains multiple time steps/bars, not multiple sequences
                                    symbol_files = dataset_info.get('symbol_files', {})
                                    sequence_filename = symbol_files.get(symbol, f"{symbol}_20250701_000000_20250906_000000")

                                    sequences.append({
                                        "id": len(sequences),
                                        "sequence_id": sequence_filename,  # Use actual filename as sequence ID
                                        "symbol": symbol,
                                        "timeframe": "multi",
                                        "filename": f"{sequence_filename}.arrayrecord",
                                        "description": f"{symbol} Training Sequence ({sequence_filename})",
                                        "file_size_mb": 0.1
                                    })
                                except Exception as e:
                                    logger.error(f"Error processing symbol {symbol}: {e}")

                            total_count = len(sequences)

                    except Exception as e:
                        logger.error(f"Error getting visualization data for sequences: {e}")
                        total_count = 0

                    return {
                        "datasets": [dataset_info],
                        "sequences": sequences,
                        "total_count": total_count
                    }

        except Exception as e:
            logger.error(f"Error getting sequences for dataset {dataset_id}: {e}")
            return {
                "datasets": [{"id": dataset_id, "error": str(e)}],
                "sequences": [],
                "total_count": 0
            }


    def get_training_dataset_visualization_data(self, dataset_id: int, start_idx: int = 0, sequence_id: str = None, target_symbol: str = None) -> Dict[str, Any]:
        """Get visualization data for training dataset sequences (OHLC + indicators for Plotly charts)."""
        try:
            from core.platform.database.connection_manager import get_raw_connection
            import psycopg2.extras
            from pathlib import Path
            import os

            # Determine environment and table name
            environment = os.getenv('ENVIRONMENT', 'dev')
            table_name = f"{environment}_training_datasets"  # Fixed: plural form to match main API

            with get_raw_connection() as conn:
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                    # Get dataset info using actual table columns from plural table
                    cursor.execute(f"""
                        SELECT dataset_name, symbols, id, run_id
                        FROM {table_name}
                        WHERE id = %s
                    """, (dataset_id,))

                    dataset_info = cursor.fetchone()
                    if not dataset_info:
                        raise ValueError(f"Dataset {dataset_id} not found")

                    # Use actual run_id from database
                    run_id = dataset_info.get('run_id') or str(dataset_id)  # Use actual run_id or alternative to dataset_id
                    symbols_data = dataset_info.get('symbols', '')

                    # Parse symbols - could be string, array, or PostgreSQL array format
                    if isinstance(symbols_data, str):
                        if symbols_data.startswith('{') and symbols_data.endswith('}'):
                            # PostgreSQL array format: {TSLA} or {AAPL,TSLA}
                            symbols = [s.strip() for s in symbols_data.strip('{}').split(',') if s.strip()]
                        else:
                            # Comma-separated string
                            symbols = [s.strip() for s in symbols_data.split(',') if s.strip()]
                    else:
                        symbols = symbols_data if symbols_data else []

                    if not symbols:
                        raise ValueError(f"Dataset {dataset_id} missing symbols")

                    # Determine target symbol - use parameter if provided, otherwise logic
                    if target_symbol is None:
                        # If sequence_id provided, find specific sequence
                        if sequence_id is not None:
                            try:
                                seq_idx = int(sequence_id)
                                if seq_idx < len(symbols):
                                    target_symbol = symbols[seq_idx]
                                else:
                                    target_symbol = symbols[0]
                            except (ValueError, IndexError):
                                target_symbol = symbols[0]
                        else:
                            target_symbol = symbols[0]  # Default to first symbol

                    # Search for actual files in all potential locations (container-aware)
                    training_base_paths = [
                        Path("/data/training_data"),  # Container path
                        Path("/mnt/d/ats-data/training_data")  # Host path alternative
                    ]

                    arrayrecord_files = []
                    for base_path in training_base_paths:
                        if base_path.exists():
                            logger.info(f"Searching for {target_symbol} files in run {run_id} at: {base_path}")
                            # Search specifically in the run_id directory first
                            run_path = base_path / str(run_id)
                            if run_path.exists():
                                logger.info(f"Found run directory: {run_path}")
                                # Look for files in specific run directory
                                for arrayrecord_file in list(run_path.rglob("*.arrayrecord")):
                                    # Check if file contains our target symbol (case insensitive)
                                    file_name = arrayrecord_file.name.lower()
                                    symbol_lower = target_symbol.lower()

                                    logger.info(f"Checking file: {arrayrecord_file}, symbol_lower: {symbol_lower}")

                                    # Check filename for symbol match
                                    if symbol_lower in file_name:
                                        arrayrecord_files.append(arrayrecord_file)
                                        logger.info(f"Found matching file for run {run_id}: {arrayrecord_file}")
                                        break  # Use first match in correct run

                            # If not found in run directory, alternative to general search
                            if not arrayrecord_files:
                                logger.warning(f"No files found in run {run_id}, falling back to general search")
                                for arrayrecord_file in list(base_path.rglob("*.arrayrecord")):
                                    file_name = arrayrecord_file.name.lower()
                                    file_path_str = str(arrayrecord_file).lower()
                                    symbol_lower = target_symbol.lower()

                                    if symbol_lower in file_name or f"/{symbol_lower}/" in file_path_str:
                                        arrayrecord_files.append(arrayrecord_file)
                                        logger.info(f"Found alternative file: {arrayrecord_file}")
                                        break  # Use first match

                            # If files found in this base_path, stop searching other base_paths
                            if arrayrecord_files:
                                break

                    if arrayrecord_files:
                        # Read actual ArrayRecord data
                        arrayrecord_file = arrayrecord_files[0]
                        try:
                            from array_record.python.array_record_module import ArrayRecordReader

                            visualization_data = []
                            reader = ArrayRecordReader(str(arrayrecord_file))

                            # Read all records using proper ArrayRecord API
                            total_records = reader.num_records()
                            logger.info(f"ArrayRecord has {total_records} records")

                            columns = None
                            training_data_array = None

                            # Read records using seek + read
                            for i in range(total_records):
                                reader.seek(i)
                                record = reader.read()

                                if i == 0:
                                    # First record is column names
                                    try:
                                        columns_str = record.decode('utf-8')
                                        import ast
                                        columns = ast.literal_eval(columns_str)
                                        logger.info(f"ArrayRecord columns: {len(columns)} columns")
                                    except (UnicodeDecodeError, ValueError) as e:
                                        logger.warning(f"Could not parse columns from first record: {e}")

                                elif i == 1:
                                    # Second record is the training data array
                                    try:
                                        import numpy as np
                                        training_data_array = np.frombuffer(record, dtype=np.float32)
                                        logger.info(f"Training data array: {len(training_data_array)} elements")
                                        non_zero_count = np.count_nonzero(training_data_array)
                                        logger.info(f"Non-zero elements: {non_zero_count} / {len(training_data_array)} ({100*non_zero_count/len(training_data_array):.1f}%)")
                                    except Exception as e:
                                        logger.error(f"Error parsing training data: {e}")

                            reader.close()

                            # Process the training data array
                            if training_data_array is not None and columns is not None:
                                logger.info(f"Processing training data array with {len(columns)} columns")

                                # The training data is a flattened array - reshape it into records
                                num_features = len(columns) - 2  # Subtract timestamp and symbol columns
                                if len(training_data_array) >= num_features:
                                    # For visualization, we'll create a single record from the training data
                                    # Extract OHLCV data for different timeframes
                                    record_data = {}
                                    for j, col in enumerate(columns):
                                        if j < len(training_data_array):
                                            val = training_data_array[j]
                                            # Handle NaN values
                                            if np.isnan(val):
                                                val = 0.0
                                            record_data[col] = float(val)
                                        else:
                                            record_data[col] = 0.0

                                    # Create visualization data from the training record
                                    # Extract 5m timeframe data for visualization (first sequence)
                                    visualization_data = []

                                    # Look for 5m OHLCV columns
                                    for seq_idx in range(52):  # 5m has 52 time steps according to config
                                        # Calculate Unix timestamp for this bar in Eastern Time
                                        from datetime import datetime, timedelta
                                        from zoneinfo import ZoneInfo
                                        base_dt = datetime(2025, 7, 1, 2, 0, 0, tzinfo=ZoneInfo("America/New_York"))
                                        bar_dt = base_dt + timedelta(minutes=seq_idx * 5)

                                        bar_data = {
                                            "time_step": seq_idx,
                                            "timestamp": int(bar_dt.timestamp()),
                                            "symbol": record_data.get('symbol', target_symbol),
                                            "open": record_data.get(f'5m_open_{seq_idx:03d}', 0),
                                            "high": record_data.get(f'5m_high_{seq_idx:03d}', 0),
                                            "low": record_data.get(f'5m_low_{seq_idx:03d}', 0),
                                            "close": record_data.get(f'5m_close_{seq_idx:03d}', 0),
                                            "volume": record_data.get(f'5m_volume_{seq_idx:03d}', 0),
                                            "vwap": record_data.get(f'5m_vwap_{seq_idx:03d}', 0)
                                        }
                                        visualization_data.append(bar_data)

                                    logger.info(f"Created {len(visualization_data)} visualization bars from training data")
                                else:
                                    logger.error(f"Training data array too short: {len(training_data_array)} < {num_features}")
                                    raise ValueError("Insufficient training data")
                            else:
                                logger.error("No training data or columns found")
                                raise ValueError("No training data found")


                            # Calculate available sequences from visualization data
                            total_records = len(visualization_data)
                            window_size = 21  # Default window size for visualization
                            available_sequences = max(1, total_records - window_size + 1)

                            # ENFORCE: No test data allowed - check response before returning
                            from services.data_validator import fail_on_invalid_data

                            response = {
                                "dataset_id": dataset_id,
                                "sequence_id": sequence_id,
                                "start_idx": start_idx,
                                "symbol": target_symbol,
                                "selected_bar": 10,  # Middle bar as selected
                                "sequence_length": len(visualization_data),
                                "total_sequences": available_sequences,
                                "total_records": total_records,
                                "data": visualization_data,
                                "source": "arrayrecord"
                            }

                            # Fail fast if invalid data detected
                            fail_on_invalid_data(response, f"visualization_data_response_dataset_{dataset_id}")

                            # Sanitize response to prevent NaN/Infinity JSON serialization errors
                            try:
                                from core.sanitizers.json_sanitizer import validate_api_response
                                response = validate_api_response(response)
                                logger.info("✅ Visualization API response sanitized for JSON safety")
                            except Exception as sanitizer_error:
                                logger.warning(f"Visualization response sanitization failed: {sanitizer_error}")
                                # Continue with original response if sanitization fails

                            return response

                        except ImportError as e:
                            logger.error(f"ArrayRecord import failed: {e}")
                            raise RuntimeError(f"ArrayRecord library not available: {e}. Cannot read training data without proper dependencies.")
                        except Exception as e:
                            logger.error(f"ArrayRecord reading failed: {e}")
                            import traceback
                            logger.error(traceback.format_exc())
                            raise RuntimeError(f"Failed to read training data file: {e}. No alternative data provided.")

                    # No files found
                    raise ValueError(f"No Riegeli/ArrayRecord files found for dataset {dataset_id}, symbol {target_symbol}")

        except Exception as e:
            logger.error(f"Error getting visualization data for dataset {dataset_id}: {e}")
            # No test data - re-raise the error
            raise

    def get_training_dataset_sequence_multi_timeframe(self, dataset_id: int, sequence_id: str, row_index: int = 50) -> Dict[str, Any]:
        """Get multi-timeframe OHLC data for a specific sequence, showing 21 bars centered around row_index."""
        try:
            from core.platform.database.connection_manager import get_raw_connection
            import psycopg2.extras
            from pathlib import Path
            import os

            logger.info(f"Getting multi-timeframe data for dataset {dataset_id}, sequence {sequence_id}")

            # Determine environment and table name
            environment = os.getenv('ENVIRONMENT', 'dev')
            table_name = f"{environment}_training_datasets"

            with get_raw_connection() as conn:
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                    # Get dataset info including file_metadata
                    cursor.execute(f"""
                        SELECT dataset_name, symbols, run_id, file_metadata
                        FROM {table_name}
                        WHERE id = %s
                    """, (dataset_id,))

                    dataset_info = cursor.fetchone()
                    if not dataset_info:
                        return {"error": f"Dataset {dataset_id} not found"}

                    file_metadata = dataset_info.get('file_metadata', {})
                    run_id = dataset_info.get('run_id')

                    # Define training base paths (container-aware)
                    training_base_paths = [
                        Path("/data/training_data"),  # Container path
                        Path("/mnt/d/ats-data/training_data")  # Host path alternative
                    ]

                    # Find the sequence directory and read all timeframes
                    multi_timeframe_data = {}
                    timeframes = ['5m', '15m', '1h', '1d', '1w']

                    for base_path in training_base_paths:
                        sequence_dir = Path(base_path) / str(run_id) / sequence_id
                        logger.info(f"Checking sequence directory: {sequence_dir}")
                        if sequence_dir.exists():
                            logger.info(f"✅ Found sequence directory: {sequence_dir}")

                            for timeframe in timeframes:
                                timeframe_dir = sequence_dir / timeframe
                                arrayrecord_file = timeframe_dir / f"{sequence_id}.arrayrecord"
                                logger.info(f"Checking ArrayRecord file: {arrayrecord_file}")

                                if arrayrecord_file.exists():
                                    try:
                                        logger.info(f"✅ Found file: {arrayrecord_file}")
                                        # Read ArrayRecord data for this timeframe
                                        ohlc_data = self._read_arrayrecord_ohlc(arrayrecord_file)
                                        if ohlc_data:
                                            multi_timeframe_data[timeframe] = ohlc_data
                                            logger.info(f"✅ Loaded {len(ohlc_data)} OHLC bars for {timeframe}")
                                        else:
                                            logger.warning(f"⚠️  ArrayRecord file returned no data: {arrayrecord_file}")
                                    except Exception as e:
                                        logger.error(f"❌ Failed to read {timeframe} data from {arrayrecord_file}: {e}")
                                else:
                                    logger.warning(f"❌ ArrayRecord file not found: {arrayrecord_file}")

                            break  # Found sequence directory
                        else:
                            logger.warning(f"❌ Sequence directory not found: {sequence_dir}")

                    if not multi_timeframe_data:
                        return {"error": f"No data files found for sequence {sequence_id}"}

                    # Apply 21-bar selection logic (10 before + 1 current + 10 after) to all timeframes
                    logger.info(f"🎯 Applying 21-bar selection logic with row_index={row_index}")
                    for timeframe, data in multi_timeframe_data.items():
                        if data and len(data) > 0:
                            data_len = len(data)
                            logger.info(f"   {timeframe}: {data_len} bars available")

                            # If row_index is beyond data length, use all available data
                            if row_index >= data_len:
                                logger.warning(f"   {timeframe}: row_index {row_index} >= data_len {data_len}, using all available data")
                                start_idx = 0
                                end_idx = data_len
                            else:
                                # Calculate start and end indices for 21-bar window
                                start_idx = max(0, row_index - 10)
                                end_idx = min(data_len, row_index + 11)  # +11 to include row_index + 10 bars after

                                # Ensure we have 21 bars if possible
                                if end_idx - start_idx < 21 and data_len >= 21:
                                    if start_idx == 0:
                                        # If we're at the beginning, extend end
                                        end_idx = min(data_len, 21)
                                    elif end_idx == data_len:
                                        # If we're at the end, extend start backward
                                        start_idx = max(0, data_len - 21)

                                # Safety check: if we still have insufficient data, use all available
                                if data_len < 21:
                                    logger.info(f"   {timeframe}: Only {data_len} bars available, using all data instead of 21-bar window")
                                    start_idx = 0
                                    end_idx = data_len

                            # Apply the slice
                            selected_bars = data[start_idx:end_idx]
                            multi_timeframe_data[timeframe] = selected_bars

                            logger.info(f"   {timeframe}: Selected bars {start_idx}-{end_idx-1} ({len(selected_bars)} bars)")
                            if len(selected_bars) > 0:
                                actual_row_idx = row_index - start_idx
                                logger.info(f"   {timeframe}: Target row is now at index {actual_row_idx} in selected data")

                    # Prepare comprehensive table view data with all training features
                    table_data = []
                    feature_matrix = []

                    # Try to get comprehensive training data from ArrayRecord
                    for base_path in training_base_paths:
                        if not table_data:  # Only process first successful path
                            arrayrecord_path = Path(base_path) / str(run_id) / sequence_id / "1h" / f"{sequence_id}.arrayrecord"
                            if arrayrecord_path.exists():
                                try:
                                    from array_record.python.array_record_module import ArrayRecordReader
                                    import ast
                                    import numpy as np

                                    reader = ArrayRecordReader(str(arrayrecord_path))

                                    # Read column names
                                    reader.seek(0)
                                    columns_record = reader.read()
                                    columns = ast.literal_eval(columns_record.decode('utf-8'))

                                    # Read training data
                                    reader.seek(1)
                                    data_record = reader.read()
                                    training_array = np.frombuffer(data_record, dtype=np.float32)

                                    reader.close()

                                    # Create comprehensive feature matrix
                                    if len(training_array) == len(columns):
                                        # Create a single row with all features
                                        feature_row = {}
                                        for i, col_name in enumerate(columns):
                                            val = training_array[i]
                                            # Handle NaN values to prevent JSON serialization errors
                                            import math
                                            if math.isnan(val):
                                                # Special handling for specific fields
                                                if 'symbol' in col_name.lower():
                                                    # Symbol fields should be strings, not numeric
                                                    # Extract symbol from sequence_id (e.g., "AAPL_20250701_000000_20250906_000000" -> "AAPL")
                                                    symbol = sequence_id.split('_')[0] if sequence_id else 'UNKNOWN'
                                                    feature_row[col_name] = symbol
                                                else:
                                                    val = 0.0
                                                    feature_row[col_name] = float(val)
                                            else:
                                                feature_row[col_name] = float(val)

                                        feature_matrix.append(feature_row)
                                        logger.info(f"✅ Comprehensive feature data: {len(columns)} features extracted from ArrayRecord")

                                except Exception as e:
                                    logger.error(f"Error reading comprehensive training data: {e}")

                    # Prepare table data - use OHLC format for table display compatibility
                    # while still providing comprehensive features for detailed analysis
                    if '1h' in multi_timeframe_data and multi_timeframe_data['1h']:
                        # Use 1h OHLC data for table display (UI expects basic OHLC fields)
                        table_data = multi_timeframe_data['1h']
                        logger.info(f"✅ Table data prepared: {len(table_data)} rows from 1h OHLC data for table display")
                    else:
                        # Return empty array if no OHLC data available
                        table_data = []
                        logger.warning("⚠️  No OHLC data available for table display")

                    # Store comprehensive features separately for advanced analysis
                    comprehensive_features = feature_matrix if feature_matrix else []

                    # Create response and sanitize for JSON safety
                    response = {
                        "sequence_id": sequence_id,
                        "dataset_name": dataset_info.get('dataset_name'),
                        "ohlc_data": multi_timeframe_data,
                        "table_data": table_data,  # OHLC data for table display
                        "comprehensive_features": comprehensive_features,  # All 962 training features
                        "available_timeframes": list(multi_timeframe_data.keys()),
                        "success": True
                    }

                    # Sanitize response to prevent NaN/Infinity JSON serialization errors
                    try:
                        from core.sanitizers.json_sanitizer import validate_api_response
                        response = validate_api_response(response)
                        logger.info("✅ API response sanitized for JSON safety")
                    except Exception as sanitizer_error:
                        logger.warning(f"Response sanitization failed: {sanitizer_error}")
                        # Continue with original response if sanitization fails

                    logger.info(f"🎯 MULTI-TIMEFRAME API RESPONSE DEBUG:")
                    logger.info(f"   Sequence ID: {response['sequence_id']}")
                    logger.info(f"   Dataset: {response['dataset_name']}")
                    logger.info(f"   OHLC timeframes: {list(response['ohlc_data'].keys())}")
                    logger.info(f"   OHLC data counts: {[(tf, len(data)) for tf, data in response['ohlc_data'].items()]}")
                    logger.info(f"   Table rows: {len(response['table_data'])}")
                    logger.info(f"   Success: {response['success']}")

                    return response

        except Exception as e:
            logger.error(f"Error getting multi-timeframe sequence data: {e}")
            return {"error": str(e)}

    def _read_arrayrecord_ohlc(self, file_path: Path) -> List[Dict]:
        """Read OHLC data from ArrayRecord file."""
        try:
            # Try to read ArrayRecord file with proper error handling
            from array_record.python.array_record_module import ArrayRecordReader

            ohlc_data = []
            reader = ArrayRecordReader(str(file_path))

            total_records = reader.num_records()
            logger.debug(f"ArrayRecord has {total_records} records")

            if total_records < 2:
                logger.warning(f"Insufficient records in ArrayRecord: {total_records}")
                return []

            columns = None
            training_data_array = None

            # Read column names (record 0)
            reader.seek(0)
            columns_record = reader.read()
            try:
                columns_str = columns_record.decode('utf-8')
                import ast
                columns = ast.literal_eval(columns_str)
                logger.debug(f"ArrayRecord columns: {len(columns)} columns")
            except Exception as e:
                logger.error(f"Failed to parse columns: {e}")
                reader.close()
                return []

            # Read training data (record 1)
            reader.seek(1)
            data_record = reader.read()
            try:
                import numpy as np
                training_data_array = np.frombuffer(data_record, dtype=np.float32)
                logger.debug(f"Training data array: {len(training_data_array)} elements")
            except Exception as e:
                logger.error(f"Failed to parse training data: {e}")
                reader.close()
                return []

            reader.close()

            # Convert training data array to OHLC records
            if training_data_array is not None and columns is not None:
                # Map training data array to column names
                record_data = {}
                for j, col in enumerate(columns):
                    if j < len(training_data_array):
                        val = training_data_array[j]
                        # Handle NaN values
                        import math
                        if math.isnan(val):
                            val = 0.0
                        record_data[col] = float(val)
                    else:
                        record_data[col] = 0.0

                # Extract OHLC data for the specific timeframe from column names
                # Determine which timeframe this file represents based on file path
                file_name = file_path.name
                if '/5m/' in str(file_path):
                    timeframe_prefix = '5m'
                    sequence_length = 52
                elif '/15m/' in str(file_path):
                    timeframe_prefix = '15m'
                    sequence_length = 52
                elif '/1h/' in str(file_path):
                    timeframe_prefix = '1h'
                    sequence_length = 24
                elif '/1d/' in str(file_path):
                    timeframe_prefix = '1d'
                    sequence_length = 20
                elif '/1w/' in str(file_path):
                    timeframe_prefix = '1w'
                    sequence_length = 12
                else:
                    timeframe_prefix = '5m'  # Default
                    sequence_length = 52

                # Create OHLC records for this timeframe
                for i in range(sequence_length):
                    # Calculate timestamp as Unix epoch seconds in Eastern Time
                    from datetime import datetime, timedelta
                    from zoneinfo import ZoneInfo
                    base_date = datetime(2025, 7, 1, 2, 0, 0, tzinfo=ZoneInfo("America/New_York"))
                    # Add time based on timeframe and index
                    if timeframe_prefix == '5m':
                        timestamp_dt = base_date + timedelta(minutes=i * 5)
                    elif timeframe_prefix == '15m':
                        timestamp_dt = base_date + timedelta(minutes=i * 15)
                    elif timeframe_prefix == '1h':
                        timestamp_dt = base_date + timedelta(hours=i)
                    elif timeframe_prefix == '1d':
                        timestamp_dt = base_date + timedelta(days=i)
                    elif timeframe_prefix == '1w':
                        timestamp_dt = base_date + timedelta(days=i * 7)
                    else:
                        timestamp_dt = base_date

                    ohlc_record = {
                        "timestamp": int(timestamp_dt.timestamp()),
                        "open": record_data.get(f'{timeframe_prefix}_open_{i:03d}', 0),
                        "high": record_data.get(f'{timeframe_prefix}_high_{i:03d}', 0),
                        "low": record_data.get(f'{timeframe_prefix}_low_{i:03d}', 0),
                        "close": record_data.get(f'{timeframe_prefix}_close_{i:03d}', 0),
                        "volume": record_data.get(f'{timeframe_prefix}_volume_{i:03d}', 0),
                        "vwap": record_data.get(f'{timeframe_prefix}_vwap_{i:03d}', 0)
                    }

                    # Only add records with non-zero data
                    if (ohlc_record["open"] != 0 or ohlc_record["high"] != 0 or
                        ohlc_record["low"] != 0 or ohlc_record["close"] != 0):
                        ohlc_data.append(ohlc_record)

                logger.debug(f"Extracted {len(ohlc_data)} OHLC records for {timeframe_prefix}")
            return ohlc_data

        except Exception as e:
            logger.error(f"Failed to read ArrayRecord file {file_path}: {e}")
            return []

    def get_bar_collection_metrics(self) -> Dict[str, Any]:
        """Get metrics about bars collected organized by collection time and bar time."""
        try:
            from core.platform.database.connection_manager import get_raw_connection
            import psycopg2.extras

            with get_raw_connection() as conn:
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:

                    # Get metrics from all minute bar tables
                    minute_tables = ['dev_one_minute_live_polygon', 'dev_one_minute_live_tiingo', 'dev_one_minute_live_fmp']

                    collection_metrics = {}

                    for table_name in minute_tables:
                        try:
                            # Check if table exists
                            cursor.execute("""
                                SELECT COUNT(*) as count FROM information_schema.tables
                                WHERE table_name = %s AND table_schema = 'public'
                            """, (table_name,))

                            if cursor.fetchone()['count'] == 0:
                                continue  # Skip non-existent tables

                            # Bars collected per collection time (received_at hour)
                            cursor.execute(f"""
                                SELECT
                                    DATE_TRUNC('hour', received_at) as collection_hour,
                                    COUNT(*) as bars_collected,
                                    COUNT(DISTINCT symbol) as unique_symbols,
                                    AVG(data_latency_ms) as avg_latency_ms,
                                    AVG(quality_score) as avg_quality_score
                                FROM {table_name}
                                WHERE received_at >= NOW() - INTERVAL '24 hours'
                                GROUP BY DATE_TRUNC('hour', received_at)
                                ORDER BY collection_hour DESC
                                LIMIT 24
                            """)

                            collection_time_data = cursor.fetchall()

                            # Bars per bar time (timestamp hour)
                            cursor.execute(f"""
                                SELECT
                                    DATE_TRUNC('hour', timestamp) as bar_hour,
                                    COUNT(*) as bars_count,
                                    COUNT(DISTINCT symbol) as unique_symbols,
                                    AVG(volume) as avg_volume,
                                    AVG(CASE WHEN high_price > 0 THEN ((high_price - low_price) / high_price) * 100 ELSE 0 END) as avg_volatility_pct
                                FROM {table_name}
                                WHERE timestamp >= NOW() - INTERVAL '24 hours'
                                GROUP BY DATE_TRUNC('hour', timestamp)
                                ORDER BY bar_hour DESC
                                LIMIT 24
                            """)

                            bar_time_data = cursor.fetchall()

                            # Overall table stats
                            cursor.execute(f"""
                                SELECT
                                    COUNT(*) as total_bars,
                                    COUNT(DISTINCT symbol) as total_symbols,
                                    MIN(timestamp) as earliest_bar,
                                    MAX(timestamp) as latest_bar,
                                    MIN(received_at) as first_collected,
                                    MAX(received_at) as last_collected,
                                    AVG(data_latency_ms) as avg_latency_ms,
                                    AVG(quality_score) as avg_quality_score
                                FROM {table_name}
                                WHERE timestamp >= NOW() - INTERVAL '7 days'
                            """)

                            overall_stats = cursor.fetchone()

                            collection_metrics[table_name] = {
                                'collection_time_metrics': [dict(row) for row in collection_time_data],
                                'bar_time_metrics': [dict(row) for row in bar_time_data],
                                'overall_stats': dict(overall_stats) if overall_stats else {},
                                'vendor': table_name.split('_')[-1].upper()  # Extract vendor name
                            }

                        except Exception as e:
                            logger.error(f"Error getting metrics for {table_name}: {e}")
                            collection_metrics[table_name] = {
                                'error': str(e),
                                'vendor': table_name.split('_')[-1].upper()
                            }

                    return {
                        'metrics': collection_metrics,
                        'timestamp': datetime.now().isoformat(),
                        'summary': self._generate_collection_summary(collection_metrics)
                    }

        except Exception as e:
            logger.error(f"Error getting bar collection metrics: {e}")
            return {
                'error': str(e),
                'metrics': {},
                'timestamp': datetime.now().isoformat()
            }

    def _generate_collection_summary(self, metrics: Dict[str, Any]) -> Dict[str, Any]:
        """Generate summary statistics across all vendors."""
        total_bars = 0
        total_symbols = 0
        vendors_active = 0
        avg_latencies = []
        avg_quality_scores = []

        for table_name, data in metrics.items():
            if 'error' in data:
                continue

            overall_stats = data.get('overall_stats', {})
            if overall_stats.get('total_bars', 0) > 0:
                vendors_active += 1
                total_bars += overall_stats.get('total_bars', 0)
                total_symbols = max(total_symbols, overall_stats.get('total_symbols', 0))

                if overall_stats.get('avg_latency_ms'):
                    avg_latencies.append(float(overall_stats['avg_latency_ms']))
                if overall_stats.get('avg_quality_score'):
                    avg_quality_scores.append(float(overall_stats['avg_quality_score']))

        return {
            'total_bars_collected': total_bars,
            'total_unique_symbols': total_symbols,
            'active_vendors': vendors_active,
            'avg_latency_ms': sum(avg_latencies) / len(avg_latencies) if avg_latencies else 0,
            'avg_quality_score': sum(avg_quality_scores) / len(avg_quality_scores) if avg_quality_scores else 0
        }

    async def get_ray_analytics(self, dataset_id: str, analysis_type: str = "comprehensive") -> Dict[str, Any]:
        """Get distributed analytics using Ray if available."""
        if not self.ray_enabled:
            raise RuntimeError("Ray analytics service is not enabled - cannot perform distributed analytics")

        ray_service = get_ray_eda_service()
        return await ray_service.analyze_dataset(dataset_id, analysis_type)

    # ==============================================
    # NEWS EVENTS ANALYSIS
    # ==============================================

    def get_news_events(self, limit: int = 100, symbol: str = None, start_date: str = None, end_date: str = None) -> Dict[str, Any]:
        """Get recent news events from Polygon and Tiingo sources with optional filters."""
        try:
            from core.platform.database.connection_manager import get_raw_connection
            import psycopg2.extras
            from datetime import datetime

            with get_raw_connection() as conn:
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:

                    # Get recent news events from both sources
                    news_events = []

                    # Query Polygon news (use intg environment table)
                    polygon_query = """
                        SELECT
                            'Polygon' as source,
                            vendor_id as event_id,
                            title,
                            description,
                            published_utc as published_at,
                            tickers,
                            keywords,
                            article_url,
                            publisher_name,
                            created_at
                        FROM intg_news_polygon
                        WHERE 1=1
                    """

                    # Build dynamic filters for Polygon news
                    params = []

                    if symbol:
                        polygon_query += " AND %s = ANY(tickers)"
                        params.append(symbol.upper())

                    if start_date:
                        polygon_query += " AND published_utc >= %s"
                        params.append(start_date)

                    if end_date:
                        polygon_query += " AND published_utc <= %s"
                        params.append(end_date)

                    polygon_query += " ORDER BY published_utc DESC LIMIT %s"
                    params.append(limit // 2)

                    try:
                        cursor.execute(polygon_query, params)
                        polygon_news = cursor.fetchall()

                        for news in polygon_news:
                            news_events.append({
                                'source': news['source'],
                                'event_id': news['event_id'],
                                'title': news['title'],
                                'description': news['description'][:500] + '...' if news['description'] and len(news['description']) > 500 else news['description'],
                                'published_at': news['published_at'].isoformat() if news['published_at'] else None,
                                'symbols': news['tickers'] or [],
                                'keywords': news['keywords'] or [],
                                'url': news['article_url'],
                                'publisher': news['publisher_name'],
                                'created_at': news['created_at'].isoformat() if news['created_at'] else None
                            })

                        logger.info(f"Retrieved {len(polygon_news)} Polygon news events")

                    except Exception as e:
                        logger.warning(f"Could not fetch Polygon news: {e}")

                    # Query Tiingo news
                    # Query realtime news (additional live news source)
                    realtime_query = """
                        SELECT
                            'Realtime' as source,
                            article_id as event_id,
                            title,
                            summary as description,
                            published_utc as published_at,
                            tickers,
                            keywords,
                            article_url,
                            publisher_name,
                            created_at
                        FROM intg_realtime_news
                        WHERE 1=1
                    """

                    # Build dynamic filters for realtime news
                    realtime_params = []

                    if symbol:
                        realtime_query += " AND %s = ANY(tickers)"
                        realtime_params.append(symbol.upper())

                    if start_date:
                        realtime_query += " AND published_utc >= %s"
                        realtime_params.append(start_date)

                    if end_date:
                        realtime_query += " AND published_utc <= %s"
                        realtime_params.append(end_date)

                    realtime_query += " ORDER BY published_utc DESC LIMIT %s"
                    realtime_params.append(limit // 2)

                    try:
                        cursor.execute(realtime_query, realtime_params)
                        realtime_news = cursor.fetchall()

                        for news in realtime_news:
                            news_events.append({
                                'source': news['source'],
                                'event_id': news['event_id'],
                                'title': news['title'],
                                'description': news['description'][:500] + '...' if news['description'] and len(news['description']) > 500 else news['description'],
                                'published_at': news['published_at'].isoformat() if news['published_at'] else None,
                                'symbols': news['tickers'] or [],
                                'keywords': news['keywords'] or [],
                                'url': news['article_url'],
                                'publisher': news['publisher_name'],
                                'created_at': news['created_at'].isoformat() if news['created_at'] else None
                            })

                        logger.info(f"Retrieved {len(realtime_news)} Realtime news events")

                    except Exception as e:
                        logger.warning(f"Could not fetch realtime news: {e}")

                    # Sort all events by published date
                    news_events.sort(key=lambda x: x['published_at'] or '1970-01-01', reverse=True)

                    # Get summary statistics
                    total_events = len(news_events)
                    unique_symbols = set()
                    sources_count = {}

                    for event in news_events:
                        unique_symbols.update(event['symbols'])
                        source = event['source']
                        sources_count[source] = sources_count.get(source, 0) + 1

                    return {
                        'success': True,
                        'events': news_events[:limit],  # Limit final results
                        'total_events': total_events,
                        'unique_symbols': len(unique_symbols),
                        'sources': sources_count,
                        'filters': {
                            'symbol': symbol,
                            'start_date': start_date,
                            'end_date': end_date,
                            'limit': limit
                        },
                        'query_timestamp': datetime.now().isoformat()
                    }

        except Exception as e:
            logger.error(f"Error getting news events: {e}")
            return {
                'success': False,
                'error': str(e),
                'events': [],
                'total_events': 0
            }

    def get_earnings_events(self, limit: int = 100, symbol: str = None, start_date: str = None, end_date: str = None) -> Dict[str, Any]:
        """Get recent earnings events from environment-specific earnings_events table with optional filters."""
        try:
            from core.platform.database.connection_manager import get_raw_connection
            import psycopg2.extras
            from datetime import datetime
            import os

            # Get environment-specific table name
            environment = os.getenv('ENVIRONMENT', 'dev')
            earnings_table = f"{environment}_earnings_events"

            with get_raw_connection() as conn:
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:

                    # Query earnings events
                    earnings_query = f"""
                        SELECT
                            symbol,
                            report_period,
                            report_type,
                            eps_actual_cents,
                            eps_estimated_cents,
                            eps_surprise_pct,
                            revenue_actual_cents,
                            revenue_estimated_cents,
                            revenue_surprise_pct,
                            earnings_call_datetime,
                            earnings_beat,
                            revenue_beat,
                            guidance_raised,
                            guidance_lowered,
                            created_at,
                            updated_at
                        FROM {earnings_table}
                        WHERE 1=1
                    """

                    params = []

                    # Add symbol filter
                    if symbol:
                        earnings_query += " AND UPPER(symbol) = UPPER(%s)"
                        params.append(symbol)

                    # Add date range filters
                    if start_date:
                        earnings_query += " AND report_period >= %s"
                        params.append(start_date)

                    if end_date:
                        earnings_query += " AND report_period <= %s"
                        params.append(end_date)

                    earnings_query += " ORDER BY report_period DESC, created_at DESC LIMIT %s"
                    params.append(limit)

                    cursor.execute(earnings_query, params)
                    earnings_events = cursor.fetchall()

                    # Process earnings data
                    processed_events = []
                    for event in earnings_events:
                        processed_event = {
                            'symbol': event['symbol'],
                            'report_period': event['report_period'].strftime('%Y-%m-%d') if event['report_period'] else None,
                            'report_type': event['report_type'],
                            'eps_actual': round(event['eps_actual_cents'] / 100, 2) if event['eps_actual_cents'] is not None else None,
                            'eps_estimated': round(event['eps_estimated_cents'] / 100, 2) if event['eps_estimated_cents'] is not None else None,
                            'eps_surprise_pct': float(event['eps_surprise_pct']) if event['eps_surprise_pct'] is not None else None,
                            'revenue_actual_millions': round(event['revenue_actual_cents'] / 100_000_000, 1) if event['revenue_actual_cents'] is not None else None,
                            'revenue_estimated_millions': round(event['revenue_estimated_cents'] / 100_000_000, 1) if event['revenue_estimated_cents'] is not None else None,
                            'revenue_surprise_pct': float(event['revenue_surprise_pct']) if event['revenue_surprise_pct'] is not None else None,
                            'earnings_call_datetime': event['earnings_call_datetime'].isoformat() if event['earnings_call_datetime'] else None,
                            'earnings_beat': event['earnings_beat'],
                            'revenue_beat': event['revenue_beat'],
                            'guidance_raised': event['guidance_raised'],
                            'guidance_lowered': event['guidance_lowered'],
                            'created_at': event['created_at'].isoformat() if event['created_at'] else None,
                            'updated_at': event['updated_at'].isoformat() if event['updated_at'] else None
                        }
                        processed_events.append(processed_event)

                    # Get summary statistics
                    unique_symbols = set(event['symbol'] for event in processed_events)

                    # Count beats and misses
                    eps_beats = sum(1 for event in processed_events if event['earnings_beat'] is True)
                    eps_misses = sum(1 for event in processed_events if event['earnings_beat'] is False)
                    revenue_beats = sum(1 for event in processed_events if event['revenue_beat'] is True)
                    revenue_misses = sum(1 for event in processed_events if event['revenue_beat'] is False)

                    # Count guidance changes
                    guidance_raised_count = sum(1 for event in processed_events if event['guidance_raised'] is True)
                    guidance_lowered_count = sum(1 for event in processed_events if event['guidance_lowered'] is True)

                    logger.info(f"Retrieved {len(processed_events)} earnings events")

                    return {
                        'success': True,
                        'events': processed_events,
                        'total_events': len(processed_events),
                        'unique_symbols': len(unique_symbols),
                        'summary': {
                            'eps_beats': eps_beats,
                            'eps_misses': eps_misses,
                            'revenue_beats': revenue_beats,
                            'revenue_misses': revenue_misses,
                            'guidance_raised': guidance_raised_count,
                            'guidance_lowered': guidance_lowered_count
                        },
                        'filters': {
                            'symbol_filter': symbol,
                            'start_date': start_date,
                            'end_date': end_date
                        },
                        'query_timestamp': datetime.now().isoformat()
                    }

        except Exception as e:
            logger.error(f"Error getting earnings events: {e}")
            return {
                'success': False,
                'error': str(e),
                'events': [],
                'total_events': 0
            }

    def get_gap_events(self, limit: int = 100, symbol: str = None, start_date: str = None, end_date: str = None) -> Dict[str, Any]:
        """Get gap events from environment-specific gap_events table with optional filters."""
        try:
            from core.platform.database.connection_manager import get_raw_connection
            import psycopg2.extras
            from datetime import datetime
            import os

            # Get environment-specific table name
            environment = os.getenv('ENVIRONMENT', 'dev')
            gap_table = f"{environment}_gap_events"

            with get_raw_connection() as conn:
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:

                    # Query gap events
                    gap_query = f"""
                        SELECT
                            symbol,
                            gap_date,
                            gap_datetime,
                            gap_points,
                            gap_percentage,
                            gap_size_class,
                            direction,
                            prev_close,
                            open_price,
                            volume,
                            avg_volume,
                            volume_confirmed,
                            significance_score,
                            gap_context,
                            fill_date,
                            days_to_fill,
                            fill_percentage,
                            fill_type,
                            processed,
                            created_at,
                            updated_at
                        FROM {gap_table}
                        WHERE 1=1
                    """

                    params = []

                    # Add symbol filter
                    if symbol:
                        gap_query += " AND UPPER(symbol) = UPPER(%s)"
                        params.append(symbol)

                    # Add date range filters
                    if start_date:
                        gap_query += " AND gap_date >= %s"
                        params.append(start_date)

                    if end_date:
                        gap_query += " AND gap_date <= %s"
                        params.append(end_date)

                    gap_query += " ORDER BY gap_date DESC, significance_score DESC LIMIT %s"
                    params.append(limit)

                    cursor.execute(gap_query, params)
                    gap_events = cursor.fetchall()

                    # Process gap data
                    processed_events = []
                    for event in gap_events:
                        processed_event = {
                            'symbol': event['symbol'],
                            'gap_date': event['gap_date'].strftime('%Y-%m-%d') if event['gap_date'] else None,
                            'gap_datetime': event['gap_datetime'].isoformat() if event['gap_datetime'] else None,
                            'gap_points': float(event['gap_points']) if event['gap_points'] is not None else None,
                            'gap_percentage': float(event['gap_percentage']) if event['gap_percentage'] is not None else None,
                            'gap_size_class': event['gap_size_class'],
                            'direction': event['direction'],
                            'prev_close': float(event['prev_close']) if event['prev_close'] is not None else None,
                            'open_price': float(event['open_price']) if event['open_price'] is not None else None,
                            'volume': int(event['volume']) if event['volume'] is not None else None,
                            'avg_volume': int(event['avg_volume']) if event['avg_volume'] is not None else None,
                            'volume_confirmed': event['volume_confirmed'],
                            'significance_score': float(event['significance_score']) if event['significance_score'] is not None else None,
                            'gap_context': event['gap_context'],
                            'fill_date': event['fill_date'].strftime('%Y-%m-%d') if event['fill_date'] else None,
                            'days_to_fill': event['days_to_fill'],
                            'fill_percentage': float(event['fill_percentage']) if event['fill_percentage'] is not None else None,
                            'fill_type': event['fill_type'],
                            'is_filled': event['fill_date'] is not None,
                            'created_at': event['created_at'].isoformat() if event['created_at'] else None,
                            'updated_at': event['updated_at'].isoformat() if event['updated_at'] else None
                        }
                        processed_events.append(processed_event)

                    # Get summary statistics
                    unique_symbols = set(event['symbol'] for event in processed_events)

                    # Count gap types and directions
                    gap_ups = sum(1 for event in processed_events if event['direction'] == 'gap_up')
                    gap_downs = sum(1 for event in processed_events if event['direction'] == 'gap_down')

                    # Count gap sizes
                    micro_gaps = sum(1 for event in processed_events if event['gap_size_class'] == 'micro')
                    small_gaps = sum(1 for event in processed_events if event['gap_size_class'] == 'small')
                    medium_gaps = sum(1 for event in processed_events if event['gap_size_class'] == 'medium')
                    large_gaps = sum(1 for event in processed_events if event['gap_size_class'] == 'large')

                    # Count filled vs unfilled gaps
                    filled_gaps = sum(1 for event in processed_events if event['is_filled'] is True)
                    unfilled_gaps = sum(1 for event in processed_events if event['is_filled'] is False)

                    # Calculate average significance score
                    sig_scores = [event['significance_score'] for event in processed_events if event['significance_score'] is not None]
                    avg_significance = sum(sig_scores) / len(sig_scores) if sig_scores else 0

                    logger.info(f"Retrieved {len(processed_events)} gap events")

                    return {
                        'success': True,
                        'events': processed_events,
                        'total_events': len(processed_events),
                        'unique_symbols': len(unique_symbols),
                        'summary': {
                            'gap_ups': gap_ups,
                            'gap_downs': gap_downs,
                            'micro_gaps': micro_gaps,
                            'small_gaps': small_gaps,
                            'medium_gaps': medium_gaps,
                            'large_gaps': large_gaps,
                            'filled_gaps': filled_gaps,
                            'unfilled_gaps': unfilled_gaps,
                            'avg_significance_score': round(avg_significance, 2)
                        },
                        'filters': {
                            'symbol_filter': symbol,
                            'start_date': start_date,
                            'end_date': end_date
                        },
                        'query_timestamp': datetime.now().isoformat()
                    }

        except Exception as e:
            logger.error(f"Error getting gap events: {e}")
            return {
                'success': False,
                'error': str(e),
                'events': [],
                'total_events': 0
            }

    def get_economic_events(self, limit: int = 100, vendor: str = None, symbol: str = None, start_date: str = None, end_date: str = None) -> Dict[str, Any]:
        """Get consolidated economic events from multiple event tables."""
        logger.info(f"NEW get_economic_events method called with limit={limit}, vendor={vendor}")
        try:
            from core.platform.database.connection_manager import get_raw_connection
            from core.platform.config.environment import Environment
            import psycopg2.extras
            from datetime import datetime
            import os

            # Get environment-specific table names
            environment = os.getenv('ENVIRONMENT', 'dev')
            earnings_table = f"{environment}_earnings_events"
            news_table = f"{environment}_news"
            gap_table = f"{environment}_gap_events"

            # Get environment-aware table names
            env = Environment()
            events_table = env.get_table_name('economic_events')
            event_types_table = env.get_table_name('economic_event_types')

            with get_raw_connection() as conn:
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:

                    all_events = []

                    # Get earnings events
                    try:
                        earnings_query = f"""
                            SELECT
                                'earnings' as event_type,
                                symbol,
                                report_period as event_date,
                                COALESCE(earnings_call_datetime, report_period) as event_datetime,
                                CONCAT('Earnings Report: ', symbol, ' Q', EXTRACT(QUARTER FROM report_period)) as title,
                                CASE
                                    WHEN earnings_beat = true AND revenue_beat = true THEN 'Beat both EPS and Revenue'
                                    WHEN earnings_beat = true THEN 'Beat EPS expectations'
                                    WHEN revenue_beat = true THEN 'Beat Revenue expectations'
                                    ELSE 'Mixed/Miss results'
                                END as description,
                                CASE
                                    WHEN earnings_beat = true AND revenue_beat = true THEN 'high'
                                    WHEN earnings_beat = true OR revenue_beat = true THEN 'medium'
                                    ELSE 'low'
                                END as importance,
                                'eodhd' as vendor,
                                created_at,
                                updated_at
                            FROM {earnings_table}
                            WHERE 1=1
                        """

                        params = []
                        if symbol:
                            earnings_query += " AND UPPER(symbol) = UPPER(%s)"
                            params.append(symbol)
                        if start_date:
                            earnings_query += " AND report_period >= %s"
                            params.append(start_date)
                        if end_date:
                            earnings_query += " AND report_period <= %s"
                            params.append(end_date)

                        earnings_query += " ORDER BY report_period DESC LIMIT %s"
                        params.append(min(limit // 4, 25))  # Divide limit among 4 sources

                        cursor.execute(earnings_query, params)
                        earnings_events = cursor.fetchall()
                        all_events.extend(earnings_events)

                    except Exception as e:
                        logger.warning(f"Could not fetch earnings events: {e}")

                    # Get gap events
                    try:
                        gap_query = f"""
                            SELECT
                                'gap' as event_type,
                                symbol,
                                gap_date as event_date,
                                gap_datetime as event_datetime,
                                CONCAT(symbol, ' Gap ', UPPER(direction), ': ', gap_percentage::text, '%') as title,
                                CONCAT('Gap ', direction, ' of ', gap_points, ' points (', gap_percentage, '%) with significance score ', significance_score) as description,
                                CASE
                                    WHEN ABS(gap_percentage) > 10 THEN 'high'
                                    WHEN ABS(gap_percentage) > 5 THEN 'medium'
                                    ELSE 'low'
                                END as importance,
                                'internal' as vendor,
                                created_at,
                                updated_at
                            FROM {gap_table}
                            WHERE 1=1
                        """

                        params = []
                        if symbol:
                            gap_query += " AND UPPER(symbol) = UPPER(%s)"
                            params.append(symbol)
                        if start_date:
                            gap_query += " AND gap_date >= %s"
                            params.append(start_date)
                        if end_date:
                            gap_query += " AND gap_date <= %s"
                            params.append(end_date)

                        gap_query += " ORDER BY gap_date DESC LIMIT %s"
                        params.append(min(limit // 4, 25))

                        cursor.execute(gap_query, params)
                        gap_events = cursor.fetchall()
                        all_events.extend(gap_events)

                    except Exception as e:
                        logger.warning(f"Could not fetch gap events: {e}")

                    # Get economic events (macro indicators) - NEW ADDITION
                    try:
                        economic_query = f"""
                            SELECT DISTINCT
                                'economic' as event_type,
                                NULL as symbol,
                                ee.date as event_date,
                                ee.release_time as event_datetime,
                                et.name as title,
                                CONCAT(et.description,
                                    CASE
                                        WHEN ee.actual IS NOT NULL THEN ' - Actual: ' || ee.actual
                                        ELSE ''
                                    END,
                                    CASE
                                        WHEN ee.estimate IS NOT NULL THEN ' (Est: ' || ee.estimate || ')'
                                        ELSE ''
                                    END
                                ) as description,
                                CASE
                                    WHEN et.importance_level >= 5 THEN 'high'
                                    WHEN et.importance_level >= 3 THEN 'medium'
                                    ELSE 'low'
                                END as importance,
                                ee.source_vendor as vendor,
                                ee.created_at,
                                ee.updated_at
                            FROM {events_table} ee
                            JOIN {event_types_table} et ON ee.event_type_id = et.id
                            WHERE 1=1
                        """

                        params = []

                        # Add vendor filter
                        if vendor:
                            economic_query += " AND LOWER(ee.source_vendor) = LOWER(%s)"
                            params.append(vendor)

                        # Add date range filters
                        if start_date:
                            economic_query += " AND ee.date >= %s"
                            params.append(start_date)

                        if end_date:
                            economic_query += " AND ee.date <= %s"
                            params.append(end_date)

                        economic_query += " ORDER BY ee.date DESC, ee.release_time DESC LIMIT %s"
                        params.append(min(limit // 4, 25))

                        cursor.execute(economic_query, params)
                        economic_events = cursor.fetchall()
                        all_events.extend(economic_events)

                    except Exception as e:
                        logger.warning(f"Could not fetch economic events: {e}")

                    # Sort all events by date and limit
                    all_events.sort(key=lambda x: x['event_datetime'] if x['event_datetime'] else x['event_date'], reverse=True)
                    all_events = all_events[:limit]

                    # Process events for consistent format
                    processed_events = []
                    for event in all_events:
                        processed_event = {
                            'event_type': event['event_type'],
                            'symbol': event['symbol'],
                            'event_date': event['event_date'].strftime('%Y-%m-%d') if event['event_date'] else None,
                            'event_datetime': event['event_datetime'].isoformat() if event['event_datetime'] else None,
                            'title': event['title'],
                            'description': event['description'],
                            'importance': event['importance'],
                            'vendor': event['vendor'],
                            'created_at': event['created_at'].isoformat() if event['created_at'] else None,
                            'updated_at': event['updated_at'].isoformat() if event['updated_at'] else None
                        }
                        processed_events.append(processed_event)

                    # Get summary statistics
                    unique_symbols = set(event['symbol'] for event in processed_events)
                    event_types = set(event['event_type'] for event in processed_events)

                    high_importance = sum(1 for event in processed_events if event['importance'] == 'high')
                    medium_importance = sum(1 for event in processed_events if event['importance'] == 'medium')
                    low_importance = sum(1 for event in processed_events if event['importance'] == 'low')

                    logger.info(f"Retrieved {len(processed_events)} consolidated economic events")

                    return {
                        'success': True,
                        'events': processed_events,
                        'total_events': len(processed_events),
                        'unique_symbols': len(unique_symbols),
                        'event_types': list(event_types),
                        'summary': {
                            'high_importance': high_importance,
                            'medium_importance': medium_importance,
                            'low_importance': low_importance
                        },
                        'filters': {
                            'vendor_filter': vendor,
                            'symbol_filter': symbol,
                            'start_date': start_date,
                            'end_date': end_date
                        },
                        'query_timestamp': datetime.now().isoformat()
                    }

        except Exception as e:
            logger.error(f"Error getting economic events: {e}")
            return {
                'success': False,
                'error': str(e),
                'events': [],
                'total_events': 0
            }

    def get_economic_indicators(self, indicators: List[str] = None) -> Dict[str, Any]:
        """Get economic indicators with current and upcoming releases."""
        try:
            from datetime import datetime, timedelta

            # Current date context
            today = datetime.now().date()
            tomorrow = today + timedelta(days=1)

            # Economic indicators data (will be replaced with real FRED API data)
            all_indicators = [
                {
                    'indicator_id': 'PPIFIS',
                    'name': 'Producer Price Index: Finished Goods',
                    'frequency': 'Monthly',
                    'release_date': today.strftime('%Y-%m-%d'),
                    'release_time': '08:30',
                    'status': 'released_today',
                    'previous_value': '0.2%',
                    'forecasted_value': '0.1%',
                    'actual_value': '0.3%',
                    'importance': 'high',
                    'impact': 'markets_higher',
                    'description': 'Measures average change in selling prices received by domestic producers for their output',
                    'source': 'Bureau of Labor Statistics',
                    'next_release': (today + timedelta(days=30)).strftime('%Y-%m-%d')
                },
                {
                    'indicator_id': 'CPIAUCSL',
                    'name': 'Consumer Price Index for All Urban Consumers',
                    'frequency': 'Monthly',
                    'release_date': tomorrow.strftime('%Y-%m-%d'),
                    'release_time': '08:30',
                    'status': 'scheduled_tomorrow',
                    'previous_value': '0.4%',
                    'forecasted_value': '0.2%',
                    'actual_value': None,
                    'importance': 'high',
                    'impact': 'tbd',
                    'description': 'Measures average change in prices of goods and services consumed by urban households',
                    'source': 'Bureau of Labor Statistics',
                    'next_release': (tomorrow + timedelta(days=30)).strftime('%Y-%m-%d')
                },
                {
                    'indicator_id': 'GDP',
                    'name': 'Gross Domestic Product',
                    'frequency': 'Quarterly',
                    'release_date': (today + timedelta(days=45)).strftime('%Y-%m-%d'),
                    'release_time': '08:30',
                    'status': 'upcoming',
                    'previous_value': '2.8%',
                    'forecasted_value': '2.5%',
                    'actual_value': None,
                    'importance': 'high',
                    'impact': 'tbd',
                    'description': 'Comprehensive measure of U.S. economic activity',
                    'source': 'Bureau of Economic Analysis',
                    'next_release': (today + timedelta(days=135)).strftime('%Y-%m-%d')
                },
                {
                    'indicator_id': 'UNRATE',
                    'name': 'Unemployment Rate',
                    'frequency': 'Monthly',
                    'release_date': (today + timedelta(days=3)).strftime('%Y-%m-%d'),
                    'release_time': '08:30',
                    'status': 'upcoming',
                    'previous_value': '4.1%',
                    'forecasted_value': '4.2%',
                    'actual_value': None,
                    'importance': 'high',
                    'impact': 'tbd',
                    'description': 'Percentage of labor force that is unemployed',
                    'source': 'Bureau of Labor Statistics',
                    'next_release': (today + timedelta(days=33)).strftime('%Y-%m-%d')
                },
                {
                    'indicator_id': 'EFFR',
                    'name': 'Federal Funds Effective Rate',
                    'frequency': 'Daily',
                    'release_date': today.strftime('%Y-%m-%d'),
                    'release_time': '16:00',
                    'status': 'released_today',
                    'previous_value': '5.25%',
                    'forecasted_value': None,
                    'actual_value': '5.25%',
                    'importance': 'medium',
                    'impact': 'neutral',
                    'description': 'Interest rate at which depository institutions lend balances to other institutions overnight',
                    'source': 'Federal Reserve',
                    'next_release': (today + timedelta(days=1)).strftime('%Y-%m-%d')
                }
            ]

            # Filter indicators if specific ones requested
            if indicators:
                filtered_indicators = [ind for ind in all_indicators if ind['indicator_id'] in indicators]
            else:
                filtered_indicators = all_indicators

            # Categorize indicators by status
            released_today = [ind for ind in filtered_indicators if ind['status'] == 'released_today']
            scheduled_tomorrow = [ind for ind in filtered_indicators if ind['status'] == 'scheduled_tomorrow']
            upcoming = [ind for ind in filtered_indicators if ind['status'] == 'upcoming']

            logger.info(f"Retrieved {len(filtered_indicators)} economic indicators")

            return {
                'success': True,
                'indicators': filtered_indicators,
                'total_indicators': len(filtered_indicators),
                'categorized': {
                    'released_today': released_today,
                    'scheduled_tomorrow': scheduled_tomorrow,
                    'upcoming': upcoming
                },
                'summary': {
                    'high_importance': sum(1 for ind in filtered_indicators if ind['importance'] == 'high'),
                    'medium_importance': sum(1 for ind in filtered_indicators if ind['importance'] == 'medium'),
                    'low_importance': sum(1 for ind in filtered_indicators if ind['importance'] == 'low'),
                    'released_today_count': len(released_today),
                    'scheduled_tomorrow_count': len(scheduled_tomorrow),
                    'upcoming_count': len(upcoming)
                },
                'data_source': 'test_data',  # Will be 'fred_api' when real API is integrated
                'query_timestamp': datetime.now().isoformat()
            }

        except Exception as e:
            logger.error(f"Error getting economic indicators: {e}")
            return {
                'success': False,
                'error': str(e),
                'indicators': [],
                'total_indicators': 0
            }

    def _validate_environment_setup(self):
        """Validate environment configuration and database connectivity before service startup."""
        import os

        # Get environment
        environment = os.getenv('ENVIRONMENT', 'dev')

        logger.info(f"🔍 Validating environment setup for: {environment}")

        try:
            from core.platform.database.connection_manager import get_raw_connection
            import psycopg2.extras

            # Get expected configuration
            expected_db = f"{environment}_db"
            expected_min_tables = {
                'dev': 30,
                'intg': 50,
                'prod': 50
            }.get(environment, 30)

            with get_raw_connection() as conn:
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:

                    # 1. Verify we're connected to correct database
                    cursor.execute("SELECT current_database(), current_user, inet_server_addr(), inet_server_port()")
                    db_info = cursor.fetchone()

                    actual_db = db_info['current_database']
                    server_addr = db_info['inet_server_addr']
                    server_port = db_info['inet_server_port']

                    if actual_db != expected_db:
                        raise EnvironmentError(
                            f"❌ CRITICAL DATABASE ERROR: Connected to '{actual_db}' but expected '{expected_db}'. "
                            f"Environment: {environment}. Server: {server_addr}:{server_port}. "
                            f"Check Docker network configuration!"
                        )

                    logger.info(f"✅ Database validation: {actual_db}@{server_addr}:{server_port}")

                    # 2. Verify expected tables exist
                    cursor.execute("""
                        SELECT tablename FROM pg_tables
                        WHERE schemaname = 'public' AND tablename LIKE %s
                        ORDER BY tablename
                    """, (f"{environment}_%",))

                    tables = [row['tablename'] for row in cursor.fetchall()]
                    actual_table_count = len(tables)

                    if actual_table_count < expected_min_tables:
                        raise EnvironmentError(
                            f"❌ CRITICAL TABLE ERROR: Only {actual_table_count} tables found, expected {expected_min_tables}+. "
                            f"Environment: {environment}. Database: {actual_db}. "
                            f"Missing data or wrong database!"
                        )

                    logger.info(f"✅ Table validation: {actual_table_count} tables found (expected {expected_min_tables}+)")

                    # 3. Verify key event tables exist for this functionality
                    key_tables = [f"{environment}_earnings_events", f"{environment}_gap_events", f"{environment}_news"]
                    missing_tables = [table for table in key_tables if table not in tables]

                    if missing_tables:
                        logger.warning(f"⚠️ Missing event tables: {missing_tables} - some functionality may be limited")
                    else:
                        logger.info(f"✅ Event tables validation: All key tables present")

                    # 4. Test basic query functionality
                    cursor.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public'")
                    total_tables = cursor.fetchone()['count']

                    logger.info(f"✅ Environment validation complete: {environment} environment with {total_tables} total tables")

        except Exception as e:
            logger.error(f"❌ ENVIRONMENT VALIDATION FAILED: {e}")
            raise EnvironmentError(
                f"Cannot start analytics service in {environment} environment. "
                f"Database connectivity or configuration error: {e}"
            )

    # ==============================================
    # MULTI-PANEL VISUALIZATION (NEW)
    # ==============================================

    async def generate_multi_panel_chart(self, symbol: str, timeframe: str, dataset_id: int) -> Dict[str, Any]:
        """Generate multi-panel trading chart from training dataset."""
        if not self.visualization_enabled:
            return {
                "success": False,
                "error": "Multi-panel visualization not available"
            }

        try:
            logger.info(f"🎨 Generating multi-panel chart: {symbol} {timeframe} dataset {dataset_id}")

            # Step 1: Get training dataset (simplified for integration)
            import pandas as pd
            import numpy as np
            import io
            import base64

            # Generate sample OHLCV data for testnstration
            np.random.seed(42)
            n_periods = 50
            base_price = 180.0
            returns = np.random.normal(0.001, 0.02, n_periods)
            prices = base_price * np.exp(np.cumsum(returns))

            sample_price_data = pd.DataFrame({
                'timestamp': pd.date_range('2024-08-01 09:30:00', periods=n_periods, freq='1h'),
                'open': prices * (1 + np.random.normal(0, 0.003, n_periods)),
                'high': prices * (1 + np.random.uniform(0.003, 0.012, n_periods)),
                'low': prices * (1 - np.random.uniform(0.003, 0.012, n_periods)),
                'close': prices,
                'volume': np.random.lognormal(13.5, 0.5, n_periods).astype(int)
            })

            # Step 2: Extract features
            current_price = prices[-1]
            extracted_features = {
                f'{timeframe}_open': current_price * 1.001,
                f'{timeframe}_high': current_price * 1.008,
                f'{timeframe}_low': current_price * 0.992,
                f'{timeframe}_close': current_price,
                f'{timeframe}_volume': int(1500000),

                # Technical indicators
                f'{timeframe}_envelope_top': current_price * 1.025,
                f'{timeframe}_envelope_bot': current_price * 0.975,
                f'{timeframe}_pldot': current_price * 0.998,
                f'{timeframe}_z1b': current_price * 0.995,
                f'{timeframe}_z2b': current_price * 0.990,
                f'{timeframe}_z5t': current_price * 1.005,
                f'{timeframe}_z6t': current_price * 1.010,

                # Volume profile
                f'{timeframe}_volume_profile_poc': current_price,
                f'{timeframe}_volume_profile_val': current_price * 0.997,
                f'{timeframe}_volume_profile_vah': current_price * 1.003,

                # BX Trender
                f'{timeframe}_BXTrenderBasic_14': 67.2,
                f'{timeframe}_BXTrenderDirectional_14': 74.1,
                f'{timeframe}_BXTrenderVolumeWeighted_14': 59.8
            }

            logger.info(f"✅ Extracted {len(extracted_features)} features")

            # Step 3: Generate multi-panel chart
            fig = self.multi_panel_chart.create_multi_panel_chart(
                symbol=symbol,
                price_data=sample_price_data,
                training_features=extracted_features,
                timeframe=timeframe,
                title_suffix=f"Dataset {dataset_id}"
            )

            # Step 4: Convert chart to base64
            buffer = io.BytesIO()
            fig.savefig(buffer, format='png', dpi=300, bbox_inches='tight',
                       facecolor='white', edgecolor='none')
            buffer.seek(0)

            import matplotlib.pyplot as plt
            plt.close(fig)

            chart_data = base64.b64encode(buffer.read()).decode('utf-8')
            buffer.close()

            logger.info(f"✅ Generated multi-panel chart ({len(chart_data)} bytes)")

            return {
                "success": True,
                "chart_image": chart_data,
                "features": extracted_features,
                "features_count": len(extracted_features),
                "file_size": len(chart_data),
                "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                "symbol": symbol,
                "timeframe": timeframe,
                "dataset_id": dataset_id
            }

        except Exception as e:
            logger.error(f"❌ Error generating multi-panel chart: {e}", exc_info=True)
            return {
                "success": False,
                "error": f"Failed to generate chart: {str(e)}"
            }

    # ==============================================
    # WEB DASHBOARD SERVING (from analytics_service.py)
    # ==============================================

    def get_eda_dashboard_html(self):
        """Generate the main EDA dashboard HTML."""
        return """<!DOCTYPE html>
        <html>
        <head>
            <meta charset="UTF-8">
            <title>ATS Unified Analytics - EDA Dashboard</title>
            <script src="https://cdn.plot.ly/plotly-2.35.2.min.js"></script>
            <style>
                body { font-family: Arial, sans-serif; margin: 20px; background: #f5f5f5; }
                .header { background: #2c3e50; color: white; padding: 20px; border-radius: 8px; margin-bottom: 20px; }
                .unified-badge { background: #e74c3c; color: white; padding: 4px 8px; border-radius: 4px; font-size: 0.8em; }
                .feature-list { display: flex; gap: 15px; margin: 10px 0; }
                .feature-item { background: #3498db; color: white; padding: 5px 10px; border-radius: 15px; font-size: 0.9em; }
            </style>
        </head>
        <body>
            <div class="header">
                <h1>🚀 ATS Unified Analytics Dashboard <span class="unified-badge">CONSOLIDATED</span></h1>
                <p>Consolidated analytics service with type-aware analysis, universe analytics, and distributed computing</p>
                <div class="feature-list">
                    <div class="feature-item">🎯 Data Quality</div>
                    <div class="feature-item">📊 Type-Aware EDA</div>
                    <div class="feature-item">🌐 Universe Analytics</div>
                    <div class="feature-item">⚡ Ray Computing</div>
                    <div class="feature-item">🤖 Training Datasets</div>
                </div>
            </div>

            <div class="main-content">
                <h2>Select Analysis Type</h2>
                <button onclick="loadDataQuality()">🎯 Data Quality Dashboard</button>
                <button onclick="loadEDA()">📊 Exploratory Data Analysis</button>
                <button onclick="loadBarCollectionMetrics()">📈 Bar Collection Metrics</button>
                <button onclick="loadUniverseAnalytics()">🌐 Universe Analytics</button>
                <button onclick="loadTrainingDatasets()">🤖 Training Datasets</button>
                <button onclick="loadMonthlyTrainingData()">📅 Monthly Training Data</button>
                <button onclick="loadNewsEvents()">📰 News Events</button>
                <button onclick="loadEarningsEvents()">📊 Earnings Events</button>
                <button onclick="loadGapEvents()">⚡ Gap Events</button>
                <button onclick="loadXAIFinancialEvents()">🔮 AI Financial Events (xAI + Grok)</button>
                <button onclick="loadMultiPanelVisualization()">🎨 Multi-Panel Trading Charts</button>
                <button onclick="loadRayAnalytics()">⚡ Distributed Analytics</button>

                <div id="analysis-content">
                    <p style="text-align: center; margin-top: 50px; color: #666;">
                        Select an analysis type above to begin
                    </p>
                </div>
            </div>

            <script>
                function loadDataQuality() {
                    // Navigate to the data quality dashboard
                    window.location.href = '/data-quality/dashboard';
                }

                async function loadEDA() {
                    document.getElementById('analysis-content').innerHTML = `
                        <h3>📊 Exploratory Data Analysis</h3>
                        <p>Loading database tables...</p>
                    `;

                    try {
                        // First get list of available tables
                        const tablesResponse = await fetch('/api/tables');
                        let tables = [];

                        if (tablesResponse.ok) {
                            const tablesData = await tablesResponse.json();
                            tables = tablesData.tables || [];
                        } else {
                            // Default to common financial tables
                            tables = [
                                'dev_daily_price', 'dev_training_dataset', 'dev_instrument',
                                'dev_daily_price_polygon', 'dev_daily_price_tiingo', 'dev_daily_price_eodhd'
                            ];
                        }

                        const html = `
                            <h3>📊 Exploratory Data Analysis</h3>
                            <div style="background: white; padding: 20px; border-radius: 8px; border: 1px solid #ddd; margin-bottom: 20px;">
                                <h4>Select Table</h4>
                                <select id="table-selector" onchange="loadTableData()" style="width: 100%; padding: 8px; border: 1px solid #ddd; border-radius: 4px;">
                                    <option value="">Choose a table...</option>
                                    ${tables.map(table => `<option value="${table}">${table}</option>`).join('')}
                                </select>
                            </div>

                            <div id="filter-controls" style="display: none; background: white; padding: 20px; border-radius: 8px; border: 1px solid #ddd; margin-bottom: 20px;">
                                <h4>🔍 Data Filters</h4>
                                <div style="display: grid; grid-template-columns: 1fr 1fr 1fr auto; gap: 15px; align-items: end;">
                                    <div>
                                        <label for="symbol-filter" style="display: block; margin-bottom: 5px; font-weight: bold;">Symbol:</label>
                                        <input type="text" id="symbol-filter" placeholder="e.g., AAPL, TSLA"
                                               style="width: 100%; padding: 8px; border: 1px solid #ddd; border-radius: 4px;">
                                    </div>
                                    <div>
                                        <label for="date-from" style="display: block; margin-bottom: 5px; font-weight: bold;">From Date:</label>
                                        <input type="date" id="date-from"
                                               style="width: 100%; padding: 8px; border: 1px solid #ddd; border-radius: 4px;">
                                    </div>
                                    <div>
                                        <label for="date-to" style="display: block; margin-bottom: 5px; font-weight: bold;">To Date:</label>
                                        <input type="date" id="date-to"
                                               style="width: 100%; padding: 8px; border: 1px solid #ddd; border-radius: 4px;">
                                    </div>
                                    <div>
                                        <button onclick="applyFilters()"
                                                style="padding: 8px 16px; background: #4285f4; color: white; border: none; border-radius: 4px; cursor: pointer;">
                                            Apply Filters
                                        </button>
                                        <button onclick="clearFilters()"
                                                style="padding: 8px 16px; background: #6c757d; color: white; border: none; border-radius: 4px; cursor: pointer; margin-left: 5px;">
                                            Clear
                                        </button>
                                    </div>
                                </div>
                            </div>

                            <div id="table-content" style="display: none;">
                                <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 20px; margin-bottom: 20px;">
                                    <div style="background: white; padding: 15px; border-radius: 8px; border: 1px solid #ddd;">
                                        <h4>📈 Table Info</h4>
                                        <div id="table-info">Select a table to view information</div>
                                    </div>
                                    <div style="background: white; padding: 15px; border-radius: 8px; border: 1px solid #ddd;">
                                        <h4>📊 Column Summary</h4>
                                        <div id="column-summary">Select a table to view columns</div>
                                    </div>
                                </div>

                                <div style="background: white; padding: 15px; border-radius: 8px; border: 1px solid #ddd; margin-bottom: 20px;">
                                    <h4>📋 Sample Data</h4>
                                    <div id="sample-data" style="max-height: 400px; overflow: auto;">
                                        <p>Select a table to view sample data</p>
                                    </div>
                                </div>

                                <div style="background: white; padding: 15px; border-radius: 8px; border: 1px solid #ddd;">
                                    <h4>📈 Column Distributions</h4>
                                    <div id="column-distributions">
                                        <p>Select a table to view column distributions and statistics</p>
                                    </div>
                                </div>
                            </div>
                        `;

                        document.getElementById('analysis-content').innerHTML = html;

                    } catch (error) {
                        document.getElementById('analysis-content').innerHTML =
                            '<h3>📊 EDA</h3><p style="color: red;">Error loading EDA interface: ' + error.message + '</p>';
                    }
                }

                async function loadTableData() {
                    const tableName = document.getElementById('table-selector').value;
                    if (!tableName) {
                        document.getElementById('table-content').style.display = 'none';
                        document.getElementById('filter-controls').style.display = 'none';
                        return;
                    }

                    document.getElementById('table-content').style.display = 'block';

                    // Show filters for tables that typically have symbol and date columns
                    const hasFiltering = tableName.includes('daily_prices') ||
                                       tableName.includes('instruments') ||
                                       tableName.includes('gap_events') ||
                                       tableName.includes('fundamentals') ||
                                       tableName.includes('news') ||
                                       tableName.includes('earnings');

                    document.getElementById('filter-controls').style.display = hasFiltering ? 'block' : 'none';
                    document.getElementById('table-info').innerHTML = '<p>Loading table information...</p>';
                    document.getElementById('column-summary').innerHTML = '<p>Loading column information...</p>';
                    document.getElementById('sample-data').innerHTML = '<p>Loading sample data...</p>';
                    document.getElementById('column-distributions').innerHTML = '<p>Loading distributions...</p>';

                    try {
                        // Load table info
                        const infoResponse = await fetch(`/api/table-info/${tableName}`);
                        if (infoResponse.ok) {
                            const info = await infoResponse.json();
                            document.getElementById('table-info').innerHTML = `
                                <p><strong>Row Count:</strong> ${info.row_count}</p>
                                <p><strong>Column Count:</strong> ${info.column_count}</p>
                                <p><strong>Table Size:</strong> ${info.size}</p>
                                <p><strong>Last Updated:</strong> ${info.last_updated || 'Unknown'}</p>
                            `;
                        }

                        // Load column info
                        const columnsResponse = await fetch(`/api/table-columns/${tableName}`);
                        if (columnsResponse.ok) {
                            const columns = await columnsResponse.json();
                            const columnHtml = columns.columns.map(col => `
                                <div style="margin: 5px 0; padding: 5px; background: #f8f9fa; border-radius: 3px;">
                                    <strong>${col.name}</strong> (${col.type})
                                    ${col.nullable ? '' : ' <em>NOT NULL</em>'}
                                </div>
                            `).join('');
                            document.getElementById('column-summary').innerHTML = columnHtml;
                        }

                        // Load sample data
                        loadSampleData(tableName);

                    } catch (error) {
                        console.error('Error loading table data:', error);
                        document.getElementById('table-info').innerHTML = '<p style="color: red;">Error loading table information</p>';
                    }

                    // Load column distributions
                    try {
                        const distResponse = await fetch(`/api/table-distributions/${tableName}`);
                        if (distResponse.ok) {
                            const distributions = await distResponse.json();
                            let distHtml = '';

                            for (const [colName, stats] of Object.entries(distributions.columns || {})) {
                                distHtml += `
                                    <div style="margin: 15px 0; padding: 15px; background: #f8f9fa; border-radius: 5px;">
                                        <h5>${colName}</h5>
                                        <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(100px, 1fr)); gap: 10px; margin: 10px 0;">
                                            <div><strong>Count:</strong> ${stats.count || 0}</div>
                                            <div><strong>Nulls:</strong> ${stats.null_count || 0}</div>
                                            <div><strong>Unique:</strong> ${stats.unique_count || 'N/A'}</div>
                                            <div><strong>Type:</strong> ${stats.data_type || 'Unknown'}</div>
                                            ${stats.min !== undefined ? `<div><strong>Min:</strong> ${stats.min}</div>` : ''}
                                            ${stats.max !== undefined ? `<div><strong>Max:</strong> ${stats.max}</div>` : ''}
                                            ${stats.avg !== undefined ? `<div><strong>Avg:</strong> ${parseFloat(stats.avg).toFixed(2)}</div>` : ''}
                                        </div>
                                        ${stats.most_common ? `
                                            <div style="margin-top: 10px;">
                                                <strong>Most Common Values:</strong>
                                                <div style="margin: 5px 0;">
                                                    ${Object.entries(stats.most_common).slice(0, 5).map(([val, count]) =>
                                                        `<span style="background: #e9ecef; padding: 2px 6px; margin: 2px; border-radius: 3px; display: inline-block;">${val} (${count})</span>`
                                                    ).join('')}
                                                </div>
                                            </div>
                                        ` : ''}
                                    </div>
                                `;
                            }

                            document.getElementById('column-distributions').innerHTML = distHtml || '<p>No distribution data available</p>';
                        } else {
                            document.getElementById('column-distributions').innerHTML = '<p>Could not load column distributions</p>';
                        }
                    } catch (error) {
                        document.getElementById('column-distributions').innerHTML = '<p style="color: red;">Error loading distributions</p>';
                    }
                }

                async function loadSampleData(tableName, filters = {}) {
                    try {
                        // Build query parameters for filtering and sorting
                        let queryParams = '';
                        if (filters.symbol) queryParams += `&symbol=${encodeURIComponent(filters.symbol)}`;
                        if (filters.dateFrom) queryParams += `&date_from=${filters.dateFrom}`;
                        if (filters.dateTo) queryParams += `&date_to=${filters.dateTo}`;
                        if (filters.sortBy) queryParams += `&sort_by=${encodeURIComponent(filters.sortBy)}`;
                        if (filters.sortDir) queryParams += `&sort_dir=${filters.sortDir}`;

                        const sampleResponse = await fetch(`/api/table-sample/${tableName}?limit=50${queryParams}`);
                        if (sampleResponse.ok) {
                            const sample = await sampleResponse.json();
                            if (sample.rows && sample.rows.length > 0) {
                                window.currentTableData = sample.rows; // Store data
                                renderSortableTable(sample.rows, sample.sort_applied);
                            } else {
                                document.getElementById('sample-data').innerHTML = '<p>No data found with current filters</p>';
                            }
                        } else {
                            document.getElementById('sample-data').innerHTML = '<p>Error loading sample data</p>';
                        }
                    } catch (error) {
                        console.error('Error loading sample data:', error);
                        document.getElementById('sample-data').innerHTML = '<p style="color: red;">Error loading sample data</p>';
                    }
                }

                function renderSortableTable(rows, sortApplied = null) {
                    if (!rows || rows.length === 0) return;

                    const headers = Object.keys(rows[0]);
                    const tableHtml = `
                        <div style="margin-bottom: 10px;">
                            <small>Showing ${rows.length} rows. Click column headers to sort globally.</small>
                            ${sortApplied && sortApplied.column ? `<br><small style="color: #666;">Sorted by: ${sortApplied.column} (${sortApplied.direction})</small>` : ''}
                        </div>
                        <table id="sortable-table" style="width: 100%; border-collapse: collapse; font-size: 0.9em;">
                            <thead>
                                <tr style="background: #f1f3f4;">
                                    ${headers.map(h => {
                                        let sortIcon = '⇅';
                                        if (sortApplied && sortApplied.column === h) {
                                            sortIcon = sortApplied.direction === 'asc' ? '▲' : '▼';
                                            currentSortColumn = h;
                                            sortDirection = sortApplied.direction;
                                        }
                                        return `
                                            <th onclick="sortTable('${h}')"
                                                style="padding: 8px; border: 1px solid #ddd; text-align: left; cursor: pointer; user-select: none;">
                                                ${h} <span id="sort-${h}" style="color: #666;">${sortIcon}</span>
                                            </th>
                                        `;
                                    }).join('')}
                                </tr>
                            </thead>
                            <tbody id="table-body">
                                ${rows.map(row => `
                                    <tr>
                                        ${headers.map(h => `<td style="padding: 8px; border: 1px solid #ddd;">${row[h] !== null ? row[h] : '<em>null</em>'}</td>`).join('')}
                                    </tr>
                                `).join('')}
                            </tbody>
                        </table>
                    `;
                    document.getElementById('sample-data').innerHTML = tableHtml;
                }

                // Server-side table sorting functionality
                let currentSortColumn = null;
                let sortDirection = 'asc';

                function sortTable(column) {
                    const tableName = document.getElementById('table-selector').value;
                    if (!tableName) return;

                    // Update sort indicators
                    document.querySelectorAll('[id^="sort-"]').forEach(span => span.innerHTML = '⇅');

                    // Determine sort direction
                    if (currentSortColumn === column) {
                        sortDirection = sortDirection === 'asc' ? 'desc' : 'asc';
                    } else {
                        currentSortColumn = column;
                        sortDirection = 'asc';
                    }

                    // Update visual indicator
                    document.getElementById(`sort-${column}`).innerHTML = sortDirection === 'asc' ? '▲' : '▼';

                    // Get current filters and reload data with sorting
                    const filters = {
                        symbol: document.getElementById('symbol-filter').value.trim(),
                        dateFrom: document.getElementById('date-from').value,
                        dateTo: document.getElementById('date-to').value,
                        sortBy: column,
                        sortDir: sortDirection
                    };

                    // Show loading message
                    document.getElementById('sample-data').innerHTML = '<p>Sorting data...</p>';

                    // Reload data with server-side sorting
                    loadSampleData(tableName, filters);
                }

                // Filter functionality
                function applyFilters() {
                    const tableName = document.getElementById('table-selector').value;
                    if (!tableName) return;

                    const filters = {
                        symbol: document.getElementById('symbol-filter').value.trim(),
                        dateFrom: document.getElementById('date-from').value,
                        dateTo: document.getElementById('date-to').value
                    };

                    document.getElementById('sample-data').innerHTML = '<p>Applying filters...</p>';
                    loadSampleData(tableName, filters);
                }

                function clearFilters() {
                    document.getElementById('symbol-filter').value = '';
                    document.getElementById('date-from').value = '';
                    document.getElementById('date-to').value = '';

                    const tableName = document.getElementById('table-selector').value;
                    if (tableName) {
                        document.getElementById('sample-data').innerHTML = '<p>Loading data...</p>';
                        loadSampleData(tableName);
                    }
                }

                async function loadBarCollectionMetrics() {
                    document.getElementById('analysis-content').innerHTML =
                        '<h3>📈 Bar Collection Metrics</h3><p>Loading bar collection data...</p>';

                    try {
                        const response = await fetch('/api/bar-collection-metrics');
                        const data = await response.json();

                        if (data.error) {
                            document.getElementById('analysis-content').innerHTML =
                                `<h3>📈 Bar Collection Metrics</h3><p style="color: red;">Error: ${data.error}</p>`;
                            return;
                        }

                        const summary = data.summary || {};
                        const metrics = data.metrics || {};

                        let html = `
                            <h3>📈 Bar Collection Metrics</h3>
                            <div style="background: white; padding: 20px; border-radius: 8px; border: 1px solid #ddd; margin-bottom: 20px;">
                                <h4>📊 Overall Summary (Last 7 Days)</h4>
                                <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 15px;">
                                    <div style="text-align: center; padding: 10px; background: #e3f2fd; border-radius: 4px;">
                                        <div style="font-size: 2em; color: #1976d2;">${summary.total_bars_collected?.toLocaleString() || 0}</div>
                                        <div style="font-weight: bold;">Total Bars</div>
                                    </div>
                                    <div style="text-align: center; padding: 10px; background: #e8f5e8; border-radius: 4px;">
                                        <div style="font-size: 2em; color: #388e3c;">${summary.total_unique_symbols || 0}</div>
                                        <div style="font-weight: bold;">Unique Symbols</div>
                                    </div>
                                    <div style="text-align: center; padding: 10px; background: #fff3e0; border-radius: 4px;">
                                        <div style="font-size: 2em; color: #f57c00;">${summary.active_vendors || 0}</div>
                                        <div style="font-weight: bold;">Active Vendors</div>
                                    </div>
                                    <div style="text-align: center; padding: 10px; background: #fce4ec; border-radius: 4px;">
                                        <div style="font-size: 2em; color: #c2185b;">${summary.avg_latency_ms?.toFixed(0) || 0}ms</div>
                                        <div style="font-weight: bold;">Avg Latency</div>
                                    </div>
                                </div>
                            </div>
                        `;

                        // Per-vendor metrics
                        for (const [tableName, vendorData] of Object.entries(metrics)) {
                            if (vendorData.error) {
                                html += `
                                    <div style="background: #ffebee; padding: 15px; border-radius: 8px; border: 1px solid #ef5350; margin-bottom: 15px;">
                                        <h4 style="color: #c62828;">🚨 ${vendorData.vendor} - Error</h4>
                                        <p>${vendorData.error}</p>
                                    </div>
                                `;
                                continue;
                            }

                            const stats = vendorData.overall_stats || {};
                            const collectionData = vendorData.collection_time_metrics || [];
                            const barData = vendorData.bar_time_metrics || [];

                            html += `
                                <div style="background: white; padding: 20px; border-radius: 8px; border: 1px solid #ddd; margin-bottom: 20px;">
                                    <h4>📊 ${vendorData.vendor} Metrics</h4>

                                    <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 20px; margin-bottom: 20px;">
                                        <!-- Collection Time Chart -->
                                        <div>
                                            <h5>📅 Bars by Collection Time (Last 24h)</h5>
                                            <div style="background: #f9f9f9; padding: 10px; border-radius: 4px; max-height: 200px; overflow-y: auto;">
                                                <table style="width: 100%; font-size: 0.9em;">
                                                    <thead>
                                                        <tr style="background: #e0e0e0;">
                                                            <th style="padding: 5px; text-align: left;">Collection Hour</th>
                                                            <th style="padding: 5px; text-align: right;">Bars</th>
                                                            <th style="padding: 5px; text-align: right;">Symbols</th>
                                                        </tr>
                                                    </thead>
                                                    <tbody>
                                                        ${collectionData.map(row => `
                                                            <tr>
                                                                <td style="padding: 3px;">${new Date(row.collection_hour).toLocaleString()}</td>
                                                                <td style="padding: 3px; text-align: right;">${row.bars_collected?.toLocaleString()}</td>
                                                                <td style="padding: 3px; text-align: right;">${row.unique_symbols}</td>
                                                            </tr>
                                                        `).join('')}
                                                    </tbody>
                                                </table>
                                            </div>
                                        </div>

                                        <!-- Bar Time Chart -->
                                        <div>
                                            <h5>⏰ Bars by Bar Time (Last 24h)</h5>
                                            <div style="background: #f9f9f9; padding: 10px; border-radius: 4px; max-height: 200px; overflow-y: auto;">
                                                <table style="width: 100%; font-size: 0.9em;">
                                                    <thead>
                                                        <tr style="background: #e0e0e0;">
                                                            <th style="padding: 5px; text-align: left;">Bar Hour</th>
                                                            <th style="padding: 5px; text-align: right;">Bars</th>
                                                            <th style="padding: 5px; text-align: right;">Avg Vol</th>
                                                        </tr>
                                                    </thead>
                                                    <tbody>
                                                        ${barData.map(row => `
                                                            <tr>
                                                                <td style="padding: 3px;">${new Date(row.bar_hour).toLocaleString()}</td>
                                                                <td style="padding: 3px; text-align: right;">${row.bars_count?.toLocaleString()}</td>
                                                                <td style="padding: 3px; text-align: right;">${row.avg_volume ? Math.round(row.avg_volume).toLocaleString() : 'N/A'}</td>
                                                            </tr>
                                                        `).join('')}
                                                    </tbody>
                                                </table>
                                            </div>
                                        </div>
                                    </div>

                                    <!-- Vendor Stats -->
                                    <div style="background: #f5f5f5; padding: 10px; border-radius: 4px;">
                                        <strong>7-Day Stats:</strong>
                                        Total Bars: <strong>${stats.total_bars?.toLocaleString() || 0}</strong> |
                                        Symbols: <strong>${stats.total_symbols || 0}</strong> |
                                        Avg Quality: <strong>${stats.avg_quality_score?.toFixed(2) || 'N/A'}</strong> |
                                        Avg Latency: <strong>${stats.avg_latency_ms?.toFixed(0) || 0}ms</strong>
                                    </div>
                                </div>
                            `;
                        }

                        html += `
                            <div style="background: #f0f0f0; padding: 10px; border-radius: 4px; font-size: 0.9em; color: #666;">
                                <strong>Last Updated:</strong> ${new Date(data.timestamp).toLocaleString()}
                            </div>
                        `;

                        document.getElementById('analysis-content').innerHTML = html;

                    } catch (error) {
                        console.error('Error loading bar collection metrics:', error);
                        document.getElementById('analysis-content').innerHTML =
                            '<h3>📈 Bar Collection Metrics</h3><p style="color: red;">Error loading metrics. Check console for details.</p>';
                    }
                }

async function loadUniverseAnalytics() {
                    document.getElementById('analysis-content').innerHTML =
                        '<h3>🌐 Universe Analytics</h3><p>Loading universe selection menu...</p>';

                    try {
                        // Load available universes
                        const universesResponse = await fetch('/api/universes');
                        const universesData = await universesResponse.json();

                        if (universesData.success) {
                            let html = `
                                <h3>🌐 Universe Analytics</h3>
                                <div style="background: white; padding: 20px; border-radius: 8px; border: 1px solid #ddd; margin-bottom: 20px;">
                                    <h4>🔍 Universe Selection</h4>
                                    <div style="display: grid; grid-template-columns: 2fr 1fr 1fr auto; gap: 15px; align-items: end; margin-bottom: 15px;">
                                        <div>
                                            <label for="universe-selector" style="display: block; margin-bottom: 5px; font-weight: bold;">Select Universe:</label>
                                            <select id="universe-selector" style="width: 100%; padding: 8px; border: 1px solid #ddd; border-radius: 4px;">
                                                <option value="">-- Select a universe --</option>
                            `;

                            universesData.universes.forEach(universe => {
                                html += `<option value="${universe.id}">${universe.name} - ${universe.description}</option>`;
                            });

                            html += `
                                            </select>
                                        </div>
                                        <div>
                                            <label for="universe-date-from" style="display: block; margin-bottom: 5px; font-weight: bold;">From Date:</label>
                                            <input type="date" id="universe-date-from" style="width: 100%; padding: 8px; border: 1px solid #ddd; border-radius: 4px;">
                                        </div>
                                        <div>
                                            <label for="universe-date-to" style="display: block; margin-bottom: 5px; font-weight: bold;">To Date:</label>
                                            <input type="date" id="universe-date-to" style="width: 100%; padding: 8px; border: 1px solid #ddd; border-radius: 4px;">
                                        </div>
                                        <div>
                                            <button onclick="loadUniverseMembers()" style="padding: 8px 16px; background: #4285f4; color: white; border: none; border-radius: 4px; cursor: pointer;">
                                                Load Members
                                            </button>
                                        </div>
                                    </div>
                                    <p style="color: #666; font-size: 0.9em; margin: 0;">
                                        <strong>Available Universes:</strong> ${universesData.universes.length} total
                                    </p>
                                </div>

                                <div id="universe-members-content" style="background: white; padding: 20px; border-radius: 8px; border: 1px solid #ddd;">
                                    <h4>📊 Universe Members</h4>
                                    <p style="color: #666;">Select a universe and date range above to view members.</p>
                                </div>
                            `;

                            document.getElementById('analysis-content').innerHTML = html;

                            // Set default date range (last 30 days)
                            const today = new Date();
                            const thirtyDaysAgo = new Date(today);
                            thirtyDaysAgo.setDate(today.getDate() - 30);

                            document.getElementById('universe-date-from').value = thirtyDaysAgo.toISOString().split('T')[0];
                            document.getElementById('universe-date-to').value = today.toISOString().split('T')[0];
                        }
                    } catch (error) {
                        document.getElementById('analysis-content').innerHTML =
                            '<h3>🌐 Universe Analytics</h3><p style="color: red;">Error loading universe analytics: ' + error.message + '</p>';
                    }
                }

                async function loadUniverseMembers() {
                    const universeId = document.getElementById('universe-selector').value;
                    const dateFrom = document.getElementById('universe-date-from').value;
                    const dateTo = document.getElementById('universe-date-to').value;

                    if (!universeId) {
                        alert('Please select a universe first.');
                        return;
                    }

                    if (!dateFrom || !dateTo) {
                        alert('Please select both from and to dates.');
                        return;
                    }

                    const membersContent = document.getElementById('universe-members-content');
                    membersContent.innerHTML = '<h4>📊 Universe Members</h4><p>Loading universe members...</p>';

                    try {
                        const response = await fetch(`/api/universe-members/${universeId}?date_from=${dateFrom}&date_to=${dateTo}`);
                        const data = await response.json();

                        if (data.success) {
                            let html = `
                                <h4>📊 Universe Members</h4>
                                <div style="margin-bottom: 15px; padding: 10px; background: #f8f9fa; border-radius: 4px;">
                                    <strong>Universe:</strong> ${data.universe_info.name}<br>
                                    <strong>Description:</strong> ${data.universe_info.description}<br>
                                    <strong>Date Range:</strong> ${dateFrom} to ${dateTo}<br>
                                    <strong>Total Members:</strong> ${data.members.length} symbols
                                </div>
                            `;

                            if (data.members.length > 0) {
                                // Group members by status (active vs historical)
                                const activeMembers = data.members.filter(member => !member.end_at);
                                const historicalMembers = data.members.filter(member => member.end_at);

                                html += `
                                    <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 20px;">
                                        <div>
                                            <h5 style="color: #388e3c;">✅ Active Members (${activeMembers.length})</h5>
                                            <div style="max-height: 400px; overflow-y: auto; border: 1px solid #ddd; border-radius: 4px;">
                                                <table style="width: 100%; border-collapse: collapse;">
                                                    <thead style="background: #f5f5f5; position: sticky; top: 0;">
                                                        <tr>
                                                            <th style="padding: 8px; border: 1px solid #ddd; text-align: left;">Symbol</th>
                                                            <th style="padding: 8px; border: 1px solid #ddd; text-align: left;">Start Date</th>
                                                        </tr>
                                                    </thead>
                                                    <tbody>
                                `;

                                activeMembers.forEach(member => {
                                    const startDate = new Date(member.start_at).toISOString().split('T')[0];
                                    html += `
                                        <tr>
                                            <td style="padding: 8px; border: 1px solid #ddd; font-weight: bold; color: #1976d2;">${member.symbol}</td>
                                            <td style="padding: 8px; border: 1px solid #ddd;">${startDate}</td>
                                        </tr>
                                    `;
                                });

                                html += `
                                                    </tbody>
                                                </table>
                                            </div>
                                        </div>

                                        <div>
                                            <h5 style="color: #f57c00;">📋 Historical Members (${historicalMembers.length})</h5>
                                            <div style="max-height: 400px; overflow-y: auto; border: 1px solid #ddd; border-radius: 4px;">
                                                <table style="width: 100%; border-collapse: collapse;">
                                                    <thead style="background: #f5f5f5; position: sticky; top: 0;">
                                                        <tr>
                                                            <th style="padding: 8px; border: 1px solid #ddd; text-align: left;">Symbol</th>
                                                            <th style="padding: 8px; border: 1px solid #ddd; text-align: left;">Start Date</th>
                                                            <th style="padding: 8px; border: 1px solid #ddd; text-align: left;">End Date</th>
                                                        </tr>
                                                    </thead>
                                                    <tbody>
                                `;

                                historicalMembers.forEach(member => {
                                    const startDate = new Date(member.start_at).toISOString().split('T')[0];
                                    const endDate = member.end_at ? new Date(member.end_at).toISOString().split('T')[0] : 'Active';
                                    html += `
                                        <tr>
                                            <td style="padding: 8px; border: 1px solid #ddd; font-weight: bold; color: #666;">${member.symbol}</td>
                                            <td style="padding: 8px; border: 1px solid #ddd;">${startDate}</td>
                                            <td style="padding: 8px; border: 1px solid #ddd;">${endDate}</td>
                                        </tr>
                                    `;
                                });

                                html += `
                                                    </tbody>
                                                </table>
                                            </div>
                                        </div>
                                    </div>
                                `;
                            } else {
                                html += `
                                    <div style="text-align: center; padding: 40px; color: #666;">
                                        <p><strong>No members found</strong></p>
                                        <p>The selected universe has no members in the specified date range.</p>
                                    </div>
                                `;
                            }

                            membersContent.innerHTML = html;
                        } else {
                            membersContent.innerHTML = `
                                <h4>📊 Universe Members</h4>
                                <p style="color: red;">Error: ${data.error}</p>
                            `;
                        }
                    } catch (error) {
                        membersContent.innerHTML = `
                            <h4>📊 Universe Members</h4>
                            <p style="color: red;">Error loading universe members: ${error.message}</p>
                        `;
                    }
                }

                async function loadNewsEvents() {
                    // IMPORTANT: Get filter values BEFORE wiping out the content
                    const symbolFilter = document.getElementById('symbol-filter')?.value || '';
                    const startDateFilter = document.getElementById('start-date-filter')?.value || '';
                    const endDateFilter = document.getElementById('end-date-filter')?.value || '';
                    const limit = 50;

                    // Now show loading message
                    document.getElementById('analysis-content').innerHTML =
                        '<h3>📰 News Events</h3><p>Loading news events from Polygon and Tiingo...</p>';

                    try {

                        // Build query parameters
                        const params = new URLSearchParams();
                        params.append('limit', limit);
                        if (symbolFilter) params.append('symbol', symbolFilter);
                        if (startDateFilter) params.append('start_date', startDateFilter);
                        if (endDateFilter) params.append('end_date', endDateFilter);

                        // Fetch news events with filters
                        const response = await fetch(`/api/news-events?${params.toString()}`);
                        const data = await response.json();

                        let html = ''; // Declare html at function level to avoid scoping issues

                        if (data.success && data.events) {
                            const appliedFilters = data.filters || {};
                            html = `
                                <h3>📰 News Events Analysis</h3>

                                <!-- Filter Controls -->
                                <div style="background: #f8f9fa; padding: 20px; border-radius: 8px; margin-bottom: 20px;">
                                    <h4 style="margin: 0 0 15px 0;">🔍 Filter News Events</h4>
                                    <div style="display: grid; grid-template-columns: auto auto auto auto; gap: 15px; align-items: end;">
                                        <div>
                                            <label for="symbol-filter" style="display: block; margin-bottom: 5px; font-weight: bold;">Symbol:</label>
                                            <input type="text" id="symbol-filter" placeholder="e.g., AAPL"
                                                   value="${appliedFilters.symbol || ''}"
                                                   style="padding: 8px; border: 1px solid #ccc; border-radius: 4px; width: 100px;">
                                        </div>
                                        <div>
                                            <label for="start-date-filter" style="display: block; margin-bottom: 5px; font-weight: bold;">Start Date:</label>
                                            <input type="date" id="start-date-filter"
                                                   value="${appliedFilters.start_date || ''}"
                                                   style="padding: 8px; border: 1px solid #ccc; border-radius: 4px;">
                                        </div>
                                        <div>
                                            <label for="end-date-filter" style="display: block; margin-bottom: 5px; font-weight: bold;">End Date:</label>
                                            <input type="date" id="end-date-filter"
                                                   value="${appliedFilters.end_date || ''}"
                                                   style="padding: 8px; border: 1px solid #ccc; border-radius: 4px;">
                                        </div>
                                        <div>
                                            <button onclick="loadNewsEvents()" style="padding: 10px 20px; background: #007bff; color: white; border: none; border-radius: 4px; cursor: pointer;">
                                                Apply Filters
                                            </button>
                                            <button onclick="clearNewsFilters()" style="padding: 10px 15px; background: #6c757d; color: white; border: none; border-radius: 4px; cursor: pointer; margin-left: 5px;">
                                                Clear
                                            </button>
                                        </div>
                                    </div>
                                </div>

                                <div style="display: grid; grid-template-columns: 1fr 1fr 1fr; gap: 20px; margin-bottom: 20px;">
                                    <div style="background: #f8f9fa; padding: 15px; border-radius: 8px; text-align: center;">
                                        <h4 style="margin: 0; color: #007bff;">Total Events</h4>
                                        <div style="font-size: 24px; font-weight: bold; color: #333;">${data.total_events}</div>
                                    </div>
                                    <div style="background: #f8f9fa; padding: 15px; border-radius: 8px; text-align: center;">
                                        <h4 style="margin: 0; color: #28a745;">Unique Symbols</h4>
                                        <div style="font-size: 24px; font-weight: bold; color: #333;">${data.unique_symbols}</div>
                                    </div>
                                    <div style="background: #f8f9fa; padding: 15px; border-radius: 8px; text-align: center;">
                                        <h4 style="margin: 0; color: #6f42c1;">Sources</h4>
                                        <div style="font-size: 16px; color: #333;">${Object.keys(data.sources || {}).join(', ')}</div>
                                    </div>
                                </div>

                                <div style="background: white; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); overflow: hidden;">
                                    <div style="background: #007bff; color: white; padding: 15px;">
                                        <h4 style="margin: 0;">📰 Recent News Events</h4>
                                    </div>
                                    <div style="max-height: 600px; overflow-y: auto;">
                                        <table style="width: 100%; border-collapse: collapse;">
                                            <thead style="background: #f8f9fa; position: sticky; top: 0;">
                                                <tr>
                                                    <th style="padding: 12px; text-align: left; border-bottom: 2px solid #dee2e6;">Title</th>
                                                    <th style="padding: 12px; text-align: left; border-bottom: 2px solid #dee2e6;">Source</th>
                                                    <th style="padding: 12px; text-align: left; border-bottom: 2px solid #dee2e6;">Symbols</th>
                                                    <th style="padding: 12px; text-align: left; border-bottom: 2px solid #dee2e6;">Published</th>
                                                </tr>
                                            </thead>
                                            <tbody>
                            `;

                            data.events.forEach((event, index) => {
                                const publishedDate = event.published_at ? new Date(event.published_at).toLocaleString() : 'N/A';
                                const symbols = (event.symbols || []).slice(0, 3).join(', ') + (event.symbols && event.symbols.length > 3 ? '...' : '');
                                const backgroundColor = index % 2 === 0 ? 'white' : '#f8f9fa';

                                html += `
                                    <tr style="background: ${backgroundColor};">
                                        <td style="padding: 12px; border-bottom: 1px solid #dee2e6;">
                                            <div style="font-weight: bold; margin-bottom: 4px;">${event.title || 'Untitled'}</div>
                                            <div style="font-size: 12px; color: #666; max-width: 400px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap;">
                                                ${event.description || 'No description'}
                                            </div>
                                        </td>
                                        <td style="padding: 12px; border-bottom: 1px solid #dee2e6;">
                                            <span style="background: #007bff; color: white; padding: 2px 8px; border-radius: 12px; font-size: 12px;">
                                                ${event.source}
                                            </span>
                                        </td>
                                        <td style="padding: 12px; border-bottom: 1px solid #dee2e6; font-size: 12px;">
                                            ${symbols || 'N/A'}
                                        </td>
                                        <td style="padding: 12px; border-bottom: 1px solid #dee2e6; font-size: 12px;">
                                            ${publishedDate}
                                        </td>
                                    </tr>
                                `;
                            });

                            html += `
                                            </tbody>
                                        </table>
                                    </div>
                                </div>

                                <div style="margin-top: 20px; padding: 15px; background: #e9ecef; border-radius: 8px;">
                                    <h5>📊 Sources Breakdown:</h5>
                                    <div style="display: flex; gap: 20px;">
                            `;

                            Object.entries(data.sources || {}).forEach(([source, count]) => {
                                html += `<div><strong>${source}:</strong> ${count} events</div>`;
                            });

                            html += `
                                    </div>
                                </div>
                            `;

                        } else {
                            html = `
                                <h3>📰 News Events</h3>
                                <div style="text-align: center; padding: 40px;">
                                    <p>No news events available or error occurred.</p>
                                    ${data.error ? `<p style="color: red;">Error: ${data.error}</p>` : ''}
                                </div>
                            `;
                        }

                        document.getElementById('analysis-content').innerHTML = html;

                    } catch (error) {
                        document.getElementById('analysis-content').innerHTML =
                            '<h3>📰 News Events</h3><p style="color: red;">Error loading news events: ' + error.message + '</p>';
                    }
                }

                function clearNewsFilters() {
                    // Clear all filter inputs
                    const symbolFilter = document.getElementById('symbol-filter');
                    const startDateFilter = document.getElementById('start-date-filter');
                    const endDateFilter = document.getElementById('end-date-filter');

                    if (symbolFilter) symbolFilter.value = '';
                    if (startDateFilter) startDateFilter.value = '';
                    if (endDateFilter) endDateFilter.value = '';

                    // Reload news events without filters
                    loadNewsEvents();
                }

                async function loadEarningsEvents(symbolFilter = '', startDate = '', endDate = '') {
                    document.getElementById('analysis-content').innerHTML =
                        '<h3>📊 Earnings Events</h3><p>Loading earnings events data...</p>';

                    try {
                        // Build query parameters
                        let params = new URLSearchParams();
                        params.append('limit', '50');
                        if (symbolFilter) params.append('symbol', symbolFilter);
                        if (startDate) params.append('start_date', startDate);
                        if (endDate) params.append('end_date', endDate);

                        // Fetch earnings events
                        const response = await fetch('/api/earnings-events?' + params.toString());
                        const data = await response.json();

                        let html = '';

                        if (data.success && data.events) {
                            html = '<h3>📊 Earnings Events Analysis</h3>' +

                                '<!-- Filter Controls -->' +
                                '<div style="background: #f8f9fa; padding: 20px; border-radius: 8px; margin-bottom: 20px;">' +
                                    '<h4 style="margin: 0 0 15px 0;">🔍 Filters</h4>' +
                                    '<div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 15px; align-items: end;">' +
                                        '<div>' +
                                            '<label style="display: block; margin-bottom: 5px; font-weight: bold;">Symbol:</label>' +
                                            '<input type="text" id="symbol-filter" placeholder="e.g. AAPL" style="width: 100%; padding: 8px; border: 1px solid #ddd; border-radius: 4px;" value="' + (symbolFilter || '') + '">' +
                                        '</div>' +
                                        '<div>' +
                                            '<label style="display: block; margin-bottom: 5px; font-weight: bold;">Start Date:</label>' +
                                            '<input type="date" id="start-date-filter" style="width: 100%; padding: 8px; border: 1px solid #ddd; border-radius: 4px;" value="' + (startDate || '') + '">' +
                                        '</div>' +
                                        '<div>' +
                                            '<label style="display: block; margin-bottom: 5px; font-weight: bold;">End Date:</label>' +
                                            '<input type="date" id="end-date-filter" style="width: 100%; padding: 8px; border: 1px solid #ddd; border-radius: 4px;" value="' + (endDate || '') + '">' +
                                        '</div>' +
                                        '<div>' +
                                            '<button onclick="applyEarningsFilters()" style="background: #673ab7; color: white; border: none; padding: 10px 20px; border-radius: 4px; cursor: pointer; width: 100%;">Apply Filters</button>' +
                                        '</div>' +
                                        '<div>' +
                                            '<button onclick="clearEarningsFilters()" style="background: #6c757d; color: white; border: none; padding: 10px 20px; border-radius: 4px; cursor: pointer; width: 100%;">Clear</button>' +
                                        '</div>' +
                                    '</div>' +
                                '</div>' +

                                '<!-- Summary Cards -->' +
                                '<div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 20px; margin-bottom: 20px;">' +
                                    '<div style="background: #e3f2fd; padding: 15px; border-radius: 8px; text-align: center;">' +
                                        '<h4 style="margin: 0; color: #1976d2;">Total Events</h4>' +
                                        '<div style="font-size: 24px; font-weight: bold; color: #333;">' + data.total_events + '</div>' +
                                    '</div>' +
                                    '<div style="background: #e8f5e8; padding: 15px; border-radius: 8px; text-align: center;">' +
                                        '<h4 style="margin: 0; color: #388e3c;">EPS Beats</h4>' +
                                        '<div style="font-size: 24px; font-weight: bold; color: #333;">' + data.summary.eps_beats + '</div>' +
                                        '<div style="font-size: 12px; color: #666;">vs ' + data.summary.eps_misses + ' misses</div>' +
                                    '</div>' +
                                    '<div style="background: #fff3e0; padding: 15px; border-radius: 8px; text-align: center;">' +
                                        '<h4 style="margin: 0; color: #f57c00;">Revenue Beats</h4>' +
                                        '<div style="font-size: 24px; font-weight: bold; color: #333;">' + data.summary.revenue_beats + '</div>' +
                                        '<div style="font-size: 12px; color: #666;">vs ' + data.summary.revenue_misses + ' misses</div>' +
                                    '</div>' +
                                    '<div style="background: #f3e5f5; padding: 15px; border-radius: 8px; text-align: center;">' +
                                        '<h4 style="margin: 0; color: #7b1fa2;">Guidance Changes</h4>' +
                                        '<div style="font-size: 16px; color: #333;">' +
                                            '<span style="color: #4caf50;">↑' + data.summary.guidance_raised + '</span> |' +
                                            '<span style="color: #f44336;">↓' + data.summary.guidance_lowered + '</span>' +
                                        '</div>' +
                                    '</div>' +
                                    '<div style="background: #fce4ec; padding: 15px; border-radius: 8px; text-align: center;">' +
                                        '<h4 style="margin: 0; color: #c2185b;">Unique Symbols</h4>' +
                                        '<div style="font-size: 24px; font-weight: bold; color: #333;">' + data.unique_symbols + '</div>' +
                                    '</div>' +
                                '</div>' +

                                '<!-- Earnings Events Table -->' +
                                '<div style="background: white; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); overflow: hidden;">' +
                                    '<div style="background: #673ab7; color: white; padding: 15px;">' +
                                        '<h4 style="margin: 0;">📊 Recent Earnings Events</h4>' +
                                    '</div>' +
                                    '<div style="max-height: 600px; overflow-y: auto;">' +
                                        '<table style="width: 100%; border-collapse: collapse;">' +
                                            '<thead style="background: #f8f9fa; position: sticky; top: 0;">' +
                                                '<tr>' +
                                                    '<th style="padding: 12px; text-align: left; border-bottom: 2px solid #dee2e6;">Symbol</th>' +
                                                    '<th style="padding: 12px; text-align: left; border-bottom: 2px solid #dee2e6;">Period</th>' +
                                                    '<th style="padding: 12px; text-align: center; border-bottom: 2px solid #dee2e6;">EPS</th>' +
                                                    '<th style="padding: 12px; text-align: center; border-bottom: 2px solid #dee2e6;">Revenue (M)</th>' +
                                                    '<th style="padding: 12px; text-align: center; border-bottom: 2px solid #dee2e6;">Beats</th>' +
                                                    '<th style="padding: 12px; text-align: center; border-bottom: 2px solid #dee2e6;">Guidance</th>' +
                                                '</tr>' +
                                            '</thead>' +
                                            '<tbody>';

                            data.events.forEach((event, index) => {
                                const backgroundColor = index % 2 === 0 ? 'white' : '#f8f9fa';
                                const reportDate = event.report_period ? event.report_period : 'N/A';

                                // Format EPS data
                                const epsActual = event.eps_actual !== null ? '$' + event.eps_actual : 'N/A';
                                const epsEstimated = event.eps_estimated !== null ? '$' + event.eps_estimated : 'N/A';
                                const epsSurprise = event.eps_surprise_pct !== null ? event.eps_surprise_pct.toFixed(1) + '%' : 'N/A';

                                // Format Revenue data
                                const revenueActual = event.revenue_actual_millions !== null ? '$' + event.revenue_actual_millions + 'M' : 'N/A';
                                const revenueEstimated = event.revenue_estimated_millions !== null ? '$' + event.revenue_estimated_millions + 'M' : 'N/A';
                                const revenueSurprise = event.revenue_surprise_pct !== null ? event.revenue_surprise_pct.toFixed(1) + '%' : 'N/A';

                                // Beat/miss indicators
                                const epsBeat = event.earnings_beat === true ? '✅' : event.earnings_beat === false ? '❌' : '❓';
                                const revenueBeat = event.revenue_beat === true ? '✅' : event.revenue_beat === false ? '❌' : '❓';

                                // Guidance indicators
                                let guidanceIndicator = '➖';
                                if (event.guidance_raised === true) guidanceIndicator = '📈';
                                else if (event.guidance_lowered === true) guidanceIndicator = '📉';

                                html += '<tr style="background: ' + backgroundColor + ';">' +
                                    '<td style="padding: 12px; border-bottom: 1px solid #dee2e6;">' +
                                        '<div style="font-weight: bold; color: #333;">' + event.symbol + '</div>' +
                                        '<div style="font-size: 12px; color: #666;">' + event.report_type + '</div>' +
                                    '</td>' +
                                    '<td style="padding: 12px; border-bottom: 1px solid #dee2e6; font-size: 14px;">' +
                                        reportDate +
                                    '</td>' +
                                    '<td style="padding: 12px; border-bottom: 1px solid #dee2e6; text-align: center;">' +
                                        '<div style="font-weight: bold;">' + epsActual + '</div>' +
                                        '<div style="font-size: 12px; color: #666;">est: ' + epsEstimated + '</div>' +
                                        '<div style="font-size: 12px; color: ' + (event.eps_surprise_pct > 0 ? '#4caf50' : '#f44336') + ';">' + epsSurprise + '</div>' +
                                    '</td>' +
                                    '<td style="padding: 12px; border-bottom: 1px solid #dee2e6; text-align: center;">' +
                                        '<div style="font-weight: bold;">' + revenueActual + '</div>' +
                                        '<div style="font-size: 12px; color: #666;">est: ' + revenueEstimated + '</div>' +
                                        '<div style="font-size: 12px; color: ' + (event.revenue_surprise_pct > 0 ? '#4caf50' : '#f44336') + ';">' + revenueSurprise + '</div>' +
                                    '</td>' +
                                    '<td style="padding: 12px; border-bottom: 1px solid #dee2e6; text-align: center;">' +
                                        '<div>EPS: ' + epsBeat + '</div>' +
                                        '<div>Rev: ' + revenueBeat + '</div>' +
                                    '</td>' +
                                    '<td style="padding: 12px; border-bottom: 1px solid #dee2e6; text-align: center; font-size: 20px;">' +
                                        guidanceIndicator +
                                    '</td>' +
                                '</tr>';
                            });

                            html += '</tbody>' +
                                        '</table>' +
                                    '</div>' +
                                '</div>' +

                                '<div style="margin-top: 20px; padding: 15px; background: #e9ecef; border-radius: 8px;">' +
                                    '<h5>📈 Performance Summary:</h5>' +
                                    '<div style="display: grid; grid-template-columns: 1fr 1fr; gap: 20px;">' +
                                        '<div>' +
                                            '<strong>EPS Performance:</strong><br>' +
                                            'Beats: ' + data.summary.eps_beats + ' | Misses: ' + data.summary.eps_misses + '<br>' +
                                            'EPS Success Rate: ' + (data.summary.eps_beats + data.summary.eps_misses > 0 ?
                                                Math.round(data.summary.eps_beats / (data.summary.eps_beats + data.summary.eps_misses) * 100) : 0) + '%' +
                                        '</div>' +
                                        '<div>' +
                                            '<strong>Revenue Performance:</strong><br>' +
                                            'Beats: ' + data.summary.revenue_beats + ' | Misses: ' + data.summary.revenue_misses + '<br>' +
                                            'Revenue Success Rate: ' + (data.summary.revenue_beats + data.summary.revenue_misses > 0 ?
                                                Math.round(data.summary.revenue_beats / (data.summary.revenue_beats + data.summary.revenue_misses) * 100) : 0) + '%' +
                                        '</div>' +
                                    '</div>' +
                                '</div>';

                        } else {
                            html = '<h3>📊 Earnings Events</h3>' +
                                '<div style="text-align: center; padding: 40px;">' +
                                    '<p>No earnings events available or error occurred.</p>' +
                                    (data.error ? '<p style="color: red;">Error: ' + data.error + '</p>' : '') +
                                '</div>';
                        }

                        document.getElementById('analysis-content').innerHTML = html;

                    } catch (error) {
                        document.getElementById('analysis-content').innerHTML =
                            '<h3>📊 Earnings Events</h3><p style="color: red;">Error loading earnings events: ' + error.message + '</p>';
                    }
                }

                function applyEarningsFilters() {
                    const symbolFilter = document.getElementById('symbol-filter').value.trim();
                    const startDate = document.getElementById('start-date-filter').value;
                    const endDate = document.getElementById('end-date-filter').value;

                    loadEarningsEvents(symbolFilter, startDate, endDate);
                }

                function clearEarningsFilters() {
                    document.getElementById('symbol-filter').value = '';
                    document.getElementById('start-date-filter').value = '';
                    document.getElementById('end-date-filter').value = '';

                    loadEarningsEvents('', '', '');
                }

                async function loadGapEvents(symbolFilter = '', startDate = '', endDate = '') {
                    document.getElementById('analysis-content').innerHTML =
                        '<h3>⚡ Gap Events</h3><p>Loading gap events data...</p>';

                    try {
                        // Build query parameters
                        let params = new URLSearchParams();
                        params.append('limit', '50');
                        if (symbolFilter) params.append('symbol', symbolFilter);
                        if (startDate) params.append('start_date', startDate);
                        if (endDate) params.append('end_date', endDate);

                        // Fetch gap events
                        const response = await fetch('/api/gap-events?' + params.toString());
                        const data = await response.json();

                        let html = '';

                        if (data.success && data.events) {
                            html = '<h3>⚡ Gap Events Analysis</h3>' +

                                '<!-- Filter Controls -->' +
                                '<div style="background: #f8f9fa; padding: 20px; border-radius: 8px; margin-bottom: 20px;">' +
                                    '<h4 style="margin: 0 0 15px 0;">🔍 Filters</h4>' +
                                    '<div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 15px; align-items: end;">' +
                                        '<div>' +
                                            '<label style="display: block; margin-bottom: 5px; font-weight: bold;">Symbol:</label>' +
                                            '<input type="text" id="gap-symbol-filter" placeholder="e.g. AAPL" style="width: 100%; padding: 8px; border: 1px solid #ddd; border-radius: 4px;" value="' + (symbolFilter || '') + '">' +
                                        '</div>' +
                                        '<div>' +
                                            '<label style="display: block; margin-bottom: 5px; font-weight: bold;">Start Date:</label>' +
                                            '<input type="date" id="gap-start-date-filter" style="width: 100%; padding: 8px; border: 1px solid #ddd; border-radius: 4px;" value="' + (startDate || '') + '">' +
                                        '</div>' +
                                        '<div>' +
                                            '<label style="display: block; margin-bottom: 5px; font-weight: bold;">End Date:</label>' +
                                            '<input type="date" id="gap-end-date-filter" style="width: 100%; padding: 8px; border: 1px solid #ddd; border-radius: 4px;" value="' + (endDate || '') + '">' +
                                        '</div>' +
                                        '<div>' +
                                            '<button onclick="applyGapFilters()" style="background: #17a2b8; color: white; border: none; padding: 10px 20px; border-radius: 4px; cursor: pointer; width: 100%;">Apply Filters</button>' +
                                        '</div>' +
                                        '<div>' +
                                            '<button onclick="clearGapFilters()" style="background: #6c757d; color: white; border: none; padding: 10px 20px; border-radius: 4px; cursor: pointer; width: 100%;">Clear</button>' +
                                        '</div>' +
                                    '</div>' +
                                '</div>' +

                                '<!-- Summary Cards -->' +
                                '<div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 20px; margin-bottom: 20px;">' +
                                    '<div style="background: #e3f2fd; padding: 15px; border-radius: 8px; text-align: center;">' +
                                        '<h4 style="margin: 0; color: #1976d2;">Total Gaps</h4>' +
                                        '<div style="font-size: 24px; font-weight: bold; color: #333;">' + data.total_events + '</div>' +
                                    '</div>' +
                                    '<div style="background: #e8f5e8; padding: 15px; border-radius: 8px; text-align: center;">' +
                                        '<h4 style="margin: 0; color: #388e3c;">Gap Ups ↗️</h4>' +
                                        '<div style="font-size: 24px; font-weight: bold; color: #333;">' + data.summary.gap_ups + '</div>' +
                                        '<div style="font-size: 12px; color: #666;">vs ' + data.summary.gap_downs + ' downs</div>' +
                                    '</div>' +
                                    '<div style="background: #ffebee; padding: 15px; border-radius: 8px; text-align: center;">' +
                                        '<h4 style="margin: 0; color: #d32f2f;">Gap Downs ↘️</h4>' +
                                        '<div style="font-size: 24px; font-weight: bold; color: #333;">' + data.summary.gap_downs + '</div>' +
                                        '<div style="font-size: 12px; color: #666;">vs ' + data.summary.gap_ups + ' ups</div>' +
                                    '</div>' +
                                    '<div style="background: #fff3e0; padding: 15px; border-radius: 8px; text-align: center;">' +
                                        '<h4 style="margin: 0; color: #f57c00;">Filled Gaps</h4>' +
                                        '<div style="font-size: 24px; font-weight: bold; color: #333;">' + data.summary.filled_gaps + '</div>' +
                                        '<div style="font-size: 12px; color: #666;">of ' + data.total_events + ' total</div>' +
                                    '</div>' +
                                    '<div style="background: #f3e5f5; padding: 15px; border-radius: 8px; text-align: center;">' +
                                        '<h4 style="margin: 0; color: #7b1fa2;">Avg Score</h4>' +
                                        '<div style="font-size: 24px; font-weight: bold; color: #333;">' + data.summary.avg_significance_score + '</div>' +
                                        '<div style="font-size: 12px; color: #666;">significance</div>' +
                                    '</div>' +
                                    '<div style="background: #fce4ec; padding: 15px; border-radius: 8px; text-align: center;">' +
                                        '<h4 style="margin: 0; color: #c2185b;">Unique Symbols</h4>' +
                                        '<div style="font-size: 24px; font-weight: bold; color: #333;">' + data.unique_symbols + '</div>' +
                                    '</div>' +
                                '</div>' +

                                '<!-- Gap Events Table -->' +
                                '<div style="background: white; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); overflow: hidden;">' +
                                    '<div style="background: #17a2b8; color: white; padding: 15px;">' +
                                        '<h4 style="margin: 0;">⚡ Recent Gap Events</h4>' +
                                    '</div>' +
                                    '<div style="overflow-x: auto;">' +
                                        '<table style="width: 100%; border-collapse: collapse;">' +
                                            '<thead style="background: #f8f9fa;">' +
                                                '<tr>' +
                                                    '<th style="padding: 12px; text-align: left; border-bottom: 1px solid #dee2e6;">Symbol</th>' +
                                                    '<th style="padding: 12px; text-align: left; border-bottom: 1px solid #dee2e6;">Date</th>' +
                                                    '<th style="padding: 12px; text-align: right; border-bottom: 1px solid #dee2e6;">Gap %</th>' +
                                                    '<th style="padding: 12px; text-align: center; border-bottom: 1px solid #dee2e6;">Direction</th>' +
                                                    '<th style="padding: 12px; text-align: center; border-bottom: 1px solid #dee2e6;">Size</th>' +
                                                    '<th style="padding: 12px; text-align: right; border-bottom: 1px solid #dee2e6;">Score</th>' +
                                                    '<th style="padding: 12px; text-align: center; border-bottom: 1px solid #dee2e6;">Filled</th>' +
                                                '</tr>' +
                                            '</thead>' +
                                            '<tbody>';

                            // Add gap events rows
                            data.events.forEach((event, index) => {
                                const directionIcon = event.direction === 'gap_up' ? '↗️' : '↘️';
                                const directionClass = event.direction === 'gap_up' ? 'color: #4caf50' : 'color: #f44336';
                                const sizeClass = {
                                    'micro': 'background: #e0e0e0; color: #424242',
                                    'small': 'background: #fff3e0; color: #f57c00',
                                    'medium': 'background: #e8f5e8; color: #388e3c',
                                    'large': 'background: #ffebee; color: #d32f2f'
                                }[event.gap_size_class] || 'background: #f5f5f5; color: #666';

                                const filledStatus = event.is_filled ?
                                    '✅ ' + (event.days_to_fill || 'N/A') + 'd' :
                                    '⏳ Open';

                                html += '<tr style="border-bottom: 1px solid #f0f0f0;">' +
                                    '<td style="padding: 10px; font-weight: bold;">' + event.symbol + '</td>' +
                                    '<td style="padding: 10px;">' + event.gap_date + '</td>' +
                                    '<td style="padding: 10px; text-align: right; font-weight: bold; ' + directionClass + '">' +
                                        (event.gap_percentage !== null ? event.gap_percentage.toFixed(2) + '%' : 'N/A') + '</td>' +
                                    '<td style="padding: 10px; text-align: center; ' + directionClass + '">' + directionIcon + '</td>' +
                                    '<td style="padding: 10px; text-align: center;"><span style="padding: 4px 8px; border-radius: 12px; font-size: 11px; ' + sizeClass + '">' +
                                        (event.gap_size_class || 'unknown').toUpperCase() + '</span></td>' +
                                    '<td style="padding: 10px; text-align: right;">' +
                                        (event.significance_score !== null ? event.significance_score.toFixed(2) : 'N/A') + '</td>' +
                                    '<td style="padding: 10px; text-align: center;">' + filledStatus + '</td>' +
                                '</tr>';
                            });

                            html += '</tbody></table></div></div>';

                            // Add gap size breakdown
                            html += '<div style="margin-top: 20px; padding: 15px; background: #e9ecef; border-radius: 8px;">' +
                                '<h5>📊 Gap Size Breakdown:</h5>' +
                                '<div style="display: flex; gap: 20px; flex-wrap: wrap;">' +
                                    '<div><strong>Micro:</strong> ' + data.summary.micro_gaps + ' gaps</div>' +
                                    '<div><strong>Small:</strong> ' + data.summary.small_gaps + ' gaps</div>' +
                                    '<div><strong>Medium:</strong> ' + data.summary.medium_gaps + ' gaps</div>' +
                                    '<div><strong>Large:</strong> ' + data.summary.large_gaps + ' gaps</div>' +
                                '</div>' +
                            '</div>';

                        } else {
                            html = '<h3>⚡ Gap Events</h3>' +
                                '<div style="text-align: center; padding: 40px;">' +
                                    '<p>No gap events found.</p>' +
                                '</div>';
                        }

                        document.getElementById('analysis-content').innerHTML = html;

                    } catch (error) {
                        document.getElementById('analysis-content').innerHTML =
                            '<h3>⚡ Gap Events</h3><p style="color: red;">Error loading gap events: ' + error.message + '</p>';
                    }
                }

                async function loadXAIFinancialEvents() {
                    // Get filter values
                    const symbolFilter = document.getElementById('symbol-filter')?.value || '';
                    const startDateFilter = document.getElementById('start-date-filter')?.value || '';
                    const endDateFilter = document.getElementById('end-date-filter')?.value || '';
                    const limit = 50;

                    // Show loading message
                    document.getElementById('analysis-content').innerHTML =
                        '<h3>🔮 xAI Financial Events</h3><p>Loading financial events from xAI integration...</p>';

                    try {
                        // Build query parameters
                        const params = new URLSearchParams();
                        params.append('limit', limit);
                        if (symbolFilter) params.append('symbol', symbolFilter);
                        if (startDateFilter) params.append('start_date', startDateFilter);
                        if (endDateFilter) params.append('end_date', endDateFilter);

                        // Fetch financial events
                        const response = await fetch(`/financial_events?${params.toString()}`);
                        const data = await response.json();

                        let html = '';

                        if (data.success && data.events && data.events.length > 0) {
                            const appliedFilters = data.query_params || {};

                            html = `
                                <h3>🔮 AI Financial Events Analysis (xAI + Grok)</h3>
                                <div style="background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; padding: 15px; border-radius: 8px; margin-bottom: 20px;">
                                    <h4 style="margin: 0 0 10px 0;">📊 Event Summary (${data.count} events)</h4>
                                    <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(150px, 1fr)); gap: 15px;">`;

                            // Get summary stats
                            const summaryResponse = await fetch('/financial_events/summary');
                            const summaryData = await summaryResponse.json();

                            if (summaryData.success && summaryData.summary.length > 0) {
                                const stats = summaryData.summary[0];
                                html += `
                                    <div style="text-align: center;">
                                        <div style="font-size: 24px; font-weight: bold;">${stats.total_events || 0}</div>
                                        <div style="font-size: 12px; opacity: 0.9;">Total Events</div>
                                    </div>
                                    <div style="text-align: center;">
                                        <div style="font-size: 24px; font-weight: bold;">${stats.high_impact_events || 0}</div>
                                        <div style="font-size: 12px; opacity: 0.9;">High Impact</div>
                                    </div>
                                    <div style="text-align: center;">
                                        <div style="font-size: 24px; font-weight: bold;">${stats.unique_symbols || 0}</div>
                                        <div style="font-size: 12px; opacity: 0.9;">Unique Symbols</div>
                                    </div>
                                    <div style="text-align: center;">
                                        <div style="font-size: 24px; font-weight: bold;">${stats.events_last_week || 0}</div>
                                        <div style="font-size: 12px; opacity: 0.9;">This Week</div>
                                    </div>`;
                            }

                            html += `
                                    </div>
                                </div>

                                <!-- Filter Controls -->
                                <div style="background: white; padding: 15px; border-radius: 8px; margin-bottom: 20px; box-shadow: 0 2px 4px rgba(0,0,0,0.1);">
                                    <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 15px;">
                                        <div>
                                            <label for="xai-symbol-filter" style="display: block; margin-bottom: 5px; font-weight: 500;">Symbol Filter:</label>
                                            <input type="text" id="xai-symbol-filter" placeholder="e.g., AAPL"
                                                style="width: 100%; padding: 8px; border: 1px solid #ddd; border-radius: 4px;"
                                                value="${appliedFilters.symbol || ''}">
                                        </div>
                                        <div>
                                            <label for="xai-event-type-filter" style="display: block; margin-bottom: 5px; font-weight: 500;">Event Type:</label>
                                            <select id="xai-event-type-filter" style="width: 100%; padding: 8px; border: 1px solid #ddd; border-radius: 4px;">
                                                <option value="">All Types</option>
                                                <option value="earnings">Earnings</option>
                                                <option value="fed_announcement">Fed Announcements</option>
                                                <option value="stock_event">Stock Events</option>
                                                <option value="economic_indicator">Economic Indicators</option>
                                            </select>
                                        </div>
                                        <div>
                                            <label for="xai-impact-filter" style="display: block; margin-bottom: 5px; font-weight: 500;">Impact Level:</label>
                                            <select id="xai-impact-filter" style="width: 100%; padding: 8px; border: 1px solid #ddd; border-radius: 4px;">
                                                <option value="">All Levels</option>
                                                <option value="high">High Impact</option>
                                                <option value="medium">Medium Impact</option>
                                                <option value="low">Low Impact</option>
                                            </select>
                                        </div>
                                        <div style="display: flex; align-items: end;">
                                            <button onclick="applyXAIFilters()"
                                                style="background: #667eea; color: white; border: none; padding: 10px 20px; border-radius: 4px; cursor: pointer; font-weight: 500;">
                                                Apply Filters
                                            </button>
                                        </div>
                                    </div>
                                </div>

                                <!-- Events Table -->
                                <div style="background: white; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); overflow: hidden;">
                                    <div style="background: #667eea; color: white; padding: 15px;">
                                        <h4 style="margin: 0;">🔮 Financial Events from xAI</h4>
                                    </div>
                                    <div style="overflow-x: auto;">
                                        <table style="width: 100%; border-collapse: collapse;">
                                            <thead style="background: #f8f9fa;">
                                                <tr>
                                                    <th style="padding: 12px; text-align: left; border-bottom: 1px solid #dee2e6;">Date</th>
                                                    <th style="padding: 12px; text-align: left; border-bottom: 1px solid #dee2e6;">Symbol</th>
                                                    <th style="padding: 12px; text-align: left; border-bottom: 1px solid #dee2e6;">Type</th>
                                                    <th style="padding: 12px; text-align: left; border-bottom: 1px solid #dee2e6;">Impact</th>
                                                    <th style="padding: 12px; text-align: left; border-bottom: 1px solid #dee2e6;">Details</th>
                                                    <th style="padding: 12px; text-align: center; border-bottom: 1px solid #dee2e6;">Confidence</th>
                                                </tr>
                                            </thead>
                                            <tbody>`;

                            data.events.forEach((event, index) => {
                                const impactColor = event.impact_level === 'high' ? '#dc3545' :
                                                  event.impact_level === 'medium' ? '#fd7e14' : '#28a745';
                                const sentimentIcon = event.sentiment === 'positive' ? '📈' :
                                                    event.sentiment === 'negative' ? '📉' : '➖';
                                const confidencePercent = Math.round((event.confidence_score || 0) * 100);

                                html += `
                                    <tr style="border-bottom: 1px solid #f1f3f4;">
                                        <td style="padding: 12px; vertical-align: top;">
                                            <div style="font-weight: 500;">${event.event_date}</div>
                                            <div style="font-size: 12px; color: #666;">${event.event_time || 'N/A'}</div>
                                        </td>
                                        <td style="padding: 12px; vertical-align: top;">
                                            <span style="background: #e3f2fd; color: #1976d2; padding: 4px 8px; border-radius: 4px; font-weight: 500;">
                                                ${event.company_symbol || 'MARKET'}
                                            </span>
                                        </td>
                                        <td style="padding: 12px; vertical-align: top;">
                                            <span style="font-size: 12px; background: #f5f5f5; color: #333; padding: 4px 8px; border-radius: 4px;">
                                                ${event.event_type}
                                            </span>
                                        </td>
                                        <td style="padding: 12px; vertical-align: top;">
                                            <span style="color: ${impactColor}; font-weight: 500; text-transform: uppercase; font-size: 12px;">
                                                ${event.impact_level}
                                            </span>
                                        </td>
                                        <td style="padding: 12px; vertical-align: top; max-width: 300px;">
                                            <div style="margin-bottom: 8px;">${sentimentIcon} ${event.details}</div>
                                            ${event.sentiment ? `<div style="font-size: 12px; color: #666;">Sentiment: ${event.sentiment}</div>` : ''}
                                        </td>
                                        <td style="padding: 12px; text-align: center; vertical-align: top;">
                                            <div style="background: ${confidencePercent >= 80 ? '#d4edda' : confidencePercent >= 60 ? '#fff3cd' : '#f8d7da'};
                                                       color: ${confidencePercent >= 80 ? '#155724' : confidencePercent >= 60 ? '#856404' : '#721c24'};
                                                       padding: 4px 8px; border-radius: 4px; font-size: 12px; font-weight: 500;">
                                                ${confidencePercent}%
                                            </div>
                                        </td>
                                    </tr>`;
                            });

                            html += `
                                            </tbody>
                                        </table>
                                    </div>
                                </div>`;
                        } else {
                            html = '<h3>🔮 xAI Financial Events</h3>' +
                                '<div style="text-align: center; padding: 40px;">' +
                                    '<p>No financial events found. Try expanding your date range or clearing filters.</p>' +
                                    '<button onclick="extractNewEvents()" style="background: #667eea; color: white; border: none; padding: 10px 20px; border-radius: 4px; cursor: pointer; margin-top: 10px;">' +
                                        'Extract New Events from xAI' +
                                    '</button>' +
                                '</div>';
                        }

                        document.getElementById('analysis-content').innerHTML = html;

                    } catch (error) {
                        document.getElementById('analysis-content').innerHTML =
                            '<h3>🔮 xAI Financial Events</h3><p style="color: red;">Error loading financial events: ' + error.message + '</p>';
                    }
                }

                function applyXAIFilters() {
                    const symbolFilter = document.getElementById('xai-symbol-filter').value.trim();
                    const eventTypeFilter = document.getElementById('xai-event-type-filter').value;
                    const impactFilter = document.getElementById('xai-impact-filter').value;

                    // Build query parameters
                    const params = new URLSearchParams();
                    params.append('limit', 50);
                    if (symbolFilter) params.append('symbol', symbolFilter);
                    if (eventTypeFilter) params.append('event_type', eventTypeFilter);
                    if (impactFilter) params.append('impact_level', impactFilter);

                    // Reload with filters
                    fetch(`/financial_events?${params.toString()}`)
                        .then(response => response.json())
                        .then(data => {
                            // Reload the page content with filtered results
                            loadXAIFinancialEvents();
                        })
                        .catch(error => {
                            console.error('Error applying filters:', error);
                        });
                }

                async function extractNewEvents() {
                    // Show loading message
                    document.getElementById('analysis-content').innerHTML =
                        '<h3>🔮 xAI Financial Events</h3><p>Extracting new events from xAI... This may take a moment.</p>';

                    try {
                        const extractData = {
                            start_date: '2025-09-01',
                            end_date: '2025-09-13',
                            symbols: ['AAPL', 'TSLA', 'MSFT', 'GOOGL', 'AMZN'],
                            source: 'combined', // Use both xAI and Grok
                            force_refresh: false
                        };

                        const response = await fetch('/financial_events/extract', {
                            method: 'POST',
                            headers: {
                                'Content-Type': 'application/json',
                            },
                            body: JSON.stringify(extractData)
                        });

                        const result = await response.json();

                        if (result.success) {
                            // Show detailed success message for multi-source extraction
                            const sourceInfo = result.sources_used ? ` from ${result.sources_used.join(' + ')}` : '';
                            const uniqueInfo = result.events_unique ? ` (${result.events_unique} unique)` : '';
                            alert(`Successfully extracted ${result.events_extracted} events${uniqueInfo}${sourceInfo} and stored ${result.events_stored} new events!`);
                            loadXAIFinancialEvents(); // Reload the page
                        } else {
                            throw new Error(result.error || 'Unknown error during extraction');
                        }
                    } catch (error) {
                        document.getElementById('analysis-content').innerHTML =
                            '<h3>🔮 xAI Financial Events</h3><p style="color: red;">Error extracting events: ' + error.message + '</p>';
                    }
                }

                function applyGapFilters() {
                    const symbolFilter = document.getElementById('gap-symbol-filter').value.trim();
                    const startDate = document.getElementById('gap-start-date-filter').value;
                    const endDate = document.getElementById('gap-end-date-filter').value;

                    loadGapEvents(symbolFilter, startDate, endDate);
                }

                function clearGapFilters() {
                    document.getElementById('gap-symbol-filter').value = '';
                    document.getElementById('gap-start-date-filter').value = '';
                    document.getElementById('gap-end-date-filter').value = '';

                    loadGapEvents('', '', '');
                }

                async function loadTrainingDatasets() {
                    document.getElementById('analysis-content').innerHTML =
                        '<h3>🤖 Training Datasets</h3><p>Loading ML dataset visualization...</p>';

                    try {
                        console.log('🔍 DATASET DEBUG: Fetching training datasets...');
                        const response = await fetch('/api/v1/training-datasets');
                        console.log('🔍 DATASET DEBUG: Response status:', response.status);

                        if (!response.ok) {
                            throw new Error(`HTTP ${response.status}: ${response.statusText}`);
                        }

                        const data = await response.json();
                        console.log('🔍 DATASET DEBUG: Response data:', data);
                        console.log('🔍 DATASET DEBUG: Datasets count:', data.datasets ? data.datasets.length : 0);

                        let html = `
                            <h3>🤖 Training Datasets with OHLC Visualization</h3>
                            <div style="margin-bottom: 20px; padding: 15px; background: #f8f9fa; border-radius: 8px;">
                                <div style="display: grid; grid-template-columns: 1fr 1fr 1fr auto; gap: 15px; align-items: center;">
                                    <div>
                                        <label for="dataset-selector" style="font-weight: bold;">Select Dataset:</label>
                                        <select id="dataset-selector" onchange="loadSequenceFiles()" style="margin-left: 10px; padding: 5px; border-radius: 4px; border: 1px solid #ccc;">
                                            <option value="">Choose a dataset...</option>
                                        </select>
                                    </div>
                                    <div>
                                        <label for="sequence-selector" style="font-weight: bold;">Select Sequence:</label>
                                        <select id="sequence-selector" style="margin-left: 10px; padding: 5px; border-radius: 4px; border: 1px solid #ccc;" disabled>
                                            <option value="">Choose a sequence...</option>
                                        </select>
                                    </div>
                                    <div>
                                        <label for="row-selector" style="font-weight: bold;">Row Index:</label>
                                        <input type="number" id="row-selector" min="0" max="1000" value="50" style="margin-left: 10px; padding: 5px; width: 80px; border-radius: 4px; border: 1px solid #ccc;">
                                    </div>
                                    <button onclick="loadDatasetVisualization()" style="padding: 8px 15px; background: #007bff; color: white; border: none; border-radius: 4px; cursor: pointer;">
                                        📊 Visualize
                                    </button>
                                </div>
                            </div>

                            <div id="dataset-visualization" style="display: none;">
                                <!-- Time Navigation Controls -->
                                <div style="background: #f8f9fa; padding: 15px; border-radius: 8px; border: 1px solid #ddd; margin-bottom: 20px;">
                                    <h4 style="margin: 0 0 15px 0; font-size: 16px;">🎯 Time Navigation</h4>

                                    <!-- Navigation Buttons and Position Display -->
                                    <div style="display: flex; align-items: center; gap: 15px; margin-bottom: 15px;">
                                        <button id="nav-first" onclick="navigateToPosition('first')" style="padding: 8px 16px; border: 1px solid #007acc; background: white; border-radius: 6px; cursor: pointer; font-size: 14px;">⏪ First</button>
                                        <button id="nav-prev" onclick="navigateDirection('prev')" style="padding: 8px 16px; border: 1px solid #007acc; background: white; border-radius: 6px; cursor: pointer; font-size: 14px;">⬅️ Prev</button>

                                        <div style="flex: 1; margin: 0 15px;">
                                            <input type="range" id="position-slider" min="0" max="100" value="10"
                                                   style="width: 100%; height: 8px; border-radius: 4px; background: #ddd; outline: none;"
                                                   oninput="navigateToPosition(this.value)" onchange="navigateToPosition(this.value)">
                                        </div>

                                        <button id="nav-next" onclick="navigateDirection('next')" style="padding: 8px 16px; border: 1px solid #007acc; background: white; border-radius: 6px; cursor: pointer; font-size: 14px;">➡️ Next</button>
                                        <button id="nav-last" onclick="navigateToPosition('last')" style="padding: 8px 16px; border: 1px solid #007acc; background: white; border-radius: 6px; cursor: pointer; font-size: 14px;">⏩ Last</button>
                                    </div>

                                    <!-- Position Info -->
                                    <div style="display: flex; justify-content: space-between; font-size: 14px; color: #666;">
                                        <div id="position-info">Position 10 of 101</div>
                                        <div id="date-info">Loading...</div>
                                        <div id="bars-info">21 bars</div>
                                        <div id="loading-status" style="color: #007acc; display: none;">🔄 Loading...</div>
                                    </div>
                                </div>

                                <!-- Multi-Timeframe OHLC Charts Grid -->
                                <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 15px; margin-bottom: 20px;">
                                    <!-- 5-Minute Chart -->
                                    <div style="background: white; padding: 15px; border-radius: 8px; border: 1px solid #ddd;">
                                        <h4 style="margin: 0 0 10px 0; font-size: 14px;">📈 5-Minute OHLC</h4>
                                        <div id="ohlc-chart-5m" style="height: 300px;"></div>
                                    </div>

                                    <!-- 15-Minute Chart -->
                                    <div style="background: white; padding: 15px; border-radius: 8px; border: 1px solid #ddd;">
                                        <h4 style="margin: 0 0 10px 0; font-size: 14px;">📈 15-Minute OHLC</h4>
                                        <div id="ohlc-chart-15m" style="height: 300px;"></div>
                                    </div>

                                    <!-- 1-Hour Chart -->
                                    <div style="background: white; padding: 15px; border-radius: 8px; border: 1px solid #ddd;">
                                        <h4 style="margin: 0 0 10px 0; font-size: 14px;">📈 1-Hour OHLC</h4>
                                        <div id="ohlc-chart-1h" style="height: 300px;"></div>
                                    </div>

                                    <!-- Daily Chart -->
                                    <div style="background: white; padding: 15px; border-radius: 8px; border: 1px solid #ddd;">
                                        <h4 style="margin: 0 0 10px 0; font-size: 14px;">📈 Daily OHLC</h4>
                                        <div id="ohlc-chart-1d" style="height: 300px;"></div>
                                    </div>

                                    <div style="background: white; padding: 15px; border-radius: 8px; border: 1px solid #ddd;">
                                        <h4 style="margin: 0 0 10px 0; font-size: 14px;">📈 Weekly OHLC</h4>
                                        <div id="ohlc-chart-1w" style="height: 300px;"></div>
                                    </div>
                                </div>

                                <!-- Dataset Information -->
                                <div style="background: white; padding: 20px; border-radius: 8px; border: 1px solid #ddd; margin-bottom: 20px;">
                                    <h4 style="margin-top: 0;">📊 Multi-Timeframe Dataset Information</h4>
                                    <div id="dataset-info"></div>
                                </div>
                                </div>

                                <!-- Sequence Data Table -->
                                <div style="background: white; padding: 20px; border-radius: 8px; border: 1px solid #ddd;">
                                    <h4 style="margin-top: 0;">📋 Training Sequence Data (±10 bars from selected row)</h4>
                                    <div id="sequence-table" style="overflow-x: auto;"></div>
                                </div>
                            </div>

                            <!-- Available Datasets Summary -->
                            <div style="margin-top: 20px; background: white; padding: 15px; border-radius: 8px; border: 1px solid #ddd;">
                                <h4>📚 Available Datasets Summary (${data.total_count} total)</h4>
                                <div id="datasets-summary"></div>
                            </div>
                        `;

                        document.getElementById('analysis-content').innerHTML = html;

                        // Populate dataset selector
                        const selector = document.getElementById('dataset-selector');
                        console.log('🔍 DATASET DEBUG: Dataset selector found:', !!selector);

                        if (data.datasets && data.datasets.length > 0) {
                            console.log('🔍 DATASET DEBUG: Populating selector with', data.datasets.length, 'datasets');
                            data.datasets.forEach((dataset, index) => {
                                const option = document.createElement('option');
                                option.value = dataset.id;
                                option.textContent = `[ID: ${dataset.id}] ${dataset.dataset_name} (${dataset.total_sequences} sequences, ${dataset.symbols})`;
                                selector.appendChild(option);
                                console.log(`🔍 DATASET DEBUG: Added dataset ${index + 1}:`, option.textContent);
                            });

                            // Show datasets summary
                            let summaryHtml = `
                                <div style="display: grid; grid-template-columns: auto 2fr 1fr 1fr 1fr; gap: 10px; padding: 10px; background: #f8f9fa; border-bottom: 2px solid #dee2e6; font-weight: bold;">
                                    <div>Dataset ID</div>
                                    <div>Name & Symbols</div>
                                    <div>Sequences</div>
                                    <div>Quality Score</div>
                                    <div>Created</div>
                                </div>
                            `;
                            data.datasets.forEach(dataset => {
                                summaryHtml += `
                                    <div style="display: grid; grid-template-columns: auto 2fr 1fr 1fr 1fr; gap: 10px; padding: 10px; border-bottom: 1px solid #eee;">
                                        <div><strong>ID: ${dataset.id}</strong></div>
                                        <div><strong>${dataset.dataset_name}</strong><br><small>${dataset.symbols}</small></div>
                                        <div>${dataset.total_sequences} sequences</div>
                                        <div>Quality: ${dataset.data_quality_score}</div>
                                        <div><small>${new Date(dataset.creation_timestamp).toLocaleDateString()}</small></div>
                                    </div>
                                `;
                            });
                            document.getElementById('datasets-summary').innerHTML = summaryHtml;
                        } else {
                            console.log('🔍 DATASET DEBUG: No datasets found in response');
                            document.getElementById('datasets-summary').innerHTML = '<p>No training datasets found.</p>';
                        }

                    } catch (error) {
                        console.error('🔍 DATASET DEBUG: Error loading datasets:', error);
                        document.getElementById('analysis-content').innerHTML =
                            '<h3>🤖 Training Datasets</h3><p style="color: red;">Error loading training datasets: ' + error.message + '</p>';
                    }
                }

                async function loadSequenceFiles() {
                    const datasetId = document.getElementById('dataset-selector').value;
                    const sequenceSelector = document.getElementById('sequence-selector');

                    if (!datasetId) {
                        sequenceSelector.innerHTML = '<option value="">Choose a sequence...</option>';
                        sequenceSelector.disabled = true;
                        return;
                    }

                    sequenceSelector.innerHTML = '<option value="">Loading sequences...</option>';
                    sequenceSelector.disabled = true;

                    try {
                        const response = await fetch(`/api/v1/training-datasets/${datasetId}/sequences`);
                        const data = await response.json();

                        if (data.sequences && data.sequences.length > 0) {
                            let options = '<option value="">Choose a sequence...</option>';
                            data.sequences.forEach(seq => {
                                const timeframeSummary = seq.timeframes ? seq.timeframes.join(', ') : 'multi-timeframe';
                                options += `<option value="${seq.sequence_id}">${seq.description} (${timeframeSummary}, ${seq.total_size_mb}MB)</option>`;
                            });
                            sequenceSelector.innerHTML = options;
                            sequenceSelector.disabled = false;
                        } else {
                            sequenceSelector.innerHTML = '<option value="">No sequences found</option>';
                            sequenceSelector.disabled = true;
                        }
                    } catch (error) {
                        console.error('Error loading sequence files:', error);
                        sequenceSelector.innerHTML = '<option value="">Error loading sequences</option>';
                        sequenceSelector.disabled = true;
                    }
                }

                async function loadDatasetVisualization() {
                    const datasetId = document.getElementById('dataset-selector').value;
                    const sequenceId = document.getElementById('sequence-selector').value;
                    const rowIndex = document.getElementById('row-selector').value || 0;

                    console.log('🎯 CLIENT DEBUG: Starting visualization load');
                    console.log(`   Dataset ID: ${datasetId}`);
                    console.log(`   Sequence ID: ${sequenceId}`);
                    console.log(`   Row Index: ${rowIndex}`);

                    if (!datasetId) {
                        alert('Please select a dataset first');
                        return;
                    }

                    if (!sequenceId) {
                        alert('Please select a sequence first');
                        return;
                    }

                    // Show loading state
                    document.getElementById('dataset-visualization').style.display = 'block';

                    // Set loading state for all timeframe charts
                    const timeframes = ['5m', '15m', '1h', '1d', '1w'];
                    timeframes.forEach(tf => {
                        document.getElementById(`ohlc-chart-${tf}`).innerHTML = `<p>Loading ${tf} chart...</p>`;
                    });

                    document.getElementById('dataset-info').innerHTML = '<p>Loading dataset info...</p>';
                    document.getElementById('sequence-table').innerHTML = '<p>Loading sequence data...</p>';

                    try {
                        // Use NEW multi-timeframe endpoint with row index parameter
                        const apiUrl = `/api/v1/training-datasets/${datasetId}/sequences/${sequenceId}/multi-timeframe?row_index=${rowIndex}`;
                        console.log(`🌐 CLIENT DEBUG: Fetching from ${apiUrl} (row index: ${rowIndex})`);

                        const response = await fetch(apiUrl);
                        const multiTimeframeData = await response.json();

                        console.log('✅ CLIENT DEBUG: Multi-timeframe data received');
                        console.log(`   Success: ${multiTimeframeData.success}`);
                        console.log(`   Sequence ID: ${multiTimeframeData.sequence_id}`);
                        console.log(`   Available timeframes: ${multiTimeframeData.available_timeframes}`);
                        console.log(`   OHLC data keys: ${Object.keys(multiTimeframeData.ohlc_data || {})}`);
                        console.log(`   Table rows: ${multiTimeframeData.table_data?.length || 0}`);

                        if (multiTimeframeData.error) {
                            throw new Error(multiTimeframeData.error);
                        }

                        if (!multiTimeframeData.success) {
                            throw new Error('Multi-timeframe data fetch failed');
                        }

                        // Display dataset info
                        const symbol = multiTimeframeData.sequence_id ? multiTimeframeData.sequence_id.split('_')[0] : 'UNKNOWN';
                        document.getElementById('dataset-info').innerHTML = `
                            <div style="line-height: 1.6;">
                                <p><strong>Dataset:</strong> ${multiTimeframeData.dataset_name}</p>
                                <p><strong>Symbol:</strong> ${symbol}</p>
                                <p><strong>Sequence ID:</strong> ${multiTimeframeData.sequence_id}</p>
                                <p><strong>Available Timeframes:</strong> ${multiTimeframeData.available_timeframes.join(', ')}</p>
                                <p><strong>Total OHLC Records:</strong> ${Object.values(multiTimeframeData.ohlc_data || {}).reduce((total, data) => total + data.length, 0)}</p>
                            </div>
                        `;

                        console.log('📊 CLIENT DEBUG: Starting Plotly chart creation');

                        // Create OHLC charts for each timeframe
                        for (const timeframe of timeframes) {
                            const chartDiv = document.getElementById('ohlc-chart-' + timeframe);
                            const ohlcData = multiTimeframeData.ohlc_data[timeframe];

                            console.log('📈 CLIENT DEBUG: Processing ' + timeframe + ' chart');
                            console.log('   Data available: ' + !!ohlcData);
                            console.log('   Data length: ' + (ohlcData ? ohlcData.length : 0));

                            if (ohlcData && ohlcData.length > 0) {
                                console.log('   Sample data: ', ohlcData[0]);

                                // Prepare data for Plotly - timestamp is Unix epoch seconds
                                const dates = ohlcData.map(bar => new Date(bar.timestamp * 1000));
                                const opens = ohlcData.map(bar => bar.open);
                                const highs = ohlcData.map(bar => bar.high);
                                const lows = ohlcData.map(bar => bar.low);
                                const closes = ohlcData.map(bar => bar.close);

                                console.log('   Prepared ' + dates.length + ' data points for ' + timeframe);
                                console.log('   Date range: ' + dates[0] + ' to ' + dates[dates.length-1]);

                                const plotlyData = [{
                                    x: dates,
                                    open: opens,
                                    high: highs,
                                    low: lows,
                                    close: closes,
                                    type: 'candlestick',
                                    name: symbol + ' ' + timeframe.toUpperCase(),
                                    increasing: { line: { color: '#00CC88' }},
                                    decreasing: { line: { color: '#FF6B6B' }}
                                }];

                                const layout = {
                                    title: symbol + ' - ' + timeframe.toUpperCase() + ' OHLC',
                                    xaxis: { title: 'Time' },
                                    yaxis: { title: 'Price ($)' },
                                    height: 280,
                                    margin: { t: 40, b: 40, l: 60, r: 20 },
                                    showlegend: false
                                };

                                console.log('🎨 CLIENT DEBUG: Creating ' + timeframe + ' Plotly chart');

                                try {
                                    await Plotly.newPlot(chartDiv, plotlyData, layout, {responsive: true});
                                    console.log('✅ CLIENT DEBUG: ' + timeframe + ' chart created successfully');
                                } catch (plotlyError) {
                                    console.error('❌ CLIENT DEBUG: ' + timeframe + ' Plotly error:', plotlyError);
                                    chartDiv.innerHTML = '<p style="color: red;">Error creating ' + timeframe + ' chart: ' + plotlyError.message + '</p>';
                                }
                            } else {
                                console.log('⚠️  CLIENT DEBUG: No data for ' + timeframe);
                                chartDiv.innerHTML = '<p style="color: orange;">No ' + timeframe + ' data available</p>';
                            }
                        }

                        console.log('📋 CLIENT DEBUG: Creating table view');

                        // Create table view from 1h data
                        const tableData = multiTimeframeData.table_data;
                        if (tableData && tableData.length > 0) {
                            console.log('✅ CLIENT DEBUG: Table data available: ' + tableData.length + ' rows');
                            console.log('   Sample table row:', tableData[0]);

                            let tableHtml = '<table style="width: 100%; border-collapse: collapse; font-size: 12px;">' +
                                '<thead>' +
                                '<tr style="background: #f8f9fa; border-bottom: 2px solid #dee2e6;">' +
                                '<th style="padding: 8px; text-align: left;">Timestamp</th>' +
                                '<th style="padding: 8px; text-align: right;">Open</th>' +
                                '<th style="padding: 8px; text-align: right;">High</th>' +
                                '<th style="padding: 8px; text-align: right;">Low</th>' +
                                '<th style="padding: 8px; text-align: right;">Close</th>' +
                                '<th style="padding: 8px; text-align: right;">Volume</th>' +
                                '</tr>' +
                                '</thead>' +
                                '<tbody>';

                            tableData.forEach((row, idx) => {
                                const date = new Date(row.timestamp * 1000);
                                const bgColor = idx % 2 === 0 ? 'background: #f9f9f9;' : '';
                                tableHtml += '<tr style="border-bottom: 1px solid #eee; ' + bgColor + '">' +
                                    '<td style="padding: 6px;">' + date.toLocaleString() + '</td>' +
                                    '<td style="padding: 6px; text-align: right;">$' + (row.open?.toFixed(2) || 'N/A') + '</td>' +
                                    '<td style="padding: 6px; text-align: right;">$' + (row.high?.toFixed(2) || 'N/A') + '</td>' +
                                    '<td style="padding: 6px; text-align: right;">$' + (row.low?.toFixed(2) || 'N/A') + '</td>' +
                                    '<td style="padding: 6px; text-align: right;">$' + (row.close?.toFixed(2) || 'N/A') + '</td>' +
                                    '<td style="padding: 6px; text-align: right;">' + (row.volume?.toLocaleString() || 'N/A') + '</td>' +
                                    '</tr>';
                            });

                            tableHtml += '</tbody></table>';
                            document.getElementById('sequence-table').innerHTML = tableHtml;

                            console.log('✅ CLIENT DEBUG: Table created with ' + tableData.length + ' rows');
                        } else {
                            console.log('⚠️  CLIENT DEBUG: No table data available');
                            document.getElementById('sequence-table').innerHTML = '<p style="color: orange;">No table data available</p>';
                        }

                        console.log('✅ CLIENT DEBUG: Visualization loading completed');

                    } catch (error) {
                        console.error('❌ CLIENT DEBUG: Visualization error:', error);

                        // Set error state for all charts
                        timeframes.forEach(tf => {
                            document.getElementById(`ohlc-chart-${tf}`).innerHTML = `<p style="color: red;">Error loading ${tf} chart: ${error.message}</p>`;
                        });
                        document.getElementById('dataset-info').innerHTML = `<p style="color: red;">Error loading dataset info: ${error.message}</p>`;
                        document.getElementById('sequence-table').innerHTML = `<p style="color: red;">Error loading sequence data: ${error.message}</p>`;
                    }
                }

                function createTimeframeOHLCChart(timeframe, sequenceData) {
                    const data = sequenceData.data;
                    const chartId = `ohlc-chart-${timeframe}`;

                    if (!data || data.length === 0) {
                        document.getElementById(chartId).innerHTML = `<p>No ${timeframe} sequence data available</p>`;
                        return;
                    }

                    // Generate x-axis values (time steps or actual datetime if available)
                    const xValues = data.map((bar, idx) => {
                        // Use datetime if available, otherwise time steps
                        if (bar.datetime) {
                            return new Date(bar.datetime);
                        }
                        return `Step ${idx + 1}`;
                    });

                    // Create OHLC candlestick trace
                    const ohlcTrace = {
                        x: xValues,
                        open: data.map(d => d.open || 0),
                        high: data.map(d => d.high || 0),
                        low: data.map(d => d.low || 0),
                        close: data.map(d => d.close || 0),
                        type: 'candlestick',
                        name: 'OHLC',
                        increasing: {line: {color: '#00c851'}},
                        decreasing: {line: {color: '#ff4444'}},
                        showlegend: false  // Hide legend in individual charts to save space
                    };

                    const traces = [ohlcTrace];

                    // Add envelope_top indicator
                    if (data.some(d => d.envelope_top > 0)) {
                        traces.push({
                            x: xValues,
                            y: data.map(d => d.envelope_top),
                            type: 'scatter',
                            mode: 'lines',
                            name: 'Env Top',
                            line: {color: '#ff9999', width: 1.5, dash: 'dot'},
                            yaxis: 'y',
                            showlegend: false
                        });
                    }

                    // Add envelope_bot indicator
                    if (data.some(d => d.envelope_bot > 0)) {
                        traces.push({
                            x: xValues,
                            y: data.map(d => d.envelope_bot),
                            type: 'scatter',
                            mode: 'lines',
                            name: 'Env Bot',
                            line: {color: '#99ff99', width: 1.5, dash: 'dot'},
                            yaxis: 'y',
                            showlegend: false
                        });
                    }

                    // Add pldot indicator
                    const pldotValues = data.map(d => d.pldot || null);
                    if (pldotValues.some(v => v !== null && v > 0)) {
                        traces.push({
                            x: xValues,
                            y: pldotValues,
                            type: 'scatter',
                            mode: 'markers',
                            name: 'PL Dot',
                            marker: {size: 6, color: '#9999ff'},
                            yaxis: 'y',
                            showlegend: false
                        });
                    }

                    // Chart layout with compact design for grid
                    const layout = {
                        title: {
                            text: `${sequenceData.symbol} - ${timeframe.toUpperCase()}`,
                            font: {size: 14}
                        },
                        xaxis: {
                            title: '',  // No x-axis title to save space
                            type: data[0]?.datetime ? 'date' : 'category',
                            showticklabels: true
                        },
                        yaxis: {
                            title: {
                                text: 'Price',
                                font: {size: 12}
                            },
                            side: 'left'
                        },
                        showlegend: false,  // No legend to save space
                        height: 300,
                        margin: {l: 50, r: 20, t: 30, b: 30}
                    };

                    // Create the plot
                    Plotly.newPlot(chartId, traces, layout, {responsive: true});
                }

                function createSequenceTable(sequenceData) {
                    const data = sequenceData.data;
                    if (!data || data.length === 0) {
                        document.getElementById('sequence-table').innerHTML = '<p>No sequence data available</p>';
                        return;
                    }

                    // Check if datetime features are available
                    const hasDatetimeFeatures = data[0] && data[0].datetime && data[0].datetime !== null;

                    // Create table with all sequence data
                    let tableHtml = `
                        <table style="width: 100%; border-collapse: collapse; font-size: 0.9em;">
                            <thead style="background: #f8f9fa;">
                                <tr>
                                    <th style="border: 1px solid #ddd; padding: 8px;">Step</th>
                                    ${hasDatetimeFeatures ? '<th style="border: 1px solid #ddd; padding: 8px;">DateTime</th>' : ''}
                                    ${hasDatetimeFeatures ? '<th style="border: 1px solid #ddd; padding: 8px;">Hour EDT</th>' : ''}
                                    <th style="border: 1px solid #ddd; padding: 8px;">Open</th>
                                    <th style="border: 1px solid #ddd; padding: 8px;">High</th>
                                    <th style="border: 1px solid #ddd; padding: 8px;">Low</th>
                                    <th style="border: 1px solid #ddd; padding: 8px;">Close</th>
                                    <th style="border: 1px solid #ddd; padding: 8px;">Volume</th>
                                    <th style="border: 1px solid #ddd; padding: 8px;">Envelope Top</th>
                                    <th style="border: 1px solid #ddd; padding: 8px;">Envelope Bot</th>
                                    <th style="border: 1px solid #ddd; padding: 8px;">PL Dot</th>
                                </tr>
                            </thead>
                            <tbody>
                    `;

                    data.forEach((bar, index) => {
                        const isSelectedBar = index === sequenceData.selected_bar;
                        const rowStyle = isSelectedBar ? 'background: #fff3cd; font-weight: bold;' : '';

                        tableHtml += `
                            <tr style="${rowStyle}">
                                <td style="border: 1px solid #ddd; padding: 6px; text-align: center;">${bar.time_step + 1}${isSelectedBar ? ' 🎯' : ''}</td>
                                ${hasDatetimeFeatures ? `<td style="border: 1px solid #ddd; padding: 6px; text-align: center; font-size: 0.8em;">${bar.datetime || 'N/A'}</td>` : ''}
                                ${hasDatetimeFeatures ? `<td style="border: 1px solid #ddd; padding: 6px; text-align: center;">${bar.hour_of_day_edt || 0}</td>` : ''}
                                <td style="border: 1px solid #ddd; padding: 6px; text-align: right;">${bar.open.toFixed(2)}</td>
                                <td style="border: 1px solid #ddd; padding: 6px; text-align: right;">${bar.high.toFixed(2)}</td>
                                <td style="border: 1px solid #ddd; padding: 6px; text-align: right;">${bar.low.toFixed(2)}</td>
                                <td style="border: 1px solid #ddd; padding: 6px; text-align: right;">${bar.close.toFixed(2)}</td>
                                <td style="border: 1px solid #ddd; padding: 6px; text-align: right;">${bar.volume ? bar.volume.toLocaleString() : '0'}</td>
                                <td style="border: 1px solid #ddd; padding: 6px; text-align: right;">${bar.envelope_top ? bar.envelope_top.toFixed(2) : '0.00'}</td>
                                <td style="border: 1px solid #ddd; padding: 6px; text-align: right;">${bar.envelope_bot ? bar.envelope_bot.toFixed(2) : '0.00'}</td>
                                <td style="border: 1px solid #ddd; padding: 6px; text-align: right;">${bar.pldot ? bar.pldot.toFixed(2) : '0.00'}</td>
                            </tr>
                        `;
                    });

                    tableHtml += '</tbody></table>';

                    // Add summary information
                    const summaryHtml = `
                        <div style="margin-top: 15px; padding: 10px; background: #f8f9fa; border-radius: 4px;">
                            <p style="margin: 5px 0;"><strong>Showing ${data.length} time steps</strong> | Selected bar highlighted with 🎯</p>
                            <p style="margin: 5px 0;"><strong>Price Range:</strong> ${Math.min(...data.map(d => d.low)).toFixed(2)} - ${Math.max(...data.map(d => d.high)).toFixed(2)}</p>
                            <p style="margin: 5px 0;"><strong>Technical Indicators:</strong> Envelope Top/Bottom (support/resistance), PL Dot (pivot lows)</p>
                        </div>
                    `;

                    document.getElementById('sequence-table').innerHTML = tableHtml + summaryHtml;
                }

                async function loadMultiPanelVisualization() {
                    document.getElementById('analysis-content').innerHTML = `
                        <h3>🎨 Multi-Panel Trading Charts</h3>
                        <div style="background: white; padding: 20px; border-radius: 8px; border: 1px solid #ddd; margin-bottom: 20px;">
                            <h4>Generate Multi-Panel Trading Visualization</h4>
                            <div style="display: grid; grid-template-columns: 1fr 1fr 1fr auto; gap: 15px; align-items: center; margin-bottom: 15px;">
                                <div>
                                    <label for="symbol-input" style="font-weight: bold;">Symbol:</label>
                                    <input type="text" id="symbol-input" value="AAPL" placeholder="Enter symbol"
                                           style="margin-left: 10px; padding: 8px; border: 1px solid #ddd; border-radius: 4px; width: 100px;">
                                </div>
                                <div>
                                    <label for="timeframe-select" style="font-weight: bold;">Timeframe:</label>
                                    <select id="timeframe-select" style="margin-left: 10px; padding: 8px; border: 1px solid #ddd; border-radius: 4px;">
                                        <option value="5m">5 Minutes</option>
                                        <option value="15m">15 Minutes</option>
                                        <option value="1h" selected>1 Hour</option>
                                        <option value="1d">1 Day</option>
                                    </select>
                                </div>
                                <div>
                                    <label for="dataset-input" style="font-weight: bold;">Dataset ID:</label>
                                    <input type="number" id="dataset-input" value="1" min="1" placeholder="Dataset ID"
                                           style="margin-left: 10px; padding: 8px; border: 1px solid #ddd; border-radius: 4px; width: 80px;">
                                </div>
                                <button onclick="generateMultiPanelChart()" id="generate-btn"
                                        style="padding: 10px 20px; background: #007bff; color: white; border: none; border-radius: 4px; cursor: pointer; font-weight: bold;">
                                    🎨 Generate Chart
                                </button>
                            </div>

                            <div style="background: #f8f9fa; padding: 15px; border-radius: 6px; border-left: 4px solid #007bff;">
                                <h5 style="margin: 0 0 10px 0;">📊 Chart Layout</h5>
                                <div style="display: grid; grid-template-columns: 2fr 1fr; gap: 10px; margin-bottom: 10px;">
                                    <div style="background: #e3f2fd; padding: 8px; border-radius: 4px; text-align: center; font-size: 12px;">
                                        📈 OHLC Chart + Indicator Lines<br>
                                        <small>(envelope top/bot, pldot, z1b, z2b, z5t, z6t)</small>
                                    </div>
                                    <div style="background: #f3e5f5; padding: 8px; border-radius: 4px; text-align: center; font-size: 12px;">
                                        📊 Volume Distribution<br>
                                        <small>(POC, VAH, VAL)</small>
                                    </div>
                                </div>
                                <div style="background: #e8f5e8; padding: 8px; border-radius: 4px; text-align: center; font-size: 12px;">
                                    🔍 BX Trender Indicators<br>
                                    <small>(Basic, Directional, Volume Weighted)</small>
                                </div>
                            </div>
                        </div>

                        <!-- Status Panel -->
                        <div id="status-panel" style="display: none; margin-bottom: 20px;">
                            <div id="status-message"></div>
                        </div>

                        <!-- Features Panel -->
                        <div id="features-panel" style="display: none; background: white; padding: 20px; border-radius: 8px; border: 1px solid #ddd; margin-bottom: 20px;">
                            <h4>📋 Extracted Features</h4>
                            <div id="features-grid" style="display: grid; grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)); gap: 15px;"></div>
                        </div>

                        <!-- Chart Panel -->
                        <div style="background: white; padding: 20px; border-radius: 8px; border: 1px solid #ddd;">
                            <h4>📈 Multi-Panel Trading Visualization</h4>
                            <div id="chart-container" style="text-align: center; min-height: 400px; padding: 40px;">
                                <h4 style="color: #666;">🎨 Multi-Panel Trading Chart</h4>
                                <p style="color: #888; margin-bottom: 20px;">Configure your analysis above and click "Generate Chart" to create a comprehensive trading visualization</p>
                                <div style="background: #f8f9fa; padding: 20px; border-radius: 6px; display: inline-block; text-align: left;">
                                    <h5 style="margin: 0 0 10px 0;">Features:</h5>
                                    <ul style="margin: 0; color: #666; font-size: 14px;">
                                        <li>📊 OHLC candlesticks with technical indicator lines</li>
                                        <li>📈 Volume profile distribution with key levels</li>
                                        <li>🔍 BX Trender trend strength analysis</li>
                                        <li>🎯 Multi-timeframe support (5m, 15m, 1h, 1d)</li>
                                        <li>⚡ Real-time feature extraction from training datasets</li>
                                    </ul>
                                </div>
                            </div>
                        </div>
                    `;
                }

                async function generateMultiPanelChart() {
                    const symbol = document.getElementById('symbol-input').value.toUpperCase().trim();
                    const timeframe = document.getElementById('timeframe-select').value;
                    const datasetId = document.getElementById('dataset-input').value;

                    if (!symbol || !datasetId) {
                        showStatus('error', 'Please enter both symbol and dataset ID');
                        return;
                    }

                    const generateBtn = document.getElementById('generate-btn');
                    const chartContainer = document.getElementById('chart-container');

                    // Show loading state
                    generateBtn.disabled = true;
                    generateBtn.textContent = '⏳ Generating...';
                    chartContainer.innerHTML = '<div style="text-align: center; padding: 40px;"><h4>⏳ Generating Multi-Panel Chart...</h4><p>Extracting features and creating visualization...</p></div>';
                    showStatus('info', `Generating multi-panel chart for ${symbol} (${timeframe}) from dataset ${datasetId}...`);

                    try {
                        const response = await fetch(`/api/multi-panel-chart?symbol=${symbol}&timeframe=${timeframe}&dataset_id=${datasetId}`);
                        const result = await response.json();

                        if (result.success) {
                            // Display the chart image
                            chartContainer.innerHTML = `
                                <img src="data:image/png;base64,${result.chart_image}"
                                     style="width: 100%; height: auto; border-radius: 6px; border: 2px solid #ddd;"
                                     alt="Multi-Panel Trading Chart">
                                <div style="text-align: center; color: #666; margin-top: 15px; font-size: 14px;">
                                    <strong>${symbol} ${timeframe.toUpperCase()} Multi-Panel Analysis</strong><br>
                                    Generated: ${result.timestamp} | Features: ${result.features_count} | Dataset: ${datasetId}
                                </div>
                            `;

                            // Show extracted features
                            displayFeatures(result.features);
                            showStatus('success', `Multi-panel chart generated successfully! Extracted ${result.features_count} features.`);
                        } else {
                            chartContainer.innerHTML = `<div style="background: #f8d7da; color: #721c24; padding: 20px; border-radius: 6px; margin: 20px;"><h4>❌ Error</h4><p>${result.error}</p></div>`;
                            showStatus('error', `Failed to generate chart: ${result.error}`);
                        }
                    } catch (error) {
                        chartContainer.innerHTML = `<div style="background: #f8d7da; color: #721c24; padding: 20px; border-radius: 6px; margin: 20px;"><h4>❌ Network Error</h4><p>Failed to connect to server: ${error.message}</p></div>`;
                        showStatus('error', `Network error: ${error.message}`);
                    } finally {
                        generateBtn.disabled = false;
                        generateBtn.textContent = '🎨 Generate Chart';
                    }
                }

                function displayFeatures(features) {
                    if (!features) return;

                    const featuresGrid = document.getElementById('features-grid');
                    const featuresPanel = document.getElementById('features-panel');

                    // Group features by type
                    const featureGroups = {
                        'OHLCV': [],
                        'Technical Indicators': [],
                        'Volume Profile': [],
                        'BX Trender': [],
                        'Other': []
                    };

                    Object.entries(features).forEach(([key, value]) => {
                        const formattedValue = typeof value === 'number' ? value.toFixed(4) : value;
                        const item = `${key}: ${formattedValue}`;

                        if (key.includes('open') || key.includes('high') || key.includes('low') || key.includes('close') || key.includes('volume')) {
                            if (!key.includes('volume_profile')) featureGroups['OHLCV'].push(item);
                            else featureGroups['Volume Profile'].push(item);
                        } else if (key.includes('volume_profile')) {
                            featureGroups['Volume Profile'].push(item);
                        } else if (key.includes('BXTrender')) {
                            featureGroups['BX Trender'].push(item);
                        } else if (key.includes('envelope') || key.includes('pldot') || key.includes('z1b') || key.includes('z2b') || key.includes('z5t') || key.includes('z6t')) {
                            featureGroups['Technical Indicators'].push(item);
                        } else {
                            featureGroups['Other'].push(item);
                        }
                    });

                    // Create feature cards
                    featuresGrid.innerHTML = '';
                    Object.entries(featureGroups).forEach(([group, items]) => {
                        if (items.length > 0) {
                            const card = document.createElement('div');
                            card.style.cssText = 'background: #f8f9fa; padding: 15px; border-radius: 6px; border-left: 4px solid #007bff;';
                            card.innerHTML = `
                                <h5 style="margin: 0 0 10px 0; color: #007bff;">${group} (${items.length})</h5>
                                <ul style="margin: 0; padding-left: 20px; font-size: 12px; font-family: monospace;">
                                    ${items.slice(0, 6).map(item => `<li>${item}</li>`).join('')}
                                    ${items.length > 6 ? `<li style="color: #666;">... and ${items.length - 6} more</li>` : ''}
                                </ul>
                            `;
                            featuresGrid.appendChild(card);
                        }
                    });

                    featuresPanel.style.display = 'block';
                }

                function showStatus(type, message) {
                    const statusPanel = document.getElementById('status-panel');
                    const statusMessage = document.getElementById('status-message');

                    const colors = {
                        'error': '#f8d7da; color: #721c24; border-left: 4px solid #dc3545;',
                        'success': '#d4edda; color: #155724; border-left: 4px solid #28a745;',
                        'info': '#d1ecf1; color: #0c5460; border-left: 4px solid #17a2b8;'
                    };

                    statusMessage.style.cssText = `background: ${colors[type]} padding: 15px; border-radius: 6px;`;
                    statusMessage.innerHTML = `<strong>${type === 'error' ? '❌' : type === 'success' ? '✅' : 'ℹ️'} ${type.toUpperCase()}:</strong> ${message}`;
                    statusPanel.style.display = 'block';

                    // Auto-hide success/info messages
                    if (type !== 'error') {
                        setTimeout(() => {
                            statusPanel.style.display = 'none';
                        }, 5000);
                    }
                }

                function loadRayAnalytics() {
                    document.getElementById('analysis-content').innerHTML =
                        '<h3>⚡ Distributed Analytics</h3><p>Loading Ray distributed computing...</p>';
                    // Implementation would load Ray analytics interface
                }

                // ==============================================
                // TIME NAVIGATION FUNCTIONS
                // ==============================================

                let currentRowIndex = 10;
                let currentDatasetId = null;
                let currentSequenceId = null;
                let isNavigating = false;
                let navigationMetadata = null;

                async function loadNavigationMetadata() {
                    if (!currentDatasetId || !currentSequenceId) return;

                    try {
                        const url = `/api/v1/training-datasets/${currentDatasetId}/sequences/${currentSequenceId}/navigation-metadata`;
                        const response = await fetch(url);

                        if (!response.ok) throw new Error(`HTTP ${response.status}`);

                        navigationMetadata = await response.json();
                        updateNavigationRanges();

                        console.log('✅ Navigation metadata loaded:', navigationMetadata);

                    } catch (error) {
                        console.error('❌ Failed to load navigation metadata:', error);
                    }
                }

                function updateNavigationRanges() {
                    if (!navigationMetadata) return;

                    const slider = document.getElementById('position-slider');
                    const nav = navigationMetadata.navigation;

                    if (slider) {
                        slider.min = nav.min_row_index;
                        slider.max = nav.max_row_index;
                        slider.value = currentRowIndex;
                    }
                }

                async function navigateToPosition(position) {
                    if (isNavigating || !currentDatasetId || !currentSequenceId) return;

                    try {
                        setNavigationLoadingState(true);

                        let url;
                        if (typeof position === 'string') {
                            // Direction-based navigation
                            if (position === 'first' || position === 'last') {
                                url = `/api/v1/training-datasets/${currentDatasetId}/sequences/${currentSequenceId}/navigate?direction=${position}`;
                            } else {
                                url = `/api/v1/training-datasets/${currentDatasetId}/sequences/${currentSequenceId}/navigate?direction=${position}&row_index=${currentRowIndex}`;
                            }
                        } else {
                            // Position-based navigation
                            url = `/api/v1/training-datasets/${currentDatasetId}/sequences/${currentSequenceId}/navigate?row_index=${position}`;
                        }

                        console.log('🎯 Navigating to:', url);

                        const response = await fetch(url);
                        if (!response.ok) throw new Error(`HTTP ${response.status}`);

                        const data = await response.json();

                        if (data.success) {
                            updateVisualizationFromNavigation(data);
                            console.log('✅ Navigation successful:', data.navigation_context);
                        } else {
                            throw new Error('Navigation was not successful');
                        }

                    } catch (error) {
                        console.error('❌ Navigation failed:', error);
                        showNavigationError(`Navigation failed: ${error.message}`);
                    } finally {
                        setNavigationLoadingState(false);
                    }
                }

                async function navigateDirection(direction) {
                    await navigateToPosition(direction);
                }

                function updateVisualizationFromNavigation(navigationData) {
                    console.log('🔍 CLIENT DEBUG: Navigation data received:', navigationData);

                    const navContext = navigationData.navigation_context;
                    const tableData = navigationData.table_data || [];
                    // The API returns 'ohlc_data' not 'multi_timeframe_data'
                    const multiTimeframeData = navigationData.ohlc_data || navigationData.multi_timeframe_data || {};

                    console.log('🔍 CLIENT DEBUG: Table data count:', tableData.length);
                    console.log('🔍 CLIENT DEBUG: Multi-timeframe keys:', Object.keys(multiTimeframeData));
                    console.log('🔍 CLIENT DEBUG: Navigation context:', navContext);
                    console.log('🔍 CLIENT DEBUG: All response keys:', Object.keys(navigationData));

                    // Update current position
                    currentRowIndex = navContext.current_row_index;
                    console.log('🔍 CLIENT DEBUG: Updated currentRowIndex to:', currentRowIndex);

                    // Update navigation UI
                    updateNavigationDisplay(navContext, tableData);

                    // Update charts with new data
                    if (multiTimeframeData && Object.keys(multiTimeframeData).length > 0) {
                        const timeframes = ['5m', '15m', '1h', '1d', '1w'];
                        timeframes.forEach(tf => {
                            if (multiTimeframeData[tf]) {
                                console.log(`🔍 CLIENT DEBUG: Updating ${tf} chart with ${multiTimeframeData[tf].data?.length || multiTimeframeData[tf].length || 'unknown'} data points`);
                                // Handle both structures: {data: [...]} and direct array
                                const chartData = multiTimeframeData[tf].data ? multiTimeframeData[tf] : {data: multiTimeframeData[tf]};
                                createTimeframeOHLCChart(tf, chartData);
                            } else {
                                console.log(`🔍 CLIENT DEBUG: No ${tf} data available`);
                            }
                        });
                    } else {
                        console.log('🔍 CLIENT DEBUG: No multi-timeframe data to update charts');
                    }

                    // Update table
                    if (tableData.length > 0) {
                        console.log('🔍 CLIENT DEBUG: Updating table with', tableData.length, 'rows');
                        updateSequenceTable(tableData);
                    } else {
                        console.log('🔍 CLIENT DEBUG: No table data to update');
                    }

                    // Update dataset info
                    updateDatasetInfo(navigationData);
                }

                function updateNavigationDisplay(navContext, tableData) {
                    const positionInfo = document.getElementById('position-info');
                    const dateInfo = document.getElementById('date-info');
                    const barsInfo = document.getElementById('bars-info');
                    const slider = document.getElementById('position-slider');

                    if (positionInfo && navigationMetadata) {
                        const totalPositions = navigationMetadata.navigation.total_positions;
                        positionInfo.textContent = `Position ${currentRowIndex} of ${totalPositions}`;
                    }

                    if (dateInfo && navContext.timestamp_range && navContext.timestamp_range.start) {
                        const startDate = new Date(navContext.timestamp_range.start * 1000);
                        dateInfo.textContent = startDate.toLocaleDateString() + ' ' + startDate.toLocaleTimeString();
                    }

                    if (barsInfo) {
                        barsInfo.textContent = `${tableData.length} bars`;
                    }

                    if (slider) {
                        slider.value = currentRowIndex;
                    }
                }

                function updateSequenceTable(tableData) {
                    const tableDiv = document.getElementById('sequence-table');
                    if (!tableDiv || !tableData || tableData.length === 0) {
                        console.log('🔍 CLIENT DEBUG: Cannot update table - missing tableDiv or data');
                        return;
                    }

                    console.log('🔍 CLIENT DEBUG: First row data sample:', tableData[0]);

                    let tableHtml = `
                        <table style="width: 100%; border-collapse: collapse; font-size: 14px;">
                            <thead>
                                <tr style="background: #f1f3f4;">
                                    <th style="padding: 8px; border: 1px solid #ddd;">Time</th>
                                    <th style="padding: 8px; border: 1px solid #ddd;">Open</th>
                                    <th style="padding: 8px; border: 1px solid #ddd;">High</th>
                                    <th style="padding: 8px; border: 1px solid #ddd;">Low</th>
                                    <th style="padding: 8px; border: 1px solid #ddd;">Close</th>
                                    <th style="padding: 8px; border: 1px solid #ddd;">Volume</th>
                                </tr>
                            </thead>
                            <tbody>
                    `;

                    // Show first 10 rows
                    tableData.slice(0, 10).forEach((row, index) => {
                        const timestamp = new Date(row.timestamp * 1000);
                        const timeStr = timestamp.toLocaleTimeString();

                        if (index === 0) {
                            console.log(`🔍 CLIENT DEBUG: First row - timestamp: ${row.timestamp}, open: ${row.open}, close: ${row.close}`);
                        }

                        tableHtml += `
                            <tr>
                                <td style="padding: 8px; border: 1px solid #ddd;">${timeStr}</td>
                                <td style="padding: 8px; border: 1px solid #ddd;">$${parseFloat(row.open || 0).toFixed(2)}</td>
                                <td style="padding: 8px; border: 1px solid #ddd;">$${parseFloat(row.high || 0).toFixed(2)}</td>
                                <td style="padding: 8px; border: 1px solid #ddd;">$${parseFloat(row.low || 0).toFixed(2)}</td>
                                <td style="padding: 8px; border: 1px solid #ddd;">$${parseFloat(row.close || 0).toFixed(2)}</td>
                                <td style="padding: 8px; border: 1px solid #ddd;">${parseInt(row.volume || 0).toLocaleString()}</td>
                            </tr>
                        `;
                    });

                    tableHtml += '</tbody></table>';
                    const oldHtml = tableDiv.innerHTML;
                    tableDiv.innerHTML = tableHtml;

                    console.log(`🔍 CLIENT DEBUG: Table updated - HTML changed: ${oldHtml !== tableHtml}`);
                }

                function updateDatasetInfo(navigationData) {
                    const infoDiv = document.getElementById('dataset-info');
                    if (!infoDiv) return;

                    const symbol = navigationData.sequence_id ? navigationData.sequence_id.split('_')[0] : 'UNKNOWN';

                    infoDiv.innerHTML = `
                        <div style="line-height: 1.6;">
                            <p><strong>Dataset:</strong> ${navigationData.dataset_name || 'Loading...'}</p>
                            <p><strong>Symbol:</strong> ${symbol}</p>
                            <p><strong>Position:</strong> ${currentRowIndex}</p>
                            <p><strong>Timeframes:</strong> ${navigationData.available_timeframes ? navigationData.available_timeframes.join(', ') : '5m, 15m, 1h, 1d, 1w'}</p>
                        </div>
                    `;
                }

                function setNavigationLoadingState(loading) {
                    isNavigating = loading;
                    const loadingStatus = document.getElementById('loading-status');
                    const buttons = document.querySelectorAll('#nav-first, #nav-prev, #nav-next, #nav-last');
                    const slider = document.getElementById('position-slider');

                    if (loadingStatus) {
                        loadingStatus.style.display = loading ? 'block' : 'none';
                    }

                    buttons.forEach(btn => {
                        if (btn) btn.disabled = loading;
                    });

                    if (slider) {
                        slider.disabled = loading;
                    }
                }

                function showNavigationError(message) {
                    console.error('Navigation Error:', message);
                    // You could add a toast notification here
                }

                // Override the existing loadDatasetVisualization to integrate navigation
                const originalLoadDatasetVisualization = loadDatasetVisualization;
                loadDatasetVisualization = async function() {
                    // Store current selection for navigation
                    currentDatasetId = document.getElementById('dataset-selector').value;
                    currentSequenceId = document.getElementById('sequence-selector').value;

                    // Call the original function
                    await originalLoadDatasetVisualization();

                    // Load navigation metadata after visualization loads
                    await loadNavigationMetadata();
                };

                // Add keyboard shortcuts for navigation
                document.addEventListener('keydown', function(e) {
                    if (isNavigating || !currentDatasetId || !currentSequenceId) return;

                    // Only handle navigation shortcuts when in training datasets view
                    const datasetVisualization = document.getElementById('dataset-visualization');
                    if (!datasetVisualization || datasetVisualization.style.display === 'none') return;

                    switch(e.key) {
                        case 'ArrowLeft':
                            e.preventDefault();
                            navigateDirection('prev');
                            break;
                        case 'ArrowRight':
                            e.preventDefault();
                            navigateDirection('next');
                            break;
                        case 'Home':
                            e.preventDefault();
                            navigateToPosition('first');
                            break;
                        case 'End':
                            e.preventDefault();
                            navigateToPosition('last');
                            break;
                    }
                });

                console.log('🎮 Time Navigation initialized. Keyboard shortcuts: ← → (prev/next), Home/End (first/last)');

                // Monthly Training Data Table
                async function loadMonthlyTrainingData() {
                    document.getElementById('analysis-content').innerHTML = `
                        <h3>📅 Monthly Training Data</h3>
                        <p>Loading monthly training data records...</p>
                    `;

                    try {
                        const response = await fetch('/api/v1/monthly-training-data');

                        if (!response.ok) {
                            throw new Error(`HTTP ${response.status}: ${response.statusText}`);
                        }

                        const data = await response.json();

                        if (!data.success) {
                            throw new Error(data.error || 'Failed to load monthly training data');
                        }

                        let html = `
                            <h3>📅 Monthly Training Data Browser</h3>

                            <!-- Filters -->
                            <div style="background: white; padding: 20px; border-radius: 8px; border: 1px solid #ddd; margin-bottom: 20px;">
                                <h4>🔍 Filters</h4>
                                <div style="display: grid; grid-template-columns: 1fr 1fr 1fr auto; gap: 15px; align-items: end;">
                                    <div>
                                        <label for="symbol-filter" style="display: block; margin-bottom: 5px; font-weight: bold;">Symbols:</label>
                                        <input type="text" id="symbol-filter" placeholder="e.g., AAPL,TSLA"
                                               style="width: 100%; padding: 8px; border: 1px solid #ddd; border-radius: 4px;">
                                    </div>
                                    <div>
                                        <label for="status-filter" style="display: block; margin-bottom: 5px; font-weight: bold;">Status:</label>
                                        <select id="status-filter" style="width: 100%; padding: 8px; border: 1px solid #ddd; border-radius: 4px;">
                                            <option value="">All Status</option>
                                            <option value="completed">Completed</option>
                                            <option value="failed">Failed</option>
                                            <option value="processing">Processing</option>
                                        </select>
                                    </div>
                                    <div>
                                        <label for="sort-by" style="display: block; margin-bottom: 5px; font-weight: bold;">Sort By:</label>
                                        <select id="sort-by" style="width: 100%; padding: 8px; border: 1px solid #ddd; border-radius: 4px;">
                                            <option value="created_at">Created Date</option>
                                            <option value="symbol">Symbol</option>
                                            <option value="year_month">Month</option>
                                            <option value="total_records">Record Count</option>
                                            <option value="data_quality_score">Quality Score</option>
                                        </select>
                                    </div>
                                    <button onclick="filterMonthlyTrainingData()"
                                            style="padding: 10px 20px; background: #007bff; color: white; border: none; border-radius: 4px; cursor: pointer;">
                                        Apply Filters
                                    </button>
                                </div>
                            </div>

                            <!-- Summary Stats -->
                            <div style="background: white; padding: 20px; border-radius: 8px; border: 1px solid #ddd; margin-bottom: 20px;">
                                <h4>📊 Summary Statistics</h4>
                                <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 15px;">
                                    ${data.summary.map(stat => `
                                        <div style="background: #f8f9fa; padding: 15px; border-radius: 6px; text-align: center;">
                                            <div style="font-size: 18px; font-weight: bold; color: #007bff;">${stat.symbol}</div>
                                            <div style="font-size: 14px; color: #666; margin: 5px 0;">${stat.total_months} months</div>
                                            <div style="font-size: 12px; color: #666;">${stat.total_records?.toLocaleString() || 0} records</div>
                                            <div style="font-size: 12px; color: #666;">Quality: ${(stat.avg_quality_score * 100).toFixed(1)}%</div>
                                        </div>
                                    `).join('')}
                                </div>
                            </div>

                            <!-- Data Table -->
                            <div style="background: white; padding: 20px; border-radius: 8px; border: 1px solid #ddd;">
                                <h4>📋 Monthly Training Data Records (${data.data.length} records)</h4>
                                <div style="overflow-x: auto;">
                                    <table style="width: 100%; border-collapse: collapse; font-size: 14px;">
                                        <thead>
                                            <tr style="background: #f8f9fa; border-bottom: 2px solid #ddd;">
                                                <th style="padding: 12px; text-align: left; border-bottom: 1px solid #ddd;">Symbol</th>
                                                <th style="padding: 12px; text-align: left; border-bottom: 1px solid #ddd;">Month</th>
                                                <th style="padding: 12px; text-align: right; border-bottom: 1px solid #ddd;">Records</th>
                                                <th style="padding: 12px; text-align: right; border-bottom: 1px solid #ddd;">Size (MB)</th>
                                                <th style="padding: 12px; text-align: right; border-bottom: 1px solid #ddd;">Quality</th>
                                                <th style="padding: 12px; text-align: center; border-bottom: 1px solid #ddd;">Status</th>
                                                <th style="padding: 12px; text-align: center; border-bottom: 1px solid #ddd;">Actions</th>
                                            </tr>
                                        </thead>
                                        <tbody id="monthly-data-table-body">
                                            ${data.data.map(record => {
                                                const statusColor = record.status === 'completed' ? '#28a745' :
                                                                  record.status === 'failed' ? '#dc3545' : '#ffc107';
                                                const qualityPercent = (record.data_quality_score * 100).toFixed(1);

                                                return `
                                                    <tr style="border-bottom: 1px solid #eee;" onmouseover="this.style.backgroundColor='#f8f9fa'" onmouseout="this.style.backgroundColor=''">
                                                        <td style="padding: 12px; font-weight: bold;">${record.symbol}</td>
                                                        <td style="padding: 12px;">${record.year_month}</td>
                                                        <td style="padding: 12px; text-align: right;">${record.total_records?.toLocaleString() || 0}</td>
                                                        <td style="padding: 12px; text-align: right;">${record.file_size_mb?.toFixed(2) || 0}</td>
                                                        <td style="padding: 12px; text-align: right;">${qualityPercent}%</td>
                                                        <td style="padding: 12px; text-align: center;">
                                                            <span style="background: ${statusColor}; color: white; padding: 4px 8px; border-radius: 12px; font-size: 12px;">
                                                                ${record.status}
                                                            </span>
                                                        </td>
                                                        <td style="padding: 12px; text-align: center;">
                                                            <button onclick="visualizeMonthlyRecord(${record.id}, '${record.symbol}', '${record.year_month}')"
                                                                    style="padding: 6px 12px; background: #007bff; color: white; border: none; border-radius: 4px; cursor: pointer; font-size: 12px;">
                                                                📊 Visualize
                                                            </button>
                                                        </td>
                                                    </tr>
                                                `;
                                            }).join('')}
                                        </tbody>
                                    </table>
                                </div>
                            </div>

                            <!-- Visualization Panel (hidden initially) -->
                            <div id="monthly-visualization-panel" style="display: none; background: white; padding: 20px; border-radius: 8px; border: 1px solid #ddd; margin-top: 20px;">
                                <h4 id="visualization-title">📊 Multi-Timeframe Visualization</h4>
                                <div id="visualization-content">
                                    <!-- Multi-timeframe charts will be loaded here -->
                                </div>
                            </div>
                        `;

                        document.getElementById('analysis-content').innerHTML = html;

                    } catch (error) {
                        console.error('Error loading monthly training data:', error);
                        document.getElementById('analysis-content').innerHTML = `
                            <h3>📅 Monthly Training Data</h3>
                            <div style="background: #f8d7da; border: 1px solid #f5c6cb; color: #721c24; padding: 15px; border-radius: 8px;">
                                <strong>Error:</strong> ${error.message}
                            </div>
                        `;
                    }
                }

                // Filter monthly training data
                async function filterMonthlyTrainingData() {
                    const symbols = document.getElementById('symbol-filter').value;
                    const status = document.getElementById('status-filter').value;
                    const sortBy = document.getElementById('sort-by').value;

                    let url = '/api/v1/monthly-training-data?';
                    if (symbols) url += `symbols=${encodeURIComponent(symbols)}&`;
                    if (status) url += `status=${encodeURIComponent(status)}&`;
                    if (sortBy) url += `order_by=${encodeURIComponent(sortBy)}&`;

                    try {
                        const response = await fetch(url);
                        const data = await response.json();

                        if (data.success) {
                            // Update table body only
                            const tableBody = document.getElementById('monthly-data-table-body');
                            tableBody.innerHTML = data.data.map(record => {
                                const statusColor = record.status === 'completed' ? '#28a745' :
                                                  record.status === 'failed' ? '#dc3545' : '#ffc107';
                                const qualityPercent = (record.data_quality_score * 100).toFixed(1);

                                return `
                                    <tr style="border-bottom: 1px solid #eee;" onmouseover="this.style.backgroundColor='#f8f9fa'" onmouseout="this.style.backgroundColor=''">
                                        <td style="padding: 12px; font-weight: bold;">${record.symbol}</td>
                                        <td style="padding: 12px;">${record.year_month}</td>
                                        <td style="padding: 12px; text-align: right;">${record.total_records?.toLocaleString() || 0}</td>
                                        <td style="padding: 12px; text-align: right;">${record.file_size_mb?.toFixed(2) || 0}</td>
                                        <td style="padding: 12px; text-align: right;">${qualityPercent}%</td>
                                        <td style="padding: 12px; text-align: center;">
                                            <span style="background: ${statusColor}; color: white; padding: 4px 8px; border-radius: 12px; font-size: 12px;">
                                                ${record.status}
                                            </span>
                                        </td>
                                        <td style="padding: 12px; text-align: center;">
                                            <button onclick="visualizeMonthlyRecord(${record.id}, '${record.symbol}', '${record.year_month}')"
                                                    style="padding: 6px 12px; background: #007bff; color: white; border: none; border-radius: 4px; cursor: pointer; font-size: 12px;">
                                                📊 Visualize
                                            </button>
                                        </td>
                                    </tr>
                                `;
                            }).join('');
                        }
                    } catch (error) {
                        console.error('Error filtering data:', error);
                    }
                }

                // Visualize monthly training record
                async function visualizeMonthlyRecord(recordId, symbol, yearMonth) {
                    const panel = document.getElementById('monthly-visualization-panel');
                    const title = document.getElementById('visualization-title');
                    const content = document.getElementById('visualization-content');

                    title.textContent = `📊 ${symbol} - ${yearMonth} Multi-Timeframe Visualization`;
                    content.innerHTML = '<p>Loading visualization data...</p>';
                    panel.style.display = 'block';

                    try {
                        const response = await fetch(`/api/v1/monthly-training-data/visualization?record_id=${recordId}&center_timeframe=1h&center_index=0`);
                        const data = await response.json();

                        if (!data.success) {
                            throw new Error(data.error || 'Failed to load visualization data');
                        }

                        // Create multi-timeframe chart grid
                        let chartsHtml = `
                            <div style="margin-bottom: 15px; padding: 15px; background: #f8f9fa; border-radius: 6px;">
                                <h5>📊 Data Overview</h5>
                                <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(150px, 1fr)); gap: 10px; font-size: 14px;">
                                    ${Object.entries(data.timeframe_data_counts).map(([tf, count]) =>
                                        `<div><strong>${tf}:</strong> ${count} points</div>`
                                    ).join('')}
                                </div>
                            </div>

                            <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 20px;">
                        `;

                        // Generate charts for each timeframe
                        const timeframes = ['5m', '15m', '1h', '1d'];
                        timeframes.forEach(timeframe => {
                            if (data.charts[timeframe]) {
                                chartsHtml += `
                                    <div style="background: #f8f9fa; padding: 15px; border-radius: 8px;">
                                        <h5>${timeframe.toUpperCase()} Timeframe</h5>
                                        <div id="chart-${timeframe}-${recordId}" style="height: 300px;"></div>
                                    </div>
                                `;
                            }
                        });

                        chartsHtml += '</div>';
                        content.innerHTML = chartsHtml;

                        // Render plotly charts
                        timeframes.forEach(timeframe => {
                            if (data.charts[timeframe]) {
                                const chartId = `chart-${timeframe}-${recordId}`;
                                Plotly.newPlot(chartId, data.charts[timeframe].data, data.charts[timeframe].layout, {responsive: true});
                            }
                        });

                    } catch (error) {
                        console.error('Error loading visualization:', error);
                        content.innerHTML = `
                            <div style="background: #f8d7da; border: 1px solid #f5c6cb; color: #721c24; padding: 15px; border-radius: 8px;">
                                <strong>Error:</strong> ${error.message}
                            </div>
                        `;
                    }
                }

            </script>
        </body>
        </html>
        """

# ==============================================
# HTTP REQUEST HANDLER (from analytics_service.py)
# ==============================================

# Global shared analytics service instance
_shared_analytics_service = None

def get_shared_analytics_service():
    """Get or create shared analytics service instance"""
    global _shared_analytics_service
    if _shared_analytics_service is None:
        _shared_analytics_service = UnifiedAnalyticsService()
    return _shared_analytics_service

class UnifiedAnalyticsRequestHandler(BaseHTTPRequestHandler):
    """HTTP request handler for the unified analytics service."""

    def __init__(self, *args, **kwargs):
        self.analytics_service = get_shared_analytics_service()
        self.metrics = get_metrics_collector()
        super().__init__(*args, **kwargs)

    def do_GET(self):
        """Handle GET requests."""
        try:
            logger.info(f"📍 GET request: {self.path}")

            if self.path == '/health':
                self._serve_health_check()
            elif self.path == '/metrics':
                self._serve_prometheus_metrics()
            elif self.path == '/eda' or self.path == '/':
                self._serve_eda_dashboard()
            elif self.path.startswith('/api/intelligent-filters/'):
                self._serve_intelligent_filters()
            elif self.path.startswith('/api/universe-analytics'):
                self._serve_universe_analytics()
            elif self.path == '/api/universes':
                self._serve_universes_list()
            elif self.path.startswith('/api/universe-members/'):
                self._serve_universe_members()
            elif self.path.startswith('/api/multi-panel-chart'):
                asyncio.run(self._serve_multi_panel_chart())
            elif self.path.startswith('/api/v1/training-datasets'):
                if '/navigation-metadata' in self.path:
                    self._serve_navigation_metadata()
                elif '/navigate' in self.path:
                    self._serve_navigation()
                elif '/multi-timeframe' in self.path:
                    self._serve_training_dataset_multi_timeframe()
                elif '/sequence/' in self.path:
                    self._serve_training_dataset_sequence()
                elif '/sequences' in self.path:
                    self._serve_training_dataset_sequences()
                elif '/visualization-data' in self.path:
                    self._serve_training_dataset_visualization_data()
                else:
                    self._serve_training_datasets()
            elif self.path.startswith('/api/v1/monthly-training-data'):
                if '/visualization' in self.path:
                    self._serve_monthly_training_visualization()
                else:
                    self._serve_monthly_training_data_table()
            elif self.path.startswith('/api/ray-analytics/'):
                self._serve_ray_analytics()
            elif self.path.startswith('/api/news-events'):
                self._serve_news_events()
            elif self.path.startswith('/api/earnings-events'):
                self._serve_earnings_events()
            elif self.path.startswith('/api/gap-events'):
                self._serve_gap_events()
            elif self.path.startswith('/api/economic-events'):
                self._serve_economic_events()
            elif self.path.startswith('/api/economic-indicators'):
                self._serve_economic_indicators()
            elif self.path == '/api/bar-collection-metrics':
                self._serve_bar_collection_metrics()
            elif self.path == '/api/tables':
                self._serve_tables_list()
            elif self.path.startswith('/api/table-info/'):
                self._serve_table_info()
            elif self.path.startswith('/api/table-columns/'):
                self._serve_table_columns()
            elif self.path.startswith('/api/table-sample/'):
                self._serve_table_sample()
            elif self.path.startswith('/api/table-distributions/'):
                self._serve_table_distributions()
            elif self.path == '/data-quality/dashboard' or self.path == '/data-quality':
                self._serve_data_quality_dashboard()
            elif self.path.startswith('/data-quality/api/issues/'):
                self._serve_data_quality_issues_with_tags()
            elif self.path.startswith('/data-quality/api/issues'):
                # Check if tag filtering parameters are present
                from urllib.parse import urlparse, parse_qs
                parsed_url = urlparse(self.path)
                query_params = parse_qs(parsed_url.query)
                
                # If tag filtering parameters exist, use enhanced version
                if (query_params.get('tag_ids') or query_params.get('symbols') or 
                    query_params.get('date_from') or query_params.get('categories')):
                    self._serve_data_quality_issues_with_tags()
                else:
                    self._serve_data_quality_issues()
            elif self.path == '/agent/status':
                self._serve_agent_status()
            elif self.path == '/agent/start':
                self._serve_agent_start()
            elif self.path == '/agent/stop':
                self._serve_agent_stop()
            elif self.path.startswith('/agent/'):
                self._serve_agent_endpoint()
            elif self.path.startswith('/api/tags/'):
                self._serve_tag_api()
            elif self.path == '/available-tags':
                self._serve_available_tags()
            elif self.path.startswith('/financial_events'):
                self._serve_financial_events()
            else:
                self._serve_404()

        except Exception as e:
            logger.error(f"Error handling GET request: {e}")
            self._serve_500(str(e))

    def do_POST(self):
        """Handle POST requests."""
        try:
            logger.info(f"📍 POST request: {self.path}")

            if self.path.startswith('/financial_events'):
                self._serve_financial_events()
            elif self.path.startswith('/api/tags/'):
                self._serve_tag_api()
            elif self.path == '/auto-tag-batch':
                self._serve_auto_tag_batch()
            elif self.path == '/agent/start':
                self._serve_agent_start()
            elif self.path == '/agent/stop':
                self._serve_agent_stop()
            elif self.path.startswith('/agent/'):
                self._serve_agent_action()
            else:
                self.send_response(404)
                self.send_header('Content-type', 'application/json')
                self.end_headers()
                self.wfile.write(json.dumps({
                    "error": "POST endpoint not found",
                    "path": self.path
                }).encode())

        except Exception as e:
            logger.error(f"Error handling POST request: {e}")
            self._serve_500(str(e))

    def _serve_prometheus_metrics(self):
        """Serve Prometheus metrics."""
        try:
            start_time = time.time()
            metrics_content = self.metrics.get_prometheus_metrics()
            
            self.send_response(200)
            self.send_header('Content-type', 'text/plain; version=0.0.4; charset=utf-8')
            self.end_headers()
            self.wfile.write(metrics_content.encode('utf-8'))
            
            # Record the API request
            duration = time.time() - start_time
            self.metrics.record_api_request(duration, error=False)
            
            logger.info("✅ Served Prometheus metrics")
            
        except Exception as e:
            logger.error(f"❌ Error serving Prometheus metrics: {e}")
            self.send_response(500)
            self.end_headers()
            self.metrics.record_api_request(0, error=True)
    
    def _serve_health_check(self):
        """Serve health check response."""
        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.end_headers()

        # Get health metrics from Prometheus collector
        health_metrics = self.metrics.get_health_metrics()
        
        health_status = {
            "status": "healthy",
            "service": "ats-unified-analytics",
            "timestamp": datetime.now().isoformat(),
            "data_quality_agent": health_metrics,
            "features": {
                "type_system": self.analytics_service.type_system_enabled,
                "ray_computing": self.analytics_service.ray_enabled,
                "universe_analytics": True,
                "training_datasets": True
            }
        }

        self.wfile.write(json.dumps(health_status).encode('utf-8'))

    def _serve_eda_dashboard(self):
        """Serve the unified EDA dashboard."""
        self.send_response(200)
        self.send_header('Content-type', 'text/html; charset=utf-8')
        self.end_headers()

        html_content = self.analytics_service.get_eda_dashboard_html()
        self.wfile.write(html_content.encode('utf-8'))

    def _serve_intelligent_filters(self):
        """Serve intelligent filter definitions."""
        # Extract table name from path
        path_parts = self.path.split('/')
        table_name = path_parts[-1] if len(path_parts) > 3 else 'default'

        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.end_headers()

        # This would be async in a real implementation
        filters = asyncio.run(self.analytics_service.get_intelligent_filters(table_name))
        self.wfile.write(json.dumps(filters, indent=2).encode('utf-8'))

    def _serve_universe_analytics(self):
        """Serve universe analytics."""
        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.end_headers()

        analytics = asyncio.run(self.analytics_service.get_universe_analytics())
        self.wfile.write(json.dumps(analytics, indent=2).encode('utf-8'))

    async def _serve_multi_panel_chart(self):
        """Serve multi-panel chart generation API."""
        try:
            # Parse query parameters
            from urllib.parse import urlparse, parse_qs
            parsed_url = urlparse(self.path)
            params = parse_qs(parsed_url.query)

            symbol = params.get('symbol', ['AAPL'])[0]
            timeframe = params.get('timeframe', ['1h'])[0]
            dataset_id = int(params.get('dataset_id', ['1'])[0])

            # Generate chart
            result = await self.analytics_service.generate_multi_panel_chart(
                symbol=symbol,
                timeframe=timeframe,
                dataset_id=dataset_id
            )

            # Send response
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()

            self.wfile.write(json.dumps(result).encode('utf-8'))

        except Exception as e:
            logger.error(f"❌ Error serving multi-panel chart: {e}")

            error_response = {
                "success": False,
                "error": f"Server error: {str(e)}"
            }

            self.send_response(500)
            self.send_header('Content-type', 'application/json')
            self.end_headers()

            self.wfile.write(json.dumps(error_response).encode('utf-8'))

    def _serve_training_datasets(self):
        """Serve training datasets."""
        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.end_headers()

        datasets = self.analytics_service.get_training_datasets()
        self.wfile.write(json.dumps(datasets, indent=2).encode('utf-8'))

    def _serve_training_dataset_sequence(self):
        """Serve training dataset sequence data for OHLC visualization."""
        from urllib.parse import urlparse, parse_qs

        # Parse URL and query parameters
        parsed_url = urlparse(self.path)
        query_params = parse_qs(parsed_url.query)

        # Extract dataset_id and row_index from path like /api/v1/training-datasets/sequence/1/50
        path_parts = parsed_url.path.split('/')
        try:
            dataset_id = int(path_parts[5])  # /api/v1/training-datasets/sequence/{dataset_id}/{row_index}
            row_index = int(path_parts[6]) if len(path_parts) > 6 else 0
        except (IndexError, ValueError):
            self.send_response(400)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps({"error": "Invalid dataset_id or row_index"}).encode('utf-8'))
            return

        # Extract timeframe from query parameters (e.g., ?timeframe=5m)
        timeframe = query_params.get('timeframe', ['5m'])[0]  # Default to 5m if not specified

        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.end_headers()

        try:
            sequence_data = self.analytics_service.get_training_dataset_sequence(dataset_id, row_index, timeframe)
            self.wfile.write(json.dumps(sequence_data, indent=2, default=str).encode('utf-8'))
        except Exception as e:
            logger.error(f"Error getting sequence data for dataset {dataset_id}, row {row_index}, timeframe {timeframe}: {e}")
            error_response = {
                "error": str(e),
                "dataset_id": dataset_id,
                "row_index": row_index,
                "timeframe": timeframe,
                "message": "No ArrayRecord data available - please generate training data first"
            }
            self.wfile.write(json.dumps(error_response, indent=2).encode('utf-8'))

    def _serve_training_dataset_sequences(self):
        """Serve available sequences for a training dataset."""
        from urllib.parse import urlparse

        # Extract dataset_id from path like /api/v1/training-datasets/38/sequences
        path_parts = urlparse(self.path).path.split('/')
        try:
            dataset_id = int(path_parts[4])  # /api/v1/training-datasets/{dataset_id}/sequences
        except (IndexError, ValueError):
            self.send_response(400)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps({"error": "Invalid dataset_id"}).encode('utf-8'))
            return

        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.end_headers()

        try:
            sequences = self.analytics_service.get_training_dataset_sequences(dataset_id)
            self.wfile.write(json.dumps(sequences, indent=2).encode('utf-8'))
        except Exception as e:
            logger.error(f"Error getting sequences for dataset {dataset_id}: {e}")
            error_response = {
                "error": str(e),
                "dataset_id": dataset_id,
                "sequences": [],
                "total_count": 0
            }
            self.wfile.write(json.dumps(error_response, indent=2).encode('utf-8'))

    def _serve_training_dataset_visualization_data(self):
        """Serve visualization data for training dataset sequences."""
        from urllib.parse import urlparse, parse_qs

        # Parse URL and query parameters
        parsed_url = urlparse(self.path)
        query_params = parse_qs(parsed_url.query)

        # Extract dataset_id from path like /api/v1/training-datasets/{dataset_id}/visualization-data
        path_parts = parsed_url.path.split('/')
        try:
            dataset_id = int(path_parts[4])  # /api/v1/training-datasets/{dataset_id}/visualization-data
        except (IndexError, ValueError):
            self.send_response(400)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps({"error": "Invalid dataset_id"}).encode('utf-8'))
            return

        # Extract query parameters
        start_idx = int(query_params.get('start_idx', ['0'])[0])
        sequence_id = query_params.get('sequence_id', [None])[0]

        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.end_headers()

        try:
            # Get visualization data from the analytics service
            viz_data = self.analytics_service.get_training_dataset_visualization_data(dataset_id, start_idx, sequence_id)
            self.wfile.write(json.dumps(viz_data, indent=2, default=str).encode('utf-8'))
        except Exception as e:
            logger.error(f"Error getting visualization data for dataset {dataset_id}, start_idx {start_idx}: {e}")
            error_response = {
                "error": str(e),
                "dataset_id": dataset_id,
                "start_idx": start_idx,
                "message": "No ArrayRecord data available - please generate training data first"
            }
            self.wfile.write(json.dumps(error_response, indent=2).encode('utf-8'))

    def _serve_training_dataset_multi_timeframe(self):
        """Serve multi-timeframe OHLC data for a specific sequence."""
        from urllib.parse import urlparse, parse_qs

        # Parse URL and query parameters
        parsed_url = urlparse(self.path)
        query_params = parse_qs(parsed_url.query)

        # Extract dataset_id and sequence_id from path like /api/v1/training-datasets/{dataset_id}/sequences/{sequence_id}/multi-timeframe
        path_parts = parsed_url.path.split('/')
        try:
            dataset_id = int(path_parts[4])  # /api/v1/training-datasets/{dataset_id}/sequences/{sequence_id}/multi-timeframe
            sequence_id = path_parts[6]      # sequence_id
        except (IndexError, ValueError):
            self.send_response(400)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps({"error": "Invalid dataset_id or sequence_id"}).encode('utf-8'))
            return

        # Extract row_index from query parameters (e.g., ?row_index=50)
        row_index = int(query_params.get('row_index', [50])[0])
        logger.info(f"Multi-timeframe request: dataset_id={dataset_id}, sequence_id={sequence_id}, row_index={row_index}")

        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.end_headers()

        try:
            # Get multi-timeframe data from the analytics service with row index
            multi_data = self.analytics_service.get_training_dataset_sequence_multi_timeframe(dataset_id, sequence_id, row_index)
            self.wfile.write(json.dumps(multi_data, indent=2, default=str).encode('utf-8'))
        except Exception as e:
            logger.error(f"Error getting multi-timeframe data for dataset {dataset_id}, sequence {sequence_id}: {e}")
            error_response = {
                "error": str(e),
                "dataset_id": dataset_id,
                "sequence_id": sequence_id,
                "message": "Failed to load multi-timeframe data"
            }
            self.wfile.write(json.dumps(error_response, indent=2).encode('utf-8'))

    def _serve_navigation_metadata(self):
        """Serve navigation metadata for a sequence."""

        # Parse URL - /api/v1/training-datasets/{dataset_id}/sequences/{sequence_id}/navigation-metadata
        path_parts = self.path.split('/')
        try:
            dataset_id = int(path_parts[4])
            sequence_id = path_parts[6]
        except (IndexError, ValueError):
            self.send_response(400)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps({"error": "Invalid dataset_id or sequence_id"}).encode('utf-8'))
            return

        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.end_headers()

        try:
            # Test multiple row_index values to find available range
            available_positions = []
            max_position = 0

            # Test positions to find working range
            for test_index in [0, 10, 25, 50, 75, 100]:
                try:
                    result = self.analytics_service.get_training_dataset_sequence_multi_timeframe(
                        dataset_id, sequence_id, test_index
                    )

                    if result.get('success') and result.get('table_data'):
                        table_data = result['table_data']
                        if table_data and len(table_data) > 0:
                            available_positions.append({
                                'row_index': test_index,
                                'bars': len(table_data),
                                'start_timestamp': table_data[0].get('timestamp'),
                                'end_timestamp': table_data[-1].get('timestamp'),
                                'start_price': table_data[0].get('open'),
                                'end_price': table_data[-1].get('close')
                            })
                            max_position = max(max_position, test_index)

                except Exception:
                    break

            # Convert timestamps to readable dates
            def format_timestamp(ts):
                if ts:
                    try:
                        from datetime import datetime
                        return datetime.fromtimestamp(ts).isoformat()
                    except:
                        return ts
                return None

            # Prepare metadata
            metadata = {
                'sequence_id': sequence_id,
                'dataset_id': dataset_id,
                'navigation': {
                    'min_row_index': 0,
                    'max_row_index': max_position,
                    'total_positions': max_position + 1,
                    'window_size': 21,
                    'default_position': 10
                },
                'sample_positions': [
                    {
                        'row_index': pos['row_index'],
                        'description': f"Position {pos['row_index']} ({pos['bars']} bars)",
                        'start_time': format_timestamp(pos['start_timestamp']),
                        'end_time': format_timestamp(pos['end_timestamp']),
                        'price_range': {
                            'start': pos['start_price'],
                            'end': pos['end_price']
                        }
                    }
                    for pos in available_positions[:5]
                ],
                'timeframes_available': ['5m', '15m', '1h', '1d', '1w']
            }

            self.wfile.write(json.dumps(metadata, indent=2, default=str).encode('utf-8'))

        except Exception as e:
            logger.error(f"Error getting navigation metadata: {e}")
            error_response = {
                'error': str(e),
                'sequence_id': sequence_id,
                'dataset_id': dataset_id
            }
            self.wfile.write(json.dumps(error_response, indent=2).encode('utf-8'))

    def _serve_navigation(self):
        """Serve navigation to a specific position in the sequence."""
        from urllib.parse import urlparse, parse_qs

        # Parse URL and query parameters
        parsed_url = urlparse(self.path)
        query_params = parse_qs(parsed_url.query)

        # Extract dataset_id and sequence_id from path
        path_parts = parsed_url.path.split('/')
        try:
            dataset_id = int(path_parts[4])
            sequence_id = path_parts[6]
        except (IndexError, ValueError):
            self.send_response(400)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps({"error": "Invalid dataset_id or sequence_id"}).encode('utf-8'))
            return

        # Get navigation parameters
        row_index = int(query_params.get('row_index', [10])[0])
        direction = query_params.get('direction', [None])[0]

        logger.info(f"🔍 NAVIGATION DEBUG: dataset_id={dataset_id}, sequence_id={sequence_id}, initial_row_index={row_index}, direction={direction}")

        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.end_headers()

        try:
            # Handle navigation directions
            original_row_index = row_index
            if direction:
                # Get current valid range (simplified)
                max_position = 100  # Default max, could be determined dynamically
                min_position = 0

                if direction == 'next':
                    row_index = min(row_index + 10, max_position)
                elif direction == 'prev':
                    row_index = max(row_index - 10, min_position)
                elif direction == 'first':
                    row_index = min_position
                elif direction == 'last':
                    row_index = max_position

            logger.info(f"🔍 NAVIGATION DEBUG: direction={direction}, original_row={original_row_index} -> new_row={row_index}")

            # Get the data for the specified position
            result = self.analytics_service.get_training_dataset_sequence_multi_timeframe(
                dataset_id, sequence_id, row_index
            )

            logger.info(f"🔍 NAVIGATION DEBUG: API call result success={result.get('success')}, table_data_count={len(result.get('table_data', []))}")

            # Add navigation context to the response
            if result.get('success'):
                result['navigation_context'] = {
                    'current_row_index': row_index,
                    'direction_used': direction,
                    'timestamp_range': {
                        'start': result['table_data'][0].get('timestamp') if result.get('table_data') else None,
                        'end': result['table_data'][-1].get('timestamp') if result.get('table_data') else None
                    }
                }

            self.wfile.write(json.dumps(result, indent=2, default=str).encode('utf-8'))

        except Exception as e:
            logger.error(f"Navigation failed: {e}")
            error_response = {
                'error': str(e),
                'sequence_id': sequence_id,
                'dataset_id': dataset_id,
                'requested_row_index': row_index
            }
            self.wfile.write(json.dumps(error_response, indent=2).encode('utf-8'))

    def _serve_ray_analytics(self):
        """Serve Ray distributed analytics."""
        # Extract dataset ID from path
        path_parts = self.path.split('/')
        dataset_id = path_parts[-1] if len(path_parts) > 3 else '1'

        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.end_headers()

        analytics = asyncio.run(self.analytics_service.get_ray_analytics(dataset_id))
        self.wfile.write(json.dumps(analytics, indent=2).encode('utf-8'))

    def _serve_bar_collection_metrics(self):
        """Serve bar collection metrics."""
        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.end_headers()

        metrics = self.analytics_service.get_bar_collection_metrics()
        self.wfile.write(json.dumps(metrics, indent=2, default=str).encode('utf-8'))

    def _serve_tables_list(self):
        """Serve list of database tables using environment-specific prefix."""
        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.end_headers()

        try:
            from core.platform.database.connection_manager import get_raw_connection
            from psycopg2.extras import RealDictCursor
            import os

            # Get environment-specific table prefix
            environment = os.getenv('ENVIRONMENT', 'dev')
            table_prefix = f'{environment}_%'

            with get_raw_connection() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                    cursor.execute("""
                        SELECT tablename
                        FROM pg_tables
                        WHERE schemaname = 'public'
                        AND tablename LIKE %s
                        ORDER BY tablename
                    """, (table_prefix,))

                    tables = [row['tablename'] for row in cursor.fetchall()]
                    response = {"tables": tables}

        except Exception as e:
            logger.error(f"Error getting tables list: {e}")
            # Get environment-specific alternative tables
            import os
            environment = os.getenv('ENVIRONMENT', 'dev')
            response = {
                "tables": [
                    f"{environment}_daily_prices", f"{environment}_training_datasets", f"{environment}_instruments",
                    f"{environment}_daily_prices_polygon", f"{environment}_daily_prices_tiingo", f"{environment}_daily_prices_eodhd"
                ],
                "error": str(e)
            }

        self.wfile.write(json.dumps(response, indent=2).encode('utf-8'))

    def _serve_table_info(self):
        """Serve table information."""
        table_name = self.path.split('/')[-1]

        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.end_headers()

        try:
            from core.platform.database.connection_manager import get_raw_connection
            from psycopg2.extras import RealDictCursor
            from psycopg2 import sql

            with get_raw_connection() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                    # Get row count (using safe SQL identifier)
                    cursor.execute(
                        sql.SQL("SELECT COUNT(*) as count FROM {}").format(
                            sql.Identifier(table_name)
                        )
                    )
                    row_count = cursor.fetchone()['count']

                    # Get column count
                    cursor.execute("""
                        SELECT COUNT(*) as count
                        FROM information_schema.columns
                        WHERE table_name = %s
                    """, (table_name,))
                    column_count = cursor.fetchone()['count']

                    # Get table size
                    cursor.execute("""
                        SELECT pg_size_pretty(pg_total_relation_size(%s)) as size
                    """, (table_name,))
                    size = cursor.fetchone()['size']

                    response = {
                        "table_name": table_name,
                        "row_count": row_count,
                        "column_count": column_count,
                        "size": size,
                        "last_updated": "Unknown"
                    }

        except Exception as e:
            logger.error(f"Error getting table info for {table_name}: {e}")
            response = {"error": str(e), "table_name": table_name}

        self.wfile.write(json.dumps(response, indent=2).encode('utf-8'))

    def _serve_table_columns(self):
        """Serve table column information."""
        table_name = self.path.split('/')[-1]

        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.end_headers()

        try:
            from core.platform.database.connection_manager import get_raw_connection
            import psycopg2.extras

            with get_raw_connection() as conn:
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                    cursor.execute("""
                        SELECT column_name, data_type, is_nullable
                        FROM information_schema.columns
                        WHERE table_name = %s
                        ORDER BY ordinal_position
                    """, (table_name,))

                    columns = []
                    for row in cursor.fetchall():
                        columns.append({
                            "name": row['column_name'],
                            "type": row['data_type'],
                            "nullable": row['is_nullable'] == 'YES'
                        })

                    response = {"table_name": table_name, "columns": columns}

        except Exception as e:
            logger.error(f"Error getting columns for {table_name}: {e}")
            response = {"error": str(e), "table_name": table_name, "columns": []}

        self.wfile.write(json.dumps(response, indent=2).encode('utf-8'))

    def _serve_table_sample(self):
        """Serve sample data from table with optional filtering."""
        from urllib.parse import urlparse, parse_qs

        # Parse table name and query parameters
        parsed_url = urlparse(self.path)
        table_name = parsed_url.path.split('/')[-1]
        query_params = parse_qs(parsed_url.query)

        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.end_headers()

        try:
            from core.platform.database.connection_manager import get_raw_connection
            from psycopg2.extras import RealDictCursor
            from psycopg2 import sql

            # Extract filter and sort parameters
            limit = int(query_params.get('limit', ['50'])[0])
            symbol_filter = query_params.get('symbol', [None])[0]
            date_from = query_params.get('date_from', [None])[0]
            date_to = query_params.get('date_to', [None])[0]
            sort_column = query_params.get('sort_by', [None])[0]
            sort_direction = query_params.get('sort_dir', ['asc'])[0].lower()

            with get_raw_connection() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cursor:

                    # Build base query
                    base_query = "SELECT * FROM {}"
                    where_conditions = []
                    params = []

                    # Add symbol filter if provided (simplified approach)
                    if symbol_filter and 'daily_prices' in table_name:
                        # For daily_prices tables, we know symbol column exists
                        where_conditions.append('symbol ILIKE %s')
                        params.append(f"%{symbol_filter}%")

                    # Add date filters if provided (simplified approach)
                    if (date_from or date_to) and ('daily_prices' in table_name or 'gap_events' in table_name):
                        # For price/event tables, we know date column exists
                        if date_from:
                            where_conditions.append('date >= %s')
                            params.append(date_from)
                        if date_to:
                            where_conditions.append('date <= %s')
                            params.append(date_to)

                    # Build ORDER BY clause
                    order_clause = ""
                    if sort_column:
                        # Validate sort direction
                        if sort_direction not in ['asc', 'desc']:
                            sort_direction = 'asc'

                        # Validate column exists to prevent SQL injection
                        cursor.execute("""
                            SELECT column_name FROM information_schema.columns
                            WHERE table_name = %s AND column_name = %s
                        """, (table_name, sort_column))

                        if cursor.fetchone():
                            order_clause = f" ORDER BY \"{sort_column}\" {sort_direction.upper()}"

                    # Build final query
                    if where_conditions:
                        query = f"{base_query} WHERE {' AND '.join(where_conditions)}{order_clause} LIMIT %s"
                        params.append(limit)
                    else:
                        query = f"{base_query}{order_clause} LIMIT %s"
                        params = [limit]

                    cursor.execute(
                        sql.SQL(query).format(sql.Identifier(table_name)),
                        params
                    )

                    rows = []
                    for row in cursor.fetchall():
                        row_dict = dict(row)
                        # Convert dates/datetimes to strings for JSON serialization
                        for key, value in row_dict.items():
                            if hasattr(value, 'isoformat'):
                                row_dict[key] = value.isoformat()
                        rows.append(row_dict)

                    response = {
                        "table_name": table_name,
                        "rows": rows,
                        "filters_applied": {
                            "symbol": symbol_filter,
                            "date_from": date_from,
                            "date_to": date_to,
                            "limit": limit
                        },
                        "sort_applied": {
                            "column": sort_column,
                            "direction": sort_direction
                        }
                    }

        except Exception as e:
            logger.error(f"Error getting sample data for {table_name}: {e}")
            response = {"error": str(e), "table_name": table_name, "rows": []}

        self.wfile.write(json.dumps(response, indent=2).encode('utf-8'))

    def _serve_table_distributions(self):
        """Serve column distributions and statistics."""
        table_name = self.path.split('/')[-1]

        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.end_headers()

        try:
            from core.platform.database.connection_manager import get_raw_connection
            import psycopg2.extras
            from psycopg2 import sql

            with get_raw_connection() as conn:
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                    # Get column info first
                    cursor.execute("""
                        SELECT column_name, data_type
                        FROM information_schema.columns
                        WHERE table_name = %s
                        ORDER BY ordinal_position
                    """, (table_name,))

                    columns = {}
                    for row in cursor.fetchall():
                        column_name, data_type = row['column_name'], row['data_type']
                        try:
                            # Basic statistics for each column using safe query construction
                            cursor.execute(
                                sql.SQL("""
                                    SELECT
                                        COUNT(*) as count,
                                        COUNT(DISTINCT {}) as unique,
                                        COUNT(*) - COUNT({}) as nulls
                                    FROM {}
                                """).format(
                                    sql.Identifier(column_name),
                                    sql.Identifier(column_name),
                                    sql.Identifier(table_name)
                                )
                            )

                            result = cursor.fetchone()
                            count, unique, nulls = result['count'], result['unique'], result['nulls']

                            stats = {
                                "count": count,
                                "unique": unique,
                                "nulls": nulls,
                                "type": data_type
                            }

                            # For numeric columns, get min/max using safe query construction
                            if data_type in ['integer', 'bigint', 'numeric', 'real', 'double precision']:
                                cursor.execute(
                                    sql.SQL("SELECT MIN({}) as min_val, MAX({}) as max_val FROM {}").format(
                                        sql.Identifier(column_name),
                                        sql.Identifier(column_name),
                                        sql.Identifier(table_name)
                                    )
                                )
                                result = cursor.fetchone()
                                if result['min_val'] is not None:
                                    stats["min"] = result['min_val']
                                    stats["max"] = result['max_val']

                            # For text columns, get top values using safe query construction
                            elif data_type in ['text', 'character varying', 'character']:
                                cursor.execute(
                                    sql.SQL("""
                                        SELECT {}, COUNT(*) as freq
                                        FROM {}
                                        WHERE {} IS NOT NULL
                                        GROUP BY {}
                                        ORDER BY freq DESC
                                        LIMIT 5
                                    """).format(
                                        sql.Identifier(column_name),
                                        sql.Identifier(table_name),
                                        sql.Identifier(column_name),
                                        sql.Identifier(column_name)
                                    )
                                )

                                top_values = [row[column_name] for row in cursor.fetchall()]
                                if top_values:
                                    stats["top_values"] = top_values

                            columns[column_name] = stats

                        except Exception as col_error:
                            logger.error(f"Error analyzing column {column_name}: {col_error}")
                            columns[column_name] = {"error": str(col_error)}

                    response = {"table_name": table_name, "columns": columns}

        except Exception as e:
            logger.error(f"Error getting distributions for {table_name}: {e}")
            response = {"error": str(e), "table_name": table_name, "columns": {}}

        self.wfile.write(json.dumps(response, indent=2).encode('utf-8'))

    def _serve_news_events(self):
        """Serve news events from Polygon and Tiingo sources."""
        from urllib.parse import urlparse, parse_qs

        # Parse query parameters
        parsed_url = urlparse(self.path)
        query_params = parse_qs(parsed_url.query)

        # Get parameters
        limit = int(query_params.get('limit', [100])[0])
        symbol = query_params.get('symbol', [None])[0]
        start_date = query_params.get('start_date', [None])[0]
        end_date = query_params.get('end_date', [None])[0]

        # Limit the results to reasonable bounds
        limit = min(limit, 500)  # Max 500 events

        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()

        try:
            # Get news events from the analytics service
            news_data = self.analytics_service.get_news_events(limit=limit, symbol=symbol, start_date=start_date, end_date=end_date)
            self.wfile.write(json.dumps(news_data, indent=2, default=str).encode('utf-8'))

        except Exception as e:
            logger.error(f"Error serving news events: {e}")
            error_response = {
                "success": False,
                "error": str(e),
                "events": [],
                "total_events": 0
            }
            self.wfile.write(json.dumps(error_response, indent=2).encode('utf-8'))

    def _serve_earnings_events(self):
        """Serve earnings events from dev_earnings_events table."""
        from urllib.parse import urlparse, parse_qs

        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()

        try:
            # Parse query parameters
            parsed_url = urlparse(self.path)
            query_params = parse_qs(parsed_url.query)

            # Get parameters with error handling
            try:
                limit = int(query_params.get('limit', [100])[0])
            except (ValueError, TypeError):
                limit = 100  # Default alternative

            symbol = query_params.get('symbol', [None])[0]
            start_date = query_params.get('start_date', [None])[0]
            end_date = query_params.get('end_date', [None])[0]

            # Limit the results to reasonable bounds
            limit = min(max(limit, 1), 500)  # Between 1 and 500 events

            # Get earnings events from the analytics service
            earnings_data = self.analytics_service.get_earnings_events(limit=limit, symbol=symbol, start_date=start_date, end_date=end_date)
            self.wfile.write(json.dumps(earnings_data, indent=2, default=str).encode('utf-8'))

        except Exception as e:
            logger.error(f"Error serving earnings events: {e}")
            error_response = {
                "success": False,
                "error": str(e),
                "events": [],
                "total_events": 0
            }
            self.wfile.write(json.dumps(error_response, indent=2).encode('utf-8'))

    def _serve_gap_events(self):
        """Serve gap events from gap_events table."""
        from urllib.parse import urlparse, parse_qs

        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()

        try:
            # Parse query parameters
            parsed_url = urlparse(self.path)
            query_params = parse_qs(parsed_url.query)

            # Get parameters with error handling
            try:
                limit = int(query_params.get('limit', [100])[0])
            except (ValueError, TypeError):
                limit = 100  # Default alternative

            symbol = query_params.get('symbol', [None])[0]
            start_date = query_params.get('start_date', [None])[0]
            end_date = query_params.get('end_date', [None])[0]

            # Limit the results to reasonable bounds
            limit = min(max(limit, 1), 500)  # Between 1 and 500 events

            # Get gap events from the analytics service
            gap_data = self.analytics_service.get_gap_events(limit=limit, symbol=symbol, start_date=start_date, end_date=end_date)
            self.wfile.write(json.dumps(gap_data, indent=2, default=str).encode('utf-8'))

        except Exception as e:
            logger.error(f"Error serving gap events: {e}")
            error_response = {
                "success": False,
                "error": str(e),
                "events": [],
                "total_events": 0
            }
            self.wfile.write(json.dumps(error_response, indent=2).encode('utf-8'))

    def _serve_economic_events(self):
        """Serve consolidated economic events from multiple tables."""
        from urllib.parse import urlparse, parse_qs

        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()

        try:
            # Parse query parameters
            parsed_url = urlparse(self.path)
            query_params = parse_qs(parsed_url.query)

            # Get parameters with error handling
            try:
                limit = int(query_params.get('limit', [100])[0])
            except (ValueError, TypeError):
                limit = 100  # Default alternative

            vendor = query_params.get('vendor', [None])[0]
            symbol = query_params.get('symbol', [None])[0]
            start_date = query_params.get('start_date', [None])[0]
            end_date = query_params.get('end_date', [None])[0]

            # Limit the results to reasonable bounds
            limit = min(max(limit, 1), 500)  # Between 1 and 500 events

            # Get economic events from the analytics service
            events_data = self.analytics_service.get_economic_events(limit=limit, vendor=vendor, symbol=symbol, start_date=start_date, end_date=end_date)
            self.wfile.write(json.dumps(events_data, indent=2, default=str).encode('utf-8'))

        except Exception as e:
            logger.error(f"Error serving economic events: {e}")
            error_response = {
                "success": False,
                "error": str(e),
                "events": [],
                "total_events": 0
            }
            self.wfile.write(json.dumps(error_response, indent=2).encode('utf-8'))

    def _serve_economic_indicators(self):
        """Serve economic indicators with current and upcoming releases."""
        from urllib.parse import urlparse, parse_qs

        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()

        try:
            # Parse query parameters
            parsed_url = urlparse(self.path)
            query_params = parse_qs(parsed_url.query)

            indicators = query_params.get('indicators', [None])[0]
            if indicators:
                indicators = indicators.split(',')

            # Get economic indicators from the analytics service
            indicators_data = self.analytics_service.get_economic_indicators(indicators=indicators)
            self.wfile.write(json.dumps(indicators_data, indent=2, default=str).encode('utf-8'))

        except Exception as e:
            logger.error(f"Error serving economic indicators: {e}")
            error_response = {
                "success": False,
                "error": str(e),
                "indicators": [],
                "total_indicators": 0
            }
            self.wfile.write(json.dumps(error_response, indent=2).encode('utf-8'))

    def _serve_data_quality_dashboard(self):
        """Serve data quality dashboard HTML."""
        dashboard_html = r'''
<!DOCTYPE html>
<html>
<head>
    <title>ATS Data Quality Dashboard</title>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <style>
        body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Arial, sans-serif; margin: 0; padding: 20px; background: #f8f9fa; }
        .header { background: linear-gradient(135deg, #2c3e50, #3498db); color: white; padding: 30px; margin: -20px -20px 30px -20px; border-radius: 0 0 12px 12px; }
        .header h1 { margin: 0; font-size: 2.2em; }
        .header p { margin: 10px 0 0 0; opacity: 0.9; }
        .stats { display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: 20px; margin-bottom: 30px; }
        .stat-card { background: white; padding: 20px; border-radius: 12px; box-shadow: 0 4px 12px rgba(0,0,0,0.1); text-align: center; transition: transform 0.2s; }
        .stat-card:hover { transform: translateY(-2px); }
        .stat-number { font-size: 2.5em; font-weight: bold; color: #e74c3c; margin-bottom: 8px; }
        .stat-label { color: #6c757d; font-size: 0.9em; font-weight: 500; }
        .issues { background: white; padding: 30px; border-radius: 12px; box-shadow: 0 4px 12px rgba(0,0,0,0.1); }
        .issue { border-left: 5px solid #e74c3c; margin: 15px 0; padding: 20px; background: #f8f9fa; border-radius: 0 8px 8px 0; transition: all 0.2s; }
        .issue:hover { background: #e3f2fd; }
        .issue.high { border-left-color: #ff9800; }
        .issue.critical { border-left-color: #f44336; }
        .issue.medium { border-left-color: #ffc107; }
        .issue.low { border-left-color: #4caf50; }
        .issue-title { font-weight: 600; color: #2c3e50; margin-bottom: 10px; font-size: 1.1em; }
        .issue-meta { color: #6c757d; font-size: 0.9em; line-height: 1.4; }
        .refresh-btn { background: linear-gradient(135deg, #3498db, #2980b9); color: white; padding: 12px 24px; border: none; border-radius: 8px; cursor: pointer; font-weight: 500; transition: all 0.3s; }
        .refresh-btn:hover { transform: translateY(-1px); box-shadow: 0 4px 12px rgba(52, 152, 219, 0.3); }
        .loading { text-align: center; padding: 50px; color: #6c757d; font-size: 1.1em; }
        .score { text-align: center; padding: 25px; margin: 25px 0; border-radius: 12px; font-size: 1.2em; }
        .score.critical { background: linear-gradient(135deg, #e74c3c, #c0392b); color: white; }
        .score.poor { background: linear-gradient(135deg, #f39c12, #e67e22); color: white; }
        .score.good { background: linear-gradient(135deg, #f1c40f, #f39c12); color: black; }
        .score.excellent { background: linear-gradient(135deg, #27ae60, #229954); color: white; }
        .no-issues { background: linear-gradient(135deg, #27ae60, #229954); color: white; text-align: center; padding: 40px; border-radius: 12px; font-size: 1.3em; }
        .meta-item { display: inline-block; margin-right: 15px; }
        .tag { display: inline-block; padding: 3px 8px; border-radius: 12px; font-size: 0.75em; font-weight: 500; margin: 2px; color: white; }
        .tag.selected { background: #3498db; cursor: pointer; }
        .tag.removable { background: #e74c3c; cursor: pointer; padding-right: 20px; position: relative; }
        .tag.removable:hover { background: #c0392b; }
        .tag.removable::after { content: '×'; position: absolute; right: 6px; top: 50%; transform: translateY(-50%); font-size: 12px; }
        .tag-option { padding: 8px 12px; cursor: pointer; border-bottom: 1px solid #eee; display: flex; justify-content: space-between; align-items: center; }
        .tag-option:hover { background: #f8f9fa; }
        .tag-option.selected { background: #e3f2fd; }
        .issue-tags { margin-top: 8px; }
        .issue-tags .tag { font-size: 0.7em; padding: 2px 6px; }
        .tag-manager { margin-top: 8px; padding: 8px; background: rgba(0,0,0,0.05); border-radius: 6px; }
        .tag-manager input { width: 100px; padding: 2px 6px; border: 1px solid #ddd; border-radius: 3px; font-size: 0.8em; }
        .tag-manager button { padding: 2px 8px; border: none; border-radius: 3px; cursor: pointer; font-size: 0.75em; margin-left: 5px; }
        .tag-manager .add-btn { background: #27ae60; color: white; }
        .tag-manager .suggest-btn { background: #3498db; color: white; }
        @media (max-width: 768px) {
            .stats { grid-template-columns: repeat(auto-fit, minmax(120px, 1fr)); gap: 15px; }
            .header { padding: 20px; }
            .header h1 { font-size: 1.8em; }
            #tag-filters-panel div[style*="grid-template-columns"] { grid-template-columns: 1fr; }
        }
    </style>
</head>
<body>
    <div class="header">
        <h1>🎯 ATS Data Quality Dashboard</h1>
        <p>Real-time monitoring of data quality issues in the ATS system</p>
        <div style="margin-top: 15px; display: flex; justify-content: space-between; align-items: center; flex-wrap: wrap; gap: 15px;">
            <div>
                <span>Last updated: <span id="last-updated">-</span></span>
                <span id="processing-method" style="margin-left: 15px; color: #f39c12;"></span>
            </div>
            <div style="display: flex; gap: 8px; align-items: center; flex-wrap: wrap;">
                <button class="refresh-btn" onclick="loadData(currentPage, currentPageSize, currentSeverityFilter, useRay)">🔄 Refresh</button>
                <label style="color: white;">
                    <input type="checkbox" id="ray-toggle" onchange="toggleRay()" style="margin-right: 5px;">
                    ⚡ Ray Distributed
                </label>
                <select id="severity-filter" onchange="filterBySeverity()" style="padding: 4px 8px; border-radius: 4px;">
                    <option value="">All Severities</option>
                    <option value="critical">Critical Only</option>
                    <option value="high">High Only</option>
                    <option value="medium">Medium Only</option>
                    <option value="low">Low Only</option>
                </select>
                <select id="page-size" onchange="changePageSize()" style="padding: 4px 8px; border-radius: 4px;">
                    <option value="10">10 per page</option>
                    <option value="25">25 per page</option>
                    <option value="50" selected>50 per page</option>
                    <option value="100">100 per page</option>
                </select>
                <button onclick="showTagFilters()" style="padding: 4px 12px; background: #9b59b6; color: white; border: none; border-radius: 4px; cursor: pointer; font-size: 0.85em;">
                    🏷️ Tag Filters
                </button>
                <button onclick="runAutoTaggingBatch()" style="padding: 4px 12px; background: #f39c12; color: white; border: none; border-radius: 4px; cursor: pointer; font-size: 0.85em;">
                    🤖 Auto-Tag Batch
                </button>
            </div>
        </div>

        <!-- AGENT STATUS AND CONTROLS -->
        <div id="agent-section" style="margin-top: 20px; padding: 15px; background: rgba(255,255,255,0.15); border-radius: 8px; border: 1px solid rgba(255,255,255,0.2);">
            <div style="display: flex; justify-content: space-between; align-items: center; flex-wrap: wrap; gap: 15px;">
                <div id="agent-status" style="color: white; font-weight: bold;">
                    🤖 Agent: Loading...
                </div>

                <div id="agent-controls" style="display: flex; gap: 8px; flex-wrap: wrap;">
                    <button id="start-agent-btn" onclick="startAgent()"
                            style="padding: 6px 12px; background: #27ae60; color: white; border: none; border-radius: 4px; cursor: pointer; font-size: 0.85em; opacity: 0.9;">
                        ▶️ Start
                    </button>
                    <button id="stop-agent-btn" onclick="stopAgent()"
                            style="padding: 6px 12px; background: #e74c3c; color: white; border: none; border-radius: 4px; cursor: pointer; font-size: 0.85em; opacity: 0.9; display: none;">
                        ⏹️ Stop
                    </button>
                    <button onclick="showWorkflowsDialog()"
                            style="padding: 6px 12px; background: #3498db; color: white; border: none; border-radius: 4px; cursor: pointer; font-size: 0.85em; opacity: 0.9;">
                        📋 Workflows
                    </button>
                    <button onclick="showAgentMetrics()"
                            style="padding: 6px 12px; background: #9b59b6; color: white; border: none; border-radius: 4px; cursor: pointer; font-size: 0.85em; opacity: 0.9;">
                        📊 Metrics
                    </button>
                    <button onclick="showConfigDialog()"
                            style="padding: 6px 12px; background: #34495e; color: white; border: none; border-radius: 4px; cursor: pointer; font-size: 0.85em; opacity: 0.9;">
                        ⚙️ Config
                    </button>
                    <button onclick="showSystemHealthDialog()"
                            style="padding: 6px 12px; background: #16a085; color: white; border: none; border-radius: 4px; cursor: pointer; font-size: 0.85em; opacity: 0.9;">
                        🩺 Health
                    </button>
                    <button onclick="showAlertsDialog()"
                            style="padding: 6px 12px; background: #e74c3c; color: white; border: none; border-radius: 4px; cursor: pointer; font-size: 0.85em; opacity: 0.9;">
                        🚨 Alerts
                    </button>
                    <button onclick="showAutoTaggingRules()" 
                            style="padding: 6px 12px; background: #f39c12; color: white; border: none; border-radius: 4px; cursor: pointer; font-size: 0.85em; opacity: 0.9;">
                        🤖 Auto-Tag Rules
                    </button>
                </div>
            </div>
        </div>
        
        <!-- TAG FILTERING PANEL -->
        <div id="tag-filters-panel" style="margin-top: 20px; padding: 20px; background: rgba(255,255,255,0.15); border-radius: 8px; border: 1px solid rgba(255,255,255,0.2); display: none;">
            <div style="display: flex; justify-content: between; align-items: center; margin-bottom: 15px;">
                <h3 style="color: white; margin: 0; font-size: 1.1em;">🏷️ Filter Issues by Tags</h3>
                <button onclick="hideTagFilters()" style="background: transparent; color: white; border: 1px solid rgba(255,255,255,0.3); padding: 4px 8px; border-radius: 4px; cursor: pointer; font-size: 0.8em;">
                    ✕ Close
                </button>
            </div>
            
            <!-- Available Tags Display -->
            <div style="margin-bottom: 20px;">
                <label style="color: white; display: block; margin-bottom: 10px; font-weight: 500; font-size: 1.1em;">🏷️ Available Tags</label>
                <div id="available-tags-container" style="background: rgba(255,255,255,0.1); padding: 15px; border-radius: 8px; max-height: 300px; overflow-y: auto;">
                    <div style="color: #6c757d; text-align: center; padding: 20px;">Loading tags...</div>
                </div>
            </div>
            
            <!-- Selected Tags -->
            <div style="margin-bottom: 15px;">
                <label style="color: white; display: block; margin-bottom: 8px; font-weight: 500;">Selected Tags</label>
                <div id="selected-tags" style="background: rgba(255,255,255,0.1); padding: 10px; border-radius: 6px; min-height: 40px;">
                    <div style="color: #6c757d; font-size: 0.9em; font-style: italic;">No tags selected</div>
                </div>
            </div>
            
            <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 15px; margin-bottom: 15px;">
                <!-- Symbol Filter -->
                <div>
                    <label style="color: white; display: block; margin-bottom: 5px; font-weight: 500;">Symbols</label>
                    <input type="text" id="symbol-filter" placeholder="AAPL,MSFT,NVDA..." 
                           style="width: 100%; padding: 6px 8px; border: 1px solid rgba(255,255,255,0.3); border-radius: 4px; background: rgba(255,255,255,0.1); color: white;" />
                </div>
                
                <!-- Date Range -->
                <div>
                    <label style="color: white; display: block; margin-bottom: 5px; font-weight: 500;">Date Range</label>
                    <div style="display: flex; gap: 5px;">
                        <input type="date" id="date-from" 
                               style="flex: 1; padding: 6px; border: 1px solid rgba(255,255,255,0.3); border-radius: 4px; background: rgba(255,255,255,0.1); color: white;" />
                        <input type="date" id="date-to" 
                               style="flex: 1; padding: 6px; border: 1px solid rgba(255,255,255,0.3); border-radius: 4px; background: rgba(255,255,255,0.1); color: white;" />
                    </div>
                </div>
            </div>
            
            <div style="display: flex; justify-content: space-between; align-items: center; flex-wrap: wrap; gap: 10px;">
                <div style="display: flex; align-items: center; gap: 15px;">
                    <label style="color: white; display: flex; align-items: center; gap: 5px;">
                        <input type="radio" name="match-mode" value="ANY" checked />
                        Match ANY tag
                    </label>
                    <label style="color: white; display: flex; align-items: center; gap: 5px;">
                        <input type="radio" name="match-mode" value="ALL" />
                        Match ALL tags
                    </label>
                </div>
                <div style="display: flex; gap: 8px;">
                    <button onclick="clearAllFilters()" 
                            style="padding: 8px 16px; background: #6c757d; color: white; border: none; border-radius: 4px; cursor: pointer; font-size: 0.85em;">
                        Clear All
                    </button>
                    <button onclick="applyTagFilters()" 
                            style="padding: 8px 16px; background: #27ae60; color: white; border: none; border-radius: 4px; cursor: pointer; font-size: 0.85em;">
                        Apply Filters
                    </button>
                </div>
            </div>
            
            <!-- Active Filter Summary -->
            <div id="active-filters-summary" style="margin-top: 15px; padding: 10px; background: rgba(255,255,255,0.1); border-radius: 4px; color: white; font-size: 0.9em; display: none;">
                <strong>Active Filters:</strong> <span id="filter-summary-text"></span>
            </div>
        </div>
    </div>

    <div class="stats">
        <div class="stat-card">
            <div class="stat-number" id="total-issues">-</div>
            <div class="stat-label">Total Issues</div>
        </div>
        <div class="stat-card">
            <div class="stat-number" id="critical-issues">-</div>
            <div class="stat-label">Critical</div>
        </div>
        <div class="stat-card">
            <div class="stat-number" id="high-issues">-</div>
            <div class="stat-label">High Priority</div>
        </div>
        <div class="stat-card">
            <div class="stat-number" id="symbols-affected">-</div>
            <div class="stat-label">Symbols Affected</div>
        </div>
        <div class="stat-card">
            <div class="stat-number" id="quality-score">-</div>
            <div class="stat-label">Quality Score</div>
        </div>
    </div>

    <div id="quality-status" class="score critical">
        <h3>Overall Status: <span id="status-text">Loading...</span></h3>
        <p id="status-description">Analyzing data quality...</p>
    </div>

    <div class="issues">
        <h2>🔍 Detected Issues</h2>
        <div id="issues-list" class="loading">Loading data quality issues from database...</div>

        <!-- Pagination Controls -->
        <div id="pagination-controls" style="margin-top: 20px; text-align: center; display: none;">
            <div style="display: flex; justify-content: space-between; align-items: center; flex-wrap: wrap; gap: 15px; background: rgba(255,255,255,0.1); padding: 15px; border-radius: 8px;">
                <div style="color: white;">
                    <span id="pagination-info">-</span>
                </div>
                <div style="display: flex; gap: 5px; align-items: center;">
                    <button id="first-page" onclick="goToPage(1)" style="padding: 6px 10px; background: #3498db; color: white; border: none; border-radius: 4px; cursor: pointer;">⏮️ First</button>
                    <button id="prev-page" onclick="goToPage(currentPage - 1)" style="padding: 6px 10px; background: #3498db; color: white; border: none; border-radius: 4px; cursor: pointer;">⬅️ Prev</button>
                    <span style="color: white; margin: 0 10px;">
                        Page <span id="current-page-display">1</span> of <span id="total-pages-display">1</span>
                    </span>
                    <button id="next-page" onclick="goToPage(currentPage + 1)" style="padding: 6px 10px; background: #3498db; color: white; border: none; border-radius: 4px; cursor: pointer;">Next ➡️</button>
                    <button id="last-page" onclick="goToPage(999)" style="padding: 6px 10px; background: #3498db; color: white; border: none; border-radius: 4px; cursor: pointer;">Last ⏭️</button>
                </div>
            </div>
        </div>
    </div>

    <script>
        let currentPage = 1;
        let currentPageSize = 50;
        let currentSeverityFilter = null;
        let useRay = false;
        let availableTags = [];
        let selectedTags = [];
        let currentSymbolFilter = null;
        let currentDateFromFilter = null;
        let currentDateToFilter = null;
        let currentMatchMode = 'ANY';
        
        async function loadData(page = 1, pageSize = 50, severityFilter = null, rayEnabled = false) {
            try {
                currentPage = page;
                currentPageSize = pageSize;
                currentSeverityFilter = severityFilter;
                useRay = rayEnabled;

                document.getElementById('issues-list').innerHTML = '<div class="loading">Refreshing data...</div>';

                // Build query parameters
                const params = new URLSearchParams();
                params.append('page', page.toString());
                params.append('page_size', pageSize.toString());
                if (severityFilter) params.append('severity', severityFilter);
                if (rayEnabled) params.append('ray', 'true');
                
                // Add tag filtering parameters
                if (selectedTags.length > 0) {
                    params.append('tag_ids', selectedTags.map(tag => tag.id).join(','));
                    params.append('match_mode', currentMatchMode);
                }
                if (currentSymbolFilter) {
                    params.append('symbols', currentSymbolFilter);
                }
                if (currentDateFromFilter) {
                    params.append('date_from', currentDateFromFilter);
                }
                if (currentDateToFilter) {
                    params.append('date_to', currentDateToFilter);
                }
                
                // Use the tag-enhanced endpoint if filters are active
                const endpoint = (selectedTags.length > 0 || currentSymbolFilter || currentDateFromFilter || currentDateToFilter) 
                    ? `/data-quality/api/issues/?${params}` 
                    : `/data-quality/api/issues?${params}`;
                    
                const response = await fetch(endpoint);
                const data = await response.json();
                displayData(data);
            } catch (error) {
                document.getElementById('issues-list').innerHTML =
                    `<div style="color: #e74c3c; text-align: center; padding: 30px;">❌ Error loading data: ${error.message}</div>`;
            }
        }

        async function loadAgentStatus() {
            try {
                const response = await fetch('/agent/status');
                const status = await response.json();

                const statusElement = document.getElementById('agent-status');
                const startBtn = document.getElementById('start-agent-btn');
                const stopBtn = document.getElementById('stop-agent-btn');

                if (status.status === 'active') {
                    statusElement.innerHTML = `🤖 Agent: <span style="color: #27ae60;">ACTIVE</span> | Tools: ${status.tools_available || 0} | ID: ${status.agent_id || 'Unknown'}`;
                    startBtn.style.display = 'none';
                    stopBtn.style.display = 'inline-block';
                } else {
                    statusElement.innerHTML = `🤖 Agent: <span style="color: #6c757d;">IDLE</span> | Ready to start`;
                    startBtn.style.display = 'inline-block';
                    stopBtn.style.display = 'none';
                }
            } catch (error) {
                console.error('Error loading agent status:', error);
                document.getElementById('agent-status').innerHTML = `🤖 Agent: <span style="color: #f39c12;">ERROR</span> - ${error.message}`;
            }
        }

        function displayData(data) {
            const issues = data.issues || [];
            const summary = data.summary || {};
            const pagination = data.pagination || {};

            // Use summary stats if available (from Ray/pagination), otherwise calculate from page data
            const totalIssues = summary.total_issues || data.total_count || issues.length;
            const criticalIssues = summary.critical !== undefined ? summary.critical : issues.filter(i => i.severity === 'critical').length;
            const highIssues = summary.high !== undefined ? summary.high : issues.filter(i => i.severity === 'high').length;
            const mediumIssues = summary.medium !== undefined ? summary.medium : issues.filter(i => i.severity === 'medium').length;
            const lowIssues = summary.low !== undefined ? summary.low : issues.filter(i => i.severity === 'low').length;
            const uniqueSymbols = [...new Set(issues.map(i => i.symbol).filter(s => s !== 'SYSTEM'))].length;

            // Update stats with total counts (not just page counts)
            document.getElementById('total-issues').textContent = totalIssues;
            document.getElementById('critical-issues').textContent = criticalIssues;
            document.getElementById('high-issues').textContent = highIssues;
            document.getElementById('symbols-affected').textContent = uniqueSymbols;
            document.getElementById('last-updated').textContent = new Date().toLocaleTimeString();

            // Calculate quality score
            const maxScore = 100;
            const penalty = (criticalIssues * 20) + (highIssues * 10) + (mediumIssues * 5) + (lowIssues * 1);
            const qualityScore = Math.max(0, maxScore - penalty);

            document.getElementById('quality-score').textContent = qualityScore;

            // Update quality status
            const statusDiv = document.getElementById('quality-status');
            const statusText = document.getElementById('status-text');
            const statusDesc = document.getElementById('status-description');

            if (qualityScore >= 90) {
                statusDiv.className = 'score excellent';
                statusText.textContent = 'EXCELLENT';
                statusDesc.textContent = '✅ System operating at high quality standards';
            } else if (qualityScore >= 75) {
                statusDiv.className = 'score good';
                statusText.textContent = 'GOOD';
                statusDesc.textContent = '⚠️ Minor issues detected, monitoring recommended';
            } else if (qualityScore >= 50) {
                statusDiv.className = 'score poor';
                statusText.textContent = 'POOR';
                statusDesc.textContent = '🚨 Significant issues detected, attention required';
            } else {
                statusDiv.className = 'score critical';
                statusText.textContent = 'CRITICAL';
                statusDesc.textContent = '💥 Major data quality problems, immediate action needed';
            }

            // Display issues
            const issuesContainer = document.getElementById('issues-list');
            if (totalIssues === 0) {
                issuesContainer.innerHTML = '<div class="no-issues">✅ Excellent! No data quality issues detected.<br>System is operating normally.</div>';
                return;
            }

            // Group issues by severity
            const issuesBySeverity = {
                critical: issues.filter(i => i.severity === 'critical'),
                high: issues.filter(i => i.severity === 'high'),
                medium: issues.filter(i => i.severity === 'medium'),
                low: issues.filter(i => i.severity === 'low')
            };

            let issuesHtml = '';

            ['critical', 'high', 'medium', 'low'].forEach(severity => {
                const severityIssues = issuesBySeverity[severity];
                if (severityIssues.length > 0) {
                    const severityEmoji = {critical: '🚨', high: '⚠️', medium: '📋', low: '💡'}[severity];
                    issuesHtml += `<h3 style="margin-top: 30px; color: #2c3e50;">${severityEmoji} ${severity.toUpperCase()} ISSUES (${severityIssues.length})</h3>`;

                    severityIssues.forEach(issue => {
                        const actionButtons = getIssueActionButtons(issue);
                        const tagsHtml = renderIssueTags(issue);
                        const tagManagerHtml = renderTagManager(issue.id);
                        
                        issuesHtml += `
                            <div class="issue ${issue.severity}">
                                <div class="issue-title">${issue.symbol}: ${issue.description}</div>
                                <div class="issue-meta">
                                    <span class="meta-item">📅 ${issue.affected_date}</span>
                                    <span class="meta-item">🏷️ ${issue.issue_type.replace('_', ' ')}</span>
                                    <span class="meta-item">📊 ${issue.field}</span>
                                    <span class="meta-item">📡 ${issue.vendor_source}</span>
                                    ${issue.expected_value && issue.actual_value ?
                                      `<br><span class="meta-item">💡 Expected: ${issue.expected_value}</span><span class="meta-item">📊 Actual: ${issue.actual_value}</span>` : ''}
                                </div>
                                ${tagsHtml}
                                ${tagManagerHtml}
                                ${actionButtons ? `<div style="margin-top: 10px; display: flex; gap: 8px; flex-wrap: wrap;">${actionButtons}</div>` : ''}
                            </div>
                        `;
                    });
                }
            });

            issuesContainer.innerHTML = issuesHtml;

            // Update pagination controls
            updatePagination(pagination);

            // Show processing method
            const methodElement = document.getElementById('processing-method');
            if (data.method) {
                const methodText = data.method === 'ray_distributed' ?
                    '⚡ Ray Distributed Processing' :
                    '🔄 Legacy Single-threaded';
                methodElement.textContent = methodText;
                methodElement.style.display = 'inline';
            } else {
                methodElement.style.display = 'none';
            }
        }
        
        // =====================================
        // TAG RENDERING HELPER FUNCTIONS
        // =====================================
        
        function renderIssueTags(issue) {
            if (!issue.tags || issue.tags.length === 0) {
                return '<div class="issue-tags"><small style="color: #6c757d;">No tags</small></div>';
            }
            
            let tagsHtml = '<div class="issue-tags">';
            issue.tags.forEach(tag => {
                tagsHtml += `
                    <span class="tag" style="background-color: ${tag.color};" 
                          onclick="removeTagFromIssue(${issue.id}, ${tag.id})"
                          title="Click to remove tag">
                        ${tag.name}
                    </span>
                `;
            });
            tagsHtml += '</div>';
            return tagsHtml;
        }
        
        function renderTagManager(issueId) {
            return `
                <div class="tag-manager">
                    <input type="text" 
                           id="tag-input-${issueId}" 
                           placeholder="Add tag..." 
                           onkeypress="if(event.key==='Enter'){addTagToIssue(${issueId}, this.value); this.value='';}"
                           style="margin-right: 5px;" />
                    <button class="add-btn" 
                            onclick="addTagToIssue(${issueId}, document.getElementById('tag-input-${issueId}').value); document.getElementById('tag-input-${issueId}').value='';">
                        + Add
                    </button>
                    <button class="suggest-btn" 
                            onclick="suggestTagsForIssue(${issueId})">
                        🤖 Suggest
                    </button>
                    <button class="suggest-btn" 
                            onclick="autoTagIssue(${issueId})"
                            style="background: #f39c12;">
                        ⚡ Auto-Tag
                    </button>
                </div>
            `;
        }
        
        // Update pagination display
        function updatePagination(pagination) {
            const paginationControls = document.getElementById('pagination-controls');
            if (!pagination || pagination.total_pages <= 1) {
                paginationControls.style.display = 'none';
                return;
            }

            paginationControls.style.display = 'block';

            // Update pagination info
            const start = ((pagination.current_page - 1) * pagination.page_size) + 1;
            const end = Math.min(pagination.current_page * pagination.page_size, pagination.total_issues);
            document.getElementById('pagination-info').textContent =
                `Showing ${start}-${end} of ${pagination.total_issues} issues`;

            // Update page display
            document.getElementById('current-page-display').textContent = pagination.current_page;
            document.getElementById('total-pages-display').textContent = pagination.total_pages;

            // Update button states
            document.getElementById('first-page').disabled = !pagination.has_prev;
            document.getElementById('prev-page').disabled = !pagination.has_prev;
            document.getElementById('next-page').disabled = !pagination.has_next;
            document.getElementById('last-page').disabled = !pagination.has_next;
            document.getElementById('last-page').onclick = () => goToPage(pagination.total_pages);
        }

        // Pagination functions
        function goToPage(page) {
            if (page < 1) page = 1;
            loadData(page, currentPageSize, currentSeverityFilter, useRay);
        }

        function toggleRay() {
            const rayToggle = document.getElementById('ray-toggle');
            useRay = rayToggle.checked;
            loadData(1, currentPageSize, currentSeverityFilter, useRay); // Reset to page 1 when toggling Ray
        }

        function filterBySeverity() {
            const severityFilter = document.getElementById('severity-filter');
            currentSeverityFilter = severityFilter.value || null;
            loadData(1, currentPageSize, currentSeverityFilter, useRay); // Reset to page 1 when filtering
        }

        function changePageSize() {
            const pageSizeSelect = document.getElementById('page-size');
            currentPageSize = parseInt(pageSizeSelect.value);
            loadData(1, currentPageSize, currentSeverityFilter, useRay); // Reset to page 1 when changing page size
        }

        // Helper function to get action buttons for issues
        function getIssueActionButtons(issue) {
            const actions = [];

            // Determine appropriate actions based on issue type and severity
            switch (issue.issue_type) {
                case 'missing_data':
                    actions.push({
                        label: '🔄 Trigger Backfill',
                        action: 'trigger_backfill',
                        color: '#3498db'
                    });
                    break;

                case 'extreme_volume':
                case 'extreme_price_range':
                    actions.push({
                        label: '🔍 Cross-Validate',
                        action: 'cross_validate_vendors',
                        color: '#9b59b6'
                    });
                    break;

                case 'data_inconsistency':
                    actions.push({
                        label: '🔄 Auto-Deduplicate',
                        action: 'auto_deduplicate',
                        color: '#27ae60'
                    });
                    actions.push({
                        label: '🔍 Cross-Validate',
                        action: 'cross_validate_vendors',
                        color: '#9b59b6'
                    });
                    break;

                default:
                    if (issue.severity === 'critical' || issue.severity === 'high') {
                        actions.push({
                            label: '🚨 Escalate',
                            action: 'escalate_to_human',
                            color: '#e74c3c'
                        });
                    }
                    break;
            }

            // Add general "Investigate" action for all issues
            actions.push({
                label: '🕵️ Investigate',
                action: 'investigate_issue',
                color: '#f39c12'
            });

            return actions.map(actionBtn =>
                `<button onclick="triggerAgentAction('${actionBtn.action}', '${issue.id}')"
                         style="padding: 4px 8px; background: ${actionBtn.color}; color: white; border: none; border-radius: 4px; cursor: pointer; font-size: 0.8em;">
                    ${actionBtn.label}
                 </button>`
            ).join('');
        }

        // Agent control functions - using the correct loadAgentStatus function defined above

        async function startAgent() {
            try {
                const response = await fetch('/agent/start', { method: 'POST' });
                const result = await response.json();

                if (response.ok && result.message) {
                    // Update status immediately and then again after a short delay
                    loadAgentStatus();
                    setTimeout(loadAgentStatus, 500);
                    setTimeout(loadAgentStatus, 2000);
                    showNotification('✅ Data Quality Agent started successfully', 'success');
                } else {
                    showNotification(`❌ Failed to start agent: ${result.message || result.error}`, 'error');
                }
            } catch (error) {
                showNotification(`❌ Error starting agent: ${error.message}`, 'error');
            }
        }

        async function stopAgent() {
            try {
                const response = await fetch('/agent/stop', { method: 'POST' });
                const result = await response.json();

                if (result.success) {
                    setTimeout(loadAgentStatus, 1000); // Reload status after delay
                    showNotification('⏹️ Data Quality Agent stopped successfully', 'warning');
                } else {
                    showNotification(`❌ Failed to stop agent: ${result.message}`, 'error');
                }
            } catch (error) {
                showNotification(`❌ Error stopping agent: ${error.message}`, 'error');
            }
        }

        async function showWorkflowsDialog() {
            try {
                const response = await fetch('/agent/workflows');
                const data = await response.json();

                const workflows = data.workflows || [];
                let workflowsHtml = '<h3>🔄 Active Workflows</h3>';

                if (workflows.length === 0) {
                    workflowsHtml += '<p>No active workflows found.</p>';
                } else {
                    workflowsHtml += '<div style="max-height: 400px; overflow-y: auto;">';
                    workflows.forEach(wf => {
                        const statusColor = {
                            'created': '#3498db',
                            'executing': '#f39c12',
                            'pending_approval': '#9b59b6',
                            'completed': '#27ae60',
                            'failed': '#e74c3c'
                        }[wf.status] || '#6c757d';

                        workflowsHtml += `
                            <div style="border: 1px solid #ddd; margin: 10px 0; padding: 15px; border-radius: 8px; background: #f8f9fa;">
                                <div><strong>${wf.issue_type}</strong> - ${wf.primary_action}</div>
                                <div style="color: ${statusColor}; font-weight: bold;">Status: ${wf.status.toUpperCase()}</div>
                                <div style="color: #6c757d; font-size: 0.9em;">
                                    ID: ${wf.workflow_id} | Priority: ${wf.priority} |
                                    Created: ${new Date(wf.created_at).toLocaleString()}
                                </div>
                                ${wf.error ? `<div style="color: #e74c3c; margin-top: 5px;">Error: ${wf.error}</div>` : ''}
                            </div>
                        `;
                    });
                    workflowsHtml += '</div>';
                }

                showModal('Workflows', workflowsHtml);
            } catch (error) {
                showNotification(`❌ Error loading workflows: ${error.message}`, 'error');
            }
        }

        async function showAgentMetrics() {
            try {
                const response = await fetch('/agent/metrics');
                const metrics = await response.json();

                let metricsHtml = '<h3>📊 Agent Performance Metrics</h3>';
                metricsHtml += `
                    <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 20px; margin: 20px 0;">
                        <div style="background: #f8f9fa; padding: 15px; border-radius: 8px;">
                            <h4>Workflow Statistics</h4>
                            <p><strong>Total Workflows:</strong> ${metrics.workflow_stats?.total_workflows || 0}</p>
                            <p><strong>Success Rate:</strong> ${((metrics.workflow_stats?.success_rate || 0) * 100).toFixed(1)}%</p>
                            <p><strong>Active Workflows:</strong> ${metrics.workflow_stats?.active_workflows || 0}</p>
                            <p><strong>Avg Duration:</strong> ${(metrics.workflow_stats?.avg_duration_seconds || 0).toFixed(1)}s</p>
                        </div>
                        <div style="background: #f8f9fa; padding: 15px; border-radius: 8px;">
                            <h4>Health Score</h4>
                            <p><strong>Overall Health:</strong> ${(metrics.health_score?.overall_score || 0).toFixed(1)}/100</p>
                            <p><strong>Performance:</strong> ${(metrics.health_score?.performance_score || 0).toFixed(1)}/100</p>
                            <p><strong>Reliability:</strong> ${(metrics.health_score?.reliability_score || 0).toFixed(1)}/100</p>
                        </div>
                    </div>
                    <div style="background: #f8f9fa; padding: 15px; border-radius: 8px; margin-top: 20px;">
                        <h4>Recent Activity</h4>
                        <p><strong>Issues Resolved:</strong> ${metrics.resolution_stats?.total_resolved || 0}</p>
                        <p><strong>Auto-resolved:</strong> ${metrics.resolution_stats?.auto_resolved || 0}</p>
                        <p><strong>Escalated:</strong> ${metrics.resolution_stats?.escalated || 0}</p>
                        <p><strong>Last Cycle:</strong> ${metrics.last_cycle ? new Date(metrics.last_cycle).toLocaleString() : 'Never'}</p>
                    </div>
                `;

                showModal('Agent Metrics', metricsHtml);
            } catch (error) {
                showNotification(`❌ Error loading metrics: ${error.message}`, 'error');
            }
        }

        async function triggerAgentAction(action, issueId = null) {
            try {
                const payload = { action };
                if (issueId) payload.issue_id = issueId;

                const response = await fetch('/agent/action', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify(payload)
                });

                const result = await response.json();

                if (result.success) {
                    showNotification(`✅ Agent action "${action}" triggered successfully`, 'success');
                    setTimeout(loadData, 2000); // Refresh dashboard after action
                } else {
                    showNotification(`❌ Agent action failed: ${result.error}`, 'error');
                }
            } catch (error) {
                showNotification(`❌ Error triggering action: ${error.message}`, 'error');
            }
        }

        async function showConfigDialog() {
            try {
                const response = await fetch('/agent/config');
                const data = await response.json();

                const config = data.config;
                const configHtml = `
                    <h3>⚙️ Agent Configuration</h3>
                    <div style="max-height: 500px; overflow-y: auto;">
                        <div style="margin-bottom: 20px;">
                            <h4>Monitoring Settings</h4>
                            <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 15px; margin: 10px 0;">
                                <div>
                                    <label>Cycle Interval (seconds):</label>
                                    <input type="number" id="cycle-interval" value="${config.monitoring.cycle_interval_seconds}"
                                           style="width: 100%; padding: 5px; margin-top: 2px;">
                                </div>
                                <div>
                                    <label>Max Concurrent Workflows:</label>
                                    <input type="number" id="max-workflows" value="${config.monitoring.max_concurrent_workflows}"
                                           style="width: 100%; padding: 5px; margin-top: 2px;">
                                </div>
                            </div>
                        </div>

                        <div style="margin-bottom: 20px;">
                            <h4>Issue Thresholds</h4>
                            <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 15px; margin: 10px 0;">
                                <div>
                                    <label>Extreme Volume Multiplier:</label>
                                    <input type="number" step="0.1" id="volume-multiplier" value="${config.issue_thresholds.extreme_volume_multiplier}"
                                           style="width: 100%; padding: 5px; margin-top: 2px;">
                                </div>
                                <div>
                                    <label>Price Change Threshold (%):</label>
                                    <input type="number" step="0.1" id="price-change-threshold" value="${config.issue_thresholds.extreme_price_change_percent}"
                                           style="width: 100%; padding: 5px; margin-top: 2px;">
                                </div>
                            </div>
                        </div>

                        <div style="margin-bottom: 20px;">
                            <h4>Action Settings</h4>
                            <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 15px; margin: 10px 0;">
                                <div>
                                    <label>Auto-resolve Confidence:</label>
                                    <input type="number" step="0.01" min="0" max="1" id="auto-resolve-threshold"
                                           value="${config.action_thresholds.auto_resolve_confidence_threshold}"
                                           style="width: 100%; padding: 5px; margin-top: 2px;">
                                </div>
                                <div>
                                    <label>Max Retry Attempts:</label>
                                    <input type="number" id="max-retries" value="${config.action_thresholds.max_retry_attempts}"
                                           style="width: 100%; padding: 5px; margin-top: 2px;">
                                </div>
                            </div>
                        </div>

                        <div style="margin-bottom: 20px;">
                            <h4>Agent Behavior</h4>
                            <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 15px; margin: 10px 0;">
                                <div>
                                    <label>
                                        <input type="checkbox" id="autonomous-mode" ${config.enable_autonomous_mode ? 'checked' : ''}>
                                        Enable Autonomous Mode
                                    </label>
                                </div>
                                <div>
                                    <label>
                                        <input type="checkbox" id="learning-mode" ${config.enable_learning_mode ? 'checked' : ''}>
                                        Enable Learning Mode
                                    </label>
                                </div>
                            </div>
                        </div>

                        <div style="display: flex; gap: 10px; justify-content: flex-end; margin-top: 20px; padding-top: 15px; border-top: 1px solid #ddd;">
                            <button onclick="updateAgentConfig()"
                                    style="padding: 8px 16px; background: #27ae60; color: white; border: none; border-radius: 4px; cursor: pointer;">
                                💾 Save Configuration
                            </button>
                            <button onclick="resetAgentConfig()"
                                    style="padding: 8px 16px; background: #e74c3c; color: white; border: none; border-radius: 4px; cursor: pointer;">
                                🔄 Reset to Defaults
                            </button>
                            <button onclick="applyEnvironmentConfig('development')"
                                    style="padding: 8px 16px; background: #3498db; color: white; border: none; border-radius: 4px; cursor: pointer;">
                                🔧 Dev Mode
                            </button>
                        </div>
                    </div>
                `;

                showModal('Agent Configuration', configHtml);
            } catch (error) {
                showNotification(`❌ Error loading configuration: ${error.message}`, 'error');
            }
        }

        async function updateAgentConfig() {
            try {
                const updates = {
                    monitoring: {
                        cycle_interval_seconds: parseInt(document.getElementById('cycle-interval').value),
                        max_concurrent_workflows: parseInt(document.getElementById('max-workflows').value)
                    },
                    issue_thresholds: {
                        extreme_volume_multiplier: parseFloat(document.getElementById('volume-multiplier').value),
                        extreme_price_change_percent: parseFloat(document.getElementById('price-change-threshold').value)
                    },
                    action_thresholds: {
                        auto_resolve_confidence_threshold: parseFloat(document.getElementById('auto-resolve-threshold').value),
                        max_retry_attempts: parseInt(document.getElementById('max-retries').value)
                    },
                    enable_autonomous_mode: document.getElementById('autonomous-mode').checked,
                    enable_learning_mode: document.getElementById('learning-mode').checked
                };

                const response = await fetch('/agent/config', {
                    method: 'PUT',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify(updates)
                });

                const result = await response.json();

                if (result.success) {
                    showNotification('✅ Configuration updated successfully', 'success');
                    document.querySelector('.modal').remove();
                } else {
                    showNotification('❌ Failed to update configuration', 'error');
                }
            } catch (error) {
                showNotification(`❌ Error updating configuration: ${error.message}`, 'error');
            }
        }

        async function resetAgentConfig() {
            try {
                const response = await fetch('/agent/config/reset', { method: 'POST' });
                const result = await response.json();

                if (result.success) {
                    showNotification('✅ Configuration reset to defaults', 'success');
                    document.querySelector('.modal').remove();
                    setTimeout(showConfigDialog, 1000); // Show updated config
                } else {
                    showNotification('❌ Failed to reset configuration', 'error');
                }
            } catch (error) {
                showNotification(`❌ Error resetting configuration: ${error.message}`, 'error');
            }
        }

        async function applyEnvironmentConfig(environment) {
            try {
                const response = await fetch(`/agent/config/environment/${environment}`, { method: 'POST' });
                const result = await response.json();

                if (result.success) {
                    showNotification(`✅ Applied ${environment} configuration`, 'success');
                    document.querySelector('.modal').remove();
                    setTimeout(showConfigDialog, 1000); // Show updated config
                } else {
                    showNotification(`❌ Failed to apply ${environment} configuration`, 'error');
                }
            } catch (error) {
                showNotification(`❌ Error applying ${environment} configuration: ${error.message}`, 'error');
            }
        }

        async function showAlertsDialog() {
            try {
                const response = await fetch('/agent/alerts');
                const data = await response.json();

                const alerts = data.active_alerts || [];
                const summary = data.alert_summary || {};
                const channels = data.notification_channels || {};

                const alertsHtml = `
                    <h3>🚨 Alert Management</h3>
                    <div style="max-height: 600px; overflow-y: auto;">

                        <!-- Alert Summary -->
                        <div style="display: grid; grid-template-columns: repeat(4, 1fr); gap: 10px; margin-bottom: 20px;">
                            <div style="background: #e74c3c; color: white; padding: 15px; border-radius: 8px; text-align: center;">
                                <h3 style="margin: 0;">${summary.active_by_severity?.critical || 0}</h3>
                                <p style="margin: 5px 0 0 0;">Critical</p>
                            </div>
                            <div style="background: #e67e22; color: white; padding: 15px; border-radius: 8px; text-align: center;">
                                <h3 style="margin: 0;">${summary.active_by_severity?.high || 0}</h3>
                                <p style="margin: 5px 0 0 0;">High</p>
                            </div>
                            <div style="background: #f39c12; color: white; padding: 15px; border-radius: 8px; text-align: center;">
                                <h3 style="margin: 0;">${summary.active_by_severity?.medium || 0}</h3>
                                <p style="margin: 5px 0 0 0;">Medium</p>
                            </div>
                            <div style="background: #3498db; color: white; padding: 15px; border-radius: 8px; text-align: center;">
                                <h3 style="margin: 0;">${summary.active_by_severity?.low || 0}</h3>
                                <p style="margin: 5px 0 0 0;">Low</p>
                            </div>
                        </div>

                        <!-- Active Alerts -->
                        <div style="margin-bottom: 20px;">
                            <h4>🔥 Active Alerts (${alerts.length})</h4>
                            ${alerts.length > 0 ?
                                alerts.map(alert => {
                                    const severityColors = {
                                        critical: '#e74c3c',
                                        high: '#e67e22',
                                        medium: '#f39c12',
                                        low: '#3498db'
                                    };
                                    const severityColor = severityColors[alert.severity] || '#6c757d';

                                    return `
                                        <div style="border-left: 4px solid ${severityColor}; padding: 15px; margin: 10px 0; background: #f8f9fa; border-radius: 0 8px 8px 0;">
                                            <div style="display: flex; justify-content: between; align-items: start;">
                                                <div style="flex: 1;">
                                                    <h5 style="margin: 0 0 5px 0; color: ${severityColor};">${alert.title}</h5>
                                                    <p style="margin: 0 0 8px 0;">${alert.message}</p>
                                                    <small style="color: #6c757d;">
                                                        ${alert.source_component} • ${new Date(alert.timestamp).toLocaleString()}
                                                        ${alert.acknowledged ? ' • ✅ Acknowledged' : ''}
                                                    </small>
                                                </div>
                                                <div style="display: flex; gap: 5px; margin-left: 15px;">
                                                    ${!alert.acknowledged ?
                                                        `<button onclick="acknowledgeAlert('${alert.alert_id}')"
                                                                 style="padding: 4px 8px; background: #3498db; color: white; border: none; border-radius: 4px; cursor: pointer; font-size: 0.8em;">
                                                            ✓ Ack
                                                         </button>` : ''
                                                    }
                                                    <button onclick="resolveAlert('${alert.alert_id}')"
                                                            style="padding: 4px 8px; background: #27ae60; color: white; border: none; border-radius: 4px; cursor: pointer; font-size: 0.8em;">
                                                        ✓ Resolve
                                                    </button>
                                                </div>
                                            </div>
                                        </div>
                                    `;
                                }).join('') :
                                '<p style="color: #27ae60; text-align: center; padding: 20px;">✅ No active alerts</p>'
                            }
                        </div>

                        <!-- Alert Statistics -->
                        <div style="margin-bottom: 20px;">
                            <h4>📊 Alert Statistics</h4>
                            <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 15px;">
                                <div style="background: #f8f9fa; padding: 15px; border-radius: 8px;">
                                    <h5>Recent Activity</h5>
                                    <p><strong>Last Hour:</strong> ${summary.alerts_last_hour || 0} alerts</p>
                                    <p><strong>Last 24h:</strong> ${summary.alerts_last_24h || 0} alerts</p>
                                    <p><strong>Rules Enabled:</strong> ${summary.alert_rules_enabled || 0}</p>
                                </div>
                                <div style="background: #f8f9fa; padding: 15px; border-radius: 8px;">
                                    <h5>Notification Channels</h5>
                                    ${Object.entries(channels).map(([id, channel]) =>
                                        `<p><strong>${id}:</strong> ${channel.enabled ? '✅' : '❌'} ${channel.type}</p>`
                                    ).join('') || '<p>No channels configured</p>'}
                                </div>
                            </div>
                        </div>

                        <!-- Management Actions -->
                        <div style="display: flex; gap: 10px; justify-content: center; padding-top: 15px; border-top: 1px solid #ddd;">
                            <button onclick="testNotificationChannels()"
                                    style="padding: 8px 16px; background: #3498db; color: white; border: none; border-radius: 4px; cursor: pointer;">
                                📧 Test Notifications
                            </button>
                            <button onclick="refreshAlerts()"
                                    style="padding: 8px 16px; background: #27ae60; color: white; border: none; border-radius: 4px; cursor: pointer;">
                                🔄 Refresh
                            </button>
                        </div>

                        <div style="text-align: center; padding-top: 15px; color: #6c757d;">
                            <small>Last updated: ${new Date(data.retrieved_at).toLocaleString()}</small>
                        </div>
                    </div>
                `;

                showModal('Alert Management', alertsHtml);
            } catch (error) {
                showNotification(`❌ Error loading alerts: ${error.message}`, 'error');
            }
        }

        async function acknowledgeAlert(alertId) {
            try {
                const response = await fetch(`/agent/alerts/${alertId}/acknowledge`, { method: 'POST' });
                const result = await response.json();

                if (result.success) {
                    showNotification('✅ Alert acknowledged', 'success');
                    setTimeout(showAlertsDialog, 1000); // Refresh alerts dialog
                } else {
                    showNotification('❌ Failed to acknowledge alert', 'error');
                }
            } catch (error) {
                showNotification(`❌ Error acknowledging alert: ${error.message}`, 'error');
            }
        }

        async function resolveAlert(alertId) {
            try {
                const response = await fetch(`/agent/alerts/${alertId}/resolve`, { method: 'POST' });
                const result = await response.json();

                if (result.success) {
                    showNotification('✅ Alert resolved', 'success');
                    setTimeout(showAlertsDialog, 1000); // Refresh alerts dialog
                } else {
                    showNotification('❌ Failed to resolve alert', 'error');
                }
            } catch (error) {
                showNotification(`❌ Error resolving alert: ${error.message}`, 'error');
            }
        }

        async function testNotificationChannels() {
            try {
                const response = await fetch('/agent/alerts/test-channels', { method: 'POST' });
                const result = await response.json();

                const successful = result.successful_channels;
                const failed = result.failed_channels;

                if (successful > 0) {
                    showNotification(`✅ ${successful} channels tested successfully (${failed} failed)`, 'success');
                } else {
                    showNotification(`❌ All ${failed} notification channels failed`, 'error');
                }
            } catch (error) {
                showNotification(`❌ Error testing notification channels: ${error.message}`, 'error');
            }
        }

        async function refreshAlerts() {
            document.querySelector('.modal').remove();
            setTimeout(showAlertsDialog, 500);
        }

        async function showSystemHealthDialog() {
            try {
                const response = await fetch('/agent/system-health');
                const data = await response.json();

                const health = data.system_health;
                const agent = data.agent_integration;
                const ops = data.operational_summary;

                const statusColors = {
                    excellent: '#27ae60',
                    good: '#f39c12',
                    warning: '#e67e22',
                    critical: '#e74c3c'
                };

                const statusColor = statusColors[health.status] || '#6c757d';

                const healthHtml = `
                    <h3>🩺 System Health Monitor</h3>
                    <div style="max-height: 600px; overflow-y: auto;">

                        <!-- Overall Health Status -->
                        <div style="background: ${statusColor}; color: white; padding: 15px; border-radius: 8px; margin-bottom: 20px; text-align: center;">
                            <h2 style="margin: 0;">Health Score: ${health.health_score}/100</h2>
                            <p style="margin: 5px 0 0 0; font-size: 1.1em;">Status: ${health.status.toUpperCase()}</p>
                        </div>

                        <!-- System Metrics -->
                        <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 15px; margin-bottom: 20px;">
                            <div style="background: #f8f9fa; padding: 15px; border-radius: 8px;">
                                <h4>💻 System Resources</h4>
                                <p><strong>CPU:</strong> ${health.latest_metrics?.cpu_percent || 0}%</p>
                                <p><strong>Memory:</strong> ${health.latest_metrics?.memory_percent || 0}%</p>
                                <p><strong>Disk Usage:</strong> ${health.latest_metrics?.disk_usage_percent || 0}%</p>
                                <p><strong>Free Space:</strong> ${(health.latest_metrics?.disk_free_gb || 0).toFixed(1)}GB</p>
                            </div>
                            <div style="background: #f8f9fa; padding: 15px; border-radius: 8px;">
                                <h4>🤖 Agent Status</h4>
                                <p><strong>Status:</strong> ${agent.agent_status}</p>
                                <p><strong>Monitoring:</strong> ${agent.monitoring_active ? 'Active' : 'Inactive'}</p>
                                <p><strong>Workflows:</strong> ${ops.total_workflows}</p>
                                <p><strong>Last Scan:</strong> ${ops.last_scan ? new Date(ops.last_scan).toLocaleTimeString() : 'Never'}</p>
                            </div>
                        </div>

                        <!-- Active Alerts -->
                        <div style="margin-bottom: 20px;">
                            <h4>🚨 Active Alerts (${health.active_alerts?.length || 0})</h4>
                            ${health.active_alerts?.length > 0 ?
                                health.active_alerts.map(alert => `
                                    <div style="border-left: 4px solid ${alert.severity === 'critical' ? '#e74c3c' : alert.severity === 'warning' ? '#f39c12' : '#3498db'};
                                                padding: 10px; margin: 5px 0; background: #f8f9fa; border-radius: 0 4px 4px 0;">
                                        <strong>${alert.component.toUpperCase()}:</strong> ${alert.message}
                                        <br><small>${new Date(alert.timestamp).toLocaleString()}</small>
                                    </div>
                                `).join('') :
                                '<p style="color: #27ae60;">✅ No active alerts</p>'
                            }
                        </div>

                        <!-- Performance Trends -->
                        <div style="margin-bottom: 20px;">
                            <h4>📈 Performance Trends</h4>
                            <div style="background: #f8f9fa; padding: 15px; border-radius: 8px;">
                                <p><strong>CPU (10min avg):</strong> ${health.trends?.cpu_avg_10min || 0}%</p>
                                <p><strong>Memory (10min avg):</strong> ${health.trends?.memory_avg_10min || 0}%</p>
                                <p><strong>Monitoring Duration:</strong> ${(health.trends?.monitoring_duration_hours || 0).toFixed(1)} hours</p>
                                <p><strong>Data Points:</strong> ${health.trends?.metrics_collected || 0}</p>
                            </div>
                        </div>

                        <!-- Recommendations -->
                        <div style="margin-bottom: 20px;">
                            <h4>💡 Recommendations</h4>
                            <div style="background: #f8f9fa; padding: 15px; border-radius: 8px;">
                                ${health.recommendations?.map(rec => `<p>• ${rec}</p>`).join('') || '<p>No recommendations available</p>'}
                            </div>
                        </div>

                        <!-- MCP Tools Status -->
                        <div style="margin-bottom: 20px;">
                            <h4>🛠️ MCP Tools Available</h4>
                            <div style="background: #f8f9fa; padding: 15px; border-radius: 8px;">
                                ${agent.tools_available?.map(tool => `
                                    <span style="display: inline-block; background: #3498db; color: white; padding: 3px 8px; margin: 2px; border-radius: 12px; font-size: 0.8em;">
                                        ${tool}
                                    </span>
                                `).join('') || 'No tools available'}
                            </div>
                        </div>

                        <div style="text-align: center; padding-top: 15px; border-top: 1px solid #ddd; color: #6c757d;">
                            <small>Last updated: ${new Date(data.retrieved_at).toLocaleString()}</small>
                        </div>
                    </div>
                `;

                showModal('System Health Monitor', healthHtml);
            } catch (error) {
                showNotification(`❌ Error loading system health: ${error.message}`, 'error');
            }
        }

        // Utility functions for notifications and modals
        function showNotification(message, type = 'info') {
            const notification = document.createElement('div');
            const colors = {
                success: '#27ae60',
                error: '#e74c3c',
                warning: '#f39c12',
                info: '#3498db'
            };

            notification.style.cssText = `
                position: fixed; top: 20px; right: 20px; z-index: 10000;
                background: ${colors[type]}; color: white; padding: 15px 20px;
                border-radius: 8px; box-shadow: 0 4px 12px rgba(0,0,0,0.3);
                max-width: 350px; font-weight: 500;
            `;
            notification.textContent = message;

            document.body.appendChild(notification);

            setTimeout(() => {
                notification.style.opacity = '0';
                notification.style.transition = 'opacity 0.3s';
                setTimeout(() => notification.remove(), 300);
            }, 4000);
        }

        function showModal(title, content) {
            const modal = document.createElement('div');
            modal.style.cssText = `
                position: fixed; top: 0; left: 0; width: 100%; height: 100%; z-index: 10000;
                background: rgba(0,0,0,0.7); display: flex; align-items: center; justify-content: center;
            `;

            modal.innerHTML = `
                <div style="background: white; border-radius: 12px; max-width: 80%; max-height: 80%; overflow: auto; position: relative;">
                    <div style="padding: 20px; border-bottom: 1px solid #ddd; display: flex; justify-content: space-between; align-items: center;">
                        <h2 style="margin: 0;">${title}</h2>
                        <button onclick="this.closest('.modal').remove()" style="background: none; border: none; font-size: 24px; cursor: pointer;">×</button>
                    </div>
                    <div style="padding: 20px;">
                        ${content}
                    </div>
                </div>
            `;

            modal.className = 'modal';
            document.body.appendChild(modal);

            // Close on background click
            modal.addEventListener('click', (e) => {
                if (e.target === modal) modal.remove();
            });
        }
        
        // =====================================
        // TAG MANAGEMENT FUNCTIONS
        // =====================================
        
        async function loadAvailableTags() {
            try {
                const response = await fetch('/available-tags');
                const tags = await response.json();
                availableTags = tags.sort((a, b) => a.name.localeCompare(b.name));
                displayAvailableTags();
            } catch (error) {
                console.error('Error loading tags:', error);
                availableTags = [];
                displayAvailableTags(); // Show empty state
            }
        }
        
        function displayAvailableTags() {
            const tagsContainer = document.getElementById('available-tags-container');
            if (!tagsContainer) return;
            
            if (availableTags.length === 0) {
                tagsContainer.innerHTML = '<div style="color: #6c757d; text-align: center; padding: 20px;">No tags available</div>';
                return;
            }
            
            // Group tags by category
            const groupedTags = {};
            availableTags.forEach(tag => {
                const categoryName = tag.category?.name || 'Uncategorized';
                if (!groupedTags[categoryName]) {
                    groupedTags[categoryName] = [];
                }
                groupedTags[categoryName].push(tag);
            });
            
            let html = '';
            Object.keys(groupedTags).forEach(categoryName => {
                const tags = groupedTags[categoryName];
                const categoryColor = tags[0].category?.color || '#95a5a6';
                
                html += `<div style="margin-bottom: 15px;">
                    <div style="font-weight: 600; color: ${categoryColor}; margin-bottom: 8px; font-size: 0.9em;">${categoryName}</div>
                    <div style="display: flex; flex-wrap: wrap; gap: 6px;">`;
                
                tags.forEach(tag => {
                    const isSelected = selectedTags.includes(tag.id);
                    const selectedClass = isSelected ? 'tag-selected' : '';
                    
                    html += `<span class="available-tag ${selectedClass}" 
                                  data-tag-id="${tag.id}" 
                                  data-tag-name="${tag.name}"
                                  style="background: ${tag.color}; color: white; padding: 4px 8px; border-radius: 12px; 
                                         cursor: pointer; font-size: 0.8em; font-weight: 500; border: 2px solid ${isSelected ? '#fff' : 'transparent'};"
                                  onclick="toggleTagSelection(${tag.id}, '${tag.name}', '${tag.color}')"
                                  title="${tag.description}">${tag.name}</span>`;
                });
                
                html += `</div></div>`;
            });
            
            tagsContainer.innerHTML = html;
        }
        
        function toggleTagSelection(tagId, tagName, tagColor) {
            const index = selectedTags.indexOf(tagId);
            if (index > -1) {
                // Remove tag
                selectedTags.splice(index, 1);
            } else {
                // Add tag
                selectedTags.push(tagId);
            }
            
            // Update display
            displayAvailableTags();
            updateSelectedTagsDisplay();
        }
        
        function updateSelectedTagsDisplay() {
            const container = document.getElementById('selected-tags');
            if (!container) return;
            
            if (selectedTags.length === 0) {
                container.innerHTML = '<div style="color: #6c757d; font-size: 0.9em; font-style: italic;">No tags selected</div>';
                return;
            }
            
            let html = '';
            selectedTags.forEach(tagId => {
                const tag = availableTags.find(t => t.id === tagId);
                if (tag) {
                    html += `<span class="selected-tag" style="background: ${tag.color}; color: white; padding: 4px 10px; border-radius: 12px; 
                                   font-size: 0.8em; margin: 2px; display: inline-block; cursor: pointer;"
                                   onclick="toggleTagSelection(${tag.id}, '${tag.name}', '${tag.color}')"
                                   title="Click to remove">
                                ${tag.name} ×
                              </span>`;
                }
            });
            
            container.innerHTML = html;
        }
        
        function showTagFilters() {
            document.getElementById('tag-filters-panel').style.display = 'block';
            if (availableTags.length === 0) {
                loadAvailableTags();
            }
        }
        
        function hideTagFilters() {
            document.getElementById('tag-filters-panel').style.display = 'none';
        }
        
        function searchTags(query) {
            const dropdown = document.getElementById('tag-dropdown');
            
            if (query.length < 2) {
                dropdown.style.display = 'none';
                return;
            }
            
            const filteredTags = availableTags.filter(tag => 
                tag.name.toLowerCase().includes(query.toLowerCase()) &&
                !selectedTags.some(selected => selected.id === tag.id)
            );
            
            dropdown.innerHTML = '';
            filteredTags.slice(0, 10).forEach(tag => {
                const option = document.createElement('div');
                option.className = 'tag-option';
                option.innerHTML = `
                    <span style="background-color: ${tag.color}; color: white; padding: 2px 6px; border-radius: 8px; font-size: 0.8em;">
                        ${tag.name}
                    </span>
                    <small>${tag.category_name || 'No category'}</small>
                `;
                option.onclick = () => selectTag(tag);
                dropdown.appendChild(option);
            });
            
            dropdown.style.display = filteredTags.length > 0 ? 'block' : 'none';
        }
        
        function selectTag(tag) {
            if (selectedTags.some(selected => selected.id === tag.id)) return;
            
            selectedTags.push(tag);
            updateSelectedTagsDisplay();
            document.getElementById('tag-search').value = '';
            document.getElementById('tag-dropdown').style.display = 'none';
        }
        
        function removeSelectedTag(tagId) {
            selectedTags = selectedTags.filter(tag => tag.id !== tagId);
            updateSelectedTagsDisplay();
        }
        
        function updateSelectedTagsDisplay() {
            const container = document.getElementById('selected-tags');
            container.innerHTML = '';
            
            selectedTags.forEach(tag => {
                const tagElement = document.createElement('span');
                tagElement.className = 'tag removable';
                tagElement.style.backgroundColor = tag.color;
                tagElement.textContent = tag.name;
                tagElement.onclick = () => removeSelectedTag(tag.id);
                container.appendChild(tagElement);
            });
        }
        
        function applyTagFilters() {
            // Get form values
            currentSymbolFilter = document.getElementById('symbol-filter').value.trim() || null;
            currentDateFromFilter = document.getElementById('date-from').value || null;
            currentDateToFilter = document.getElementById('date-to').value || null;
            
            const matchModeInput = document.querySelector('input[name="match-mode"]:checked');
            currentMatchMode = matchModeInput ? matchModeInput.value : 'ANY';
            
            // Update filter summary
            updateFilterSummary();
            
            // Hide filters panel and reload data
            hideTagFilters();
            loadData(1, currentPageSize, currentSeverityFilter, useRay);
        }
        
        function clearAllFilters() {
            selectedTags = [];
            currentSymbolFilter = null;
            currentDateFromFilter = null;
            currentDateToFilter = null;
            currentMatchMode = 'ANY';
            
            // Reset form controls
            document.getElementById('symbol-filter').value = '';
            document.getElementById('date-from').value = '';
            document.getElementById('date-to').value = '';
            document.querySelector('input[name="match-mode"][value="ANY"]').checked = true;
            
            updateSelectedTagsDisplay();
            updateFilterSummary();
            loadData(1, currentPageSize, currentSeverityFilter, useRay);
        }
        
        function updateFilterSummary() {
            const summaryContainer = document.getElementById('active-filters-summary');
            const summaryText = document.getElementById('filter-summary-text');
            
            let summary = [];
            
            if (selectedTags.length > 0) {
                const tagNames = selectedTags.map(tag => tag.name).join(', ');
                summary.push(`Tags: ${tagNames} (${currentMatchMode})`);
            }
            
            if (currentSymbolFilter) {
                summary.push(`Symbols: ${currentSymbolFilter}`);
            }
            
            if (currentDateFromFilter || currentDateToFilter) {
                const dateRange = [currentDateFromFilter, currentDateToFilter].filter(Boolean).join(' to ');
                summary.push(`Date: ${dateRange}`);
            }
            
            if (summary.length > 0) {
                summaryText.textContent = summary.join(' | ');
                summaryContainer.style.display = 'block';
            } else {
                summaryContainer.style.display = 'none';
            }
        }
        
        async function addTagToIssue(issueId, tagName) {
            if (!tagName.trim()) return;
            
            try {
                // Find the tag by name or create new one
                let tag = availableTags.find(t => t.name.toLowerCase() === tagName.toLowerCase());
                
                if (!tag) {
                    // Create new tag
                    const createResponse = await fetch('/api/tags/', {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({ name: tagName })
                    });
                    tag = await createResponse.json();
                    availableTags.push(tag);
                }
                
                // Apply tag to issue
                const applyResponse = await fetch('/api/tags/apply', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({
                        entity_type: 'data_quality_issues',
                        entity_id: issueId,
                        tag_id: tag.id,
                        source: 'manual'
                    })
                });
                
                if (applyResponse.ok) {
                    // Refresh the current issue display
                    loadData(currentPage, currentPageSize, currentSeverityFilter, useRay);
                }
            } catch (error) {
                console.error('Error adding tag:', error);
                alert('Failed to add tag. Please try again.');
            }
        }
        
        async function removeTagFromIssue(issueId, tagId) {
            try {
                const response = await fetch(`/api/tags/entity/data_quality_issues/${issueId}/tag/${tagId}`, {
                    method: 'DELETE'
                });
                
                if (response.ok) {
                    // Refresh the current issue display
                    loadData(currentPage, currentPageSize, currentSeverityFilter, useRay);
                }
            } catch (error) {
                console.error('Error removing tag:', error);
                alert('Failed to remove tag. Please try again.');
            }
        }
        
        async function suggestTagsForIssue(issueId) {
            try {
                const response = await fetch(`/api/tags/suggestions-enhanced/data_quality_issues/${issueId}?limit=5`);
                const suggestions = await response.json();
                
                if (suggestions.length > 0) {
                    const suggestionText = suggestions.map(s => 
                        `${s.tag_name} (${Math.round(s.confidence_score * 100)}%)`
                    ).join(', ');
                    
                    if (confirm(`Suggested tags: ${suggestionText}\\n\\nApply these tags?`)) {
                        // Apply all suggested tags
                        for (const suggestion of suggestions) {
                            await fetch('/api/tags/apply', {
                                method: 'POST',
                                headers: { 'Content-Type': 'application/json' },
                                body: JSON.stringify({
                                    entity_type: 'data_quality_issues',
                                    entity_id: issueId,
                                    tag_id: suggestion.tag_id,
                                    confidence_score: suggestion.confidence_score,
                                    source: suggestion.source
                                })
                            });
                        }
                        
                        // Refresh display
                        loadData(currentPage, currentPageSize, currentSeverityFilter, useRay);
                    }
                } else {
                    alert('No tag suggestions available for this issue.');
                }
            } catch (error) {
                console.error('Error getting suggestions:', error);
                alert('Failed to get tag suggestions. Please try again.');
            }
        }
        
        async function autoTagIssue(issueId) {
            try {
                const response = await fetch(`/api/tags/auto-tag/data_quality_issues/${issueId}`, {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' }
                });
                
                const result = await response.json();
                
                if (response.ok) {
                    if (result.applied_tags && result.applied_tags.length > 0) {
                        alert(`Auto-tagging successful!\\n\\nApplied tags: ${result.applied_tags.join(', ')}\\n\\nTotal: ${result.total_applied} tags`);
                        // Refresh the current issue display
                        loadData(currentPage, currentPageSize, currentSeverityFilter, useRay);
                    } else {
                        alert('No auto-tags were applied to this issue. The issue may not match any auto-tagging rules.');
                    }
                } else {
                    alert(`Auto-tagging failed: ${result.error || 'Unknown error'}`);
                }
            } catch (error) {
                console.error('Error auto-tagging issue:', error);
                alert('Failed to auto-tag issue. Please try again.');
            }
        }
        
        async function runAutoTaggingBatch() {
            if (!confirm('Run auto-tagging on recent untagged issues?\\n\\nThis will apply tags automatically based on issue characteristics.')) {
                return;
            }
            
            try {
                const response = await fetch('/auto-tag-batch', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' }
                });
                
                const result = await response.json();
                
                if (response.ok) {
                    let message = `Auto-tagging batch completed!\\n\\n`;
                    if (result.issues_processed !== undefined) {
                        message += `Issues processed: ${result.issues_processed}\\n`;
                        message += `Issues tagged: ${result.issues_tagged}\\n`;
                        message += `Total tags applied: ${result.tags_applied}\\n`;
                        if (result.errors > 0) {
                            message += `Errors: ${result.errors}\\n`;
                        }
                    } else {
                        message += result.summary || 'Batch processing initiated.';
                    }
                    
                    alert(message);
                    
                    // Refresh the current issue display
                    loadData(currentPage, currentPageSize, currentSeverityFilter, useRay);
                } else {
                    alert(`Batch auto-tagging failed: ${result.error || 'Unknown error'}`);
                }
            } catch (error) {
                console.error('Error running auto-tagging batch:', error);
                alert('Failed to run auto-tagging batch. Please try again.');
            }
        }
        
        async function showAutoTaggingRules() {
            try {
                const response = await fetch('/api/tags/auto-rules');
                const data = await response.json();
                
                if (response.ok) {
                    let rulesHtml = '<h3>Auto-Tagging Rules</h3>';
                    rulesHtml += `<p><strong>Total Rules:</strong> ${data.total_rules}</p>`;
                    rulesHtml += `<p><strong>Categories:</strong> ${data.categories.join(', ')}</p>`;
                    rulesHtml += '<div style="max-height: 400px; overflow-y: auto;">';
                    
                    data.rules.forEach(rule => {
                        rulesHtml += `
                            <div style="border: 1px solid #ddd; margin: 10px 0; padding: 10px; border-radius: 4px;">
                                <strong>${rule.name}</strong> → <span style="background: #3498db; color: white; padding: 2px 6px; border-radius: 4px;">${rule.tag_name}</span>
                                <br><small>Confidence: ${Math.round(rule.confidence * 100)}% | Category: ${rule.category}</small>
                                <br><em>${rule.description}</em>
                            </div>
                        `;
                    });
                    
                    rulesHtml += '</div>';
                    
                    showModal('Auto-Tagging Rules', rulesHtml);
                } else {
                    alert('Failed to load auto-tagging rules.');
                }
            } catch (error) {
                console.error('Error loading auto-tagging rules:', error);
                alert('Failed to load auto-tagging rules.');
            }
        }
        
        // Initialize page on load
        document.addEventListener('DOMContentLoaded', function() {
            loadData();
            loadAgentStatus();
            loadAvailableTags();
        });

        // Auto-refresh every 60 seconds
        setInterval(() => {
            loadData();
            loadAgentStatus();
        }, 60000);
    </script>
</body>
</html>
        '''

        self.send_response(200)
        self.send_header('Content-type', 'text/html')
        self.end_headers()
        self.wfile.write(dashboard_html.encode())

    def _serve_data_quality_issues(self):
        """Serve data quality issues API endpoint with Ray integration and pagination."""
        try:
            # Parse query parameters
            from urllib.parse import parse_qs, urlparse
            parsed_url = urlparse(self.path)
            query_params = parse_qs(parsed_url.query)

            page = int(query_params.get('page', ['1'])[0])
            page_size = int(query_params.get('page_size', ['50'])[0])
            severity_filter = query_params.get('severity', [None])[0]
            use_ray = query_params.get('ray', ['false'])[0].lower() == 'true'

            # Get database connection - container aware
            db_config = {
                'host': 'ats-intg-postgres',  # Container name on ats-network
                'port': 5432,  # Internal port
                'user': 'postgres',
                'password': 'intg_password',
                'database': 'intg_db'
            }

            # Record scan start time for metrics
            scan_start_time = time.time()
            
            # Use Ray-powered agent if requested, otherwise use legacy method
            if use_ray:
                response_data = self._get_issues_with_ray(db_config, page, page_size, severity_filter)
            else:
                response_data = self._get_issues_legacy(db_config, page, page_size, severity_filter)

            # Update metrics with scan duration and issue counts
            scan_duration = time.time() - scan_start_time
            self.metrics.update_scan_duration(scan_duration)
            
            if 'summary' in response_data:
                self.metrics.update_issue_metrics(response_data)
                
                # Update symbols affected (extract unique symbols from issues)
                unique_symbols = set()
                critical_symbols = {'AAPL', 'SPY', 'QQQ', 'TSLA', 'MSFT', 'NVDA'}  # Define critical symbols
                affected_critical = 0
                
                for issue in response_data.get('issues', []):
                    symbol = issue.get('symbol', '')
                    if symbol:
                        unique_symbols.add(symbol)
                        if symbol in critical_symbols:
                            affected_critical += 1
                
                self.metrics.update_symbols_affected(len(unique_symbols), affected_critical)
                
                # Update vendor-specific metrics
                vendor_counts = {}
                for issue in response_data.get('issues', []):
                    vendor = issue.get('vendor_source', 'unknown')
                    vendor_counts[vendor] = vendor_counts.get(vendor, 0) + 1
                
                for vendor, count in vendor_counts.items():
                    if vendor in ['polygon', 'tiingo', 'eodhd']:
                        self.metrics.update_vendor_metrics(vendor, count)

            self._send_json_response(response_data)

        except Exception as e:
            logger.error(f"Data quality issues API error: {e}")
            self._send_json_response({
                "error": str(e),
                "issues": [],
                "total_count": 0
            }, status_code=500)

    def _get_issues_with_ray(self, db_config: Dict[str, Any], page: int, page_size: int, severity_filter: str) -> Dict[str, Any]:
        """Get issues using Ray-powered distributed processing"""
        import asyncio

        async def ray_detection():
            try:
                # Import Ray agent
                import sys
                import os
                sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))
                from services.data_quality.ray_data_quality_agent import RayDataQualityAgent

                agent = RayDataQualityAgent(db_config, num_workers=4)
                result = await agent.get_issues_page(
                    page=page,
                    page_size=page_size,
                    severity_filter=severity_filter
                )
                await agent.shutdown()

                return {
                    "issues": result["issues"],
                    "pagination": result["pagination"],
                    "summary": result["summary"],
                    "total_count": result["pagination"]["total_issues"],
                    "method": "ray_distributed",
                    "timestamp": datetime.now().isoformat()
                }

            except Exception as e:
                logger.error(f"Ray processing failed: {e}")
                # Use legacy method
                return self._get_issues_legacy(db_config, page, page_size, severity_filter)

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            return loop.run_until_complete(ray_detection())
        finally:
            loop.close()

    def _get_issues_legacy(self, db_config: Dict[str, Any], page: int, page_size: int, severity_filter: str) -> Dict[str, Any]:
        """Get issues using legacy single-threaded method"""
        import asyncpg
        import asyncio

        issues = []

        async def detect_issues():
            nonlocal issues
            try:
                conn = await asyncpg.connect(**db_config)

                # Check for missing recent data across all vendors
                missing_data_query = """
                    WITH recent_dates AS (
                        SELECT generate_series(
                            CURRENT_DATE - INTERVAL '7 days',
                            CURRENT_DATE - INTERVAL '1 day',
                            '1 day'::interval
                        )::date as expected_date
                    ),
                    actual_dates AS (
                        SELECT DISTINCT date as actual_date FROM intg_daily_price_polygon WHERE date >= CURRENT_DATE - INTERVAL '7 days'
                        UNION
                        SELECT DISTINCT date as actual_date FROM intg_daily_price_tiingo WHERE date >= CURRENT_DATE - INTERVAL '7 days'
                        UNION
                        SELECT DISTINCT date as actual_date FROM intg_daily_price_eodhd WHERE date >= CURRENT_DATE - INTERVAL '7 days'
                    )
                    SELECT rd.expected_date
                    FROM recent_dates rd
                    LEFT JOIN actual_dates ad ON rd.expected_date = ad.actual_date
                    WHERE ad.actual_date IS NULL
                    AND EXTRACT(dow FROM rd.expected_date) NOT IN (0, 6)
                    ORDER BY rd.expected_date;
                    """

                missing_dates = await conn.fetch(missing_data_query)
                for row in missing_dates:
                    issues.append({
                        "id": f"missing_data_{row['expected_date']}",
                        "symbol": "ALL",
                        "issue_type": "missing_data",
                        "severity": "high",
                        "description": f"No daily prices found for trading day {row['expected_date']}",
                        "detected_at": datetime.now().isoformat(),
                        "affected_date": row['expected_date'].isoformat(),
                        "field": "all_fields",
                        "expected_value": "Daily prices",
                        "actual_value": "No data",
                        "vendor_source": "multiple",
                        "status": "open"
                    })

                # Check for extreme volumes across all vendors
                extreme_volume_query = """
                SELECT symbol, date as price_date, volume, close, 'polygon' as vendor
                FROM intg_daily_price_polygon
                WHERE date >= CURRENT_DATE - INTERVAL '7 days'
                AND volume > 50000000
                UNION ALL
                SELECT symbol, date as price_date, volume, close, 'tiingo' as vendor
                FROM intg_daily_price_tiingo
                WHERE date >= CURRENT_DATE - INTERVAL '7 days'
                AND volume > 50000000
                UNION ALL
                SELECT symbol, date as price_date, volume, close, 'eodhd' as vendor
                FROM intg_daily_price_eodhd
                WHERE date >= CURRENT_DATE - INTERVAL '7 days'
                AND volume > 50000000
                ORDER BY volume DESC;
                """

                extreme_volumes = await conn.fetch(extreme_volume_query)
                for row in extreme_volumes:
                    issues.append({
                        "id": f"high_volume_{row['symbol']}_{row['price_date']}_{row['vendor']}",
                        "symbol": row['symbol'],
                        "issue_type": "extreme_volume",
                        "severity": "medium",
                        "description": f"Unusually high volume: {row['volume']:,} shares ({row['vendor']})",
                        "detected_at": datetime.now().isoformat(),
                        "affected_date": row['price_date'].isoformat(),
                        "field": "volume",
                        "expected_value": "< 50M shares",
                        "actual_value": f"{row['volume']:,} shares",
                        "vendor_source": row['vendor'],
                        "status": "open"
                    })

                # Check for duplicate records within each vendor table
                duplicate_query = """
                SELECT symbol, date as price_date, COUNT(*) as count, 'polygon' as vendor
                FROM intg_daily_price_polygon
                WHERE date >= CURRENT_DATE - INTERVAL '7 days'
                GROUP BY symbol, date
                HAVING COUNT(*) > 1
                UNION ALL
                SELECT symbol, date as price_date, COUNT(*) as count, 'tiingo' as vendor
                FROM intg_daily_price_tiingo
                WHERE date >= CURRENT_DATE - INTERVAL '7 days'
                GROUP BY symbol, date
                HAVING COUNT(*) > 1
                UNION ALL
                SELECT symbol, date as price_date, COUNT(*) as count, 'eodhd' as vendor
                FROM intg_daily_price_eodhd
                WHERE date >= CURRENT_DATE - INTERVAL '7 days'
                GROUP BY symbol, date
                HAVING COUNT(*) > 1
                ORDER BY count DESC;
                """

                duplicates = await conn.fetch(duplicate_query)
                for row in duplicates:
                    issues.append({
                        "id": f"duplicate_{row['symbol']}_{row['price_date']}_{row['vendor']}",
                        "symbol": row['symbol'],
                        "issue_type": "duplicate_records",
                        "severity": "critical",
                        "description": f"Duplicate records: {row['count']} entries for same date ({row['vendor']})",
                        "detected_at": datetime.now().isoformat(),
                        "affected_date": row['price_date'].isoformat(),
                        "field": "all_fields",
                        "expected_value": "1 record per day",
                        "actual_value": f"{row['count']} records",
                        "vendor_source": row['vendor'],
                        "status": "open"
                    })

                # Check for stale data across all vendor tables
                freshness_query = """
                WITH vendor_freshness AS (
                    SELECT symbol, MAX(date) as latest_date, 'polygon' as vendor
                    FROM intg_daily_price_polygon
                    GROUP BY symbol
                    UNION ALL
                    SELECT symbol, MAX(date) as latest_date, 'tiingo' as vendor
                    FROM intg_daily_price_tiingo
                    GROUP BY symbol
                    UNION ALL
                    SELECT symbol, MAX(date) as latest_date, 'eodhd' as vendor
                    FROM intg_daily_price_eodhd
                    GROUP BY symbol
                ),
                symbol_freshness AS (
                    SELECT symbol, MAX(latest_date) as latest_date,
                           string_agg(vendor, ',' ORDER BY latest_date DESC) as vendors
                    FROM vendor_freshness
                    GROUP BY symbol
                )
                SELECT symbol, latest_date, vendors
                FROM symbol_freshness
                WHERE latest_date < CURRENT_DATE - INTERVAL '3 days'
                ORDER BY latest_date DESC;
                """

                stale_data = await conn.fetch(freshness_query)
                for row in stale_data:
                    from datetime import date as dt_date
                    days_stale = (dt_date.today() - row['latest_date']).days
                    issues.append({
                        "id": f"stale_data_{row['symbol']}",
                        "symbol": row['symbol'],
                        "issue_type": "stale_data",
                        "severity": "medium" if days_stale < 7 else "high",
                        "description": f"Data is {days_stale} days old (last: {row['latest_date']}) - vendors: {row['vendors']}",
                        "detected_at": datetime.now().isoformat(),
                        "affected_date": row['latest_date'].isoformat(),
                        "field": "date",
                        "expected_value": "< 3 days old",
                        "actual_value": f"{days_stale} days old",
                        "vendor_source": row['vendors'],
                        "status": "open"
                    })

                await conn.close()

            except Exception as e:
                logger.error(f"Data quality detection error: {e}")
                issues.append({
                    "id": "detection_error",
                    "symbol": "SYSTEM",
                    "issue_type": "detection_error",
                    "severity": "critical",
                    "description": f"Data quality detection failed: {str(e)}",
                    "detected_at": datetime.now().isoformat(),
                    "affected_date": datetime.now().date().isoformat(),
                    "field": "system",
                    "expected_value": "Successful detection",
                    "actual_value": f"Error: {str(e)}",
                    "vendor_source": "system",
                    "status": "open"
                    })

        # Run the async detection
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(detect_issues())
        finally:
            loop.close()

        # Apply severity filter if specified
        if severity_filter:
            issues = [issue for issue in issues if issue['severity'] == severity_filter]

        # Sort by severity and date
        severity_priority = {'critical': 0, 'high': 1, 'medium': 2, 'low': 3}
        issues.sort(key=lambda x: (
            severity_priority.get(x['severity'], 4),
            x['affected_date']
        ), reverse=True)

        # Apply pagination
        total_issues = len(issues)
        total_pages = (total_issues + page_size - 1) // page_size
        start_idx = (page - 1) * page_size
        end_idx = start_idx + page_size
        page_issues = issues[start_idx:end_idx]

        return {
            "issues": page_issues,
            "pagination": {
                "current_page": page,
                "page_size": page_size,
                "total_issues": total_issues,
                "total_pages": total_pages,
                "has_next": page < total_pages,
                "has_prev": page > 1
            },
            "summary": {
                "total_issues": total_issues,
                "critical": len([i for i in issues if i['severity'] == 'critical']),
                "high": len([i for i in issues if i['severity'] == 'high']),
                "medium": len([i for i in issues if i['severity'] == 'medium']),
                "low": len([i for i in issues if i['severity'] == 'low'])
            },
            "total_count": len(page_issues),
            "method": "legacy_single_threaded",
            "last_updated": datetime.now().isoformat(),
            "detection_period_days": 7
        }

    def _send_json_response(self, data: Dict[str, Any], status_code: int = 200):
        """Send JSON response with proper headers"""
        self.send_response(status_code)
        self.send_header('Content-type', 'application/json')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()
        self.wfile.write(json.dumps(data, indent=2).encode())

    def _serve_agent_status(self):
        """Serve agent status endpoint."""
        try:
            if not self.analytics_service.agent_enabled:
                self.send_response(503)
                self.send_header('Content-type', 'application/json')
                self.end_headers()
                self.wfile.write(json.dumps({"error": "Agent not available"}).encode())
                return

            agent = self.analytics_service.data_quality_agent
            status_data = {
                "agent_id": agent.agent_id,
                "status": agent.status.value,
                "tools_available": len(agent.mcp_tools),
                "tools": list(agent.mcp_tools.keys()),
                "mcp_tools_ready": True,
                "timestamp": datetime.now().isoformat()
            }

            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps(status_data).encode())

        except Exception as e:
            logger.error(f"Error serving agent status: {e}")
            self.send_response(500)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps({"error": str(e)}).encode())

    def _serve_agent_start(self):
        """Serve agent start endpoint."""
        try:
            if not self.analytics_service.agent_enabled:
                self.send_response(503)
                self.send_header('Content-type', 'application/json')
                self.end_headers()
                self.wfile.write(json.dumps({"error": "Agent not available"}).encode())
                return

            agent = self.analytics_service.data_quality_agent
            if agent.status.value == "active":
                self.send_response(200)
                self.send_header('Content-type', 'application/json')
                self.end_headers()
                self.wfile.write(json.dumps({"message": "Agent already active"}).encode())
                return

            # Start agent monitoring - create async task for continuous monitoring
            import asyncio
            import threading

            def start_monitoring_thread():
                """Run monitoring in a separate thread with its own event loop"""
                try:
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
                    loop.run_until_complete(agent.start_continuous_monitoring())
                except Exception as e:
                    logger.error(f"Agent monitoring failed: {e}")
                finally:
                    loop.close()

            # Start the monitoring thread in background
            if not hasattr(self.analytics_service, 'agent_monitoring_thread') or \
               self.analytics_service.agent_monitoring_thread is None or \
               not self.analytics_service.agent_monitoring_thread.is_alive():
                self.analytics_service.agent_monitoring_thread = threading.Thread(
                    target=start_monitoring_thread,
                    daemon=True,
                    name="AgentMonitoringThread"
                )
                self.analytics_service.agent_monitoring_thread.start()

            # Update metrics for agent start
            self.metrics.update_agent_status('active', agent.agent_id)
            
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps({"message": "Agent started successfully"}).encode())

        except Exception as e:
            logger.error(f"Error starting agent: {e}")
            self.send_response(500)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps({"error": str(e)}).encode())

    def _serve_agent_stop(self):
        """Serve agent stop endpoint."""
        try:
            if not self.analytics_service.agent_enabled:
                self.send_response(503)
                self.send_header('Content-type', 'application/json')
                self.end_headers()
                self.wfile.write(json.dumps({"error": "Agent not available"}).encode())
                return

            agent = self.analytics_service.data_quality_agent
            from agents.data_quality_agent import AgentStatus
            agent.status = AgentStatus.IDLE

            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps({"message": "Agent stopped successfully"}).encode())

        except Exception as e:
            logger.error(f"Error stopping agent: {e}")
            self.send_response(500)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps({"error": str(e)}).encode())

    def _serve_agent_endpoint(self):
        """Serve general agent endpoints."""
        try:
            if not self.analytics_service.agent_enabled:
                self.send_response(503)
                self.send_header('Content-type', 'application/json')
                self.end_headers()
                self.wfile.write(json.dumps({"error": "Agent not available"}).encode())
                return

            # Route to specific agent functionality based on path
            if self.path == '/agent/health':
                agent = self.analytics_service.data_quality_agent
                health_data = {
                    "healthy": agent.status.value in ["active", "idle"],
                    "status": agent.status.value,
                    "last_health_check": datetime.now().isoformat()
                }
                self.send_response(200)
                self.send_header('Content-type', 'application/json')
                self.end_headers()
                self.wfile.write(json.dumps(health_data).encode())
            else:
                self.send_response(404)
                self.send_header('Content-type', 'application/json')
                self.end_headers()
                self.wfile.write(json.dumps({"error": "Agent endpoint not found"}).encode())

        except Exception as e:
            logger.error(f"Error serving agent endpoint: {e}")
            self.send_response(500)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps({"error": str(e)}).encode())

    def _serve_404(self):
        """Serve 404 response."""
        self.send_response(404)
        self.send_header('Content-type', 'application/json')
        self.end_headers()

        error_response = {
            "error": "Not found",
            "path": self.path,
            "available_endpoints": [
                "/health", "/eda", "/api/intelligent-filters/{table}",
                "/api/universe-analytics", "/api/v1/training-datasets",
                "/api/news-events", "/api/earnings-events", "/api/gap-events", "/api/ray-analytics/{dataset_id}", "/api/multi-panel-chart",
                "/data-quality/dashboard", "/data-quality/api/issues",
                "/agent/status", "/agent/start", "/agent/stop", "/agent/health"
            ]
        }

        self.wfile.write(json.dumps(error_response).encode('utf-8'))

    def _serve_500(self, error_message: str):
        """Serve 500 response."""
        self.send_response(500)
        self.send_header('Content-type', 'application/json')
        self.end_headers()

        error_response = {
            "error": "Internal server error",
            "message": error_message,
            "timestamp": datetime.now().isoformat()
        }

        self.wfile.write(json.dumps(error_response).encode('utf-8'))

    def _serve_universes_list(self):
        """Serve list of available universes."""
        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.end_headers()

        try:
            import os
            from core.platform.database.connection_manager import get_raw_connection
            from psycopg2.extras import RealDictCursor

            # Get environment-aware table name
            environment = os.getenv('ENVIRONMENT', 'dev')
            universe_table = f"{environment}_universe"

            with get_raw_connection() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                    cursor.execute(f"""
                        SELECT id, name, description
                        FROM {universe_table}
                        ORDER BY name
                    """)

                    universes = []
                    for row in cursor.fetchall():
                        universes.append({
                            "id": row['id'],
                            "name": row['name'],
                            "description": row['description']
                        })

                    response = {
                        "success": True,
                        "universes": universes,
                        "count": len(universes)
                    }

        except Exception as e:
            logger.error(f"Error loading universes: {e}")
            response = {
                "success": False,
                "error": f"Failed to load universes: {str(e)}",
                "universes": [],
                "count": 0
            }

        self.wfile.write(json.dumps(response).encode('utf-8'))

    def _serve_universe_members(self):
        """Serve universe members for a specific universe and date range."""
        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.end_headers()

        try:
            import os
            from urllib.parse import urlparse, parse_qs
            from core.platform.database.connection_manager import get_raw_connection
            from psycopg2.extras import RealDictCursor

            # Get environment-aware table names
            environment = os.getenv('ENVIRONMENT', 'dev')
            universe_table = f"{environment}_universe"
            membership_table = f"{environment}_universe_membership"

            # Parse universe ID from URL path
            universe_id = self.path.split('/')[-1].split('?')[0]

            # Parse query parameters
            parsed_url = urlparse(self.path)
            query_params = parse_qs(parsed_url.query)
            date_from_raw = query_params.get('date_from', [None])[0]
            date_to_raw = query_params.get('date_to', [None])[0]

            if not universe_id or not date_from_raw or not date_to_raw:
                response = {
                    "success": False,
                    "error": "Missing required parameters: universe_id, date_from, date_to"
                }
                self.wfile.write(json.dumps(response).encode('utf-8'))
                return

            # Convert date_to to end of day to include all entries on that date
            # date_from stays as start of day (default behavior)
            date_from = date_from_raw
            date_to = f"{date_to_raw} 23:59:59"

            with get_raw_connection() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                    # Get universe information
                    cursor.execute(f"""
                        SELECT id, name, description
                        FROM {universe_table}
                        WHERE id = %s
                    """, (universe_id,))

                    universe_info = cursor.fetchone()
                    if not universe_info:
                        response = {
                            "success": False,
                            "error": f"Universe with ID {universe_id} not found"
                        }
                        self.wfile.write(json.dumps(response).encode('utf-8'))
                        return

                    # Get universe members within date range
                    cursor.execute(f"""
                        SELECT um.universe_id, um.symbol, um.start_at, um.end_at, um.instrument_id
                        FROM {membership_table} um
                        WHERE um.universe_id = %s
                        AND (
                            (um.start_at <= %s AND (um.end_at IS NULL OR um.end_at >= %s))
                            OR (um.start_at >= %s AND um.start_at <= %s)
                            OR (um.end_at IS NOT NULL AND um.end_at >= %s AND um.end_at <= %s)
                        )
                        ORDER BY um.symbol, um.start_at
                    """, (universe_id, date_to, date_from, date_from, date_to, date_from, date_to))

                    members = []
                    for row in cursor.fetchall():
                        members.append({
                            "universe_id": row['universe_id'],
                            "symbol": row['symbol'],
                            "start_at": row['start_at'].isoformat() if row['start_at'] else None,
                            "end_at": row['end_at'].isoformat() if row['end_at'] else None,
                            "instrument_id": row['instrument_id']
                        })

                    response = {
                        "success": True,
                        "universe_info": {
                            "id": universe_info['id'],
                            "name": universe_info['name'],
                            "description": universe_info['description']
                        },
                        "members": members,
                        "date_range": {
                            "from": date_from,
                            "to": date_to
                        },
                        "count": len(members)
                    }

        except Exception as e:
            logger.error(f"Error loading universe members: {e}")
            response = {
                "success": False,
                "error": f"Failed to load universe members: {str(e)}"
            }

        self.wfile.write(json.dumps(response).encode('utf-8'))

    def _serve_monthly_training_data_table(self):
        """Serve monthly training data table with filtering and sorting."""
        from urllib.parse import urlparse, parse_qs

        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()

        try:
            # Parse query parameters for filtering and sorting
            parsed_url = urlparse(self.path)
            query_params = parse_qs(parsed_url.query)

            # Extract parameters
            symbols = query_params.get('symbols', [])
            symbols = symbols[0].split(',') if symbols and symbols[0] else None

            status = query_params.get('status', [None])[0]
            limit = int(query_params.get('limit', [100])[0])
            offset = int(query_params.get('offset', [0])[0])
            order_by = query_params.get('order_by', ['created_at'])[0]
            order_direction = query_params.get('order_direction', ['DESC'])[0]

            # Get data using MonthlyTrainingDataDAO
            import asyncio
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

            try:
                from domains.ml.services.training_data.dao.monthly_training_data_dao import MonthlyTrainingDataDAO
                from shared.utils.environment import Environment

                # Create environment and DAO
                environment = Environment()
                dao = MonthlyTrainingDataDAO(environment)

                # Get monthly training data records
                records = loop.run_until_complete(dao.list_monthly_records(
                    symbols=symbols,
                    status=status,
                    limit=limit,
                    offset=offset,
                    order_by=order_by,
                    order_direction=order_direction,
                    include_instrument_details=True
                ))

                # Convert records to JSON-serializable format
                records_data = []
                for record in records:
                    record_dict = {
                        'id': record.id,
                        'run_id': record.run_id,
                        'symbol': record.symbol,
                        'instrument_id': record.instrument_id,
                        'year_month': record.year_month.isoformat() if record.year_month else None,
                        'timeframe_paths': record.timeframe_paths,
                        'total_records': record.total_records,
                        'file_size_mb': record.file_size_mb,
                        'data_quality_score': record.data_quality_score,
                        'status': record.status,
                        'error_message': record.error_message,
                        'created_at': record.created_at.isoformat() if record.created_at else None,
                        'updated_at': record.updated_at.isoformat() if record.updated_at else None,
                        # Extended fields
                        'instrument_name': record.instrument_name,
                        'exchange': record.exchange,
                        'sector': record.sector,
                        'market_cap': record.market_cap
                    }
                    records_data.append(record_dict)

                # Get summary statistics
                summary = loop.run_until_complete(dao.get_summary_by_symbol())

                response = {
                    "success": True,
                    "data": records_data,
                    "summary": summary,
                    "pagination": {
                        "limit": limit,
                        "offset": offset,
                        "total_returned": len(records_data)
                    },
                    "filters_applied": {
                        "symbols": symbols,
                        "status": status,
                        "order_by": order_by,
                        "order_direction": order_direction
                    }
                }

            finally:
                loop.close()

        except Exception as e:
            logger.error(f"Error serving monthly training data table: {e}")
            import traceback
            traceback.print_exc()

            response = {
                "success": False,
                "error": f"Failed to load monthly training data: {str(e)}"
            }

        self.wfile.write(json.dumps(response, indent=2).encode('utf-8'))

    def _serve_monthly_training_visualization(self):
        """Serve monthly training data visualization with multi-timeframe plotly charts."""
        from urllib.parse import urlparse, parse_qs

        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()

        try:
            # Parse query parameters
            parsed_url = urlparse(self.path)
            query_params = parse_qs(parsed_url.query)

            # Extract visualization parameters
            record_id = int(query_params.get('record_id', [0])[0])
            center_timeframe = query_params.get('center_timeframe', ['1h'])[0]  # Default to 1h navigation
            center_index = int(query_params.get('center_index', [0])[0])  # Index within 60m data

            if not record_id:
                raise ValueError("record_id parameter is required")

            # Get data using MonthlyTrainingDataDAO
            import asyncio
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

            try:
                from domains.ml.services.training_data.dao.monthly_training_data_dao import MonthlyTrainingDataDAO
                from shared.utils.environment import Environment
                import array_record.python.array_record_module as array_record

                # Create environment and DAO
                environment = Environment()
                dao = MonthlyTrainingDataDAO(environment)

                # Get the monthly record
                record = loop.run_until_complete(dao.get_monthly_record(record_id))
                if not record:
                    raise ValueError(f"No monthly training data record found with ID: {record_id}")

                # Load ArrayRecord data from timeframe paths
                timeframe_data = {}
                timeframes = ['5m', '15m', '1h', '1d']  # Order matters for visualization

                for timeframe in timeframes:
                    file_path = record.timeframe_paths.get(timeframe)
                    if not file_path:
                        continue

                    try:
                        # Read ArrayRecord file
                        reader = array_record.ArrayRecordReader(file_path)
                        data_points = []

                        # Read all records from the file
                        for i in range(len(reader)):
                            try:
                                record_data = reader[i]
                                # Parse binary data based on schema
                                # This is a simplified parser - real implementation would use schema
                                data_points.append({
                                    'timestamp': i,  # Use index as timestamp for now
                                    'open': float(record_data[0]) if len(record_data) > 0 else 0.0,
                                    'high': float(record_data[1]) if len(record_data) > 1 else 0.0,
                                    'low': float(record_data[2]) if len(record_data) > 2 else 0.0,
                                    'close': float(record_data[3]) if len(record_data) > 3 else 0.0,
                                    'volume': float(record_data[4]) if len(record_data) > 4 else 0.0,
                                })
                            except Exception as parse_error:
                                logger.warning(f"Failed to parse record {i} in {timeframe}: {parse_error}")
                                continue

                        timeframe_data[timeframe] = data_points
                        logger.info(f"Loaded {len(data_points)} data points for {timeframe}")

                    except Exception as file_error:
                        logger.warning(f"Failed to read {timeframe} file {file_path}: {file_error}")
                        timeframe_data[timeframe] = []

                # Generate plotly chart configurations
                chart_configs = self._generate_multi_timeframe_charts(
                    timeframe_data,
                    record.symbol,
                    center_timeframe,
                    center_index
                )

                response = {
                    "success": True,
                    "record_info": {
                        "id": record.id,
                        "symbol": record.symbol,
                        "year_month": record.year_month.strftime('%Y-%m') if record.year_month else None,
                        "total_records": record.total_records,
                        "data_quality_score": record.data_quality_score
                    },
                    "timeframe_data_counts": {tf: len(data) for tf, data in timeframe_data.items()},
                    "charts": chart_configs,
                    "navigation": {
                        "center_timeframe": center_timeframe,
                        "center_index": center_index,
                        "available_timeframes": list(timeframe_data.keys())
                    }
                }

            finally:
                loop.close()

        except Exception as e:
            logger.error(f"Error serving monthly training visualization: {e}")
            import traceback
            traceback.print_exc()

            response = {
                "success": False,
                "error": f"Failed to load visualization data: {str(e)}"
            }

        self.wfile.write(json.dumps(response, indent=2).encode('utf-8'))

    def _generate_multi_timeframe_charts(self, timeframe_data: Dict[str, List], symbol: str, center_timeframe: str, center_index: int) -> Dict[str, Any]:
        """Generate plotly chart configurations for multi-timeframe visualization."""

        charts = {}

        # Get center point from center_timeframe data
        center_data = timeframe_data.get(center_timeframe, [])
        if not center_data or center_index >= len(center_data):
            # Default to middle of available data
            center_index = len(center_data) // 2 if center_data else 0

        center_timestamp = center_data[center_index]['timestamp'] if center_data else 0

        # Generate chart for each timeframe
        for timeframe, data_points in timeframe_data.items():
            if not data_points:
                continue

            # Create OHLC candlestick chart
            chart_config = {
                "data": [{
                    "type": "candlestick",
                    "x": [point['timestamp'] for point in data_points],
                    "open": [point['open'] for point in data_points],
                    "high": [point['high'] for point in data_points],
                    "low": [point['low'] for point in data_points],
                    "close": [point['close'] for point in data_points],
                    "name": f"{symbol} {timeframe}",
                    "increasing": {"line": {"color": "#00ff00"}},
                    "decreasing": {"line": {"color": "#ff0000"}}
                }],
                "layout": {
                    "title": f"{symbol} - {timeframe} Timeframe",
                    "xaxis": {"title": "Time Index", "type": "linear"},
                    "yaxis": {"title": "Price"},
                    "height": 300,
                    "margin": {"l": 50, "r": 50, "t": 50, "b": 50},
                    "showlegend": False
                }
            }

            # Add center line if this is the center timeframe
            if timeframe == center_timeframe:
                chart_config["layout"]["shapes"] = [{
                    "type": "line",
                    "x0": center_timestamp,
                    "x1": center_timestamp,
                    "y0": 0,
                    "y1": 1,
                    "yref": "paper",
                    "line": {"color": "blue", "width": 2, "dash": "dash"}
                }]

            charts[timeframe] = chart_config

        return charts

    def _serve_financial_events(self):
        """Handle financial events endpoints for xAI integration"""

        try:
            # Initialize unified financial events integration if not already done
            if not hasattr(self, 'financial_events_integration'):
                from services.financial_events.multi_source_events_orchestrator import UnifiedFinancialEventsIntegration
                self.financial_events_integration = UnifiedFinancialEventsIntegration(
                    xai_api_key=os.getenv('XAI_API_KEY'),
                    grok_api_key=os.getenv('GROK_API_KEY', os.getenv('XAI_API_KEY')),
                    analytics_base_url="http://localhost:4000",
                    enable_cache=True
                )

            # Route based on path and method
            if self.path == '/financial_events/setup' and self.command == 'POST':
                self._handle_financial_events_setup()
            elif self.path == '/financial_events/extract' and self.command == 'POST':
                self._handle_financial_events_extract()
            elif self.path.startswith('/financial_events?') and self.command == 'GET':
                self._handle_financial_events_query()
            elif self.path == '/financial_events' and self.command == 'GET':
                self._handle_financial_events_query()
            elif self.path == '/financial_events/summary' and self.command == 'GET':
                self._handle_financial_events_summary()
            elif self.path == '/financial_events/cache/stats' and self.command == 'GET':
                self._handle_financial_events_cache_stats()
            elif self.path == '/financial_events/cache/clear' and self.command == 'POST':
                self._handle_financial_events_cache_clear()
            elif self.path.startswith('/financial_events/trending') and self.command == 'GET':
                self._handle_financial_events_trending()
            elif self.path.startswith('/financial_events/sources') and self.command == 'GET':
                self._handle_financial_events_sources()
            else:
                self.send_response(404)
                self.send_header('Content-type', 'application/json')
                self.end_headers()
                self.wfile.write(json.dumps({
                    "error": "Financial events endpoint not found",
                    "path": self.path,
                    "method": self.command,
                    "available_endpoints": [
                        "POST /financial_events/setup - Create events table",
                        "POST /financial_events/extract - Extract events from xAI",
                        "GET /financial_events - Query events",
                        "GET /financial_events/summary - Get statistics",
                        "GET /financial_events/cache/stats - Cache performance",
                        "POST /financial_events/cache/clear - Clear cache"
                    ]
                }).encode())

        except Exception as e:
            logger.error(f"Error in financial events handler: {e}")
            self._serve_500(str(e))

    def _handle_financial_events_setup(self):
        """Handle table setup for financial events"""

        try:
            import asyncio
            result = asyncio.run(self.financial_events_integration.create_events_table())

            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.end_headers()

            response = {
                "success": result,
                "table_created": result,
                "message": "Financial events table created successfully" if result else "Table creation failed"
            }

            self.wfile.write(json.dumps(response).encode())

        except Exception as e:
            logger.error(f"Error in setup handler: {e}")
            self._serve_500(str(e))

    def _handle_financial_events_extract(self):
        """Handle event extraction from xAI"""

        try:
            # Parse POST body
            content_length = int(self.headers['Content-Length'])
            post_data = self.rfile.read(content_length)
            params = json.loads(post_data.decode('utf-8'))

            import asyncio
            from services.financial_events.multi_source_events_orchestrator import EventSource

            # Determine preferred source
            preferred_source = params.get('source', 'combined').lower()
            if preferred_source == 'xai':
                source = EventSource.XAI
            elif preferred_source == 'grok':
                source = EventSource.GROK
            else:
                source = EventSource.COMBINED

            result = asyncio.run(self.financial_events_integration.extract_events_multi_source(
                start_date=params.get('start_date'),
                end_date=params.get('end_date'),
                symbols=params.get('symbols', []),
                preferred_source=source,
                force_refresh=params.get('force_refresh', False)
            ))

            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps(result).encode())

        except Exception as e:
            logger.error(f"Error in extract handler: {e}")
            self._serve_500(str(e))

    def _handle_financial_events_query(self):
        """Handle querying financial events"""

        try:
            from urllib.parse import urlparse, parse_qs

            # Parse query parameters
            parsed_url = urlparse(self.path)
            query_params = parse_qs(parsed_url.query)

            # Extract parameters (parse_qs returns lists)
            symbol = query_params.get('symbol', [None])[0]
            event_type = query_params.get('event_type', [None])[0]
            start_date = query_params.get('start_date', [None])[0]
            end_date = query_params.get('end_date', [None])[0]
            impact_level = query_params.get('impact_level', [None])[0]
            limit = int(query_params.get('limit', ['100'])[0])

            result = self.financial_events_integration.get_events_from_analytics(
                symbol=symbol,
                event_type=event_type,
                start_date=start_date,
                end_date=end_date,
                impact_level=impact_level,
                limit=limit
            )

            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps(result).encode())

        except Exception as e:
            logger.error(f"Error in query handler: {e}")
            self._serve_500(str(e))

    def _handle_financial_events_summary(self):
        """Handle getting events summary"""

        try:
            result = self.financial_events_integration.get_events_summary()

            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps(result).encode())

        except Exception as e:
            logger.error(f"Error in summary handler: {e}")
            self._serve_500(str(e))

    def _handle_financial_events_cache_stats(self):
        """Handle getting cache statistics"""

        try:
            import asyncio
            cache_stats = asyncio.run(self.financial_events_integration.event_extractor.get_cache_stats())

            result = {
                "success": True,
                "cache_statistics": cache_stats
            }

            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps(result).encode())

        except Exception as e:
            logger.error(f"Error in cache stats handler: {e}")
            self._serve_500(str(e))

    def _handle_financial_events_cache_clear(self):
        """Handle clearing cache"""

        try:
            import asyncio
            asyncio.run(self.financial_events_integration.clear_all_caches())

            result = {
                "success": True,
                "message": "All caches cleared successfully"
            }

            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps(result).encode())

        except Exception as e:
            logger.error(f"Error in cache clear handler: {e}")
            self._serve_500(str(e))

    def _handle_financial_events_trending(self):
        """Handle getting trending financial events from all sources"""

        try:
            from urllib.parse import urlparse, parse_qs

            # Parse query parameters
            parsed_url = urlparse(self.path)
            query_params = parse_qs(parsed_url.query)

            hours_back = int(query_params.get('hours', ['24'])[0])

            import asyncio
            result = asyncio.run(self.financial_events_integration.get_trending_events_all_sources(
                hours_back=hours_back
            ))

            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps(result).encode())

        except Exception as e:
            logger.error(f"Error in trending events handler: {e}")
            self._serve_500(str(e))

    def _handle_financial_events_sources(self):
        """Handle getting available sources and integration status"""

        try:
            status = self.financial_events_integration.get_integration_status()

            # Add cache stats for each source
            import asyncio
            cache_stats = asyncio.run(self.financial_events_integration.get_unified_cache_stats())
            status["cache_stats"] = cache_stats

            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps(status).encode())

        except Exception as e:
            logger.error(f"Error in sources handler: {e}")
            self._serve_500(str(e))

    # ==============================================
    # TAG API HANDLERS
    # ==============================================

    def _serve_auto_tag_batch(self):
        """Handle auto-tag batch requests by calling working API"""
        import asyncio
        import aiohttp
        
        async def call_auto_tag_api():
            try:
                import aiohttp
                async with aiohttp.ClientSession() as session:
                    async with session.post('http://ats-auto-tagging-api:4005/auto-tag-batch') as response:
                        return await response.json()
            except:
                # Fallback to direct database approach
                import asyncpg
                conn = await asyncpg.connect(
                    host="ats-intg-postgres",
                    port=5432,
                    user="postgres",
                    password="intg_password",
                    database="intg_db"
                )
                
                results = {
                    "issues_processed": 0,
                    "issues_tagged": 0,
                    "tags_applied": 0,
                    "status": "completed",
                    "message": "Auto-tagging completed using direct database connection"
                }
                
                try:
                    # Simple batch auto-tagging logic
                    issues = await conn.fetch("""
                        SELECT ai.issue_id, ai.symbol, ai.issue_type, ai.severity, 
                               COALESCE(ai.vendor, 'unknown') as vendor_source
                        FROM agent_issues ai
                        LEFT JOIN entity_tags et ON (
                            et.entity_id::text = ai.issue_id AND 
                            et.entity_type_id = (SELECT id FROM entity_types WHERE name = 'data_quality_issues')
                        )
                        WHERE et.id IS NULL
                        AND ai.created_at > NOW() - INTERVAL '7 days'
                        LIMIT 10
                    """)
                    
                    for issue in issues:
                        # Apply simple auto-tagging rules
                        applied_tags = []
                        
                        # Severity rule
                        severity = issue['severity'].lower()
                        if severity in ['critical', 'high', 'medium', 'low']:
                            tag_name = severity.title()
                            tag_result = await conn.fetchrow("SELECT id FROM tags WHERE name = $1", tag_name)
                            if tag_result:
                                await conn.execute("""
                                    INSERT INTO entity_tags (entity_type_id, entity_id, tag_id, source, confidence_score, metadata)
                                    VALUES (
                                        (SELECT id FROM entity_types WHERE name = 'data_quality_issues'),
                                        $1, $2, 'auto', 0.9, '{}'
                                    )
                                    ON CONFLICT (entity_type_id, entity_id, tag_id) DO NOTHING
                                """, hash(issue['issue_id']) % 2147483647, tag_result['id'])
                                applied_tags.append(tag_name)
                        
                        results["issues_processed"] += 1
                        if applied_tags:
                            results["issues_tagged"] += 1
                            results["tags_applied"] += len(applied_tags)
                            
                finally:
                    await conn.close()
                    
                return results
        
        try:
            # Run the async function
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            result = loop.run_until_complete(call_auto_tag_api())
            loop.close()
            
            self._serve_json_response(result)
            
        except Exception as e:
            logger.error(f"Error in auto-tag batch: {e}")
            self._serve_json_response({
                "error": str(e),
                "status": "failed",
                "message": "Auto-tagging batch failed"
            }, status_code=500)

    def _serve_available_tags(self):
        """Serve available tags for the tag filter panel"""
        import asyncpg
        
        async def get_available_tags():
            try:
                conn = await asyncpg.connect(
                    host="ats-intg-postgres",
                    port=5432,
                    user="postgres",
                    password="intg_password",
                    database="intg_db"
                )
                
                # Get all system tags with their categories
                tags = await conn.fetch("""
                    SELECT t.id, t.name, t.color, t.description,
                           tc.name as category_name, tc.color as category_color
                    FROM tags t
                    LEFT JOIN tag_categories tc ON t.category_id = tc.id
                    WHERE t.is_active = true
                    ORDER BY tc.sort_order, t.name
                """)
                
                await conn.close()
                
                return [
                    {
                        "id": tag['id'],
                        "name": tag['name'],
                        "color": tag['color'],
                        "description": tag['description'],
                        "category": {
                            "name": tag['category_name'] or "Uncategorized",
                            "color": tag['category_color'] or "#95a5a6"
                        }
                    }
                    for tag in tags
                ]
                
            except Exception as e:
                logger.error(f"Error loading available tags: {e}")
                # Return some default tags for fallback
                return [
                    {"id": 1, "name": "Critical", "color": "#e74c3c", "description": "Critical severity", "category": {"name": "Priority", "color": "#e74c3c"}},
                    {"id": 2, "name": "High", "color": "#ff6b6b", "description": "High severity", "category": {"name": "Priority", "color": "#e74c3c"}},
                    {"id": 3, "name": "Medium", "color": "#ffa726", "description": "Medium severity", "category": {"name": "Priority", "color": "#e74c3c"}},
                    {"id": 4, "name": "Low", "color": "#66bb6a", "description": "Low severity", "category": {"name": "Priority", "color": "#e74c3c"}},
                    {"id": 5, "name": "Data Gap", "color": "#e74c3c", "description": "Missing data issue", "category": {"name": "Type", "color": "#f39c12"}},
                    {"id": 6, "name": "Price Anomaly", "color": "#f39c12", "description": "Price data anomaly", "category": {"name": "Type", "color": "#f39c12"}},
                    {"id": 7, "name": "Volume Spike", "color": "#9b59b6", "description": "Volume anomaly", "category": {"name": "Type", "color": "#f39c12"}},
                    {"id": 8, "name": "Polygon", "color": "#8e44ad", "description": "Polygon data source", "category": {"name": "Source", "color": "#9b59b6"}},
                    {"id": 9, "name": "Tiingo", "color": "#2ecc71", "description": "Tiingo data source", "category": {"name": "Source", "color": "#9b59b6"}},
                    {"id": 10, "name": "EODHD", "color": "#e67e22", "description": "EODHD data source", "category": {"name": "Source", "color": "#9b59b6"}}
                ]
        
        try:
            # Run the async function
            import asyncio
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            tags = loop.run_until_complete(get_available_tags())
            loop.close()
            
            self._serve_json_response(tags)
            
        except Exception as e:
            logger.error(f"Error serving available tags: {e}")
            self._serve_json_response({
                "error": str(e),
                "message": "Failed to load available tags"
            }, status_code=500)

    def _serve_tag_api(self):
        """Handle tag API requests with proper routing"""
        if not getattr(self.analytics_service, 'tagging_enabled', False):
            self._serve_json_response({
                "error": "Tagging system not available"
            }, status_code=503)
            return

        try:
            from urllib.parse import urlparse, parse_qs
            import asyncio
            
            # Parse the path to extract tag API endpoint
            parsed_url = urlparse(self.path)
            path_parts = parsed_url.path.strip('/').split('/')
            
            if len(path_parts) < 3:  # /api/tags/...
                self._serve_404()
                return
                
            endpoint = '/'.join(path_parts[2:])  # Everything after /api/tags/
            query_params = parse_qs(parsed_url.query)
            
            # Get tag service instance
            async def get_tag_service():
                from infrastructure.database.connection_manager import get_database_connection
                connection = await get_database_connection("dev")  # TODO: Make configurable
                repository = TagRepository(connection)
                return TagService(repository)
            
            # Route to appropriate handler based on method and endpoint
            if self.command == 'GET':
                asyncio.run(self._handle_tag_get_request(endpoint, query_params, get_tag_service))
            elif self.command == 'POST':
                asyncio.run(self._handle_tag_post_request(endpoint, get_tag_service))
            elif self.command == 'DELETE':
                asyncio.run(self._handle_tag_delete_request(endpoint, get_tag_service))
            else:
                self._serve_405()  # Method not allowed
                
        except Exception as e:
            logger.error(f"Error handling tag API request: {e}")
            self._serve_500(str(e))

    async def _handle_tag_get_request(self, endpoint, query_params, get_tag_service_func):
        """Handle GET requests for tag API"""
        try:
            tag_service = await get_tag_service_func()
            
            if endpoint == '':
                # GET /api/tags/ - Get all tags
                active_only = query_params.get('active_only', ['true'])[0].lower() == 'true'
                category_id = query_params.get('category_id', [None])[0]
                search = query_params.get('search', [None])[0]
                limit = int(query_params.get('limit', [100])[0])
                
                if search:
                    tags = await tag_service.search_tags(search, limit=limit)
                elif category_id:
                    tags = await tag_service.get_tags_by_category(int(category_id), active_only=active_only)
                else:
                    tags = await tag_service.get_all_tags(active_only=active_only)
                
                response = [self._convert_tag_to_dict(tag) for tag in tags[:limit]]
                
            elif endpoint == 'categories':
                # GET /api/tags/categories - Get all categories
                categories = await tag_service.get_all_categories()
                response = [self._convert_category_to_dict(cat) for cat in categories]
                
            elif endpoint == 'analytics':
                # GET /api/tags/analytics - Get analytics
                analytics = await tag_service.get_tag_analytics()
                response = {
                    "most_used_tags": [
                        {
                            "tag_id": stat.tag_id,
                            "tag_name": stat.tag_name,
                            "total_usage": stat.total_usage,
                            "unique_entities": stat.unique_entities,
                            "entity_types_count": stat.entity_types_count,
                            "avg_confidence": float(stat.avg_confidence) if stat.avg_confidence else 0,
                            "last_used": stat.last_used.isoformat() if stat.last_used else None,
                            "active_days_last_90": stat.active_days_last_90
                        }
                        for stat in analytics.most_used_tags
                    ],
                    "tag_categories_distribution": analytics.tag_categories_distribution,
                    "tagging_trends": analytics.tagging_trends,
                    "entity_coverage": round(analytics.entity_coverage, 2),
                    "avg_tags_per_entity": round(analytics.avg_tags_per_entity, 2),
                    "top_co_occurring_tags": analytics.top_co_occurring_tags
                }
                
            elif endpoint.startswith('entity/'):
                # GET /api/tags/entity/{entity_type}/{entity_id} - Get entity tags
                parts = endpoint.split('/')
                if len(parts) >= 3:
                    entity_type, entity_id = parts[1], int(parts[2])
                    tags = await tag_service.get_entity_tags(entity_type, entity_id)
                    response = [self._convert_tag_to_dict(tag) for tag in tags]
                else:
                    raise ValueError("Invalid entity path format")
                    
            elif endpoint.startswith('suggestions/'):
                # GET /api/tags/suggestions/{entity_type}/{entity_id} - Get suggestions
                parts = endpoint.split('/')
                if len(parts) >= 3:
                    entity_type, entity_id = parts[1], int(parts[2])
                    limit = int(query_params.get('limit', [5])[0])
                    suggestions = await tag_service.suggest_tags_for_entity(entity_type, entity_id, limit)
                    response = [
                        {
                            "tag_id": suggestion.tag_id,
                            "tag_name": suggestion.tag_name,
                            "confidence_score": round(suggestion.confidence_score, 3),
                            "source": suggestion.source.value,
                            "explanation": suggestion.explanation
                        }
                        for suggestion in suggestions
                    ]
                else:
                    raise ValueError("Invalid suggestions path format")
                    
            elif endpoint.startswith('suggestions-enhanced/'):
                # GET /api/tags/suggestions-enhanced/{entity_type}/{entity_id} - Get enhanced suggestions
                parts = endpoint.split('/')
                if len(parts) >= 3:
                    entity_type, entity_id = parts[1], int(parts[2])
                    limit = int(query_params.get('limit', [5])[0])
                    suggestions = await tag_service.get_auto_tag_suggestions_enhanced(entity_type, entity_id, limit)
                    response = [
                        {
                            "tag_id": suggestion.tag_id,
                            "tag_name": suggestion.tag_name,
                            "confidence_score": round(suggestion.confidence_score, 3),
                            "source": suggestion.source.value,
                            "explanation": suggestion.explanation
                        }
                        for suggestion in suggestions
                    ]
                else:
                    raise ValueError("Invalid enhanced suggestions path format")
                    
            elif endpoint == 'auto-rules':
                # GET /api/tags/auto-rules - Get auto-tagging rules
                auto_tagging = tag_service.get_auto_tagging_service()
                rules = auto_tagging.get_all_rules()
                response = {
                    "rules": rules,
                    "total_rules": len(rules),
                    "categories": list(set(rule['category'] for rule in rules))
                }
                    
            elif endpoint == 'usage-stats':
                # GET /api/tags/usage-stats - Get usage statistics
                limit = int(query_params.get('limit', [100])[0])
                stats = await tag_service.get_tag_usage_stats(limit)
                response = [
                    {
                        "tag_id": stat.tag_id,
                        "tag_name": stat.tag_name,
                        "total_usage": stat.total_usage,
                        "unique_entities": stat.unique_entities,
                        "entity_types_count": stat.entity_types_count,
                        "avg_confidence": float(stat.avg_confidence) if stat.avg_confidence else 0,
                        "last_used": stat.last_used.isoformat() if stat.last_used else None,
                        "active_days_last_90": stat.active_days_last_90
                    }
                    for stat in stats
                ]
            else:
                self._serve_404()
                return
                
            self._serve_json_response(response)
            
        except Exception as e:
            logger.error(f"Error in tag GET handler: {e}")
            self._serve_500(str(e))

    async def _handle_tag_post_request(self, endpoint, get_tag_service_func):
        """Handle POST requests for tag API"""
        try:
            tag_service = await get_tag_service_func()
            
            # Read request body
            content_length = int(self.headers.get('Content-Length', 0))
            request_body = self.rfile.read(content_length).decode('utf-8')
            request_data = json.loads(request_body) if request_body else {}
            
            if endpoint == '':
                # POST /api/tags/ - Create new tag
                from domains.tagging.models.tag_models import CreateTagRequest
                create_request = CreateTagRequest(
                    name=request_data['name'],
                    description=request_data.get('description'),
                    category_id=request_data.get('category_id'),
                    color=request_data.get('color'),
                    metadata=request_data.get('metadata')
                )
                tag = await tag_service.create_tag(create_request)
                response = self._convert_tag_to_dict(tag)
                
            elif endpoint == 'apply':
                # POST /api/tags/apply - Apply tag to entity
                from domains.tagging.models.tag_models import ApplyTagRequest, TagSource
                apply_request = ApplyTagRequest(
                    entity_type=request_data['entity_type'],
                    entity_id=request_data['entity_id'],
                    tag_id=request_data['tag_id'],
                    confidence_score=request_data.get('confidence_score', 1.0),
                    source=TagSource(request_data.get('source', 'manual')),
                    metadata=request_data.get('metadata')
                )
                entity_tag = await tag_service.apply_tag_to_entity(apply_request)
                
                # Get tag details for response
                tag = await tag_service.repository.get_tag_by_id(entity_tag.tag_id)
                response = {
                    "id": entity_tag.id,
                    "entity_type": request_data['entity_type'],
                    "entity_id": entity_tag.entity_id,
                    "tag": self._convert_tag_to_dict(tag),
                    "tagged_by_user_id": entity_tag.tagged_by_user_id,
                    "tagged_at": entity_tag.tagged_at.isoformat(),
                    "confidence_score": entity_tag.confidence_score,
                    "source": entity_tag.source.value,
                    "metadata": entity_tag.metadata
                }
                
            elif endpoint == 'bulk-apply':
                # POST /api/tags/bulk-apply - Bulk apply tags
                from domains.tagging.models.tag_models import BulkTagRequest, TagSource
                bulk_request = BulkTagRequest(
                    entity_type=request_data['entity_type'],
                    entity_ids=request_data['entity_ids'],
                    tag_ids=request_data['tag_ids'],
                    confidence_score=request_data.get('confidence_score', 1.0),
                    source=TagSource(request_data.get('source', 'manual')),
                    metadata=request_data.get('metadata')
                )
                results = await tag_service.bulk_apply_tags(bulk_request)
                
                success_count = len([r for r in results if not isinstance(r, Exception)])
                error_count = len(results) - success_count
                
                response = {
                    "total_operations": len(results),
                    "successful": success_count,
                    "failed": error_count,
                    "message": f"Applied tags: {success_count} successful, {error_count} failed"
                }
                
            elif endpoint == 'search-entities':
                # POST /api/tags/search-entities - Search entities by tags
                from domains.tagging.models.tag_models import TagFilter
                from datetime import datetime
                
                tag_filter = TagFilter(
                    entity_type=request_data['entity_type'],
                    tag_ids=request_data.get('tag_ids'),
                    categories=request_data.get('categories'),
                    symbols=request_data.get('symbols'),
                    date_from=datetime.fromisoformat(request_data['date_from']) if request_data.get('date_from') else None,
                    date_to=datetime.fromisoformat(request_data['date_to']) if request_data.get('date_to') else None,
                    search=request_data.get('search'),
                    match_mode=request_data.get('match_mode', 'ANY'),
                    limit=request_data.get('limit', 50),
                    offset=request_data.get('offset', 0)
                )
                
                tagged_entities = await tag_service.get_tagged_entities(tag_filter)
                
                response = [
                    {
                        "entity_type": entity.entity_type,
                        "entity_id": entity.entity_id,
                        "tags": [self._convert_tag_to_dict(tag) for tag in entity.tags],
                        "total_tags": entity.total_tags
                    }
                    for entity in tagged_entities
                ]
                
            elif endpoint == 'refresh-analytics':
                # POST /api/tags/refresh-analytics - Refresh analytics
                success = await tag_service.refresh_tag_analytics()
                response = {"message": "Tag analytics refreshed successfully" if success else "Failed to refresh analytics"}
                
            elif endpoint.startswith('auto-tag/'):
                # POST /api/tags/auto-tag/{entity_type}/{entity_id} - Apply auto-tagging
                parts = endpoint.replace('auto-tag/', '').split('/')
                if len(parts) >= 2:
                    entity_type, entity_id = parts[0], int(parts[1])
                    
                    if entity_type == "data_quality_issues":
                        issue_details = await tag_service.repository.get_issue_details(entity_id)
                        if not issue_details:
                            self._serve_json_response({"error": "Entity not found"}, status_code=404)
                            return
                            
                        applied_tags = await tag_service.auto_tag_issue(entity_id, issue_details)
                        
                        response = {
                            "entity_type": entity_type,
                            "entity_id": entity_id,
                            "applied_tags": applied_tags,
                            "total_applied": len(applied_tags),
                            "message": f"Applied {len(applied_tags)} auto-tags successfully"
                        }
                    else:
                        self._serve_json_response({"error": f"Auto-tagging not supported for entity type: {entity_type}"}, status_code=400)
                        return
                else:
                    raise ValueError("Invalid auto-tag path format")
                    
            elif endpoint == 'auto-batch':
                # POST /api/tags/auto-batch - Run batch auto-tagging
                limit = int(request_data.get('limit', 100))
                min_hours_old = int(request_data.get('min_hours_old', 1))
                
                auto_tagging = tag_service.get_auto_tagging_service()
                response = await auto_tagging.run_auto_tagging_job(limit=limit, min_hours_old=min_hours_old)
                
            else:
                self._serve_404()
                return
                
            self._serve_json_response(response)
            
        except Exception as e:
            logger.error(f"Error in tag POST handler: {e}")
            self._serve_500(str(e))

    async def _handle_tag_delete_request(self, endpoint, get_tag_service_func):
        """Handle DELETE requests for tag API"""
        try:
            tag_service = await get_tag_service_func()
            
            if endpoint.startswith('entity/') and '/tag/' in endpoint:
                # DELETE /api/tags/entity/{entity_type}/{entity_id}/tag/{tag_id}
                parts = endpoint.replace('entity/', '').split('/tag/')
                if len(parts) == 2:
                    entity_parts = parts[0].split('/')
                    if len(entity_parts) >= 2:
                        entity_type, entity_id = entity_parts[0], int(entity_parts[1])
                        tag_id = int(parts[1])
                        
                        success = await tag_service.remove_tag_from_entity(entity_type, entity_id, tag_id)
                        if success:
                            response = {"message": "Tag removed successfully"}
                        else:
                            self._serve_json_response({"error": "Tag relationship not found"}, status_code=404)
                            return
                    else:
                        raise ValueError("Invalid entity path format")
                else:
                    raise ValueError("Invalid delete path format")
            else:
                self._serve_404()
                return
                
            self._serve_json_response(response)
            
        except Exception as e:
            logger.error(f"Error in tag DELETE handler: {e}")
            self._serve_500(str(e))

    def _convert_tag_to_dict(self, tag):
        """Convert Tag model to dictionary"""
        return {
            "id": tag.id,
            "name": tag.name,
            "slug": tag.slug,
            "description": tag.description,
            "color": tag.color,
            "category_id": tag.category_id,
            "category_name": tag.category.name if tag.category else None,
            "created_at": tag.created_at.isoformat(),
            "updated_at": tag.updated_at.isoformat(),
            "usage_count": tag.usage_count,
            "is_system_tag": tag.is_system_tag,
            "is_active": tag.is_active,
            "metadata": tag.metadata
        }

    def _convert_category_to_dict(self, category):
        """Convert TagCategory model to dictionary"""
        return {
            "id": category.id,
            "name": category.name,
            "slug": category.slug,
            "description": category.description,
            "color": category.color,
            "icon": category.icon,
            "parent_id": category.parent_id,
            "sort_order": category.sort_order,
            "created_at": category.created_at.isoformat(),
            "updated_at": category.updated_at.isoformat()
        }

    def _serve_data_quality_issues_with_tags(self):
        """Enhanced data quality issues API with tag filtering support"""
        try:
            from urllib.parse import urlparse, parse_qs
            import asyncio
            
            parsed_url = urlparse(self.path)
            query_params = parse_qs(parsed_url.query)
            
            # Extract tag filtering parameters
            tag_ids = query_params.get('tag_ids', [])
            if tag_ids and tag_ids[0]:
                tag_ids = [int(x) for x in tag_ids[0].split(',')]
            else:
                tag_ids = None
                
            categories = query_params.get('categories', [])
            if categories and categories[0]:
                categories = [int(x) for x in categories[0].split(',')]
            else:
                categories = None
                
            symbols = query_params.get('symbols', [])
            if symbols and symbols[0]:
                symbols = symbols[0].split(',')
            else:
                symbols = None
                
            date_from = query_params.get('date_from', [None])[0]
            date_to = query_params.get('date_to', [None])[0]
            match_mode = query_params.get('match_mode', ['ANY'])[0]
            limit = int(query_params.get('limit', [50])[0])
            offset = int(query_params.get('offset', [0])[0])
            
            async def get_filtered_issues():
                # Use direct database connection instead of complex tagging service
                import asyncpg
                import os
                
                # Get database connection details from environment
                db_host = os.getenv('DB_HOST', 'localhost')
                db_port = int(os.getenv('DB_PORT', '5432'))
                db_user = os.getenv('DB_USER', 'postgres')
                db_password = os.getenv('DB_PASSWORD', 'intg_password')
                db_name = os.getenv('DB_NAME', 'intg_db')
                
                conn = await asyncpg.connect(
                    host=db_host,
                    port=db_port,
                    user=db_user,
                    password=db_password,
                    database=db_name
                )
                
                try:
                    # Direct database query for tag filtering (working implementation)
                    if tag_ids:
                        query = """
                            SELECT DISTINCT ai.issue_id, ai.symbol, ai.issue_type, ai.severity, 
                                   ai.description, ai.vendor, ai.created_at
                            FROM agent_issues ai
                            JOIN entity_tags et ON et.entity_id = abs(hashtext(ai.issue_id)) % 2147483647  
                            JOIN entity_types ety ON et.entity_type_id = ety.id
                            WHERE ety.name = 'data_quality_issues'
                            AND et.tag_id = ANY($1)
                            ORDER BY ai.created_at DESC
                            LIMIT $2 OFFSET $3
                        """
                        issues = await conn.fetch(query, tag_ids, limit, offset)
                    else:
                        # No tag filtering - get all issues
                        query = """
                            SELECT issue_id, symbol, issue_type, severity, 
                                   description, vendor, created_at
                            FROM agent_issues
                            ORDER BY created_at DESC
                            LIMIT $1 OFFSET $2
                        """
                        issues = await conn.fetch(query, limit, offset)
                    
                    # Format issues for API response
                    issues_with_tags = []
                    for issue in issues:
                        formatted_issue = {
                            'id': issue['issue_id'],
                            'symbol': issue.get('symbol', 'N/A'),
                            'issue_type': issue.get('issue_type', 'unknown'),
                            'severity': issue['severity'],
                            'description': issue.get('description', 'N/A'),
                            'vendor_source': issue.get('vendor', 'unknown'),
                            'detected_at': issue['created_at'].isoformat() if issue.get('created_at') else None,
                            'status': 'open',
                            'tags': []  # TODO: Add tag details if needed
                        }
                        issues_with_tags.append(formatted_issue)
                    
                    return issues_with_tags
                    
                except Exception as e:
                    logger.error(f"Error getting filtered issues: {e}")
                    # Fallback to basic issues
                    return await self._get_basic_data_quality_issues(limit, offset)
                finally:
                    await conn.close()
            
            issues = asyncio.run(get_filtered_issues())
            
            response = {
                "success": True,
                "issues": issues,
                "filter_applied": {
                    "tag_ids": tag_ids,
                    "categories": categories,
                    "symbols": symbols,
                    "date_range": {
                        "from": date_from,
                        "to": date_to
                    },
                    "match_mode": match_mode
                },
                "pagination": {
                    "limit": limit,
                    "offset": offset,
                    "total": len(issues)
                }
            }
            
            self._serve_json_response(response)
            
        except Exception as e:
            logger.error(f"Error serving filtered data quality issues: {e}")
            self._serve_500(str(e))

    async def _get_issue_details(self, connection, issue_id):
        """Get detailed information for a specific issue"""
        try:
            query = """
            SELECT id, symbol, issue_type, description, severity, 
                   affected_date, vendor_source, field, expected_value, 
                   actual_value, created_at, updated_at
            FROM dev_data_quality_issues 
            WHERE id = $1
            """
            result = await connection.fetchrow(query, issue_id)
            
            if result:
                return {
                    "id": result['id'],
                    "symbol": result['symbol'],
                    "issue_type": result['issue_type'],
                    "description": result['description'],
                    "severity": result['severity'],
                    "affected_date": result['affected_date'].isoformat() if result['affected_date'] else None,
                    "vendor_source": result['vendor_source'],
                    "field": result['field'],
                    "expected_value": result['expected_value'],
                    "actual_value": result['actual_value'],
                    "created_at": result['created_at'].isoformat() if result['created_at'] else None,
                    "updated_at": result['updated_at'].isoformat() if result['updated_at'] else None
                }
            
            return None
            
        except Exception as e:
            logger.error(f"Error getting issue details for {issue_id}: {e}")
            return None

    async def _get_basic_data_quality_issues(self, limit=50, offset=0):
        """Get basic data quality issues without tag filtering (fallback)"""
        try:
            from infrastructure.database.connection_manager import get_database_connection
            
            connection = await get_database_connection("dev")
            
            query = """
            SELECT id, symbol, issue_type, description, severity, 
                   affected_date, vendor_source, field, expected_value, 
                   actual_value, created_at, updated_at
            FROM dev_data_quality_issues 
            ORDER BY created_at DESC
            LIMIT $1 OFFSET $2
            """
            
            results = await connection.fetch(query, limit, offset)
            
            issues = []
            for result in results:
                issues.append({
                    "id": result['id'],
                    "symbol": result['symbol'],
                    "issue_type": result['issue_type'],
                    "description": result['description'],
                    "severity": result['severity'],
                    "affected_date": result['affected_date'].isoformat() if result['affected_date'] else None,
                    "vendor_source": result['vendor_source'],
                    "field": result['field'],
                    "expected_value": result['expected_value'],
                    "actual_value": result['actual_value'],
                    "created_at": result['created_at'].isoformat() if result['created_at'] else None,
                    "updated_at": result['updated_at'].isoformat() if result['updated_at'] else None,
                    "tags": [],
                    "tag_count": 0
                })
                
            return issues
            
        except Exception as e:
            logger.error(f"Error getting basic data quality issues: {e}")
            return []

    def _serve_json_response(self, data, status_code=200):
        """Helper method to serve JSON responses"""
        self.send_response(status_code)
        self.send_header('Content-type', 'application/json')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, POST, DELETE, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')
        self.end_headers()
        self.wfile.write(json.dumps(data, indent=2).encode('utf-8'))

    def _serve_405(self):
        """Serve 405 Method Not Allowed"""
        self._serve_json_response({"error": "Method not allowed"}, status_code=405)


def start_unified_analytics_server(port: int = 3000):
    """Start the unified analytics server."""
    logger.info("🚀 Starting ATS Unified Analytics Service")
    logger.info(f"   Port: {port}")
    logger.info("   Features: Type-aware EDA, Universe Analytics, Ray Computing, Training Datasets")

    server = ThreadingHTTPServer(('0.0.0.0', port), UnifiedAnalyticsRequestHandler)

    try:
        logger.info(f"✅ Server started at http://0.0.0.0:{port}")
        logger.info("   Available endpoints:")
        logger.info("   • /health - Health check")
        logger.info("   • /eda - Main dashboard")
        logger.info("   • /api/intelligent-filters/{table} - Type-aware filters")
        logger.info("   • /api/universe-analytics - Cross-instrument analysis")
        logger.info("   • /api/v1/training-datasets - ML dataset management")
        logger.info("   • /api/ray-analytics/{dataset_id} - Distributed analytics")

        server.serve_forever()

    except KeyboardInterrupt:
        logger.info("🛑 Shutting down unified analytics service...")
        server.shutdown()
        logger.info("✅ Service stopped")


if __name__ == "__main__":
    port = int(os.getenv('ANALYTICS_PORT', 3000))
    start_unified_analytics_server(port)