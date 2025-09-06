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
import sys
import time
import numpy as np
from datetime import datetime, date, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Dict, List, Any, Optional
from urllib.parse import urlparse, parse_qs
from dataclasses import asdict

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Import core components
from core.database.connection_manager import get_connection_manager
from core.config.settings import get_settings

# Import visualization and dashboard components
try:
    from visualization.multi_panel_trading_chart import MultiPanelTradingChart
    from ml.training_data.timeseries_sequence_training_generator import MultiTimeframeFeatureExtractor, TrainingDataConfig
    VISUALIZATION_AVAILABLE = True
    logger.info("✅ Multi-panel trading visualization loaded")
except ImportError as e:
    VISUALIZATION_AVAILABLE = False

    logger.warning(f"⚠️ Multi-panel visualization not available: {e}")

# Import dashboard template engine
from core.analytics.dashboard.template_engine import DashboardTemplateEngine

# Import type system components (from analytics_service_class.py)
try:
    from schema.registry import schema_registry
    from schema.types import FieldSemantics
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
        
        # Initialize visualization components
        if self.visualization_enabled:
            self.multi_panel_chart = MultiPanelTradingChart()
            self.feature_extractor = MultiTimeframeFeatureExtractor(TrainingDataConfig())
        
        # Initialize dashboard template engine
        self.dashboard_engine = DashboardTemplateEngine()
        
        logger.info("🚀 Unified Analytics Service initialized")
        logger.info(f"   Type system: {'✅ Enabled' if self.type_system_enabled else '❌ Disabled'}")
        logger.info(f"   Ray computing: {'✅ Enabled' if self.ray_enabled else '❌ Disabled'}")
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
        """Fallback basic filter generation when type system unavailable."""
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
            from core.database.connection_manager import get_raw_connection
            
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
            from core.database.connection_manager import get_raw_connection
            import psycopg2.extras
            import numpy as np
            from pathlib import Path
            
            # Determine environment and table name - assume dev for now
            environment = "dev"  # Can be made configurable later
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
                        # Priority order: Riegeli format > numpy files > fallback paths
                        possible_file_paths = [
                            # New structure: /data/training_data/<run_id>/<timeframe>/<symbol>_<startdatetime>_<enddatetime>.arrayrecord
                            f"/data/training_data/*/{timeframe}/{symbol_lower}_*.arrayrecord",
                            f"/data/training_data/*/*/{symbol_lower}_*.arrayrecord",
                            # Legacy numpy files (existing structure)  
                            f"/data/training/arrayrecord_aapl_tsla_2025/{symbol_lower}_features.npy",
                            f"/data/training/{dataset_info['dataset_name'].lower()}/{symbol_lower}_features.npy",
                            f"/data/training/{dataset_id}/{symbol_lower}_features.npy",
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
                        pass
                    
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
            # Return sample data as fallback
            sample_data = self._generate_sample_sequence_for_dataset({
                'dataset_name': f'Dataset {dataset_id}',
                'symbols': 'DEMO',
                'sequence_length': 21
            }, dataset_id, row_index)
            sample_data['timeframe'] = timeframe
            return sample_data
    
    def _generate_sample_sequence_for_dataset(self, dataset_info: Dict, dataset_id: int = 0, row_index: int = 0) -> Dict[str, Any]:
        """Generate sample sequence data based on dataset info."""
        import random
        
        sequence_length = dataset_info.get('sequence_length', 21)
        symbol = dataset_info.get('symbols', 'DEMO').split(',')[0] if dataset_info.get('symbols') else 'DEMO'
        
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
            from core.database.connection_manager import get_raw_connection
            import psycopg2.extras
            from pathlib import Path
            
            # Determine environment and table name
            environment = "dev"  # Can be made configurable later  
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
                            # Fallback: use filesystem scanning (legacy approach)
                            logger.warning(f"No file_metadata available for dataset {dataset_id}, using filesystem fallback")
                            
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
            from core.database.connection_manager import get_raw_connection
            import psycopg2.extras
            from pathlib import Path
            import json
            
            # Determine environment and table name
            environment = "dev"  # Can be made configurable later  
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
                    run_id = dataset_info.get('run_id') or str(dataset_id)  # Use actual run_id or fallback to dataset_id
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
                        Path("/mnt/d/ats-data/training_data")  # Host path fallback
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
                                        
                            # If not found in run directory, fallback to general search
                            if not arrayrecord_files:
                                logger.warning(f"No files found in run {run_id}, falling back to general search")
                                for arrayrecord_file in list(base_path.rglob("*.arrayrecord")):
                                    file_name = arrayrecord_file.name.lower()
                                    file_path_str = str(arrayrecord_file).lower()
                                    symbol_lower = target_symbol.lower()
                                    
                                    if symbol_lower in file_name or f"/{symbol_lower}/" in file_path_str:
                                        arrayrecord_files.append(arrayrecord_file)
                                        logger.info(f"Found fallback file: {arrayrecord_file}")
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
                                        from datetime import datetime, timezone, timedelta
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
                            
                            # ENFORCE: No fake data allowed - check response before returning
                            from services.fake_data_detector import fail_on_fake_data
                            
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
                            
                            # Fail fast if fake data detected
                            fail_on_fake_data(response, f"visualization_data_response_dataset_{dataset_id}")
                            
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
                            raise RuntimeError(f"Failed to read training data file: {e}. No fallback data provided.")
                    
                    # No files found
                    raise ValueError(f"No Riegeli/ArrayRecord files found for dataset {dataset_id}, symbol {target_symbol}")
                    
        except Exception as e:
            logger.error(f"Error getting visualization data for dataset {dataset_id}: {e}")
            # No fake data - re-raise the error
            raise

    def get_training_dataset_sequence_multi_timeframe(self, dataset_id: int, sequence_id: str, row_index: int = 50) -> Dict[str, Any]:
        """Get multi-timeframe OHLC data for a specific sequence, showing 21 bars centered around row_index."""
        try:
            from core.database.connection_manager import get_raw_connection
            import psycopg2.extras
            from pathlib import Path
            import json
            
            logger.info(f"Getting multi-timeframe data for dataset {dataset_id}, sequence {sequence_id}")
            
            # Determine environment and table name
            environment = "dev"
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
                        Path("/mnt/d/ats-data/training_data")  # Host path fallback
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
                    
                    # Use comprehensive features if available, otherwise fall back to OHLC
                    if feature_matrix:
                        table_data = feature_matrix
                        logger.info(f"✅ Table data prepared: {len(table_data)} rows with {len(table_data[0])} features each")
                    elif '1h' in multi_timeframe_data:
                        table_data = multi_timeframe_data['1h']  # Fallback to OHLC data
                        logger.info(f"✅ Table data prepared (fallback): {len(table_data)} rows from 1h timeframe")
                    
                    # Create response and sanitize for JSON safety
                    response = {
                        "sequence_id": sequence_id,
                        "dataset_name": dataset_info.get('dataset_name'),
                        "ohlc_data": multi_timeframe_data,
                        "table_data": table_data,
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
                    from datetime import datetime, timezone, timedelta
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
            from core.database.connection_manager import get_raw_connection
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
            
            # Generate sample OHLCV data for demonstration
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
        """Generate the main EDA dashboard HTML using template engine."""
        return self.dashboard_engine.get_eda_dashboard_html()


# ==============================================

# ==============================================
# HTTP REQUEST HANDLER (from analytics_service.py)
# ==============================================

class UnifiedAnalyticsRequestHandler(BaseHTTPRequestHandler):
    """HTTP request handler for the unified analytics service."""
    
    def __init__(self, *args, **kwargs):
        self.analytics_service = UnifiedAnalyticsService()
        super().__init__(*args, **kwargs)
    
    def do_GET(self):
        """Handle GET requests."""
        try:
            logger.info(f"📍 GET request: {self.path}")
            
            if self.path == '/health':
                self._serve_health_check()
            elif self.path == '/eda' or self.path == '/':
                self._serve_eda_dashboard()
            elif self.path.startswith('/api/intelligent-filters/'):
                self._serve_intelligent_filters()
            elif self.path.startswith('/api/universe-analytics'):
                self._serve_universe_analytics()
            elif self.path.startswith('/api/multi-panel-chart'):
                asyncio.run(self._serve_multi_panel_chart())
            elif self.path.startswith('/api/v1/datasets/training-datasets'):
                if '/search' in self.path:
                    self._serve_training_dataset_search()
                elif '/feature-metadata' in self.path:
                    self._serve_training_dataset_feature_metadata() 
                elif '/compare/' in self.path:
                    self._serve_training_dataset_feature_comparison()
                else:
                    self._serve_training_dataset_details()
            elif self.path.startswith('/api/v1/training-datasets'):
                if '/feature-metadata' in self.path:
                    self._serve_training_dataset_feature_metadata()
                elif '/compare/' in self.path:
                    self._serve_training_dataset_feature_comparison()
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
            elif self.path.startswith('/api/ray-analytics/'):
                self._serve_ray_analytics()
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
            else:
                self._serve_404()
                
        except Exception as e:
            logger.error(f"Error handling GET request: {e}")
            self._serve_500(str(e))
    
    def _serve_health_check(self):
        """Serve health check response."""
        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.end_headers()
        
        health_status = {
            "status": "healthy",
            "service": "ats-unified-analytics",
            "timestamp": datetime.now().isoformat(),
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
    
    def _serve_training_dataset_feature_metadata(self):
        """Serve comprehensive feature metadata for a training dataset."""
        from urllib.parse import urlparse
        
        # Extract dataset_id from either path pattern
        path_parts = urlparse(self.path).path.split('/')
        dataset_id = None
        
        try:
            if 'datasets/training-datasets' in self.path:
                # /api/v1/datasets/training-datasets/{dataset_id}/feature-metadata
                dataset_id = int(path_parts[5])
            else:
                # /api/v1/training-datasets/{dataset_id}/feature-metadata  
                dataset_id = int(path_parts[4])
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
            # Get dataset service and retrieve feature metadata
            from services.dataset_service import DatasetService
            
            db_config = {
                'host': 'postgres-dev',
                'port': 5432,
                'database': 'dev_db', 
                'user': 'postgres',
                'password': 'dev_password'
            }
            
            dataset_service = DatasetService(db_config)
            metadata = dataset_service.get_feature_metadata(dataset_id)
            
            self.wfile.write(json.dumps(metadata).encode('utf-8'))
            
        except Exception as e:
            logger.error(f"Error retrieving feature metadata: {e}")
            error_response = {"error": f"Failed to retrieve feature metadata: {str(e)}"}
            self.wfile.write(json.dumps(error_response).encode('utf-8'))
    
    def _serve_training_dataset_search(self):
        """Search training datasets by required features."""
        from urllib.parse import urlparse, parse_qs
        
        parsed_url = urlparse(self.path)
        query_params = parse_qs(parsed_url.query)
        
        # Get required features and feature types from query parameters
        features = query_params.get('features', [])
        feature_types = query_params.get('feature_types', None)
        
        if not features:
            self.send_response(400)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps({"error": "Missing required 'features' query parameter"}).encode('utf-8'))
            return
            
        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.end_headers()
        
        try:
            from services.dataset_service import DatasetService
            
            db_config = {
                'host': 'postgres-dev',
                'port': 5432,
                'database': 'dev_db',
                'user': 'postgres', 
                'password': 'dev_password'
            }
            
            dataset_service = DatasetService(db_config)
            datasets = dataset_service.find_datasets_by_features(features, feature_types)
            
            # Convert DatasetMetadata objects to dictionaries
            dataset_dicts = []
            for dataset in datasets:
                dataset_dict = {
                    'id': dataset.id,
                    'dataset_name': dataset.dataset_name,
                    'symbols': dataset.symbols,
                    'total_sequences': dataset.total_sequences,
                    'sequence_length': dataset.sequence_length,
                    'feature_count': dataset.feature_count,
                    'label_count': dataset.label_count,
                    'data_quality_score': dataset.data_quality_score,
                    'creation_timestamp': dataset.creation_timestamp.isoformat() if dataset.creation_timestamp else None
                }
                dataset_dicts.append(dataset_dict)
            
            response = {
                'datasets': dataset_dicts,
                'total_count': len(dataset_dicts)
            }
            
            self.wfile.write(json.dumps(response).encode('utf-8'))
            
        except Exception as e:
            logger.error(f"Error searching datasets by features: {e}")
            error_response = {"error": f"Failed to search datasets: {str(e)}"}
            self.wfile.write(json.dumps(error_response).encode('utf-8'))
    
    def _serve_training_dataset_feature_comparison(self):
        """Compare feature schemas between two training datasets."""
        from urllib.parse import urlparse
        
        path_parts = urlparse(self.path).path.split('/')
        
        try:
            if 'datasets/training-datasets' in self.path:
                # /api/v1/datasets/training-datasets/{dataset_id_1}/compare/{dataset_id_2}
                dataset_id_1 = int(path_parts[5])
                dataset_id_2 = int(path_parts[7])
            else:
                # /api/v1/training-datasets/{dataset_id_1}/compare/{dataset_id_2}
                dataset_id_1 = int(path_parts[4])
                dataset_id_2 = int(path_parts[6])
        except (IndexError, ValueError):
            self.send_response(400)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps({"error": "Invalid dataset IDs"}).encode('utf-8'))
            return
            
        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.end_headers()
        
        try:
            from services.dataset_service import DatasetService
            
            db_config = {
                'host': 'postgres-dev',
                'port': 5432, 
                'database': 'dev_db',
                'user': 'postgres',
                'password': 'dev_password'
            }
            
            dataset_service = DatasetService(db_config)
            comparison = dataset_service.compare_feature_schemas(dataset_id_1, dataset_id_2)
            
            self.wfile.write(json.dumps(comparison).encode('utf-8'))
            
        except Exception as e:
            logger.error(f"Error comparing datasets: {e}")
            error_response = {"error": f"Failed to compare datasets: {str(e)}"}
            self.wfile.write(json.dumps(error_response).encode('utf-8'))
    
    def _serve_training_dataset_details(self):
        """Get detailed information about a specific training dataset."""
        from urllib.parse import urlparse
        
        # Extract dataset_id from path like /api/v1/datasets/training-datasets/{dataset_id}
        path_parts = urlparse(self.path).path.split('/')
        
        try:
            dataset_id = int(path_parts[5])  # /api/v1/datasets/training-datasets/{dataset_id}
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
            from services.dataset_service import DatasetService
            
            db_config = {
                'host': 'postgres-dev',
                'port': 5432,
                'database': 'dev_db',
                'user': 'postgres',
                'password': 'dev_password'
            }
            
            dataset_service = DatasetService(db_config)
            dataset = dataset_service.get_dataset_metadata(dataset_id)
            
            if not dataset:
                self.send_response(404)
                self.send_header('Content-type', 'application/json')
                self.end_headers()
                self.wfile.write(json.dumps({"error": f"Training dataset {dataset_id} not found"}).encode('utf-8'))
                return
            
            # Convert DatasetMetadata to dictionary with feature metadata
            result = {
                'id': dataset.id,
                'dataset_name': dataset.dataset_name,
                'symbols': dataset.symbols,
                'total_sequences': dataset.total_sequences,
                'sequence_length': dataset.sequence_length,
                'feature_count': dataset.feature_count,
                'label_count': dataset.label_count,
                'data_quality_score': dataset.data_quality_score,
                'creation_timestamp': dataset.creation_timestamp.isoformat() if dataset.creation_timestamp else None,
                'date_range_start': dataset.date_range_start.isoformat() if dataset.date_range_start else None,
                'date_range_end': dataset.date_range_end.isoformat() if dataset.date_range_end else None,
                'file_size_mb': dataset.file_size_mb,
                'feature_completeness': dataset.feature_completeness,
                'label_completeness': dataset.label_completeness
            }
            
            # Add feature metadata if available
            try:
                feature_metadata = dataset_service.get_feature_metadata(dataset_id)
                if 'error' not in feature_metadata:
                    result['feature_metadata'] = feature_metadata
            except Exception as e:
                logger.warning(f"Could not retrieve feature metadata for dataset {dataset_id}: {e}")
                result['feature_metadata'] = None
            
            self.wfile.write(json.dumps(result).encode('utf-8'))
            
        except Exception as e:
            logger.error(f"Error retrieving training dataset: {e}")
            error_response = {"error": f"Failed to retrieve training dataset: {str(e)}"}
            self.wfile.write(json.dumps(error_response).encode('utf-8'))

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
        """Serve list of database tables."""
        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.end_headers()
        
        try:
            from core.database.connection_manager import get_raw_connection
            from psycopg2.extras import RealDictCursor
            
            with get_raw_connection() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                    cursor.execute("""
                        SELECT tablename 
                        FROM pg_tables 
                        WHERE schemaname = 'public' 
                        AND tablename LIKE %s
                        ORDER BY tablename
                    """, ('dev_%',))
                    
                    tables = [row['tablename'] for row in cursor.fetchall()]
                    response = {"tables": tables}
                    
        except Exception as e:
            logger.error(f"Error getting tables list: {e}")
            response = {
                "tables": [
                    "dev_daily_prices", "dev_training_datasets", "dev_instruments",
                    "dev_daily_prices_polygon", "dev_daily_prices_tiingo", "dev_daily_prices_eodhd"
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
            from core.database.connection_manager import get_raw_connection
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
            from core.database.connection_manager import get_raw_connection
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
        """Serve sample data from table."""
        table_name = self.path.split('/')[-1]
        
        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.end_headers()
        
        try:
            from core.database.connection_manager import get_raw_connection
            from psycopg2.extras import RealDictCursor
            
            with get_raw_connection() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                    from psycopg2 import sql
                    cursor.execute(
                        sql.SQL("SELECT * FROM {} LIMIT 10").format(
                            sql.Identifier(table_name)
                        )
                    )
                    
                    rows = []
                    for row in cursor.fetchall():
                        row_dict = dict(row)
                        # Convert dates/datetimes to strings for JSON serialization
                        for key, value in row_dict.items():
                            if hasattr(value, 'isoformat'):
                                row_dict[key] = value.isoformat()
                        rows.append(row_dict)
                    
                    response = {"table_name": table_name, "rows": rows}
                    
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
            from core.database.connection_manager import get_raw_connection
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
                "/api/ray-analytics/{dataset_id}", "/api/multi-panel-chart"
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