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
from typing import Dict, List, Any, Optional
from urllib.parse import urlparse, parse_qs
from dataclasses import asdict

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Import core components
from core.database.connection_manager import get_connection_manager
from core.config.settings import get_settings

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
        
        logger.info("🚀 Unified Analytics Service initialized")
        logger.info(f"   Type system: {'✅ Enabled' if self.type_system_enabled else '❌ Disabled'}")
        logger.info(f"   Ray computing: {'✅ Enabled' if self.ray_enabled else '❌ Disabled'}")
        
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
                               dataset_path, symbol_files
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
                    
                    # Get available sequences by calling the visualization data API
                    try:
                        viz_data = self.get_training_dataset_visualization_data(dataset_id)
                        
                        if viz_data and not viz_data.get('error'):
                            symbols = dataset_info.get('symbols', [])
                            target_symbol = symbols[0] if symbols else 'AAPL'
                            
                            total_sequences = viz_data.get('total_sequences', 0)
                            file_size_mb = viz_data.get('total_records', 21) * 0.005  # Estimate file size
                            
                            # Generate sequence entries based on available sequences
                            for seq_id in range(total_sequences):
                                sequences.append({
                                    "id": seq_id,
                                    "sequence_id": seq_id,
                                    "symbol": target_symbol,
                                    "filename": f"{target_symbol.lower()}_visualization.arrayrecord",
                                    "description": f"{target_symbol} Sequence {seq_id}",
                                    "timeframe": "hourly", 
                                    "file_size_mb": round(file_size_mb, 2)
                                })
                            
                            total_count = len(sequences)
                            logger.info(f"Generated {total_count} sequences for dataset {dataset_id}, symbol {target_symbol}")
                        else:
                            logger.warning(f"No visualization data available for dataset {dataset_id}")
                    
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

    def get_training_dataset_visualization_data(self, dataset_id: int, start_idx: int = 0, sequence_id: str = None) -> Dict[str, Any]:
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
                    
                    # Search for actual Riegeli files in all potential locations
                    training_base_paths = [
                        Path("/data/training"),
                        Path("/data/training_data")
                    ]
                    
                    arrayrecord_files = []
                    for base_path in training_base_paths:
                        if base_path.exists():
                            logger.info(f"Searching for {target_symbol} files in: {base_path}")
                            # Find all Riegeli and ArrayRecord files containing the target symbol
                            for arrayrecord_file in list(base_path.rglob("*.arrayrecord")):
                                # Check if file contains our target symbol (case insensitive)
                                file_name = arrayrecord_file.name.lower()
                                file_path_str = str(arrayrecord_file).lower()
                                symbol_lower = target_symbol.lower()
                                
                                logger.debug(f"Checking file: {arrayrecord_file}, symbol_lower: {symbol_lower}")
                                
                                # Check both filename and path for symbol match
                                if symbol_lower in file_name or f"/{symbol_lower}/" in file_path_str:
                                    arrayrecord_files.append(arrayrecord_file)
                                    logger.info(f"Found matching file: {arrayrecord_file}")
                                    break  # Use first match
                    
                    if arrayrecord_files:
                        # Read actual ArrayRecord data
                        arrayrecord_file = arrayrecord_files[0]
                        try:
                            from array_record.python.array_record_module import ArrayRecordReader
                            
                            visualization_data = []
                            reader = ArrayRecordReader(str(arrayrecord_file))
                            
                            # Read all records first
                            all_records = []
                            try:
                                while True:
                                    record = reader.read()
                                    if not record:
                                        break
                                    all_records.append(record)
                            except:
                                pass  # End of file
                            
                            # Parse records starting from start_idx
                            for i, record_bytes in enumerate(all_records[start_idx:start_idx + 21]):  # Get 21 bars
                                    try:
                                        record_data = json.loads(record_bytes.decode())
                                        
                                        # Extract OHLC and indicator data
                                        bar_data = {
                                            "time_step": i,
                                            "datetime": record_data.get('datetime', ''),
                                            "symbol": record_data.get('symbol', target_symbol),
                                            "open": record_data.get('open', 0),
                                            "high": record_data.get('high', 0), 
                                            "low": record_data.get('low', 0),
                                            "close": record_data.get('close', 0),
                                            "volume": record_data.get('volume', 0),
                                            "envelope_top": record_data.get('envelope_top', 0),
                                            "envelope_bot": record_data.get('envelope_bot', 0),
                                            "pldot": record_data.get('pldot', 0)
                                        }
                                        visualization_data.append(bar_data)
                                        
                                    except (json.JSONDecodeError, KeyError) as e:
                                        logger.warning(f"Error parsing record {i + start_idx}: {e}")
                                        continue
                            
                            # Calculate available sequences (total records - window size + 1)
                            total_records = len(all_records)
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
    # WEB DASHBOARD SERVING (from analytics_service.py)
    # ==============================================
    
    def get_eda_dashboard_html(self):
        """Generate the main EDA dashboard HTML."""
        return """<!DOCTYPE html>
        <html>
        <head>
            <meta charset="UTF-8">
            <title>ATS Unified Analytics - EDA Dashboard</title>
            <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
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
                    <div class="feature-item">📊 Type-Aware EDA</div>
                    <div class="feature-item">🌐 Universe Analytics</div>
                    <div class="feature-item">⚡ Ray Computing</div>
                    <div class="feature-item">🤖 Training Datasets</div>
                    <div class="feature-item">📈 Real-time Quality</div>
                </div>
            </div>
            
            <div class="main-content">
                <h2>Select Analysis Type</h2>
                <button onclick="loadEDA()">📊 Exploratory Data Analysis</button>
                <button onclick="loadBarCollectionMetrics()">📈 Bar Collection Metrics</button>
                <button onclick="loadUniverseAnalytics()">🌐 Universe Analytics</button>
                <button onclick="loadTrainingDatasets()">🤖 Training Datasets</button>
                <button onclick="loadRayAnalytics()">⚡ Distributed Analytics</button>
                
                <div id="analysis-content">
                    <p style="text-align: center; margin-top: 50px; color: #666;">
                        Select an analysis type above to begin
                    </p>
                </div>
            </div>
            
            <script>
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
                            // Fallback to common financial tables
                            tables = [
                                'dev_daily_prices', 'dev_training_datasets', 'dev_instruments',
                                'dev_daily_prices_polygon', 'dev_daily_prices_tiingo', 'dev_daily_prices_eodhd'
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
                        return;
                    }
                    
                    document.getElementById('table-content').style.display = 'block';
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
                        const sampleResponse = await fetch(`/api/table-sample/${tableName}`);
                        if (sampleResponse.ok) {
                            const sample = await sampleResponse.json();
                            if (sample.rows && sample.rows.length > 0) {
                                const headers = Object.keys(sample.rows[0]);
                                const tableHtml = `
                                    <table style="width: 100%; border-collapse: collapse; font-size: 0.9em;">
                                        <thead>
                                            <tr style="background: #f1f3f4;">
                                                ${headers.map(h => `<th style="padding: 8px; border: 1px solid #ddd; text-align: left;">${h}</th>`).join('')}
                                            </tr>
                                        </thead>
                                        <tbody>
                                            ${sample.rows.slice(0, 10).map(row => `
                                                <tr>
                                                    ${headers.map(h => `<td style="padding: 8px; border: 1px solid #ddd;">${row[h] !== null ? row[h] : '<em>null</em>'}</td>`).join('')}
                                                </tr>
                                            `).join('')}
                                        </tbody>
                                    </table>
                                `;
                                document.getElementById('sample-data').innerHTML = tableHtml;
                            } else {
                                document.getElementById('sample-data').innerHTML = '<p>No data found in table</p>';
                            }
                        }
                        
                        // Load column distributions
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
                                            <div><strong>Unique:</strong> ${stats.unique || 0}</div>
                                            <div><strong>Nulls:</strong> ${stats.nulls || 0}</div>
                                            <div><strong>Type:</strong> ${stats.type || 'unknown'}</div>
                                        </div>
                                        ${stats.min !== undefined ? `<div><strong>Min:</strong> ${stats.min} <strong>Max:</strong> ${stats.max}</div>` : ''}
                                        ${stats.top_values ? `<div><strong>Top Values:</strong> ${stats.top_values.slice(0, 5).join(', ')}</div>` : ''}
                                    </div>
                                `;
                            }
                            
                            document.getElementById('column-distributions').innerHTML = distHtml || '<p>No distribution data available</p>';
                        }
                        
                    } catch (error) {
                        document.getElementById('table-info').innerHTML = '<p style="color: red;">Error: ' + error.message + '</p>';
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
                        '<h3>🌐 Universe Analytics</h3><p>Loading cross-instrument analysis...</p>';
                    
                    try {
                        const response = await fetch('/api/universe-analytics');
                        const data = await response.json();
                        
                        const html = `
                            <h3>🌐 Universe Analytics</h3>
                            <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 20px; margin-top: 20px;">
                                <div style="background: white; padding: 15px; border-radius: 8px; border: 1px solid #ddd;">
                                    <h4>📊 Composition</h4>
                                    <p><strong>Total Instruments:</strong> ${data.composition.total_instruments}</p>
                                    <p><strong>By Exchange:</strong> ${JSON.stringify(data.composition.by_exchange)}</p>
                                    <p><strong>By Sector:</strong> ${JSON.stringify(data.composition.by_sector)}</p>
                                </div>
                                <div style="background: white; padding: 15px; border-radius: 8px; border: 1px solid #ddd;">
                                    <h4>📈 Performance</h4>
                                    <p><strong>Sharpe Ratio:</strong> ${data.performance.sharpe_ratio}</p>
                                    <p><strong>Max Drawdown:</strong> ${data.performance.max_drawdown}</p>
                                    <p><strong>Analysis Time:</strong> ${data.analysis_timestamp}</p>
                                </div>
                            </div>
                            <div style="background: white; padding: 15px; border-radius: 8px; border: 1px solid #ddd; margin-top: 20px;">
                                <h4>🔗 Correlations</h4>
                                <p><strong>Universe:</strong> ${data.universe_name}</p>
                                <p><em>Note: This is a demonstration of the universe analytics API. In a full implementation, 
                                this would show correlation matrices, sector analysis, and interactive charts.</em></p>
                            </div>
                        `;
                        
                        document.getElementById('analysis-content').innerHTML = html;
                        
                    } catch (error) {
                        document.getElementById('analysis-content').innerHTML = 
                            '<h3>🌐 Universe Analytics</h3><p style="color: red;">Error loading universe analytics: ' + error.message + '</p>';
                    }
                }
                
                async function loadTrainingDatasets() {
                    document.getElementById('analysis-content').innerHTML = 
                        '<h3>🤖 Training Datasets</h3><p>Loading ML dataset visualization...</p>';
                    
                    try {
                        const response = await fetch('/api/v1/training-datasets');
                        const data = await response.json();
                        
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
                        if (data.datasets && data.datasets.length > 0) {
                            data.datasets.forEach(dataset => {
                                const option = document.createElement('option');
                                option.value = dataset.id;
                                option.textContent = `[ID: ${dataset.id}] ${dataset.dataset_name} (${dataset.total_sequences} sequences, ${dataset.symbols})`;
                                selector.appendChild(option);
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
                            document.getElementById('datasets-summary').innerHTML = '<p>No training datasets found.</p>';
                        }
                        
                    } catch (error) {
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
                                options += `<option value="${seq.sequence_id}">${seq.symbol} - ${seq.timeframe} (${seq.file_size_mb}MB)</option>`;
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
                    
                    if (!datasetId) {
                        alert('Please select a dataset first');
                        return;
                    }
                    
                    // Show loading state
                    document.getElementById('dataset-visualization').style.display = 'block';
                    
                    // Set loading state for all four timeframe charts
                    const timeframes = ['5m', '15m', '1h', '1d'];
                    timeframes.forEach(tf => {
                        document.getElementById(`ohlc-chart-${tf}`).innerHTML = `<p>Loading ${tf} chart...</p>`;
                    });
                    
                    document.getElementById('dataset-info').innerHTML = '<p>Loading dataset info...</p>';
                    document.getElementById('sequence-table').innerHTML = '<p>Loading sequence data...</p>';
                    
                    try {
                        // Fetch sequence data for visualization
                        let apiUrl = `/api/v1/training-datasets/${datasetId}/visualization-data?start_idx=${rowIndex}`;
                        if (sequenceId) {
                            apiUrl += `&sequence_id=${sequenceId}`;
                        }
                        
                        const response = await fetch(apiUrl);
                        const visualizationData = await response.json();
                        
                        // Create data structure for charts (maintaining backward compatibility)
                        const sequenceDataPromises = timeframes.map(async (timeframe) => {
                            // For now, use the same data for all timeframes
                            // In future, could enhance API to return different timeframes
                            return { 
                                timeframe, 
                                data: {
                                    ...visualizationData,
                                    symbol: sequenceId ? sequenceId.split('_')[0] : 'UNKNOWN'
                                }
                            };
                        });
                        
                        const sequenceResults = await Promise.all(sequenceDataPromises);
                        
                        // Use the first result for dataset info (all timeframes have same metadata)
                        const primarySequenceData = sequenceResults[0].data;
                        
                        // Display dataset info
                        document.getElementById('dataset-info').innerHTML = `
                            <div style="line-height: 1.6;">
                                <p><strong>Dataset:</strong> ${primarySequenceData.dataset_name}</p>
                                <p><strong>Symbol:</strong> ${primarySequenceData.symbol}</p>
                                <p><strong>Row Index:</strong> ${primarySequenceData.row_index}</p>
                                <p><strong>Sequence Length:</strong> ${primarySequenceData.sequence_length}</p>
                                <p><strong>Selected Bar:</strong> ${primarySequenceData.selected_bar}</p>
                                <p><strong>Data Source:</strong> ${primarySequenceData.source}</p>
                                ${primarySequenceData.total_sequences ? `<p><strong>Total Sequences:</strong> ${primarySequenceData.total_sequences}</p>` : ''}
                            </div>
                        `;
                        
                        // Create Plotly OHLC charts for each timeframe
                        sequenceResults.forEach(({ timeframe, data }) => {
                            createTimeframeOHLCChart(timeframe, data);
                        });
                        
                        // Create sequence data table (using primary timeframe data)
                        createSequenceTable(primarySequenceData);
                        
                    } catch (error) {
                        // Set error state for all charts
                        timeframes.forEach(tf => {
                            document.getElementById(`ohlc-chart-${tf}`).innerHTML = `<p style="color: red;">Error loading ${tf} chart: ${error.message}</p>`;
                        });
                        document.getElementById('dataset-info').innerHTML = '<p style="color: red;">Error loading dataset info</p>';
                        document.getElementById('sequence-table').innerHTML = '<p style="color: red;">Error loading sequence data</p>';
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
                
                function loadRayAnalytics() {
                    document.getElementById('analysis-content').innerHTML = 
                        '<h3>⚡ Distributed Analytics</h3><p>Loading Ray distributed computing...</p>';
                    // Implementation would load Ray analytics interface
                }
            </script>
        </body>
        </html>
        """

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
            elif self.path.startswith('/api/v1/training-datasets'):
                if '/sequence/' in self.path:
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
                "/api/ray-analytics/{dataset_id}"
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