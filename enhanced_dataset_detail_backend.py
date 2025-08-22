#!/usr/bin/env python3
"""
Enhanced Dataset Detail Backend APIs for Row-Level Access

This module adds new APIs to support the dataset detail page with:
1. Row-level sequence filtering and access
2. Individual sequence OHLC charts 
3. Dynamic distribution visualization for filtered sequences
4. Dual-axis charts (price/volume top, indicators bottom)
"""

import asyncio
import numpy as np
import json
import os
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple
from pathlib import Path

class EnhancedDatasetManager:
    """Enhanced dataset manager with row-level access capabilities."""
    
    def __init__(self, data_directory: str = "training_data_output"):
        self.data_directory = Path(data_directory)
        self._dataset_cache = {}
        
    def _load_dataset_files(self, dataset_name: str) -> Dict[str, Any]:
        """Load all files for a dataset (features, labels, masks, metadata)."""
        if dataset_name in self._dataset_cache:
            return self._dataset_cache[dataset_name]
            
        try:
            # Load metadata first
            metadata_path = self.data_directory / f"{dataset_name}_metadata.json"
            with open(metadata_path, 'r') as f:
                metadata = json.load(f)
            
            # Load numpy arrays
            features = np.load(self.data_directory / f"{dataset_name}_features.npy")
            labels = np.load(self.data_directory / f"{dataset_name}_labels.npy")
            
            # Try to load masks (might not exist for all datasets)
            mask_path = self.data_directory / f"{dataset_name}_feature_masks.npy"
            masks = np.load(mask_path) if mask_path.exists() else None
            
            dataset_data = {
                'metadata': metadata,
                'features': features,
                'labels': labels,
                'masks': masks,
                'sequence_count': features.shape[0],
                'timesteps': features.shape[1],
                'feature_count': features.shape[2]
            }
            
            self._dataset_cache[dataset_name] = dataset_data
            return dataset_data
            
        except Exception as e:
            raise FileNotFoundError(f"Could not load dataset {dataset_name}: {e}")
    
    async def list_dataset_sequences(self, dataset_name: str, 
                                   limit: int = 50, offset: int = 0,
                                   symbol_filter: str = None,
                                   feature_filter: str = None,
                                   value_min: float = None,
                                   value_max: float = None) -> Dict[str, Any]:
        """List sequences within a dataset with filtering capabilities."""
        
        dataset_data = self._load_dataset_files(dataset_name)
        metadata = dataset_data['metadata']
        features = dataset_data['features']
        labels = dataset_data['labels']
        
        # Build sequence list with summary statistics
        sequences = []
        feature_names = [f['name'] for f in metadata['features']]
        
        for seq_idx in range(dataset_data['sequence_count']):
            sequence_features = features[seq_idx]  # Shape: (timesteps, features)
            sequence_labels = labels[seq_idx] if len(labels.shape) > 2 else labels[seq_idx:seq_idx+1]
            
            # Calculate sequence statistics
            seq_stats = {}
            for feat_idx, feat_name in enumerate(feature_names):
                feat_values = sequence_features[:, feat_idx]
                seq_stats[feat_name] = {
                    'mean': float(np.mean(feat_values)),
                    'std': float(np.std(feat_values)),
                    'min': float(np.min(feat_values)),
                    'max': float(np.max(feat_values))
                }
            
            # Apply filtering
            if symbol_filter and symbol_filter.upper() not in metadata.get('symbols', []):
                continue
                
            if feature_filter and value_min is not None and value_max is not None:
                if feature_filter in seq_stats:
                    feat_mean = seq_stats[feature_filter]['mean']
                    if not (value_min <= feat_mean <= value_max):
                        continue
            
            # Extract date range for this sequence (if available)
            start_date = metadata.get('date_range', {}).get('start', '2023-01-01')
            base_date = datetime.fromisoformat(start_date.replace('Z', '+00:00').replace('+00:00', ''))
            seq_start = base_date + timedelta(days=seq_idx)
            seq_end = seq_start + timedelta(days=dataset_data['timesteps']-1)
            
            sequences.append({
                'sequence_id': seq_idx,
                'start_date': seq_start.isoformat(),
                'end_date': seq_end.isoformat(),
                'feature_stats': seq_stats,
                'label_preview': sequence_labels.tolist()[:3] if len(sequence_labels.shape) > 1 else sequence_labels.tolist(),
                'symbols': metadata.get('symbols', ['UNKNOWN'])
            })
        
        # Apply pagination
        total_sequences = len(sequences)
        sequences = sequences[offset:offset + limit]
        
        return {
            'dataset_name': dataset_name,
            'sequences': sequences,
            'total_sequences': total_sequences,
            'feature_names': feature_names,
            'available_symbols': metadata.get('symbols', []),
            'metadata': {
                'sequence_length': dataset_data['timesteps'],
                'feature_count': dataset_data['feature_count'],
                'total_sequences': dataset_data['sequence_count']
            }
        }
    
    async def get_sequence_details(self, dataset_name: str, sequence_id: int) -> Dict[str, Any]:
        """Get detailed information for a specific sequence."""
        
        dataset_data = self._load_dataset_files(dataset_name)
        metadata = dataset_data['metadata']
        features = dataset_data['features']
        labels = dataset_data['labels']
        
        if sequence_id >= dataset_data['sequence_count']:
            raise ValueError(f"Sequence {sequence_id} not found in dataset {dataset_name}")
        
        sequence_features = features[sequence_id]  # Shape: (timesteps, features)
        sequence_labels = labels[sequence_id] if len(labels.shape) > 2 else labels[sequence_id:seq_idx+1]
        
        # Build detailed feature data
        feature_data = []
        for feat_idx, feat_info in enumerate(metadata['features']):
            feat_values = sequence_features[:, feat_idx]
            feature_data.append({
                'feature_name': feat_info['name'],
                'feature_type': feat_info['feature_type'],
                'values': feat_values.tolist(),
                'statistics': {
                    'mean': float(np.mean(feat_values)),
                    'std': float(np.std(feat_values)),
                    'min': float(np.min(feat_values)),
                    'max': float(np.max(feat_values))
                },
                'metadata': feat_info
            })
        
        return {
            'sequence_id': sequence_id,
            'dataset_name': dataset_name,
            'features': feature_data,
            'labels': sequence_labels.tolist(),
            'symbols': metadata.get('symbols', []),
            'timesteps': dataset_data['timesteps'],
            'date_range': self._get_sequence_date_range(metadata, sequence_id, dataset_data['timesteps'])
        }
    
    async def get_sequence_ohlc_data(self, dataset_name: str, sequence_id: int) -> Dict[str, Any]:
        """Generate OHLC + indicators data for dual-axis chart visualization."""
        
        sequence_details = await self.get_sequence_details(dataset_name, sequence_id)
        features = sequence_details['features']
        
        # Extract OHLC and technical indicators
        price_features = ['open', 'high', 'low', 'close', 'price']
        volume_features = ['volume', 'volume_ratio']
        indicator_features = ['ema', 'rsi', 'atr', 'vwap', 'etop', 'ebot', 'pldot']
        
        ohlc_data = []
        volume_data = []
        indicator_data = {}
        
        # Initialize date range
        start_date = datetime.fromisoformat(sequence_details['date_range']['start'])
        
        for timestep in range(sequence_details['timesteps']):
            timestamp = start_date + timedelta(days=timestep)
            
            # Build OHLC data point
            ohlc_point = {'date': timestamp.isoformat()}
            volume_point = {'date': timestamp.isoformat()}
            
            # Extract price and volume data
            for feature in features:
                feat_name = feature['feature_name'].lower()
                feat_value = feature['values'][timestep]
                
                if any(price_term in feat_name for price_term in price_features):
                    if 'open' in feat_name:
                        ohlc_point['open'] = feat_value
                    elif 'high' in feat_name:
                        ohlc_point['high'] = feat_value
                    elif 'low' in feat_name:
                        ohlc_point['low'] = feat_value
                    elif 'close' in feat_name or 'price' in feat_name:
                        ohlc_point['close'] = feat_value
                
                elif any(vol_term in feat_name for vol_term in volume_features):
                    volume_point['volume'] = feat_value
                
                elif any(ind_term in feat_name for ind_term in indicator_features):
                    # Group technical indicators
                    for ind_type in indicator_features:
                        if ind_type in feat_name:
                            if ind_type not in indicator_data:
                                indicator_data[ind_type] = []
                            if len(indicator_data[ind_type]) <= timestep:
                                indicator_data[ind_type].extend([None] * (timestep + 1 - len(indicator_data[ind_type])))
                            indicator_data[ind_type][timestep] = {
                                'date': timestamp.isoformat(),
                                'value': feat_value
                            }
            
            # Fill missing OHLC with synthetic data if needed
            if 'close' in ohlc_point and len(ohlc_point) < 5:  # Missing some OHLC components
                close_price = ohlc_point['close']
                ohlc_point.setdefault('open', close_price + np.random.normal(0, 0.5))
                ohlc_point.setdefault('high', max(ohlc_point['open'], close_price) + abs(np.random.normal(0, 1)))
                ohlc_point.setdefault('low', min(ohlc_point['open'], close_price) - abs(np.random.normal(0, 1)))
            
            ohlc_data.append(ohlc_point)
            if volume_point.get('volume') is not None:
                volume_data.append(volume_point)
        
        return {
            'sequence_id': sequence_id,
            'dataset_name': dataset_name,
            'symbols': sequence_details['symbols'],
            'ohlc_data': ohlc_data,
            'volume_data': volume_data,
            'technical_indicators': {
                name: [point for point in data if point is not None] 
                for name, data in indicator_data.items()
            },
            'chart_config': {
                'type': 'dual_axis',
                'top_panel': ['ohlc', 'volume'],
                'bottom_panel': list(indicator_data.keys()),
                'date_range': sequence_details['date_range']
            }
        }
    
    async def get_filtered_distributions(self, dataset_name: str,
                                       sequence_ids: List[int] = None,
                                       **filters) -> Dict[str, Any]:
        """Calculate dynamic feature distributions for filtered sequences."""
        
        dataset_data = self._load_dataset_files(dataset_name)
        metadata = dataset_data['metadata']
        features = dataset_data['features']
        
        # Default to all sequences if none specified
        if sequence_ids is None:
            sequence_ids = list(range(dataset_data['sequence_count']))
        
        # Calculate distributions for each feature across filtered sequences
        distributions = {}
        feature_names = [f['name'] for f in metadata['features']]
        
        for feat_idx, feat_name in enumerate(feature_names):
            # Collect all values for this feature across filtered sequences
            all_values = []
            for seq_id in sequence_ids:
                if seq_id < dataset_data['sequence_count']:
                    seq_values = features[seq_id, :, feat_idx]
                    all_values.extend(seq_values.flatten())
            
            if all_values:
                all_values = np.array(all_values)
                hist, bins = np.histogram(all_values, bins=30)
                
                distributions[feat_name] = {
                    'feature_name': feat_name,
                    'histogram_bins': bins.tolist(),
                    'histogram_counts': hist.tolist(),
                    'min_value': float(np.min(all_values)),
                    'max_value': float(np.max(all_values)),
                    'mean_value': float(np.mean(all_values)),
                    'std_value': float(np.std(all_values)),
                    'sample_count': len(all_values),
                    'sequence_count': len(sequence_ids)
                }
        
        return {
            'dataset_name': dataset_name,
            'filtered_sequences': len(sequence_ids),
            'total_sequences': dataset_data['sequence_count'],
            'distributions': distributions
        }
    
    def _get_sequence_date_range(self, metadata: Dict, sequence_id: int, timesteps: int) -> Dict[str, str]:
        """Calculate date range for a specific sequence."""
        base_start = metadata.get('date_range', {}).get('start', '2023-01-01')
        start_date = datetime.fromisoformat(base_start.replace('Z', '+00:00').replace('+00:00', ''))
        
        seq_start = start_date + timedelta(days=sequence_id)
        seq_end = seq_start + timedelta(days=timesteps-1)
        
        return {
            'start': seq_start.isoformat(),
            'end': seq_end.isoformat()
        }

# FastAPI Endpoint Extensions
# Add these endpoints to the existing FastAPI application

async def list_dataset_sequences_endpoint(dataset_id: int, 
                                        limit: int = 50, 
                                        offset: int = 0,
                                        symbol_filter: str = None,
                                        feature_filter: str = None,
                                        value_min: float = None,
                                        value_max: float = None):
    """API endpoint for listing sequences within a dataset."""
    # Map dataset_id to actual dataset name
    dataset_mapping = {
        1: "aapl_tsla",
        2: "dataset_20250821_183848",
        3: "dataset_20250821_183855"
    }
    
    dataset_name = dataset_mapping.get(dataset_id)
    if not dataset_name:
        raise HTTPException(status_code=404, detail="Dataset not found")
    
    manager = EnhancedDatasetManager()
    return await manager.list_dataset_sequences(
        dataset_name, limit, offset, symbol_filter, feature_filter, value_min, value_max
    )

async def get_sequence_details_endpoint(dataset_id: int, sequence_id: int):
    """API endpoint for getting detailed sequence information."""
    dataset_mapping = {
        1: "aapl_tsla",
        2: "dataset_20250821_183848", 
        3: "dataset_20250821_183855"
    }
    
    dataset_name = dataset_mapping.get(dataset_id)
    if not dataset_name:
        raise HTTPException(status_code=404, detail="Dataset not found")
    
    manager = EnhancedDatasetManager()
    return await manager.get_sequence_details(dataset_name, sequence_id)

async def get_sequence_ohlc_endpoint(dataset_id: int, sequence_id: int):
    """API endpoint for getting sequence OHLC + indicators data."""
    dataset_mapping = {
        1: "aapl_tsla",
        2: "dataset_20250821_183848",
        3: "dataset_20250821_183855"
    }
    
    dataset_name = dataset_mapping.get(dataset_id)
    if not dataset_name:
        raise HTTPException(status_code=404, detail="Dataset not found")
    
    manager = EnhancedDatasetManager()
    return await manager.get_sequence_ohlc_data(dataset_name, sequence_id)

async def get_filtered_distributions_endpoint(dataset_id: int,
                                            sequence_ids: str = None):
    """API endpoint for getting dynamic distributions of filtered sequences."""
    dataset_mapping = {
        1: "aapl_tsla",
        2: "dataset_20250821_183848",
        3: "dataset_20250821_183855"
    }
    
    dataset_name = dataset_mapping.get(dataset_id)
    if not dataset_name:
        raise HTTPException(status_code=404, detail="Dataset not found")
    
    # Parse sequence_ids if provided
    seq_ids = None
    if sequence_ids:
        try:
            seq_ids = [int(x.strip()) for x in sequence_ids.split(',')]
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid sequence_ids format")
    
    manager = EnhancedDatasetManager()
    return await manager.get_filtered_distributions(dataset_name, seq_ids)


"""
New API Endpoints to Add:

1. GET /api/v1/datasets/{dataset_id}/sequences
   - Query params: limit, offset, symbol_filter, feature_filter, value_min, value_max
   - Returns: List of sequences with summary stats and filtering

2. GET /api/v1/datasets/{dataset_id}/sequences/{sequence_id}
   - Returns: Detailed sequence data with all features and labels

3. GET /api/v1/datasets/{dataset_id}/sequences/{sequence_id}/ohlc
   - Returns: OHLC + technical indicators for dual-axis chart

4. GET /api/v1/datasets/{dataset_id}/filtered-distributions
   - Query params: sequence_ids (comma-separated)
   - Returns: Dynamic distributions for filtered sequence subset

Frontend Integration Plan:
1. Dataset detail page with sequence table
2. Filtering controls for sequences
3. Individual row OHLC chart links  
4. Dual-axis chart implementation
5. Dynamic distribution updates
"""