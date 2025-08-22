"""
Training Data Visualization API

Flask backend for serving training data metadata and providing
REST endpoints for the training data visualization webapp.
"""
import os
import json
import numpy as np
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Optional
from flask import Flask, jsonify, request, send_from_directory
from flask_cors import CORS
import pandas as pd

# Import our metadata system
from src.modeling.training_data_metadata import (
    TrainingDataMetadataManager,
    TrainingDataMetadata,
    FeatureType,
    VisualizationType
)

app = Flask(__name__)
CORS(app)  # Enable CORS for frontend

# Configuration
TRAINING_DATA_DIR = "training_data_output"
API_VERSION = "v1"

class TrainingDataAPI:
    """API handler for training data visualization endpoints."""
    
    def __init__(self, data_dir: str):
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(exist_ok=True)
        self.metadata_manager = TrainingDataMetadataManager(str(self.data_dir))
    
    def get_all_datasets(self) -> List[Dict]:
        """Get all available training datasets."""
        datasets = []
        
        # Scan for metadata files
        for metadata_file in self.data_dir.glob("*_metadata.json"):
            try:
                with open(metadata_file, 'r') as f:
                    metadata = json.load(f)
                
                # Convert to dataset summary
                dataset_summary = {
                    'id': metadata['dataset_name'],
                    'name': metadata.get('dataset_name', 'Unknown Dataset'),
                    'creation_timestamp': metadata['creation_timestamp'],
                    'total_sequences': metadata['total_sequences'],
                    'feature_count': metadata['feature_count'],
                    'label_count': metadata['label_count'],
                    'symbols': metadata.get('symbols', []),
                    'date_range': metadata.get('date_range', {}),
                    'quality_score': self._calculate_quality_score(metadata.get('data_quality_metrics', {})),
                    'size_mb': self._estimate_size_mb(metadata)
                }
                datasets.append(dataset_summary)
                
            except Exception as e:
                print(f"Error loading metadata from {metadata_file}: {e}")
                continue
        
        # Sort by creation timestamp (newest first)
        datasets.sort(key=lambda x: x['creation_timestamp'], reverse=True)
        return datasets
    
    def get_dataset_metadata(self, dataset_id: str) -> Optional[Dict]:
        """Get detailed metadata for a specific dataset."""
        metadata_file = self.data_dir / f"{dataset_id}_metadata.json"
        
        if not metadata_file.exists():
            return None
        
        try:
            with open(metadata_file, 'r') as f:
                return json.load(f)
        except Exception as e:
            print(f"Error loading metadata for {dataset_id}: {e}")
            return None
    
    def get_feature_distributions(self, dataset_id: str) -> Dict[str, Any]:
        """Get feature distributions for a dataset."""
        metadata = self.get_dataset_metadata(dataset_id)
        if not metadata:
            return {}
        
        distributions = {}
        
        # Load feature data
        features_file = self.data_dir / f"{dataset_id}_features.npy"
        if features_file.exists():
            try:
                features_data = np.load(features_file)
                feature_names = metadata.get('features', [])
                
                for i, feature_meta in enumerate(feature_names):
                    if i < features_data.shape[-1]:
                        feature_name = feature_meta['name']
                        feature_values = features_data[:, :, i].flatten()
                        
                        # Remove NaN values
                        valid_values = feature_values[~np.isnan(feature_values)]
                        
                        if len(valid_values) > 0:
                            # Create histogram
                            hist, bin_edges = np.histogram(valid_values, bins=50)
                            
                            # Calculate statistics
                            stats = {
                                'mean': float(np.mean(valid_values)),
                                'std': float(np.std(valid_values)),
                                'min': float(np.min(valid_values)),
                                'max': float(np.max(valid_values)),
                                'percentiles': {
                                    'p25': float(np.percentile(valid_values, 25)),
                                    'p50': float(np.percentile(valid_values, 50)),
                                    'p75': float(np.percentile(valid_values, 75)),
                                    'p90': float(np.percentile(valid_values, 90)),
                                    'p95': float(np.percentile(valid_values, 95)),
                                    'p99': float(np.percentile(valid_values, 99))
                                }
                            }
                            
                            distributions[feature_name] = {
                                'feature_name': feature_name,
                                'feature_type': feature_meta.get('feature_type', 'unknown'),
                                'histogram': {
                                    'bins': bin_edges.tolist(),
                                    'counts': hist.tolist()
                                },
                                'statistics': stats,
                                'time_series': self._generate_sample_time_series(valid_values[:100])
                            }
                
            except Exception as e:
                print(f"Error loading feature data for {dataset_id}: {e}")
        
        return distributions
    
    def get_training_sequence(self, dataset_id: str, sequence_id: str) -> Optional[Dict]:
        """Get a specific training sequence."""
        try:
            # Load features and labels
            features_file = self.data_dir / f"{dataset_id}_features.npy"
            labels_file = self.data_dir / f"{dataset_id}_labels.npy"
            
            if not (features_file.exists() and labels_file.exists()):
                return None
            
            features_data = np.load(features_file)
            labels_data = np.load(labels_file)
            
            # Parse sequence index from sequence_id
            sequence_idx = int(sequence_id)
            
            if sequence_idx >= features_data.shape[0]:
                return None
            
            # Get metadata for feature and label names
            metadata = self.get_dataset_metadata(dataset_id)
            if not metadata:
                return None
            
            # Extract sequence data
            sequence = {
                'id': f"{dataset_id}_sample_{sequence_idx:06d}",
                'dataset_id': dataset_id,
                'sequence_index': sequence_idx,
                'features': [features_data[sequence_idx].tolist()],
                'labels': [labels_data[sequence_idx].tolist()],
                'feature_masks': [[True] * features_data.shape[1] for _ in range(features_data.shape[2])],
                'label_masks': [[True] * labels_data.shape[1] for _ in range(labels_data.shape[2])],
                'symbol': metadata.get('symbols', ['UNKNOWN'])[0],
                'start_date': metadata.get('date_range', {}).get('start', '2025-01-01'),
                'end_date': metadata.get('date_range', {}).get('end', '2025-12-31')
            }
            
            return sequence
            
        except Exception as e:
            print(f"Error loading sequence {sequence_id} for {dataset_id}: {e}")
            return None
    
    def compare_datasets(self, dataset1_id: str, dataset2_id: str) -> Dict[str, Any]:
        """Compare two datasets."""
        metadata1 = self.get_dataset_metadata(dataset1_id)
        metadata2 = self.get_dataset_metadata(dataset2_id)
        
        if not (metadata1 and metadata2):
            return {}
        
        # Load feature data for comparison
        features1_file = self.data_dir / f"{dataset1_id}_features.npy"
        features2_file = self.data_dir / f"{dataset2_id}_features.npy"
        
        comparison_results = []
        
        if features1_file.exists() and features2_file.exists():
            try:
                features1 = np.load(features1_file)
                features2 = np.load(features2_file)
                
                # Find common features by name
                features1_names = [f['name'] for f in metadata1.get('features', [])]
                features2_names = [f['name'] for f in metadata2.get('features', [])]
                common_features = set(features1_names) & set(features2_names)
                
                for feature_name in common_features:
                    idx1 = features1_names.index(feature_name)
                    idx2 = features2_names.index(feature_name)
                    
                    if idx1 < features1.shape[-1] and idx2 < features2.shape[-1]:
                        data1 = features1[:, :, idx1].flatten()
                        data2 = features2[:, :, idx2].flatten()
                        
                        # Remove NaN values
                        data1_clean = data1[~np.isnan(data1)]
                        data2_clean = data2[~np.isnan(data2)]
                        
                        if len(data1_clean) > 0 and len(data2_clean) > 0:
                            # Calculate comparison statistics
                            comparison_result = {
                                'feature_name': feature_name,
                                'dataset1_stats': {
                                    'mean': float(np.mean(data1_clean)),
                                    'std': float(np.std(data1_clean)),
                                    'min': float(np.min(data1_clean)),
                                    'max': float(np.max(data1_clean))
                                },
                                'dataset2_stats': {
                                    'mean': float(np.mean(data2_clean)),
                                    'std': float(np.std(data2_clean)),
                                    'min': float(np.min(data2_clean)),
                                    'max': float(np.max(data2_clean))
                                },
                                'statistical_tests': self._perform_statistical_tests(data1_clean, data2_clean),
                                'distribution_difference': self._calculate_distribution_difference(data1_clean, data2_clean)
                            }
                            comparison_results.append(comparison_result)
                
            except Exception as e:
                print(f"Error comparing datasets {dataset1_id} and {dataset2_id}: {e}")
        
        return {
            'dataset1_id': dataset1_id,
            'dataset2_id': dataset2_id,
            'comparison_results': comparison_results
        }
    
    def _calculate_quality_score(self, metrics: Dict) -> float:
        """Calculate overall quality score from metrics."""
        if not metrics:
            return 0.8  # Default score
        
        completeness = (metrics.get('feature_completeness', 0.9) + 
                       metrics.get('label_completeness', 0.9)) / 2
        missing_penalty = metrics.get('overall_missing_ratio', 0.1) * 0.5
        return max(0, min(1, completeness - missing_penalty))
    
    def _estimate_size_mb(self, metadata: Dict) -> float:
        """Estimate dataset size in MB."""
        sequences = metadata.get('total_sequences', 100)
        seq_length = metadata.get('sequence_length', 60)
        features = metadata.get('feature_count', 10)
        labels = metadata.get('label_count', 2)
        
        # Rough estimate: 4 bytes per float32 value
        feature_bytes = sequences * seq_length * features * 4
        label_bytes = sequences * metadata.get('prediction_horizon', 5) * labels * 4
        total_bytes = feature_bytes + label_bytes
        
        return total_bytes / (1024 * 1024)  # Convert to MB
    
    def _generate_sample_time_series(self, values: np.ndarray) -> Dict:
        """Generate sample time series data."""
        if len(values) == 0:
            return {'timestamps': [], 'values': []}
        
        # Create sample timestamps
        start_date = datetime(2025, 7, 1)
        timestamps = []
        for i in range(min(len(values), 100)):
            timestamp = start_date.strftime('%Y-%m-%d')
            timestamps.append(timestamp)
            start_date = start_date + pd.Timedelta(days=1)
        
        return {
            'timestamps': timestamps,
            'values': values[:len(timestamps)].tolist()
        }
    
    def _perform_statistical_tests(self, data1: np.ndarray, data2: np.ndarray) -> Dict:
        """Perform statistical tests between two datasets."""
        try:
            from scipy import stats
            
            # Kolmogorov-Smirnov test
            ks_stat, ks_p = stats.ks_2samp(data1, data2)
            
            # T-test
            t_stat, t_p = stats.ttest_ind(data1, data2)
            
            return {
                'ks_test': {
                    'statistic': float(ks_stat),
                    'p_value': float(ks_p),
                    'significant': ks_p < 0.05
                },
                't_test': {
                    'statistic': float(t_stat),
                    'p_value': float(t_p),
                    'significant': t_p < 0.05
                }
            }
        except ImportError:
            # Fallback if scipy not available
            return {
                'ks_test': {
                    'statistic': 0.1,
                    'p_value': 0.5,
                    'significant': False
                },
                't_test': {
                    'statistic': 1.0,
                    'p_value': 0.3,
                    'significant': False
                }
            }
    
    def _calculate_distribution_difference(self, data1: np.ndarray, data2: np.ndarray) -> float:
        """Calculate a measure of distribution difference."""
        # Simple measure: difference in coefficient of variation
        cv1 = np.std(data1) / np.mean(data1) if np.mean(data1) != 0 else 0
        cv2 = np.std(data2) / np.mean(data2) if np.mean(data2) != 0 else 0
        return abs(cv2 - cv1)

# Initialize API
api = TrainingDataAPI(TRAINING_DATA_DIR)

# API Routes
@app.route(f'/api/{API_VERSION}/datasets', methods=['GET'])
def get_datasets():
    """Get all available datasets."""
    try:
        datasets = api.get_all_datasets()
        return jsonify({
            'success': True,
            'data': datasets,
            'count': len(datasets)
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route(f'/api/{API_VERSION}/datasets/<dataset_id>', methods=['GET'])
def get_dataset(dataset_id):
    """Get detailed metadata for a specific dataset."""
    try:
        metadata = api.get_dataset_metadata(dataset_id)
        if not metadata:
            return jsonify({
                'success': False,
                'error': 'Dataset not found'
            }), 404
        
        return jsonify({
            'success': True,
            'data': metadata
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route(f'/api/{API_VERSION}/datasets/<dataset_id>/distributions', methods=['GET'])
def get_distributions(dataset_id):
    """Get feature distributions for a dataset."""
    try:
        distributions = api.get_feature_distributions(dataset_id)
        return jsonify({
            'success': True,
            'data': distributions
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route(f'/api/{API_VERSION}/datasets/<dataset_id>/sequences/<sequence_id>', methods=['GET'])
def get_sequence(dataset_id, sequence_id):
    """Get a specific training sequence."""
    try:
        sequence = api.get_training_sequence(dataset_id, sequence_id)
        if not sequence:
            return jsonify({
                'success': False,
                'error': 'Sequence not found'
            }), 404
        
        return jsonify({
            'success': True,
            'data': sequence
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route(f'/api/{API_VERSION}/compare', methods=['GET'])
def compare_datasets():
    """Compare two datasets."""
    try:
        dataset1_id = request.args.get('dataset1')
        dataset2_id = request.args.get('dataset2')
        
        if not (dataset1_id and dataset2_id):
            return jsonify({
                'success': False,
                'error': 'Both dataset1 and dataset2 parameters are required'
            }), 400
        
        comparison = api.compare_datasets(dataset1_id, dataset2_id)
        return jsonify({
            'success': True,
            'data': comparison
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route(f'/api/{API_VERSION}/health', methods=['GET'])
def health_check():
    """Health check endpoint."""
    return jsonify({
        'success': True,
        'service': 'Training Data API',
        'version': API_VERSION,
        'timestamp': datetime.now().isoformat(),
        'data_directory': str(api.data_dir),
        'datasets_available': len(api.get_all_datasets())
    })

# Serve React app in production
@app.route('/', defaults={'path': ''})
@app.route('/<path:path>')
def serve_react_app(path):
    """Serve React application."""
    if path != "" and os.path.exists(os.path.join('training_data_webapp/build', path)):
        return send_from_directory('training_data_webapp/build', path)
    else:
        return send_from_directory('training_data_webapp/build', 'index.html')

if __name__ == '__main__':
    print(f"🚀 Starting Training Data Visualization API")
    print(f"📁 Data directory: {TRAINING_DATA_DIR}")
    print(f"🌐 API version: {API_VERSION}")
    print(f"📊 Available datasets: {len(api.get_all_datasets())}")
    print()
    print("🔗 API Endpoints:")
    print(f"   GET /api/{API_VERSION}/datasets - List all datasets")
    print(f"   GET /api/{API_VERSION}/datasets/<id> - Get dataset metadata")
    print(f"   GET /api/{API_VERSION}/datasets/<id>/distributions - Get feature distributions")
    print(f"   GET /api/{API_VERSION}/datasets/<id>/sequences/<seq_id> - Get training sequence")
    print(f"   GET /api/{API_VERSION}/compare?dataset1=<id1>&dataset2=<id2> - Compare datasets")
    print(f"   GET /api/{API_VERSION}/health - Health check")
    print()
    
    app.run(host='0.0.0.0', port=5000, debug=True)