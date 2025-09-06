#!/usr/bin/env python3
"""
EDA with Dataset Service Integration
Demonstrates how EDA processes use dataset service for metadata and data access.
"""

import os
import sys
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
from datetime import datetime
import logging
from typing import Dict, List, Any, Optional

# Add project root to path
sys.path.append(str(Path(__file__).parent.parent))

from src.clients.dataset_client import DatasetClient

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class EDAAnalyzer:
    """EDA analysis methods - uses DatasetClient for all data operations."""
    
    def __init__(self, dataset_client: DatasetClient):
        """Initialize EDA analyzer with dataset client."""
        self.client = dataset_client
        logger.info("✅ EDA Analyzer initialized")
    
    def explore_available_datasets(self, symbols: Optional[List[str]] = None) -> pd.DataFrame:
        """Get overview of available datasets."""
        
        logger.info(f"📊 Exploring available datasets for symbols: {symbols}")
        
        datasets = self.client.list_available_datasets(symbols=symbols)
        
        if not datasets:
            logger.warning("⚠️ No datasets found")
            return pd.DataFrame()
        
        # Create summary DataFrame
        df_summary = pd.DataFrame(datasets)
        
        # Add analysis columns
        df_summary['sequences_k'] = df_summary['size'].str.extract(r'([\d,]+)').str.replace(',', '').astype(float) / 1000
        df_summary['memory_mb'] = df_summary['memory'].str.extract(r'(\d+)').astype(float)
        df_summary['quality_pct'] = df_summary['quality'].str.rstrip('%').astype(float)
        
        logger.info(f"✅ Found {len(df_summary)} datasets")
        logger.info(f"   Quality range: {df_summary['quality_pct'].min():.1f}% - {df_summary['quality_pct'].max():.1f}%")
        logger.info(f"   Size range: {df_summary['sequences_k'].min():.1f}K - {df_summary['sequences_k'].max():.1f}K sequences")
        
        return df_summary
    
    def analyze_dataset(self, dataset_id: int, analysis_type: str = 'comprehensive') -> Dict[str, Any]:
        """Perform comprehensive analysis of a specific dataset."""
        
        logger.info(f"🔍 Analyzing dataset {dataset_id} with {analysis_type} analysis")
        
        # Get dataset configuration for EDA
        metadata = self.client.service.get_dataset_metadata(dataset_id)
        if not metadata:
            logger.error(f"❌ Dataset {dataset_id} not found")
            return {}
        
        # Use generic training config but adapt for EDA
        # EDA can use the same config as training - it's just metadata
        config = self.client.get_training_data_config(
            symbols=metadata.symbols,
            min_sequences=100,  # Lower requirement for EDA
            preferred_timeframes=metadata.timeframes
        )
        
        if not config:
            logger.error(f"❌ Could not create configuration for dataset {dataset_id}")
            return {}
        
        # Create data loader using generic interface
        data_loader = self.client.create_data_loader(config)
        
        # For EDA, use smaller sample size for performance
        sample_size = min(10000, config['total_sequences'])
        logger.info(f"📊 Loading sample of {sample_size} records for analysis")
        
        X_sample, y_sample = data_loader.get_sample(sample_size=sample_size)
        
        if len(X_sample) == 0:
            logger.error("❌ No sample data loaded")
            return {}
        
        # Perform analysis
        analysis_results = {
            'dataset_info': {
                'dataset_id': dataset_id,
                'dataset_name': metadata.dataset_name,
                'symbols': metadata.symbols,
                'analysis_date': datetime.now().isoformat(),
                'sample_size': len(X_sample)
            },
            'data_characteristics': self._analyze_data_characteristics(X_sample, y_sample, config),
            'quality_assessment': self._assess_data_quality(X_sample, y_sample, metadata),
            'statistical_summary': self._calculate_statistical_summary(X_sample, y_sample),
            'temporal_analysis': self._analyze_temporal_patterns(X_sample, y_sample, config),
            'feature_analysis': self._analyze_features(X_sample, config)
        }
        
        logger.info(f"✅ Analysis completed for dataset {dataset_id}")
        return analysis_results
    
    def _analyze_data_characteristics(self, X: np.ndarray, y: np.ndarray, config: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze basic data characteristics."""
        
        characteristics = {
            'shape': {
                'features_shape': X.shape,
                'targets_shape': y.shape,
                'feature_count': X.shape[-1] if len(X.shape) > 1 else 1,
                'sequence_count': len(X)
            },
            'data_types': {
                'features_dtype': str(X.dtype),
                'targets_dtype': str(y.dtype),
                'memory_usage_mb': (X.nbytes + y.nbytes) / (1024 * 1024)
            },
            'value_ranges': {
                'features_min': float(np.min(X)) if X.size > 0 else 0,
                'features_max': float(np.max(X)) if X.size > 0 else 0,
                'targets_min': float(np.min(y)) if y.size > 0 else 0,
                'targets_max': float(np.max(y)) if y.size > 0 else 0
            },
            'data_quality': {
                'features_nan_count': int(np.isnan(X).sum()) if X.size > 0 else 0,
                'targets_nan_count': int(np.isnan(y).sum()) if y.size > 0 else 0,
                'features_inf_count': int(np.isinf(X).sum()) if X.size > 0 else 0,
                'targets_inf_count': int(np.isinf(y).sum()) if y.size > 0 else 0
            },
            'dataset_metadata': {
                'symbols': config.get('symbols', []),
                'timeframes': config.get('timeframes', []),
                'technical_indicators': config.get('technical_indicators', [])
            }
        }
        
        return characteristics
    
    def _assess_data_quality(self, X: np.ndarray, y: np.ndarray, metadata) -> Dict[str, Any]:
        """Assess data quality metrics."""
        
        total_elements = X.size + y.size if X.size > 0 and y.size > 0 else 0
        
        if total_elements == 0:
            return {'quality_score': 0.0, 'issues': ['No data available']}
        
        # Count quality issues
        nan_count = np.isnan(X).sum() + np.isnan(y).sum() if X.size > 0 and y.size > 0 else 0
        inf_count = np.isinf(X).sum() + np.isinf(y).sum() if X.size > 0 and y.size > 0 else 0
        zero_count = (X == 0).sum() + (y == 0).sum() if X.size > 0 and y.size > 0 else 0
        
        # Calculate quality metrics
        completeness = 1.0 - (nan_count / total_elements)
        validity = 1.0 - (inf_count / total_elements)
        non_zero_rate = 1.0 - (zero_count / total_elements)
        
        # Overall quality score
        quality_score = (completeness + validity + non_zero_rate) / 3
        
        issues = []
        if completeness < 0.95:
            issues.append(f"Low completeness: {completeness:.3f}")
        if validity < 0.95:
            issues.append(f"Validity issues: {validity:.3f}")
        if non_zero_rate < 0.8:
            issues.append(f"High zero rate: {1-non_zero_rate:.3f}")
        
        quality_assessment = {
            'quality_score': float(quality_score),
            'completeness': float(completeness),
            'validity': float(validity),
            'non_zero_rate': float(non_zero_rate),
            'issues': issues,
            'metadata_quality_score': float(metadata.data_quality_score),
            'quality_comparison': {
                'calculated_vs_metadata': float(quality_score - metadata.data_quality_score)
            }
        }
        
        return quality_assessment
    
    def _calculate_statistical_summary(self, X: np.ndarray, y: np.ndarray) -> Dict[str, Any]:
        """Calculate comprehensive statistical summary."""
        
        if X.size == 0 or y.size == 0:
            return {'error': 'No data available for statistical analysis'}
        
        # Features statistics
        if len(X.shape) > 2:
            # 3D data - flatten for statistics
            X_flat = X.reshape(-1, X.shape[-1])
        else:
            X_flat = X
        
        feature_stats = {}
        if X_flat.shape[1] > 0:
            for i in range(min(X_flat.shape[1], 10)):  # Limit to first 10 features
                feature_data = X_flat[:, i]
                feature_stats[f'feature_{i}'] = {
                    'mean': float(np.mean(feature_data)),
                    'std': float(np.std(feature_data)),
                    'min': float(np.min(feature_data)),
                    'max': float(np.max(feature_data)),
                    'median': float(np.median(feature_data)),
                    'q25': float(np.percentile(feature_data, 25)),
                    'q75': float(np.percentile(feature_data, 75))
                }
        
        # Target statistics
        target_stats = {
            'mean': float(np.mean(y)),
            'std': float(np.std(y)),
            'min': float(np.min(y)),
            'max': float(np.max(y)),
            'median': float(np.median(y)),
            'q25': float(np.percentile(y, 25)),
            'q75': float(np.percentile(y, 75)),
            'skewness': float(self._calculate_skewness(y)),
            'kurtosis': float(self._calculate_kurtosis(y))
        }
        
        summary = {
            'feature_statistics': feature_stats,
            'target_statistics': target_stats,
            'correlations': self._calculate_correlations(X_flat, y) if X_flat.shape[1] > 0 else {},
            'data_distribution': {
                'feature_variance_explained': self._calculate_variance_explained(X_flat) if X_flat.shape[1] > 1 else 0.0,
                'target_distribution_type': self._assess_distribution_type(y)
            }
        }
        
        return summary
    
    def _analyze_temporal_patterns(self, X: np.ndarray, y: np.ndarray, config: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze temporal patterns in the data."""
        
        if len(X.shape) < 3:
            return {'temporal_analysis': 'Not applicable - no sequence dimension'}
        
        sequence_length = X.shape[1]
        
        # Analyze temporal patterns
        temporal_stats = {
            'sequence_length': sequence_length,
            'temporal_variance': {},
            'trend_analysis': {},
            'seasonality_indicators': {}
        }
        
        # Calculate temporal variance for each feature
        for feature_idx in range(min(X.shape[2], 5)):  # First 5 features
            feature_data = X[:, :, feature_idx]  # All sequences, all time steps, one feature
            
            # Variance across time steps
            temporal_var = np.var(feature_data, axis=1)  # Variance within each sequence
            
            temporal_stats['temporal_variance'][f'feature_{feature_idx}'] = {
                'mean_temporal_variance': float(np.mean(temporal_var)),
                'std_temporal_variance': float(np.std(temporal_var)),
                'temporal_stability': float(1.0 / (1.0 + np.mean(temporal_var)))  # Inverse relationship
            }
        
        # Analyze trends in targets
        if len(y) > 1:
            # Simple trend analysis
            y_diff = np.diff(y)
            temporal_stats['trend_analysis'] = {
                'mean_change': float(np.mean(y_diff)),
                'volatility': float(np.std(y_diff)),
                'positive_changes': int(np.sum(y_diff > 0)),
                'negative_changes': int(np.sum(y_diff < 0)),
                'trend_strength': float(np.abs(np.mean(y_diff)) / np.std(y_diff)) if np.std(y_diff) > 0 else 0.0
            }
        
        return temporal_stats
    
    def _analyze_features(self, X: np.ndarray, config: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze individual features and their characteristics."""
        
        if len(X.shape) > 2:
            X_flat = X.reshape(-1, X.shape[-1])
        else:
            X_flat = X
        
        if X_flat.shape[1] == 0:
            return {'feature_analysis': 'No features to analyze'}
        
        feature_analysis = {
            'feature_count': X_flat.shape[1],
            'feature_importance_proxy': {},
            'feature_distributions': {},
            'feature_correlations': {},
            'technical_indicators': config.get('technical_indicators', [])
        }
        
        # Analyze each feature
        for i in range(min(X_flat.shape[1], 10)):  # Limit analysis
            feature_data = X_flat[:, i]
            
            feature_analysis['feature_distributions'][f'feature_{i}'] = {
                'unique_values': int(len(np.unique(feature_data))),
                'value_range': float(np.max(feature_data) - np.min(feature_data)),
                'coefficient_of_variation': float(np.std(feature_data) / np.mean(feature_data)) if np.mean(feature_data) != 0 else float('inf'),
                'outlier_percentage': float(self._calculate_outlier_percentage(feature_data))
            }
        
        return feature_analysis
    
    def _calculate_skewness(self, data: np.ndarray) -> float:
        """Calculate skewness of data."""
        if len(data) == 0:
            return 0.0
        mean = np.mean(data)
        std = np.std(data)
        if std == 0:
            return 0.0
        return np.mean(((data - mean) / std) ** 3)
    
    def _calculate_kurtosis(self, data: np.ndarray) -> float:
        """Calculate kurtosis of data."""
        if len(data) == 0:
            return 0.0
        mean = np.mean(data)
        std = np.std(data)
        if std == 0:
            return 0.0
        return np.mean(((data - mean) / std) ** 4) - 3
    
    def _calculate_correlations(self, X: np.ndarray, y: np.ndarray) -> Dict[str, float]:
        """Calculate feature correlations with target."""
        correlations = {}
        for i in range(min(X.shape[1], 10)):
            try:
                corr = np.corrcoef(X[:, i], y)[0, 1]
                correlations[f'feature_{i}'] = float(corr) if not np.isnan(corr) else 0.0
            except:
                correlations[f'feature_{i}'] = 0.0
        return correlations
    
    def _calculate_variance_explained(self, X: np.ndarray) -> float:
        """Calculate variance explained by first principal component."""
        try:
            if X.shape[1] > 1:
                cov_matrix = np.cov(X.T)
                eigenvals = np.linalg.eigvals(cov_matrix)
                return float(np.max(eigenvals) / np.sum(eigenvals))
            return 1.0
        except:
            return 0.0
    
    def _assess_distribution_type(self, data: np.ndarray) -> str:
        """Simple distribution type assessment."""
        skewness = self._calculate_skewness(data)
        kurtosis = self._calculate_kurtosis(data)
        
        if abs(skewness) < 0.5 and abs(kurtosis) < 0.5:
            return 'approximately_normal'
        elif skewness > 1:
            return 'right_skewed'
        elif skewness < -1:
            return 'left_skewed'
        elif kurtosis > 1:
            return 'heavy_tailed'
        else:
            return 'non_normal'
    
    def _calculate_outlier_percentage(self, data: np.ndarray) -> float:
        """Calculate percentage of outliers using IQR method."""
        q25, q75 = np.percentile(data, [25, 75])
        iqr = q75 - q25
        if iqr == 0:
            return 0.0
        
        lower_bound = q25 - 1.5 * iqr
        upper_bound = q75 + 1.5 * iqr
        
        outliers = np.sum((data < lower_bound) | (data > upper_bound))
        return (outliers / len(data)) * 100
    
    def generate_eda_report(self, dataset_id: int, output_file: Optional[str] = None) -> str:
        """Generate comprehensive EDA report."""
        
        logger.info(f"📋 Generating EDA report for dataset {dataset_id}")
        
        analysis = self.analyze_dataset(dataset_id, analysis_type='comprehensive')
        
        if not analysis:
            return "❌ Could not generate report - analysis failed"
        
        # Generate report
        report_lines = []
        report_lines.append("# Dataset EDA Report")
        report_lines.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report_lines.append("")
        
        # Dataset Info
        info = analysis['dataset_info']
        report_lines.append("## Dataset Information")
        report_lines.append(f"- **Dataset ID**: {info['dataset_id']}")
        report_lines.append(f"- **Name**: {info['dataset_name']}")
        report_lines.append(f"- **Symbols**: {', '.join(info['symbols'])}")
        report_lines.append(f"- **Sample Size**: {info['sample_size']:,} records")
        report_lines.append("")
        
        # Data Characteristics
        chars = analysis['data_characteristics']
        report_lines.append("## Data Characteristics")
        report_lines.append(f"- **Features Shape**: {chars['shape']['features_shape']}")
        report_lines.append(f"- **Feature Count**: {chars['shape']['feature_count']}")
        report_lines.append(f"- **Memory Usage**: {chars['data_types']['memory_usage_mb']:.1f} MB")
        report_lines.append(f"- **Data Quality Issues**: {chars['data_quality']['features_nan_count']} NaN, {chars['data_quality']['features_inf_count']} Inf")
        report_lines.append("")
        
        # Quality Assessment
        quality = analysis['quality_assessment']
        report_lines.append("## Quality Assessment")
        report_lines.append(f"- **Overall Quality Score**: {quality['quality_score']:.3f}")
        report_lines.append(f"- **Data Completeness**: {quality['completeness']:.3f}")
        report_lines.append(f"- **Data Validity**: {quality['validity']:.3f}")
        if quality['issues']:
            report_lines.append(f"- **Issues**: {', '.join(quality['issues'])}")
        report_lines.append("")
        
        # Statistical Summary
        stats = analysis['statistical_summary']
        if 'target_statistics' in stats:
            target = stats['target_statistics']
            report_lines.append("## Target Variable Statistics")
            report_lines.append(f"- **Mean**: {target['mean']:.6f}")
            report_lines.append(f"- **Std Dev**: {target['std']:.6f}")
            report_lines.append(f"- **Range**: [{target['min']:.6f}, {target['max']:.6f}]")
            report_lines.append(f"- **Distribution**: {stats['data_distribution']['target_distribution_type']}")
            report_lines.append("")
        
        # Temporal Analysis
        temporal = analysis['temporal_analysis']
        if 'sequence_length' in temporal:
            report_lines.append("## Temporal Analysis")
            report_lines.append(f"- **Sequence Length**: {temporal['sequence_length']}")
            if 'trend_analysis' in temporal:
                trend = temporal['trend_analysis']
                report_lines.append(f"- **Trend Strength**: {trend.get('trend_strength', 0):.3f}")
                report_lines.append(f"- **Volatility**: {trend.get('volatility', 0):.6f}")
            report_lines.append("")
        
        report_text = "\n".join(report_lines)
        
        if output_file:
            with open(output_file, 'w') as f:
                f.write(report_text)
            logger.info(f"✅ Report saved to {output_file}")
        
        return report_text

def main():
    """Demonstrate EDA with dataset service integration."""
    
    logger.info("🚀 Starting EDA with Dataset Service")
    
    # Initialize dataset client (single source of truth)
    client = DatasetClient()
    
    # Initialize EDA analyzer that uses the client
    eda = EDAAnalyzer(client)
    
    try:
        # Explore available datasets
        logger.info("📊 Step 1: Exploring available datasets")
        datasets_overview = eda.explore_available_datasets(['AAPL', 'TSLA'])
        
        if not datasets_overview.empty:
            print("\n📋 Available Datasets:")
            print(datasets_overview[['id', 'name', 'symbols', 'size', 'quality', 'created']].to_string(index=False))
            
            # Analyze the best quality dataset
            best_dataset = datasets_overview.loc[datasets_overview['quality_pct'].idxmax()]
            dataset_id = int(best_dataset['id'])
            
            logger.info(f"🔍 Step 2: Analyzing dataset {dataset_id} ({best_dataset['name']})")
            
            # Generate comprehensive report
            report = eda.generate_eda_report(dataset_id)
            
            print(f"\n📋 EDA Report for Dataset {dataset_id}:")
            print("=" * 60)
            print(report)
            
            # Save report to file
            report_file = f"eda_report_dataset_{dataset_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.md"
            with open(report_file, 'w') as f:
                f.write(report)
            
            logger.info(f"✅ EDA completed successfully")
            logger.info(f"📄 Report saved: {report_file}")
            
        else:
            logger.warning("⚠️ No datasets found for analysis")
            logger.info("Ensure datasets are registered in the dataset service")
            
    except Exception as e:
        logger.error(f"❌ EDA with dataset service failed: {e}")
        raise

if __name__ == "__main__":
    main()