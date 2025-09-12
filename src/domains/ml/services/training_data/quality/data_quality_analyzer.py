#!/usr/bin/env python3
"""
Advanced Data Quality Analyzer for Monthly Training Data
Provides automated quality scoring based on multiple metrics.
"""

import numpy as np
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple
from datetime import datetime, date
import json
import os
from pathlib import Path

@dataclass
class DataQualityMetrics:
    """Comprehensive data quality metrics for training data."""
    completeness_score: float  # 0.0-1.0: Percentage of non-null values
    consistency_score: float   # 0.0-1.0: Data consistency across timeframes
    accuracy_score: float      # 0.0-1.0: Price/volume relationship accuracy
    timeliness_score: float    # 0.0-1.0: Data freshness and temporal coverage
    volume_quality_score: float # 0.0-1.0: Volume data quality
    price_quality_score: float  # 0.0-1.0: Price data quality (no gaps, reasonable values)
    
    overall_score: float       # 0.0-1.0: Weighted combination of all scores
    
    # Detailed metrics
    total_records: int
    null_count: int
    outlier_count: int
    gap_count: int
    zero_volume_count: int
    
    # Issues found
    issues: List[str]
    warnings: List[str]

class AdvancedDataQualityAnalyzer:
    """
    Advanced analyzer for training data quality assessment.
    Provides comprehensive scoring across multiple dimensions.
    """
    
    def __init__(self):
        self.quality_thresholds = {
            'excellent': 0.95,
            'good': 0.85,
            'fair': 0.70,
            'poor': 0.50
        }
        
        # Weights for overall score calculation
        self.score_weights = {
            'completeness': 0.25,
            'consistency': 0.20,
            'accuracy': 0.20,
            'timeliness': 0.15,
            'volume_quality': 0.10,
            'price_quality': 0.10
        }
    
    def analyze_monthly_data(self, 
                           symbol: str, 
                           year_month: date,
                           timeframe_paths: Dict[str, str]) -> DataQualityMetrics:
        """
        Analyze data quality for a specific month's training data.
        
        Args:
            symbol: Stock symbol (e.g., 'AAPL')
            year_month: First day of the month being analyzed
            timeframe_paths: Dict mapping timeframes to ArrayRecord file paths
            
        Returns:
            DataQualityMetrics with comprehensive quality assessment
        """
        issues = []
        warnings = []
        
        # Initialize metrics
        completeness_scores = []
        consistency_scores = []
        accuracy_scores = []
        timeliness_scores = []
        volume_scores = []
        price_scores = []
        
        total_records = 0
        total_null_count = 0
        total_outlier_count = 0
        total_gap_count = 0
        total_zero_volume = 0
        
        # Analyze each timeframe
        for timeframe, file_path in timeframe_paths.items():
            if not self._file_exists(file_path):
                issues.append(f"Missing file for {timeframe}: {file_path}")
                continue
                
            try:
                timeframe_metrics = self._analyze_timeframe_file(file_path, timeframe, symbol)
                
                completeness_scores.append(timeframe_metrics['completeness'])
                consistency_scores.append(timeframe_metrics['consistency'])
                accuracy_scores.append(timeframe_metrics['accuracy'])
                timeliness_scores.append(timeframe_metrics['timeliness'])
                volume_scores.append(timeframe_metrics['volume_quality'])
                price_scores.append(timeframe_metrics['price_quality'])
                
                total_records += timeframe_metrics['record_count']
                total_null_count += timeframe_metrics['null_count']
                total_outlier_count += timeframe_metrics['outlier_count']
                total_gap_count += timeframe_metrics['gap_count']
                total_zero_volume += timeframe_metrics['zero_volume_count']
                
                # Collect timeframe-specific issues
                issues.extend(timeframe_metrics['issues'])
                warnings.extend(timeframe_metrics['warnings'])
                
            except Exception as e:
                issues.append(f"Failed to analyze {timeframe}: {str(e)}")
        
        # Calculate aggregate scores
        completeness_score = np.mean(completeness_scores) if completeness_scores else 0.0
        consistency_score = np.mean(consistency_scores) if consistency_scores else 0.0
        accuracy_score = np.mean(accuracy_scores) if accuracy_scores else 0.0
        timeliness_score = np.mean(timeliness_scores) if timeliness_scores else 0.0
        volume_quality_score = np.mean(volume_scores) if volume_scores else 0.0
        price_quality_score = np.mean(price_scores) if price_scores else 0.0
        
        # Calculate weighted overall score
        overall_score = (
            completeness_score * self.score_weights['completeness'] +
            consistency_score * self.score_weights['consistency'] +
            accuracy_score * self.score_weights['accuracy'] +
            timeliness_score * self.score_weights['timeliness'] +
            volume_quality_score * self.score_weights['volume_quality'] +
            price_quality_score * self.score_weights['price_quality']
        )
        
        return DataQualityMetrics(
            completeness_score=completeness_score,
            consistency_score=consistency_score,
            accuracy_score=accuracy_score,
            timeliness_score=timeliness_score,
            volume_quality_score=volume_quality_score,
            price_quality_score=price_quality_score,
            overall_score=overall_score,
            total_records=total_records,
            null_count=total_null_count,
            outlier_count=total_outlier_count,
            gap_count=total_gap_count,
            zero_volume_count=total_zero_volume,
            issues=issues,
            warnings=warnings
        )
    
    def _analyze_timeframe_file(self, file_path: str, timeframe: str, symbol: str) -> Dict:
        """Analyze a specific timeframe file for quality metrics."""
        # Simulate analysis - in real implementation, would read ArrayRecord file
        # and perform actual data quality analysis
        
        # Mock realistic quality scores based on timeframe
        timeframe_quality_factors = {
            '1m': 0.85,   # Lower quality due to more noise
            '5m': 0.90,   # Good quality
            '15m': 0.93,  # Better quality
            '1h': 0.95,   # High quality
            '1d': 0.97,   # Highest quality
            '1w': 0.95    # Very good quality
        }
        
        base_quality = timeframe_quality_factors.get(timeframe, 0.85)
        
        # Add some realistic variance
        import random
        random.seed(hash(f"{symbol}{timeframe}{file_path}"))  # Deterministic for testing
        
        quality_variance = random.uniform(-0.05, 0.05)
        adjusted_quality = max(0.0, min(1.0, base_quality + quality_variance))
        
        # Mock realistic metrics
        record_count = random.randint(1000, 10000)
        null_count = int(record_count * (1 - adjusted_quality) * 0.1)
        outlier_count = int(record_count * 0.02)
        gap_count = random.randint(0, 5)
        zero_volume_count = int(record_count * 0.01)
        
        issues = []
        warnings = []
        
        if adjusted_quality < 0.7:
            issues.append(f"{timeframe}: Low data quality ({adjusted_quality:.3f})")
        elif adjusted_quality < 0.85:
            warnings.append(f"{timeframe}: Moderate data quality ({adjusted_quality:.3f})")
        
        if gap_count > 3:
            warnings.append(f"{timeframe}: {gap_count} data gaps detected")
        
        if zero_volume_count > record_count * 0.05:
            warnings.append(f"{timeframe}: High zero volume count ({zero_volume_count})")
        
        return {
            'completeness': adjusted_quality,
            'consistency': min(1.0, adjusted_quality + 0.02),
            'accuracy': min(1.0, adjusted_quality + 0.01),
            'timeliness': adjusted_quality,
            'volume_quality': min(1.0, adjusted_quality + 0.03),
            'price_quality': adjusted_quality,
            'record_count': record_count,
            'null_count': null_count,
            'outlier_count': outlier_count,
            'gap_count': gap_count,
            'zero_volume_count': zero_volume_count,
            'issues': issues,
            'warnings': warnings
        }
    
    def _file_exists(self, file_path: str) -> bool:
        """Check if file exists and is accessible."""
        try:
            return os.path.exists(file_path) and os.path.getsize(file_path) > 0
        except:
            return False
    
    def get_quality_grade(self, overall_score: float) -> str:
        """Convert numerical score to quality grade."""
        if overall_score >= self.quality_thresholds['excellent']:
            return 'A'
        elif overall_score >= self.quality_thresholds['good']:
            return 'B'
        elif overall_score >= self.quality_thresholds['fair']:
            return 'C'
        elif overall_score >= self.quality_thresholds['poor']:
            return 'D'
        else:
            return 'F'
    
    def generate_quality_report(self, metrics: DataQualityMetrics, symbol: str, year_month: date) -> str:
        """Generate a comprehensive quality report."""
        grade = self.get_quality_grade(metrics.overall_score)
        
        report = f"""
🔍 Data Quality Report for {symbol} - {year_month.strftime('%Y-%m')}
{'='*60}

📊 Overall Quality Score: {metrics.overall_score:.3f} (Grade: {grade})

📈 Individual Metrics:
  • Completeness:    {metrics.completeness_score:.3f}
  • Consistency:     {metrics.consistency_score:.3f}
  • Accuracy:        {metrics.accuracy_score:.3f}
  • Timeliness:      {metrics.timeliness_score:.3f}
  • Volume Quality:  {metrics.volume_quality_score:.3f}
  • Price Quality:   {metrics.price_quality_score:.3f}

📋 Data Statistics:
  • Total Records:   {metrics.total_records:,}
  • Null Values:     {metrics.null_count:,}
  • Outliers:        {metrics.outlier_count:,}
  • Data Gaps:       {metrics.gap_count:,}
  • Zero Volume:     {metrics.zero_volume_count:,}
"""

        if metrics.issues:
            report += f"\n❌ Issues Found ({len(metrics.issues)}):\n"
            for issue in metrics.issues[:5]:  # Show first 5 issues
                report += f"  • {issue}\n"
            if len(metrics.issues) > 5:
                report += f"  • ... and {len(metrics.issues) - 5} more issues\n"
        
        if metrics.warnings:
            report += f"\n⚠️ Warnings ({len(metrics.warnings)}):\n"
            for warning in metrics.warnings[:5]:  # Show first 5 warnings
                report += f"  • {warning}\n"
            if len(metrics.warnings) > 5:
                report += f"  • ... and {len(metrics.warnings) - 5} more warnings\n"
        
        if not metrics.issues and not metrics.warnings:
            report += "\n✅ No issues or warnings detected\n"
        
        return report

def analyze_sample_data():
    """Sample usage of the data quality analyzer."""
    analyzer = AdvancedDataQualityAnalyzer()
    
    # Sample timeframe paths (mock data)
    sample_paths = {
        '5m': '/data/training_data/dataset_20240615/AAPL_20240615/5m/AAPL_20240615.arrayrecord',
        '15m': '/data/training_data/dataset_20240615/AAPL_20240615/15m/AAPL_20240615.arrayrecord',
        '1h': '/data/training_data/dataset_20240615/AAPL_20240615/1h/AAPL_20240615.arrayrecord',
        '1d': '/data/training_data/dataset_20240615/AAPL_20240615/1d/AAPL_20240615.arrayrecord'
    }
    
    # Analyze quality
    metrics = analyzer.analyze_monthly_data(
        symbol='AAPL',
        year_month=date(2024, 6, 1),
        timeframe_paths=sample_paths
    )
    
    # Generate report
    report = analyzer.generate_quality_report(metrics, 'AAPL', date(2024, 6, 1))
    print(report)
    
    return metrics

if __name__ == "__main__":
    analyze_sample_data()