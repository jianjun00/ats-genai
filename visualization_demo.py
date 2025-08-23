#!/usr/bin/env python3
"""
Multi-Timeframe Training Data Visualization Demo

Creates interactive visualizations of our enhanced multi-timeframe training data
to demonstrate the system's capabilities and validate data quality.
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

import numpy as np
import pandas as pd
import json
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
from pathlib import Path

try:
    from modeling.enhanced_feature_types import (
        FeatureSpecification, FeatureType, TimeframeSpec, 
        TechnicalIndicator, EnhancedFeatureRegistry
    )
except ImportError as e:
    print(f"❌ Import error: {e}")
    print("Please ensure src/ directory is in PYTHONPATH")
    sys.exit(1)


class MultiTimeframeVisualization:
    """Creates comprehensive visualizations for multi-timeframe training data."""
    
    def __init__(self, data_dir: str = "manual_verification_output"):
        print("📊 Multi-Timeframe Training Data Visualization")
        print("=" * 50)
        
        self.data_dir = data_dir
        self.registry = EnhancedFeatureRegistry()
        
        # Load verification data
        self.metadata = self.load_metadata()
        self.data_cache = {}
        
        # Set up matplotlib for better plots
        plt.style.use('default')
        plt.rcParams['figure.figsize'] = (15, 10)
        plt.rcParams['font.size'] = 10
        
    def load_metadata(self) -> dict:
        """Load metadata from verification data."""
        metadata_path = os.path.join(self.data_dir, 'verification_metadata.json')
        
        if not os.path.exists(metadata_path):
            print(f"❌ Metadata file not found: {metadata_path}")
            print("Please run manual_verification_demo.py first")
            sys.exit(1)
        
        with open(metadata_path, 'r') as f:
            metadata = json.load(f)
        
        print(f"✅ Loaded metadata for {len(metadata['symbols'])} symbols")
        return metadata
    
    def load_feature_data(self, symbol: str, feature_name: str) -> np.ndarray:
        """Load feature data for a specific symbol and feature."""
        cache_key = f"{symbol}_{feature_name}"
        
        if cache_key in self.data_cache:
            return self.data_cache[cache_key]
        
        feature_path = os.path.join(self.data_dir, symbol.lower(), f"{feature_name}.npy")
        
        if not os.path.exists(feature_path):
            print(f"⚠️  Feature file not found: {feature_path}")
            return None
        
        data = np.load(feature_path)
        self.data_cache[cache_key] = data
        return data
    
    def load_labels(self, symbol: str) -> np.ndarray:
        """Load labels for a symbol."""
        labels_path = os.path.join(self.data_dir, f"{symbol.lower()}_labels.npy")
        
        if not os.path.exists(labels_path):
            print(f"⚠️  Labels file not found: {labels_path}")
            return None
        
        return np.load(labels_path)
    
    def create_ohlc_candlestick_plot(self, symbol: str, sample_idx: int = 0):
        """Create OHLC candlestick plot for a specific sample."""
        
        ohlc_data = self.load_feature_data(symbol, 'ohlc_daily_16')
        
        if ohlc_data is None:
            print(f"❌ Cannot create OHLC plot for {symbol}")
            return None
        
        # Extract single sample
        sample_ohlc = ohlc_data[sample_idx]  # Shape: (16, 4)
        
        fig, ax = plt.subplots(figsize=(12, 6))
        
        # Create candlestick-style plot
        for i in range(len(sample_ohlc)):
            open_price, high, low, close = sample_ohlc[i]
            
            # Color: green if close > open, red otherwise
            color = 'green' if close >= open_price else 'red'
            
            # Draw high-low line
            ax.plot([i, i], [low, high], color='black', linewidth=1)
            
            # Draw body
            body_height = abs(close - open_price)
            body_bottom = min(close, open_price)
            
            ax.bar(i, body_height, bottom=body_bottom, 
                   color=color, alpha=0.7, width=0.8)
        
        ax.set_title(f'{symbol} OHLC Candlestick - Sample {sample_idx}', fontsize=14, fontweight='bold')
        ax.set_xlabel('Time Interval')
        ax.set_ylabel('Price')
        ax.grid(True, alpha=0.3)
        
        return fig
    
    def create_technical_indicators_overlay(self, symbol: str, sample_idx: int = 0):
        """Create technical indicators overlay plot."""
        
        # Load OHLC data for price reference
        ohlc_data = self.load_feature_data(symbol, 'ohlc_daily_16')
        
        # Load technical indicators
        indicators = {}
        indicator_names = ['etop_daily_16', 'ebot_daily_16', 'pldot_daily_16', 'ema_daily_16']
        
        for indicator_name in indicator_names:
            data = self.load_feature_data(symbol, indicator_name)
            if data is not None:
                indicators[indicator_name] = data[sample_idx].flatten()
        
        if ohlc_data is None or not indicators:
            print(f"❌ Cannot create indicators plot for {symbol}")
            return None
        
        sample_ohlc = ohlc_data[sample_idx]
        closes = sample_ohlc[:, 3]  # Close prices
        
        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 10), 
                                       gridspec_kw={'height_ratios': [3, 1]})
        
        # Main price plot with indicators
        x_axis = range(len(closes))
        
        # Plot close prices
        ax1.plot(x_axis, closes, 'b-', linewidth=2, label='Close Price')
        
        # Plot technical indicators
        colors = {'etop_daily_16': '#FF5722', 'ebot_daily_16': '#4CAF50', 
                 'pldot_daily_16': '#2196F3', 'ema_daily_16': '#9C27B0'}
        
        for indicator_name, values in indicators.items():
            if len(values) == len(closes):
                color = colors.get(indicator_name, '#666666')
                display_name = indicator_name.replace('_daily_16', '').upper()
                ax1.plot(x_axis, values, color=color, linewidth=1.5, 
                        label=display_name, alpha=0.8)
        
        ax1.set_title(f'{symbol} Price with Technical Indicators - Sample {sample_idx}', 
                     fontsize=14, fontweight='bold')
        ax1.set_ylabel('Price')
        ax1.legend(loc='upper left')
        ax1.grid(True, alpha=0.3)
        
        # RSI subplot (if available)
        rsi_data = self.load_feature_data(symbol, 'rsi_daily_16')
        if rsi_data is not None:
            rsi_values = rsi_data[sample_idx].flatten()
            ax2.plot(x_axis, rsi_values, 'purple', linewidth=1.5, label='RSI')
            ax2.axhline(y=70, color='r', linestyle='--', alpha=0.7, label='Overbought')
            ax2.axhline(y=30, color='g', linestyle='--', alpha=0.7, label='Oversold')
            ax2.set_ylim(0, 100)
            ax2.set_ylabel('RSI')
            ax2.legend(loc='upper right')
            ax2.grid(True, alpha=0.3)
        
        ax2.set_xlabel('Time Interval')
        
        plt.tight_layout()
        return fig
    
    def create_cross_timeframe_comparison(self, symbol: str, sample_idx: int = 0):
        """Create cross-timeframe feature comparison plot."""
        
        # Load daily and cross-timeframe versions
        daily_etop = self.load_feature_data(symbol, 'etop_daily_16')
        cross_etop = self.load_feature_data(symbol, 'etop_daily_on_5min')
        
        if daily_etop is None or cross_etop is None:
            print(f"❌ Cannot create cross-timeframe plot for {symbol}")
            return None
        
        daily_sample = daily_etop[sample_idx].flatten()
        cross_sample = cross_etop[sample_idx].flatten()
        
        fig, (ax1, ax2, ax3) = plt.subplots(3, 1, figsize=(14, 12))
        
        # Daily ETOP
        ax1.plot(range(len(daily_sample)), daily_sample, 'r-', 
                linewidth=2, marker='o', markersize=4, label='Daily ETOP')
        ax1.set_title(f'{symbol} - Daily ETOP (16 intervals)', fontweight='bold')
        ax1.set_ylabel('ETOP Value')
        ax1.legend()
        ax1.grid(True, alpha=0.3)
        
        # Cross-timeframe ETOP (aligned to 5-minute)
        ax2.plot(range(len(cross_sample)), cross_sample, 'b-', 
                linewidth=1.5, alpha=0.7, label='ETOP Daily-on-5min')
        ax2.set_title(f'{symbol} - Cross-Timeframe ETOP (32 intervals)', fontweight='bold')
        ax2.set_ylabel('ETOP Value')
        ax2.legend()
        ax2.grid(True, alpha=0.3)
        
        # Comparison: Resampled daily vs cross-timeframe
        # Resample daily to match cross-timeframe length for comparison
        daily_resampled = np.repeat(daily_sample, len(cross_sample) // len(daily_sample))
        
        ax3.plot(range(len(cross_sample)), cross_sample, 'b-', 
                alpha=0.7, label='Cross-timeframe Aligned')
        ax3.plot(range(len(daily_resampled)), daily_resampled, 'r--', 
                alpha=0.7, label='Daily Resampled')
        ax3.set_title(f'{symbol} - Alignment Comparison', fontweight='bold')
        ax3.set_xlabel('Time Interval (5-minute equivalent)')
        ax3.set_ylabel('ETOP Value')
        ax3.legend()
        ax3.grid(True, alpha=0.3)
        
        plt.tight_layout()
        return fig
    
    def create_feature_distribution_analysis(self, symbol: str):
        """Create feature distribution analysis plots."""
        
        feature_names = ['etop_daily_16', 'ebot_daily_16', 'rsi_daily_16', 'pldot_daily_16']
        
        fig, axes = plt.subplots(2, 2, figsize=(15, 10))
        axes = axes.flatten()
        
        for i, feature_name in enumerate(feature_names):
            data = self.load_feature_data(symbol, feature_name)
            if data is None:
                continue
            
            # Flatten all samples to get distribution
            flattened = data.flatten()
            flattened = flattened[~np.isnan(flattened)]  # Remove NaN values
            
            ax = axes[i]
            
            # Create histogram
            ax.hist(flattened, bins=30, alpha=0.7, color=plt.cm.tab10(i), 
                   density=True, edgecolor='black', linewidth=0.5)
            
            # Add statistics
            mean_val = np.mean(flattened)
            std_val = np.std(flattened)
            
            ax.axvline(mean_val, color='red', linestyle='--', linewidth=2, 
                      label=f'Mean: {mean_val:.2f}')
            ax.axvline(mean_val + std_val, color='orange', linestyle=':', 
                      label=f'+1σ: {mean_val + std_val:.2f}')
            ax.axvline(mean_val - std_val, color='orange', linestyle=':', 
                      label=f'-1σ: {mean_val - std_val:.2f}')
            
            display_name = feature_name.replace('_daily_16', '').upper()
            ax.set_title(f'{symbol} {display_name} Distribution')
            ax.set_xlabel('Value')
            ax.set_ylabel('Density')
            ax.legend()
            ax.grid(True, alpha=0.3)
        
        plt.suptitle(f'{symbol} Feature Distributions Analysis', 
                    fontsize=16, fontweight='bold')
        plt.tight_layout()
        return fig
    
    def create_sample_sequence_heatmap(self, symbol: str, feature_name: str = 'ohlc_daily_16'):
        """Create heatmap showing multiple samples of a feature."""
        
        data = self.load_feature_data(symbol, feature_name)
        if data is None:
            return None
        
        # Take first 20 samples for visualization
        num_samples = min(20, data.shape[0])
        
        if feature_name.startswith('ohlc'):
            # For OHLC, show close prices only
            heatmap_data = data[:num_samples, :, 3]  # Close prices
            title_suffix = "(Close Prices)"
        else:
            # For indicators, flatten the single dimension
            heatmap_data = data[:num_samples, :, 0]
            title_suffix = ""
        
        fig, ax = plt.subplots(figsize=(12, 8))
        
        im = ax.imshow(heatmap_data, cmap='RdYlBu_r', aspect='auto')
        
        ax.set_title(f'{symbol} {feature_name.upper()} Sequence Heatmap {title_suffix}', 
                    fontsize=14, fontweight='bold')
        ax.set_xlabel('Time Interval')
        ax.set_ylabel('Sample Index')
        
        # Add colorbar
        cbar = plt.colorbar(im, ax=ax)
        cbar.set_label('Value')
        
        # Add grid
        ax.set_xticks(range(0, heatmap_data.shape[1], 2))
        ax.set_yticks(range(0, num_samples, 2))
        ax.grid(True, alpha=0.3)
        
        return fig
    
    def create_comprehensive_dashboard(self, symbol: str = 'AAPL', sample_idx: int = 0):
        """Create comprehensive multi-panel dashboard."""
        
        print(f"📊 Creating comprehensive dashboard for {symbol}...")
        
        # Create figure with complex grid layout
        fig = plt.figure(figsize=(20, 15))
        gs = gridspec.GridSpec(3, 3, figure=fig, hspace=0.3, wspace=0.3)
        
        # Load data
        ohlc_data = self.load_feature_data(symbol, 'ohlc_daily_16')
        etop_data = self.load_feature_data(symbol, 'etop_daily_16')
        ebot_data = self.load_feature_data(symbol, 'ebot_daily_16')
        rsi_data = self.load_feature_data(symbol, 'rsi_daily_16')
        cross_etop = self.load_feature_data(symbol, 'etop_daily_on_5min')
        
        if not all([d is not None for d in [ohlc_data, etop_data, ebot_data, rsi_data, cross_etop]]):
            print(f"❌ Missing data for comprehensive dashboard")
            return None
        
        # Panel 1: OHLC Candlestick (top-left)
        ax1 = fig.add_subplot(gs[0, 0])
        sample_ohlc = ohlc_data[sample_idx]
        closes = sample_ohlc[:, 3]
        ax1.plot(closes, 'b-', linewidth=2, marker='o', markersize=3)
        ax1.set_title(f'{symbol} Close Prices', fontweight='bold')
        ax1.grid(True, alpha=0.3)
        
        # Panel 2: Technical Indicators (top-middle)
        ax2 = fig.add_subplot(gs[0, 1])
        etop_values = etop_data[sample_idx].flatten()
        ebot_values = ebot_data[sample_idx].flatten()
        ax2.plot(etop_values, 'r-', label='ETOP', linewidth=1.5)
        ax2.plot(ebot_values, 'g-', label='EBOT', linewidth=1.5)
        ax2.plot(closes, 'b-', alpha=0.7, label='Close')
        ax2.set_title('Price with Envelopes', fontweight='bold')
        ax2.legend()
        ax2.grid(True, alpha=0.3)
        
        # Panel 3: RSI (top-right)
        ax3 = fig.add_subplot(gs[0, 2])
        rsi_values = rsi_data[sample_idx].flatten()
        ax3.plot(rsi_values, 'purple', linewidth=2)
        ax3.axhline(y=70, color='r', linestyle='--', alpha=0.7)
        ax3.axhline(y=30, color='g', linestyle='--', alpha=0.7)
        ax3.set_ylim(0, 100)
        ax3.set_title('RSI Oscillator', fontweight='bold')
        ax3.grid(True, alpha=0.3)
        
        # Panel 4: Cross-timeframe Comparison (middle-left)
        ax4 = fig.add_subplot(gs[1, 0])
        cross_values = cross_etop[sample_idx].flatten()
        ax4.plot(cross_values, 'b-', alpha=0.7, linewidth=1.5)
        ax4.set_title('Cross-Timeframe ETOP', fontweight='bold')
        ax4.grid(True, alpha=0.3)
        
        # Panel 5: Feature Correlation Matrix (middle-middle)
        ax5 = fig.add_subplot(gs[1, 1])
        
        # Create correlation matrix from current sample
        sample_features = np.column_stack([
            closes, etop_values, ebot_values, rsi_values
        ])
        
        corr_matrix = np.corrcoef(sample_features.T)
        im = ax5.imshow(corr_matrix, cmap='RdBu_r', vmin=-1, vmax=1)
        
        feature_labels = ['Close', 'ETOP', 'EBOT', 'RSI']
        ax5.set_xticks(range(len(feature_labels)))
        ax5.set_yticks(range(len(feature_labels)))
        ax5.set_xticklabels(feature_labels)
        ax5.set_yticklabels(feature_labels)
        ax5.set_title('Feature Correlation', fontweight='bold')
        
        # Add correlation values
        for i in range(len(feature_labels)):
            for j in range(len(feature_labels)):
                text = ax5.text(j, i, f'{corr_matrix[i, j]:.2f}',
                               ha="center", va="center", color="black", fontweight='bold')
        
        # Panel 6: Data Quality Metrics (middle-right)
        ax6 = fig.add_subplot(gs[1, 2])
        
        # Calculate quality metrics
        all_ohlc = ohlc_data.flatten()
        quality_metrics = {
            'Valid Samples': f"{len(all_ohlc[~np.isnan(all_ohlc)]):,}",
            'Missing Values': f"{len(all_ohlc[np.isnan(all_ohlc)]):,}",
            'Value Range': f"${np.min(all_ohlc[~np.isnan(all_ohlc)]):.2f} - ${np.max(all_ohlc[~np.isnan(all_ohlc)]):.2f}",
            'Data Quality': f"{(1 - len(all_ohlc[np.isnan(all_ohlc)]) / len(all_ohlc)) * 100:.1f}%"
        }
        
        y_pos = 0.8
        for metric, value in quality_metrics.items():
            ax6.text(0.1, y_pos, f"{metric}:", fontweight='bold', transform=ax6.transAxes)
            ax6.text(0.6, y_pos, value, transform=ax6.transAxes)
            y_pos -= 0.15
        
        ax6.set_xlim(0, 1)
        ax6.set_ylim(0, 1)
        ax6.axis('off')
        ax6.set_title('Data Quality', fontweight='bold')
        
        # Panel 7: Feature Shapes Summary (bottom-left)
        ax7 = fig.add_subplot(gs[2, 0])
        
        shape_info = [
            f"OHLC: {ohlc_data.shape}",
            f"ETOP: {etop_data.shape}",
            f"Cross-TF: {cross_etop.shape}",
            f"Sample: {sample_idx}/{ohlc_data.shape[0]}"
        ]
        
        y_pos = 0.8
        for info in shape_info:
            ax7.text(0.1, y_pos, info, transform=ax7.transAxes, fontfamily='monospace')
            y_pos -= 0.2
        
        ax7.set_xlim(0, 1)
        ax7.set_ylim(0, 1)
        ax7.axis('off')
        ax7.set_title('Shape Information', fontweight='bold')
        
        # Panel 8: Labels Distribution (bottom-middle)
        ax8 = fig.add_subplot(gs[2, 1])
        
        labels = self.load_labels(symbol)
        if labels is not None:
            unique, counts = np.unique(labels, return_counts=True)
            bars = ax8.bar(unique, counts, color=['red', 'green'], alpha=0.7)
            ax8.set_title('Label Distribution', fontweight='bold')
            ax8.set_xlabel('Label')
            ax8.set_ylabel('Count')
            
            # Add value labels on bars
            for bar, count in zip(bars, counts):
                height = bar.get_height()
                ax8.text(bar.get_x() + bar.get_width()/2., height + height*0.02,
                        f'{count}', ha='center', va='bottom')
        
        # Panel 9: System Information (bottom-right)
        ax9 = fig.add_subplot(gs[2, 2])
        
        system_info = [
            f"Symbol: {symbol}",
            f"Created: {self.metadata['creation_time'][:10]}",
            f"Features: {len(self.metadata['feature_types'])}",
            f"Registry: {len(self.registry.feature_specs)} specs"
        ]
        
        y_pos = 0.8
        for info in system_info:
            ax9.text(0.1, y_pos, info, transform=ax9.transAxes)
            y_pos -= 0.2
        
        ax9.set_xlim(0, 1)
        ax9.set_ylim(0, 1)
        ax9.axis('off')
        ax9.set_title('System Info', fontweight='bold')
        
        # Main title
        plt.suptitle(f'Enhanced Multi-Timeframe Training Data Dashboard - {symbol}', 
                    fontsize=18, fontweight='bold', y=0.98)
        
        return fig
    
    def save_all_visualizations(self, output_dir: str = "visualization_output"):
        """Generate and save all visualizations."""
        
        print(f"🎨 Generating comprehensive visualizations...")
        os.makedirs(output_dir, exist_ok=True)
        
        symbols = self.metadata['symbols']
        
        for symbol in symbols:
            print(f"  📊 Creating visualizations for {symbol}...")
            
            # 1. Comprehensive Dashboard
            dashboard = self.create_comprehensive_dashboard(symbol, sample_idx=0)
            if dashboard:
                dashboard.savefig(os.path.join(output_dir, f'{symbol.lower()}_dashboard.png'), 
                                dpi=150, bbox_inches='tight')
                plt.close(dashboard)
            
            # 2. OHLC Candlestick
            candlestick = self.create_ohlc_candlestick_plot(symbol, sample_idx=0)
            if candlestick:
                candlestick.savefig(os.path.join(output_dir, f'{symbol.lower()}_ohlc.png'), 
                                  dpi=150, bbox_inches='tight')
                plt.close(candlestick)
            
            # 3. Technical Indicators
            indicators = self.create_technical_indicators_overlay(symbol, sample_idx=0)
            if indicators:
                indicators.savefig(os.path.join(output_dir, f'{symbol.lower()}_indicators.png'), 
                                 dpi=150, bbox_inches='tight')
                plt.close(indicators)
            
            # 4. Cross-timeframe Comparison
            cross_tf = self.create_cross_timeframe_comparison(symbol, sample_idx=0)
            if cross_tf:
                cross_tf.savefig(os.path.join(output_dir, f'{symbol.lower()}_cross_timeframe.png'), 
                               dpi=150, bbox_inches='tight')
                plt.close(cross_tf)
            
            # 5. Feature Distributions
            distributions = self.create_feature_distribution_analysis(symbol)
            if distributions:
                distributions.savefig(os.path.join(output_dir, f'{symbol.lower()}_distributions.png'), 
                                    dpi=150, bbox_inches='tight')
                plt.close(distributions)
            
            # 6. Sequence Heatmap
            heatmap = self.create_sample_sequence_heatmap(symbol, 'etop_daily_16')
            if heatmap:
                heatmap.savefig(os.path.join(output_dir, f'{symbol.lower()}_heatmap.png'), 
                              dpi=150, bbox_inches='tight')
                plt.close(heatmap)
        
        print(f"✅ All visualizations saved to {os.path.abspath(output_dir)}")
        
        # Create index file
        self.create_visualization_index(output_dir)
    
    def create_visualization_index(self, output_dir: str):
        """Create HTML index file for all visualizations."""
        
        html_content = f"""
<!DOCTYPE html>
<html>
<head>
    <title>Enhanced Multi-Timeframe Training Data Visualizations</title>
    <style>
        body {{ font-family: Arial, sans-serif; margin: 40px; background: #f5f5f5; }}
        h1 {{ color: #333; text-align: center; }}
        h2 {{ color: #666; border-bottom: 2px solid #ddd; }}
        .symbol-section {{ margin: 30px 0; background: white; padding: 20px; border-radius: 8px; }}
        .viz-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(400px, 1fr)); gap: 20px; }}
        .viz-item {{ text-align: center; }}
        .viz-item img {{ max-width: 100%; height: auto; border: 1px solid #ddd; border-radius: 4px; }}
        .viz-item h3 {{ margin: 10px 0 5px 0; }}
        .metadata {{ background: #e8f4f8; padding: 15px; border-radius: 4px; margin: 20px 0; }}
    </style>
</head>
<body>
    <h1>🚀 Enhanced Multi-Timeframe Training Data Visualizations</h1>
    
    <div class="metadata">
        <h3>📊 Dataset Information</h3>
        <p><strong>Created:</strong> {self.metadata['creation_time']}</p>
        <p><strong>Symbols:</strong> {', '.join(self.metadata['symbols'])}</p>
        <p><strong>Total Features:</strong> {len(self.metadata['feature_types'])}</p>
        <p><strong>Feature Types:</strong> OHLC Intervals, Price Indicators, Cross-Timeframe Indicators</p>
    </div>
"""
        
        for symbol in self.metadata['symbols']:
            html_content += f"""
    <div class="symbol-section">
        <h2>📈 {symbol} Visualizations</h2>
        <div class="viz-grid">
            <div class="viz-item">
                <h3>Comprehensive Dashboard</h3>
                <img src="{symbol.lower()}_dashboard.png" alt="{symbol} Dashboard">
            </div>
            <div class="viz-item">
                <h3>OHLC Candlestick</h3>
                <img src="{symbol.lower()}_ohlc.png" alt="{symbol} OHLC">
            </div>
            <div class="viz-item">
                <h3>Technical Indicators</h3>
                <img src="{symbol.lower()}_indicators.png" alt="{symbol} Indicators">
            </div>
            <div class="viz-item">
                <h3>Cross-Timeframe Comparison</h3>
                <img src="{symbol.lower()}_cross_timeframe.png" alt="{symbol} Cross-Timeframe">
            </div>
            <div class="viz-item">
                <h3>Feature Distributions</h3>
                <img src="{symbol.lower()}_distributions.png" alt="{symbol} Distributions">
            </div>
            <div class="viz-item">
                <h3>Sequence Heatmap</h3>
                <img src="{symbol.lower()}_heatmap.png" alt="{symbol} Heatmap">
            </div>
        </div>
    </div>
"""
        
        html_content += """
    <div class="metadata">
        <h3>🔧 System Capabilities Demonstrated</h3>
        <ul>
            <li><strong>Multi-Timeframe Data Collection:</strong> OHLC data at multiple time intervals</li>
            <li><strong>Technical Indicators:</strong> ETOP, EBOT, PLDOT, EMA, RSI calculations</li>
            <li><strong>Cross-Timeframe Alignment:</strong> Daily indicators aligned to 5-minute intervals</li>
            <li><strong>Feature Engineering:</strong> Typed features with comprehensive metadata</li>
            <li><strong>Data Quality:</strong> Validation, cleaning, and quality metrics</li>
            <li><strong>Visualization:</strong> Interactive and comprehensive data exploration</li>
        </ul>
    </div>
</body>
</html>
"""
        
        with open(os.path.join(output_dir, 'index.html'), 'w') as f:
            f.write(html_content)
        
        print(f"📄 Visualization index created: {os.path.join(output_dir, 'index.html')}")


def main():
    """Main function to run visualization demo."""
    
    # Check if manual verification data exists
    data_dir = "manual_verification_output"
    if not os.path.exists(data_dir):
        print("❌ Manual verification data not found!")
        print("Please run: python manual_verification_demo.py")
        return
    
    # Create visualizations
    viz = MultiTimeframeVisualization(data_dir)
    viz.save_all_visualizations()
    
    print("\n🎉 Multi-Timeframe Visualization Demo Complete!")
    print(f"📁 Open visualization_output/index.html in your browser")


if __name__ == "__main__":
    main()