"""
Volume Profile Chart Visualization Components
Provides per-timeframe chart visualization for Volume Profile indicators and other signals.
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.patches as patches
from matplotlib.patches import Rectangle
from typing import Dict, Any, List, Tuple, Optional, Union
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)


class VolumeProfileChart:
    """Volume Profile chart visualization with multi-timeframe support."""

    def __init__(self, figsize: Tuple[int, int] = (15, 10)):
        self.figsize = figsize
        self.colors = {
            'poc': '#FF6B35',           # Point of Control - bright orange
            'vah': '#4ECDC4',           # Value Area High - teal
            'val': '#4ECDC4',           # Value Area Low - teal
            'value_area': '#4ECDC4',    # Value Area fill - teal with alpha
            'volume_bars': '#95E1D3',   # Volume bars - light teal
            'price_bars': '#3D5A80',    # Price bars - dark blue
            'background': '#FCEADE',    # Background - light cream
            'grid': '#E8E8E8'          # Grid lines - light gray
        }

    def create_multi_timeframe_chart(self,
                                   price_data: Dict[str, pd.DataFrame],
                                   volume_profile_results: Dict[str, Dict[str, Any]],
                                   other_signals: Optional[Dict[str, Dict[str, Any]]] = None) -> plt.Figure:
        """
        Create multi-timeframe chart with Volume Profile overlays.

        Args:
            price_data: Dict mapping timeframe -> OHLCV DataFrame
            volume_profile_results: Dict mapping timeframe -> volume profile results
            other_signals: Optional dict of other indicator signals

        Returns:
            matplotlib Figure with multi-timeframe charts
        """
        timeframes = list(price_data.keys())
        n_timeframes = len(timeframes)

        # Create subplots - 2 columns for better layout
        cols = 2
        rows = (n_timeframes + 1) // 2

        fig, axes = plt.subplots(rows, cols, figsize=self.figsize,
                                facecolor=self.colors['background'])
        fig.suptitle('Multi-Timeframe Volume Profile Analysis', fontsize=16, fontweight='bold')

        # Flatten axes for easy iteration
        if n_timeframes == 1:
            axes = [axes]
        elif rows == 1:
            axes = axes.flatten()
        else:
            axes = axes.flatten()

        for i, timeframe in enumerate(timeframes):
            if i >= len(axes):
                break

            ax = axes[i]

            # Plot price chart with volume profile overlay
            self._plot_single_timeframe(
                ax=ax,
                timeframe=timeframe,
                price_data=price_data[timeframe],
                volume_profile=volume_profile_results.get(timeframe),
                other_signals=other_signals.get(timeframe) if other_signals else None
            )

        # Hide unused subplots
        for i in range(len(timeframes), len(axes)):
            axes[i].set_visible(False)

        plt.tight_layout()
        return fig

    def _plot_single_timeframe(self,
                              ax: plt.Axes,
                              timeframe: str,
                              price_data: pd.DataFrame,
                              volume_profile: Optional[Dict[str, Any]],
                              other_signals: Optional[Dict[str, Any]] = None):
        """Plot single timeframe with price chart and volume profile overlay."""

        # Prepare data
        timestamps = pd.to_datetime(price_data.index) if hasattr(price_data.index, 'tz') else range(len(price_data))
        closes = price_data['close'].values
        highs = price_data['high'].values
        lows = price_data['low'].values
        volumes = price_data['volume'].values if 'volume' in price_data.columns else None

        # Plot price chart (simplified candlestick)
        ax.plot(timestamps, closes, color=self.colors['price_bars'], linewidth=1.5, label='Close Price')
        ax.fill_between(timestamps, lows, highs, alpha=0.3, color=self.colors['price_bars'], label='Price Range')

        # Add volume profile overlay if available
        if volume_profile and volume_profile.get('status') == 'valid':
            self._add_volume_profile_overlay(ax, volume_profile, timestamps)

        # Add other signals if available
        if other_signals:
            self._add_other_signals(ax, other_signals, timestamps, closes)

        # Styling
        ax.set_title(f'{timeframe} Timeframe Analysis', fontweight='bold')
        ax.set_xlabel('Time')
        ax.set_ylabel('Price')
        ax.grid(True, alpha=0.3, color=self.colors['grid'])
        ax.legend(loc='upper left', fontsize=8)

        # Set background color
        ax.set_facecolor(self.colors['background'])

    def _add_volume_profile_overlay(self,
                                   ax: plt.Axes,
                                   volume_profile: Dict[str, Any],
                                   timestamps: Union[pd.DatetimeIndex, range]):
        """Add volume profile overlay to price chart."""

        poc = volume_profile.get('poc')
        vah = volume_profile.get('vah')
        val = volume_profile.get('val')

        if not all([poc, vah, val]):
            logger.warning("Incomplete volume profile data, skipping overlay")
            return

        # Get chart bounds
        x_min = timestamps[0] if hasattr(timestamps, '__getitem__') else 0
        x_max = timestamps[-1] if hasattr(timestamps, '__getitem__') else len(timestamps) - 1

        # Calculate chart width for volume profile positioning
        if hasattr(timestamps, '__getitem__') and hasattr(timestamps[0], 'timestamp'):
            chart_width = (timestamps[-1] - timestamps[0]).total_seconds() / 3600  # hours
            profile_start_offset = chart_width * 0.85  # Start at 85% of chart width
        else:
            chart_width = len(timestamps)
            profile_start_offset = int(chart_width * 0.85)

        # Add horizontal lines for POC, VAH, VAL
        ax.axhline(y=poc, color=self.colors['poc'], linewidth=3,
                  linestyle='-', alpha=0.8, label=f'POC: ${poc:.2f}')
        ax.axhline(y=vah, color=self.colors['vah'], linewidth=2,
                  linestyle='--', alpha=0.7, label=f'VAH: ${vah:.2f}')
        ax.axhline(y=val, color=self.colors['val'], linewidth=2,
                  linestyle='--', alpha=0.7, label=f'VAL: ${val:.2f}')

        # Add value area shading
        ax.fill_between(timestamps, val, vah, alpha=0.1, color=self.colors['value_area'],
                       label=f'Value Area ({volume_profile.get("value_area_volume_pct", 70):.0f}%)')

        # Add volume profile bars on the right side
        self._add_volume_bars(ax, volume_profile, timestamps)

        # Add profile metadata text
        self._add_profile_metadata(ax, volume_profile)

    def _add_volume_bars(self,
                        ax: plt.Axes,
                        volume_profile: Dict[str, Any],
                        timestamps: Union[pd.DatetimeIndex, range]):
        """Add volume profile bars to the right side of the chart."""

        distribution_summary = volume_profile.get('volume_distribution_summary', {})
        top_levels = distribution_summary.get('top_volume_levels', [])

        if not top_levels:
            return

        # Calculate positioning
        if hasattr(timestamps, '__getitem__') and hasattr(timestamps[0], 'timestamp'):
            x_start = timestamps[-1] + (timestamps[-1] - timestamps[0]) * 0.02  # 2% offset from end
            bar_width_scale = (timestamps[-1] - timestamps[0]) * 0.15 / 100  # Scale bars to 15% of chart width
        else:
            x_start = len(timestamps) + len(timestamps) * 0.02
            bar_width_scale = len(timestamps) * 0.15 / 100

        # Find max volume for scaling
        max_volume_pct = max(level['volume_pct'] for level in top_levels) if top_levels else 1

        # Draw volume bars
        for level in top_levels[:10]:  # Top 10 levels only
            price = level['price']
            volume_pct = level['volume_pct']

            # Calculate bar width based on volume percentage
            bar_width = volume_pct * bar_width_scale

            # Determine bar color based on volume intensity
            alpha = min(0.8, volume_pct / max_volume_pct * 0.8 + 0.2)

            # Create horizontal bar
            if hasattr(timestamps, '__getitem__') and hasattr(timestamps[0], 'timestamp'):
                rect = Rectangle((x_start, price - 0.01), bar_width, 0.02,
                               facecolor=self.colors['volume_bars'], alpha=alpha,
                               edgecolor='none')
            else:
                rect = Rectangle((x_start, price - 0.01), bar_width, 0.02,
                               facecolor=self.colors['volume_bars'], alpha=alpha,
                               edgecolor='none')

            ax.add_patch(rect)

    def _add_profile_metadata(self, ax: plt.Axes, volume_profile: Dict[str, Any]):
        """Add volume profile metadata text box."""

        shape = volume_profile.get('profile_shape', 'unknown')
        bias = volume_profile.get('dominant_side', 'neutral')
        total_volume = volume_profile.get('total_volume', 0)
        concentration = volume_profile.get('volume_concentration', 0)

        # Create metadata text
        metadata_text = (
            f"Profile Shape: {shape.title()}\n"
            f"Market Bias: {bias.title()}\n"
            f"Total Volume: {total_volume:,.0f}\n"
            f"Concentration: {concentration:.1%}"
        )

        # Add text box in upper right corner
        ax.text(0.98, 0.98, metadata_text, transform=ax.transAxes,
               verticalalignment='top', horizontalalignment='right',
               bbox=dict(boxstyle='round,pad=0.3', facecolor='white', alpha=0.8),
               fontsize=8, family='monospace')

    def _add_other_signals(self,
                          ax: plt.Axes,
                          signals: Dict[str, Any],
                          timestamps: Union[pd.DatetimeIndex, range],
                          closes: np.ndarray):
        """Add other technical indicator signals to chart."""

        # Add BX Trender signals if available
        if 'BXTrender' in signals:
            bx_data = signals['BXTrender']
            if bx_data.get('status') == 'valid':
                bx_value = bx_data.get('bx_trender', 50)
                trend_strength = bx_data.get('trend_strength', 0)

                # Color code based on BX Trender value
                if bx_value > 70:
                    color = '#27AE60'  # Strong bullish - green
                elif bx_value > 50:
                    color = '#2ECC71'  # Bullish - light green
                elif bx_value < 30:
                    color = '#E74C3C'  # Strong bearish - red
                elif bx_value < 50:
                    color = '#EC7063'  # Bearish - light red
                else:
                    color = '#95A5A6'  # Neutral - gray

                # Add trend strength indicator as background color intensity
                ax.axhspan(min(closes), max(closes), alpha=trend_strength * 0.1,
                          color=color, label=f'BX Trend: {bx_value:.1f}')

        # Add other indicators as needed
        for indicator_name, indicator_data in signals.items():
            if indicator_name != 'BXTrender' and indicator_data.get('status') == 'valid':
                # Generic indicator plotting
                value = indicator_data.get('value')
                if value is not None:
                    # Add as horizontal line if it's a price level
                    if isinstance(value, (int, float)) and min(closes) <= value <= max(closes):
                        ax.axhline(y=value, color='purple', linewidth=1,
                                  linestyle=':', alpha=0.6,
                                  label=f'{indicator_name}: {value:.2f}')


class MultiTimeframeSignalVisualizer:
    """Comprehensive multi-timeframe signal visualization system."""

    def __init__(self, timeframes: List[str] = ['5m', '15m', '1h', '1d']):
        self.timeframes = timeframes
        self.volume_profile_chart = VolumeProfileChart()

    def create_comprehensive_analysis_dashboard(self,
                                              market_data: Dict[str, pd.DataFrame],
                                              all_signals: Dict[str, Dict[str, Dict[str, Any]]]) -> plt.Figure:
        """
        Create comprehensive analysis dashboard with all signals across timeframes.

        Args:
            market_data: Dict[timeframe -> OHLCV DataFrame]
            all_signals: Dict[timeframe -> Dict[indicator_name -> indicator_result]]

        Returns:
            matplotlib Figure with comprehensive dashboard
        """

        # Extract volume profile results from all signals
        volume_profile_results = {}
        other_signals = {}

        for timeframe in self.timeframes:
            if timeframe in all_signals:
                tf_signals = all_signals[timeframe]

                # Extract volume profile results
                for signal_name, signal_data in tf_signals.items():
                    if 'VolumeProfile' in signal_name:
                        volume_profile_results[timeframe] = signal_data
                        break

                # Extract other signals
                other_signals[timeframe] = {
                    name: data for name, data in tf_signals.items()
                    if 'VolumeProfile' not in name
                }

        # Create the comprehensive chart
        return self.volume_profile_chart.create_multi_timeframe_chart(
            price_data=market_data,
            volume_profile_results=volume_profile_results,
            other_signals=other_signals
        )

    def save_analysis_chart(self,
                           fig: plt.Figure,
                           filename: str,
                           symbol: str,
                           timestamp: Optional[datetime] = None) -> str:
        """
        Save analysis chart to file with standardized naming.

        Args:
            fig: matplotlib Figure to save
            filename: Base filename
            symbol: Trading symbol
            timestamp: Optional timestamp for filename

        Returns:
            Full path to saved file
        """
        if timestamp is None:
            timestamp = datetime.now()

        timestamp_str = timestamp.strftime('%Y%m%d_%H%M%S')
        full_filename = f"{filename}_{symbol}_{timestamp_str}.png"

        # Save with high DPI for quality
        fig.savefig(full_filename, dpi=300, bbox_inches='tight',
                   facecolor='white', edgecolor='none')

        logger.info(f"Analysis chart saved: {full_filename}")
        return full_filename

    def create_signal_comparison_chart(self,
                                     signals_data: Dict[str, Dict[str, Any]],
                                     timeframe: str = '1h') -> plt.Figure:
        """
        Create comparison chart of different signals for analysis.

        Args:
            signals_data: Dict mapping signal name -> signal results
            timeframe: Timeframe label for the chart

        Returns:
            matplotlib Figure with signal comparison
        """

        fig, axes = plt.subplots(2, 2, figsize=(15, 10))
        fig.suptitle(f'Technical Signals Comparison - {timeframe}', fontsize=16)

        axes = axes.flatten()

        # Plot different signal types
        signal_types = [
            ('BX Trender', ['BXTrender_basic', 'BXTrender_directional', 'BXTrender_volume_weighted']),
            ('Volume Profile', ['VolumeProfile_20_50', 'VolumeProfile_20_30']),
            ('Moving Averages', ['EMA_21', 'EMA_50']),
            ('Momentum', ['RSI_14', 'ATR_14'])
        ]

        for i, (signal_category, signal_names) in enumerate(signal_types):
            if i >= len(axes):
                break

            ax = axes[i]
            ax.set_title(signal_category, fontweight='bold')

            # Plot signals in this category
            for signal_name in signal_names:
                if signal_name in signals_data:
                    signal_data = signals_data[signal_name]
                    if signal_data.get('status') == 'valid':
                        value = signal_data.get('value')
                        if value is not None:
                            ax.bar(signal_name, value, alpha=0.7)

            ax.tick_params(axis='x', rotation=45)
            ax.grid(True, alpha=0.3)

        plt.tight_layout()
        return fig


def create_training_data_visualization_sample():
    """Create sample visualization for training data with volume profile features."""

    # Generate sample data
    np.random.seed(42)
    dates = pd.date_range('2024-01-01', periods=100, freq='1H')

    sample_data = {
        '1h': pd.DataFrame({
            'open': np.random.uniform(100, 102, 100),
            'high': np.random.uniform(101, 103, 100),
            'low': np.random.uniform(99, 101, 100),
            'close': np.random.uniform(100, 102, 100),
            'volume': np.random.uniform(10000, 50000, 100)
        }, index=dates)
    }

    # Mock volume profile results
    volume_profile_results = {
        '1h': {
            'status': 'valid',
            'poc': 101.25,
            'vah': 101.75,
            'val': 100.75,
            'value_area_volume_pct': 70.0,
            'profile_shape': 'balanced',
            'dominant_side': 'bullish',
            'total_volume': 2500000,
            'volume_concentration': 0.35,
            'volume_distribution_summary': {
                'top_volume_levels': [
                    {'price': 101.25, 'volume': 875000, 'volume_pct': 35.0},
                    {'price': 101.50, 'volume': 625000, 'volume_pct': 25.0},
                    {'price': 101.00, 'volume': 500000, 'volume_pct': 20.0}
                ]
            }
        }
    }

    # Create visualization
    visualizer = MultiTimeframeSignalVisualizer(['1h'])
    chart = VolumeProfileChart()

    fig = chart.create_multi_timeframe_chart(
        price_data=sample_data,
        volume_profile_results=volume_profile_results
    )

    return fig


if __name__ == "__main__":
    # Create sample visualization
    sample_fig = create_training_data_visualization_sample()
    sample_fig.savefig('/tmp/volume_profile_sample.png', dpi=150, bbox_inches='tight')
    print("Sample volume profile chart saved to /tmp/volume_profile_sample.png")