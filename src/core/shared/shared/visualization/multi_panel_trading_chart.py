"""
Multi-Panel Trading Chart Visualization
Provides comprehensive trading visualization with specific panel layout:
- OHLC chart in the middle with indicator lines
- Volume distribution on the right
- BX Trender at the bottom
- All indicators: envelope top, envelope bot, pldot, z1b, z2b, z5t, z6t
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
from matplotlib.patches import Rectangle
from typing import Dict, Tuple, Optional
from datetime import datetime
import logging

# Import existing visualization components
from visualization.volume_profile_chart import VolumeProfileChart
try:
    from domains.ml.legacy.evaluation.simple_trade_chart import SimpleTradeChart, TechnicalIndicators, MarketEvent
except ImportError:
    # Create minimal classes if imports not available
    SimpleTradeChart = None
    TechnicalIndicators = None
    MarketEvent = None

logger = logging.getLogger(__name__)


class MultiPanelTradingChart:
    """
    Comprehensive multi-panel trading visualization with specific layout:

    Layout:
    ┌─────────────────────────────────┬─────────────────┐
    │                                 │                 │
    │           OHLC CHART            │    VOLUME       │
    │        + INDICATOR LINES        │  DISTRIBUTION   │
    │     (envelope, pldot, z-series) │                 │
    │                                 │                 │
    ├─────────────────────────────────┴─────────────────┤
    │                BX TRENDER                         │
    └───────────────────────────────────────────────────┘
    """

    def __init__(self, figsize: Tuple[int, int] = (18, 12)):
        self.figsize = figsize
        self.volume_profile_chart = VolumeProfileChart()

        # Define colors for different indicators
        self.indicator_colors = {
            'envelope_top': '#FF6B35',     # Orange
            'envelope_bot': '#FF6B35',     # Orange (same as top)
            'pldot': '#4ECDC4',           # Teal
            'z1b': '#95E1D3',             # Light teal
            'z2b': '#3D5A80',             # Dark blue
            'z5t': '#FF6B6B',             # Red
            'z6t': '#FFD93D',             # Yellow
            'bx_trender': '#9B59B6',       # Purple
            'ohlc': '#2C3E50',            # Dark gray
            'volume': '#95A5A6'           # Gray
        }

        self.bx_trender_colors = {
            'strong_bullish': '#27AE60',   # Strong green (>70)
            'bullish': '#2ECC71',          # Green (50-70)
            'bearish': '#EC7063',          # Light red (30-50)
            'strong_bearish': '#E74C3C',   # Red (<30)
            'neutral': '#95A5A6'           # Gray (50)
        }

    def create_multi_panel_chart(self,
                                symbol: str,
                                price_data: pd.DataFrame,
                                training_features: Dict[str, float],
                                timeframe: str = '1h',
                                title_suffix: str = '') -> plt.Figure:
        """
        Create comprehensive multi-panel trading chart.

        Args:
            symbol: Trading symbol (e.g., 'AAPL')
            price_data: OHLCV DataFrame
            training_features: Dict of extracted features from training dataset
            timeframe: Timeframe label
            title_suffix: Additional title text

        Returns:
            matplotlib Figure with multi-panel layout
        """

        # Create figure with custom grid layout
        fig = plt.figure(figsize=self.figsize, facecolor='white')

        # Define grid: 3 rows, 4 columns
        # Row 0-1: OHLC chart (cols 0-2) + Volume distribution (col 3)
        # Row 2: BX Trender (cols 0-3)
        gs = gridspec.GridSpec(3, 4, figure=fig,
                              height_ratios=[3, 3, 1],  # OHLC spans 2 rows, BX Trender 1 row
                              width_ratios=[2, 2, 2, 1])  # Volume distribution narrower

        # Create subplots
        ax_ohlc = fig.add_subplot(gs[0:2, 0:3])      # OHLC chart (main area)
        ax_volume = fig.add_subplot(gs[0:2, 3])      # Volume distribution (right)
        ax_bx_trender = fig.add_subplot(gs[2, 0:4])  # BX Trender (bottom, full width)

        # Set main title
        title = f'{symbol} Multi-Panel Trading Analysis - {timeframe.upper()}'
        if title_suffix:
            title += f' - {title_suffix}'
        fig.suptitle(title, fontsize=16, fontweight='bold', y=0.95)

        # Plot each panel
        self._plot_ohlc_with_indicators(ax_ohlc, price_data, training_features, timeframe)
        self._plot_volume_distribution(ax_volume, training_features, timeframe)
        self._plot_bx_trender(ax_bx_trender, training_features, timeframe)

        # Adjust layout and spacing
        plt.tight_layout(rect=[0, 0.02, 1, 0.93])

        return fig

    def _plot_ohlc_with_indicators(self,
                                  ax: plt.Axes,
                                  price_data: pd.DataFrame,
                                  features: Dict[str, float],
                                  timeframe: str):
        """Plot OHLC candlesticks with indicator lines."""

        # Prepare data
        if len(price_data) == 0:
            ax.text(0.5, 0.5, 'No OHLC data available',
                   transform=ax.transAxes, ha='center', va='center')
            return

        timestamps = range(len(price_data))  # Use index positions for simplicity
        opens = price_data['open'].values
        highs = price_data['high'].values
        lows = price_data['low'].values
        closes = price_data['close'].values

        # Plot OHLC as candlesticks (simplified)
        for i in range(len(price_data)):
            open_val, high_val, low_val, close_val = opens[i], highs[i], lows[i], closes[i]

            # Determine candle color
            color = '#27AE60' if close_val >= open_val else '#E74C3C'  # Green/Red

            # Plot high-low line
            ax.plot([i, i], [low_val, high_val], color='#34495E', linewidth=1)

            # Plot open-close body
            body_height = abs(close_val - open_val)
            body_bottom = min(open_val, close_val)
            rect = Rectangle((i-0.3, body_bottom), 0.6, body_height,
                           facecolor=color, alpha=0.7, edgecolor='#2C3E50')
            ax.add_patch(rect)

        # Add indicator lines from training features
        self._add_indicator_lines(ax, features, timeframe, len(price_data))

        # Styling
        ax.set_title('OHLC Chart with Technical Indicators', fontweight='bold', pad=10)
        ax.set_xlabel('Time (bars)')
        ax.set_ylabel('Price')
        ax.grid(True, alpha=0.3, linestyle='--')
        ax.legend(loc='upper left', fontsize=8, framealpha=0.9)

        # Set reasonable y-limits
        if len(price_data) > 0:
            price_range = highs.max() - lows.min()
            ax.set_ylim(lows.min() - price_range * 0.05, highs.max() + price_range * 0.05)

    def _add_indicator_lines(self,
                            ax: plt.Axes,
                            features: Dict[str, float],
                            timeframe: str,
                            data_length: int):
        """Add technical indicator lines to OHLC chart."""

        # Define indicators to show as horizontal lines
        indicators_to_plot = [
            'envelope_top', 'envelope_bot', 'pldot',
            'z1b', 'z2b', 'z5t', 'z6t'
        ]

        x_range = [0, data_length - 1] if data_length > 0 else [0, 1]

        for indicator in indicators_to_plot:
            # Look for feature with timeframe prefix
            feature_key = f'{timeframe}_{indicator}'

            if feature_key in features:
                value = features[feature_key]
                if pd.notna(value) and value > 0:  # Valid indicator value
                    color = self.indicator_colors.get(indicator, '#95A5A6')
                    linestyle = '--' if 'envelope' in indicator else '-'
                    linewidth = 2 if indicator == 'pldot' else 1.5

                    # Plot horizontal line across the chart
                    ax.axhline(y=value, color=color, linewidth=linewidth,
                             linestyle=linestyle, alpha=0.8,
                             label=f'{indicator.upper()}: {value:.2f}')

        # Add special handling for envelope top/bottom as a zone
        env_top_key = f'{timeframe}_envelope_top'
        env_bot_key = f'{timeframe}_envelope_bot'

        if env_top_key in features and env_bot_key in features:
            env_top = features[env_top_key]
            env_bot = features[env_bot_key]

            if pd.notna(env_top) and pd.notna(env_bot) and env_top > env_bot:
                # Add envelope zone shading
                ax.fill_between(x_range, env_bot, env_top,
                               color=self.indicator_colors['envelope_top'],
                               alpha=0.1, label=f'Envelope Zone')

    def _plot_volume_distribution(self,
                                 ax: plt.Axes,
                                 features: Dict[str, float],
                                 timeframe: str):
        """Plot volume profile distribution on the right panel."""

        # Extract volume profile features
        volume_features = {
            'poc': f'{timeframe}_volume_profile_poc',
            'vah': f'{timeframe}_volume_profile_vah',
            'val': f'{timeframe}_volume_profile_val',
            'va_range': f'{timeframe}_volume_profile_va_range',
            'price_vs_poc': f'{timeframe}_volume_profile_price_vs_poc',
            'price_vs_val': f'{timeframe}_volume_profile_price_vs_val',
            'price_vs_vah': f'{timeframe}_volume_profile_price_vs_vah'
        }

        # Get feature values
        vp_values = {}
        for key, feature_name in volume_features.items():
            if feature_name in features:
                vp_values[key] = features[feature_name]

        if not vp_values or 'poc' not in vp_values:
            ax.text(0.5, 0.5, 'No Volume Profile\\nData Available',
                   transform=ax.transAxes, ha='center', va='center', fontsize=10)
            ax.set_title('Volume Distribution', fontweight='bold')
            return

        # Create volume distribution visualization
        poc = vp_values['poc']
        vah = vp_values.get('vah', poc * 1.01)
        val = vp_values.get('val', poc * 0.99)

        # Simulate volume levels around POC, VAH, VAL
        price_levels = np.linspace(val * 0.98, vah * 1.02, 20)
        volume_levels = []

        for price in price_levels:
            # Higher volume near POC, VAH, VAL
            distance_to_poc = abs(price - poc) / poc
            distance_to_vah = abs(price - vah) / vah
            distance_to_val = abs(price - val) / val

            # Volume peaks at key levels
            min_distance = min(distance_to_poc, distance_to_vah, distance_to_val)
            volume_intensity = max(0.1, 1.0 - min_distance * 100)  # Peak at key levels
            volume_levels.append(volume_intensity)

        volume_levels = np.array(volume_levels)

        # Plot horizontal volume bars
        for i, (price, volume) in enumerate(zip(price_levels, volume_levels)):
            bar_width = volume * 0.8  # Scale bar width

            # Color based on key levels
            if abs(price - poc) / poc < 0.001:
                color = '#FF6B35'  # POC - orange
                alpha = 0.9
            elif abs(price - vah) / vah < 0.001 or abs(price - val) / val < 0.001:
                color = '#4ECDC4'  # VAH/VAL - teal
                alpha = 0.8
            else:
                color = '#95E1D3'  # Regular volume - light teal
                alpha = max(0.3, volume)

            # Draw horizontal bar
            rect = Rectangle((0, price - (price_levels[1] - price_levels[0])/2),
                           bar_width, price_levels[1] - price_levels[0],
                           facecolor=color, alpha=alpha, edgecolor='none')
            ax.add_patch(rect)

        # Add key level lines
        ax.axhline(y=poc, color='#FF6B35', linewidth=2, linestyle='-',
                  label=f'POC: {poc:.2f}')
        ax.axhline(y=vah, color='#4ECDC4', linewidth=1.5, linestyle='--',
                  label=f'VAH: {vah:.2f}')
        ax.axhline(y=val, color='#4ECDC4', linewidth=1.5, linestyle='--',
                  label=f'VAL: {val:.2f}')

        # Add value area shading
        ax.fill_between([0, 1], val, vah, alpha=0.2, color='#4ECDC4',
                       label='Value Area (70%)')

        # Styling
        ax.set_title('Volume Distribution', fontweight='bold')
        ax.set_xlabel('Volume Intensity')
        ax.set_ylabel('Price')
        ax.set_xlim(0, 1)
        ax.legend(loc='upper right', fontsize=8, framealpha=0.9)
        ax.grid(True, alpha=0.3, linestyle='--')

    def _plot_bx_trender(self,
                        ax: plt.Axes,
                        features: Dict[str, float],
                        timeframe: str):
        """Plot BX Trender indicator at the bottom."""

        # Look for BX Trender features
        bx_features = [
            'BXTrenderBasic_14',
            'BXTrenderDirectional_14',
            'BXTrenderVolumeWeighted_14'
        ]

        found_bx_values = {}
        for bx_feature in bx_features:
            feature_key = f'{timeframe}_{bx_feature}'
            if feature_key in features:
                found_bx_values[bx_feature] = features[feature_key]

        if not found_bx_values:
            ax.text(0.5, 0.5, 'No BX Trender Data Available',
                   transform=ax.transAxes, ha='center', va='center')
            ax.set_title('BX Trender Indicators', fontweight='bold')
            return

        # Plot BX Trender values as bars
        x_positions = range(len(found_bx_values))
        values = list(found_bx_values.values())
        labels = [name.replace('BXTrender', 'BXT').replace('_14', '') for name in found_bx_values.keys()]

        # Determine colors based on values
        colors = []
        for value in values:
            if pd.isna(value):
                colors.append(self.bx_trender_colors['neutral'])
            elif value > 70:
                colors.append(self.bx_trender_colors['strong_bullish'])
            elif value > 50:
                colors.append(self.bx_trender_colors['bullish'])
            elif value < 30:
                colors.append(self.bx_trender_colors['strong_bearish'])
            elif value < 50:
                colors.append(self.bx_trender_colors['bearish'])
            else:
                colors.append(self.bx_trender_colors['neutral'])

        # Create bar chart
        bars = ax.bar(x_positions, values, color=colors, alpha=0.8,
                     edgecolor='#2C3E50', linewidth=1)

        # Add value labels on bars
        for i, (bar, value) in enumerate(zip(bars, values)):
            if pd.notna(value):
                ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 1,
                       f'{value:.1f}', ha='center', va='bottom', fontweight='bold',
                       fontsize=10)

        # Add reference lines
        ax.axhline(y=70, color='#27AE60', linestyle='--', alpha=0.7,
                  label='Strong Bullish (70)')
        ax.axhline(y=50, color='#F39C12', linestyle='--', alpha=0.7,
                  label='Neutral (50)')
        ax.axhline(y=30, color='#E74C3C', linestyle='--', alpha=0.7,
                  label='Strong Bearish (30)')

        # Styling
        ax.set_title('BX Trender Indicators', fontweight='bold')
        ax.set_xlabel('BX Trender Types')
        ax.set_ylabel('Indicator Value')
        ax.set_xticks(x_positions)
        ax.set_xticklabels(labels, rotation=0)
        ax.set_ylim(0, 100)
        ax.grid(True, alpha=0.3, linestyle='--', axis='y')
        ax.legend(loc='upper right', fontsize=8, framealpha=0.9)

    def save_chart(self,
                  fig: plt.Figure,
                  symbol: str,
                  timeframe: str,
                  output_dir: str = '/tmp',
                  timestamp: Optional[datetime] = None) -> str:
        """
        Save multi-panel chart to file.

        Args:
            fig: matplotlib Figure
            symbol: Trading symbol
            timeframe: Timeframe
            output_dir: Output directory
            timestamp: Optional timestamp for filename

        Returns:
            Path to saved file
        """
        if timestamp is None:
            timestamp = datetime.now()

        timestamp_str = timestamp.strftime('%Y%m%d_%H%M%S')
        filename = f"multi_panel_trading_{symbol}_{timeframe}_{timestamp_str}.png"
        filepath = f"{output_dir}/{filename}"

        # Save with high quality
        fig.savefig(filepath, dpi=300, bbox_inches='tight',
                   facecolor='white', edgecolor='none',
                   pad_inches=0.2)

        logger.info(f"Multi-panel trading chart saved: {filepath}")
        return filepath


def create_sample_multi_panel_chart():
    """Create sample multi-panel chart for testing."""

    # Generate sample OHLCV data
    np.random.seed(42)
    n_periods = 50

    base_price = 150.0
    returns = np.random.normal(0.001, 0.02, n_periods)
    prices = base_price * np.exp(np.cumsum(returns))

    sample_price_data = pd.DataFrame({
        'open': prices * (1 + np.random.normal(0, 0.005, n_periods)),
        'high': prices * (1 + np.random.uniform(0.005, 0.015, n_periods)),
        'low': prices * (1 - np.random.uniform(0.005, 0.015, n_periods)),
        'close': prices,
        'volume': np.random.lognormal(12, 0.5, n_periods).astype(int)
    })

    # Generate sample training features
    current_price = prices[-1]
    sample_features = {
        '1h_envelope_top': current_price * 1.02,
        '1h_envelope_bot': current_price * 0.98,
        '1h_pldot': current_price * 0.995,
        '1h_z1b': current_price * 0.99,
        '1h_z2b': current_price * 0.985,
        '1h_z5t': current_price * 1.01,
        '1h_z6t': current_price * 1.015,
        '1h_volume_profile_poc': current_price,
        '1h_volume_profile_vah': current_price * 1.005,
        '1h_volume_profile_val': current_price * 0.995,
        '1h_BXTrenderBasic_14': 65.5,
        '1h_BXTrenderDirectional_14': 72.3,
        '1h_BXTrenderVolumeWeighted_14': 58.9
    }

    # Create chart
    chart = MultiPanelTradingChart()
    fig = chart.create_multi_panel_chart(
        symbol='AAPL',
        price_data=sample_price_data,
        training_features=sample_features,
        timeframe='1h',
        title_suffix='Sample Data'
    )

    return fig, chart


if __name__ == "__main__":
    # Create and save sample chart
    sample_fig, chart = create_sample_multi_panel_chart()
    output_path = chart.save_chart(
        fig=sample_fig,
        symbol='AAPL',
        timeframe='1h',
        output_dir='/tmp'
    )
    print(f"Sample multi-panel trading chart saved to: {output_path}")