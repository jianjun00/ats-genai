"""
Simple Trade Chart with OHLC, Indicators, and Events

Creates focused OHLC candlestick charts with:
- PLDOT (Pivot Low Dot) - Support levels
- ETOP (Expected Top) - Resistance levels  
- EBOT (Expected Bottom) - Support levels
- Major market events and announcements
"""

import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.patches import Rectangle
import pandas as pd
import numpy as np
from datetime import datetime
from typing import List, Optional, Tuple
from dataclasses import dataclass


@dataclass
class MarketEvent:
    """Market event with timing and description"""
    date: datetime
    event_type: str  # "earnings", "fed", "economic", "news"
    description: str
    impact: str  # "positive", "negative", "neutral"
    importance: int  # 1-5 scale


@dataclass 
class TechnicalIndicators:
    """Technical indicators for the chart"""
    pldot: List[Tuple[datetime, float]]  # Pivot low dots (support)
    etop: List[Tuple[datetime, float]]   # Expected tops (resistance)
    ebot: List[Tuple[datetime, float]]   # Expected bottoms (support)


class SimpleTradeChart:
    """
    Simple focused chart for individual stock analysis
    
    Shows:
    - OHLC candlesticks
    - PLDOT, ETOP, EBOT indicators
    - Major relevant events
    - Trade entry/exit points
    """
    
    def __init__(self):
        self.event_colors = {
            "earnings": "#FFD700",  # Gold
            "fed": "#FF6B6B",       # Red
            "economic": "#4ECDC4",  # Teal
            "news": "#95E1D3"       # Light green
        }
        
        self.impact_markers = {
            "positive": "^",
            "negative": "v", 
            "neutral": "o"
        }
    
    def create_ohlc_chart(self,
                         symbol: str,
                         price_data: pd.DataFrame,
                         indicators: TechnicalIndicators,
                         events: List[MarketEvent],
                         trade_date: Optional[datetime] = None,
                         trade_action: Optional[str] = None,
                         output_path: Optional[str] = None) -> str:
        """
        Create OHLC chart with indicators and events
        
        Args:
            symbol: Stock symbol
            price_data: DataFrame with OHLC data (columns: open, high, low, close, volume)
            indicators: Technical indicators (PLDOT, ETOP, EBOT)
            events: List of relevant market events
            trade_date: Date of trade execution
            trade_action: "buy", "sell", or None
            output_path: Path to save chart
            
        Returns:
            Path to saved chart
        """
        # Set up the figure
        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 10), 
                                      gridspec_kw={'height_ratios': [3, 1]})
        fig.suptitle(f'{symbol} - Price Action with Indicators & Events', 
                    fontsize=16, fontweight='bold')
        
        # Plot OHLC candlesticks
        self._plot_candlesticks(ax1, price_data)
        
        # Add technical indicators
        self._add_indicators(ax1, indicators, price_data.index)
        
        # Add market events
        self._add_events(ax1, events, price_data)
        
        # Add trade marker if provided
        if trade_date and trade_action:
            self._add_trade_marker(ax1, trade_date, price_data, trade_action)
        
        # Plot volume
        self._plot_volume(ax2, price_data)
        
        # Format axes
        self._format_axes(ax1, ax2, price_data.index)
        
        # Add legend
        self._add_legend(ax1)
        
        # Save chart
        if output_path is None:
            output_path = f"{symbol}_trade_chart_{datetime.now().strftime('%Y%m%d')}.png"
        
        plt.tight_layout()
        plt.savefig(output_path, dpi=300, bbox_inches='tight', facecolor='white')
        plt.close()
        
        return output_path
    
    def _plot_candlesticks(self, ax, price_data):
        """Plot OHLC candlesticks"""
        # Convert to format needed for candlestick plotting
        dates = price_data.index
        opens = price_data['open'].values
        highs = price_data['high'].values
        lows = price_data['low'].values
        closes = price_data['close'].values
        
        # Plot candlesticks manually for better control
        for i, (date, o, h, l, c) in enumerate(zip(dates, opens, highs, lows, closes)):
            # Determine color
            color = 'green' if c >= o else 'red'
            edge_color = 'darkgreen' if c >= o else 'darkred'
            
            # Plot high-low line
            ax.plot([date, date], [l, h], color=edge_color, linewidth=1, alpha=0.8)
            
            # Plot body rectangle
            body_height = abs(c - o)
            body_bottom = min(c, o)
            
            rect = Rectangle((mdates.date2num(date) - 0.3, body_bottom), 
                           0.6, body_height, 
                           facecolor=color, edgecolor=edge_color, 
                           alpha=0.7, linewidth=0.5)
            ax.add_patch(rect)
    
    def _add_indicators(self, ax, indicators: TechnicalIndicators, dates):
        """Add PLDOT, ETOP, EBOT indicators"""
        
        # PLDOT - Pivot Low Dots (Support levels)
        if indicators.pldot:
            pldot_dates, pldot_prices = zip(*indicators.pldot)
            ax.scatter(pldot_dates, pldot_prices, 
                      c='blue', marker='o', s=50, alpha=0.8,
                      label='PLDOT (Support)', zorder=5)
            
            # Draw support lines
            for date, price in indicators.pldot:
                ax.axhline(y=price, color='blue', alpha=0.3, linestyle='--', linewidth=1)
        
        # ETOP - Expected Tops (Resistance levels)
        if indicators.etop:
            etop_dates, etop_prices = zip(*indicators.etop)
            ax.scatter(etop_dates, etop_prices,
                      c='red', marker='^', s=60, alpha=0.8,
                      label='ETOP (Resistance)', zorder=5)
            
            # Draw resistance lines
            for date, price in indicators.etop:
                ax.axhline(y=price, color='red', alpha=0.3, linestyle='--', linewidth=1)
        
        # EBOT - Expected Bottoms (Support levels)
        if indicators.ebot:
            ebot_dates, ebot_prices = zip(*indicators.ebot)
            ax.scatter(ebot_dates, ebot_prices,
                      c='green', marker='v', s=60, alpha=0.8,
                      label='EBOT (Support)', zorder=5)
            
            # Draw support lines
            for date, price in indicators.ebot:
                ax.axhline(y=price, color='green', alpha=0.3, linestyle='--', linewidth=1)
    
    def _add_events(self, ax, events: List[MarketEvent], price_data):
        """Add market events to chart"""
        for event in events:
            if event.date in price_data.index:
                # Get price at event date for positioning
                event_price = price_data.loc[event.date, 'high'] * 1.02
                
                # Plot event marker
                marker = self.impact_markers[event.impact]
                color = self.event_colors[event.event_type]
                
                ax.scatter(event.date, event_price, 
                          c=color, marker=marker, s=100 * event.importance,
                          alpha=0.8, edgecolors='black', linewidth=1,
                          zorder=10)
                
                # Add event text
                ax.annotate(event.description, 
                           (event.date, event_price),
                           xytext=(10, 10), textcoords='offset points',
                           bbox=dict(boxstyle='round,pad=0.3', fc=color, alpha=0.7),
                           fontsize=8, ha='left')
    
    def _add_trade_marker(self, ax, trade_date, price_data, trade_action):
        """Add trade execution marker"""
        if trade_date in price_data.index:
            trade_price = price_data.loc[trade_date, 'close']
            
            if trade_action == 'buy':
                marker = '^'
                color = 'lime'
                label = 'BUY'
            elif trade_action == 'sell':
                marker = 'v'
                color = 'red'
                label = 'SELL'
            else:
                marker = 'o'
                color = 'yellow'
                label = 'HOLD'
            
            ax.scatter(trade_date, trade_price,
                      c=color, marker=marker, s=200, 
                      alpha=0.9, edgecolors='black', linewidth=2,
                      label=f'{label} Signal', zorder=15)
    
    def _plot_volume(self, ax, price_data):
        """Plot volume bars"""
        dates = price_data.index
        volumes = price_data['volume'].values
        
        # Color volume bars based on price movement
        colors = []
        for i, (o, c) in enumerate(zip(price_data['open'], price_data['close'])):
            colors.append('green' if c >= o else 'red')
        
        ax.bar(dates, volumes, color=colors, alpha=0.6, width=0.8)
        ax.set_ylabel('Volume', fontsize=10)
        ax.ticklabel_format(style='plain', axis='y')
    
    def _format_axes(self, ax1, ax2, dates):
        """Format chart axes"""
        # Price axis formatting
        ax1.set_ylabel('Price ($)', fontsize=12)
        ax1.grid(True, alpha=0.3)
        ax1.tick_params(axis='x', labelbottom=False)  # Hide x labels on main chart
        
        # Volume axis formatting
        ax2.grid(True, alpha=0.3)
        ax2.set_xlabel('Date', fontsize=12)
        
        # Format x-axis dates
        if len(dates) > 30:
            ax2.xaxis.set_major_locator(mdates.WeekdayLocator(interval=1))
            ax2.xaxis.set_major_formatter(mdates.DateFormatter('%m/%d'))
        else:
            ax2.xaxis.set_major_locator(mdates.DayLocator(interval=1))
            ax2.xaxis.set_major_formatter(mdates.DateFormatter('%m/%d'))
        
        plt.setp(ax2.xaxis.get_majorticklabels(), rotation=45)
    
    def _add_legend(self, ax):
        """Add legend to chart"""
        # Create legend with custom entries for events
        legend_elements = ax.get_legend_handles_labels()[0]
        legend_labels = ax.get_legend_handles_labels()[1]
        
        # Add event type legend
        from matplotlib.lines import Line2D
        for event_type, color in self.event_colors.items():
            legend_elements.append(Line2D([0], [0], marker='o', color='w', 
                                        markerfacecolor=color, markersize=8))
            legend_labels.append(f'{event_type.title()} Event')
        
        ax.legend(legend_elements, legend_labels, 
                 loc='upper left', bbox_to_anchor=(0, 1), fontsize=9)


def generate_sample_data(symbol: str, 
                        start_date: datetime, 
                        days: int = 30) -> Tuple[pd.DataFrame, TechnicalIndicators, List[MarketEvent]]:
    """
    Generate sample OHLC data, indicators, and events for demonstration
    
    Args:
        symbol: Stock symbol
        start_date: Start date for data
        days: Number of days of data
        
    Returns:
        Tuple of (price_data, indicators, events)
    """
    # Generate dates
    dates = pd.date_range(start_date, periods=days, freq='D')
    
    # Generate realistic OHLC data
    np.random.seed(hash(symbol) % 2**32)  # Consistent data per symbol
    
    base_price = 100
    returns = np.random.normal(0.001, 0.02, days)  # Daily returns
    prices = [base_price]
    
    for ret in returns:
        prices.append(prices[-1] * (1 + ret))
    
    prices = prices[1:]  # Remove initial price
    
    # Generate OHLC from closing prices
    ohlc_data = []
    for i, close_price in enumerate(prices):
        # Open price (previous close + gap)
        if i == 0:
            open_price = close_price
        else:
            gap = np.random.normal(0, 0.005)
            open_price = prices[i-1] * (1 + gap)
        
        # High and low
        daily_vol = abs(np.random.normal(0, 0.015))
        high = max(open_price, close_price) * (1 + daily_vol)
        low = min(open_price, close_price) * (1 - daily_vol)
        
        # Volume
        volume = int(np.random.lognormal(13, 0.5))  # Realistic volume distribution
        
        ohlc_data.append({
            'open': round(open_price, 2),
            'high': round(high, 2),
            'low': round(low, 2),
            'close': round(close_price, 2),
            'volume': volume
        })
    
    price_df = pd.DataFrame(ohlc_data, index=dates)
    
    # Generate technical indicators
    indicators = TechnicalIndicators(
        # PLDOT - Support levels at local lows
        pldot=[(dates[i], price_df.iloc[i]['low'] * 0.99) 
               for i in range(2, days-2, 7) 
               if price_df.iloc[i]['low'] < price_df.iloc[i-1]['low'] and 
                  price_df.iloc[i]['low'] < price_df.iloc[i+1]['low']],
        
        # ETOP - Resistance levels at local highs
        etop=[(dates[i], price_df.iloc[i]['high'] * 1.01)
              for i in range(2, days-2, 6)
              if price_df.iloc[i]['high'] > price_df.iloc[i-1]['high'] and
                 price_df.iloc[i]['high'] > price_df.iloc[i+1]['high']],
        
        # EBOT - Expected support levels
        ebot=[(dates[i], price_df.iloc[i]['low'] * 0.98)
              for i in range(1, days-1, 10)
              if price_df.iloc[i]['low'] < price_df.iloc[max(0, i-5):i+5]['low'].mean()]
    )
    
    # Generate sample events
    events = [
        MarketEvent(
            date=dates[5],
            event_type="earnings",
            description="Q3 Earnings Beat",
            impact="positive",
            importance=4
        ),
        MarketEvent(
            date=dates[12],
            event_type="fed",
            description="Fed Rate Decision",
            impact="neutral",
            importance=3
        ),
        MarketEvent(
            date=dates[18],
            event_type="news",
            description="Analyst Upgrade",
            impact="positive",
            importance=2
        ),
        MarketEvent(
            date=dates[25],
            event_type="economic",
            description="GDP Report",
            impact="negative",
            importance=3
        )
    ]
    
    return price_df, indicators, events


# Example usage function
def create_example_chart():
    """Create example chart for demonstration"""
    chart_creator = SimpleTradeChart()
    
    # Generate sample data
    symbol = "AAPL"
    start_date = datetime(2024, 1, 1)
    price_data, indicators, events = generate_sample_data(symbol, start_date, 30)
    
    # Create chart with trade signal
    trade_date = price_data.index[15]  # Day 15
    output_path = chart_creator.create_ohlc_chart(
        symbol=symbol,
        price_data=price_data,
        indicators=indicators,
        events=events,
        trade_date=trade_date,
        trade_action="buy"
    )
    
    print(f"Chart saved to: {output_path}")
    return output_path


if __name__ == "__main__":
    create_example_chart()