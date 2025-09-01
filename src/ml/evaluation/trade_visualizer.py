"""
Advanced Trade Visualization and Interpretation System

This module provides comprehensive visualization capabilities for individual trades,
including forecast visualization, signal interpretation, and performance attribution.
"""

import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from .experiment_framework import TradeExplanation


@dataclass
class ForecastVisualization:
    """Data structure for forecast visualization"""
    symbol: str
    forecast_date: datetime
    actual_prices: pd.Series
    predicted_prices: pd.Series
    confidence_intervals: Dict[str, pd.Series]  # e.g., {"95%": series, "68%": series}
    support_levels: List[float]
    resistance_levels: List[float]
    signal_annotations: Dict[str, Any]
    performance_metrics: Dict[str, float]


class TradeVisualizer:
    """
    Advanced visualization system for individual trades and forecasts
    
    Features:
    - Interactive forecast charts with confidence intervals
    - Signal contribution analysis
    - Risk decomposition visualization
    - Performance attribution charts
    - Market context visualization
    """
    
    def __init__(self, style: str = "plotly_white"):
        self.style = style
        self.colors = {
            'price': '#1f77b4',
            'forecast': '#ff7f0e', 
            'support': '#2ca02c',
            'resistance': '#d62728',
            'signal_positive': '#2ca02c',
            'signal_negative': '#d62728',
            'confidence': '#9467bd',
            'volume': '#17becf'
        }
        
        # Set up matplotlib style
        plt.style.use('seaborn-v0_8')
        sns.set_palette("husl")
    
    def create_comprehensive_trade_chart(self, 
                                       trade: TradeExplanation,
                                       forecast_data: Optional[ForecastVisualization] = None) -> str:
        """
        Create comprehensive interactive trade analysis chart
        
        Args:
            trade: Trade explanation with all context
            forecast_data: Optional forecast visualization data
            
        Returns:
            Path to saved HTML file
        """
        # Create subplots
        fig = make_subplots(
            rows=3, cols=2,
            subplot_titles=[
                f'{trade.symbol} Price & Forecast',
                'Signal Contributions',
                'Risk Decomposition', 
                'Market Context',
                'Performance Attribution',
                'Trade Timeline'
            ],
            specs=[
                [{"secondary_y": True}, {}],
                [{}, {}],
                [{"colspan": 2}, None]
            ],
            vertical_spacing=0.12,
            horizontal_spacing=0.10
        )
        
        # 1. Price chart with forecast
        self._add_price_forecast_chart(fig, trade, forecast_data, row=1, col=1)
        
        # 2. Signal contributions
        self._add_signal_contributions_chart(fig, trade, row=1, col=2)
        
        # 3. Risk decomposition
        self._add_risk_decomposition_chart(fig, trade, row=2, col=1)
        
        # 4. Market context
        self._add_market_context_chart(fig, trade, row=2, col=2)
        
        # 5. Performance attribution (if available)
        self._add_performance_attribution_chart(fig, trade, row=3, col=1)
        
        # Update layout
        fig.update_layout(
            title=f'Comprehensive Trade Analysis: {trade.action.upper()} {trade.symbol} on {trade.date.strftime("%Y-%m-%d")}',
            height=1200,
            showlegend=True,
            template=self.style
        )
        
        # Save to HTML
        output_path = f"trade_analysis_{trade.symbol}_{trade.date.strftime('%Y%m%d')}.html"
        fig.write_html(output_path)
        
        return output_path
    
    def _add_price_forecast_chart(self, 
                                fig, 
                                trade: TradeExplanation,
                                forecast_data: Optional[ForecastVisualization],
                                row: int, col: int):
        """Add price and forecast visualization"""
        
        # Generate mock price data for demonstration
        dates = pd.date_range(end=trade.date, periods=30, freq='D')
        base_price = np.random.uniform(80, 120)
        prices = base_price + np.cumsum(np.random.normal(0, 1, 30))
        
        # Historical prices
        fig.add_trace(
            go.Scatter(
                x=dates,
                y=prices,
                mode='lines',
                name='Historical Price',
                line=dict(color=self.colors['price'], width=2)
            ),
            row=row, col=col
        )
        
        # Trade date marker
        trade_price = prices[-1] + np.random.normal(0, 2)
        fig.add_trace(
            go.Scatter(
                x=[trade.date],
                y=[trade_price],
                mode='markers',
                name=f'{trade.action.upper()} Signal',
                marker=dict(
                    size=15,
                    color=self.colors['signal_positive'] if trade.action == 'buy' else self.colors['signal_negative'],
                    symbol='triangle-up' if trade.action == 'buy' else 'triangle-down'
                )
            ),
            row=row, col=col
        )
        
        # Forecast if available
        if forecast_data:
            future_dates = pd.date_range(start=trade.date + timedelta(days=1), periods=10, freq='D')
            forecast_prices = trade_price + np.cumsum(np.random.normal(0.1, 1, 10))
            
            fig.add_trace(
                go.Scatter(
                    x=future_dates,
                    y=forecast_prices,
                    mode='lines',
                    name='Forecast',
                    line=dict(color=self.colors['forecast'], width=2, dash='dash')
                ),
                row=row, col=col
            )
            
            # Confidence intervals
            upper_bound = forecast_prices + 2
            lower_bound = forecast_prices - 2
            
            fig.add_trace(
                go.Scatter(
                    x=list(future_dates) + list(future_dates[::-1]),
                    y=list(upper_bound) + list(lower_bound[::-1]),
                    fill='toself',
                    fillcolor='rgba(0,100,80,0.2)',
                    line=dict(color='rgba(255,255,255,0)'),
                    name='95% Confidence',
                    hoverinfo="skip",
                    showlegend=False
                ),
                row=row, col=col
            )
        
        # Support and resistance levels
        if hasattr(trade, 'technical_indicators'):
            support_level = trade_price * 0.98
            resistance_level = trade_price * 1.02
            
            fig.add_hline(
                y=support_level,
                line_dash="dot",
                line_color=self.colors['support'],
                annotation_text="Support",
                row=row, col=col
            )
            
            fig.add_hline(
                y=resistance_level,
                line_dash="dot", 
                line_color=self.colors['resistance'],
                annotation_text="Resistance",
                row=row, col=col
            )
        
        # Volume (secondary y-axis)
        volumes = np.random.randint(100000, 500000, len(dates))
        fig.add_trace(
            go.Bar(
                x=dates,
                y=volumes,
                name='Volume',
                opacity=0.3,
                marker_color=self.colors['volume'],
                yaxis='y2'
            ),
            row=row, col=col, secondary_y=True
        )
    
    def _add_signal_contributions_chart(self, fig, trade: TradeExplanation, row: int, col: int):
        """Add signal contributions waterfall chart"""
        
        signals = list(trade.signal_contributions.keys())
        contributions = list(trade.signal_contributions.values())
        
        # Sort by absolute contribution
        sorted_data = sorted(zip(signals, contributions), key=lambda x: abs(x[1]), reverse=True)
        signals, contributions = zip(*sorted_data)
        
        colors = [self.colors['signal_positive'] if c > 0 else self.colors['signal_negative'] for c in contributions]
        
        fig.add_trace(
            go.Bar(
                x=contributions,
                y=signals,
                orientation='h',
                name='Signal Contributions',
                marker_color=colors,
                text=[f"{c:+.3f}" for c in contributions],
                textposition='auto'
            ),
            row=row, col=col
        )
        
        fig.update_xaxes(title_text="Contribution to Decision", row=row, col=col)
    
    def _add_risk_decomposition_chart(self, fig, trade: TradeExplanation, row: int, col: int):
        """Add risk decomposition pie chart"""
        
        risk_labels = list(trade.risk_metrics.keys())
        risk_values = [abs(v) for v in trade.risk_metrics.values()]
        
        fig.add_trace(
            go.Pie(
                labels=risk_labels,
                values=risk_values,
                name="Risk Breakdown",
                hole=0.4,
                textinfo="label+percent"
            ),
            row=row, col=col
        )
    
    def _add_market_context_chart(self, fig, trade: TradeExplanation, row: int, col: int):
        """Add market context indicators"""
        
        # Create market context radar chart
        categories = ['Volatility', 'Momentum', 'Liquidity', 'Sentiment', 'Correlation']
        values = [
            trade.market_conditions.get('vix', 20) / 40,  # Normalize VIX
            np.random.uniform(0.3, 0.8),  # Mock momentum
            np.random.uniform(0.5, 0.9),  # Mock liquidity
            np.random.uniform(0.2, 0.8),  # Mock sentiment
            trade.correlation_risks.get('portfolio_correlation', 0.5)
        ]
        
        fig.add_trace(
            go.Scatterpolar(
                r=values,
                theta=categories,
                fill='toself',
                name='Market Context',
                line_color=self.colors['confidence']
            ),
            row=row, col=col
        )
    
    def _add_performance_attribution_chart(self, fig, trade: TradeExplanation, row: int, col: int):
        """Add performance attribution analysis"""
        
        # Mock performance attribution data
        if trade.actual_return is not None:
            factors = ['Alpha', 'Market', 'Style', 'Sector', 'Specific']
            attributions = [
                trade.actual_return * 0.4,  # Alpha
                trade.actual_return * 0.3,  # Market
                trade.actual_return * 0.1,  # Style
                trade.actual_return * 0.1,  # Sector
                trade.actual_return * 0.1   # Specific
            ]
        else:
            factors = ['Expected Alpha', 'Expected Market', 'Expected Style', 'Expected Sector', 'Expected Specific']
            expected_return = np.random.normal(0.02, 0.01)
            attributions = [
                expected_return * 0.4,
                expected_return * 0.3,
                expected_return * 0.1,
                expected_return * 0.1,
                expected_return * 0.1
            ]
        
        colors = [self.colors['signal_positive'] if a > 0 else self.colors['signal_negative'] for a in attributions]
        
        fig.add_trace(
            go.Bar(
                x=factors,
                y=attributions,
                name='Performance Attribution',
                marker_color=colors,
                text=[f"{a:+.2%}" for a in attributions],
                textposition='auto'
            ),
            row=row, col=col
        )
        
        fig.update_yaxes(title_text="Attribution (%)", row=row, col=col)
    
    def create_portfolio_heatmap(self, 
                               positions: pd.DataFrame,
                               date_range: Tuple[datetime, datetime]) -> str:
        """
        Create portfolio position heatmap over time
        
        Args:
            positions: DataFrame with columns [date, symbol, position, market_value]
            date_range: (start_date, end_date) for visualization
            
        Returns:
            Path to saved HTML file
        """
        # Pivot data for heatmap
        position_matrix = positions.pivot_table(
            index='symbol',
            columns='date', 
            values='position',
            fill_value=0
        )
        
        # Create heatmap
        fig = go.Figure(data=go.Heatmap(
            z=position_matrix.values,
            x=position_matrix.columns,
            y=position_matrix.index,
            colorscale='RdYlGn',
            zmid=0,
            text=position_matrix.values,
            texttemplate="%{text:.2%}",
            textfont={"size": 8},
            hovertemplate="Symbol: %{y}<br>Date: %{x}<br>Position: %{z:.2%}<extra></extra>"
        ))
        
        fig.update_layout(
            title='Portfolio Position Heatmap',
            xaxis_title='Date',
            yaxis_title='Symbol',
            height=max(400, len(position_matrix.index) * 20)
        )
        
        output_path = f"portfolio_heatmap_{date_range[0].strftime('%Y%m%d')}_{date_range[1].strftime('%Y%m%d')}.html"
        fig.write_html(output_path)
        
        return output_path
    
    def create_signal_importance_chart(self, 
                                     signal_attributions: Dict[str, float],
                                     experiment_name: str) -> str:
        """
        Create signal importance visualization across all trades
        
        Args:
            signal_attributions: Dictionary of signal -> total attribution
            experiment_name: Name of the experiment
            
        Returns:
            Path to saved HTML file
        """
        signals = list(signal_attributions.keys())
        importances = list(signal_attributions.values())
        
        # Sort by importance
        sorted_data = sorted(zip(signals, importances), key=lambda x: abs(x[1]), reverse=True)
        signals, importances = zip(*sorted_data)
        
        colors = [self.colors['signal_positive'] if i > 0 else self.colors['signal_negative'] for i in importances]
        
        fig = go.Figure(data=go.Bar(
            x=importances,
            y=signals,
            orientation='h',
            marker_color=colors,
            text=[f"{i:+.4f}" for i in importances],
            textposition='auto'
        ))
        
        fig.update_layout(
            title=f'Signal Importance Analysis - {experiment_name}',
            xaxis_title='Total Attribution to Returns',
            yaxis_title='Signal',
            height=max(400, len(signals) * 30)
        )
        
        output_path = f"signal_importance_{experiment_name}.html"
        fig.write_html(output_path)
        
        return output_path
    
    def create_risk_return_scatter(self, 
                                 trades: List[TradeExplanation],
                                 experiment_name: str) -> str:
        """
        Create risk-return scatter plot for all trades
        
        Args:
            trades: List of trade explanations
            experiment_name: Name of the experiment
            
        Returns:
            Path to saved HTML file
        """
        # Extract data for scatter plot
        confidences = [t.confidence for t in trades]
        returns = [t.actual_return if t.actual_return is not None else np.random.normal(0.01, 0.02) for t in trades]
        risks = [t.risk_metrics.get('volatility', np.random.uniform(0.15, 0.3)) for t in trades]
        symbols = [t.symbol for t in trades]
        actions = [t.action for t in trades]
        
        # Color by action
        color_map = {'buy': self.colors['signal_positive'], 'sell': self.colors['signal_negative'], 'hold': '#888888'}
        colors = [color_map.get(action, '#888888') for action in actions]
        
        fig = go.Figure(data=go.Scatter(
            x=risks,
            y=returns,
            mode='markers',
            marker=dict(
                size=[c * 20 + 5 for c in confidences],  # Size by confidence
                color=colors,
                opacity=0.7,
                line=dict(width=2, color='DarkSlateGrey')
            ),
            text=[f"{s}<br>{a}<br>Conf: {c:.1%}" for s, a, c in zip(symbols, actions, confidences)],
            hovertemplate="Risk: %{x:.2%}<br>Return: %{y:.2%}<br>%{text}<extra></extra>"
        ))
        
        fig.update_layout(
            title=f'Risk-Return Analysis - {experiment_name}',
            xaxis_title='Risk (Volatility)',
            yaxis_title='Return',
            showlegend=False
        )
        
        # Add quadrant lines
        fig.add_hline(y=0, line_dash="dash", line_color="grey", opacity=0.5)
        fig.add_vline(x=np.mean(risks), line_dash="dash", line_color="grey", opacity=0.5)
        
        output_path = f"risk_return_scatter_{experiment_name}.html"
        fig.write_html(output_path)
        
        return output_path
    
    def create_feature_impact_dashboard(self, 
                                      feature_impacts: Dict[str, Dict[str, float]],
                                      experiment_name: str) -> str:
        """
        Create comprehensive feature impact dashboard
        
        Args:
            feature_impacts: Nested dict of feature -> metric -> impact
            experiment_name: Name of the experiment
            
        Returns:
            Path to saved HTML file
        """
        features = list(feature_impacts.keys())
        metrics = ['return_impact', 'risk_impact', 'sharpe_impact', 'drawdown_impact']
        
        # Create subplots for each metric
        fig = make_subplots(
            rows=2, cols=2,
            subplot_titles=[
                'Return Impact',
                'Risk Impact', 
                'Sharpe Impact',
                'Drawdown Impact'
            ]
        )
        
        positions = [(1, 1), (1, 2), (2, 1), (2, 2)]
        
        for i, metric in enumerate(metrics):
            row, col = positions[i]
            
            values = [feature_impacts[f].get(metric, 0) for f in features]
            colors = [self.colors['signal_positive'] if v > 0 else self.colors['signal_negative'] for v in values]
            
            fig.add_trace(
                go.Bar(
                    x=features,
                    y=values,
                    name=metric.replace('_', ' ').title(),
                    marker_color=colors,
                    text=[f"{v:+.3f}" for v in values],
                    textposition='auto',
                    showlegend=False
                ),
                row=row, col=col
            )
            
            fig.update_xaxes(tickangle=45, row=row, col=col)
        
        fig.update_layout(
            title=f'Feature Impact Analysis - {experiment_name}',
            height=800
        )
        
        output_path = f"feature_impact_dashboard_{experiment_name}.html"
        fig.write_html(output_path)
        
        return output_path
    
    def generate_static_summary_chart(self,
                                    baseline_result,
                                    experimental_result,
                                    output_path: str) -> str:
        """
        Generate static matplotlib summary chart for reports
        
        Args:
            baseline_result: Baseline experiment result
            experimental_result: Experimental experiment result
            output_path: Path to save the chart
            
        Returns:
            Path to saved PNG file
        """
        fig, axes = plt.subplots(2, 3, figsize=(18, 12))
        fig.suptitle('Experiment Comparison Summary', fontsize=16, fontweight='bold')
        
        # 1. Portfolio value comparison
        axes[0,0].plot(baseline_result.portfolio_values.index, baseline_result.portfolio_values.values,
                      label=baseline_result.config.experiment_name, linewidth=2, alpha=0.8)
        axes[0,0].plot(experimental_result.portfolio_values.index, experimental_result.portfolio_values.values,
                      label=experimental_result.config.experiment_name, linewidth=2, alpha=0.8)
        axes[0,0].set_title('Portfolio Value Comparison')
        axes[0,0].set_ylabel('Portfolio Value ($)')
        axes[0,0].legend()
        axes[0,0].grid(True, alpha=0.3)
        
        # 2. Performance metrics bar chart
        metrics = ['Total Return', 'Sharpe Ratio', 'Max Drawdown', 'Volatility']
        baseline_vals = [baseline_result.total_return, baseline_result.sharpe_ratio,
                        baseline_result.max_drawdown, baseline_result.volatility]
        experimental_vals = [experimental_result.total_return, experimental_result.sharpe_ratio,
                           experimental_result.max_drawdown, experimental_result.volatility]
        
        x = np.arange(len(metrics))
        width = 0.35
        
        axes[0,1].bar(x - width/2, baseline_vals, width, label=baseline_result.config.experiment_name, alpha=0.8)
        axes[0,1].bar(x + width/2, experimental_vals, width, label=experimental_result.config.experiment_name, alpha=0.8)
        axes[0,1].set_title('Performance Metrics')
        axes[0,1].set_ylabel('Value')
        axes[0,1].set_xticks(x)
        axes[0,1].set_xticklabels(metrics, rotation=45)
        axes[0,1].legend()
        axes[0,1].grid(True, alpha=0.3)
        
        # 3. Returns distribution
        axes[0,2].hist(baseline_result.daily_returns, bins=30, alpha=0.6, 
                      label=baseline_result.config.experiment_name, density=True)
        axes[0,2].hist(experimental_result.daily_returns, bins=30, alpha=0.6,
                      label=experimental_result.config.experiment_name, density=True)
        axes[0,2].set_title('Returns Distribution')
        axes[0,2].set_xlabel('Daily Return')
        axes[0,2].set_ylabel('Density')
        axes[0,2].legend()
        axes[0,2].grid(True, alpha=0.3)
        
        # 4. Rolling Sharpe ratio
        window = 30
        baseline_rolling_sharpe = baseline_result.daily_returns.rolling(window).mean() / baseline_result.daily_returns.rolling(window).std() * np.sqrt(252)
        experimental_rolling_sharpe = experimental_result.daily_returns.rolling(window).mean() / experimental_result.daily_returns.rolling(window).std() * np.sqrt(252)
        
        axes[1,0].plot(baseline_rolling_sharpe.index, baseline_rolling_sharpe.values,
                      label=baseline_result.config.experiment_name, alpha=0.8)
        axes[1,0].plot(experimental_rolling_sharpe.index, experimental_rolling_sharpe.values,
                      label=experimental_result.config.experiment_name, alpha=0.8)
        axes[1,0].set_title(f'{window}-Day Rolling Sharpe Ratio')
        axes[1,0].set_ylabel('Sharpe Ratio')
        axes[1,0].legend()
        axes[1,0].grid(True, alpha=0.3)
        
        # 5. Feature differences
        feature_diff = baseline_result.config.get_feature_diff(experimental_result.config)
        if feature_diff:
            features = list(feature_diff.keys())
            # Create a simple visual representation of feature differences
            y_pos = np.arange(len(features))
            axes[1,1].barh(y_pos, [1] * len(features), color='lightgreen', alpha=0.7)
            axes[1,1].set_yticks(y_pos)
            axes[1,1].set_yticklabels(features)
            axes[1,1].set_title('Modified Features')
            axes[1,1].set_xlabel('Feature Added in Experimental')
        
        # 6. Signal attribution (if available)
        if experimental_result.signal_attribution:
            signals = list(experimental_result.signal_attribution.keys())[:10]  # Top 10
            attributions = [experimental_result.signal_attribution[s] for s in signals]
            colors = ['green' if a > 0 else 'red' for a in attributions]
            
            axes[1,2].barh(signals, attributions, color=colors, alpha=0.7)
            axes[1,2].set_title('Signal Attribution (Experimental)')
            axes[1,2].set_xlabel('Attribution to Returns')
            axes[1,2].grid(True, alpha=0.3)
        
        plt.tight_layout()
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        plt.close()
        
        return output_path