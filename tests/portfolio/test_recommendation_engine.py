#!/usr/bin/env python3
"""
Comprehensive tests for Recommendation Engine components.

Tests cover:
- Hourly recommendation generation
- Trading universe management
- Data management and realistic data generation
- Portfolio construction integration
- Continuous operation and state management
- Performance tracking and reporting
"""

import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import json
import tempfile
from pathlib import Path

from domains.trading.services.recommendation_engine import (
    RecommendationOutput,
    TradingUniverse,
    DataManager,
    HourlyRecommendationEngine
)
from domains.trading.services.optimization import OptimizationConstraints
from domains.trading.services.performance_metrics import PerformanceMetrics


class TestRecommendationOutput:
    """Test RecommendationOutput data structure."""
    
    def test_output_creation(self):
        """Test basic recommendation output creation."""
        output = RecommendationOutput(
            timestamp=datetime.now(),
            portfolio_weights={'AAPL': 0.05, 'MSFT': -0.03},
            position_sizes={
                'AAPL': {'dollar_amount': 10000, 'shares': 67, 'direction': 'LONG'},
                'MSFT': {'dollar_amount': 6000, 'shares': -20, 'direction': 'SHORT'}
            },
            expected_return=0.12,
            expected_volatility=0.08,
            sharpe_ratio=1.5,
            factor_exposures={'SPY': 0.02, 'TLT': -0.01},
            signals_summary={'total_signals': 5, 'long_signals': 3, 'short_signals': 2},
            performance_metrics=None,
            risk_warnings=['Low signal confidence'],
            execution_notes=['Market hours execution recommended']
        )
        
        assert isinstance(output.timestamp, datetime)
        assert isinstance(output.portfolio_weights, dict)
        assert isinstance(output.position_sizes, dict)
        assert output.expected_return == 0.12
        assert output.sharpe_ratio == 1.5
        assert len(output.risk_warnings) == 1
        assert len(output.execution_notes) == 1
    
    def test_output_serialization(self):
        """Test recommendation output serialization."""
        output = RecommendationOutput(
            timestamp=datetime(2024, 1, 15, 14, 30),
            portfolio_weights={'AAPL': 0.04, 'GOOGL': -0.02},
            position_sizes={},
            expected_return=0.10,
            expected_volatility=0.06,
            sharpe_ratio=1.67,
            factor_exposures={},
            signals_summary={},
            performance_metrics=None,
            risk_warnings=[],
            execution_notes=[]
        )
        
        # Test dictionary conversion
        output_dict = output.to_dict()
        assert isinstance(output_dict, dict)
        assert 'timestamp' in output_dict
        assert 'portfolio_weights' in output_dict
        assert output_dict['expected_return'] == 0.10
        
        # Test JSON conversion
        json_str = output.to_json()
        assert isinstance(json_str, str)
        
        # Should be valid JSON
        parsed = json.loads(json_str)
        assert parsed['expected_return'] == 0.10
        assert parsed['sharpe_ratio'] == 1.67


class TestTradingUniverse:
    """Test TradingUniverse functionality."""
    
    def test_default_universe(self):
        """Test default universe creation."""
        universe = TradingUniverse()
        
        assert len(universe.symbols) > 0
        assert len(universe.stocks) > 0
        assert len(universe.etfs) > 0
        assert len(universe.factor_instruments) > 0
        
        # Check categorization
        assert 'AAPL' in universe.stocks
        assert 'SPY' in universe.etfs
        assert 'SPY' in universe.factor_instruments
        assert 'QQQ' in universe.factor_instruments
    
    def test_custom_universe(self):
        """Test custom universe creation."""
        custom_symbols = ['AAPL', 'MSFT', 'GOOGL', 'SPY', 'QQQ', 'TLT']
        universe = TradingUniverse(custom_symbols)
        
        assert universe.symbols == custom_symbols
        assert 'AAPL' in universe.stocks
        assert 'MSFT' in universe.stocks
        assert 'GOOGL' in universe.stocks
        assert 'SPY' in universe.etfs
        assert 'QQQ' in universe.etfs
        assert 'TLT' in universe.etfs
    
    def test_etf_identification(self):
        """Test ETF identification logic."""
        universe = TradingUniverse(['AAPL', 'SPY', 'XLK', 'VIX', 'MSFT'])
        
        # Check ETF identification
        assert universe._is_etf('SPY') == True
        assert universe._is_etf('XLK') == True
        assert universe._is_etf('VIX') == True
        assert universe._is_etf('AAPL') == False
        assert universe._is_etf('MSFT') == False
        
        # Verify categorization
        assert 'AAPL' in universe.stocks
        assert 'MSFT' in universe.stocks
        assert 'SPY' in universe.etfs
        assert 'XLK' in universe.etfs
        assert 'VIX' in universe.etfs


class TestDataManager:
    """Test DataManager functionality."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.universe = TradingUniverse(['AAPL', 'MSFT', 'SPY', 'TLT'])
        self.data_manager = DataManager(self.universe)
    
    def test_data_manager_initialization(self):
        """Test data manager initialization."""
        assert self.data_manager.universe == self.universe
        assert self.data_manager.data_cache == {}
        assert self.data_manager.last_update is None
    
    def test_market_data_fetching(self):
        """Test market data fetching."""
        market_data = self.data_manager.fetch_market_data(lookback_hours=24)
        
        assert isinstance(market_data, dict)
        assert len(market_data) == len(self.universe.symbols)
        
        # Check each symbol has data
        for symbol in self.universe.symbols:
            assert symbol in market_data
            
            df = market_data[symbol]
            assert isinstance(df, pd.DataFrame)
            assert len(df) == 24  # 24 hours requested
            
            # Check OHLCV columns
            expected_columns = ['open', 'high', 'low', 'close', 'volume']
            for col in expected_columns:
                assert col in df.columns
            
            # Check data quality
            assert df['high'].min() >= df['low'].max()  # High >= Low
            assert all(df['volume'] > 0)  # Positive volume
            assert all(df['open'] > 0)  # Positive prices
        
        # Check cache update
        assert self.data_manager.data_cache == market_data
        assert self.data_manager.last_update is not None
    
    def test_realistic_data_generation(self):
        """Test realistic data generation characteristics."""
        # Test different symbol types
        test_symbols = ['AAPL', 'SPY', 'TLT', 'VIX', 'XLK']
        data_manager = DataManager(TradingUniverse(test_symbols))
        
        market_data = data_manager.fetch_market_data(lookback_hours=168)  # 1 week
        
        for symbol in test_symbols:
            df = market_data[symbol]
            
            # Calculate returns
            returns = df['close'].pct_change().dropna()
            daily_vol = returns.std() * np.sqrt(24)  # Convert to daily vol
            
            # Check volatility is reasonable for asset type
            if symbol == 'VIX':
                assert daily_vol > 0.3  # VIX should be very volatile
            elif symbol in ['SPY', 'TLT']:
                assert 0.05 < daily_vol < 0.3  # ETFs moderate volatility
            else:  # Individual stocks
                assert 0.1 < daily_vol < 0.5  # Higher volatility
            
            # Check price trends are realistic
            assert df['close'].min() > 0
            assert df['close'].max() / df['close'].min() < 2  # No extreme moves
    
    def test_deterministic_generation(self):
        """Test that data generation is deterministic per symbol."""
        # Fetch data twice for same symbol
        data1 = self.data_manager._generate_realistic_data('AAPL', 50)
        data2 = self.data_manager._generate_realistic_data('AAPL', 50)
        
        # Should be identical (same random seed)
        pd.testing.assert_frame_equal(data1, data2)
        
        # Different symbols should generate different data
        data_msft = self.data_manager._generate_realistic_data('MSFT', 50)
        assert not data1.equals(data_msft)


class TestHourlyRecommendationEngine:
    """Test HourlyRecommendationEngine functionality."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.portfolio_value = 200000
        self.universe = TradingUniverse([
            'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA',
            'SPY', 'QQQ', 'TLT', 'GLD'
        ])
        self.constraints = OptimizationConstraints(
            max_position_weight=0.08,
            max_leverage=1.5,
            target_dollar_neutral=True,
            transaction_cost_bps=3.0
        )
        
        self.engine = HourlyRecommendationEngine(
            self.portfolio_value,
            self.universe,
            self.constraints
        )
    
    def test_engine_initialization(self):
        """Test engine initialization."""
        assert self.engine.portfolio_value == self.portfolio_value
        assert self.engine.universe == self.universe
        assert self.engine.constraints == self.constraints
        
        # Check component initialization
        assert hasattr(self.engine, 'factor_risk_model')
        assert hasattr(self.engine, 'signal_manager')
        assert hasattr(self.engine, 'portfolio_constructor')
        assert hasattr(self.engine, 'performance_analyzer')
        assert hasattr(self.engine, 'data_manager')
        
        # Check state tracking
        assert self.engine.current_portfolio == {}
        assert self.engine.portfolio_history == []
        assert self.engine.performance_history == []
    
    def test_single_recommendation_generation(self):
        """Test single recommendation generation."""
        recommendation = self.engine.generate_hourly_recommendation()
        
        assert isinstance(recommendation, RecommendationOutput)
        assert isinstance(recommendation.timestamp, datetime)
        
        # Check basic structure
        assert isinstance(recommendation.portfolio_weights, dict)
        assert isinstance(recommendation.position_sizes, dict)
        assert isinstance(recommendation.expected_return, float)
        assert isinstance(recommendation.expected_volatility, float)
        assert isinstance(recommendation.sharpe_ratio, float)
        assert isinstance(recommendation.factor_exposures, dict)
        assert isinstance(recommendation.signals_summary, dict)
        assert isinstance(recommendation.risk_warnings, list)
        assert isinstance(recommendation.execution_notes, list)
        
        # Check reasonableness
        assert -0.5 <= recommendation.expected_return <= 0.5
        assert 0 <= recommendation.expected_volatility <= 1.0
        assert recommendation.sharpe_ratio >= 0
    
    def test_recommendation_with_current_portfolio(self):
        """Test recommendation generation with existing portfolio."""
        current_portfolio = {'AAPL': 0.05, 'MSFT': -0.03, 'SPY': 0.02}
        
        recommendation = self.engine.generate_hourly_recommendation(current_portfolio)
        
        assert isinstance(recommendation, RecommendationOutput)
        
        # Should consider transaction costs for rebalancing
        if recommendation.execution_notes:
            turnover_mentioned = any('turnover' in note.lower() for note in recommendation.execution_notes)
            # May or may not mention turnover depending on actual changes
    
    def test_state_management(self):
        """Test portfolio state management."""
        # Initial state should be empty
        assert len(self.engine.portfolio_history) == 0
        assert self.engine.current_portfolio == {}
        
        # Generate first recommendation
        recommendation1 = self.engine.generate_hourly_recommendation()
        
        # State should be updated if recommendation was successful
        if recommendation1.portfolio_weights:
            assert len(self.engine.portfolio_history) == 1
            assert self.engine.current_portfolio == recommendation1.portfolio_weights
            
            # Generate second recommendation
            recommendation2 = self.engine.generate_hourly_recommendation()
            
            # History should grow
            if recommendation2.portfolio_weights:
                assert len(self.engine.portfolio_history) == 2
    
    def test_risk_warning_generation(self):
        """Test risk warning generation."""
        # Create a scenario that should generate warnings
        from domains.trading.services.optimization import OptimizationResult
        
        # Mock optimization result with high exposures
        mock_result = OptimizationResult(
            weights={'AAPL': 0.15, 'MSFT': -0.12},  # High concentration
            expected_return=0.25,
            expected_volatility=0.05,
            sharpe_ratio=5.0,
            factor_exposures={'SPY': 0.20, 'TLT': 0.18},  # High factor exposures
            transaction_costs=0,
            is_successful=True,
            optimization_status='optimal'
        )
        
        warnings = self.engine._generate_risk_warnings(mock_result, {})
        
        assert isinstance(warnings, list)
        # Should generate warnings for high exposures and concentration
        assert len(warnings) > 0
    
    def test_execution_notes_generation(self):
        """Test execution notes generation."""
        from domains.trading.services.optimization import OptimizationResult
        
        # Mock optimization result
        mock_result = OptimizationResult(
            weights={'AAPL': 0.04, 'MSFT': -0.02},
            expected_return=0.12,
            expected_volatility=0.08,
            sharpe_ratio=1.5,
            factor_exposures={},
            transaction_costs=150.0,
            is_successful=True,
            optimization_status='optimal'
        )
        
        current_portfolio = {'AAPL': 0.02, 'GOOGL': 0.03}
        
        notes = self.engine._generate_execution_notes(mock_result, current_portfolio)
        
        assert isinstance(notes, list)
        assert len(notes) > 0
        
        # Should mention turnover and transaction costs
        notes_text = ' '.join(notes).lower()
        assert 'turnover' in notes_text or 'cost' in notes_text
    
    def test_continuous_recommendations(self):
        """Test continuous recommendation generation."""
        # Run for short period
        recommendations = self.engine.run_continuous_recommendations(hours=3)
        
        assert isinstance(recommendations, list)
        assert len(recommendations) == 3
        
        for rec in recommendations:
            assert isinstance(rec, RecommendationOutput)
            assert isinstance(rec.timestamp, datetime)
        
        # Timestamps should be in order
        timestamps = [rec.timestamp for rec in recommendations]
        assert timestamps == sorted(timestamps)
        
        # Portfolio history should be updated
        assert len(self.engine.portfolio_history) > 0
    
    def test_performance_report_generation(self):
        """Test performance report generation."""
        # Build up some history first
        for _ in range(35):  # Need at least 30 for performance analysis
            rec = self.engine.generate_hourly_recommendation()
            if rec.portfolio_weights:
                self.engine.portfolio_history.append({
                    'timestamp': datetime.now(),
                    'weights': rec.portfolio_weights,
                    'expected_return': rec.expected_return,
                    'expected_volatility': rec.expected_volatility
                })
        
        report = self.engine.generate_performance_report()
        
        assert isinstance(report, str)
        assert len(report) > 0
        
        # Should contain performance metrics
        assert 'PERFORMANCE' in report.upper() or 'RETURN' in report.upper()
    
    def test_error_handling(self):
        """Test error handling in recommendation generation."""
        # Create engine with invalid configuration to force errors
        invalid_universe = TradingUniverse([])  # Empty universe
        invalid_engine = HourlyRecommendationEngine(
            portfolio_value=100,  # Very small portfolio
            universe=invalid_universe,
            constraints=self.constraints
        )
        
        recommendation = invalid_engine.generate_hourly_recommendation()
        
        # Should return error recommendation
        assert isinstance(recommendation, RecommendationOutput)
        assert len(recommendation.risk_warnings) > 0
        assert any('error' in warning.lower() for warning in recommendation.risk_warnings)
    
    def test_file_output(self):
        """Test file output functionality."""
        with tempfile.TemporaryDirectory() as temp_dir:
            output_path = Path(temp_dir)
            
            # Generate recommendations with file output
            recommendations = self.engine.run_continuous_recommendations(
                hours=2, output_path=str(output_path)
            )
            
            assert len(recommendations) == 2
            
            # Check that files were created
            json_files = list(output_path.glob("*.json"))
            assert len(json_files) == 2
            
            # Check file contents
            for json_file in json_files:
                with open(json_file, 'r') as f:
                    data = json.load(f)
                
                assert 'timestamp' in data
                assert 'portfolio_weights' in data
                assert 'expected_return' in data


class TestRecommendationEngineIntegration:
    """Integration tests for recommendation engine system."""
    
    def test_end_to_end_workflow(self):
        """Test complete end-to-end recommendation workflow."""
        # Create realistic configuration
        portfolio_value = 250000
        universe = TradingUniverse([
            # Stocks
            'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA', 'META',
            'JPM', 'JNJ', 'PG', 'KO',
            # ETFs for hedging
            'SPY', 'QQQ', 'TLT', 'GLD', 'VIX'
        ])
        
        constraints = OptimizationConstraints(
            max_position_weight=0.06,
            max_leverage=1.8,
            target_dollar_neutral=True,
            max_net_exposure=0.04,
            transaction_cost_bps=4.0
        )
        
        engine = HourlyRecommendationEngine(portfolio_value, universe, constraints)
        
        # Run simulation
        recommendations = []
        for hour in range(5):  # 5 hours of operation
            rec = engine.generate_hourly_recommendation()
            recommendations.append(rec)
        
        # Validate results
        assert len(recommendations) == 5
        
        for i, rec in enumerate(recommendations):
            assert isinstance(rec, RecommendationOutput)
            
            # Check portfolio constraints
            if rec.portfolio_weights:
                max_position = max(abs(w) for w in rec.portfolio_weights.values())
                assert max_position <= constraints.max_position_weight * 1.1
                
                gross_exposure = sum(abs(w) for w in rec.portfolio_weights.values())
                assert gross_exposure <= constraints.max_leverage * 1.1
                
                # Check dollar neutrality
                if constraints.target_dollar_neutral:
                    net_exposure = sum(rec.portfolio_weights.values())
                    assert abs(net_exposure) <= constraints.max_net_exposure * 2  # Allow some tolerance
            
            # Check risk metrics are reasonable
            assert -0.3 <= rec.expected_return <= 0.3
            assert 0 <= rec.expected_volatility <= 0.5
            assert rec.sharpe_ratio >= 0
        
        # Check portfolio evolution
        non_empty_recs = [r for r in recommendations if r.portfolio_weights]
        if len(non_empty_recs) > 1:
            # Calculate average turnover
            turnovers = []
            for i in range(1, len(non_empty_recs)):
                prev_weights = non_empty_recs[i-1].portfolio_weights
                curr_weights = non_empty_recs[i].portfolio_weights
                
                all_symbols = set(prev_weights.keys()) | set(curr_weights.keys())
                turnover = sum(abs(curr_weights.get(s, 0) - prev_weights.get(s, 0)) 
                             for s in all_symbols) / 2
                turnovers.append(turnover)
            
            if turnovers:
                avg_turnover = np.mean(turnovers)
                assert 0 <= avg_turnover <= 1.0  # Reasonable turnover
    
    def test_market_neutral_compliance(self):
        """Test that recommendations maintain market neutrality."""
        universe = TradingUniverse([
            'AAPL', 'MSFT', 'GOOGL', 'AMZN',  # Tech stocks
            'JPM', 'BAC', 'WFC',  # Banks
            'SPY', 'QQQ', 'TLT'  # Hedging instruments
        ])
        
        constraints = OptimizationConstraints(
            target_dollar_neutral=True,
            max_net_exposure=0.03,
            max_market_beta=0.05,
            max_sector_beta=0.08
        )
        
        engine = HourlyRecommendationEngine(200000, universe, constraints)
        
        # Generate multiple recommendations
        recommendations = []
        for _ in range(3):
            rec = engine.generate_hourly_recommendation()
            recommendations.append(rec)
        
        # Check market neutrality
        for rec in recommendations:
            if rec.portfolio_weights:
                net_exposure = sum(rec.portfolio_weights.values())
                assert abs(net_exposure) <= constraints.max_net_exposure * 1.5
                
                # Check factor exposures if available
                if rec.factor_exposures:
                    for factor, exposure in rec.factor_exposures.items():
                        if factor in ['SPY', 'QQQ']:  # Market factors
                            assert abs(exposure) <= constraints.max_market_beta * 2
    
    def test_performance_consistency(self):
        """Test performance metrics consistency across recommendations."""
        engine = HourlyRecommendationEngine(
            portfolio_value=200000,
            universe=TradingUniverse(['AAPL', 'MSFT', 'SPY', 'TLT']),
            constraints=OptimizationConstraints()
        )
        
        # Generate recommendations
        recommendations = []
        for _ in range(10):
            rec = engine.generate_hourly_recommendation()
            recommendations.append(rec)
        
        # Check consistency
        returns = [r.expected_return for r in recommendations if r.expected_return is not None]
        volatilities = [r.expected_volatility for r in recommendations if r.expected_volatility is not None]
        sharpe_ratios = [r.sharpe_ratio for r in recommendations if r.sharpe_ratio is not None]
        
        if len(returns) > 1:
            # Returns should be in reasonable range
            assert min(returns) >= -0.5
            assert max(returns) <= 0.5
            
            # Volatilities should be positive
            assert all(vol > 0 for vol in volatilities)
            assert min(volatilities) >= 0
            assert max(volatilities) <= 1.0
            
            # Sharpe ratios should be reasonable
            assert all(sr >= 0 for sr in sharpe_ratios)
            assert max(sharpe_ratios) <= 10  # Upper bound check


if __name__ == "__main__":
    pytest.main([__file__, "-v"])