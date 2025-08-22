"""
Comprehensive tests for Backtest Analytics API

Tests all endpoints, error handling, authentication, and integration scenarios.
"""

import pytest
import asyncio
import json
import uuid
from datetime import date, datetime, timedelta
from unittest.mock import AsyncMock, MagicMock, patch
from fastapi.testclient import TestClient
from fastapi.websockets import WebSocket

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from api.backtest_analytics_api import app
from analytics.portfolio_analytics import PortfolioMetrics, AttributionMetrics, ModelPerformanceMetrics

@pytest.fixture
def client():
    """Create test client for the FastAPI app"""
    return TestClient(app)

@pytest.fixture
def mock_analytics_engine():
    """Create mock analytics engine"""
    engine = AsyncMock()
    
    # Mock portfolio metrics
    engine.compute_portfolio_metrics.return_value = PortfolioMetrics(
        total_return=0.15,
        annualized_return=0.12,
        volatility=0.18,
        sharpe_ratio=1.2,
        sortino_ratio=1.5,
        calmar_ratio=0.8,
        max_drawdown=0.08,
        max_drawdown_duration_days=45,
        var_95=-0.025,
        var_99=-0.045,
        expected_shortfall_95=-0.035,
        expected_shortfall_99=-0.055,
        total_trades=150,
        win_rate=0.65,
        profit_factor=1.8,
        avg_win=0.023,
        avg_loss=-0.015,
        largest_win=0.085,
        largest_loss=-0.065,
        information_ratio=0.85,
        treynor_ratio=0.095,
        jensen_alpha=0.025,
        beta=0.92,
        correlation_to_benchmark=0.78,
        start_date=date(2023, 1, 1),
        end_date=date(2024, 6, 30),
        total_days=365
    )
    
    # Mock attribution metrics
    engine.compute_attribution_analysis.return_value = AttributionMetrics(
        stock_attribution={"AAPL": 0.025, "MSFT": 0.018, "GOOGL": 0.012},
        stock_weights={"AAPL": 0.15, "MSFT": 0.12, "GOOGL": 0.10},
        stock_returns={"AAPL": 0.18, "MSFT": 0.15, "GOOGL": 0.12},
        sector_attribution={"Technology": 0.055, "Healthcare": 0.032},
        sector_weights={"Technology": 0.37, "Healthcare": 0.25},
        sector_returns={"Technology": 0.15, "Healthcare": 0.13},
        signal_attribution={"support_bounce": 0.045, "resistance_break": 0.028},
        signal_win_rates={"support_bounce": 0.72, "resistance_break": 0.68},
        signal_trade_counts={"support_bounce": 65, "resistance_break": 42},
        monthly_attribution={"2023-01": 0.018, "2023-02": 0.025},
        quarterly_attribution={"Q1-2023": 0.043, "Q2-2023": 0.038}
    )
    
    # Mock model performance
    engine.compute_model_performance.return_value = ModelPerformanceMetrics(
        support_accuracy=0.72,
        resistance_accuracy=0.68,
        overall_accuracy=0.70,
        confidence_correlation=0.65,
        confidence_calibration={
            "0.0-0.2": 0.15,
            "0.2-0.4": 0.35,
            "0.4-0.6": 0.55,
            "0.6-0.8": 0.75,
            "0.8-1.0": 0.85
        },
        support_mae=0.024,
        resistance_mae=0.028,
        overall_mae=0.026,
        model_versions=[1, 2, 3, 4],
        accuracy_by_version={1: 0.65, 2: 0.68, 3: 0.70, 4: 0.72},
        retrain_dates=[
            date(2023, 1, 15),
            date(2023, 2, 15),
            date(2023, 3, 15)
        ],
        feature_importance={
            "rsi_14": 0.15,
            "support_distance": 0.20,
            "volume_ratio": 0.12
        }
    )
    
    return engine

class TestHealthEndpoint:
    """Test health check endpoint"""
    
    def test_health_check(self, client):
        """Test basic health check"""
        response = client.get("/health")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "healthy"
        assert "timestamp" in data

class TestBacktestListEndpoint:
    """Test backtest listing endpoint"""
    
    def test_list_backtests_default(self, client):
        """Test listing backtests with default parameters"""
        response = client.get("/api/v1/backtests")
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)
        assert len(data) >= 0
        
        if data:
            backtest = data[0]
            assert "backtest_run_id" in backtest
            assert "strategy_name" in backtest
            assert "strategy_type" in backtest
    
    def test_list_backtests_with_filters(self, client):
        """Test listing backtests with filters"""
        response = client.get("/api/v1/backtests", params={
            "strategy_type": "adaptive",
            "limit": 10,
            "offset": 0
        })
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)
    
    def test_list_backtests_pagination(self, client):
        """Test pagination parameters"""
        response = client.get("/api/v1/backtests", params={
            "limit": 5,
            "offset": 10
        })
        assert response.status_code == 200
    
    def test_list_backtests_invalid_limit(self, client):
        """Test invalid limit parameter"""
        response = client.get("/api/v1/backtests", params={"limit": 150})
        assert response.status_code == 422  # Validation error

class TestPortfolioEndpoints:
    """Test portfolio analytics endpoints"""
    
    @patch('api.backtest_analytics_api.get_analytics_engine')
    def test_get_portfolio_metrics(self, mock_get_engine, client, mock_analytics_engine):
        """Test getting portfolio metrics"""
        mock_get_engine.return_value = mock_analytics_engine
        
        response = client.get("/api/v1/backtests/test_id/portfolio/metrics")
        assert response.status_code == 200
        
        data = response.json()
        assert data["total_return"] == 0.15
        assert data["sharpe_ratio"] == 1.2
        assert data["max_drawdown"] == 0.08
        
        # Verify engine was called with correct parameters
        mock_analytics_engine.compute_portfolio_metrics.assert_called_once()
    
    @patch('api.backtest_analytics_api.get_analytics_engine')
    def test_get_portfolio_metrics_with_date_range(self, mock_get_engine, client, mock_analytics_engine):
        """Test getting portfolio metrics with date range"""
        mock_get_engine.return_value = mock_analytics_engine
        
        response = client.get("/api/v1/backtests/test_id/portfolio/metrics", params={
            "start_date": "2023-01-01",
            "end_date": "2023-12-31",
            "benchmark_id": "benchmark_test"
        })
        assert response.status_code == 200
        
        # Verify correct parameters were passed
        call_args = mock_analytics_engine.compute_portfolio_metrics.call_args
        assert call_args[1]["backtest_run_id"] == "test_id"
        assert call_args[1]["benchmark_run_id"] == "benchmark_test"
    
    @patch('api.backtest_analytics_api.get_analytics_engine')
    def test_get_portfolio_performance(self, mock_get_engine, client, mock_analytics_engine):
        """Test getting portfolio performance time series"""
        # Mock performance data
        mock_analytics_engine._fetch_portfolio_performance_data.return_value = AsyncMock()
        mock_performance_data = MagicMock()
        mock_performance_data.empty = False
        mock_performance_data.iterrows.return_value = [
            (datetime(2023, 1, 1), {
                'portfolio_value': 100000,
                'daily_return': 0.01,
                'cumulative_return': 0.01,
                'drawdown': 0.0,
                'positions_count': 10
            })
        ]
        mock_analytics_engine._fetch_portfolio_performance_data.return_value = mock_performance_data
        mock_get_engine.return_value = mock_analytics_engine
        
        response = client.get("/api/v1/backtests/test_id/portfolio/performance")
        assert response.status_code == 200
    
    @patch('api.backtest_analytics_api.get_analytics_engine')
    def test_get_attribution_analysis(self, mock_get_engine, client, mock_analytics_engine):
        """Test getting attribution analysis"""
        mock_get_engine.return_value = mock_analytics_engine
        
        response = client.get("/api/v1/backtests/test_id/attribution")
        assert response.status_code == 200
        
        data = response.json()
        assert "stock_attribution" in data
        assert "AAPL" in data["stock_attribution"]
        assert data["stock_attribution"]["AAPL"] == 0.025

class TestModelEndpoints:
    """Test model performance endpoints"""
    
    @patch('api.backtest_analytics_api.get_analytics_engine')
    def test_get_model_performance(self, mock_get_engine, client, mock_analytics_engine):
        """Test getting model performance metrics"""
        mock_get_engine.return_value = mock_analytics_engine
        
        response = client.get("/api/v1/backtests/test_id/model/performance")
        assert response.status_code == 200
        
        data = response.json()
        assert data["support_accuracy"] == 0.72
        assert data["resistance_accuracy"] == 0.68
        assert data["overall_accuracy"] == 0.70
        assert len(data["retrain_dates"]) == 3
    
    def test_get_forecasts(self, client):
        """Test getting forecasts endpoint"""
        response = client.get("/api/v1/backtests/test_id/forecasts")
        assert response.status_code == 200
        
        data = response.json()
        assert isinstance(data, list)

class TestComparisonEndpoints:
    """Test comparison endpoints"""
    
    @patch('api.backtest_analytics_api.get_analytics_engine')
    def test_compare_portfolios(self, mock_get_engine, client, mock_analytics_engine):
        """Test portfolio comparison"""
        mock_get_engine.return_value = mock_analytics_engine
        
        request_data = {
            "backtest_run_ids": ["test_1", "test_2"],
            "start_date": "2023-01-01",
            "end_date": "2023-12-31"
        }
        
        response = client.post("/api/v1/comparison/portfolio", json=request_data)
        assert response.status_code == 200
        
        data = response.json()
        assert "comparison_summary" in data
        assert "individual_metrics" in data
        assert "relative_performance" in data
    
    @patch('api.backtest_analytics_api.get_analytics_engine')
    def test_compare_models(self, mock_get_engine, client, mock_analytics_engine):
        """Test model comparison"""
        mock_get_engine.return_value = mock_analytics_engine
        
        request_data = {
            "backtest_run_ids": ["test_1", "test_2"]
        }
        
        response = client.post("/api/v1/comparison/models", json=request_data)
        assert response.status_code == 200
        
        data = response.json()
        assert "comparison_summary" in data
        assert "individual_performance" in data
    
    def test_compare_portfolios_invalid_request(self, client):
        """Test portfolio comparison with invalid request"""
        request_data = {
            "backtest_run_ids": ["only_one"]  # Need at least 2
        }
        
        response = client.post("/api/v1/comparison/portfolio", json=request_data)
        assert response.status_code == 422  # Validation error

class TestDrillDownEndpoints:
    """Test drill-down analysis endpoints"""
    
    @patch('api.backtest_analytics_api.get_analytics_engine')
    def test_drill_down_period(self, mock_get_engine, client, mock_analytics_engine):
        """Test period drill-down"""
        mock_analytics_engine.drill_down_analysis.return_value = AsyncMock()
        mock_get_engine.return_value = mock_analytics_engine
        
        response = client.get("/api/v1/backtests/test_id/drill-down/period", params={
            "start_date": "2023-01-01",
            "end_date": "2023-01-31"
        })
        assert response.status_code == 200
    
    @patch('api.backtest_analytics_api.get_analytics_engine')
    def test_drill_down_stock(self, mock_get_engine, client, mock_analytics_engine):
        """Test stock drill-down"""
        mock_analytics_engine.drill_down_analysis.return_value = AsyncMock()
        mock_get_engine.return_value = mock_analytics_engine
        
        response = client.get("/api/v1/backtests/test_id/drill-down/stock/AAPL")
        assert response.status_code == 200
    
    @patch('api.backtest_analytics_api.get_analytics_engine')
    def test_drill_down_trade(self, mock_get_engine, client, mock_analytics_engine):
        """Test trade drill-down"""
        mock_analytics_engine.drill_down_analysis.return_value = AsyncMock()
        mock_get_engine.return_value = mock_analytics_engine
        
        trade_id = str(uuid.uuid4())
        response = client.get(f"/api/v1/backtests/test_id/drill-down/trade/{trade_id}")
        assert response.status_code == 200

class TestWebSocketEndpoints:
    """Test WebSocket endpoints"""
    
    def test_portfolio_websocket_connection(self, client):
        """Test WebSocket connection for portfolio updates"""
        with client.websocket_connect("/ws/backtests/test_id/portfolio") as websocket:
            # Should be able to connect
            data = websocket.receive_json()
            assert data["type"] == "portfolio_metrics"
    
    def test_portfolio_websocket_request_update(self, client):
        """Test requesting updates via WebSocket"""
        with client.websocket_connect("/ws/backtests/test_id/portfolio") as websocket:
            # Receive initial data
            initial_data = websocket.receive_json()
            assert initial_data["type"] == "portfolio_metrics"
            
            # Request update
            websocket.send_json({"type": "request_update"})
            
            # Should receive update
            update_data = websocket.receive_json()
            assert update_data["type"] == "portfolio_update"

class TestErrorHandling:
    """Test error handling scenarios"""
    
    def test_nonexistent_backtest(self, client):
        """Test accessing nonexistent backtest"""
        response = client.get("/api/v1/backtests/nonexistent/portfolio/metrics")
        # Should handle gracefully (actual behavior depends on implementation)
        assert response.status_code in [404, 500]
    
    def test_invalid_date_format(self, client):
        """Test invalid date format"""
        response = client.get("/api/v1/backtests/test_id/portfolio/metrics", params={
            "start_date": "invalid-date"
        })
        assert response.status_code == 422
    
    def test_invalid_granularity(self, client):
        """Test invalid granularity parameter"""
        response = client.get("/api/v1/backtests/test_id/portfolio/performance", params={
            "granularity": "invalid"
        })
        assert response.status_code == 422
    
    @patch('api.backtest_analytics_api.get_analytics_engine')
    def test_analytics_engine_error(self, mock_get_engine, client):
        """Test analytics engine error handling"""
        mock_engine = AsyncMock()
        mock_engine.compute_portfolio_metrics.side_effect = Exception("Database error")
        mock_get_engine.return_value = mock_engine
        
        response = client.get("/api/v1/backtests/test_id/portfolio/metrics")
        assert response.status_code == 500

class TestCacheEndpoints:
    """Test cache management endpoints"""
    
    @patch('api.backtest_analytics_api.get_analytics_engine')
    def test_invalidate_cache(self, mock_get_engine, client, mock_analytics_engine):
        """Test cache invalidation"""
        mock_get_engine.return_value = mock_analytics_engine
        
        response = client.post("/api/v1/cache/invalidate")
        assert response.status_code == 200
        
        data = response.json()
        assert data["status"] == "success"
    
    @patch('api.backtest_analytics_api.get_analytics_engine')
    def test_invalidate_cache_with_pattern(self, mock_get_engine, client, mock_analytics_engine):
        """Test cache invalidation with pattern"""
        mock_get_engine.return_value = mock_analytics_engine
        
        response = client.post("/api/v1/cache/invalidate", params={
            "pattern": "portfolio_metrics:*"
        })
        assert response.status_code == 200

@pytest.mark.integration
class TestIntegrationScenarios:
    """Integration tests for complete workflows"""
    
    @patch('api.backtest_analytics_api.get_analytics_engine')
    def test_complete_dashboard_workflow(self, mock_get_engine, client, mock_analytics_engine):
        """Test complete dashboard data loading workflow"""
        mock_get_engine.return_value = mock_analytics_engine
        
        # Mock performance data for the performance endpoint
        mock_performance_data = MagicMock()
        mock_performance_data.empty = False
        mock_performance_data.iterrows.return_value = []
        mock_analytics_engine._fetch_portfolio_performance_data.return_value = mock_performance_data
        
        backtest_id = "integration_test"
        
        # 1. Get portfolio metrics
        metrics_response = client.get(f"/api/v1/backtests/{backtest_id}/portfolio/metrics")
        assert metrics_response.status_code == 200
        
        # 2. Get performance data
        performance_response = client.get(f"/api/v1/backtests/{backtest_id}/portfolio/performance")
        assert performance_response.status_code == 200
        
        # 3. Get attribution
        attribution_response = client.get(f"/api/v1/backtests/{backtest_id}/attribution")
        assert attribution_response.status_code == 200
        
        # 4. Get model performance
        model_response = client.get(f"/api/v1/backtests/{backtest_id}/model/performance")
        assert model_response.status_code == 200
        
        # All should succeed
        assert all(r.status_code == 200 for r in [
            metrics_response, performance_response, attribution_response, model_response
        ])
    
    @patch('api.backtest_analytics_api.get_analytics_engine')
    def test_comparison_workflow(self, mock_get_engine, client, mock_analytics_engine):
        """Test strategy comparison workflow"""
        mock_get_engine.return_value = mock_analytics_engine
        
        # Compare two strategies
        comparison_request = {
            "backtest_run_ids": ["adaptive_strategy", "static_strategy"],
            "start_date": "2023-01-01",
            "end_date": "2023-12-31"
        }
        
        response = client.post("/api/v1/comparison/portfolio", json=comparison_request)
        assert response.status_code == 200
        
        data = response.json()
        assert len(data["individual_metrics"]) == 2
        assert "adaptive_strategy" in data["individual_metrics"]
        assert "static_strategy" in data["individual_metrics"]

@pytest.mark.performance
class TestPerformanceScenarios:
    """Performance and load testing scenarios"""
    
    @patch('api.backtest_analytics_api.get_analytics_engine')
    def test_concurrent_requests(self, mock_get_engine, client, mock_analytics_engine):
        """Test handling concurrent requests"""
        mock_get_engine.return_value = mock_analytics_engine
        
        import threading
        import time
        
        results = []
        
        def make_request():
            response = client.get("/api/v1/backtests/test_id/portfolio/metrics")
            results.append(response.status_code)
        
        # Create multiple threads
        threads = []
        for _ in range(10):
            thread = threading.Thread(target=make_request)
            threads.append(thread)
        
        # Start all threads
        start_time = time.time()
        for thread in threads:
            thread.start()
        
        # Wait for completion
        for thread in threads:
            thread.join()
        
        end_time = time.time()
        
        # All requests should succeed
        assert all(status == 200 for status in results)
        assert len(results) == 10
        
        # Should complete within reasonable time
        assert end_time - start_time < 5.0
    
    @patch('api.backtest_analytics_api.get_analytics_engine')
    def test_large_data_response(self, mock_get_engine, client, mock_analytics_engine):
        """Test handling large data responses"""
        # Mock large performance dataset
        large_data = MagicMock()
        large_data.empty = False
        large_data.iterrows.return_value = [
            (datetime(2023, 1, 1) + timedelta(days=i), {
                'portfolio_value': 100000 + i * 100,
                'daily_return': 0.001,
                'cumulative_return': i * 0.001,
                'drawdown': 0.0,
                'positions_count': 10
            })
            for i in range(1000)  # 1000 data points
        ]
        
        mock_analytics_engine._fetch_portfolio_performance_data.return_value = large_data
        mock_get_engine.return_value = mock_analytics_engine
        
        response = client.get("/api/v1/backtests/test_id/portfolio/performance")
        assert response.status_code == 200
        
        data = response.json()
        assert len(data) == 1000

if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])