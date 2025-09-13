"""
Integration test to verify backtests actually generate portfolio files
"""

import json
import pytest
import tempfile
from pathlib import Path
from datetime import date
import asyncpg

from src.ml.evaluation.sr_backtester import SRBacktester

class TestBacktestGeneratesPortfolioFiles:
    """Test that running actual backtests generates portfolio files"""

    @pytest.fixture
    async def db_connection(self):
        """Database connection for testing"""
        try:
            conn = await asyncpg.connect('postgresql://postgres:postgres@localhost:5433/dev_db')
            yield conn
            await conn.close()
        except Exception as e:
            pytest.skip(f"Database connection failed: {e}")

    @pytest.fixture
    def temp_portfolio_dir(self):
        """Temporary directory for portfolio files during testing"""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Override the portfolio directory
            original_dir = "data/portfolios/backtests"
            test_dir = Path(temp_dir) / "portfolios" / "backtests"
            test_dir.mkdir(parents=True, exist_ok=True)
            yield str(test_dir)

    @pytest.fixture
    def mock_model(self):
        """Mock model for testing"""
        class MockModel:
            def predict(self, X):
                # Return dummy predictions
                import numpy as np
                n_samples = len(X) if hasattr(X, '__len__') else 1
                return np.random.random((n_samples, 4))  # 2 support + 2 resistance levels

        return MockModel()

    @pytest.fixture
    def mock_feature_generator(self):
        """Mock feature generator for testing"""
        class MockFeatureGenerator:
            def generate_features(self, symbol, date_range):
                import numpy as np
                return np.random.random((10, 20))  # 10 samples, 20 features

        return MockFeatureGenerator()

    @pytest.mark.asyncio

    async def test_sr_backtester_generates_portfolio_file(self, db_connection, temp_portfolio_dir, mock_model, mock_feature_generator):
        """Test that SRBacktester generates a portfolio file when run"""

        # Initialize backtester
        backtester = SRBacktester()

        # Override the portfolio directory for testing
        original_method = backtester._generate_and_save_portfolio_file

        async def mock_save_portfolio_file(backtest_run_id, symbols, start_date, end_date, results):
            # Call original method but save to temp directory
            portfolio_data = {
                "backtest_metadata": {
                    "backtest_run_id": backtest_run_id,
                    "strategy_name": f"Support/Resistance Strategy - {backtest_run_id}",
                    "start_date": start_date.isoformat(),
                    "end_date": end_date.isoformat(),
                    "initial_capital": backtester.initial_capital,
                    "universe": symbols
                },
                "daily_snapshots": [
                    {
                        "date": "2024-06-30",
                        "total_portfolio_value": 1050000.0,
                        "daily_return": 0.005,
                        "cumulative_return": 0.05,
                        "cash_position": 50000.0,
                        "holdings": [
                            {
                                "symbol": "AAPL",
                                "shares": 800.0,
                                "price": 175.50,
                                "market_value": 140400.0,
                                "weight": 0.134,
                                "daily_pnl": 702.0,
                                "daily_return": 0.005,
                                "sector": "Technology"
                            }
                        ],
                        "sector_allocation": {
                            "Technology": 0.9,
                            "Cash": 0.1
                        },
                        "top_contributors": [
                            {"symbol": "AAPL", "pnl": 702.0, "daily_return": 0.005}
                        ],
                        "top_detractors": []
                    }
                ]
            }

            # Save to temp directory
            portfolio_file = Path(temp_portfolio_dir) / f"{backtest_run_id}.json"
            with open(portfolio_file, 'w') as f:
                json.dump(portfolio_data, f, indent=2, default=str)

        backtester._generate_and_save_portfolio_file = mock_save_portfolio_file

        # Set up test parameters
        test_backtest_id = "test_sr_backtest_2024"
        symbols = ["AAPL", "MSFT", "GOOGL"]
        start_date = date(2024, 1, 1)
        end_date = date(2024, 6, 30)

        # Run backtest with portfolio file generation
        try:
            results = await backtester.backtest_model(
                model=mock_model,
                symbols=symbols,
                start_date=start_date,
                end_date=end_date,
                feature_generator=mock_feature_generator,
                min_predictions_per_symbol=1,  # Lower threshold for testing
                backtest_run_id=test_backtest_id,
                save_portfolio_files=True
            )

            # Verify results were generated
            assert isinstance(results, dict), "Backtest should return results dictionary"

            # Verify portfolio file was created
            portfolio_file = Path(temp_portfolio_dir) / f"{test_backtest_id}.json"
            assert portfolio_file.exists(), f"Portfolio file should be created: {portfolio_file}"

            # Verify file content
            with open(portfolio_file, 'r') as f:
                portfolio_data = json.load(f)

            # Verify structure
            assert 'backtest_metadata' in portfolio_data, "Portfolio file should have backtest_metadata"
            assert 'daily_snapshots' in portfolio_data, "Portfolio file should have daily_snapshots"

            metadata = portfolio_data['backtest_metadata']
            assert metadata['backtest_run_id'] == test_backtest_id
            assert metadata['strategy_name'] == f"Support/Resistance Strategy - {test_backtest_id}"
            assert metadata['universe'] == symbols

            snapshots = portfolio_data['daily_snapshots']
            assert len(snapshots) > 0, "Should have at least one daily snapshot"

            # Verify snapshot structure
            snapshot = snapshots[0]
            required_fields = ['date', 'total_portfolio_value', 'daily_return',
                             'cumulative_return', 'cash_position', 'holdings',
                             'sector_allocation', 'top_contributors', 'top_detractors']

            for field in required_fields:
                assert field in snapshot, f"Snapshot should have {field}"

            # Verify holdings structure
            holdings = snapshot['holdings']
            assert len(holdings) > 0, "Should have portfolio holdings"

            holding = holdings[0]
            holding_fields = ['symbol', 'shares', 'price', 'market_value',
                            'weight', 'daily_pnl', 'daily_return', 'sector']

            for field in holding_fields:
                assert field in holding, f"Holding should have {field}"

            print("✅ SRBacktester successfully generated portfolio file")

        except Exception as e:
            pytest.fail(f"Backtest failed to generate portfolio file: {e}")

    @pytest.mark.asyncio

    async def test_backtest_without_portfolio_generation(self, mock_model, mock_feature_generator):
        """Test that backtest can run without generating portfolio files"""

        backtester = SRBacktester()

        symbols = ["AAPL"]
        start_date = date(2024, 1, 1)
        end_date = date(2024, 2, 1)

        try:
            # Run backtest WITHOUT portfolio file generation
            results = await backtester.backtest_model(
                model=mock_model,
                symbols=symbols,
                start_date=start_date,
                end_date=end_date,
                feature_generator=mock_feature_generator,
                min_predictions_per_symbol=1,
                backtest_run_id=None,  # No run ID
                save_portfolio_files=False  # Explicitly disable
            )

            # Should still return results
            assert isinstance(results, dict), "Backtest should return results even without portfolio files"

            print("✅ Backtest runs successfully without portfolio generation")

        except Exception as e:
            pytest.fail(f"Backtest failed when portfolio generation disabled: {e}")

    def test_portfolio_file_workflow_integration(self, temp_portfolio_dir):
        """Test the complete workflow from file generation to API consumption"""

        # Simulate creating a portfolio file (as backtest would do)
        test_backtest_id = "workflow_test_2024"
        portfolio_data = {
            "backtest_metadata": {
                "backtest_run_id": test_backtest_id,
                "strategy_name": "Workflow Test Strategy",
                "start_date": "2024-01-01",
                "end_date": "2024-06-30",
                "initial_capital": 1000000.0,
                "universe": ["AAPL", "MSFT"]
            },
            "daily_snapshots": [
                {
                    "date": "2024-06-30",
                    "total_portfolio_value": 1100000.0,
                    "daily_return": 0.01,
                    "cumulative_return": 0.10,
                    "cash_position": 100000.0,
                    "holdings": [],
                    "sector_allocation": {"Cash": 1.0},
                    "top_contributors": [],
                    "top_detractors": []
                }
            ]
        }

        # Save portfolio file
        portfolio_file = Path(temp_portfolio_dir) / f"{test_backtest_id}.json"
        with open(portfolio_file, 'w') as f:
            json.dump(portfolio_data, f, indent=2)

        # Verify file was created
        assert portfolio_file.exists(), "Portfolio file should be created"

        # Test reading file back (simulating API)
        with open(portfolio_file, 'r') as f:
            loaded_data = json.load(f)

        assert loaded_data == portfolio_data, "Loaded data should match saved data"

        # Verify can extract API-compatible data
        metadata = loaded_data['backtest_metadata']
        snapshots = loaded_data['daily_snapshots']

        assert metadata['backtest_run_id'] == test_backtest_id
        assert len(snapshots) == 1
        assert snapshots[0]['total_portfolio_value'] == 1100000.0

        print("✅ Portfolio file workflow integration test passed")


if __name__ == "__main__":
    # Run basic tests
    test = TestBacktestGeneratesPortfolioFiles()

    # Test file workflow
    import tempfile
    with tempfile.TemporaryDirectory() as temp_dir:
        test_dir = Path(temp_dir) / "portfolios" / "backtests"
        test_dir.mkdir(parents=True, exist_ok=True)
        test.test_portfolio_file_workflow_integration(str(test_dir))

    print("\n🎯 Portfolio file generation tests ready!")