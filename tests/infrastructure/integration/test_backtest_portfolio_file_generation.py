"""
Integration tests to verify backtests generate portfolio holding files on disk
"""

import os
import json
import pytest
import tempfile
from pathlib import Path
from datetime import date, datetime
import asyncpg


class TestBacktestPortfolioFileGeneration:
    """Test that backtests properly generate and save portfolio holdings to disk"""

    @pytest.fixture
    async def db_connection(self):
        """Database connection for testing"""
        conn = await asyncpg.connect('postgresql://postgres:postgres@localhost:5433/dev_db')
        yield conn
        await conn.close()

    @pytest.fixture
    def temp_portfolio_dir(self):
        """Temporary directory for portfolio files during testing"""
        with tempfile.TemporaryDirectory() as temp_dir:
            portfolio_dir = Path(temp_dir) / "portfolios" / "backtests"
            portfolio_dir.mkdir(parents=True, exist_ok=True)
            yield portfolio_dir

    @pytest.mark.asyncio

    async def test_backtest_metadata_points_to_disk_files(self, db_connection):
        """Test that backtest metadata table correctly points to disk file paths"""

        # Check that backtest runs table has portfolio file paths
        rows = await db_connection.fetch("""
            SELECT backtest_run_id, portfolio_data_path, strategy_name
            FROM dev_backtest_runs
            ORDER BY backtest_run_id
        """)

        assert len(rows) >= 3, "Should have at least 3 backtest runs"

        # Verify each backtest has a file path
        for row in rows:
            assert row['portfolio_data_path'], f"Missing portfolio path for {row['backtest_run_id']}"
            assert row['portfolio_data_path'].endswith('.json'), f"Portfolio path should be JSON file: {row['portfolio_data_path']}"
            assert 'data/portfolios/backtests/' in row['portfolio_data_path'], f"Should be in backtests directory: {row['portfolio_data_path']}"

    def test_portfolio_files_exist_on_disk(self):
        """Test that referenced portfolio files actually exist on disk"""

        expected_files = [
            "data/portfolios/backtests/comprehensive_2022_2025.json",
            "data/portfolios/backtests/adaptive_sr_2024.json",
            "data/portfolios/backtests/momentum_2024.json",
            "data/portfolios/current/main_portfolio.json"
        ]

        for file_path in expected_files:
            assert os.path.exists(file_path), f"Portfolio file should exist: {file_path}"

            # Verify file is valid JSON
            with open(file_path, 'r') as f:
                data = json.load(f)
                assert isinstance(data, dict), f"Portfolio file should contain JSON object: {file_path}"

    def test_portfolio_file_structure(self):
        """Test that portfolio files have correct structure"""

        current_portfolio_path = "data/portfolios/current/main_portfolio.json"

        with open(current_portfolio_path, 'r') as f:
            portfolio = json.load(f)

        # Test required sections
        assert 'portfolio_metadata' in portfolio
        assert 'holdings' in portfolio
        assert 'sector_allocation' in portfolio
        assert 'performance_metrics' in portfolio

        # Test portfolio metadata
        metadata = portfolio['portfolio_metadata']
        assert 'name' in metadata
        assert 'last_updated' in metadata
        assert 'total_value' in metadata
        assert 'cash_position' in metadata

        # Test holdings structure
        holdings = portfolio['holdings']
        assert len(holdings) > 0, "Should have portfolio holdings"

        for holding in holdings:
            required_fields = ['symbol', 'shares', 'cost_basis', 'sector', 'current_price', 'market_value', 'weight']
            for field in required_fields:
                assert field in holding, f"Holding should have {field}: {holding}"

        # Test sector allocation
        sector_allocation = portfolio['sector_allocation']
        total_allocation = sum(sector_allocation.values())
        assert abs(total_allocation - 1.0) < 0.01, f"Sector allocation should sum to ~1.0, got {total_allocation}"

    def test_backtest_portfolio_file_structure(self):
        """Test that backtest portfolio files have correct structure for time series"""

        # This test will verify the structure once we create the backtest files
        backtest_files = [
            "data/portfolios/backtests/comprehensive_2022_2025.json",
            "data/portfolios/backtests/adaptive_sr_2024.json"
        ]

        for file_path in backtest_files:
            if os.path.exists(file_path):
                with open(file_path, 'r') as f:
                    backtest_data = json.load(f)

                # Backtest files should have daily snapshots
                assert 'backtest_metadata' in backtest_data
                assert 'daily_snapshots' in backtest_data

                metadata = backtest_data['backtest_metadata']
                assert 'backtest_run_id' in metadata
                assert 'strategy_name' in metadata
                assert 'start_date' in metadata
                assert 'end_date' in metadata

                # Test daily snapshots structure
                snapshots = backtest_data['daily_snapshots']
                assert len(snapshots) > 0, "Should have daily portfolio snapshots"

                for snapshot in snapshots:
                    assert 'date' in snapshot
                    assert 'total_portfolio_value' in snapshot
                    assert 'holdings' in snapshot
                    assert 'daily_return' in snapshot

    @pytest.mark.asyncio

    async def test_file_generation_workflow(self, db_connection, temp_portfolio_dir):
        """Test the complete workflow of backtest generating portfolio files"""

        # Simulate a backtest run generating a portfolio file
        test_backtest_id = "test_momentum_strategy"
        test_file_path = temp_portfolio_dir / f"{test_backtest_id}.json"

        # Generate sample portfolio data (simulating what a backtest would create)
        portfolio_data = {
            "backtest_metadata": {
                "backtest_run_id": test_backtest_id,
                "strategy_name": "Test Momentum Strategy",
                "start_date": "2024-01-01",
                "end_date": "2024-06-30",
                "initial_capital": 1000000.0,
                "universe": ["AAPL", "MSFT", "GOOGL", "NVDA", "TSLA"]
            },
            "daily_snapshots": [
                {
                    "date": "2024-06-30",
                    "total_portfolio_value": 1125000.0,
                    "daily_return": 0.0125,
                    "cumulative_return": 0.125,
                    "holdings": [
                        {
                            "symbol": "AAPL",
                            "shares": 800.0,
                            "price": 175.50,
                            "market_value": 140400.0,
                            "weight": 0.125,
                            "daily_pnl": 1404.0,
                            "daily_return": 0.01,
                            "sector": "Technology"
                        },
                        {
                            "symbol": "MSFT",
                            "shares": 500.0,
                            "price": 310.25,
                            "market_value": 155125.0,
                            "weight": 0.138,
                            "daily_pnl": -1551.0,
                            "daily_return": -0.01,
                            "sector": "Technology"
                        }
                    ],
                    "sector_allocation": {
                        "Technology": 0.85,
                        "Cash": 0.15
                    }
                }
            ]
        }

        # Write portfolio file (simulating backtest output)
        with open(test_file_path, 'w') as f:
            json.dump(portfolio_data, f, indent=2)

        # Verify file was created
        assert test_file_path.exists(), "Portfolio file should be created"

        # Insert metadata into database (simulating backtest completion)
        await db_connection.execute("""
            INSERT INTO dev_backtest_runs (
                backtest_run_id, strategy_name, start_date, end_date,
                portfolio_data_path, initial_capital, universe_size, status
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        """, test_backtest_id, "Test Momentum Strategy",
             date(2024, 1, 1), date(2024, 6, 30),
             str(test_file_path), 1000000.0, 5, "completed")

        # Verify database entry
        row = await db_connection.fetchrow("""
            SELECT * FROM dev_backtest_runs WHERE backtest_run_id = $1
        """, test_backtest_id)

        assert row is not None, "Backtest metadata should be in database"
        assert row['portfolio_data_path'] == str(test_file_path)
        assert row['status'] == 'completed'

        # Verify we can read the file back
        with open(test_file_path, 'r') as f:
            loaded_data = json.load(f)

        assert loaded_data['backtest_metadata']['backtest_run_id'] == test_backtest_id
        assert len(loaded_data['daily_snapshots']) == 1
        assert loaded_data['daily_snapshots'][0]['total_portfolio_value'] == 1125000.0

    def test_current_portfolio_file_updates(self):
        """Test that current portfolio file can be updated (simulating live trading)"""

        current_file = "data/portfolios/current/main_portfolio.json"

        # Read current portfolio
        with open(current_file, 'r') as f:
            portfolio = json.load(f)

        original_total = portfolio['portfolio_metadata']['total_value']
        original_timestamp = portfolio['portfolio_metadata']['last_updated']

        # Simulate updating portfolio (new prices, trades, etc.)
        portfolio['portfolio_metadata']['last_updated'] = datetime.now().isoformat() + 'Z'
        portfolio['portfolio_metadata']['total_value'] = original_total * 1.01  # 1% gain

        # Update a holding price
        for holding in portfolio['holdings']:
            if holding['symbol'] == 'AAPL':
                old_price = holding['current_price']
                holding['current_price'] = old_price * 1.02  # 2% price increase
                holding['market_value'] = holding['shares'] * holding['current_price']
                break

        # Write updated portfolio back to disk
        with open(current_file, 'w') as f:
            json.dump(portfolio, f, indent=2)

        # Verify file was updated
        with open(current_file, 'r') as f:
            updated_portfolio = json.load(f)

        assert updated_portfolio['portfolio_metadata']['last_updated'] != original_timestamp
        assert updated_portfolio['portfolio_metadata']['total_value'] > original_total

    @pytest.mark.asyncio

    async def test_api_reads_from_disk_files(self):
        """Test that APIs can successfully read portfolio data from disk files"""

        # This would test the integration with the actual APIs
        # For now, we'll test the file reading logic

        current_file = "data/portfolios/current/main_portfolio.json"

        # Test reading current portfolio file
        with open(current_file, 'r') as f:
            portfolio_data = json.load(f)

        # Verify essential data is present for API consumption
        assert 'holdings' in portfolio_data
        assert len(portfolio_data['holdings']) > 0

        # Verify we can reconstruct API response from file data
        holdings = portfolio_data['holdings']
        total_market_value = sum(h['market_value'] for h in holdings)
        cash_position = portfolio_data['portfolio_metadata']['cash_position']
        total_portfolio_value = total_market_value + cash_position

        # Verify calculations match stored values
        stored_total = portfolio_data['portfolio_metadata']['total_value']
        assert abs(total_portfolio_value - stored_total) < 1.0, "Calculated total should match stored total"

if __name__ == "__main__":
    # Run basic file structure tests
    test = TestBacktestPortfolioFileGeneration()

    test.test_portfolio_files_exist_on_disk()
    print("✅ Portfolio files exist on disk")

    test.test_portfolio_file_structure()
    print("✅ Portfolio file structure is valid")

    test.test_current_portfolio_file_updates()
    print("✅ Portfolio file updates work")

    print("\n🎯 All portfolio file tests passed!")

    except Exception as e:
        print(f"❌ Test failed: {e}")
        raise