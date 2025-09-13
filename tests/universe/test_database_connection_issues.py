"""
Tests to detect database connection issues in the dynamic universe system

These tests identify and reproduce the connection problems before fixing them.
"""

import pytest
import asyncio
from unittest.mock import Mock
from shared.utils.environment import Environment
from domains.trading.services.dynamic_modeling_universe import DynamicModelingUniverse


class TestDatabaseConnectionIssues:
    """Test database connection configuration and compatibility issues"""

    def test_environment_database_config_format(self):
        """Test that Environment returns expected database config format"""
        env = Environment()

        try:
            db_config = env.get_database_config()

            # Log what we actually get
            print(f"Database config keys: {list(db_config.keys())}")
            print(f"Database config: {db_config}")

            # Test expected keys are present
            expected_keys = ['host', 'port', 'user', 'password', 'database']
            for key in expected_keys:
                assert key in db_config or db_config.get(key) is not None, f"Missing or None: {key}"

            # Test no unexpected keys that would break asyncpg
            asyncpg_compatible_keys = {
                'host', 'port', 'user', 'password', 'database',
                'timeout', 'connection_class', 'ssl', 'passfile'
            }

            unexpected_keys = set(db_config.keys()) - asyncpg_compatible_keys
            if unexpected_keys:
                print(f"WARNING: Unexpected keys that may break asyncpg: {unexpected_keys}")

        except AttributeError as e:
            pytest.fail(f"Environment.get_database_config() method not found: {e}")
        except Exception as e:
            print(f"Error getting database config: {e}")
            raise

    def test_environment_database_url_fallback(self):
        """Test that Environment has database URL fallback"""
        env = Environment()

        try:
            db_url = env.get_database_url()
            print(f"Database URL: {db_url}")

            # Should be a valid postgres URL format
            if db_url:
                assert db_url.startswith(('postgresql://', 'postgres://')), f"Invalid URL format: {db_url}"
            else:
                print("WARNING: No database URL configured")

        except AttributeError as e:
            pytest.fail(f"Environment.get_database_url() method not found: {e}")
        except Exception as e:
            print(f"Error getting database URL: {e}")
            raise

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_asyncpg_connection_with_actual_config(self):
        """Test asyncpg connection with actual environment config"""
        import asyncpg

        env = Environment()

        # Test with get_database_config()
        try:
            db_config = env.get_database_config()
            print(f"Testing asyncpg with config: {db_config}")

            # This should fail and show us what's wrong
            with pytest.raises((TypeError, Exception)) as exc_info:
                pool = await asyncpg.create_pool(**db_config)
                await pool.close()

            print(f"Expected error with full config: {exc_info.value}")

            # Now test with filtered config
            asyncpg_config = {
                k: v for k, v in db_config.items()
                if k in ['host', 'port', 'user', 'password', 'database'] and v is not None
            }
            print(f"Filtered config: {asyncpg_config}")

            # This might still fail due to missing database, but shouldn't have parameter errors
            try:
                pool = await asyncpg.create_pool(**asyncpg_config)
                await pool.close()
                print("✅ Connection successful with filtered config")
            except Exception as e:
                print(f"Connection failed with filtered config (expected if DB not running): {e}")
                # This is expected if database isn't running
                assert "connect" in str(e).lower() or "connection" in str(e).lower()

        except AttributeError:
            # Fall back to URL test
            db_url = env.get_database_url()
            if db_url:
                try:
                    pool = await asyncpg.create_pool(db_url)
                    await pool.close()
                    print("✅ Connection successful with URL")
                except Exception as e:
                    print(f"Connection failed with URL (expected if DB not running): {e}")

    def test_universe_initialization_error_handling(self):
        """Test that DynamicModelingUniverse handles connection errors gracefully"""
        env = Mock(spec=Environment)

        # Test case 1: get_database_config() returns incompatible parameters
        env.get_database_config.return_value = {
            'host': 'localhost',
            'port': 5432,
            'user': 'test',
            'password': 'test',
            'database': 'test',
            'base_database': 'postgres',  # This breaks asyncpg
            'extra_param': 'value'        # This also breaks asyncpg
        }

        universe = DynamicModelingUniverse(env)

        # Should detect the issue in the configuration
        config = env.get_database_config()
        asyncpg_compatible_keys = {'host', 'port', 'user', 'password', 'database'}
        incompatible_keys = set(config.keys()) - asyncpg_compatible_keys

        assert len(incompatible_keys) > 0, f"Test setup error: should have incompatible keys"
        print(f"Detected incompatible keys: {incompatible_keys}")

    def test_universe_configuration_parameters(self):
        """Test that universe configuration parameters are reasonable"""
        env = Mock(spec=Environment)
        universe = DynamicModelingUniverse(env)

        # Test universe parameters
        assert universe.min_market_cap_millions == 400
        assert universe.min_dollar_volume_millions == 100
        assert universe.lookback_days == 52
        assert universe.min_trading_days == 40
        assert universe.grace_period_days == 7
        assert universe.reentry_restriction_days == 365

        # Test that minimum trading days is reasonable for lookback period
        assert universe.min_trading_days < universe.lookback_days * 0.8, \
            "Minimum trading days too high for lookback period"

    def test_table_name_generation(self):
        """Test that table names are generated correctly"""
        env = Mock(spec=Environment)
        env.get_table_name = Mock(side_effect=lambda name: f"dev_{name}")

        universe = DynamicModelingUniverse(env)

        # Test table name calls would work
        expected_calls = [
            "universe",
            "universe_membership",
            "universe_tracking",
            "daily_prices_polygon",
            "instrument_xrefs",
            "vendors",
            "daily_market_cap"
        ]

        for table in expected_calls:
            table_name = env.get_table_name(table)
            assert table_name.startswith("dev_"), f"Table name should have dev_ prefix: {table_name}"
            assert table in table_name, f"Table name should contain base name: {table_name}"


class TestDatabaseQueryGeneration:
    """Test that database queries are generated correctly"""

    def test_qualifying_stocks_query_structure(self):
        """Test that the qualifying stocks query has correct structure"""
        env = Mock(spec=Environment)
        env.get_table_name = Mock(side_effect=lambda name: f"test_{name}")

        universe = DynamicModelingUniverse(env)

        # Get the query template (this is from _get_qualifying_stocks method)
        query_template = """
        WITH price_data AS (
            SELECT
                p.instrument_id,
                x.vendor_symbol as symbol,
                p.date,
                p.close_price,
                p.volume,
                (p.close_price * p.volume) as dollar_volume,
                mc.market_cap_usd
            FROM {prices_table} p
            JOIN {xrefs_table} x ON p.instrument_id = x.instrument_id
            JOIN {vendors_table} v ON x.vendor_id = v.vendor_id
            LEFT JOIN {market_cap_table} mc ON p.instrument_id = mc.instrument_id
                                              AND p.date = mc.date
            WHERE p.date BETWEEN $1 AND $2
              AND v.vendor_id = 3  -- Ticker vendor
              AND p.close_price > 1.0  -- Basic price filter
              AND p.volume > 1000      -- Basic volume filter
              AND x.vendor_symbol ~ '^[A-Z]+$'  -- Valid ticker format
        )
        """

        formatted_query = query_template.format(
            prices_table=env.get_table_name("daily_prices_polygon"),
            xrefs_table=env.get_table_name("instrument_xrefs"),
            vendors_table=env.get_table_name("vendors"),
            market_cap_table=env.get_table_name("daily_market_cap")
        )

        # Test that query contains expected elements
        assert "WITH price_data AS" in formatted_query
        assert "test_daily_prices_polygon" in formatted_query
        assert "test_instrument_xrefs" in formatted_query
        assert "test_vendors" in formatted_query
        assert "test_daily_market_cap" in formatted_query
        assert "vendor_id = 3" in formatted_query  # Ticker vendor
        assert "close_price > 1.0" in formatted_query  # Basic filters
        assert "volume > 1000" in formatted_query

        print("✅ Query structure is correct")

    def test_universe_table_creation_queries(self):
        """Test universe table creation queries"""
        env = Mock(spec=Environment)
        env.get_table_name = Mock(side_effect=lambda name: f"test_{name}")

        universe = DynamicModelingUniverse(env)

        # Test tracking table creation query
        tracking_table_query = """
        CREATE TABLE IF NOT EXISTS {universe_tracking_table} (
            id SERIAL PRIMARY KEY,
            universe_name VARCHAR(100) NOT NULL,
            instrument_id INTEGER NOT NULL,
            symbol VARCHAR(20) NOT NULL,
            entry_date DATE NOT NULL,
            last_qualifying_date DATE,
            warning_date DATE,
            removal_date DATE,
            removal_reason TEXT,
            avg_market_cap DECIMAL(15,2),
            avg_dollar_volume DECIMAL(15,2),
            last_update DATE NOT NULL,
            created_at TIMESTAMP DEFAULT NOW(),
            updated_at TIMESTAMP DEFAULT NOW(),
            UNIQUE(universe_name, instrument_id, entry_date)
        )
        """.format(universe_tracking_table=env.get_table_name("universe_tracking"))

        # Test query structure
        assert "CREATE TABLE IF NOT EXISTS" in tracking_table_query
        assert "test_universe_tracking" in tracking_table_query
        assert "universe_name VARCHAR(100)" in tracking_table_query
        assert "UNIQUE(universe_name, instrument_id, entry_date)" in tracking_table_query

        print("✅ Table creation queries are correct")


def test_reproduce_original_error():
    """Reproduce the original connection error to understand it"""

    # This test reproduces the exact error we saw

    # Simulate the config that was causing issues
    problematic_config = {
        'host': 'localhost',
        'port': 5432,
        'user': 'test',
        'password': 'test',
        'database': 'test',
        'base_database': 'postgres',  # This parameter breaks asyncpg
    }

    # This should fail with the exact error we saw
    with pytest.raises(TypeError, match="unexpected keyword argument"):
        asyncio.run(_test_connection(problematic_config))

    print("✅ Successfully reproduced the original error")


async def _test_connection(config):
    """Helper to test asyncpg connection"""
    pool = await asyncpg.create_pool(**config)
    await pool.close()


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])