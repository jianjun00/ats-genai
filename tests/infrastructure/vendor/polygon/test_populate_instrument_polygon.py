import os
import sys
import unittest
import tempfile
from unittest.mock import patch, MagicMock
import asyncio

# Add src to PYTHONPATH
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../src')))

from vendor.polygon.services.populate_instrument_polygon import parse_date, fetch_and_store_instruments


class TestPopulateInstrumentPolygon(unittest.TestCase):

    def test_parse_date(self):
        """Test the parse_date function"""
        self.assertEqual(parse_date("2022-01-15T00:00:00.000Z").isoformat(), "2022-01-15")
        self.assertEqual(parse_date("2022-01-15"), "2022-01-15")
        self.assertIsNone(parse_date(None))
        self.assertIsNone(parse_date("invalid-date"))

    @patch('secmaster.populate_instrument_polygon.requests.get')
    def test_fetch_single_ticker(self, mock_get):
        """Test fetching a single ticker works with the test environment"""
        # Create a temporary gin config file
        with tempfile.NamedTemporaryFile(mode='w+', suffix='.gin') as temp_gin:
            temp_gin.write("""
            # Test Gin config
            polygon_api_key = 'test_polygon_key'
            Database.host = 'localhost'
            Database.port = 5432
            Database.user = 'postgres'
            Database.password = 'password'
            Database.database = 'test_db'
            """)
            temp_gin.flush()

            # Mock the HTTP response for the ticker detail
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.json.return_value = {
                "results": {
                    "ticker": "AAPL",
                    "name": "Apple Inc.",
                    "primary_exchange": "NASDAQ",
                    "type": "CS",
                    "currency_name": "usd",
                    "share_class_figi": "BBG001S5N8V8",
                    "list_date": "1980-12-12"
                }
            }
            mock_get.return_value = mock_response

            # Mock the database pool and connection
            mock_pool = MagicMock()
            mock_conn = MagicMock()
            mock_pool.acquire.return_value.__aenter__.return_value = mock_conn

            # Test the function
            with patch('secmaster.populate_instrument_polygon.asyncpg.create_pool',
                      return_value=asyncio.Future()) as mock_create_pool:
                mock_create_pool.return_value.set_result(mock_pool)

                # Run with the test environment
                with patch('secmaster.populate_instrument_polygon.Environment') as mock_env:
                    mock_env_instance = mock_env.return_value
                    mock_env_instance.get_database_url.return_value = "postgres://postgres:password@localhost:5432/test_db"
                    mock_env_instance.get_table_name.return_value = "instrument_polygon"

                    # Run the test
                    with patch('secmaster.populate_instrument_polygon.POLYGON_API_KEY', 'test_polygon_key'):
                        asyncio.run(fetch_and_store_instruments(ticker="AAPL"))

                        # Verify the API was called correctly
                        mock_get.assert_called_once()
                        self.assertIn("AAPL", mock_get.call_args[0][0])
                        self.assertIn("test_polygon_key", mock_get.call_args[0][0])

                        # Verify the database was called
                        mock_conn.execute.assert_called_once()
                        self.assertIn("instrument_polygon", mock_conn.execute.call_args[0][0])
                        self.assertEqual(mock_conn.execute.call_args[0][1], "AAPL")

    def test_command_line_environment_param(self):
        """Test that the command line environment parameter works correctly"""
        with patch('sys.argv', ['populate_instrument_polygon.py', '--environment', 'dev', '--ticker', 'AAPL']):
            with patch('secmaster.populate_instrument_polygon.asyncio.run') as mock_run:
                with patch('secmaster.populate_instrument_polygon.gin.parse_config_file') as mock_parse:
                    with patch('secmaster.populate_instrument_polygon.os.path.exists', return_value=True):
                        # Import the module to trigger the __main__ code
                        with patch.dict('sys.modules', {'secmaster.populate_instrument_polygon': MagicMock()}):
                            import importlib
                            with patch('secmaster.populate_instrument_polygon.Environment'):
                                with patch('secmaster.populate_instrument_polygon.set_polygon_api_key'):
                                    try:
                                        # This will run the __main__ block
                                        import secmaster.populate_instrument_polygon
                                        importlib.reload(secmaster.populate_instrument_polygon)
                                    except SystemExit:
                                        pass

                                    # Verify the correct config file was used
                                    mock_parse.assert_called_once()
                                    self.assertIn('config/app_docker.gin', mock_parse.call_args[0][0])


if __name__ == '__main__':
    unittest.main()
