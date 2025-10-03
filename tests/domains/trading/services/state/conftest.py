"""
Pytest configuration for Universe State Builder Golden Tests.

Provides test fixtures and configuration for comprehensive universe state testing.
"""

import pytest
import logging
import gin
from pathlib import Path

# Import the test database fixtures so they're available to all tests in this directory
from infrastructure.database.test_db_manager import unit_test_db_clean, unit_test_db

# Configure logging for tests
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

def pytest_addoption(parser):
    """Add command-line options for universe state testing."""
    parser.addoption(
        "--update-golden",
        action="store_true", 
        default=False,
        help="Update golden files with current test results"
    )
    parser.addoption(
        "--test-symbols",
        action="store",
        default="AAPL,TSLA",
        help="Comma-separated list of symbols to test"
    )
    parser.addoption(
        "--test-timeframes", 
        action="store",
        default="1m,5m,15m,1h",
        help="Comma-separated list of timeframes to test"
    )

@pytest.fixture
def update_golden(request):
    """Fixture to get the update_golden command-line option."""
    return request.config.getoption("--update-golden")

@pytest.fixture  
def test_symbols(request):
    """Fixture to get test symbols from command line."""
    symbols_str = request.config.getoption("--test-symbols")
    return [s.strip() for s in symbols_str.split(",")]

@pytest.fixture
def test_timeframes(request):
    """Fixture to get test timeframes from command line.""" 
    timeframes_str = request.config.getoption("--test-timeframes")
    return [tf.strip() for tf in timeframes_str.split(",")]

@pytest.fixture(autouse=True, scope="function")
def gin_test_setup(request):
    """Setup gin configuration for universe state tests."""
    # Clear any existing configuration first
    gin.clear_config()

    # Load training_data.gin configuration for realistic universe state testing
    project_root = Path(__file__).parent.parent.parent.parent.parent
    training_config_path = project_root / "config" / "training_data.gin"
    
    if training_config_path.exists():
        gin.parse_config_file(str(training_config_path))
        # Override environment type for testing
        gin.parse_config(['bind = ["env_type", "test"]'])
    else:
        # Fallback gin configuration for universe state testing
        gin.parse_config([
            "domains.trading.services.state.universe_state_builder.UniverseStateIntervalBuilder.base_duration = '1m'",
            "domains.trading.services.state.universe_state_builder.UniverseStateIntervalBuilder.target_durations = '1m,5m,15m,1h'",
            "# Universe state test configuration using training_data.gin format",
            "# Use test database and minimal dependencies"
        ])
    
    yield
    
    # Clean up after test
    gin.clear_config()

@pytest.fixture(scope="session")
def test_data_dir():
    """Fixture providing path to test data directory."""
    return Path(__file__).parent / "test_data"

@pytest.fixture(scope="session") 
def golden_files_dir():
    """Fixture providing path to golden files directory."""
    return Path(__file__).parent / "golden_files"