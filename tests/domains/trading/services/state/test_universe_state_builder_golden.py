"""
Comprehensive Universe State Builder Golden File Tests

TESTING STRATEGY:
1. **Real Data Integration**: Use actual FirstRate minute bars + real business logic
2. **Golden File Validation**: Deterministic regression testing for complex business objects
3. **Comprehensive Coverage**: Single/multi symbols, all timeframes, rolling windows
4. **Business Logic Validation**: OHLCV aggregation + indicators + state management

ARCHITECTURE:
- Extends generalized golden file framework for business objects
- Uses real UniverseStateBuilder with full dependency chain
- Validates complex nested data structures (UniverseStateInterval)
- Supports both individual and collection testing patterns

USAGE:
  python -m pytest test_universe_state_builder_golden.py -v --update-golden
"""

import pytest
import asyncio
import logging
from pathlib import Path
from datetime import datetime
from typing import Dict, List

# Import testing framework
from testing_framework.golden_files import (
    UniverseStateGoldenTestCase,
    UniverseStateCollectionGoldenManager
)
from testing_framework.universe_state import (
    UniverseStateRealDataTestSuite,
    UniverseStateTestScenario
)

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Test configuration
TEST_DIR = Path(__file__).parent
GOLDEN_FILES_DIR = TEST_DIR / "golden_files"


class TestUniverseStateBuilderGolden:
    """Comprehensive golden file tests for UniverseStateBuilder."""
    
    @pytest.fixture(scope="class")
    def test_suite(self):
        """Set up test suite with real data integration."""
        return UniverseStateRealDataTestSuite(TEST_DIR)
    
    @pytest.fixture(scope="class")
    def golden_test_case(self):
        """Set up golden file test case."""
        return UniverseStateGoldenTestCase(TEST_DIR)
    
    @pytest.fixture(scope="class")
    def collection_manager(self):
        """Set up collection manager for multi-state testing."""
        return UniverseStateCollectionGoldenManager(TEST_DIR)
    
    @pytest.fixture
    def update_golden(self, request):
        """Get update golden flag from command line."""
        return request.config.getoption("--update-golden", default=False)
    
    # === SINGLE SYMBOL TESTS ===
    
    @pytest.mark.asyncio
    async def test_single_symbol_1m_market_open_golden(self, test_suite, golden_test_case, update_golden):
        """Test single symbol 1-minute intervals during market open."""
        scenario = UniverseStateTestScenario(
            scenario_name="single_aapl_1m_market_open",
            symbols=["AAPL"],
            timeframe="1m",
            start_time=datetime(2024, 8, 1, 9, 30),
            end_time=datetime(2024, 8, 1, 10, 0),
            test_type="single_symbol_market_open"
        )
        
        universe_state = await scenario.execute_scenario(test_suite.test_data_manager)
        
        success = golden_test_case.run_universe_state_golden_test(
            test_method_name="test_single_symbol_1m_market_open_golden",
            universe_state=universe_state,
            symbols=scenario.symbols,
            timeframe=scenario.timeframe,
            start_time=scenario.start_time,
            end_time=scenario.end_time,
            update_golden=update_golden
        )
        
        assert success, "Golden file test failed for single symbol 1m market open"
    
    @pytest.mark.asyncio
    async def test_single_symbol_5m_aggregation_golden(self, test_suite, golden_test_case, update_golden):
        """Test single symbol 5-minute aggregation from 1-minute base data."""
        scenario = UniverseStateTestScenario(
            scenario_name="single_aapl_5m_aggregation",
            symbols=["AAPL"],
            timeframe="5m",
            start_time=datetime(2024, 8, 1, 10, 30),
            end_time=datetime(2024, 8, 1, 11, 30),
            test_type="timeframe_aggregation"
        )
        
        universe_state = await scenario.execute_scenario(test_suite.test_data_manager)
        
        success = golden_test_case.run_universe_state_golden_test(
            test_method_name="test_single_symbol_5m_aggregation_golden",
            universe_state=universe_state,
            symbols=scenario.symbols,
            timeframe=scenario.timeframe,
            start_time=scenario.start_time,
            end_time=scenario.end_time,
            update_golden=update_golden
        )
        
        assert success, "Golden file test failed for single symbol 5m aggregation"
    
    @pytest.mark.asyncio
    async def test_single_symbol_15m_hour_block_golden(self, test_suite, golden_test_case, update_golden):
        """Test single symbol 15-minute intervals over 1-hour block."""
        scenario = UniverseStateTestScenario(
            scenario_name="single_aapl_15m_hour_block",
            symbols=["AAPL"],
            timeframe="15m",
            start_time=datetime(2024, 8, 1, 14, 0),
            end_time=datetime(2024, 8, 1, 15, 0),
            test_type="extended_timeframe"
        )
        
        universe_state = await scenario.execute_scenario(test_suite.test_data_manager)
        
        success = golden_test_case.run_universe_state_golden_test(
            test_method_name="test_single_symbol_15m_hour_block_golden",
            universe_state=universe_state,
            symbols=scenario.symbols,
            timeframe=scenario.timeframe,
            start_time=scenario.start_time,
            end_time=scenario.end_time,
            update_golden=update_golden
        )
        
        assert success, "Golden file test failed for single symbol 15m hour block"
    
    # === MULTI-SYMBOL TESTS ===
    
    @pytest.mark.asyncio
    async def test_multi_symbol_5m_universe_golden(self, test_suite, golden_test_case, update_golden):
        """Test multi-symbol universe state building with 5-minute intervals."""
        scenario = UniverseStateTestScenario(
            scenario_name="multi_symbol_5m_universe",
            symbols=["AAPL", "TSLA"],
            timeframe="5m",
            start_time=datetime(2024, 8, 1, 12, 0),
            end_time=datetime(2024, 8, 1, 13, 0),
            test_type="multi_symbol_universe"
        )
        
        universe_state = await scenario.execute_scenario(test_suite.test_data_manager)
        
        success = golden_test_case.run_universe_state_golden_test(
            test_method_name="test_multi_symbol_5m_universe_golden",
            universe_state=universe_state,
            symbols=scenario.symbols,
            timeframe=scenario.timeframe,
            start_time=scenario.start_time,
            end_time=scenario.end_time,
            update_golden=update_golden
        )
        
        assert success, "Golden file test failed for multi-symbol 5m universe"
    
    # === TIMEFRAME MATRIX TESTS ===
    
    @pytest.mark.asyncio
    async def test_timeframe_consistency_matrix_golden(self, test_suite, golden_test_case, update_golden):
        """Test consistency across multiple timeframes for same time period."""
        base_start = datetime(2024, 8, 1, 10, 0)
        base_end = datetime(2024, 8, 1, 11, 0)
        timeframes = ["1m", "5m", "15m"]
        
        matrix_results = {}
        
        for timeframe in timeframes:
            scenario = UniverseStateTestScenario(
                scenario_name=f"matrix_consistency_{timeframe}",
                symbols=["AAPL"],
                timeframe=timeframe,
                start_time=base_start,
                end_time=base_end,
                test_type="timeframe_matrix"
            )
            
            universe_state = await scenario.execute_scenario(test_suite.test_data_manager)
            matrix_results[timeframe] = universe_state
        
        # Create combined result for golden file
        # Use the 5m result as primary, but include metadata about all timeframes
        primary_state = matrix_results["5m"]
        
        success = golden_test_case.run_universe_state_golden_test(
            test_method_name="test_timeframe_consistency_matrix_golden",
            universe_state=primary_state,
            symbols=["AAPL"],
            timeframe="matrix_test",
            start_time=base_start,
            end_time=base_end,
            update_golden=update_golden,
            tested_timeframes=timeframes,
            matrix_results_summary={
                tf: {
                    "instrument_count": len(state.instrument_intervals),
                    "duration_string": state.duration.get_duration_string()
                }
                for tf, state in matrix_results.items()
            }
        )
        
        assert success, "Golden file test failed for timeframe consistency matrix"
    
    # === ROLLING WINDOW TESTS ===
    
    @pytest.mark.asyncio 
    async def test_rolling_window_sequence_golden(self, test_suite, collection_manager, update_golden):
        """Test rolling window behavior across consecutive intervals."""
        base_time = datetime(2024, 8, 1, 11, 0)
        rolling_states = []
        
        # Create sequence of 5-minute intervals
        for i in range(4):  # 4 consecutive 5-minute intervals
            start_time = base_time + timedelta(minutes=5*i)
            end_time = start_time + timedelta(minutes=5)
            
            scenario = UniverseStateTestScenario(
                scenario_name=f"rolling_window_interval_{i}",
                symbols=["AAPL"],
                timeframe="5m", 
                start_time=start_time,
                end_time=end_time,
                test_type="rolling_window",
                interval_index=i
            )
            
            universe_state = await scenario.execute_scenario(test_suite.test_data_manager)
            rolling_states.append(universe_state)
        
        # Test with collection manager
        if update_golden:
            collection_manager.save_collection_golden_file(
                test_method_name="test_rolling_window_sequence_golden",
                universe_states=rolling_states,
                collection_metadata={
                    "test_type": "rolling_window_sequence",
                    "symbol": "AAPL",
                    "timeframe": "5m",
                    "interval_count": len(rolling_states),
                    "base_time": base_time.isoformat()
                }
            )
            logger.info("🔄 Updated rolling window collection golden file")
        else:
            differences = collection_manager.load_and_compare_collection(
                test_method_name="test_rolling_window_sequence_golden",
                actual_states=rolling_states
            )
            
            if differences:
                logger.error("❌ Rolling window collection test failed:")
                for diff in differences[:10]:
                    logger.error(f"   - {diff}")
                assert False, f"Rolling window collection test failed with {len(differences)} differences"
            else:
                logger.info("✅ Rolling window collection test passed")
    
    # === EDGE CASE TESTS ===
    
    @pytest.mark.asyncio
    async def test_market_close_edge_case_golden(self, test_suite, golden_test_case, update_golden):
        """Test edge case behavior during market close."""
        scenario = UniverseStateTestScenario(
            scenario_name="market_close_edge_case",
            symbols=["AAPL"],
            timeframe="1m",
            start_time=datetime(2024, 8, 1, 15, 55),
            end_time=datetime(2024, 8, 1, 16, 0),
            test_type="edge_case_market_close"
        )
        
        universe_state = await scenario.execute_scenario(test_suite.test_data_manager)
        
        success = golden_test_case.run_universe_state_golden_test(
            test_method_name="test_market_close_edge_case_golden",
            universe_state=universe_state,
            symbols=scenario.symbols,
            timeframe=scenario.timeframe,
            start_time=scenario.start_time,
            end_time=scenario.end_time,
            update_golden=update_golden
        )
        
        assert success, "Golden file test failed for market close edge case"
    
    # === COMPREHENSIVE INTEGRATION TEST ===
    
    @pytest.mark.asyncio
    async def test_comprehensive_universe_state_integration_golden(self, test_suite, golden_test_case, update_golden):
        """Comprehensive integration test covering multiple dimensions."""
        scenario = UniverseStateTestScenario(
            scenario_name="comprehensive_integration",
            symbols=["AAPL", "TSLA"],
            timeframe="5m",
            start_time=datetime(2024, 8, 1, 13, 30),
            end_time=datetime(2024, 8, 1, 14, 30),
            test_type="comprehensive_integration"
        )
        
        universe_state = await scenario.execute_scenario(test_suite.test_data_manager)
        
        # Validate comprehensive business logic
        assert len(universe_state.instrument_intervals) == 2, "Should have 2 instruments"
        assert universe_state.duration.get_duration_string() == "5m", "Should be 5-minute timeframe"
        
        # Validate OHLCV data integrity
        for inst_id, interval in universe_state.instrument_intervals.items():
            assert interval.open > 0, f"Invalid open price for instrument {inst_id}"
            assert interval.high >= interval.open, f"High < open for instrument {inst_id}"
            assert interval.low <= interval.close, f"Low > close for instrument {inst_id}"
            assert interval.traded_volume >= 0, f"Invalid volume for instrument {inst_id}"
        
        success = golden_test_case.run_universe_state_golden_test(
            test_method_name="test_comprehensive_universe_state_integration_golden",
            universe_state=universe_state,
            symbols=scenario.symbols,
            timeframe=scenario.timeframe,
            start_time=scenario.start_time,
            end_time=scenario.end_time,
            update_golden=update_golden,
            validation_checks={
                "ohlcv_integrity": True,
                "multi_symbol": True,
                "timeframe_aggregation": True,
                "business_logic": True
            }
        )
        
        assert success, "Golden file test failed for comprehensive integration"


# Test runner configuration
def pytest_addoption(parser):
    """Add command-line option for updating golden files."""
    parser.addoption(
        "--update-golden",
        action="store_true",
        default=False,
        help="Update golden files with current test results"
    )


if __name__ == "__main__":
    """Run tests directly for development."""
    import asyncio
    
    async def run_single_test():
        """Run a single test for development."""
        test_suite = UniverseStateRealDataTestSuite(TEST_DIR)
        golden_test_case = UniverseStateGoldenTestCase(TEST_DIR)
        
        # Run single symbol test
        scenario = UniverseStateTestScenario(
            scenario_name="dev_test",
            symbols=["AAPL"],
            timeframe="5m",
            start_time=datetime(2024, 8, 1, 10, 0),
            end_time=datetime(2024, 8, 1, 11, 0),
            test_type="development"
        )
        
        try:
            universe_state = await scenario.execute_scenario(test_suite.test_data_manager)
            logger.info(f"✅ Universe state created: {len(universe_state.instrument_intervals)} instruments")
            
            # Save as golden file
            success = golden_test_case.run_universe_state_golden_test(
                test_method_name="dev_test_golden",
                universe_state=universe_state,
                symbols=scenario.symbols,
                timeframe=scenario.timeframe,
                start_time=scenario.start_time,
                end_time=scenario.end_time,
                update_golden=True
            )
            
            logger.info(f"Golden file test: {'✅ SUCCESS' if success else '❌ FAILED'}")
            
        except Exception as e:
            logger.error(f"❌ Test failed: {e}")
            import traceback
            traceback.print_exc()
    
    # Run development test
    asyncio.run(run_single_test())