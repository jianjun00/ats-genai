"""
Real Data Integration for Universe State Builder Testing

INTEGRATION STRATEGY:
1. **Real Market Data**: Use existing FirstRate minute bar data (AAPL, TSLA from Aug 2024)
2. **Real Business Logic**: Use actual UniverseStateBuilder with real dependencies
3. **Realistic Scenarios**: Market open, mid-day, close, multi-timeframe testing
4. **Comprehensive Validation**: OHLCV + indicators + rolling windows + state management

DEPENDENCIES HANDLED:
- MarketDataManager: Real FirstRate data integration
- IndicatorBuilder: Real technical indicator calculations  
- UniverseStateManager: Rolling cache and state management
- DailyMarketCapDAO: Market cap data integration
- InstrumentXrefsDAO: Symbol to instrument_id mapping
"""

from typing import List, Dict, Optional, Tuple
from pathlib import Path
from datetime import datetime, timedelta
import logging
import asyncio

from core.platform.config_env.environment import Environment, EnvironmentType
from domains.trading.services.state.universe_state_builder import UniverseStateIntervalBuilder
from domains.trading.services.state.universe_state import UniverseStateInterval
from domains.trading.services.state.universe_state_manager import UniverseStateManager


class UniverseStateTestDataManager:
    """Manages real test data setup and configuration for universe state testing."""
    
    def __init__(self, test_environment: Environment):
        self.environment = test_environment
        self.logger = logging.getLogger(f"{__name__}.TestDataManager")
        
        # Use real FirstRate production data
        self.firstrate_data_path = Path("/mnt/d/ats-data/minute-bars/firstrate")
        
        # Test universe configuration
        self.test_symbols = ["AAPL", "TSLA"]  # Available in real FirstRate production data
        self.test_universe_id = 1
        
    async def setup_test_environment(self) -> Tuple[UniverseStateIntervalBuilder, UniverseStateManager]:
        """Set up complete test environment with real dependencies."""
        self.logger.info("🔧 Setting up universe state test environment...")
        
        # Create UniverseStateManager for rolling cache management
        universe_state_manager = UniverseStateManager(self.environment)
        
        # Create UniverseStateBuilder with real configuration
        universe_builder = UniverseStateIntervalBuilder(
            env=self.environment,
            base_duration='1m',
            target_durations='1m,5m,15m,60m',  # Focus on intraday timeframes for testing (use 60m instead of 1h)
            universe_state_manager=universe_state_manager
        )
        
        self.logger.info("✅ Universe state test environment ready")
        return universe_builder, universe_state_manager
    
    async def get_test_instrument_ids(self) -> Dict[str, int]:
        """Get instrument IDs for test symbols."""
        from core.dao.instruments.instrument_xrefs_dao import InstrumentXrefsDAO
        
        xrefs_dao = InstrumentXrefsDAO(self.environment)
        
        # Get instrument IDs for our test symbols
        symbol_to_inst_id = {}
        for symbol in self.test_symbols:
            try:
                inst_id = await xrefs_dao.resolve_instrument_id_by_symbol(symbol, "ticker")
                if inst_id:
                    symbol_to_inst_id[symbol] = inst_id
                else:
                    self.logger.warning(f"No instrument_id found for symbol: {symbol}")
                    # Create a synthetic instrument_id for testing
                    symbol_to_inst_id[symbol] = hash(symbol) % 1000000
            except Exception as e:
                self.logger.warning(f"Error resolving instrument_id for {symbol}: {e}")
                # Create a synthetic instrument_id for testing
                symbol_to_inst_id[symbol] = hash(symbol) % 1000000
                
        self.logger.info(f"Test instrument mapping: {symbol_to_inst_id}")
        return symbol_to_inst_id
    
    def get_test_timeframes(self) -> List[str]:
        """Get timeframes for comprehensive testing."""
        return ['1m', '5m', '15m', '60m']  # Use 60m instead of 1h
    
    def get_test_time_scenarios(self) -> List[Dict[str, any]]:
        """Get realistic time scenarios for testing with real FirstRate data."""
        base_date = datetime(2024, 8, 1)  # Match our real FirstRate data
        
        return [
            {
                "name": "market_open",
                "description": "Market opening - first 30 minutes",
                "start_time": base_date.replace(hour=9, minute=30),
                "end_time": base_date.replace(hour=10, minute=0),
                "expected_intervals": 31  # 30 minutes + inclusive end
            },
            {
                "name": "mid_morning", 
                "description": "Mid-morning trading - 1 hour block",
                "start_time": base_date.replace(hour=10, minute=30),
                "end_time": base_date.replace(hour=11, minute=30),
                "expected_intervals": 61  # 60 minutes + inclusive end
            },
            {
                "name": "lunch_time",
                "description": "Lunch time trading - lower volume period",
                "start_time": base_date.replace(hour=12, minute=0),
                "end_time": base_date.replace(hour=13, minute=0),
                "expected_intervals": 61
            },
            {
                "name": "afternoon_rally",
                "description": "Afternoon trading - higher volume",
                "start_time": base_date.replace(hour=14, minute=0),
                "end_time": base_date.replace(hour=15, minute=0),
                "expected_intervals": 61
            },
            {
                "name": "market_close",
                "description": "Market close - final 30 minutes",
                "start_time": base_date.replace(hour=15, minute=30),
                "end_time": base_date.replace(hour=16, minute=0),
                "expected_intervals": 31
            }
        ]


class MockUniverseRunner:
    """Mock runner for universe state builder testing with real market data integration."""
    
    def __init__(self, environment: Environment, universe_id: int, instrument_ids: List[int]):
        self.environment = environment
        self.universe_id = universe_id
        self.instrument_ids = instrument_ids
        self.universe_manager = MockUniverseManager(universe_id, instrument_ids)
        
        # Set up real market data manager with FirstRate data
        self.market_data_manager = self._create_market_data_manager()
        
    def _create_market_data_manager(self):
        """Create UnifiedMarketDataManager configured for FirstRate test data."""
        from domains.market_data.services.core.unified_market_data_manager import (
            UnifiedMarketDataManager, MarketDataConfig, VendorType, StorageBackend
        )
        
        # Configure for FirstRate data with production data path
        production_data_path = "/mnt/d/ats-data/minute-bars/firstrate"
        
        config = MarketDataConfig(
            vendors=[VendorType.FIRSTRATE],  # Use FirstRate vendor for real data
            storage_backend=StorageBackend.FILE,  # Use file-based storage
            file_storage_path=production_data_path,
            enable_cache=True,
            enable_validation=True
        )
        
        return UnifiedMarketDataManager(config)
        
    def get_environment(self) -> Environment:
        return self.environment


class MockUniverseManager:
    """Mock universe manager for testing."""
    
    def __init__(self, universe_id: int, instrument_ids: List[int]):
        self.universe_id = universe_id
        self.instrument_ids = instrument_ids


class UniverseStateTestScenario:
    """Represents a complete test scenario for universe state testing."""
    
    def __init__(self, scenario_name: str, symbols: List[str], timeframe: str,
                 start_time: datetime, end_time: datetime, **metadata):
        self.scenario_name = scenario_name
        self.symbols = symbols
        self.timeframe = timeframe
        self.start_time = start_time
        self.end_time = end_time
        self.metadata = metadata
        self.logger = logging.getLogger(f"{__name__}.Scenario.{scenario_name}")
        
    async def execute_scenario(self, test_data_manager: UniverseStateTestDataManager) -> UniverseStateInterval:
        """Execute the test scenario and return UniverseStateInterval."""
        self.logger.info(f"🎯 Executing scenario: {self.scenario_name}")
        self.logger.info(f"   Symbols: {self.symbols}")
        self.logger.info(f"   Timeframe: {self.timeframe}")
        self.logger.info(f"   Time range: {self.start_time} → {self.end_time}")
        
        # Set up test environment
        universe_builder, universe_manager = await test_data_manager.setup_test_environment()
        
        # Get instrument IDs
        symbol_to_inst_id = await test_data_manager.get_test_instrument_ids()
        instrument_ids = [symbol_to_inst_id[symbol] for symbol in self.symbols if symbol in symbol_to_inst_id]
        
        if not instrument_ids:
            raise ValueError(f"No instrument IDs found for symbols: {self.symbols}")
            
        # Create mock runner
        runner = MockUniverseRunner(test_data_manager.environment, test_data_manager.test_universe_id, instrument_ids)
        
        # Execute universe state building
        from core.business.calendars.time_duration import TimeDuration
        duration = TimeDuration(self.timeframe)
        
        # CRITICAL FIX: First populate the cache with real market data by calling handleInterval
        # This simulates the normal flow where handleInterval populates the rolling cache
        self.logger.info(f"🔧 Populating cache with real market data via handleInterval...")
        
        # Call handleInterval for each minute in our time range to populate the cache
        current_minute = self.start_time
        while current_minute <= self.end_time:
            await universe_builder.handleInterval(runner, current_minute)
            current_minute += timedelta(minutes=1)
            
        self.logger.info(f"✅ Cache populated with real market data")
        
        # Now call the core test: use real UniverseStateBuilder logic
        universe_state_data = await universe_builder._build_universe_state_for_duration(
            duration, self.end_time, instrument_ids, runner
        )
        
        if not universe_state_data:
            raise ValueError(f"No universe state data generated for scenario: {self.scenario_name}")
            
        # Extract the UniverseStateInterval from the result
        # (universe_state_data is typically a dict with duration as key)
        universe_state = next(iter(universe_state_data.values())) if universe_state_data else None
        
        if not universe_state:
            raise ValueError(f"No UniverseStateInterval found in result for scenario: {self.scenario_name}")
            
        self.logger.info(f"✅ Scenario executed successfully: {len(universe_state.instrument_intervals)} instruments")
        return universe_state


class UniverseStateRealDataTestSuite:
    """Complete test suite for universe state builder with real data."""
    
    def __init__(self, test_dir: Path):
        self.test_dir = test_dir
        self.logger = logging.getLogger(f"{__name__}.TestSuite")
        
        # Create test environment (will use proper test database with migrations)
        self.environment = Environment(
            env_type=EnvironmentType.TEST,
            db_url="postgresql://test_user:test_password@localhost:5432/test_db"  # Will be overridden by fixture
        )
        
        self.test_data_manager = UniverseStateTestDataManager(self.environment)
        
    def create_comprehensive_test_scenarios(self) -> List[UniverseStateTestScenario]:
        """Create comprehensive test scenarios covering all important cases."""
        scenarios = []
        
        time_scenarios = self.test_data_manager.get_test_time_scenarios()
        timeframes = self.test_data_manager.get_test_timeframes()
        
        # Single-symbol scenarios across timeframes
        for symbol in self.test_data_manager.test_symbols:
            for timeframe in timeframes:
                for time_scenario in time_scenarios[:3]:  # Limit to first 3 time periods
                    scenario = UniverseStateTestScenario(
                        scenario_name=f"single_{symbol.lower()}_{timeframe}_{time_scenario['name']}",
                        symbols=[symbol],
                        timeframe=timeframe,
                        start_time=time_scenario['start_time'],
                        end_time=time_scenario['end_time'],
                        expected_intervals=time_scenario['expected_intervals'],
                        test_type="single_symbol"
                    )
                    scenarios.append(scenario)
        
        # Multi-symbol scenarios
        for timeframe in timeframes:
            for time_scenario in time_scenarios[:2]:  # Limit to first 2 time periods
                scenario = UniverseStateTestScenario(
                    scenario_name=f"multi_symbol_{timeframe}_{time_scenario['name']}",
                    symbols=self.test_data_manager.test_symbols,
                    timeframe=timeframe,
                    start_time=time_scenario['start_time'],
                    end_time=time_scenario['end_time'],
                    expected_intervals=time_scenario['expected_intervals'],
                    test_type="multi_symbol"
                )
                scenarios.append(scenario)
                
        # Rolling window scenarios (multiple time points for same timeframe)
        rolling_timeframe = '5m'
        base_time = datetime(2024, 8, 1, 10, 0)  # 10:00 AM
        for i in range(3):  # 3 consecutive 5-minute intervals
            start_time = base_time + timedelta(minutes=5*i)
            end_time = start_time + timedelta(minutes=5)
            
            scenario = UniverseStateTestScenario(
                scenario_name=f"rolling_window_{rolling_timeframe}_interval_{i}",
                symbols=["AAPL"],  # Focus on single symbol for rolling window
                timeframe=rolling_timeframe,
                start_time=start_time,
                end_time=end_time,
                test_type="rolling_window",
                interval_index=i
            )
            scenarios.append(scenario)
            
        self.logger.info(f"Created {len(scenarios)} test scenarios")
        return scenarios
    
    async def execute_all_scenarios(self) -> Dict[str, UniverseStateInterval]:
        """Execute all test scenarios and return results."""
        scenarios = self.create_comprehensive_test_scenarios()
        results = {}
        
        for scenario in scenarios:
            try:
                universe_state = await scenario.execute_scenario(self.test_data_manager)
                results[scenario.scenario_name] = universe_state
                self.logger.info(f"✅ Scenario completed: {scenario.scenario_name}")
            except Exception as e:
                self.logger.error(f"❌ Scenario failed: {scenario.scenario_name} - {e}")
                import traceback
                traceback.print_exc()
                
        self.logger.info(f"Completed {len(results)}/{len(scenarios)} scenarios")
        return results