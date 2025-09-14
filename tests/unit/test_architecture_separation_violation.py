"""
Test demonstrating architectural violation: UniverseStateManager contains aggregation logic.

CORRECT ARCHITECTURE:
- UniverseStateBuilder: Handles ALL data aggregation and transformation
- UniverseStateManager: Only persists and retrieves pre-aggregated data

CURRENT VIOLATION:
- UniverseStateManager contains pandas resampling/aggregation logic
- This duplicates UniverseStateBuilder functionality and violates single responsibility
"""

import pytest
import ast
import inspect

from domains.trading.services.state.universe_state_manager import UniverseStateManager
from domains.trading.services.state.universe_state_builder import UniverseStateIntervalBuilder


class TestArchitecturalSeparationViolation:
    """Test that demonstrates the architectural violation in UniverseStateManager."""

    def test_universe_state_manager_should_not_contain_aggregation_logic(self):
        """
        Test that UniverseStateManager should not contain pandas aggregation logic.

        This test demonstrates the architectural violation by detecting pandas
        resampling and aggregation methods in UniverseStateManager source code.
        """
        # Get the source code of UniverseStateManager
        manager_source = inspect.getsource(UniverseStateManager)

        # Parse the source code to detect inappropriate logic that should be in UniverseStateBuilder
        forbidden_patterns = [
            'resample',           # pandas.DataFrame.resample()
            'agg({',             # pandas aggregation with dictionary
            '.agg(',             # pandas aggregation method
            'resample_rules',    # Variable containing resampling rules
            '.rolling(',         # Technical indicator calculations
            '.ewm(',             # Exponential moving average calculations
            'sma_20 =',          # Simple moving average calculations
            'std_20 =',          # Standard deviation calculations
            'upper_band =',      # Bollinger band calculations
            'lower_band =',      # Bollinger band calculations
        ]

        # OHLC aggregation patterns (only when used together for OHLCV aggregation)
        ohlc_patterns = ["'first'", "'last'", "'max'", "'min'", "'sum'"]

        violations_found = []
        lines = manager_source.split('\n')

        for line_num, line in enumerate(lines, 1):
            # Check for forbidden patterns (aggregation and signal calculation logic)
            for pattern in forbidden_patterns:
                if pattern in line and not line.strip().startswith('#'):
                    violations_found.append(f"Line {line_num}: {line.strip()}")

            # Check for OHLCV aggregation (multiple OHLC patterns on nearby lines)
            if any(pattern in line for pattern in ohlc_patterns) and not line.strip().startswith('#'):
                # Look at surrounding lines to see if this is OHLCV aggregation
                context_lines = lines[max(0, line_num-3):line_num+3]
                ohlc_count = sum(1 for context_line in context_lines
                               for pattern in ohlc_patterns if pattern in context_line)

                # If 3+ OHLC patterns in nearby lines, it's likely OHLCV aggregation
                if ohlc_count >= 3 and 'drop_duplicates' not in line:
                    violations_found.append(f"Line {line_num}: {line.strip()}")

        # This test should FAIL initially to demonstrate the violation
        assert len(violations_found) == 0, (
            f"UniverseStateManager contains {len(violations_found)} aggregation logic violations:\n" +
            "\n".join(violations_found) +
            "\n\nUniverseStateManager should only persist/retrieve pre-aggregated data from UniverseStateBuilder."
        )

    def test_universe_state_builder_should_contain_aggregation_logic(self):
        """
        Test that UniverseStateBuilder correctly contains aggregation logic.

        This confirms the aggregation logic is in the right place.
        """
        # Get the source code of UniverseStateBuilder
        builder_source = inspect.getsource(UniverseStateIntervalBuilder)

        # Look for proper aggregation delegation to TimeDuration
        expected_aggregation_patterns = [
            'aggregate_intervals',  # Calls TimeDuration.aggregate_intervals()
            'duration.aggregate',   # Proper delegation to duration object
        ]

        aggregation_found = []
        lines = builder_source.split('\n')

        for line_num, line in enumerate(lines, 1):
            for pattern in expected_aggregation_patterns:
                if pattern in line and not line.strip().startswith('#'):
                    aggregation_found.append(f"Line {line_num}: {line.strip()}")

        assert len(aggregation_found) > 0, (
            "UniverseStateBuilder should contain proper aggregation logic delegation"
        )

    def test_proper_responsibility_separation(self):
        """
        Test the correct architectural pattern:
        - UniverseStateBuilder: Creates InstrumentInterval objects with correct OHLC
        - UniverseStateManager: Persists/retrieves UniverseStateInterval objects
        """
        # Check that UniverseStateBuilder has methods for building intervals
        builder_methods = [method for method in dir(UniverseStateIntervalBuilder)
                          if not method.startswith('_')]

        expected_builder_methods = ['handleInterval', 'handleStartOfDay', 'handleEndOfDay']
        for method in expected_builder_methods:
            assert method in builder_methods, (
                f"UniverseStateBuilder missing required method: {method}"
            )

        # Check that UniverseStateManager has methods for persistence
        manager_methods = [method for method in dir(UniverseStateManager)
                          if not method.startswith('_')]

        expected_manager_methods = ['addUniverseState', 'get_lag_prices', 'get_lead_prices']
        for method in expected_manager_methods:
            assert method in manager_methods, (
                f"UniverseStateManager missing required method: {method}"
            )

    def test_universe_state_manager_methods_should_expect_pre_aggregated_data(self):
        """
        Test that UniverseStateManager methods should work with pre-aggregated data.

        The methods should expect to receive InstrumentInterval objects that have
        already been properly aggregated by UniverseStateBuilder.
        """
        # Get method signatures
        manager_class = UniverseStateManager

        # Check addUniverseState method - should accept pre-built universe state
        add_universe_state = getattr(manager_class, 'addUniverseState', None)
        assert add_universe_state is not None, "addUniverseState method missing"

        # The method should NOT have parameters for timeframe aggregation
        method_source = inspect.getsource(add_universe_state)

        # Should not contain aggregation parameters
        forbidden_params = ['time_interval', 'resample_rule', 'agg_rules']
        for param in forbidden_params:
            assert param not in method_source, (
                f"addUniverseState should not handle aggregation parameter: {param}"
            )

    def test_architectural_fix_requirements(self):
        """
        Test defining the architectural fix requirements.

        This test documents what needs to be changed to fix the violation.
        """
        fix_requirements = {
            'remove_from_manager': [
                'pandas resampling logic',
                'OHLC aggregation dictionaries',
                'resample_rules mappings',
                'time_interval aggregation parameters'
            ],
            'ensure_in_builder': [
                'All timeframe aggregation via TimeDuration.aggregate_intervals',
                'Proper InstrumentInterval creation for each timeframe',
                'Rolling window management',
                'Multi-timeframe UniverseStateInterval creation'
            ],
            'manager_should_only': [
                'Persist pre-aggregated UniverseStateInterval objects',
                'Retrieve stored universe state data',
                'Handle database storage/querying',
                'Manage universe state lifecycle'
            ]
        }

        # This test documents the fix but doesn't enforce it yet
        # The actual fix should remove aggregation logic from UniverseStateManager
        assert len(fix_requirements['remove_from_manager']) == 4
        assert len(fix_requirements['ensure_in_builder']) == 4
        assert len(fix_requirements['manager_should_only']) == 4

        print("ARCHITECTURAL FIX REQUIREMENTS:")
        for category, items in fix_requirements.items():
            print(f"\n{category.upper().replace('_', ' ')}:")
            for item in items:
                print(f"  - {item}")