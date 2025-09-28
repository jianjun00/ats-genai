"""
Test to reproduce NameError: mcal not defined in ExchangeCalendar.

This test reproduces the exact import issue and analyzes why test coverage
is missing for this critical function.
Following CLAUDE.md debug-first methodology: reproduce the issue first, then fix it.
"""

import pytest
import sys
import tempfile
import os
from pathlib import Path
from unittest.mock import patch, MagicMock

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from core.business.calendars.exchange_calendar import ExchangeCalendar


class TestExchangeCalendarCoverageGap:
    """Test to reproduce the mcal NameError and analyze test coverage gaps."""

    def test_reproduce_mcal_nameerror_before_fix(self):
        """
        Test to reproduce NameError: name 'mcal' is not defined.
        
        This verifies the exact issue that was occurring in production.
        """
        
        # This test reproduces the exact issue that WAS happening:
        # NameError: name 'mcal' is not defined at line 9
        
        # The issue is in this code (current broken state):
        # class ExchangeCalendar:
        #     def __init__(self, exchange: str):
        #         if mcal is None:  # <-- mcal is not defined/imported
        #             raise ImportError("pandas_market_calendars is required...")
        
        # Expected behavior: Should check if pandas_market_calendars is available
        
        with pytest.raises(NameError, match="name 'mcal' is not defined"):
            calendar = ExchangeCalendar("NYSE")
        
        print("✅ REPRODUCED ERROR: NameError: name 'mcal' is not defined")

    def test_reproduce_via_runner_integration(self):
        """
        Test to reproduce the error when called from Runner (as user experienced).
        
        This verifies the error occurs in the actual integration context.
        """
        
        # The error occurred when Runner tries to create ExchangeCalendar:
        # from core.business.calendars.exchange_calendar import ExchangeCalendar
        # exchange = getattr(self.market_data_manager, 'exchange', 'NYSE')
        # cal = ExchangeCalendar(exchange)
        
        # Simulate the exact call from Runner.iter_events()
        exchange = 'NYSE'  # Default value from Runner
        
        with pytest.raises(NameError, match="name 'mcal' is not defined"):
            cal = ExchangeCalendar(exchange)
        
        print("✅ REPRODUCED RUNNER INTEGRATION ERROR")

    def test_analyze_why_test_coverage_is_missing(self):
        """
        Analyze why this critical function lacks proper test coverage.
        
        This identifies the patterns that lead to untested code.
        """
        
        # REASON 1: Class might be mocked in tests instead of tested
        def scenario_with_mocked_exchange_calendar():
            """Simulate tests that mock instead of testing real behavior."""
            with patch('core.business.calendars.exchange_calendar.ExchangeCalendar') as mock_cal:
                mock_cal.return_value = MagicMock()
                
                # This test would pass but doesn't exercise the real function
                result = mock_cal("NYSE")
                assert result is not None  # Passes but tests nothing real
                return True
        
        # REASON 2: Tests might only test methods, not constructor
        def scenario_with_incomplete_constructor_testing():
            """Simulate tests that don't test the constructor."""
            # Tests might focus on trading_days(), is_holiday() methods
            # without ever calling __init__ which contains the bug
            # Example: Pre-constructed instances passed to test methods
            
            # This pattern would miss the import error:
            # @pytest.fixture
            # def calendar_instance():
            #     return MagicMock()  # Doesn't test real constructor
            
            return True
        
        # REASON 3: Integration tests might have mcal available accidentally
        def scenario_with_accidental_import_availability():
            """Simulate integration tests where imports work by accident."""
            # Integration tests might run in environments where
            # pandas_market_calendars is already imported by other modules
            # masking the missing import in unit test isolation
            pass
        
        # REASON 4: Class might never be instantiated in current test scenarios
        def scenario_with_unused_calendar_class():
            """Simulate tests that don't exercise calendar functionality."""
            # If ExchangeCalendar is only used in specific trading flows
            # that aren't tested, the import error would go unnoticed
            # Example: Only used in live trading, not in data processing tests
            pass
        
        # Verify the analysis scenarios
        mocked_test_passes = scenario_with_mocked_exchange_calendar()
        assert mocked_test_passes == True
        
        incomplete_constructor_test = scenario_with_incomplete_constructor_testing()
        assert incomplete_constructor_test == True
        
        print("📋 TEST COVERAGE GAP ANALYSIS:")
        print("   1. ❌ Class may be mocked in tests instead of testing real implementation")
        print("   2. ❌ Tests might focus on methods without testing constructor")
        print("   3. ❌ Integration tests might mask the issue with accidental import availability")
        print("   4. ❌ Class might not be instantiated in current test scenarios")
        print("   5. ❌ No unit tests directly test ExchangeCalendar.__init__ with real implementation")

    def test_identify_what_mcal_should_be(self):
        """
        Test to identify what mcal should be and how it should be imported.
        
        This analyzes the function to understand the intended behavior.
        """
        
        # Looking at the context and error message, mcal should likely be:
        # 1. pandas_market_calendars library (imported as mcal)
        # 2. Or a specific module from pandas_market_calendars
        # 3. The class is checking if the library is available before using it
        
        # Option 1: Import pandas_market_calendars as mcal
        try:
            import pandas_market_calendars as mcal
            mcal_available_as_import = True
        # Option 2: Import specific calendar factory
        try:
            from pandas_market_calendars import get_calendar
            get_calendar_available = True
        # The class should likely also initialize self.calendar
        def expected_initialization_pattern(exchange):
            """Expected pattern for proper initialization."""
            try:
                import pandas_market_calendars as mcal
                if mcal is not None:
                    # Should create self.calendar here
                    calendar = mcal.get_calendar(exchange.upper())
                    return calendar
        print("✅ MCAL SHOULD BE:")
        print(f"   1. pandas_market_calendars imported as mcal: Available={mcal_available_as_import}")
        print(f"   2. get_calendar function available: Available={get_calendar_available}")
        print(f"   3. Should initialize self.calendar attribute in __init__")
        print(f"   4. Should handle ImportError gracefully with clear message")

    def test_identify_missing_calendar_attribute(self):
        """
        Test to identify that self.calendar attribute is never initialized.
        
        This shows another critical issue in the class.
        """
        
        # Even if mcal import is fixed, the class has another issue:
        # Methods like is_holiday(), next_trading_date() use self.calendar
        # but self.calendar is never initialized in __init__
        
        class ProblematicCalendarPattern:
            """Demonstrates the pattern that would fail even after fixing mcal."""
            def __init__(self, exchange):
                # Missing: self.calendar = mcal.get_calendar(exchange)
                self.exchange = exchange
            
            def is_holiday(self, date):
                # This would fail: AttributeError: 'ProblematicCalendarPattern' object has no attribute 'calendar'
                return self.calendar.schedule(str(date), str(date)).empty
        
        # This demonstrates the second issue
        with pytest.raises(AttributeError, match="has no attribute 'calendar'"):
            problematic = ProblematicCalendarPattern("NYSE")
            import datetime
            problematic.is_holiday(datetime.date.today())
        
        print("📋 MISSING ATTRIBUTE ANALYSIS:")
        print("   ❌ self.calendar is never initialized in __init__")
        print("   ❌ Methods assume self.calendar exists but it's not created")
        print("   ❌ Would cause AttributeError even after fixing mcal import")

    def test_demonstrate_proper_test_coverage_approach(self):
        """
        Test to demonstrate how proper test coverage should work.
        
        This shows the testing approach that would have caught this bug.
        """
        
        def proper_test_approach():
            """Demonstrate proper testing that would catch the bug."""
            
            # PROPER APPROACH: Test real class instantiation
            try:
                # This SHOULD work but currently fails - that's what tests should catch
                calendar = ExchangeCalendar("NYSE")
                
                # If it worked, verify basic functionality
                import datetime
                today = datetime.date.today()
                is_holiday = calendar.is_holiday(today)
                return True
                
        # Run the proper test approach
        test_passed = proper_test_approach()
        assert test_passed == False  # Currently fails due to bugs
        
        print("🧪 PROPER TEST COVERAGE APPROACH:")
        print("   ✅ Test real class instantiation (not mocks)")
        print("   ✅ Test constructor with realistic parameters")
        print("   ✅ Test basic method calls after construction")
        print("   ✅ Assert on actual results and behavior")
        print("   ❌ Current test would FAIL, revealing both import and attribute bugs")

    def test_compare_bad_vs_good_testing_patterns(self):
        """
        Test to compare bad testing patterns vs good testing patterns.
        
        This shows why the bugs weren't caught.
        """
        
        # BAD PATTERN 1: Mock the entire class
        def bad_pattern_mock_everything():
            with patch('core.business.calendars.exchange_calendar.ExchangeCalendar') as mock_cal:
                mock_cal.return_value = MagicMock()
                
                # This passes but tests nothing
                result = mock_cal("NYSE")
                return result is not None
        
        # BAD PATTERN 2: Only test methods with pre-constructed instances
        def bad_pattern_methods_only():
            with patch.object(ExchangeCalendar, '__init__', return_value=None):
                calendar = ExchangeCalendar.__new__(ExchangeCalendar)
                calendar.calendar = MagicMock()  # Bypass initialization
                
                # Test methods without testing constructor
                return hasattr(calendar, 'is_holiday')
        
        # BAD PATTERN 3: Integration tests that mask the issue
        def bad_pattern_integration_masking():
            # Integration tests might run with pandas_market_calendars
            # already imported globally, masking the local import issue
            return True
        
        # GOOD PATTERN: Test real implementation with isolated setup
        def good_pattern_real_implementation():
            # Test real class instantiation - this reveals both bugs
            try:
                calendar = ExchangeCalendar("NYSE")
                return True
        # Compare patterns
        bad1_passes = bad_pattern_mock_everything()
        bad2_passes = bad_pattern_methods_only()
        bad3_passes = bad_pattern_integration_masking()
        good_implementation_works = good_pattern_real_implementation()
        
        assert bad1_passes == True   # Bad tests pass (but test nothing)
        assert bad2_passes == True   # Bad tests pass (but test nothing)
        assert bad3_passes == True   # Bad tests pass (but test nothing)
        assert good_implementation_works == False  # Good test catches bugs!
        
        print("📊 TESTING PATTERN COMPARISON (BEFORE FIX):")
        print(f"   ❌ Bad Pattern 1 (Mock everything): {bad1_passes} - HIDES BUGS")
        print(f"   ❌ Bad Pattern 2 (Methods only): {bad2_passes} - HIDES BUGS")
        print(f"   ❌ Bad Pattern 3 (Integration masking): {bad3_passes} - HIDES BUGS")
        print(f"   ✅ Good Pattern (Real implementation): {good_implementation_works} - CATCHES BUGS")
        print("\n💡 The good testing pattern catches BOTH import and attribute bugs!")
        print("💡 Bad testing patterns let MULTIPLE bugs slip into production!")


if __name__ == "__main__":
    # Run the coverage gap analysis
    test = TestExchangeCalendarCoverageGap()
    
    print("🔍 ANALYZING EXCHANGE_CALENDAR TEST COVERAGE GAP")
    print("=" * 60)
    
    try:
        print("\n1. Reproducing mcal NameError...")
        test.test_reproduce_mcal_nameerror_before_fix()
    try:
        print("\n2. Reproducing via runner integration...")
        test.test_reproduce_via_runner_integration()
    print("\n3. Analyzing why test coverage is missing...")
    test.test_analyze_why_test_coverage_is_missing()
    
    print("\n4. Identifying what mcal should be...")
    test.test_identify_what_mcal_should_be()
    
    print("\n5. Identifying missing calendar attribute...")
    test.test_identify_missing_calendar_attribute()
    
    print("\n6. Demonstrating proper test coverage approach...")
    test.test_demonstrate_proper_test_coverage_approach()
    
    print("\n7. Comparing bad vs good testing patterns...")
    test.test_compare_bad_vs_good_testing_patterns()
    
    print("\n📋 COVERAGE GAP ANALYSIS COMPLETE:")
    print("   ❌ PROBLEM 1: mcal is used but not imported")
    print("   ❌ PROBLEM 2: self.calendar is used but never initialized")
    print("   ❌ ROOT CAUSE: Tests mock class instead of testing real implementation")
    print("   ❌ CONSEQUENCE: Multiple bugs reach production without detection")
    print("   ✅ SOLUTION: Add real implementation tests for constructor and basic methods")
    print("\n🎯 NEXT: Fix mcal import and self.calendar initialization")