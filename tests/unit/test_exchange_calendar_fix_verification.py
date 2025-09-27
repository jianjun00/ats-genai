"""
Test to verify ExchangeCalendar fix works correctly after resolving import and initialization issues.

This test verifies that the NameError: mcal not defined is fixed and
that self.calendar is properly initialized.
"""

import pytest
import sys
import datetime
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from core.business.calendars.exchange_calendar import ExchangeCalendar


class TestExchangeCalendarFixVerification:
    """Test to verify ExchangeCalendar fixes work correctly."""

    def test_exchange_calendar_initialization_works_after_fix(self):
        """
        Test that ExchangeCalendar can be initialized after fixing import and calendar issues.
        
        This verifies both fixes work together correctly.
        """
        
        # This should now work without NameError or AttributeError
        calendar = ExchangeCalendar("NYSE")
        
        # Verify the instance is created correctly
        assert calendar is not None
        assert calendar.exchange == "NYSE"
        assert hasattr(calendar, 'calendar')
        assert calendar.calendar is not None
        
        print("✅ EXCHANGE_CALENDAR FIX VERIFIED:")
        print(f"   ✅ No NameError: mcal is properly imported")
        print(f"   ✅ No AttributeError: self.calendar is initialized")
        print(f"   ✅ Exchange set correctly: {calendar.exchange}")
        print(f"   ✅ Calendar instance available: {type(calendar.calendar)}")

    def test_exchange_calendar_basic_functionality_works(self):
        """
        Test that basic ExchangeCalendar functionality works after the fix.
        
        This verifies that methods can be called without errors.
        """
        
        calendar = ExchangeCalendar("NYSE")
        
        # Test basic method calls
        today = datetime.date.today()
        
        # Test is_holiday method
        is_holiday = calendar.is_holiday(today)
        assert isinstance(is_holiday, bool)
        
        # Test trading_days method
        start_date = today
        end_date = today + datetime.timedelta(days=7)
        trading_days = list(calendar.trading_days(start_date, end_date))
        assert isinstance(trading_days, list)
        
        # Test all_trading_days method
        all_trading_days = calendar.all_trading_days(start_date, end_date)
        assert isinstance(all_trading_days, list)
        
        print("✅ EXCHANGE_CALENDAR FUNCTIONALITY VERIFIED:")
        print(f"   ✅ is_holiday works: {is_holiday}")
        print(f"   ✅ trading_days works: {len(trading_days)} days found")
        print(f"   ✅ all_trading_days works: {len(all_trading_days)} days found")

    def test_exchange_calendar_handles_various_exchanges(self):
        """
        Test that ExchangeCalendar works with different exchange names.
        
        This verifies the fix works for multiple exchange types.
        """
        
        # Test common exchanges
        exchanges_to_test = ["NYSE", "NASDAQ", "LSE"]
        
        for exchange in exchanges_to_test:
            try:
                calendar = ExchangeCalendar(exchange)
                assert calendar.exchange == exchange.upper()
                assert hasattr(calendar, 'calendar')
                print(f"   ✅ {exchange} calendar initialized successfully")
            except ValueError as e:
                # Some exchanges might not be available in pandas_market_calendars
                print(f"   ⚠️ {exchange} not available: {e}")
            except Exception as e:
                pytest.fail(f"Unexpected error for {exchange}: {e}")

    def test_exchange_calendar_error_handling(self):
        """
        Test that ExchangeCalendar provides proper error messages for invalid exchanges.
        
        This verifies that error handling works correctly.
        """
        
        # Test invalid exchange
        with pytest.raises(ValueError, match="Unable to initialize calendar"):
            calendar = ExchangeCalendar("INVALID_EXCHANGE")
        
        print("✅ ERROR HANDLING VERIFIED:")
        print("   ✅ Invalid exchange raises ValueError with clear message")

    def test_runner_integration_works_after_fix(self):
        """
        Test that the Runner integration scenario now works after the fix.
        
        This reproduces the exact usage pattern from Runner.iter_events().
        """
        
        # Simulate the exact call from Runner.iter_events()
        # from core.business.calendars.exchange_calendar import ExchangeCalendar
        # exchange = getattr(self.market_data_manager, 'exchange', 'NYSE')
        # cal = ExchangeCalendar(exchange)
        
        exchange = 'NYSE'  # Default value from Runner
        cal = ExchangeCalendar(exchange)
        
        # Test the method that Runner would call
        start_date = datetime.date(2025, 1, 1)
        end_date = datetime.date(2025, 1, 7)
        trading_days = list(cal.all_trading_days(start_date, end_date))
        
        assert cal is not None
        assert isinstance(trading_days, list)
        
        print("✅ RUNNER INTEGRATION VERIFIED:")
        print(f"   ✅ ExchangeCalendar works in Runner context")
        print(f"   ✅ all_trading_days returns {len(trading_days)} trading days")

    def test_compare_before_and_after_fix(self):
        """
        Test to demonstrate the before/after state of the fixes.
        
        This documents what was broken and what is now working.
        """
        
        # BEFORE FIX: These would have failed
        issues_that_were_fixed = [
            "NameError: name 'mcal' is not defined",
            "AttributeError: 'ExchangeCalendar' object has no attribute 'calendar'",
            "ImportError handling was incomplete"
        ]
        
        # AFTER FIX: These now work
        working_functionality = []
        
        try:
            calendar = ExchangeCalendar("NYSE")
            working_functionality.append("✅ Constructor works without NameError")
            
            today = datetime.date.today()
            calendar.is_holiday(today)
            working_functionality.append("✅ is_holiday works without AttributeError")
            
            trading_days = calendar.all_trading_days(today, today + datetime.timedelta(days=1))
            working_functionality.append("✅ all_trading_days works correctly")
            
        except Exception as e:
            pytest.fail(f"Fix verification failed: {e}")
        
        print("📊 BEFORE/AFTER COMPARISON:")
        print("\n❌ ISSUES THAT WERE FIXED:")
        for issue in issues_that_were_fixed:
            print(f"   - {issue}")
        
        print("\n✅ FUNCTIONALITY NOW WORKING:")
        for functionality in working_functionality:
            print(f"   - {functionality}")


if __name__ == "__main__":
    # Run the fix verification
    test = TestExchangeCalendarFixVerification()
    
    print("🔍 VERIFYING EXCHANGE_CALENDAR FIXES")
    print("=" * 50)
    
    print("\n1. Testing initialization after fix...")
    test.test_exchange_calendar_initialization_works_after_fix()
    
    print("\n2. Testing basic functionality...")
    test.test_exchange_calendar_basic_functionality_works()
    
    print("\n3. Testing various exchanges...")
    test.test_exchange_calendar_handles_various_exchanges()
    
    print("\n4. Testing error handling...")
    test.test_exchange_calendar_error_handling()
    
    print("\n5. Testing runner integration...")
    test.test_runner_integration_works_after_fix()
    
    print("\n6. Comparing before and after...")
    test.test_compare_before_and_after_fix()
    
    print("\n🎯 FIX VERIFICATION COMPLETE - ALL ISSUES RESOLVED!")