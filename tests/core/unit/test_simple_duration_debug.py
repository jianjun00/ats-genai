"""
Simple debug test to identify why only 1 duration is processed.

This test focuses on the core issue without complex mocking.
"""

import pytest
from datetime import datetime
from unittest.mock import Mock, patch
import gin

from domains.trading.services.state.universe_state_builder import UniverseStateIntervalBuilder
from core.platform.config.environment import Environment
from core.business.calendars.time_duration import TimeDuration


class TestSimpleDurationDebug:
    """Simple tests to debug duration processing."""

    def setup_method(self):
        """Set up test environment."""
        gin.clear_config()
        
        # Create mock environment
        self.mock_env = Mock()
        self.mock_env.env_type = "test"
        self.mock_env.get_table_name.return_value = "test_table"
        self.mock_env.get_env_type.return_value = "test"

    def test_debug_universe_state_builder_on_interval_complete(self):
        """Debug: Check what happens in on_interval_complete method."""
        
        print("\n=== DEBUGGING on_interval_complete METHOD ===")
        
        with patch('domains.trading.services.state.universe_state_manager.UniverseStateManager') as mock_universe_manager_class:
            mock_universe_manager = Mock()
            mock_universe_manager_class.return_value = mock_universe_manager
            
            # Create builder with multiple timeframes
            target_durations_str = '5m,15m,30m,60m,1d'
            base_duration_str = '5m'
            
            print(f"🔍 Creating builder with:")
            print(f"   base_duration: '{base_duration_str}'")
            print(f"   target_durations: '{target_durations_str}'")
            
            builder = UniverseStateIntervalBuilder(
                env=self.mock_env,
                base_duration=base_duration_str,
                target_durations=target_durations_str,
                universe_state_manager=mock_universe_manager
            )
            
            print(f"🔍 Builder configuration:")
            print(f"   base_duration: {builder.base_duration} ({type(builder.base_duration)})")
            print(f"   target_durations: {[d.get_duration_string() for d in builder.target_durations]}")
            print(f"   target_durations count: {len(builder.target_durations)}")

    def test_debug_gin_configuration_parsing(self):
        """Debug: Check if Gin configuration affects duration processing."""
        
        print("\n=== DEBUGGING GIN CONFIGURATION PARSING ===")
        
        # Test different Gin configurations
        test_configs = [
            ('5m,15m,30m,60m,1d', '5m'),
            ('1m,5m,15m,1h,1d', '1m'),
            ('15m,1h', '15m'),
            ('1h', '1h'),
        ]
        
        for target_durations_str, base_duration_str in test_configs:
            print(f"\n🔍 Testing config: target='{target_durations_str}', base='{base_duration_str}'")
            
            gin.clear_config()
            
            with patch('domains.trading.services.state.universe_state_manager.UniverseStateManager') as mock_manager_class:
                mock_manager = Mock()
                mock_manager_class.return_value = mock_manager
                
                builder = UniverseStateIntervalBuilder(
                    env=self.mock_env,
                    base_duration=base_duration_str,
                    target_durations=target_durations_str,
                    universe_state_manager=mock_manager
                )
                
                print(f"   ✅ Successfully created builder")
                print(f"   📊 Parsed {len(builder.target_durations)} target durations:")
                for i, duration in enumerate(builder.target_durations):
                    print(f"      [{i}] {duration.get_duration_string()}")
                
                # Check if base_duration is properly handled
                base_str = builder.base_duration.get_duration_string()
                target_strings = [d.get_duration_string() for d in builder.target_durations]
                
                if base_str in target_strings:
                    print(f"   ✅ Base duration '{base_str}' found in target_durations")
                else:
                    print(f"   ⚠️  Base duration '{base_str}' NOT in target_durations")
                    print(f"      This might cause processing issues")
                    
    def test_debug_check_actual_universe_state_manager_calls(self):
        """Debug: Check what actually gets passed to UniverseStateManager."""
        
        print("\n=== DEBUGGING UNIVERSE STATE MANAGER CALLS ===")
        
        with patch('domains.trading.services.state.universe_state_manager.UniverseStateManager') as mock_universe_manager_class:
            mock_universe_manager = Mock()
            mock_universe_manager_class.return_value = mock_universe_manager
            
            # Track calls to addUniverseState and addUniverseStateInterval
            mock_universe_manager.addUniverseState = Mock()
            mock_universe_manager.addUniverseStateInterval = Mock()
            
            builder = UniverseStateIntervalBuilder(
                env=self.mock_env,
                base_duration='5m',
                target_durations='5m,15m,30m,60m,1d',
                universe_state_manager=mock_universe_manager
            )
            
            print(f"🔍 Builder created with {len(builder.target_durations)} target durations")
            
            # The key insight: Look at the actual method calls to understand the flow
            print(f"🔍 Checking if builder has on_interval_complete method...")
            if hasattr(builder, 'on_interval_complete'):
                print(f"   ✅ Found on_interval_complete method")
                
                # Try to understand what this method would do with multiple durations
                import inspect
                sig = inspect.signature(builder.on_interval_complete)
                print(f"   📋 Method signature: {sig}")
                
                # The method likely processes intervals and calls UniverseStateManager
                # Let's see if we can trace what gets passed
                
            else:
                print(f"   ❌ No on_interval_complete method found")
                
            # Check other relevant methods
            for method_name in ['process_intervals', 'build_universe_state', '__call__']:
                if hasattr(builder, method_name):
                    print(f"   📋 Found method: {method_name}")

    def test_debug_find_the_bottleneck(self):
        """Debug: Find where multiple durations get reduced to 1."""
        
        print("\n=== FINDING THE BOTTLENECK ===")
        
        # The critical question: Where does the reduction from 5 durations to 1 happen?
        
        print("🔍 Possible bottleneck locations:")
        print("   1. UniverseStateIntervalBuilder.target_durations parsing")
        print("   2. UniverseStateIntervalBuilder.on_interval_complete method")
        print("   3. UniverseStateManager.addUniverseState method")
        print("   4. Some filtering logic in between")
        
        # Test hypothesis: target_durations parsing is correct
        durations_str = '5m,15m,30m,60m,1d'
        parsed_durations = [TimeDuration(d.strip()) for d in durations_str.split(',')]
        
        print(f"\n🔍 Hypothesis 1: target_durations parsing")
        print(f"   Input: '{durations_str}'")
        print(f"   Parsed: {len(parsed_durations)} durations")
        for i, d in enumerate(parsed_durations):
            print(f"      [{i}] {d.get_duration_string()}")
        print("   ✅ Parsing works correctly - not the bottleneck")
        
        # Test hypothesis: The issue is in how data flows from builder to manager
        print(f"\n🔍 Hypothesis 2: Data flow from builder to manager")
        print("   Key insight from debug output: '[USM.addUniverseState] durations=1'")
        print("   This means only 1 entry reaches UniverseStateManager.addUniverseState")
        print("   So the reduction happens BEFORE UniverseStateManager")
        print("   Likely location: UniverseStateIntervalBuilder.on_interval_complete")
        
        # The smoking gun: We need to check on_interval_complete method
        print(f"\n🔍 Next step: Examine UniverseStateIntervalBuilder.on_interval_complete")
        print("   This method likely builds duration_to_state dict")
        print("   If it only creates 1 entry, that explains the 'durations=1' output")

    def test_debug_hypothesis_verification(self):
        """Debug: Test the hypothesis that on_interval_complete is the bottleneck."""
        
        print("\n=== VERIFYING BOTTLENECK HYPOTHESIS ===")
        
        # Create a builder and check its methods
        with patch('domains.trading.services.state.universe_state_manager.UniverseStateManager') as mock_manager_class:
            mock_manager = Mock()
            mock_manager_class.return_value = mock_manager
            
            builder = UniverseStateIntervalBuilder(
                env=self.mock_env,
                base_duration='5m',
                target_durations='5m,15m,30m,60m,1d',
                universe_state_manager=mock_manager
            )
            
            print(f"🔍 Inspecting UniverseStateIntervalBuilder methods:")
            
            # List all methods
            methods = [method for method in dir(builder) if not method.startswith('_')]
            for method in methods:
                if 'interval' in method.lower() or 'complete' in method.lower() or 'process' in method.lower():
                    print(f"   📋 {method}")
            
            # The key method we need to understand
            if hasattr(builder, 'on_interval_complete'):
                method = getattr(builder, 'on_interval_complete')
                print(f"\n🔍 on_interval_complete method:")
                print(f"   Type: {type(method)}")
                
                # Get the source code location
                import inspect
                source_file = inspect.getfile(method)
                source_lines = inspect.getsourcelines(method)
                print(f"   Source: {source_file}:{source_lines[1]}")
                print("\n🎯 CRITICAL FINDING:")
                print("   To fix the duplication bug, we need to examine")
                print("   UniverseStateIntervalBuilder.on_interval_complete method")
                print("   This method likely builds the duration_to_state dict that")
                print("   gets passed to UniverseStateManager.addUniverseState")
                print("   If it only creates 1 entry instead of 5, that's our bug!")
                
            else:
                print("   ❌ on_interval_complete method not found")