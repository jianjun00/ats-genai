#!/usr/bin/env python3
"""
Debug Time Navigation Options
Analyze available data ranges and recommend navigation approach
"""

import requests
import json
from datetime import datetime

def analyze_time_navigation():
    """Analyze time navigation possibilities."""
    print("🔍 Analyzing Time Navigation Options")
    print("="*50)
    
    try:
        # Test different row_index values to understand data range
        base_url = "http://localhost:3000/api/v1/training-datasets/65/sequences/AAPL_20250701_000000_20250906_000000/multi-timeframe"
        
        test_indices = [0, 10, 25, 50, 100]
        results = {}
        
        print("🧪 Testing different row_index values:")
        
        for row_index in test_indices:
            try:
                response = requests.get(base_url, params={"row_index": row_index}, timeout=10)
                if response.status_code == 200:
                    data = response.json()
                    table_data = data.get('table_data', [])
                    
                    if table_data:
                        first_row = table_data[0]
                        last_row = table_data[-1]
                        
                        # Convert timestamps to readable dates
                        def ts_to_date(ts):
                            try:
                                return datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M')
                            except:
                                return str(ts)
                        
                        first_date = ts_to_date(first_row.get('timestamp', 0))
                        last_date = ts_to_date(last_row.get('timestamp', 0))
                        
                        results[row_index] = {
                            'rows': len(table_data),
                            'first_date': first_date,
                            'last_date': last_date,
                            'first_price': first_row.get('open', 0),
                            'last_price': last_row.get('close', 0)
                        }
                        
                        print(f"   row_index={row_index:3d}: {len(table_data):2d} rows, {first_date} to {last_date}")
                        print(f"                   Price range: ${first_row.get('open', 0):.2f} to ${last_row.get('close', 0):.2f}")
                        
                else:
                    print(f"   row_index={row_index:3d}: ERROR {response.status_code}")
                    
            except Exception as e:
                print(f"   row_index={row_index:3d}: FAILED - {e}")
        
        # Analyze comprehensive features to understand total data range
        print(f"\n🔍 Analyzing comprehensive features data structure:")
        response = requests.get(base_url, params={"row_index": 0})
        if response.status_code == 200:
            data = response.json()
            comprehensive_features = data.get('comprehensive_features', [])
            
            if comprehensive_features:
                features = comprehensive_features[0]
                
                # Count features by timeframe
                timeframe_counts = {}
                sample_features = {}
                
                for key, value in features.items():
                    for tf in ['5m_', '15m_', '1h_', '1d_', '1w_']:
                        if key.startswith(tf):
                            if tf not in timeframe_counts:
                                timeframe_counts[tf] = 0
                                sample_features[tf] = []
                            timeframe_counts[tf] += 1
                            if len(sample_features[tf]) < 3:
                                sample_features[tf].append((key, value))
                            break
                
                print(f"   Comprehensive features breakdown:")
                total_sequences = 0
                for tf, count in timeframe_counts.items():
                    # Estimate sequence length based on feature count
                    # Each sequence step has ~6 features (OHLCV + derived)
                    estimated_steps = count // 6 if count >= 6 else count
                    total_sequences += estimated_steps
                    
                    print(f"     {tf}: {count} features → ~{estimated_steps} time steps")
                    
                print(f"   Total estimated time steps across all timeframes: ~{total_sequences}")
        
        # Recommendations
        print(f"\n💡 Navigation Recommendations:")
        print("="*50)
        
        print("🎯 **Option 1: Time Slider Navigation**")
        print("   - Add a slider control with row_index range 0 to max_available")
        print("   - Each slider position shows different 21-bar window")
        print("   - Fast, responsive navigation through time")
        print("   - Best for: Detailed analysis of specific periods")
        
        print("\n🎯 **Option 2: Date Range Picker**")
        print("   - Allow users to select start/end dates")
        print("   - Backend converts dates to appropriate row_index")
        print("   - More intuitive for business users")
        print("   - Best for: Specific date analysis")
        
        print("\n🎯 **Option 3: Navigation Buttons**")
        print("   - Previous/Next buttons to move through time")
        print("   - Jump to Beginning/End buttons")
        print("   - Simple, familiar interface")
        print("   - Best for: Sequential analysis")
        
        print("\n🎯 **Option 4: Multi-Scale Navigation**")
        print("   - Combine timeframe selection (5m, 1h, 1d) with time navigation")
        print("   - Different timeframes show different time ranges")
        print("   - Most comprehensive approach")
        print("   - Best for: Professional trading analysis")
        
        # Technical implementation details
        print(f"\n🔧 Technical Implementation Notes:")
        print("="*50)
        
        if results:
            max_working_index = max(results.keys())
            print(f"   - Current working row_index range: 0 to {max_working_index}")
            print(f"   - Each position shows ~21 bars")
            print(f"   - Total navigable windows: ~{max_working_index + 1}")
            
        print(f"   - Frontend needs to:")
        print(f"     * Add navigation UI components")
        print(f"     * Make API calls with different row_index")
        print(f"     * Update charts and tables dynamically")
        print(f"   - Backend already supports row_index parameter")
        print(f"   - No database changes needed")
        
    except Exception as e:
        print(f"❌ Analysis failed: {e}")
        import traceback
        traceback.print_exc()

def recommend_implementation():
    """Recommend specific implementation approach."""
    print(f"\n🚀 **RECOMMENDED IMPLEMENTATION**")
    print("="*50)
    
    print("**Phase 1: Basic Navigation (Immediate)**")
    print("1. Add Previous/Next buttons to sequence view")
    print("2. Add row_index display (e.g., 'Bar 25 of 100')")
    print("3. Update API call when buttons clicked")
    print("4. Add keyboard shortcuts (arrow keys)")
    
    print("\n**Phase 2: Enhanced Navigation (Short-term)**")
    print("1. Add time slider with visual timeline")
    print("2. Show current date/time for selected position")
    print("3. Add jump-to-date functionality")
    print("4. Add playback mode (auto-advance through time)")
    
    print("\n**Phase 3: Advanced Navigation (Long-term)**")
    print("1. Multi-timeframe synchronized navigation")
    print("2. Bookmark specific time positions")
    print("3. Event-based navigation (earnings, splits, etc.)")
    print("4. Compare mode (side-by-side time periods)")
    
    print("\n**Sample API Usage:**")
    print("```javascript")
    print("// Navigate to specific time position")
    print("fetch(`/api/v1/training-datasets/{id}/sequences/{seq}/multi-timeframe?row_index=25`)")
    print("")
    print("// Get total available range")
    print("fetch(`/api/v1/training-datasets/{id}/sequences/{seq}/metadata`)")
    print("```")
    
    print("\n**UI Components Needed:**")
    print("- Time navigation controls (buttons, slider)")
    print("- Current position indicator")
    print("- Date/time display")
    print("- Loading states for navigation")

if __name__ == "__main__":
    analyze_time_navigation()
    recommend_implementation()