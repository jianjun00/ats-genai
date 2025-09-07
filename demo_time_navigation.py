#!/usr/bin/env python3
"""
Demo: Time Navigation API
Shows how users can navigate through time series data
"""

import requests
import json
from datetime import datetime

def demo_time_navigation():
    """Demo the time navigation functionality."""
    print("🎭 Time Navigation API Demo")
    print("="*50)
    
    base_url = "http://localhost:3000"
    dataset_id = 65
    sequence_id = "AAPL_20250701_000000_20250906_000000"
    
    # Step 1: Get navigation metadata
    print("🔍 Step 1: Get Navigation Capabilities")
    metadata_url = f"{base_url}/api/v1/training-datasets/{dataset_id}/sequences/{sequence_id}/navigation-metadata"
    
    response = requests.get(metadata_url)
    if response.status_code == 200:
        metadata = response.json()
        nav_info = metadata['navigation']
        
        print(f"   ✅ Available positions: {nav_info['min_row_index']} to {nav_info['max_row_index']}")
        print(f"   ✅ Total positions: {nav_info['total_positions']}")
        print(f"   ✅ Window size: {nav_info['window_size']} bars per position")
        print(f"   ✅ Available timeframes: {metadata['timeframes_available']}")
    else:
        print(f"   ❌ Failed to get metadata: {response.status_code}")
        return
    
    # Step 2: Navigate through different positions
    print(f"\n🎯 Step 2: Navigate Through Time")
    nav_url = f"{base_url}/api/v1/training-datasets/{dataset_id}/sequences/{sequence_id}/navigate"
    
    # Navigate to specific positions and directions
    navigation_tests = [
        ("Start at beginning", "direction=first"),
        ("Move forward", "direction=next&row_index=0"),
        ("Jump to middle", "row_index=50"),
        ("Move to end", "direction=last"),
        ("Go back", "direction=prev&row_index=100")
    ]
    
    for description, params in navigation_tests:
        url = f"{nav_url}?{params}"
        response = requests.get(url)
        
        if response.status_code == 200:
            data = response.json()
            nav_context = data.get('navigation_context', {})
            table_data = data.get('table_data', [])
            
            row_index = nav_context.get('current_row_index', 'Unknown')
            start_ts = nav_context.get('timestamp_range', {}).get('start')
            bars_count = len(table_data)
            
            # Format timestamp
            date_str = "Unknown"
            if start_ts:
                try:
                    date_str = datetime.fromtimestamp(start_ts).strftime('%Y-%m-%d %H:%M')
                except:
                    date_str = str(start_ts)
            
            # Get price info if available
            price_info = ""
            if table_data:
                first_bar = table_data[0]
                last_bar = table_data[-1]
                price_info = f", ${first_bar.get('open', 0):.2f} → ${last_bar.get('close', 0):.2f}"
            
            print(f"   ✅ {description}: Position {row_index}, {bars_count} bars, {date_str}{price_info}")
        else:
            print(f"   ❌ {description}: Failed ({response.status_code})")
    
    # Step 3: Show how it could be used in UI
    print(f"\n🌐 Step 3: How Users Would Navigate")
    print("   JavaScript code examples:")
    print("   ```javascript")
    print("   // Previous button")
    print(f"   fetch('{nav_url}?direction=prev&row_index=25')")
    print("   ")
    print("   // Next button") 
    print(f"   fetch('{nav_url}?direction=next&row_index=25')")
    print("   ")
    print("   // Slider to specific position")
    print(f"   fetch('{nav_url}?row_index=75')")
    print("   ")
    print("   // Jump to beginning")
    print(f"   fetch('{nav_url}?direction=first')")
    print("   ```")
    
    print(f"\n🎯 Step 4: UI Component Recommendations")
    print("   Recommended UI components:")
    print("   📍 Position slider: 0 ←→ 100 (with current position indicator)")
    print("   ⏮️  Previous button: Move back 10 positions")
    print("   ⏭️  Next button: Move forward 10 positions")  
    print("   ⏪ First button: Jump to beginning")
    print("   ⏩ Last button: Jump to end")
    print("   📅 Date display: Show current date/time range")
    print("   📊 Progress: 'Position 25 of 101' or 'Bar 25 of 2121 total'")
    
    print(f"\n✅ Time Navigation Demo Complete!")
    print("   The backend API is ready - frontend just needs navigation controls!")

if __name__ == "__main__":
    demo_time_navigation()