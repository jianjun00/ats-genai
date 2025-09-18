#!/usr/bin/env python3
"""
Integration Script for Financial Events with ATS Analytics Service
Sets up the integration and demonstrates the functionality
"""

import os
import sys
import asyncio
import json
from datetime import datetime, timedelta

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from domains.analytics.services.financial_events.analytics_integration import AnalyticsEventIntegration

async def setup_and_test_integration():
    """Set up and test the financial events integration"""
    
    print("🚀 ATS Financial Events Integration Setup")
    print("=" * 60)
    
    # Check if analytics service is running
    import requests
    try:
        response = requests.get("http://localhost:4000/health", timeout=5)
        if response.status_code != 200:
            print("❌ Analytics service not responding at localhost:4000")
            print("💡 Start it with: docker-compose -f docker-compose.intg.yml up -d")
            return False
        
        health_data = response.json()
        print(f"✅ Analytics service is healthy: {health_data.get('service', 'Unknown')}")
        
    except Exception as e:
        print(f"❌ Cannot connect to analytics service: {e}")
        print("💡 Start it with: docker-compose -f docker-compose.intg.yml up -d")
        return False
    
    # Initialize integration (using mock API key for demo)
    print("\n📋 Initializing financial events integration...")
    integration = AnalyticsEventIntegration(
        xai_api_key="demo_api_key_for_testing",
        analytics_base_url="http://localhost:4000"
    )
    
    # Step 1: Create events table
    print("\n🗄️  Creating financial events table...")
    table_created = await integration.create_events_table()
    if table_created:
        print("   ✅ Financial events table created successfully")
    else:
        print("   ❌ Failed to create table (may already exist)")
    
    # Step 2: Extract and store some events
    print("\n📊 Extracting financial events from xAI...")
    extraction_result = await integration.extract_and_store_events(
        start_date="2025-09-01",
        end_date="2025-09-13",
        symbols=["AAPL", "TSLA", "MSFT", "GOOGL"],
        force_refresh=False
    )
    
    if extraction_result['success']:
        print(f"   ✅ Successfully extracted and stored events:")
        print(f"      - Events extracted: {extraction_result['events_extracted']}")
        print(f"      - Events stored: {extraction_result['events_stored']}")
        print(f"      - Date range: {extraction_result['date_range']}")
        
        # Show preview of events
        if extraction_result.get('events_preview'):
            print("\n   📈 Preview of extracted events:")
            for i, event in enumerate(extraction_result['events_preview'], 1):
                symbol = event['symbol'] or 'MARKET'
                print(f"      {i}. {event['date']} | {symbol} ({event['impact']}) - {event['details']}")
    else:
        print(f"   ❌ Extraction failed: {extraction_result.get('error', 'Unknown error')}")
    
    # Step 3: Query events from analytics database
    print("\n🔍 Querying stored financial events...")
    events_query = integration.get_events_from_analytics(
        impact_level="high",
        limit=5
    )
    
    if events_query['success']:
        print(f"   ✅ Found {events_query['count']} high-impact events:")
        for i, event in enumerate(events_query['events'][:3], 1):
            symbol = event.get('company_symbol', 'MARKET')
            date_str = event['event_date']
            time_str = f" {event['event_time']}" if event['event_time'] else ""
            print(f"      {i}. {date_str}{time_str} | {symbol}: {event['details'][:70]}...")
    else:
        print(f"   ❌ Query failed: {events_query.get('error', 'Unknown error')}")
    
    # Step 4: Get summary statistics
    print("\n📊 Getting events summary...")
    summary = integration.get_events_summary()
    
    if summary['success'] and summary.get('summary'):
        stats = summary['summary'][0] if summary['summary'] else {}
        print("   📈 Events Statistics:")
        print(f"      - Total events: {stats.get('total_events', 0)}")
        print(f"      - Unique symbols: {stats.get('unique_symbols', 0)}")
        print(f"      - High impact events: {stats.get('high_impact_events', 0)}")
        print(f"      - Events last week: {stats.get('events_last_week', 0)}")
        print(f"      - Date range: {stats.get('earliest_date', 'N/A')} to {stats.get('latest_date', 'N/A')}")
    
    # Step 5: Show cache statistics
    print("\n🚀 Cache Performance:")
    cache_stats = await integration.event_extractor.get_cache_stats()
    if cache_stats.get('cache_enabled'):
        print(f"      - Hit rate: {cache_stats.get('hit_rate', 'N/A')}")
        print(f"      - Memory usage: {cache_stats.get('memory_usage_mb', 0)}MB")
        print(f"      - Total requests: {cache_stats.get('total_requests', 0)}")
        print(f"      - Cache hits: {cache_stats.get('hits', 0)}")
        print(f"      - Cache misses: {cache_stats.get('misses', 0)}")
    else:
        print("      - Caching disabled")
    
    print("\n🎉 Integration setup and test completed successfully!")
    print("\n💡 Next steps:")
    print("   1. Open http://localhost:4000 to access the analytics dashboard")
    print("   2. Navigate to the Financial Events section")
    print("   3. Use the API endpoints:")
    print("      - POST /financial_events/extract - Extract new events")
    print("      - GET /financial_events - Query stored events")
    print("      - GET /financial_events/summary - Get statistics")
    
    return True

def show_api_examples():
    """Show example API requests"""
    
    print("\n🔧 API Usage Examples:")
    print("=" * 40)
    
    examples = [
        {
            "description": "Extract events from xAI",
            "method": "POST",
            "endpoint": "/financial_events/extract",
            "body": {
                "start_date": "2025-09-01",
                "end_date": "2025-09-13",
                "symbols": ["AAPL", "TSLA"],
                "force_refresh": False
            }
        },
        {
            "description": "Query high-impact events",
            "method": "GET",
            "endpoint": "/financial_events?impact_level=high&limit=10"
        },
        {
            "description": "Get earnings events for AAPL",
            "method": "GET", 
            "endpoint": "/financial_events?symbol=AAPL&event_type=earnings"
        },
        {
            "description": "Get events summary",
            "method": "GET",
            "endpoint": "/financial_events/summary"
        }
    ]
    
    for i, example in enumerate(examples, 1):
        print(f"\n{i}. {example['description']}")
        print(f"   {example['method']} http://localhost:4000{example['endpoint']}")
        if example.get('body'):
            print(f"   Body: {json.dumps(example['body'], indent=10)}")
        
        # Show curl command
        if example['method'] == 'GET':
            print(f"   curl 'http://localhost:4000{example['endpoint']}'")
        else:
            body_str = json.dumps(example['body'])
            print(f"   curl -X {example['method']} 'http://localhost:4000{example['endpoint']}' \\")
            print(f"        -H 'Content-Type: application/json' \\")
            print(f"        -d '{body_str}'")

async def main():
    """Main integration script"""
    
    # Setup and test integration
    success = await setup_and_test_integration()
    
    if success:
        show_api_examples()
        
        # Offer to open dashboard
        try:
            import webbrowser
            print(f"\n🌐 Opening analytics dashboard in browser...")
            webbrowser.open("http://localhost:4000")
        except:
            print(f"\n🌐 Open http://localhost:4000 in your browser to view the dashboard")
    else:
        print("\n❌ Integration setup failed. Please check the requirements and try again.")

if __name__ == "__main__":
    asyncio.run(main())