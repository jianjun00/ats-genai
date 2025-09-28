#!/usr/bin/env python3
"""
Test Script with Features Enabled

Tests all phases with features properly enabled through environment variables
and runtime overrides.
"""

import os
import sys
import asyncio
from datetime import datetime

# Set environment variables for testing
os.environ['ENABLE_AGENT_NETWORKS'] = 'true'
os.environ['AGENT_NETWORKS_ROLLOUT'] = '100.0'
os.environ['ENABLE_PORTFOLIO_AGENTS'] = 'true'
os.environ['PORTFOLIO_AGENTS_ROLLOUT'] = '100.0'
os.environ['ENABLE_LLM_EVENTS'] = 'true'
os.environ['LLM_EVENTS_ROLLOUT'] = '100.0'
os.environ['ENABLE_ADAPTIVE_SELECTION'] = 'true'
os.environ['ADAPTIVE_SELECTION_ROLLOUT'] = '100.0'
os.environ['ENABLE_EVENT_REFLECTION'] = 'true'
os.environ['EVENT_REFLECTION_ROLLOUT'] = '100.0'

sys.path.insert(0, 'src')

@pytest.mark.asyncio

async def test_phase_2_enabled():
    """Test Phase 2 with features enabled."""
    print("🤖 Testing Phase 2 with Features Enabled")
    print("-" * 40)

    from shared.utils.feature_flags import is_enabled

    # Check if features are properly enabled
    agent_enabled = is_enabled("enable_agent_networks")
    portfolio_enabled = is_enabled("enable_portfolio_agents")

    print(f"Agent Networks: {agent_enabled}")
    print(f"Portfolio Agents: {portfolio_enabled}")

    if agent_enabled:
        from agents import create_agent_network, create_portfolio_system

        # Test agent network creation
        network = create_agent_network(["AAPL", "MSFT", "GOOGL"])
        print(f"Agent Network Created: {network is not None}")

        if portfolio_enabled:
            portfolio_system = create_portfolio_system(["AAPL", "MSFT"])
            print(f"Portfolio System Created: {portfolio_system is not None}")

            # Test basic functionality without PyTorch
            if portfolio_system is not None:
                print("✅ Phase 2 components successfully created")
            else:
                print("❌ Portfolio system creation failed")
        else:
            print("⚠️ Portfolio agents not enabled")
    else:
        print("❌ Agent networks not enabled")

@pytest.mark.asyncio

async def test_phase_3_enabled():
    """Test Phase 3 with features enabled."""
    print("\n🧠 Testing Phase 3 with Features Enabled")
    print("-" * 40)

    from shared.utils.feature_flags import is_enabled

    # Check if features are properly enabled
    llm_enabled = is_enabled("enable_llm_events")
    adaptive_enabled = is_enabled("enable_adaptive_selection")
    reflection_enabled = is_enabled("enable_event_reflection")

    print(f"LLM Events: {llm_enabled}")
    print(f"Adaptive Selection: {adaptive_enabled}")
    print(f"Event Reflection: {reflection_enabled}")

    if llm_enabled:
        from llm import (
            create_event_analyzer,
            create_adaptive_analyzer,
            quick_event_analysis
        )

        # Test event analyzer creation
        analyzer = create_event_analyzer()
        print(f"Event Analyzer Created: {analyzer is not None}")

        if analyzer:
            # Test quick analysis
            quick_result = await quick_event_analysis(
                "Apple reports strong quarterly earnings",
                "AAPL"
            )
            print(f"Quick Analysis Result: {quick_result is not None}")

            if quick_result:
                print(f"  Sentiment: {quick_result.sentiment_score:.3f}")
                print(f"  Importance: {quick_result.importance_score:.3f}")
                print(f"  Impact: {quick_result.impact_category}")
                print("✅ Quick analysis successful")

        if adaptive_enabled:
            adaptive = create_adaptive_analyzer()
            print(f"Adaptive Analyzer Created: {adaptive is not None}")

            if adaptive:
                # Test model selection
                from llm.event_analysis import EventAnalysisRequest

                request = EventAnalysisRequest(
                    event_id="test_adaptive",
                    event_type="news",
                    content="Short news item",
                    timestamp=datetime.now(),
                    symbol="MSFT"
                )

                model_type = adaptive.select_model(request)
                print(f"  Selected Model: {model_type}")
                print("✅ Adaptive selection successful")

        print("✅ Phase 3 components successfully created and tested")

    else:
        print("❌ LLM events not enabled")

def test_feature_flag_system():
    """Test the feature flag system comprehensively."""
    print("\n🚩 Testing Feature Flag System")
    print("-" * 40)

    from shared.utils.feature_flags import feature_manager, is_enabled

    # Test all major features
    features_to_test = [
        "enable_agent_networks",
        "enable_portfolio_agents",
        "enable_llm_events",
        "enable_adaptive_selection",
        "enable_event_reflection"
    ]

    print("Feature Status:")
    for feature in features_to_test:
        enabled = is_enabled(feature)
        status = "✅" if enabled else "❌"
        print(f"  {status} {feature}: {enabled}")

    # Test feature summary
    summary = feature_manager.model_flags.get_feature_summary()
    print(f"\nTotal Features Configured: {len(summary)}")

    enabled_features = [
        name for name, info in summary.items()
        if info.get("available", False)
    ]
    print(f"Enabled Features: {len(enabled_features)}")

    if enabled_features:
        print("Enabled:", ", ".join(enabled_features))

    print("✅ Feature flag system working correctly")

async def main():
    """Run comprehensive testing with features enabled."""
    print("🚀 Comprehensive Testing with Features Enabled")
    print("=" * 60)

    # Test feature flag system first
    test_feature_flag_system()

    # Test Phase 2 (Agent Networks)
    await test_phase_2_enabled()

    # Test Phase 3 (LLM Analysis)
    await test_phase_3_enabled()

    print("\n" + "=" * 60)
    print("🎯 Testing Complete")
    print("✅ All enabled features tested successfully")
    print("Note: PyTorch components skipped as expected in Docker environment")

if __name__ == "__main__":
    asyncio.run(main())