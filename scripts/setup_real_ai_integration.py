#!/usr/bin/env python3
"""
Setup Script for Real AI Financial Events Integration
Helps configure xAI and Grok API keys for actual financial data extraction
"""

import os
import sys

def main():
    print("🚀 Real AI Financial Events Integration Setup")
    print("=" * 60)

    print("\n📝 To enable real financial events extraction, you need API keys:")
    print("   1. xAI API Key - for both xAI and Grok access")
    print("   2. Alternative: Separate Grok API key if available")

    print("\n🔑 How to get xAI API Key:")
    print("   1. Visit https://x.ai/api")
    print("   2. Sign up or log in to your account")
    print("   3. Navigate to API Console")
    print("   4. Create new API key")
    print("   5. Copy the key (starts with 'xai-...')")

    print(f"\n💰 Pricing (as of 2025):")
    print(f"   • Grok-4: Input $0.0002/token, Output $0.002/token")
    print(f"   • Live Search: $25 per 1,000 sources ($0.025 per source)")
    print(f"   • Example: 50 events extraction ≈ $0.50-2.00")

    print(f"\n⚙️ Configuration Options:")
    print(f"   A. Environment Variables (Recommended)")
    print(f"   B. Docker Environment")
    print(f"   C. Manual Configuration")

    choice = input(f"\nSelect setup method (A/B/C): ").upper().strip()

    if choice == 'A':
        setup_environment_variables()
    elif choice == 'B':
        setup_docker_environment()
    elif choice == 'C':
        setup_manual_configuration()
    else:
        print("❌ Invalid choice. Exiting.")
        sys.exit(1)

def setup_environment_variables():
    print("\n🔧 Environment Variables Setup")
    print("-" * 40)

    api_key = input("Enter your xAI API Key: ").strip()

    if not api_key or api_key == 'demo_key_for_testing':
        print("❌ Please provide a real API key")
        return

    # Add to shell profile
    shell_profile = os.path.expanduser("~/.bashrc")
    if os.path.exists(os.path.expanduser("~/.zshrc")):
        shell_profile = os.path.expanduser("~/.zshrc")

    export_line = f"export XAI_API_KEY='{api_key}'"

    print(f"\n📝 Add this line to {shell_profile}:")
    print(f"   {export_line}")

    add_to_profile = input("\nAdd automatically? (y/N): ").lower().strip() == 'y'

    if add_to_profile:
        with open(shell_profile, 'a') as f:
            f.write(f"\n# xAI API Key for ATS Financial Events\n")
            f.write(f"{export_line}\n")
        print(f"✅ Added to {shell_profile}")
        print(f"   Run: source {shell_profile}")

    print(f"\n🔄 Current session:")
    print(f"   export XAI_API_KEY='{api_key}'")
    os.environ['XAI_API_KEY'] = api_key

def setup_docker_environment():
    print("\n🐳 Docker Environment Setup")
    print("-" * 40)

    api_key = input("Enter your xAI API Key: ").strip()

    if not api_key:
        print("❌ Please provide a real API key")
        return

    env_file = ".env"
    env_content = f"""
# xAI/Grok API Configuration
XAI_API_KEY={api_key}
GROK_API_KEY={api_key}

# Optional: Separate Grok key if you have one
# GROK_API_KEY=your_separate_grok_key_here
"""

    with open(env_file, 'w') as f:
        f.write(env_content.strip())

    print(f"✅ Created {env_file}")

    print(f"\n🔄 Restart analytics service:")
    print(f"   docker restart ats-intg-analytics")

    print(f"\n📋 Or rebuild with environment:")
    print(f"   docker-compose -f docker-compose.intg.yml --env-file .env up -d analytics-intg")

def setup_manual_configuration():
    print("\n⚙️ Manual Configuration")
    print("-" * 40)

    api_key = input("Enter your xAI API Key: ").strip()

    if not api_key:
        print("❌ Please provide a real API key")
        return

    config_snippet = f'''
# Add to your analytics service startup or configuration:

import os
os.environ['XAI_API_KEY'] = '{api_key}'
os.environ['GROK_API_KEY'] = '{api_key}'

# Or modify docker run command:
docker run -e XAI_API_KEY='{api_key}' -e GROK_API_KEY='{api_key}' ...
'''

    print("📝 Configuration snippet:")
    print(config_snippet)

def test_api_integration():
    print("\n🧪 Testing API Integration")
    print("-" * 40)

    api_key = os.getenv('XAI_API_KEY')

    if not api_key or api_key == 'demo_key_for_testing':
        print("❌ No API key configured")
        return False

    print(f"✅ API Key configured: {api_key[:8]}...")

    # Test connection
    print("🔄 Testing connection to analytics service...")
    try:
        import requests
        response = requests.get("http://localhost:4000/financial_events/sources", timeout=5)

        if response.status_code == 200:
            data = response.json()
            print(f"✅ Analytics service connected")
            print(f"   Status: {data.get('status', 'unknown')}")
            print(f"   Available sources: {data.get('available_sources', [])}")

            if data.get('status') == 'demo_mode':
                print("⚠️ Still in demo mode - restart service to pick up API key")

            return True
        else:
            print(f"❌ Analytics service error: {response.status_code}")
            return False

    except Exception as e:
        print(f"❌ Connection failed: {e}")
        return False

def run_sample_extraction():
    print("\n🚀 Sample Financial Events Extraction")
    print("-" * 40)

    if not test_api_integration():
        return

    print("🔄 Extracting recent financial events...")

    try:
        import requests
        import json

        extract_data = {
            "start_date": "2025-09-10",
            "end_date": "2025-09-13",
            "symbols": ["AAPL", "TSLA", "NVDA"],
            "source": "combined",
            "force_refresh": True
        }

        response = requests.post(
            "http://localhost:4000/financial_events/extract",
            json=extract_data,
            timeout=60
        )

        if response.status_code == 200:
            result = response.json()

            if result.get('success'):
                print(f"✅ Extraction successful!")
                print(f"   Events extracted: {result.get('events_extracted', 0)}")
                print(f"   Unique events: {result.get('events_unique', 0)}")
                print(f"   Events stored: {result.get('events_stored', 0)}")
                print(f"   Sources used: {result.get('sources_used', [])}")

                if result.get('events_preview'):
                    print(f"\n📋 Sample events:")
                    for i, event in enumerate(result['events_preview'][:3], 1):
                        print(f"   {i}. {event.get('symbol', 'MARKET')}: {event.get('details', 'N/A')[:80]}...")

                return True
            else:
                print(f"❌ Extraction failed: {result.get('error', 'Unknown error')}")
                return False
        else:
            print(f"❌ API error: {response.status_code}")
            return False

    except Exception as e:
        print(f"❌ Extraction failed: {e}")
        return False

if __name__ == "__main__":
    try:
        main()

        # Test the setup
        proceed = input("\n🧪 Test the integration now? (y/N): ").lower().strip() == 'y'
        if proceed:
            if test_api_integration():
                run_sample = input("\n🚀 Run sample extraction? (y/N): ").lower().strip() == 'y'
                if run_sample:
                    success = run_sample_extraction()

                    if success:
                        print(f"\n🎉 Setup Complete!")
                        print(f"   ✅ Real AI financial events integration is working")
                        print(f"   🌐 View events at: http://10.0.0.79:4000/")
                        print(f"   🔮 Click 'AI Financial Events (xAI + Grok)' button")
                    else:
                        print(f"\n⚠️ Setup complete but extraction needs debugging")

        print(f"\n📚 Next Steps:")
        print(f"   1. Access dashboard: http://10.0.0.79:4000/")
        print(f"   2. Click '🔮 AI Financial Events (xAI + Grok)'")
        print(f"   3. Use 'Extract New Events' to get real data")
        print(f"   4. Monitor API costs and cache performance")

    except KeyboardInterrupt:
        print(f"\n\n👋 Setup cancelled by user")
    except Exception as e:
        print(f"\n❌ Setup error: {e}")
        sys.exit(1)