"""
Dynamic Modeling Universe Demo

This demonstrates the dynamic universe system that:
1. Automatically adds stocks meeting criteria (>$400M cap, >$100M volume)
2. Issues warnings when stocks fail criteria
3. Removes stocks after 1 week grace period
4. Enforces 1-year re-entry restriction
5. Runs daily monitoring and updates

Usage:
    PYTHONPATH=src python examples/dynamic_universe_demo.py
"""

import asyncio
import sys
from pathlib import Path
from datetime import date, timedelta
import logging

# Add src to path for imports
sys.path.append(str(Path(__file__).parent.parent / "src"))

from config.environment import Environment
from universe.dynamic_modeling_universe import DynamicModelingUniverse


async def run_universe_demo():
    """Run complete universe demo"""
    print("🌌 Dynamic Modeling Universe Demo")
    print("=" * 60)
    print("Demonstrating automated universe management:")
    print("• Entry: >$400M market cap AND >$100M volume (52-day avg)")
    print("• Grace period: 1 week after failing criteria")
    print("• Re-entry restriction: 1 year after removal")
    print("• Daily monitoring and updates")
    print()
    
    # Setup logging
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    
    try:
        # Initialize system
        print("🔧 Initializing dynamic universe system...")
        env = Environment()
        universe = DynamicModelingUniverse(env)
        await universe.initialize()
        print("✅ System initialized")
        
        # Run initial update
        print("\n📊 Running initial universe update...")
        summary = await universe.run_daily_update()
        
        print(f"\n📋 Update Summary:")
        print(f"   • Current universe size: {summary['current_count']} stocks")
        print(f"   • Stocks meeting criteria: {summary['qualifying_count']}")
        print(f"   • New additions: {len(summary['added'])}")
        print(f"   • Removals: {len(summary['removed'])}")
        print(f"   • Warnings issued: {len(summary['warned'])}")
        
        # Show additions
        if summary['added']:
            print(f"\n🎉 New Stocks Added:")
            for add in summary['added']:
                print(f"   + {add['symbol']}: ${add['market_cap']:.0f}M cap, ${add['volume']:.0f}M volume")
        
        # Show warnings
        if summary['warned']:
            print(f"\n⚠️ Stocks on Warning:")
            for warn in summary['warned']:
                print(f"   ⚠ {warn['symbol']}: {warn['reason']}")
                print(f"     Grace period ends: {warn['grace_period_ends']}")
        
        # Show removals
        if summary['removed']:
            print(f"\n❌ Stocks Removed:")
            for removal in summary['removed']:
                print(f"   - {removal['symbol']}: {removal['reason']}")
        
        # Generate current report
        print(f"\n📈 Generating current universe report...")
        report = await universe.get_current_universe_report()
        
        # Save report
        report_file = f"dynamic_universe_demo_report.md"
        with open(report_file, 'w') as f:
            f.write(report)
        
        print(f"✅ Report saved to: {report_file}")
        
        # Display key sections of report
        report_lines = report.split('\n')
        
        # Find and display current universe table
        print(f"\n📊 Current Universe Preview:")
        in_table = False
        table_lines = 0
        for line in report_lines:
            if "| Symbol |" in line:
                in_table = True
                print(f"   {line}")
                table_lines = 0
            elif in_table and line.startswith("|"):
                print(f"   {line}")
                table_lines += 1
                if table_lines >= 10:  # Limit preview
                    if table_lines < len([l for l in report_lines if l.startswith("| ") and "Symbol" not in l]):
                        print(f"   ... and {len([l for l in report_lines if l.startswith('| ') and 'Symbol' not in l]) - table_lines} more stocks")
                    break
            elif in_table and not line.startswith("|"):
                break
        
        # Demonstrate future date simulation
        print(f"\n🔮 Simulating Future Updates...")
        print("   (This would normally be run by the daily CronJob)")
        
        # Simulate update 1 week later
        future_date = date.today() + timedelta(days=7)
        print(f"\n📅 Simulating update for {future_date}...")
        
        future_summary = await universe.run_daily_update(future_date)
        
        if future_summary['removed']:
            print(f"   ❌ Stocks removed after grace period:")
            for removal in future_summary['removed']:
                print(f"     - {removal['symbol']}: {removal['reason']}")
        else:
            print(f"   ℹ️ No stocks removed (grace periods still active)")
        
        print(f"\n🎯 Key Features Demonstrated:")
        print(f"   ✅ Automatic stock addition based on criteria")
        print(f"   ⚠️ Warning system with grace period")
        print(f"   ❌ Automatic removal after grace period")
        print(f"   📊 Comprehensive tracking and reporting")
        print(f"   📈 Real-time metrics updates")
        
        print(f"\n🚀 Production Deployment:")
        print(f"   • Daily CronJob at 6 AM UTC (after market close)")
        print(f"   • Kubernetes-native with volume mounts")
        print(f"   • Automatic database tracking and history")
        print(f"   • Monitoring and alerting ready")
        
        await universe.close()
        print(f"\n✅ Demo completed successfully!")
        
        return 0
        
    except Exception as e:
        print(f"\n❌ Demo failed: {e}")
        import traceback
        traceback.print_exc()
        return 1


async def test_criteria_compliance():
    """Test specific criteria compliance scenarios"""
    print("\n🧪 Testing Criteria Compliance Scenarios")
    print("=" * 50)
    
    env = Environment()
    universe = DynamicModelingUniverse(env)
    await universe.initialize()
    
    try:
        # Get current qualifying stocks
        print("📊 Analyzing current market data...")
        qualifying_metrics = await universe._get_qualifying_stocks(date.today())
        
        print(f"Found {len(qualifying_metrics)} stocks with recent data")
        
        # Analyze qualification breakdown
        qualifying = [m for m in qualifying_metrics if m.qualifies]
        market_cap_only = [m for m in qualifying_metrics if m.meets_market_cap and not m.meets_volume]
        volume_only = [m for m in qualifying_metrics if m.meets_volume and not m.meets_market_cap]
        neither = [m for m in qualifying_metrics if not m.meets_market_cap and not m.meets_volume]
        
        print(f"\n📈 Qualification Breakdown:")
        print(f"   ✅ Both criteria (qualifying): {len(qualifying)}")
        print(f"   🏢 Market cap only: {len(market_cap_only)}")
        print(f"   💰 Volume only: {len(volume_only)}")
        print(f"   ❌ Neither criteria: {len(neither)}")
        
        # Show top qualifying stocks
        if qualifying:
            print(f"\n🏆 Top Qualifying Stocks:")
            top_stocks = sorted(qualifying, key=lambda x: x.avg_dollar_volume_millions, reverse=True)[:10]
            
            for stock in top_stocks:
                print(f"   • {stock.symbol:<6}: ${stock.avg_market_cap_millions:>7.0f}M cap, "
                      f"${stock.avg_dollar_volume_millions:>7.0f}M volume")
        
        # Show edge cases
        if market_cap_only:
            print(f"\n🏢 High Market Cap, Low Volume (not qualifying):")
            for stock in market_cap_only[:5]:
                print(f"   • {stock.symbol:<6}: ${stock.avg_market_cap_millions:>7.0f}M cap, "
                      f"${stock.avg_dollar_volume_millions:>7.0f}M volume")
        
        if volume_only:
            print(f"\n💰 High Volume, Low Market Cap (not qualifying):")
            for stock in volume_only[:5]:
                print(f"   • {stock.symbol:<6}: ${stock.avg_market_cap_millions:>7.0f}M cap, "
                      f"${stock.avg_dollar_volume_millions:>7.0f}M volume")
        
    finally:
        await universe.close()


async def show_deployment_instructions():
    """Show deployment instructions"""
    print("\n🚀 Kubernetes Deployment Instructions")
    print("=" * 50)
    
    instructions = """
# 1. Create ConfigMap with application code
kubectl create configmap dynamic-universe-code \\
  --from-file=src/universe/dynamic_modeling_universe.py \\
  --from-file=src/config/ \\
  -n ats-dev

# 2. Deploy the CronJob for daily updates
kubectl apply -f k8s/dynamic-modeling-universe-job.yaml

# 3. Monitor CronJob status
kubectl get cronjobs -n ats-dev
kubectl get jobs -n ats-dev -l app=dynamic-modeling-universe

# 4. Check logs from latest job
kubectl logs -n ats-dev -l app=dynamic-modeling-universe --tail=100

# 5. Run manual update (for testing)
kubectl create job --from=cronjob/dynamic-modeling-universe-daily \\
  manual-universe-update-$(date +%Y%m%d) -n ats-dev

# 6. View universe reports (if persistent volume configured)
kubectl exec -it deployment/app-deployment -n ats-dev -- ls -la universe_*.md

# 7. Monitor universe health
kubectl describe cronjob dynamic-modeling-universe-daily -n ats-dev
    """
    
    print(instructions)
    
    print("🔧 Key Configuration:")
    print("   • Runs daily at 6 AM UTC (after US market close)")
    print("   • 52-day lookback for market cap and volume averages")
    print("   • 7-day grace period before removal")
    print("   • 1-year re-entry restriction")
    print("   • Automatic tracking in database")
    
    print("\n📊 Database Tables Created:")
    print("   • dev_universe - Universe definitions")
    print("   • dev_universe_membership - Current memberships")  
    print("   • dev_universe_tracking - Full history with warnings/removals")


async def main():
    """Main demo function"""
    print("🌟 Welcome to the Dynamic Modeling Universe System!")
    print("   This system automatically manages a trading universe based on:")
    print("   • Market capitalization > $400M (52-day average)")
    print("   • Trading volume > $100M daily (52-day average)")
    print("   • Automatic entry, warning, and removal workflows")
    print("   • 1-year re-entry restriction after removal")
    
    try:
        # Run main demo
        exit_code = await run_universe_demo()
        
        # Test criteria compliance
        await test_criteria_compliance()
        
        # Show deployment instructions
        await show_deployment_instructions()
        
        return exit_code
        
    except KeyboardInterrupt:
        print("\n\n⏹️ Demo interrupted by user")
        return 0
    except Exception as e:
        print(f"\n❌ Demo error: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)