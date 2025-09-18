#!/usr/bin/env python3
"""
Generate multi-timeframe training data for AAPL using the enhanced framework.
"""

import asyncio
import sys

# Add src to path
sys.path.append('src')

from src.domains.ml.services.training_data.runners.training_data_callback_runner import run_hourly_training_data_job_for_symbol

async def main():
    """Generate multi-timeframe training data for AAPL."""
    print("🚀 Generating multi-timeframe training data for AAPL")
    print("Using enhanced framework with universe state builder integration")
    print("")

    try:
        # Generate hourly training data with multi-timeframe features
        # Use August 2024 data since that's when we have AAPL minute data available
        results = await run_hourly_training_data_job_for_symbol(
            symbol='AAPL',
            output_dir="auto",  # Auto-generate based on environment
            days_back=500  # Go back to August 2024 timeframe
        )

        print("\n" + "=" * 60)
        print("📊 TRAINING DATA GENERATION RESULTS")
        print("=" * 60)

        print(f"Status: {results['status']}")
        print(f"Run ID: {results['run_id']}")
        print(f"Dataset IDs: {results['dataset_ids']}")

        if results['status'] == 'success':
            training_results = results['results']['training_results']
            print(f"\n✅ SUCCESS!")
            print(f"Features shape: {training_results['features_shape']}")
            print(f"Feature count: {len(training_results['feature_names'])}")

            # Show multi-timeframe features
            feature_names = training_results['feature_names']
            mtf_features = [f for f in feature_names if any(tf in f for tf in ['5m_', '15m_', '1h_', '1d_'])]

            if mtf_features:
                print(f"\n🎯 Multi-timeframe features: {len(mtf_features)}")
                print("Sample features:")
                for i, feature in enumerate(mtf_features[:10]):
                    print(f"  {i+1}. {feature}")
                if len(mtf_features) > 10:
                    print(f"  ... and {len(mtf_features) - 10} more")

                # Count by timeframe
                timeframe_counts = {}
                for tf in ['5m', '15m', '1h', '1d']:
                    count = len([f for f in mtf_features if f.startswith(f'{tf}_')])
                    if count > 0:
                        timeframe_counts[tf] = count

                print(f"\nTimeframe breakdown:")
                for tf, count in timeframe_counts.items():
                    print(f"  {tf}: {count} features")

            else:
                print("⚠️ No multi-timeframe features found")

        else:
            print(f"❌ FAILED: {results.get('error', 'Unknown error')}")

        return results

    except Exception as e:
        print(f"💥 Error: {e}")
        import traceback
        traceback.print_exc()
        return {"status": "failed", "error": str(e)}

if __name__ == "__main__":
    results = asyncio.run(main())
    if results['status'] == 'success':
        print("\n🎉 AAPL multi-timeframe training data generation completed successfully!")
    else:
        print("\n💥 Training data generation failed!")
        sys.exit(1)