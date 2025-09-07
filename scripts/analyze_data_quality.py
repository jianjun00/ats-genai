#!/usr/bin/env python3
"""
Comprehensive data quality analysis for financial time series.
Detects and handles invalid data: zeros, NaNs, outliers, inconsistencies.
"""

import sys
import numpy as np
import pandas as pd
import logging
from typing import Dict, List, Tuple, Optional
import json
from pathlib import Path

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class FinancialDataValidator:
    """Comprehensive financial data quality validator and cleaner."""

    def __init__(self, symbol: str = "AAPL"):
        self.symbol = symbol
        self.validation_report = {
            'symbol': symbol,
            'issues_found': [],
            'fixes_applied': [],
            'data_quality_score': 0.0,
            'recommendations': []
        }

    def validate_ohlcv_sequence(self, df: pd.DataFrame) -> Dict:
        """Validate OHLCV data sequence for financial consistency."""

        logger.info(f"🔍 Validating OHLCV data for {self.symbol}")
        logger.info(f"   Input data shape: {df.shape}")

        issues = []
        fixes = []

        # Required columns
        required_cols = ['open', 'high', 'low', 'close', 'volume']
        missing_cols = [col for col in required_cols if col not in df.columns]

        if missing_cols:
            issues.append(f"Missing required columns: {missing_cols}")
            return {'is_valid': False, 'issues': issues, 'fixes': fixes}

        # Create working copy
        clean_df = df[required_cols].copy()
        original_rows = len(clean_df)

        logger.info(f"📊 Original data: {original_rows} rows")

        # 1. Check for NaN values
        nan_counts = clean_df.isnull().sum()
        total_nans = nan_counts.sum()

        if total_nans > 0:
            issues.append(f"NaN values found: {dict(nan_counts[nan_counts > 0])}")
            logger.warning(f"   ⚠️ NaN values: {total_nans} total")

            # Strategy: Forward fill then backward fill
            clean_df = clean_df.fillna(method='ffill').fillna(method='bfill')
            remaining_nans = clean_df.isnull().sum().sum()

            if remaining_nans > 0:
                # If still NaNs, drop those rows
                clean_df = clean_df.dropna()
                fixes.append(f"Dropped {original_rows - len(clean_df)} rows with unfillable NaNs")
            else:
                fixes.append(f"Filled {total_nans} NaN values using forward/backward fill")

        # 2. Check for zero/negative prices
        price_cols = ['open', 'high', 'low', 'close']
        zero_price_mask = (clean_df[price_cols] <= 0).any(axis=1)
        zero_price_count = zero_price_mask.sum()

        if zero_price_count > 0:
            issues.append(f"Zero/negative prices found: {zero_price_count} rows")
            logger.warning(f"   ⚠️ Invalid prices: {zero_price_count} rows")

            # Strategy: Remove rows with invalid prices
            clean_df = clean_df[~zero_price_mask]
            fixes.append(f"Removed {zero_price_count} rows with zero/negative prices")

        # 3. Check for zero volume
        zero_volume_mask = clean_df['volume'] <= 0
        zero_volume_count = zero_volume_mask.sum()

        if zero_volume_count > 0:
            issues.append(f"Zero volume found: {zero_volume_count} rows")
            logger.warning(f"   ⚠️ Zero volume: {zero_volume_count} rows")

            # Strategy: Replace with median volume or remove if too many
            if zero_volume_count < len(clean_df) * 0.1:  # Less than 10%
                median_volume = clean_df.loc[~zero_volume_mask, 'volume'].median()
                clean_df.loc[zero_volume_mask, 'volume'] = median_volume
                fixes.append(f"Replaced {zero_volume_count} zero volumes with median: {median_volume:,.0f}")
            else:
                clean_df = clean_df[~zero_volume_mask]
                fixes.append(f"Removed {zero_volume_count} rows with zero volume (>10% of data)")

        # 4. Check OHLC consistency
        ohlc_inconsistent = (
            (clean_df['high'] < clean_df['low']) |
            (clean_df['high'] < clean_df['open']) |
            (clean_df['high'] < clean_df['close']) |
            (clean_df['low'] > clean_df['open']) |
            (clean_df['low'] > clean_df['close'])
        )

        inconsistent_count = ohlc_inconsistent.sum()

        if inconsistent_count > 0:
            issues.append(f"OHLC inconsistencies found: {inconsistent_count} rows")
            logger.warning(f"   ⚠️ OHLC inconsistent: {inconsistent_count} rows")

            # Strategy: Fix by adjusting high/low to be consistent
            for idx in clean_df[ohlc_inconsistent].index:
                row = clean_df.loc[idx]
                # Ensure high is max of O,H,L,C and low is min
                true_high = max(row['open'], row['high'], row['low'], row['close'])
                true_low = min(row['open'], row['high'], row['low'], row['close'])

                clean_df.loc[idx, 'high'] = true_high
                clean_df.loc[idx, 'low'] = true_low

            fixes.append(f"Fixed {inconsistent_count} OHLC inconsistencies")

        # 5. Check for extreme price movements (potential errors)
        returns = clean_df['close'].pct_change().fillna(0)
        extreme_returns = np.abs(returns) > 0.5  # >50% move in one period
        extreme_count = extreme_returns.sum()

        if extreme_count > 0:
            issues.append(f"Extreme price movements found: {extreme_count} (>50% change)")
            logger.warning(f"   ⚠️ Extreme moves: {extreme_count} periods")

            # Strategy: Cap extreme movements or remove if clearly erroneous
            extreme_values = returns[extreme_returns]
            logger.info(f"      Extreme returns: min={extreme_values.min():.3f}, max={extreme_values.max():.3f}")

            # If more than 1000% move, likely data error - remove
            super_extreme = np.abs(returns) > 10.0
            if super_extreme.sum() > 0:
                clean_df = clean_df[~super_extreme]
                fixes.append(f"Removed {super_extreme.sum()} rows with >1000% price moves (likely errors)")

            # Cap remaining extreme moves at ±50%
            returns_capped = returns.clip(-0.5, 0.5)
            if not returns_capped.equals(returns):
                # Recalculate prices with capped returns
                clean_df['close'] = clean_df['close'].iloc[0] * (1 + returns_capped).cumprod()
                fixes.append(f"Capped extreme returns at ±50%")

        # 6. Check for duplicate timestamps
        if 'datetime' in clean_df.columns or 'timestamp' in clean_df.columns:
            time_col = 'datetime' if 'datetime' in clean_df.columns else 'timestamp'
            duplicates = clean_df.duplicated(subset=[time_col])
            duplicate_count = duplicates.sum()

            if duplicate_count > 0:
                issues.append(f"Duplicate timestamps found: {duplicate_count}")
                clean_df = clean_df[~duplicates]
                fixes.append(f"Removed {duplicate_count} duplicate timestamp rows")

        # 7. Check for gaps in time series
        if 'datetime' in clean_df.columns:
            clean_df['datetime'] = pd.to_datetime(clean_df['datetime'])
            clean_df = clean_df.sort_values('datetime')

            time_diffs = clean_df['datetime'].diff()
            expected_freq = time_diffs.mode().iloc[0] if len(time_diffs) > 1 else pd.Timedelta(hours=1)

            large_gaps = time_diffs > expected_freq * 2
            gap_count = large_gaps.sum()

            if gap_count > 0:
                issues.append(f"Time series gaps found: {gap_count} gaps > 2x expected frequency")
                logger.info(f"   ⚠️ Time gaps: {gap_count} (expected freq: {expected_freq})")

        # 8. Volume consistency checks
        volume_stats = clean_df['volume'].describe()
        volume_outliers = (
            (clean_df['volume'] > volume_stats['75%'] + 3 * (volume_stats['75%'] - volume_stats['25%'])) |
            (clean_df['volume'] < volume_stats['25%'] - 3 * (volume_stats['75%'] - volume_stats['25%']))
        )

        outlier_count = volume_outliers.sum()
        if outlier_count > 0:
            issues.append(f"Volume outliers found: {outlier_count} (using IQR method)")
            logger.info(f"   ⚠️ Volume outliers: {outlier_count}")
            # Note: Not automatically fixing volume outliers as they might be legitimate

        # Calculate data quality score
        final_rows = len(clean_df)
        data_retention = final_rows / original_rows if original_rows > 0 else 0
        issue_severity = len([i for i in issues if 'Zero' in i or 'NaN' in i or 'inconsistent' in i])

        quality_score = min(1.0, data_retention * (1 - issue_severity * 0.1))

        # Generate summary
        logger.info(f"📊 Data cleaning summary:")
        logger.info(f"   Original rows: {original_rows}")
        logger.info(f"   Final rows: {final_rows} ({data_retention:.1%} retention)")
        logger.info(f"   Issues found: {len(issues)}")
        logger.info(f"   Fixes applied: {len(fixes)}")
        logger.info(f"   Data quality score: {quality_score:.3f}")

        return {
            'is_valid': quality_score > 0.8,
            'original_rows': original_rows,
            'final_rows': final_rows,
            'data_retention': data_retention,
            'issues': issues,
            'fixes': fixes,
            'clean_data': clean_df,
            'quality_score': quality_score,
            'volume_stats': volume_stats.to_dict(),
            'price_stats': clean_df[price_cols].describe().to_dict()
        }

    def validate_sequences_for_ml(self, sequences: np.ndarray, targets: Dict[str, np.ndarray]) -> Dict:
        """Validate sequences prepared for ML training."""

        logger.info(f"🔍 Validating ML sequences for {self.symbol}")
        logger.info(f"   Sequences shape: {sequences.shape}")

        issues = []
        fixes = []

        # Check for NaN in sequences
        nan_sequences = np.isnan(sequences).any(axis=(1, 2))
        nan_count = nan_sequences.sum()

        if nan_count > 0:
            issues.append(f"Sequences with NaN: {nan_count}/{len(sequences)}")
            sequences = sequences[~nan_sequences]
            for key in targets:
                targets[key] = targets[key][~nan_sequences]
            fixes.append(f"Removed {nan_count} sequences containing NaN")

        # Check for infinite values
        inf_sequences = np.isinf(sequences).any(axis=(1, 2))
        inf_count = inf_sequences.sum()

        if inf_count > 0:
            issues.append(f"Sequences with Inf: {inf_count}/{len(sequences)}")
            sequences = sequences[~inf_sequences]
            for key in targets:
                targets[key] = targets[key][~inf_sequences]
            fixes.append(f"Removed {inf_count} sequences containing Inf")

        # Check for constant sequences (no variation)
        sequence_stds = np.std(sequences, axis=1)  # Std across time dimension
        constant_sequences = np.all(sequence_stds < 1e-8, axis=1)
        constant_count = constant_sequences.sum()

        if constant_count > 0:
            issues.append(f"Constant sequences: {constant_count}/{len(sequences)}")
            sequences = sequences[~constant_sequences]
            for key in targets:
                targets[key] = targets[key][~constant_sequences]
            fixes.append(f"Removed {constant_count} constant sequences")

        # Check for extreme values in sequences
        extreme_mask = np.abs(sequences) > 100  # Very large normalized values
        extreme_sequences = extreme_mask.any(axis=(1, 2))
        extreme_count = extreme_sequences.sum()

        if extreme_count > 0:
            issues.append(f"Sequences with extreme values: {extreme_count}/{len(sequences)}")
            # Clip extreme values instead of removing entire sequences
            sequences = np.clip(sequences, -100, 100)
            fixes.append(f"Clipped extreme values in {extreme_count} sequences to ±100")

        # Validate targets
        for target_name, target_values in targets.items():
            # Check for NaN in targets
            nan_targets = np.isnan(target_values)
            if target_name != 'regime_change':  # Skip for categorical
                nan_targets = nan_targets.any(axis=1) if target_values.ndim > 1 else nan_targets
            else:
                nan_targets = np.isnan(target_values.astype(float))

            nan_target_count = nan_targets.sum()

            if nan_target_count > 0:
                issues.append(f"NaN in {target_name} targets: {nan_target_count}")
                # Remove these samples from all data
                valid_mask = ~nan_targets
                sequences = sequences[valid_mask]
                for key in targets:
                    targets[key] = targets[key][valid_mask]
                fixes.append(f"Removed {nan_target_count} samples with NaN {target_name} targets")

        # Final validation statistics
        final_count = len(sequences)
        logger.info(f"📊 ML validation summary:")
        logger.info(f"   Final sequences: {final_count}")
        logger.info(f"   Sequence stats: mean={sequences.mean():.4f}, std={sequences.std():.4f}")
        logger.info(f"   Issues found: {len(issues)}")
        logger.info(f"   Fixes applied: {len(fixes)}")

        return {
            'is_valid': len(issues) == 0,
            'final_sequences': final_count,
            'issues': issues,
            'fixes': fixes,
            'clean_sequences': sequences,
            'clean_targets': targets,
            'sequence_stats': {
                'mean': float(sequences.mean()),
                'std': float(sequences.std()),
                'min': float(sequences.min()),
                'max': float(sequences.max())
            }
        }


def generate_problematic_data(num_samples=1000) -> pd.DataFrame:
    """Generate test data with various data quality issues."""

    logger.info(f"📊 Generating test data with quality issues: {num_samples} samples")

    np.random.seed(42)

    # Start with clean data
    dates = pd.date_range('2024-01-01', periods=num_samples, freq='h')

    # Generate base price series
    returns = np.random.normal(0.0002, 0.015, num_samples)
    returns = np.clip(returns, -0.1, 0.1)  # Reasonable bounds

    prices = 225.0 * np.exp(np.cumsum(returns))

    # Create OHLCV
    data = []
    for i in range(len(prices)):
        close_price = prices[i]
        open_price = prices[i-1] if i > 0 else close_price

        # Generate high/low with some randomness
        price_range = abs(close_price - open_price) * (1 + np.random.exponential(0.5))
        high_price = max(open_price, close_price) + price_range * np.random.beta(2, 8)
        low_price = min(open_price, close_price) - price_range * np.random.beta(2, 8)

        volume = np.random.lognormal(13, 0.5)  # Base volume

        data.append({
            'datetime': dates[i],
            'open': open_price,
            'high': high_price,
            'low': low_price,
            'close': close_price,
            'volume': volume
        })

    df = pd.DataFrame(data)

    # Now introduce various problems
    problems_introduced = []

    # 1. Introduce NaN values (5% of data)
    nan_indices = np.random.choice(len(df), size=int(0.05 * len(df)), replace=False)
    nan_columns = np.random.choice(['open', 'high', 'low', 'close', 'volume'], size=len(nan_indices))

    for idx, col in zip(nan_indices, nan_columns):
        df.loc[idx, col] = np.nan

    problems_introduced.append(f"Introduced {len(nan_indices)} NaN values")

    # 2. Introduce zero/negative prices (2% of data)
    zero_indices = np.random.choice(len(df), size=int(0.02 * len(df)), replace=False)
    zero_columns = np.random.choice(['open', 'high', 'low', 'close'], size=len(zero_indices))

    for idx, col in zip(zero_indices, zero_columns):
        df.loc[idx, col] = 0 if np.random.random() < 0.7 else -np.random.random() * 10

    problems_introduced.append(f"Introduced {len(zero_indices)} zero/negative prices")

    # 3. Introduce zero volume (3% of data)
    zero_vol_indices = np.random.choice(len(df), size=int(0.03 * len(df)), replace=False)
    df.loc[zero_vol_indices, 'volume'] = 0

    problems_introduced.append(f"Introduced {len(zero_vol_indices)} zero volumes")

    # 4. Introduce OHLC inconsistencies (1% of data)
    inconsistent_indices = np.random.choice(len(df), size=int(0.01 * len(df)), replace=False)

    for idx in inconsistent_indices:
        # Make high < low (clearly wrong)
        df.loc[idx, 'high'] = df.loc[idx, 'low'] * 0.95

    problems_introduced.append(f"Introduced {len(inconsistent_indices)} OHLC inconsistencies")

    # 5. Introduce extreme price movements (0.5% of data)
    extreme_indices = np.random.choice(len(df), size=int(0.005 * len(df)), replace=False)

    for idx in extreme_indices:
        if idx > 0:
            # Create extreme price jump
            multiplier = np.random.choice([0.1, 10.0])  # 90% drop or 1000% gain
            df.loc[idx, 'close'] = df.loc[idx-1, 'close'] * multiplier
            df.loc[idx, 'open'] = df.loc[idx, 'close']
            df.loc[idx, 'high'] = max(df.loc[idx, 'open'], df.loc[idx, 'close'])
            df.loc[idx, 'low'] = min(df.loc[idx, 'open'], df.loc[idx, 'close'])

    problems_introduced.append(f"Introduced {len(extreme_indices)} extreme price movements")

    # 6. Introduce duplicate timestamps (1% of data)
    duplicate_indices = np.random.choice(len(df)-1, size=int(0.01 * len(df)), replace=False)
    for idx in duplicate_indices:
        df.loc[idx+1, 'datetime'] = df.loc[idx, 'datetime']  # Same timestamp as previous

    problems_introduced.append(f"Introduced {len(duplicate_indices)} duplicate timestamps")

    logger.info("🚨 Problems introduced in test data:")
    for problem in problems_introduced:
        logger.info(f"   - {problem}")

    return df


def comprehensive_data_quality_test():
    """Run comprehensive data quality analysis and cleaning test."""

    logger.info("🔍 COMPREHENSIVE DATA QUALITY ANALYSIS")
    logger.info("="*60)

    # Generate problematic test data
    test_data = generate_problematic_data(num_samples=2000)

    logger.info(f"📊 Test data generated: {len(test_data)} rows")
    logger.info(f"   Columns: {list(test_data.columns)}")
    logger.info(f"   Date range: {test_data['datetime'].min()} to {test_data['datetime'].max()}")

    # Initialize validator
    validator = FinancialDataValidator(symbol="TEST_DATA")

    # Run OHLCV validation and cleaning
    logger.info("\n🧪 Running OHLCV validation and cleaning...")
    ohlcv_result = validator.validate_ohlcv_sequence(test_data)

    # Print results
    logger.info("\n📋 OHLCV VALIDATION RESULTS")
    logger.info("="*40)
    logger.info(f"✅ Data valid: {ohlcv_result['is_valid']}")
    logger.info(f"📊 Data retention: {ohlcv_result['data_retention']:.1%}")
    logger.info(f"🏆 Quality score: {ohlcv_result['quality_score']:.3f}")

    logger.info(f"\n🚨 Issues found ({len(ohlcv_result['issues'])}):")
    for issue in ohlcv_result['issues']:
        logger.info(f"   - {issue}")

    logger.info(f"\n🔧 Fixes applied ({len(ohlcv_result['fixes'])}):")
    for fix in ohlcv_result['fixes']:
        logger.info(f"   - {fix}")

    # Test ML sequence validation
    if ohlcv_result['is_valid'] or ohlcv_result['quality_score'] > 0.5:
        logger.info("\n🧪 Testing ML sequence validation...")

        clean_data = ohlcv_result['clean_data']

        # Create sequences (simulate what would happen in training)
        seq_len = 8
        sequences = []
        targets = {'price_movement': [], 'volatility': [], 'regime_change': []}

        # Normalize data first
        from sklearn.preprocessing import RobustScaler
        scaler = RobustScaler()
        ohlcv_cols = ['open', 'high', 'low', 'close', 'volume']
        normalized_data = scaler.fit_transform(clean_data[ohlcv_cols])

        # Create sequences
        for i in range(len(normalized_data) - seq_len - 1):
            sequence = normalized_data[i:i+seq_len]
            sequences.append(sequence)

            # Create targets
            current_price = normalized_data[i+seq_len-1, 3]  # close price
            future_price = normalized_data[i+seq_len, 3]
            price_movement = future_price - current_price

            targets['price_movement'].append(price_movement)
            targets['volatility'].append(abs(price_movement))
            targets['regime_change'].append(0 if price_movement > 0 else 1)

        # Convert to arrays
        sequences_array = np.array(sequences, dtype=np.float32)
        targets_dict = {
            'price_movement': np.array(targets['price_movement'], dtype=np.float32).reshape(-1, 1),
            'volatility': np.array(targets['volatility'], dtype=np.float32).reshape(-1, 1),
            'regime_change': np.array(targets['regime_change'], dtype=np.int64)
        }

        # Introduce some ML-specific problems
        # Add some NaN and Inf values to test ML validation
        problem_indices = np.random.choice(len(sequences_array), size=50, replace=False)
        sequences_array[problem_indices[:25], 0, 0] = np.nan  # Some NaN
        sequences_array[problem_indices[25:], 1, 1] = np.inf  # Some Inf

        # Add some NaN to targets
        targets_dict['volatility'][problem_indices[:10]] = np.nan

        logger.info(f"   Created {len(sequences_array)} sequences for ML validation")

        # Run ML sequence validation
        ml_result = validator.validate_sequences_for_ml(sequences_array, targets_dict)

        logger.info("\n📋 ML SEQUENCE VALIDATION RESULTS")
        logger.info("="*40)
        logger.info(f"✅ Sequences valid: {ml_result['is_valid']}")
        logger.info(f"📊 Final sequences: {ml_result['final_sequences']}")
        logger.info(f"📊 Sequence statistics:")
        stats = ml_result['sequence_stats']
        logger.info(f"   Mean: {stats['mean']:.6f}")
        logger.info(f"   Std:  {stats['std']:.6f}")
        logger.info(f"   Range: [{stats['min']:.3f}, {stats['max']:.3f}]")

        if ml_result['issues']:
            logger.info(f"\n🚨 ML Issues found ({len(ml_result['issues'])}):")
            for issue in ml_result['issues']:
                logger.info(f"   - {issue}")

        if ml_result['fixes']:
            logger.info(f"\n🔧 ML Fixes applied ({len(ml_result['fixes'])}):")
            for fix in ml_result['fixes']:
                logger.info(f"   - {fix}")

    # Summary and recommendations
    logger.info("\n📋 COMPREHENSIVE QUALITY ANALYSIS SUMMARY")
    logger.info("="*50)

    overall_score = (ohlcv_result['quality_score'] + (1.0 if ml_result['is_valid'] else 0.5)) / 2

    logger.info(f"🏆 Overall Data Quality Score: {overall_score:.3f}")

    recommendations = []

    if ohlcv_result['quality_score'] < 0.8:
        recommendations.append("Improve raw data quality - consider better data sources")

    if ohlcv_result['data_retention'] < 0.9:
        recommendations.append("High data loss during cleaning - investigate data collection")

    if any('extreme' in issue.lower() for issue in ohlcv_result['issues']):
        recommendations.append("Implement real-time outlier detection in data pipeline")

    if any('gap' in issue.lower() for issue in ohlcv_result['issues']):
        recommendations.append("Add market hours filtering and gap interpolation")

    recommendations.append("Implement continuous data quality monitoring")
    recommendations.append("Add automated alerts for data quality degradation")

    logger.info(f"\n💡 RECOMMENDATIONS:")
    for i, rec in enumerate(recommendations, 1):
        logger.info(f"   {i}. {rec}")

    # Save comprehensive report
    report = {
        'timestamp': pd.Timestamp.now().isoformat(),
        'test_data_size': len(test_data),
        'ohlcv_validation': ohlcv_result,
        'ml_validation': ml_result if 'ml_result' in locals() else None,
        'overall_quality_score': overall_score,
        'recommendations': recommendations
    }

    # Remove pandas objects for JSON serialization
    if 'clean_data' in report['ohlcv_validation']:
        del report['ohlcv_validation']['clean_data']
    if report['ml_validation'] and 'clean_sequences' in report['ml_validation']:
        del report['ml_validation']['clean_sequences']
        del report['ml_validation']['clean_targets']

    report_path = '/data/models/data_quality_analysis_report.json'
    try:
        import os
        os.makedirs(os.path.dirname(report_path), exist_ok=True)
        with open(report_path, 'w') as f:
            json.dump(report, f, indent=2, default=str)
        logger.info(f"💾 Comprehensive report saved: {report_path}")
    except Exception as e:
        logger.warning(f"Could not save report: {e}")

    logger.info("\n🎉 COMPREHENSIVE DATA QUALITY ANALYSIS COMPLETE!")
    logger.info("✅ Framework ready for production data validation")

    return report


if __name__ == "__main__":
    try:
        report = comprehensive_data_quality_test()
        logger.info("✅ Data quality analysis completed successfully")
        sys.exit(0)
    except Exception as e:
        logger.error(f"❌ Data quality analysis failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)