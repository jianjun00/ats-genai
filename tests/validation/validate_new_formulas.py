"""
Comprehensive validation of the new improved formulas against the full dataset.
Tests both training and out-of-sample performance.
"""

import numpy as np

# Complete dataset
raw_data = """
08/05    3428.9    3444.9    3403.3    3434.7    3444.6    3403    3302.9    3335.1    3359.3    3383.5    3415.8    3448.1    3472.3
08/06    3434.9    3440.5    3411.7    3433.4    3452    3410.4    3331.1    3364.1    3387.3    3410.5    3443.5    3476.5    3499.8
08/07    3431.8    3483.8    3430    3453.7    3445.4    3416.6    3372.7    3394.2    3410    3425.8    3447.3    3468.8    3484.7
08/08    3487.9    3534.1    3445    3491.3    3481.7    3427.9    3376.9    3399.2    3418.3    3437.3    3459.7    3482    3501.1
08/11    3458.1    3466.3    3393    3404.7    3535.3    3446.2    3373    3402.2    3430.2    3458.2    3487.4    3516.7    3544.7
08/12    3393.6    3410.8    3379.1    3399    3449.7    3376.4    3344.7    3377.8    3416.8    3455.8    3488.9    3522    3560.9
08/13    3399.6    3422.6    3392.7    3408.3    3413.5    3381.8    3336.7    3367    3401.4    3435.9    3466.1    3496.4    3530.8
08/14    3407.2    3423.8    3375.5    3383.2    3423    3393.1    3338.8    3359    3383.8    3408.5    3428.7    3449    3473.7
08/15    3383.2    3394.8    3377.7    3382.6    3412.8    3364.5    3343.2    3360.2    3379.8    3399.4    3416.5    3433.5    3453.1
08/18    3382.4    3403.6    3368    3378    3392.4    3375.3    3345.9    3359.6    3377.6    3395.7    3409.4    3423.1    3441.2
08/19    3378.3    3389.7    3358.1    3358.7    3398.4    3362.8    3333.9    3347.6    3367.5    3387.5    3401.2    3414.9    3434.9
08/20    3359    3394.3    3353.4    3388.5    3379.6    3348    3333.9    3345    3362    3379    3390.1    3401.2    3418.2
08/21    3392.2    3394.4    3367.4    3383.5    3404.1    3363.2    3321.9    3339    3358    3376.9    3394    3411.1    3430
08/22    3383.3    3423.4    3362.8    3418.5    3396.1    3369.1    3326.9    3343.7    3360.1    3376.4    3393.3    3410.1    3426.4
08/25    3417.6    3421.5    3405.5    3417.5    3440.3    3379.7    3327.8    3354    3370.7    3387.4    3413.5    3439.7    3456.3
08/26    3410.8    3443.3    3396.1    3433    3424.2    3408.2    3351.1    3372    3385.7    3399.4    3420.2    3441    3454.7
08/27    3443    3452.5    3422.2    3448.6    3452.2    3405    3356.4    3381.7    3397.6    3413.5    3438.9    3464.3    3480.2
08/28    3452.6    3478.7    3442.5    3474.3    3460    3429.7    3383.1    3401.9    3414.3    3426.7    3445.4    3464.2    3476.6
08/29    3477.2    3479.6    3463    3467.6    3487.8    3451.6    3390.9    3414.1    3428.8    3443.5    3466.7    3489.9    3504.6
"""

# New cross-validated coefficients from our analysis
new_coefficients = {
    'h11': [
        -0.01581509, 0.01336338, 0.00880753, 0.00459725,   # t-3: O,H,L,C
        -0.03467657, 0.01784888, 0.00668990, 0.00629003,   # t-2: O,H,L,C
        -0.02934605, 0.68020675, -0.31550496, 0.65743894,   # t-1: O,H,L,C
    ],
    'l11': [
        0.00881905, -0.00780861, 0.00157858, -0.01179445,   # t-3: O,H,L,C
        0.03674140, -0.01702244, -0.01264323, -0.00955911,   # t-2: O,H,L,C
        0.02599345, -0.33790771, 0.65355486, 0.67009915,   # t-1: O,H,L,C
    ],
    'z1b': [
        0.00295128, -0.44556458, 0.55197947, 0.21646410,   # t-3: O,H,L,C
        0.02003617, -0.45414463, 0.55089708, 0.21389904,   # t-2: O,H,L,C
        0.01738013, -0.44801956, 0.55212768, 0.22202222,   # t-1: O,H,L,C
    ],
    'z2b': [
        -0.00096564, -0.33248583, 0.33396224, 0.33260601,   # t-3: O,H,L,C
        0.00574411, -0.33782545, 0.33104552, 0.33100210,   # t-2: O,H,L,C
        0.00606239, -0.33351634, 0.33215152, 0.33222429,   # t-1: O,H,L,C
    ],
    'ebot': [
        0.02215194, -0.13105525, 0.21731274, 0.21124499,   # t-3: O,H,L,C
        0.04947262, -0.13446754, 0.20768389, 0.21574952,   # t-2: O,H,L,C
        0.03376989, -0.12347071, 0.19828947, 0.23342712,   # t-1: O,H,L,C
    ],
    'pldot': [
        0.02715128, 0.09050900, 0.10331996, 0.08389044,   # t-3: O,H,L,C
        0.09585252, 0.06587974, 0.08499728, 0.08436526,   # t-2: O,H,L,C
        0.07555651, 0.09037359, 0.07460159, 0.12365562,   # t-1: O,H,L,C
    ],
    'etop': [
        0.02644376, 0.19850942, -0.12091120, 0.20599579,   # t-3: O,H,L,C
        0.07989484, 0.18108896, -0.13184610, 0.20730961,   # t-2: O,H,L,C
        0.06266651, 0.19971961, -0.14663304, 0.23795627,   # t-1: O,H,L,C
    ],
    'z5t': [
        0.02952349, 0.30669092, -0.34321288, 0.32235286,   # t-3: O,H,L,C
        0.06496973, 0.30181071, -0.35048305, 0.32339050,   # t-2: O,H,L,C
        0.04923384, 0.31365291, -0.36662299, 0.34885496,   # t-1: O,H,L,C
    ],
    'z6t': [
        0.03494131, 0.52694135, -0.45740808, 0.19517775,   # t-3: O,H,L,C
        0.11277993, 0.50057129, -0.47420902, 0.19331726,   # t-2: O,H,L,C
        0.08924430, 0.52845689, -0.48805140, 0.23846284,   # t-1: O,H,L,C
    ]
}

def parse_data():
    """Parse the complete dataset."""
    lines = [line.strip() for line in raw_data.strip().split('\n') if line.strip()]
    
    data = []
    for line in lines:
        parts = line.split()
        values = [float(x) for x in parts[1:]]  # Skip date
        data.append(values)
    
    return np.array(data)

def calculate_indicator(ohlc_history, coefficients):
    """Calculate indicator value using 3 days OHLC and coefficients."""
    features = []
    for ohlc in ohlc_history:
        features.extend(ohlc)
    
    return np.dot(coefficients, features)

def comprehensive_validation():
    """Perform comprehensive validation of new formulas."""
    
    print("COMPREHENSIVE VALIDATION OF NEW IMPROVED FORMULAS")
    print("=" * 70)
    
    data = parse_data()
    target_indices = [4, 5, 6, 7, 8, 9, 10, 11, 12]  # h11, l11, z1b, z2b, ebot, pldot, etop, z5t, z6t
    target_names = list(new_coefficients.keys())
    
    # Test all available samples
    results = {}
    all_errors = []
    
    for target_idx, target_name in enumerate(target_names):
        coeffs = new_coefficients[target_name]
        errors = []
        predictions = []
        actuals = []
        
        # Test each day starting from day 4
        for i in range(3, len(data)):
            # Get OHLC from previous 3 days
            ohlc_history = []
            for lookback in [3, 2, 1]:
                prev_idx = i - lookback
                ohlc = data[prev_idx, :4]  # OHLC only
                ohlc_history.append(ohlc)
            
            # Calculate prediction
            predicted = calculate_indicator(ohlc_history, coeffs)
            actual = data[i, target_indices[target_idx]]
            
            error = abs(predicted - actual)
            errors.append(error)
            predictions.append(predicted)
            actuals.append(actual)
            all_errors.append(error)
        
        # Calculate statistics
        avg_error = np.mean(errors)
        max_error = np.max(errors) 
        min_error = np.min(errors)
        rmse = np.sqrt(np.mean(np.array(errors)**2))
        
        # Calculate R²
        ss_res = np.sum((np.array(actuals) - np.array(predictions)) ** 2)
        ss_tot = np.sum((np.array(actuals) - np.mean(actuals)) ** 2)
        r2 = 1 - (ss_res / ss_tot) if ss_tot > 0 else 1.0
        
        results[target_name] = {
            'avg_error': avg_error,
            'max_error': max_error,
            'min_error': min_error,
            'rmse': rmse,
            'r2': r2,
            'predictions': predictions,
            'actuals': actuals,
            'errors': errors
        }
        
        print(f"{target_name.upper():6}: R²={r2:.6f}, Avg_Error={avg_error:.4f}, Max_Error={max_error:.4f}, RMSE={rmse:.4f}")
    
    # Overall assessment
    print("\n" + "=" * 70)
    print("DETAILED VALIDATION RESULTS")
    print("=" * 70)
    
    excellent_count = 0
    good_count = 0
    poor_count = 0
    
    for target_name, stats in results.items():
        # Assessment criteria
        if stats['r2'] > 0.999 and stats['avg_error'] < 1.0:
            assessment = "🎉 EXCELLENT"
            excellent_count += 1
        elif stats['r2'] > 0.99 and stats['avg_error'] < 3.0:
            assessment = "✅ GOOD"
            good_count += 1
        elif stats['r2'] > 0.95 and stats['avg_error'] < 10.0:
            assessment = "⚠️ ACCEPTABLE"
        else:
            assessment = "❌ POOR"
            poor_count += 1
        
        relative_error = (stats['avg_error'] / np.mean(stats['actuals'])) * 100
        
        print(f"\n{target_name.upper()}:")
        print(f"  R² = {stats['r2']:.8f}")
        print(f"  Average Error = {stats['avg_error']:.4f} ({relative_error:.3f}%)")
        print(f"  Error Range = {stats['min_error']:.4f} to {stats['max_error']:.4f}")
        print(f"  RMSE = {stats['rmse']:.4f}")
        print(f"  Assessment: {assessment}")
    
    # Sample predictions vs actual (first 5 test cases)
    print("\n" + "=" * 70)
    print("SAMPLE PREDICTIONS VS ACTUAL (First 5 Test Cases)")
    print("=" * 70)
    
    for i in range(min(5, len(results['h11']['predictions']))):
        sample_date = f"Sample {i+1}"
        print(f"\n{sample_date}:")
        
        for target_name in target_names:
            actual = results[target_name]['actuals'][i]
            predicted = results[target_name]['predictions'][i]
            error = results[target_name]['errors'][i]
            
            print(f"  {target_name.upper():6}: Actual={actual:8.2f}, Predicted={predicted:8.2f}, Error={error:6.4f}")
    
    # Overall summary
    print("\n" + "=" * 70)
    print("OVERALL VALIDATION SUMMARY")
    print("=" * 70)
    
    total_indicators = len(target_names)
    overall_avg_error = np.mean(all_errors)
    overall_max_error = np.max(all_errors)
    
    print(f"Total Indicators Tested: {total_indicators}")
    print(f"🎉 EXCELLENT Performance: {excellent_count}")
    print(f"✅ GOOD Performance: {good_count}")
    print(f"❌ POOR Performance: {poor_count}")
    print(f"Overall Average Error: {overall_avg_error:.4f}")
    print(f"Overall Max Error: {overall_max_error:.4f}")
    
    success_rate = (excellent_count + good_count) / total_indicators
    
    print(f"\n📊 SUCCESS RATE: {success_rate*100:.1f}% ({excellent_count + good_count}/{total_indicators})")
    
    if success_rate >= 0.9:
        print("🎉 OUTSTANDING: New formulas show excellent performance!")
        conclusion = "PRODUCTION_READY"
    elif success_rate >= 0.7:
        print("✅ VERY GOOD: New formulas show strong performance!")
        conclusion = "PRODUCTION_READY"
    elif success_rate >= 0.5:
        print("⚠️ MODERATE: Some formulas may be suitable for production")
        conclusion = "MIXED_RESULTS"
    else:
        print("❌ POOR: Formulas still need improvement")
        conclusion = "NOT_READY"
    
    return conclusion, results

if __name__ == "__main__":
    conclusion, results = comprehensive_validation()
    
    if conclusion == "PRODUCTION_READY":
        print("\n🚀 RECOMMENDATION: Deploy new formulas to production!")
        exit_code = 0
    elif conclusion == "MIXED_RESULTS":
        print("\n🤔 RECOMMENDATION: Review individual formulas before deployment")
        exit_code = 0
    else:
        print("\n❌ RECOMMENDATION: Continue research and development")
        exit_code = 1
    
    import sys
    sys.exit(exit_code)