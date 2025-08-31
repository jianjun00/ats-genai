"""
Test HLC-only formulas on validation dataset with different price range.
This tests true generalization across different price scales.
"""

import numpy as np

# Validation dataset from user (different price range ~23K-24K)
validation_data_raw = """
07/30    23689    23356.5    23480.5    23595.92    23364.67    23201.97    23279.83    23376.97    23474.11    23551.97    23629.83    23726.97
07/31    23845    23290.5    23365    23660.83    23328.33    23134.78    23237.17    23372.19    23507.22    23609.61    23712    23847.03
08/01    23347.5    22775    22883.75    23709.83    23155.33    22910.03    23059.75    23282.78    23505.81    23655.53    23805.25    24028.28
08/04    23352.5    22821.75    23296.5    23229.17    22656.67    22560.28    22756.58    23046.78    23336.97    23533.28    23729.58    24019.78
08/05    23404.25    23084.5    23132    23492.08    22961.33    22371.86    22629.17    22924.44    23219.72    23477.03    23734.33    24029.61
08/06    23445.25    23045.5    23422.75    2329.33    23009.58    22401.53    22629.75    22875.86    23121.97    23350.19    23578.42    23824.53
08/07    23671    23329    23496.25    23563.5    23163.75    22628.14    22867    23044.89    23222.78    23461.61    23700.5    23878.39
08/08    23767.75    23503    23713.75    23668.5    23326.5    22812.78    22996.5    23166.61    23336.72    23520.44    23704.17    23874.28
08/11    23804.5    23587.5    23637.5    23820    23555.25    23013    23208.75    23348.5    23488.25    23684    23879.75    24019.5
08/12    23953.5    23596    23938    23765.5    23548.5    23202.17    23341.25    23476.75    23612.25    23751.33    23890.42    24025.92
08/13    24068.5    23886    23946.5    24062.33    23704.83    23323.11    23483.33    23602.86    23722.39    23882.61    24042.83    24162.36
08/14    24007.75    23793.25    23930.5    24048    23865.5    23453.94    23588.33    23706.28    23824.22    23958.61    24093    24210.94
08/15    23963    23734.5    23804    24027.75    23813.25    23543.03    23686.83    23794.53    23902.22    24046.03    24189.83    24297.53
08/18    23881.75    23719    23797.75    23933.17    23704.67    23585.97    23685.17    23794.47    23903.78    24002.97    24102.17    24211.47
08/19    23838    23426    23469.5    23880    23717.25    23543.14    23642.17    23745.06    23847.94    23946.97    24046    24148.89
08/20    23485.5    23035    23324    23729.67    23317.67    23312.11    23422.67    23579.86    23737.06    23847.61    23958.17    24115.36
08/21    23369.25    23119    23219.75    23528    23077.5    23029.06    23188.67    23370.81    23552.95    23712.56    23872.17    24054.31
08/22    23650    23076.75    23569.75    23353    23102.75    22795.06    22966.83    23165.97    23365.11    23536.89    23708.67    23907.81
08/25    23616.25    23443.25    23498.25    23787.58    23214.33    22706.86    22946.5    23131.53    23316.56    23556.19    23795.83    23980.86
08/26    23611    23371.5    23607.5    23595.25    23422.25    22914.28    23097.08    23246.44    23395.81    23578.61    23761.42    23910.78
08/27    23689    23434.75    23628.75    23677.83    23438.33    23033.28    23229.92    23361.86    23493.81    23690.44    23887.08    24019.03
08/28    23803.75    23487.5    23762    23675.75    23421.5    23208.67    23327    23430.92    23534.83    23653.17    23771.5    23875.42
08/29    23762    23397.5    23454.5    23881.33    23565.08    23208.53    23367.17    23478.53    23589.89    23748.53    23907.17    24018.53
"""

# HLC-Only Coefficients (derived from ~3.3K-3.5K price range)
hlc_coefficients = {
    'h11': [
        -0.00056212, -0.00018272, 0.00019277,   # t-3: H,L,C
        0.00136978, 0.00071840, -0.00182454,   # t-2: H,L,C
        0.66686225, -0.33319001, 0.66661597,   # t-1: H,L,C
    ],
    'l11': [
        -0.00056212, -0.00018272, 0.00019277,   # t-3: H,L,C
        0.00136978, 0.00071840, -0.00182454,   # t-2: H,L,C
        -0.33313775, 0.66680999, 0.66661597,   # t-1: H,L,C
    ],
    'z1b': [
        -0.44360641, 0.55203953, 0.22238203,   # t-3: H,L,C
        -0.44299760, 0.55722853, 0.21953681,   # t-2: H,L,C
        -0.44414226, 0.55962966, 0.21992682,   # t-1: H,L,C
    ],
    'z2b': [
        -0.33375857, 0.33327147, 0.33478365,   # t-3: H,L,C
        -0.33395845, 0.33313921, 0.33324867,   # t-2: H,L,C
        -0.33277367, 0.33384496, 0.33220288,   # t-1: H,L,C
    ],
    'ebot': [
        -0.11115648, 0.22303212, 0.22206190,   # t-3: H,L,C
        -0.11250983, 0.22120078, 0.22439345,   # t-2: H,L,C
        -0.11109552, 0.22046378, 0.22360772,   # t-1: H,L,C
    ],
    'pldot': [
        0.11306077, 0.10884779, 0.10864725,   # t-3: H,L,C
        0.11441424, 0.11317815, 0.10686769,   # t-2: H,L,C
        0.11171601, 0.11384294, 0.10939732,   # t-1: H,L,C
    ],
    'etop': [
        0.22106127, -0.11318101, 0.22457886,   # t-3: H,L,C
        0.22053147, -0.11010281, 0.22546244,   # t-2: H,L,C
        0.21983177, -0.11226826, 0.22411409,   # t-1: H,L,C
    ],
    'z5t': [
        0.33298475, -0.33125052, 0.33371591,   # t-3: H,L,C
        0.33153760, -0.33584054, 0.33648807,   # t-2: H,L,C
        0.33404897, -0.33557298, 0.33388438,   # t-1: H,L,C
    ],
    'z6t': [
        0.55639359, -0.44796047, 0.22238203,   # t-3: H,L,C
        0.55700240, -0.44277147, 0.21953681,   # t-2: H,L,C
        0.55585774, -0.44037034, 0.21992682,   # t-1: H,L,C
    ]
}

def parse_validation_data():
    """Parse the validation dataset."""
    lines = [line.strip() for line in validation_data_raw.strip().split('\n') if line.strip()]
    
    data = []
    for line in lines:
        parts = line.split()
        date = parts[0]
        values = [float(x) for x in parts[1:]]
        
        # Values: high, low, close, h11, l11, z1b, z2b, ebot, pldot, etop, z5t, z6t
        hlc = values[:3]
        expected = {
            'h11': values[3], 'l11': values[4], 'z1b': values[5], 'z2b': values[6],
            'ebot': values[7], 'pldot': values[8], 'etop': values[9], 'z5t': values[10], 'z6t': values[11]
        }
        
        data.append((date, hlc, expected))
    
    return data

def calculate_hlc_indicator(hlc_history, coefficients):
    """Calculate indicator value using 3 days HLC and coefficients."""
    features = []
    for hlc in hlc_history:
        features.extend(hlc)
    
    return np.dot(coefficients, features)

def test_hlc_cross_scale_validation():
    """Test HLC-only formulas on validation dataset with different price scale."""
    
    print("TESTING HLC-ONLY FORMULAS ON CROSS-SCALE VALIDATION DATASET")
    print("=" * 80)
    print("Training Scale: ~3,300-3,500 (price range used to derive formulas)")
    print("Validation Scale: ~22,800-24,100 (7x larger price range)")
    print("Test: Cross-scale generalization capability")
    print("=" * 80)
    
    # Parse validation data
    parsed_data = parse_validation_data()
    print(f"Loaded {len(parsed_data)} validation data points")
    
    # Get price range information
    all_prices = []
    for _, hlc, _ in parsed_data:
        all_prices.extend(hlc)
    
    min_price = min(all_prices)
    max_price = max(all_prices)
    print(f"Validation price range: {min_price:.1f} - {max_price:.1f}")
    print(f"Scale factor vs training: ~{min_price/3300:.1f}x larger")
    
    # Test starting from day 4 (need 3 prior days for features)
    results = []
    total_tests = 0
    passed_tests = 0
    
    # Collect all results first
    all_errors = {indicator: [] for indicator in hlc_coefficients.keys()}
    all_actuals = {indicator: [] for indicator in hlc_coefficients.keys()}
    all_predictions = {indicator: [] for indicator in hlc_coefficients.keys()}
    
    print(f"\nDetailed Results (Testing days 4-{len(parsed_data)}):")
    print("=" * 80)
    
    for i in range(3, len(parsed_data)):  # Start from 4th day
        test_date = parsed_data[i][0]
        current_expected = parsed_data[i][2]
        
        print(f"\nTesting {test_date}:")
        print("-" * 50)
        
        # Get HLC history from previous 3 days
        hlc_history = []
        for j in range(i-3, i):  # Previous 3 days
            hlc = parsed_data[j][1]
            hlc_history.append(hlc)
        
        # Test each indicator
        day_results = {'date': test_date, 'tests': {}}
        
        for indicator_name, coeffs in hlc_coefficients.items():
            if indicator_name in current_expected:
                # Calculate prediction using HLC formula
                predicted = calculate_hlc_indicator(hlc_history, coeffs)
                actual = current_expected[indicator_name]
                
                # Skip anomalous H11 value on 08/06 (2329.33 vs expected ~23000)
                if indicator_name == 'h11' and test_date == '08/06' and actual < 3000:
                    print(f"  {indicator_name.upper():6}: Skipping anomalous value {actual} (data error)")
                    continue
                
                error = abs(predicted - actual)
                error_pct = (error / actual) * 100 if actual != 0 else 0
                
                # Collect statistics
                all_errors[indicator_name].append(error)
                all_actuals[indicator_name].append(actual)
                all_predictions[indicator_name].append(predicted)
                
                # Determine pass/fail (generous threshold for cross-scale testing)
                error_threshold = 100.0  # Allow larger errors for 7x different price scale
                test_passed = error < error_threshold
                status = "✅ PASS" if test_passed else "❌ FAIL"
                
                print(f"  {indicator_name.upper():6}: Expected={actual:8.1f}, Predicted={predicted:8.1f}, Error={error:6.1f} ({error_pct:5.2f}%) {status}")
                
                total_tests += 1
                if test_passed:
                    passed_tests += 1
        
        results.append(day_results)
    
    # Calculate comprehensive statistics
    print(f"\n" + "=" * 80)
    print("CROSS-SCALE VALIDATION STATISTICS")
    print("=" * 80)
    
    print(f"{'Indicator':<8} {'Samples':>8} {'Avg Error':>10} {'Max Error':>10} {'RMSE':>10} {'R²':>10} {'Assessment':<15}")
    print("-" * 80)
    
    excellent_count = 0
    good_count = 0
    poor_count = 0
    
    for indicator_name in hlc_coefficients.keys():
        if indicator_name in all_errors and len(all_errors[indicator_name]) > 0:
            errors = np.array(all_errors[indicator_name])
            actuals = np.array(all_actuals[indicator_name])
            predictions = np.array(all_predictions[indicator_name])
            
            # Calculate statistics
            avg_error = np.mean(errors)
            max_error = np.max(errors)
            rmse = np.sqrt(np.mean(errors**2))
            
            # Calculate R²
            ss_res = np.sum((actuals - predictions) ** 2)
            ss_tot = np.sum((actuals - np.mean(actuals)) ** 2)
            r2 = 1 - (ss_res / ss_tot) if ss_tot > 0 else 1.0
            
            # Assessment (adjusted for cross-scale testing)
            if r2 > 0.98 and avg_error < 50:
                assessment = "🎉 EXCELLENT"
                excellent_count += 1
            elif r2 > 0.9 and avg_error < 100:
                assessment = "✅ GOOD"
                good_count += 1
            elif r2 > 0.7 and avg_error < 200:
                assessment = "⚠️ ACCEPTABLE"
            else:
                assessment = "❌ POOR"
                poor_count += 1
            
            print(f"{indicator_name.upper():<8} {len(errors):>8} {avg_error:>10.1f} {max_error:>10.1f} {rmse:>10.1f} {r2:>10.6f} {assessment:<15}")
    
    # Overall summary
    total_indicators = len([k for k in hlc_coefficients.keys() if k in all_errors and len(all_errors[k]) > 0])
    success_rate = (excellent_count + good_count) / total_indicators if total_indicators > 0 else 0
    
    print(f"\n" + "=" * 80)
    print("CROSS-SCALE GENERALIZATION SUMMARY")
    print("=" * 80)
    
    print(f"Scale Challenge:")
    print(f"• Training data: 3,300-3,500 price range")
    print(f"• Validation data: 22,800-24,100 price range (7x larger)")
    print(f"• Formula coefficients unchanged from training")
    
    print(f"\nPerformance Results:")
    print(f"• Total indicators tested: {total_indicators}")
    print(f"• 🎉 EXCELLENT performance: {excellent_count}")
    print(f"• ✅ GOOD performance: {good_count}")
    print(f"• ❌ POOR performance: {poor_count}")
    print(f"• Overall success rate: {success_rate*100:.1f}%")
    
    print(f"\nDetailed Pass/Fail Analysis:")
    print(f"• Total individual tests: {total_tests}")
    print(f"• Passed tests (error < 100 points): {passed_tests}")
    print(f"• Failed tests: {total_tests - passed_tests}")
    print(f"• Point-by-point success rate: {(passed_tests/total_tests)*100:.1f}%")
    
    # Final assessment
    if success_rate >= 0.7:
        conclusion = "🎉 EXCELLENT: Formulas show strong cross-scale generalization!"
    elif success_rate >= 0.5:
        conclusion = "✅ GOOD: Formulas generalize reasonably across price scales"
    elif success_rate >= 0.3:
        conclusion = "⚠️ MODERATE: Mixed results on cross-scale testing"
    else:
        conclusion = "❌ POOR: Formulas fail to generalize across price scales"
    
    print(f"\n🏆 FINAL ASSESSMENT: {conclusion}")
    
    # Show worst and best performing indicators
    if all_errors:
        avg_errors_by_indicator = {name: np.mean(errors) for name, errors in all_errors.items() if len(errors) > 0}
        best_indicator = min(avg_errors_by_indicator, key=avg_errors_by_indicator.get)
        worst_indicator = max(avg_errors_by_indicator, key=avg_errors_by_indicator.get)
        
        print(f"\nPerformance Highlights:")
        print(f"• Best performer: {best_indicator.upper()} (avg error: {avg_errors_by_indicator[best_indicator]:.1f})")
        print(f"• Worst performer: {worst_indicator.upper()} (avg error: {avg_errors_by_indicator[worst_indicator]:.1f})")
    
    # Show sample predictions for first few test days
    print(f"\n" + "=" * 80)
    print("SAMPLE CROSS-SCALE PREDICTIONS (First 3 Test Days)")
    print("=" * 80)
    
    sample_count = 0
    for i in range(3, min(6, len(parsed_data))):  # Show first 3 test days
        test_date = parsed_data[i][0]
        current_expected = parsed_data[i][2]
        
        # Skip anomalous data day
        if test_date == '08/06':
            continue
            
        print(f"\n{test_date}:")
        
        # Get HLC history from previous 3 days
        hlc_history = []
        for j in range(i-3, i):
            hlc = parsed_data[j][1]
            hlc_history.append(hlc)
        
        # Show predictions for each indicator
        for indicator_name, coeffs in hlc_coefficients.items():
            if indicator_name in current_expected:
                predicted = calculate_hlc_indicator(hlc_history, coeffs)
                actual = current_expected[indicator_name]
                error = abs(predicted - actual)
                
                print(f"  {indicator_name.upper():6}: Actual={actual:8.1f}, Predicted={predicted:8.1f}, Error={error:6.1f}")
        
        sample_count += 1
        if sample_count >= 3:
            break
    
    return success_rate > 0.5

if __name__ == "__main__":
    success = test_hlc_cross_scale_validation()
    
    if success:
        print(f"\n🚀 CROSS-SCALE VALIDATION PASSED: HLC formulas demonstrate good generalization across price ranges")
        exit_code = 0
    else:
        print(f"\n⚠️ CROSS-SCALE VALIDATION CONCERNS: Limited generalization across different price scales")
        exit_code = 1
    
    import sys
    sys.exit(exit_code)