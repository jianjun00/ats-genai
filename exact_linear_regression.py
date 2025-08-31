import numpy as np
from numpy.linalg import lstsq

# Complete trading data with all samples including 08/06
raw_data = """
07/30    23452    23689    23356.5    23480.5    23595.92    23364.67    23201.97    23279.83    23376.97    23474.11    23551.97    23629.83    23726.97
07/31    23705    23845    23290.5    23365    23660.83    23328.33    23134.78    23237.17    23372.19    23507.22    23609.61    23712    23847.03
08/01    23308.5    23347.5    22775    22883.75    23709.83    23155.33    22910.03    23059.75    23282.78    23505.81    23655.53    23805.25    24028.28
08/04    22850    23352.5    22821.75    23296.5    23229.17    22656.67    22560.28    22756.58    23046.78    23336.97    23533.28    23729.58    24019.78
08/05    23354    23404.25    23084.5    23132    23492.08    22961.33    22371.86    22629.17    22924.44    23219.72    23477.03    23734.33    24029.61
08/06    23091.75    23445.25    23045.5    23422.75    2329.33    23009.58    22401.53    22629.75    22875.86    23121.97    23350.19    23578.42    23824.53
08/07    23418.5    23671    23329    23496.25    23563.5    23163.75    22628.14    22867    23044.89    23222.78    23461.61    23700.5    23878.39
08/08    23518.25    23767.75    23503    23713.75    23668.5    23326.5    22812.78    22996.5    23166.61    23336.72    23520.44    23704.17    23874.28
08/11    23764    23804.5    23587.5    23637.5    23820    23555.25    23013    23208.75    23348.5    23488.25    23684    23879.75    24019.5
08/12    23635    23953.5    23596    23938    23765.5    23548.5    23202.17    23341.25    23476.75    23612.25    23751.33    23890.42    24025.92
08/13    23945    24068.5    23886    23946.5    24062.33    23704.83    23323.11    23483.33    23602.86    23722.39    23882.61    24042.83    24162.36
08/14    23927.75    24007.75    23793.25    23930.5    24048    23865.5    23453.94    23588.33    23706.28    23824.22    23958.61    24093    24210.94
08/15    23888.75    23963    23734.5    23804    24027.75    23813.25    23543.03    23686.83    23794.53    23902.22    24046.03    24189.83    24297.53
08/18    23795    23881.75    23719    23797.75    23933.17    23704.67    23585.97    23685.17    23794.47    23903.78    24002.97    24102.17    24211.47
08/19    23800.75    23838    23426    23469.5    23880    23717.25    23543.14    23642.17    23745.06    23847.94    23946.97    24046    24148.89
08/20    23461    23485.5    23035    23324    23729.67    23317.67    23312.11    23422.67    23579.86    23737.06    23847.61    23958.17    24115.36
08/21    23323    23369.25    23119    23219.75    23528    23077.5    23029.06    23188.67    23370.81    23552.95    23712.56    23872.17    24054.31
08/22    23225.25    23650    23076.75    23569.75    23353    23102.75    22795.06    22966.83    23165.97    23365.11    23536.89    23708.67    23907.81
08/25    23602    23616.25    23443.25    23498.25    23787.58    23214.33    22706.86    22946.5    23131.53    23316.56    23556.19    23795.83    23980.86
08/26    23505.75    23611    23371.5    23607.5    23595.25    23422.25    22914.28    23097.08    23246.44    23395.81    23578.61    23761.42    23910.78
08/27    23604    23689    23434.75    23542    23688.5    23449    23033.28    23229.92    23361.86    23493.81    23690.44    23887.08    24019.03
08/28    23542.25    23803.75    23487.5    23762    23675.75    23421.5    23208.67    23327    23430.92    23534.83    23653.17    23771.5    23875.42
08/29    23760.5    23762    23397.5    23454.5    23881.33    23565.08    23208.53    23367.17    23478.53    23589.89    23748.53    23907.17    24018.53
"""

def solve_exact_linear_regression():
    # Parse data
    lines = [line.strip() for line in raw_data.strip().split('\n') if line.strip()]
    data = []
    for line in lines:
        parts = line.split()
        values = [float(x) for x in parts[1:]]  # Skip date
        data.append(values)

    data = np.array(data)
    print(f"Data shape: {data.shape}")

    # Create feature matrix X and target vectors
    X = []
    targets = {
        'h11': [], 'l11': [], 'z1b': [], 'z2b': [], 'ebot': [], 
        'pldot': [], 'etop': [], 'z5t': [], 'z6t': []
    }
    target_indices = [4, 5, 6, 7, 8, 9, 10, 11, 12]

    for i in range(3, len(data)):  # Start from day 4
        # Features: OHLC from 3 previous days
        features = []
        for lookback in [3, 2, 1]:  # t-3, t-2, t-1
            prev_idx = i - lookback
            features.extend(data[prev_idx, :4])  # OHLC only
        
        X.append(features)
        
        # Targets for current day
        for j, col in enumerate(targets.keys()):
            targets[col].append(data[i, target_indices[j]])

    X = np.array(X)
    print(f"Feature matrix shape: {X.shape}")
    print(f"Samples: {len(X)} (sufficient for 12 parameters)")

    feature_names = []
    for day in ['t-3', 't-2', 't-1']:
        for ohlc in ['open', 'high', 'low', 'close']:
            feature_names.append(f'{ohlc}_{day}')

    print("\n" + "="*80)
    print("EXACT LINEAR REGRESSION COEFFICIENTS (No Intercept)")
    print("Using ALL {} data points".format(len(X)))
    print("="*80)

    results = {}
    for col_name in targets.keys():
        y = np.array(targets[col_name])
        
        # Solve using least squares: min ||Ax - b||²
        coeffs, residuals, rank, s = lstsq(X, y, rcond=None)
        
        # Calculate metrics
        y_pred = X @ coeffs
        ss_res = np.sum((y - y_pred) ** 2)
        ss_tot = np.sum((y - np.mean(y)) ** 2)
        r2 = 1 - (ss_res / ss_tot) if ss_tot > 0 else 1.0
        max_error = np.max(np.abs(y - y_pred))
        avg_error = np.mean(np.abs(y - y_pred))
        
        results[col_name] = {
            'coefficients': coeffs,
            'r2': r2,
            'avg_error': avg_error,
            'max_error': max_error
        }
        
        print(f"\n{col_name.upper()}:")
        print(f"R² = {r2:.10f}")
        print(f"Average error = {avg_error:.6f}")
        print(f"Max error = {max_error:.6f}")
        
        # Print exact formula
        formula_parts = []
        for i, (coef, name) in enumerate(zip(coeffs, feature_names)):
            if abs(coef) > 1e-12:  # Show all significant coefficients
                sign = " + " if coef >= 0 and len(formula_parts) > 0 else ""
                if coef < 0 and len(formula_parts) > 0:
                    sign = " - "
                    coef = abs(coef)
                elif coef < 0:
                    sign = "-"
                    coef = abs(coef)
                formula_parts.append(f"{sign}{coef:.10f}*{name}")
        
        if formula_parts:
            print(f"Formula:")
            print(f"{col_name} = {''.join(formula_parts)}")
        else:
            print(f"Formula: {col_name} = 0")

    print("\n" + "="*80)
    print("VERIFICATION - First 5 Samples")
    print("="*80)
    for i in range(5):
        print(f"\nSample {i+1}:")
        for col_name in ['h11', 'pldot', 'z6t', 'z1b']:  # Key problematic columns
            actual = targets[col_name][i]
            predicted = np.dot(results[col_name]['coefficients'], X[i])
            error = abs(predicted - actual)
            
            print(f"  {col_name:5}: Actual={actual:10.2f}, Predicted={predicted:10.2f}, Error={error:8.4f}")

    print("\n" + "="*80)
    print("COEFFICIENT SUMMARY TABLE")
    print("="*80)
    
    # Print coefficient table
    print(f"{'Feature':<12}", end="")
    for col in targets.keys():
        print(f"{col:>14}", end="")
    print()
    print("-" * (12 + 14 * len(targets)))
    
    for i, name in enumerate(feature_names):
        print(f"{name:<12}", end="")
        for col in targets.keys():
            coef = results[col]['coefficients'][i]
            print(f"{coef:14.8f}", end="")
        print()

if __name__ == "__main__":
    solve_exact_linear_regression()