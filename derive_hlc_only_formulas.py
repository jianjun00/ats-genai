"""
Derive new linear regression formulas using only High, Low, Close (HLC) data.
Excludes open prices from the feature set to create 9-feature models (3 days × 3 HLC).
"""

import numpy as np
from numpy.linalg import lstsq

# HLC-only dataset provided by user
hlc_data = """
08/05    3444.9    3403.3    3434.7    3444.6    3403    3302.9    3335.1    3359.3    3383.5    3415.8    3448.1    3472.3
08/06    3440.5    3411.7    3433.4    3452    3410.4    3331.1    3364.1    3387.3    3410.5    3443.5    3476.5    3499.8
08/07    3483.8    3430    3453.7    3445.4    3416.6    3372.7    3394.2    3410    3425.8    3447.3    3468.8    3484.7
08/08    3534.1    3445    3491.3    3481.7    3427.9    3376.9    3399.2    3418.3    3437.3    3459.7    3482    3501.1
08/11    3466.3    3393    3404.7    3535.3    3446.2    3373    3402.2    3430.2    3458.2    3487.4    3516.7    3544.7
08/12    3410.8    3379.1    3399    3449.7    3376.4    3344.7    3377.8    3416.8    3455.8    3488.9    3522    3560.9
08/13    3422.6    3392.7    3408.3    3413.5    3381.8    3336.7    3367    3401.4    3435.9    3466.1    3496.4    3530.8
08/14    3423.8    3375.5    3383.2    3423    3393.1    3338.8    3359    3383.8    3408.5    3428.7    3449    3473.7
08/15    3394.8    3377.7    3382.6    3412.8    3364.5    3343.2    3360.2    3379.8    3399.4    3416.5    3433.5    3453.1
08/18    3403.6    3368    3378    3392.4    3375.3    3345.9    3359.6    3377.6    3395.7    3409.4    3423.1    3441.2
08/19    3389.7    3358.1    3358.7    3398.4    3362.8    3333.9    3347.6    3367.5    3387.5    3401.2    3414.9    3434.9
08/20    3394.3    3353.4    3388.5    3379.6    3348    3333.9    3345    3362    3379    3390.1    3401.2    3418.2
08/21    3394.4    3367.4    3383.5    3404.1    3363.2    3321.9    3339    3358    3376.9    3394    3411.1    3430
08/22    3423.4    3362.8    3418.5    3396.1    3369.1    3326.9    3343.7    3360.1    3376.4    3393.3    3410.1    3426.4
08/25    3421.5    3405.5    3417.5    3440.3    3379.7    3327.8    3354    3370.7    3387.4    3413.5    3439.7    3456.3
08/26    3443.3    3396.1    3433    3424.2    3408.2    3351.1    3372    3385.7    3399.4    3420.2    3441    3454.7
08/27    3452.5    3422.2    3448.6    3452.2    3405    3356.4    3381.7    3397.6    3413.5    3438.9    3464.3    3480.2
08/28    3478.7    3442.5    3474.3    3460    3429.7    3383.1    3401.9    3414.3    3426.7    3445.4    3464.2    3476.6
08/29    3479.6    3463    3467.6    3487.8    3451.6    3390.9    3414.1    3428.8    3443.5    3466.7    3489.9    3504.6
"""

def parse_hlc_data():
    """Parse the HLC-only dataset into structured format."""
    lines = [line.strip() for line in hlc_data.strip().split('\n') if line.strip()]
    
    data = []
    for line in lines:
        parts = line.split()
        values = [float(x) for x in parts[1:]]  # Skip date
        data.append(values)
    
    return np.array(data)

def derive_hlc_formulas():
    """Derive linear regression formulas using only HLC features (9 features total)."""
    
    print("DERIVING HLC-ONLY LINEAR FORMULAS")
    print("=" * 60)
    print("Features: High, Low, Close from 3 previous days (9 features)")
    print("Excludes: Open prices")
    
    data = parse_hlc_data()
    print(f"Dataset: {data.shape[0]} days × {data.shape[1]} columns")
    print(f"HLC price range: {data[:,:3].min():.1f} - {data[:,:3].max():.1f}")
    
    # Create feature matrix X and target vectors
    X = []
    targets = {
        'h11': [], 'l11': [], 'z1b': [], 'z2b': [], 'ebot': [], 
        'pldot': [], 'etop': [], 'z5t': [], 'z6t': []
    }
    target_indices = [3, 4, 5, 6, 7, 8, 9, 10, 11]  # h11, l11, z1b, z2b, ebot, pldot, etop, z5t, z6t
    target_names = list(targets.keys())
    
    # Build samples starting from day 4 (need 3 prior days)
    for i in range(3, len(data)):
        # Features: HLC from 3 previous days (9 features)
        features = []
        for lookback in [3, 2, 1]:  # t-3, t-2, t-1
            prev_idx = i - lookback
            hlc = data[prev_idx, :3]  # High, Low, Close only
            features.extend(hlc)
        
        X.append(features)
        
        # Targets for current day
        for j, target_name in enumerate(target_names):
            targets[target_name].append(data[i, target_indices[j]])
    
    X = np.array(X)
    print(f"Training samples: {X.shape[0]} (sufficient for 9 parameters)")
    print(f"Features per sample: {X.shape[1]} (3 days × 3 HLC)")
    
    feature_names = []
    for day in ['t-3', 't-2', 't-1']:
        for hlc in ['high', 'low', 'close']:
            feature_names.append(f'{hlc}_{day}')
    
    print(f"\nFeature order: {feature_names}")
    
    print("\n" + "=" * 80)
    print("NEW HLC-ONLY LINEAR REGRESSION FORMULAS")
    print("=" * 80)
    
    results = {}
    hlc_coefficients = {}
    
    for target_name in target_names:
        y = np.array(targets[target_name])
        
        # Solve using least squares: min ||Ax - b||²
        coeffs, residuals, rank, s = lstsq(X, y, rcond=None)
        
        # Calculate performance metrics
        y_pred = X @ coeffs
        ss_res = np.sum((y - y_pred) ** 2)
        ss_tot = np.sum((y - np.mean(y)) ** 2) 
        r2 = 1 - (ss_res / ss_tot) if ss_tot > 0 else 1.0
        max_error = np.max(np.abs(y - y_pred))
        avg_error = np.mean(np.abs(y - y_pred))
        rmse = np.sqrt(np.mean((y - y_pred) ** 2))
        
        results[target_name] = {
            'coefficients': coeffs,
            'r2': r2,
            'avg_error': avg_error,
            'max_error': max_error,
            'rmse': rmse,
            'actual': y,
            'predicted': y_pred
        }
        
        hlc_coefficients[target_name] = coeffs.tolist()
        
        print(f"\n{target_name.upper()}:")
        print(f"R² = {r2:.10f}")
        print(f"Average error = {avg_error:.4f}")
        print(f"Max error = {max_error:.4f}")
        print(f"RMSE = {rmse:.4f}")
        
        # Print exact formula
        print(f"Formula:")
        formula_parts = []
        for i, (coef, name) in enumerate(zip(coeffs, feature_names)):
            if i == 0:
                sign = ""
                if coef < 0:
                    sign = "-"
                    coef = abs(coef)
            else:
                sign = " + " if coef >= 0 else " - "
                if coef < 0:
                    coef = abs(coef)
            formula_parts.append(f"{sign}{coef:.8f}*{name}")
        
        print(f"{target_name} = {''.join(formula_parts)}")
    
    # Cross-validation: split data into training (first 70%) and validation (last 30%)
    train_size = int(0.7 * len(X))
    X_train, X_val = X[:train_size], X[train_size:]
    
    print("\n" + "=" * 80)
    print("CROSS-VALIDATION RESULTS (HLC-ONLY)")
    print("=" * 80)
    print(f"Training set: {len(X_train)} samples")
    print(f"Validation set: {len(X_val)} samples")
    
    cv_results = {}
    cv_coefficients = {}
    
    for target_name in target_names:
        y = np.array(targets[target_name])
        y_train, y_val = y[:train_size], y[train_size:]
        
        # Train on training set
        coeffs_cv, _, _, _ = lstsq(X_train, y_train, rcond=None)
        
        # Test on validation set
        y_val_pred = X_val @ coeffs_cv
        
        # Validation metrics
        val_avg_error = np.mean(np.abs(y_val - y_val_pred))
        val_max_error = np.max(np.abs(y_val - y_val_pred))
        val_rmse = np.sqrt(np.mean((y_val - y_val_pred) ** 2))
        
        # R² on validation set
        ss_res_val = np.sum((y_val - y_val_pred) ** 2)
        ss_tot_val = np.sum((y_val - np.mean(y_val)) ** 2)
        r2_val = 1 - (ss_res_val / ss_tot_val) if ss_tot_val > 0 else 1.0
        
        cv_results[target_name] = {
            'val_r2': r2_val,
            'val_avg_error': val_avg_error,
            'val_max_error': val_max_error,
            'val_rmse': val_rmse,
            'coefficients': coeffs_cv
        }
        
        cv_coefficients[target_name] = coeffs_cv.tolist()
        
        # Compare training vs validation performance
        train_error = results[target_name]['avg_error']
        generalization_gap = val_avg_error - train_error
        
        print(f"\n{target_name.upper()}:")
        print(f"Training R² = {results[target_name]['r2']:.6f}, Validation R² = {r2_val:.6f}")
        print(f"Training Error = {train_error:.4f}, Validation Error = {val_avg_error:.4f}")
        print(f"Generalization Gap = {generalization_gap:.4f} ({((generalization_gap/train_error)*100) if train_error > 0 else 0:.1f}%)")
        
        # Assessment
        if abs(generalization_gap) < 1.0:
            assessment = "✅ EXCELLENT - Very low generalization gap"
        elif abs(generalization_gap) < 2.0:
            assessment = "✅ GOOD - Low generalization gap"
        elif abs(generalization_gap) < 5.0:
            assessment = "⚠️ MODERATE - Acceptable generalization"
        else:
            assessment = "❌ POOR - High generalization gap (overfitting)"
        
        print(f"Assessment: {assessment}")
    
    # Summary recommendations
    print("\n" + "=" * 80)
    print("HLC-ONLY FORMULA QUALITY ASSESSMENT")
    print("=" * 80)
    
    excellent_formulas = []
    good_formulas = []
    poor_formulas = []
    
    for target_name in target_names:
        train_error = results[target_name]['avg_error'] 
        val_error = cv_results[target_name]['val_avg_error']
        gap = abs(val_error - train_error)
        val_r2 = cv_results[target_name]['val_r2']
        
        if gap < 1.0 and val_r2 > 0.98:
            excellent_formulas.append(target_name)
        elif gap < 2.0 and val_r2 > 0.9:
            good_formulas.append(target_name)
        else:
            poor_formulas.append(target_name)
    
    print(f"✅ EXCELLENT (very low error, excellent generalization): {excellent_formulas}")
    print(f"✅ GOOD (moderate error/generalization): {good_formulas}")
    print(f"❌ POOR (high error or overfitting): {poor_formulas}")
    
    # Output production-ready coefficients
    print("\n" + "=" * 80)
    print("PRODUCTION-READY HLC-ONLY COEFFICIENTS")
    print("=" * 80)
    
    production_formulas = excellent_formulas + good_formulas
    
    print("# HLC-Only Cross-Validated Coefficients (9 features)")
    print("hlc_coefficients = {")
    
    for target_name in target_names:
        coeffs = cv_results[target_name]['coefficients']
        print(f"    '{target_name}': [")
        
        for i in range(0, 9, 3):  # Group by day (3 HLC per day)
            day_name = ['t-3', 't-2', 't-1'][i//3]
            coef_line = f"        {coeffs[i]:.8f}, {coeffs[i+1]:.8f}, {coeffs[i+2]:.8f},   # {day_name}: H,L,C"
            print(coef_line)
        
        print("    ],")
        print(f"    # Validation R² = {cv_results[target_name]['val_r2']:.6f}, Error = {cv_results[target_name]['val_avg_error']:.4f}")
        print()
    
    print("}")
    
    # Compare with 12-feature (OHLC) vs 9-feature (HLC) performance
    print("\n" + "=" * 80)
    print("HLC vs OHLC FEATURE COMPARISON")
    print("=" * 80)
    print("Reduced from 12 features (OHLC) to 9 features (HLC)")
    print("Benefits:")
    print("• Simpler models with fewer parameters")
    print("• Reduced risk of overfitting")
    print("• Open price often less predictive than HLC")
    print("• Easier to implement and understand")
    
    # Overall assessment
    excellent_count = len(excellent_formulas)
    good_count = len(good_formulas)
    poor_count = len(poor_formulas)
    total_count = len(target_names)
    
    success_rate = (excellent_count + good_count) / total_count
    
    print(f"\n📊 HLC-ONLY OVERALL ASSESSMENT: {excellent_count + good_count}/{total_count} formulas have good performance")
    
    if success_rate >= 0.9:
        conclusion = "🎉 OUTSTANDING: HLC-only formulas show excellent performance!"
    elif success_rate >= 0.7:
        conclusion = "✅ VERY GOOD: HLC-only formulas show strong performance!"
    elif success_rate >= 0.5:
        conclusion = "⚠️ MODERATE: Some HLC-only formulas may be suitable"
    else:
        conclusion = "❌ POOR: HLC-only formulas still need improvement"
    
    print(conclusion)
    
    return results, cv_results, hlc_coefficients, cv_coefficients

if __name__ == "__main__":
    results, cv_results, hlc_coefficients, cv_coefficients = derive_hlc_formulas()
    
    # Determine overall success
    good_count = sum(1 for target in cv_results if cv_results[target]['val_r2'] > 0.9)
    total_count = len(cv_results)
    
    print(f"\n🎯 FINAL ASSESSMENT: {good_count}/{total_count} HLC-only formulas have good validation performance")
    
    if good_count >= total_count * 0.9:
        print("🎉 SUCCESS: HLC-only formulas show excellent generalization potential!")
        exit_code = 0
    elif good_count >= total_count * 0.7:
        print("✅ GOOD: HLC-only formulas show good generalization potential!")
        exit_code = 0
    elif good_count >= total_count * 0.5:
        print("⚠️ MIXED: Some HLC-only formulas may be suitable for production")
        exit_code = 0 
    else:
        print("❌ POOR: HLC-only formulas show concerning performance")
        exit_code = 1
    
    import sys
    sys.exit(exit_code)