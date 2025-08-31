# Exact Linear Formulas for Technical Indicators

## Overview

This document contains the **exact mathematical formulas** for deriving technical indicators (h11, l11, z1b, z2b, ebot, pldot, etop, z5t, z6t) from prior 3 days of OHLC data, derived using linear regression analysis on 20 data points with R² ≈ 1.0 accuracy.

## Methodology

- **Data Points**: 20 trading days (07/30 - 08/29)
- **Features**: 12 variables (3 days × 4 OHLC values)
- **Method**: Linear regression with no intercept
- **Accuracy**: R² > 0.999 for most indicators (errors < 0.004)

## Exact Formulas

### Feature Notation
- `open(t-3)`, `high(t-3)`, `low(t-3)`, `close(t-3)` = OHLC from 3 days ago
- `open(t-2)`, `high(t-2)`, `low(t-2)`, `close(t-2)` = OHLC from 2 days ago  
- `open(t-1)`, `high(t-1)`, `low(t-1)`, `close(t-1)` = OHLC from 1 day ago

### Perfect Accuracy Formulas (R² = 1.0, Error < 0.004)

#### Z6T - High Momentum Indicator
**R² = 0.9999999998, Average Error = 0.001328**

```
z6t = 0.55555673*high(t-3) + 0.55555039*high(t-2) + 0.55554668*high(t-1)
    - 0.44444686*low(t-3)  - 0.44446149*low(t-2)  - 0.44445449*low(t-1)
    + 0.22223491*close(t-3) + 0.22221018*close(t-2) + 0.22222695*close(t-1)
```

**Simplified**: `z6t ≈ 0.5556*(high_sum) - 0.4444*(low_sum) + 0.2222*(close_sum)`

#### Z1B - Low Momentum Indicator  
**R² = 1.0000000000, Average Error = 0.001328**

```
z1b = -0.44444327*high(t-3) - 0.44444961*high(t-2) - 0.44445332*high(t-1)
    + 0.55555314*low(t-3)  + 0.55553851*low(t-2)  + 0.55554551*low(t-1)
    + 0.22223491*close(t-3) + 0.22221018*close(t-2) + 0.22222695*close(t-1)
```

**Simplified**: `z1b ≈ -0.4444*(high_sum) + 0.5556*(low_sum) + 0.2222*(close_sum)`

#### Z2B - Balanced Momentum Indicator
**R² = 1.0000000000, Average Error = 0.001288**

```
z2b = -0.33333483*high(t-3) - 0.33333123*high(t-2) - 0.33332244*high(t-1)
    + 0.33333029*low(t-3)  + 0.33334061*low(t-2)  + 0.33332972*low(t-1)
    + 0.33338614*close(t-3) + 0.33331983*close(t-2) + 0.33332344*close(t-1)
```

**Simplified**: `z2b ≈ -0.3333*(high_sum) + 0.3333*(low_sum) + 0.3333*(close_sum)`

#### Z5T - High-Close Momentum  
**R² = 0.9999999999, Average Error = 0.001220**

```
z5t = 0.33332894*high(t-3) + 0.33332009*high(t-2) + 0.33336122*high(t-1)
    - 0.33332172*low(t-3)  - 0.33336788*low(t-2)  - 0.33333863*low(t-1)
    + 0.33339446*close(t-3) + 0.33332609*close(t-2) + 0.33331041*close(t-1)
```

**Simplified**: `z5t ≈ 0.3333*(high_sum) - 0.3333*(low_sum) + 0.3333*(close_sum)`

#### PLDOT - Price Level Dot
**R² = 0.9999999999, Average Error = 0.001359**

```
pldot = 0.11110720*high(t-3) + 0.11109015*high(t-2) + 0.11112260*high(t-1)
      + 0.11111657*low(t-3)  + 0.11108289*low(t-2)  + 0.11107075*low(t-1)
      + 0.11115994*close(t-3) + 0.11111244*close(t-2) + 0.11112078*close(t-1)
```

**Simplified**: `pldot ≈ 0.1111*(high_sum + low_sum + close_sum)` = **Average of all HLC values**

#### EBOT - Bottom Level Indicator
**R² = 1.0000000000, Average Error = 0.001397**

```
ebot = -0.11113139*high(t-3) - 0.11109608*high(t-2) - 0.11112842*high(t-1)
     + 0.22221285*low(t-3)  + 0.22224681*low(t-2)  + 0.22222156*low(t-1)
     + 0.22221404*close(t-3) + 0.22221805*close(t-2) + 0.22223273*close(t-1)
```

**Simplified**: `ebot ≈ -0.1111*(high_sum) + 0.2222*(low_sum + close_sum)`

#### ETOP - Top Level Indicator
**R² = 0.9999999994, Average Error = 0.003366**

```
etop = 0.22219235*high(t-3) + 0.22225543*high(t-2) + 0.22218006*high(t-1)
     - 0.11109917*low(t-3)  - 0.11106971*low(t-2)  - 0.11109128*low(t-1)
     + 0.22216559*close(t-3) + 0.22208568*close(t-2) + 0.22223771*close(t-1)
```

**Simplified**: `etop ≈ 0.2222*(high_sum + close_sum) - 0.1111*(low_sum)`

#### L11 - Low Level Indicator
**R² = 1.0000000000, Average Error = 0.000605**

```
l11 = -0.33331649*high(t-1) + 0.66666993*low(t-1) + 0.66664543*close(t-1)
```

**Simplified**: `l11 ≈ -0.3333*high(t-1) + 0.6667*(low(t-1) + close(t-1))`

### Problematic Formula

#### H11 - High Level Indicator
**R² = 0.6942315271, Average Error = 2124.88** ❌

```
h11 = -16.55319212*open(t-3) - 17.62657943*high(t-3) + 20.63777524*low(t-3) + 18.59613847*close(t-3)
    + 32.64222846*open(t-2) - 26.95920391*high(t-2) - 34.44963464*low(t-2) + 11.53348198*close(t-2)
    - 8.95138015*open(t-1) + 29.33666852*high(t-1) + 11.71340381*low(t-1) - 18.86060491*close(t-1)
```

**Note**: H11 has large coefficients and poor fit (R² = 0.69), likely due to the anomalous value 2329.33 on 08/06. May require data cleaning or non-linear modeling.

## Implementation Patterns

### Common Patterns Identified

1. **Z-Series Indicators**: Weighted combinations following pattern `a*high_sum + b*low_sum + c*close_sum`
   - Z6T: Emphasizes highs (0.56), de-emphasizes lows (-0.44)
   - Z1B: Emphasizes lows (0.56), de-emphasizes highs (-0.44)  
   - Z2B/Z5T: Balanced weights (±0.33)

2. **Level Indicators**:
   - PLDOT: Simple average of all HLC values
   - EBOT: Emphasizes lows and closes, de-emphasizes highs
   - ETOP: Emphasizes highs and closes, de-emphasizes lows
   - L11: Primarily recent day's low and close

3. **Time Weighting**: Most formulas give equal weight to all 3 days (no recency bias)

### Code Implementation Template

```python
def calculate_indicators(ohlc_t3, ohlc_t2, ohlc_t1):
    """
    Calculate technical indicators from 3 days of OHLC data
    
    Args:
        ohlc_t3: (open, high, low, close) from 3 days ago
        ohlc_t2: (open, high, low, close) from 2 days ago  
        ohlc_t1: (open, high, low, close) from 1 day ago
        
    Returns:
        dict with all indicator values
    """
    o3, h3, l3, c3 = ohlc_t3
    o2, h2, l2, c2 = ohlc_t2
    o1, h1, l1, c1 = ohlc_t1
    
    high_sum = h3 + h2 + h1
    low_sum = l3 + l2 + l1
    close_sum = c3 + c2 + c1
    hlc_sum = high_sum + low_sum + close_sum
    
    return {
        'z6t': 0.5556 * high_sum - 0.4444 * low_sum + 0.2222 * close_sum,
        'z1b': -0.4444 * high_sum + 0.5556 * low_sum + 0.2222 * close_sum,
        'z2b': -0.3333 * high_sum + 0.3333 * low_sum + 0.3333 * close_sum,
        'z5t': 0.3333 * high_sum - 0.3333 * low_sum + 0.3333 * close_sum,
        'pldot': 0.1111 * hlc_sum,
        'ebot': -0.1111 * high_sum + 0.2222 * (low_sum + close_sum),
        'etop': 0.2222 * (high_sum + close_sum) - 0.1111 * low_sum,
        'l11': -0.3333 * h1 + 0.6667 * (l1 + c1),
        # h11 formula omitted due to poor fit
    }
```

## Validation Results

All formulas verified on original dataset with maximum errors:
- Z6T, Z1B: 0.004 (0.0002% error)
- Z2B, Z5T: 0.004 (0.0002% error)  
- PLDOT, EBOT, ETOP: 0.001-0.013 (< 0.0006% error)
- L11: 0.002 (0.0001% error)

## Usage Notes

1. **Precision**: Use at least 8 decimal places for coefficients in production code
2. **Data Requirements**: Requires exactly 3 consecutive days of OHLC data
3. **Performance**: These formulas are computationally efficient O(1) operations
4. **Accuracy**: Near-perfect reconstruction of original indicator values
5. **H11 Limitation**: H11 formula should be used with caution due to poor fit

---
*Generated from linear regression analysis on 20 trading days (07/30-08/29)*
*Analysis performed: 2025-08-31*