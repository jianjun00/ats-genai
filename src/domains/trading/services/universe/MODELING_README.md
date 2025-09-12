# Modeling Universe Creator

This module creates universes specifically designed for modeling purposes by filtering stocks based on market capitalization and trading volume criteria. It ensures adequate liquidity and size for reliable modeling and backtesting.

## Overview

The Modeling Universe Creator selects stocks that meet specific financial criteria:
- **Market capitalization** > $400M (20-day average)
- **Dollar trading volume** > $100M (20-day average)
- **Minimum trading days** for data completeness
- **Data quality filters** (price ranges, symbol format validation)

## Architecture

### Database Schema Integration

The system uses proper normalized database joins:

```
dev_daily_prices_polygon (price/volume data)
    ↓ JOIN on instrument_id
dev_instrument_xrefs (symbol mapping)
    ↓ vendor_id = 3 (ticker vendor)
dev_vendors (vendor definitions)
    ↓ LEFT JOIN on instrument_id + date
dev_daily_market_cap (market cap data)
```

**Key Schema Details:**
- **Symbol Column**: `vendor_symbol` (not `symbol` as in migration files)
- **Vendor ID**: 3 for ticker symbols
- **Market Cap Sources**: `dev_daily_market_cap` table with fallback estimation

### Data Sources

1. **Price/Volume Data**: `dev_daily_prices_polygon` (862k+ rows)
2. **Symbol Mapping**: `dev_instrument_xrefs` via `vendor_symbol` column
3. **Market Cap Data**: `dev_daily_market_cap` (10k rows)
4. **Fallback Estimation**: `close_price × volume × 0.0001` when market cap unavailable

## Usage

### Basic Usage

```bash
# Create universe with default criteria
PYTHONPATH=src python src/universe/modeling_universe_creator.py \
  --universe-name "modeling_400m_100m" \
  --min-market-cap 400 \
  --min-dollar-volume 100

# Create universe with relaxed criteria (dollar volume only)
PYTHONPATH=src python src/universe/modeling_universe_creator.py \
  --universe-name "modeling_volume_100m" \
  --min-market-cap 0 \
  --min-dollar-volume 100 \
  --max-stocks 50
```

### Command Line Options

```bash
python src/universe/modeling_universe_creator.py [OPTIONS]

Options:
  --universe-name TEXT      Name for the new universe [default: modeling_400m_100m]
  --min-market-cap FLOAT    Minimum market cap in millions USD [default: 400]
  --min-dollar-volume FLOAT Minimum daily dollar volume in millions [default: 100]
  --min-trading-days INT    Minimum trading days in lookback period [default: 20]
  --lookback-days INT       Calendar days to look back for data [default: 30]
  --max-stocks INT          Maximum number of stocks to include
  --report-file TEXT        Output file for selection report
  --debug                   Enable debug logging
```

### Programmatic Usage

```python
from universe.modeling_universe_creator import ModelingUniverseCreator
from config.environment import Environment

# Initialize
env = Environment()
creator = ModelingUniverseCreator(env)

# Create universe
universe_id = await creator.create_modeling_universe(
    universe_name="my_modeling_universe",
    min_market_cap_millions=400,
    min_dollar_volume_millions=100,
    min_trading_days=20,
    max_stocks=100
)

# Get qualifying stocks for analysis
stocks = await creator.get_qualifying_stocks(400, 100, 20, 30)
```

## Kubernetes Deployment

### Volume Mount Pattern (Recommended)

Deploy using ConfigMaps to avoid Docker rebuilds:

```bash
# Create ConfigMap with script
kubectl create configmap modeling-universe-script \
  --from-file=src/universe/modeling_universe_creator.py \
  -n ats-dev

# Deploy job with mounted script
kubectl apply -f k8s/modeling-universe-job.yaml
```

### Example Kubernetes Job

```yaml
apiVersion: batch/v1
kind: Job
metadata:
  name: modeling-universe-creation
  namespace: ats-dev
spec:
  template:
    spec:
      containers:
      - name: modeling-universe
        image: python:3.12-slim
        command: ["/bin/bash", "-c"]
        args:
        - |
          pip install asyncpg
          python /scripts/modeling_universe_creator.py \
            --universe-name "modeling_400m_100m" \
            --min-market-cap 400 \
            --min-dollar-volume 100 \
            --max-stocks 50
        env:
        - name: DB_HOST
          value: "postgres"
        - name: ENVIRONMENT
          value: "dev"
        # ... database credentials from secrets
        volumeMounts:
        - name: script-volume
          mountPath: /scripts
      volumes:
      - name: script-volume
        configMap:
          name: modeling-universe-script
```

## Current Data Status

### ✅ Working Components

- **Database Joins**: Successfully joins price data with symbol mapping
- **Volume Filtering**: Correctly identifies high-volume stocks
- **Symbol Resolution**: Properly maps instrument_id → vendor_symbol
- **Data Quality**: 862k+ price records with 20-day lookback

### 📊 Current Results (as of latest test)

Top stocks by dollar volume:
- **AAPL**: $13.9B daily volume ✅
- **MSFT**: $10.9B daily volume ✅
- **ADBE**: $1.3B daily volume ✅
- **ACN**: $1.2B daily volume ✅

### ⚠️ Market Cap Data Issue

Current `dev_daily_market_cap` table shows very small values (~$1M) that appear to need updating. The system provides fallback estimation but actual market cap data would improve filtering accuracy.

**Workaround**: Use dollar volume only filtering:
```bash
--min-market-cap 0 --min-dollar-volume 100
```

## Output

### Universe Creation

The system creates:
1. **Universe record** in `dev_universe` table with unique timestamped name
2. **Membership records** in `dev_universe_membership` table
3. **Detailed report** (optional) with selection statistics

### Sample Report

```markdown
# Modeling Universe Report
Generated: 2025-08-18T05:39:42.123456

## Selection Criteria
- Minimum average market cap: $400M
- Minimum average dollar volume: $100M
- Based on past 20 trading days

## Summary Statistics
- Total stocks selected: 15
- Average market cap: $2,500M
- Average daily dollar volume: $500M

## Selected Stocks
| Symbol | Instrument ID | Market Cap ($M) | Dollar Volume ($M) | Avg Price | Trading Days |
|--------|---------------|-----------------|--------------------|-----------| ------------|
| AAPL   | 30            | $2,500         | $13,863           | $216.82   | 20          |
| MSFT   | 360           | $2,200         | $10,915           | $519.36   | 20          |
```

## Development

### Testing

```bash
# Run with debug logging
PYTHONPATH=src python src/universe/modeling_universe_creator.py \
  --debug \
  --min-market-cap 10 \
  --min-dollar-volume 1 \
  --report-file test_report.md

# Test database connectivity
python k8s/check-actual-column-names.yaml  # Via Kubernetes
```

### Volume Mount Development Workflow

1. **Edit script** locally in `src/universe/modeling_universe_creator.py`
2. **Update ConfigMap**: `kubectl create configmap ... --dry-run=client -o yaml | kubectl apply -f -`
3. **Redeploy job**: `kubectl apply -f k8s/modeling-universe-job.yaml`
4. **Check logs**: `kubectl logs job/modeling-universe-creation -n ats-dev`

### Troubleshooting

**Connection Issues:**
- Ensure `DB_HOST=postgres` for Kubernetes deployment
- Use `DB_HOST=localhost` with port-forward for local testing

**Schema Issues:**
- Column name is `vendor_symbol` not `symbol`
- Vendor ID 3 corresponds to ticker symbols
- Environment prefixes: `dev_`, `intg_`, `prod_`

**No Results:**
- Check if market cap data exists: `SELECT COUNT(*) FROM dev_daily_market_cap`
- Use relaxed criteria: `--min-market-cap 0 --min-dollar-volume 1`
- Verify date ranges: data may be historical

## Modeling Principles

### 🧪 CRITICAL: Test-Driven Development

**ALWAYS follow this testing workflow for ANY change:**

1. **Add thorough tests for changes** - Before implementing new features or fixes
2. **When there is an error, first add tests to verify that the error can be detected**
3. **Then fix the logic and verify that tests pass**
4. **Never make changes without corresponding tests**

**Example Workflow:**
```bash
# 1. Write test that reproduces the issue (should fail)
PYTHONPATH=src python -m pytest tests/specific_issue_test.py -v
# ❌ Test should FAIL, proving we can detect the problem

# 2. Fix the actual code
# (implement the fix)

# 3. Verify test now passes
PYTHONPATH=src python -m pytest tests/specific_issue_test.py -v
# ✅ Test should now PASS

# 4. Run full test suite to prevent regressions
PYTHONPATH=src python -m pytest tests/ -v
```

This prevents introducing bugs and ensures all changes are validated.

### ⚠️ CRITICAL: Avoid Heuristic Rules

When developing models, adhere to these fundamental principles:

**❌ DON'T:**
- Use hardcoded thresholds (e.g., `if RSI > 70 then sell`)
- Implement heuristic trading rules (e.g., `if price > MA(20) and volume > 2x then buy`)
- Apply fixed cutoffs (e.g., `if P/E < 15 then value stock`)
- Create manual decision trees with arbitrary breakpoints

**✅ DO:**
- Present raw input features to the model
- Let the model infer optimal thresholds and combinations
- Use continuous feature engineering without discretization
- Allow the model to discover non-linear relationships
- Provide rich feature sets and let the model select relevance

**Example Approach:**

Instead of:
```python
# BAD: Hardcoded heuristic
if rsi > 70 and price > sma_20 and volume_ratio > 1.5:
    signal = "sell"
```

Use:
```python
# GOOD: Raw features for model inference
features = {
    'rsi_value': rsi,                    # Continuous RSI value
    'price_to_sma_ratio': price / sma_20, # Relative price position
    'volume_ratio': volume_ratio,         # Volume relative to average
    'momentum_1d': (price - prev_price) / prev_price,
    'volatility_10d': rolling_std_10d,
    'market_regime_score': regime_indicator
}
# Let model determine optimal decision boundaries
```

**Rationale:**
- Models can discover complex, non-linear relationships
- Thresholds can adapt to changing market conditions
- Reduces overfitting to specific historical periods
- Enables discovery of unexpected patterns
- Improves generalization across different market regimes

## Future Enhancements

1. **Market Cap Data**: Update `dev_daily_market_cap` with current values
2. **Shares Outstanding**: Integrate shares outstanding data from Polygon
3. **Multiple Criteria**: Add sector, industry, or geographic filters
4. **Dynamic Ranking**: Market cap vs liquidity weighting options
5. **Historical Universes**: Point-in-time universe creation for backtesting

## Dependencies

- **Python**: asyncpg, datetime, dataclasses
- **Database**: PostgreSQL with TimescaleDB
- **Environment**: config.environment.Environment
- **Kubernetes**: For production deployment

## Related Files

- `src/universe/modeling_universe_creator.py` - Main implementation
- `k8s/modeling-universe-job.yaml` - Kubernetes deployment
- `src/db/migrations/006_create_instrument_xrefs.sql` - Schema definition
- `DEPLOYMENT.md` - Volume mount patterns and deployment guide