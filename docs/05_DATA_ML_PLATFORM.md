# 🤖 ATS Data & ML Platform Guide

**Data pipelines, storage, ETL processes, ML training, inference, and optimization for the ATS platform.**

---

## 🔄 Data Flow Architecture

### Complete Data Pipeline: Minute Bars → Training Data

```
1. Minute Bar Files (Raw OHLCV Input)
   ↓ FileBasedMinuteManager (Reads parquet files)
2. Multi-Timeframe Data Manager
   ↓ Aggregates to 5m, 15m, 1h, 1d timeframes
3. Feature Engineering
   ↓ Technical indicators, universe state, market events
4. Training Data Generator
   ↓ Creates sequences with features and labels
5. ArrayRecord Training Datasets (ML-ready output)
```

**❌ DO NOT use `dev_daily_prices` - This table is NOT used for training data**

### Data Sources & Storage

**Raw Data Input:**
- **Location**: `/mnt/d/ats-data/minute-bars/firstrate/`
- **Structure**: `{first_letter}/{SYMBOL}/{YYYY}/{MM}/{SYMBOL}_{YYYY}_{MM}.parquet`
- **Example**: `/mnt/d/ats-data/minute-bars/firstrate/A/AAPL/2025/07/AAPL_2025_07.parquet`
- **Content**: Raw minute-level OHLCV data from market

**Training Data Output:**
- **Location**: `/data/training_data/` (container path)
- **Format**: ArrayRecord format only (.arrayrecord files)
- **Structure**: `{dataset_id}/SYMBOL_STARTDATETIME_ENDDATETIME/{timeframe}/SYMBOL_STARTDATETIME_ENDDATETIME.arrayrecord`
- **Example**: `dataset_20250909_080134/TSLA_20250701_000000_20250701_235959/5m/TSLA_20250701_000000_20250701_235959.arrayrecord`
- **Content**: QR4-compliant scalar data (timestamp, symbol, open, high, low, close, volume, vwap)
- **Timeframes**: 5m, 15m, 1h, 1d (each gets separate ArrayRecord file)

---

## 📊 Data Infrastructure

### Multi-Vendor Data Integration

**Vendor API Keys (Centralized):**
```bash
POLYGON_API_KEY="wfrcZNX3ZJJt55Or_CmBXda8G8e8tABD"      # Primary minute bars, news
TIINGO_API_KEY="5f40b4f36e171405746304ec0e5a6f3aa9ca77e5"    # Daily prices, fundamentals
EODHD_API_KEY="68aa0c7d2fe831.67386369"                   # Historical data, splits/dividends
FMP_API_KEY="Qf5MGG5HrOnEaWTumhVJzx3Onb3kw7Rr"            # Fundamentals, earnings
ALPHA_VANTAGE_API_KEY="9GI0NZ3V4VNFX271"                  # Economic indicators
```

**Data Collection Services:**
```bash
# Real-time minute bar collection
python scripts/run_intg.py start --service realtime-minute-collector

# News data collection
python scripts/run_intg.py start --service news-realtime

# Daily price backfill
python scripts/run_dev.py run --script scripts/daily_price_backfill.py

# Fundamental data population
python scripts/run_dev.py run --script scripts/populate_30year_fundamental_data.py
```

### Database Schema

**Core Data Tables:**
- `dev_instruments` / `intg_instruments` - Master symbol list with metadata
- `dev_daily_prices` / `intg_daily_prices` - Daily OHLCV data
- `dev_minute_bars` / `intg_minute_bars` - Minute-level data
- `dev_training_dataset` / `intg_training_dataset` - ML dataset registry
- `dev_universe_state_interval` - Universe membership over time
- `dev_runs` / `intg_runs` - Job execution tracking

**Training Data Registry:**
```sql
-- Example training dataset record
SELECT id, dataset_name, symbols, data_quality_score, total_sequences,
       creation_timestamp, file_size_mb
FROM dev_training_dataset
ORDER BY creation_timestamp DESC LIMIT 5;
```

### Data Quality & Validation

**Data Quality Checks:**
```bash
# Validate data completeness
python scripts/run_dev.py query --query "
SELECT symbol, COUNT(*) as records, MIN(date) as start_date, MAX(date) as end_date
FROM dev_daily_prices
WHERE date >= '2024-01-01'
GROUP BY symbol
HAVING COUNT(*) < 250  -- Missing data check
"

# Check for data anomalies
python scripts/run_dev.py query --query "
SELECT symbol, date, close, volume
FROM dev_daily_prices
WHERE close <= 0 OR volume < 0 OR close > 10000  -- Unrealistic values
ORDER BY date DESC LIMIT 10
"

# Validate training data integrity
python scripts/validate_training_data.py --dataset-id 1
```

---

## 🤖 ML Training Pipeline

### Training Data Generation Workflow

**1. Generate Training Data Using Gin Config:**
```bash
# Generate training data with metadata tracking
python scripts/run_dev.py run --script src/domains/ml/services/training_data/runners/training_data_callback_runner.py

# Check the run was tracked
python scripts/run_dev.py query --query "SELECT MAX(id) as latest_run_id FROM dev_runs WHERE run_type = 'training_data_generation'"

# Get run details including gin config used
python scripts/run_dev.py get --run-id <latest_run_id>
```

**2. Verify Generated Datasets:**
```bash
# Check generated training datasets
python scripts/run_dev.py query --query "SELECT id, dataset_name, creation_timestamp FROM dev_training_dataset ORDER BY creation_timestamp DESC LIMIT 5"

# Get comprehensive dataset details
python scripts/run_dev.py training_dataset get <dataset_id>
# Shows: dataset_name, symbols, date ranges, sequence info, quality metrics, technical indicators
```

**3. Training Data Structure:**
```bash
# ArrayRecord files organized by timeframe
/data/training_data/{dataset_id}/SYMBOL_STARTDATETIME_ENDDATETIME/{timeframe}/SYMBOL_STARTDATETIME_ENDDATETIME.arrayrecord

# Example structure:
# /data/training_data/dataset_20250701_120000/TSLA_20250701_000000_20250701_235959/5m/TSLA_20250701_000000_20250701_235959.arrayrecord
# /data/training_data/dataset_20250701_120000/TSLA_20250701_000000_20250701_235959/15m/TSLA_20250701_000000_20250701_235959.arrayrecord
# /data/training_data/dataset_20250701_120000/TSLA_20250701_000000_20250701_235959/1h/TSLA_20250701_000000_20250701_235959.arrayrecord

# Container path mapping: /data/training_data (container) = /mnt/d/ats-data/training-data (host)
```

### Run Tracking & Metadata

**Training Run Management:**
```bash
# Track training data generation runs with gin config
python scripts/run_dev.py get --run-id 35    # Shows: command_line, gin config, git hash, environment

# List recent runs by type
python scripts/run_dev.py query --query "SELECT id, run_type, status, LEFT(command_line, 50) as command FROM dev_runs WHERE run_type = 'training_data_generation' ORDER BY id DESC LIMIT 5"

# View all runs with gin config tracking
python scripts/run_dev.py query --query "SELECT id, run_type, status, created_at FROM dev_runs ORDER BY id DESC LIMIT 10"
```

**Dataset Quality Metrics:**
```bash
# List training datasets with key metrics
python scripts/run_dev.py query --query "SELECT id, dataset_name, symbols, data_quality_score, file_size_mb, total_sequences FROM dev_training_dataset ORDER BY creation_timestamp DESC"

# Find datasets by symbol
python scripts/run_dev.py query --query "SELECT id, dataset_name, symbols, creation_timestamp FROM dev_training_dataset WHERE symbols LIKE '%AAPL%'"

# Check dataset quality metrics
python scripts/run_dev.py query --query "SELECT dataset_name, data_quality_score, feature_completeness, label_completeness FROM dev_training_dataset WHERE data_quality_score > 0.9"
```

---

## 🧠 ML Model Training

### GPU-Enabled Training

**Training with GPU Support:**
```bash
# Run ML training with GPU
python scripts/run_dev.py run --script scripts/training/train_model.py --gpu

# Training with specific dataset
python scripts/run_dev.py run --script scripts/train_unified_loss_REAL_DATA_ONLY.py

# Autonomous transformer training
python scripts/run_dev.py run --script scripts/train_autonomous_transformer_real_data.py
```

**Model Training Pipeline:**
```bash
# 1. Load training dataset
python -c "
import src.domains.ml.services.training_data.dao.training_dataset_dao as dao
dataset = dao.get_training_dataset(dataset_id=1)
print(f'Dataset: {dataset.dataset_name}, Sequences: {dataset.total_sequences}')
"

# 2. Start training with progress tracking
python scripts/run_dev.py run --script scripts/training/train_unified_loss.py --dataset-id 1 --epochs 100 --gpu

# 3. Monitor training progress
python scripts/run_dev.py query --query "SELECT id, run_type, status, progress FROM dev_runs WHERE run_type = 'model_training' ORDER BY id DESC LIMIT 5"
```

### Model Registry & Versioning

**Model Storage:**
```bash
# Models saved to container volume
/workspace/models/unified_loss_transformer_REAL_DATA_ONLY_run_{run_id}_{timestamp}.pth

# Model metadata in database
python scripts/run_dev.py query --query "
SELECT model_name, version, dataset_id, training_metrics, created_at
FROM dev_model_registry
ORDER BY created_at DESC LIMIT 5
"
```

**Model Evaluation:**
```bash
# Load and evaluate model
python -c "
import torch
model = torch.load('/workspace/models/unified_loss_transformer_REAL_DATA_ONLY_run_43440_20250906_194057.pth')
print(f'Model loaded: {type(model)}')
"

# Run model inference
python scripts/run_dev.py run --script scripts/inference/run_model_inference.py --model-path models/latest_model.pth
```

---

## 📈 Feature Engineering

### Multi-Timeframe Features

**Technical Indicators:**
```python
# Available indicators by timeframe
{
    "5m": ["sma_20", "ema_12", "rsi_14", "macd", "bb_upper", "bb_lower", "volume_sma"],
    "15m": ["sma_50", "ema_26", "adx_14", "stoch_k", "stoch_d", "atr_14"],
    "1h": ["sma_200", "momentum_10", "williams_r", "cci_20", "roc_12"],
    "1d": ["long_sma_200", "price_change_30d", "volume_ratio", "volatility_30d"]
}
```

**Universe State Features:**
```bash
# Universe membership over time
python scripts/run_dev.py query --query "
SELECT symbol, date, in_universe, market_cap_rank, sector, industry
FROM dev_universe_state_interval
WHERE date = '2024-01-01' AND in_universe = true
ORDER BY market_cap_rank LIMIT 10
"
```

**Economic Event Features:**
```bash
# Economic indicators integration
python scripts/run_dev.py query --query "
SELECT event_date, event_type, actual_value, forecast_value, impact_level
FROM dev_economic_events
WHERE event_date >= '2024-01-01'
AND impact_level = 'HIGH'
ORDER BY event_date DESC LIMIT 10
"
```

### Custom Feature Development

**Create New Technical Indicators:**
```python
# Example: Smart Money Zones indicator
from src.domains.trading.services.indicators.smart_money_zones import SmartMoneyZones

smz = SmartMoneyZones()
zones = smz.calculate(price_data, volume_data, timeframe='15m')
```

**Volume Profile Features:**
```python
# Volume profile analysis
from src.signals.volume_profile import VolumeProfileIndicator

vp = VolumeProfileIndicator()
profile = vp.calculate_profile(ohlcv_data, num_levels=20)
```

---

## 🔄 Data Processing Performance

### Batch Processing

**Large Dataset Processing:**
```bash
# Process multiple symbols in parallel
python scripts/run_dev.py run --script scripts/batch_processing/parallel_training_data.py --symbols AAPL,TSLA,MSFT,GOOGL --workers 4

# Ray distributed processing
python scripts/run_dev.py run --script scripts/ray_training_data_generation.py --cluster-size 4
```

**Performance Optimization:**
```bash
# Monitor processing performance
python scripts/run_dev.py query --query "
SELECT run_id, symbols, processing_time_seconds, memory_usage_mb
FROM dev_runs
WHERE run_type = 'training_data_generation'
ORDER BY processing_time_seconds DESC LIMIT 10
"

# Identify bottlenecks
python -m cProfile -o profile.stats scripts/training_data_generation.py
```

### Memory Management

**Efficient Data Loading:**
```python
# Memory-efficient ArrayRecord reading
import arrayrecord

# Read specific sequences without loading entire dataset
reader = arrayrecord.ArrayRecordReader('/data/training_data/dataset_123/AAPL_20240101/5m/data.arrayrecord')
for i in range(0, 1000, 100):  # Read every 100th sequence
    sequence = reader.read_record(i)
```

**Memory Monitoring:**
```bash
# Monitor memory usage during processing
docker stats ats-dev-analytics --format "table {{.Container}}\t{{.CPUPerc}}\t{{.MemUsage}}\t{{.MemPerc}}"

# Check for memory leaks
python -m memory_profiler scripts/your_script.py
```

---

## 📊 Data Analytics & EDA

### Interactive Data Analysis

**EDA Dashboard Access:**
```bash
# Access browser-based EDA interface
open http://localhost:3000/eda

# API endpoints for programmatic access
curl -s http://localhost:3000/api/datasets | jq
curl -s http://localhost:3000/api/datasets/1/sequences?limit=100 | jq
```

**Training Data Visualization:**
```bash
# Browser-based sequence exploration
PYTHONPATH=src python3 -m pytest tests/browser_tests/test_eda_playwright.py -v

# Generate training data visualizations
python scripts/show_volume_profile_training_examples.py
```

### Data Quality Monitoring

**Real-time Data Validation:**
```bash
# Monitor data ingestion quality
curl -s http://localhost:4080/metrics | grep "data_quality"

# Daily data validation reports
python scripts/daily_prices_validation.py

# Training data consistency checks
python scripts/check_training_data_consistency.py
```

**Data Coverage Analysis:**
```bash
# Check symbol coverage
python scripts/run_dev.py query --query "
SELECT COUNT(DISTINCT symbol) as total_symbols,
       COUNT(DISTINCT CASE WHEN date >= CURRENT_DATE - INTERVAL '30 days' THEN symbol END) as active_symbols
FROM dev_daily_prices
"

# Identify data gaps
python scripts/detect_missing_minute_bars.py --start-date 2024-01-01 --end-date 2024-12-31
```

---

## 🚀 Model Deployment & Inference

### Model Serving

**Real-time Inference API:**
```bash
# Start model serving endpoint
python scripts/run_dev.py start --service model-server

# Test model inference
curl -X POST http://localhost:8000/api/predict \
  -H "Content-Type: application/json" \
  -d '{"symbol": "AAPL", "timeframe": "5m", "sequence_length": 100}'
```

**Batch Inference:**
```bash
# Run batch predictions for multiple symbols
python scripts/run_dev.py run --script scripts/inference/batch_predict.py --symbols AAPL,TSLA,MSFT --model latest

# Generate trading signals
python scripts/run_dev.py run --script scripts/signals/generate_trading_signals.py --universe large_cap
```

### Model Monitoring

**Performance Tracking:**
```bash
# Model performance metrics
curl -s http://localhost:4080/metrics | grep "model_"

# Prediction accuracy monitoring
python scripts/run_dev.py query --query "
SELECT model_version, avg(accuracy_score) as avg_accuracy, count(*) as predictions
FROM dev_model_predictions
WHERE created_at >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY model_version
ORDER BY avg_accuracy DESC
"
```

**A/B Testing:**
```bash
# Compare model versions
python scripts/model_comparison.py --model-a v1.2.0 --model-b v1.3.0 --test-period 30d

# Gradual model rollout
python scripts/deploy_model.py --version v1.3.0 --traffic-split 0.1  # 10% traffic to new model
```

---

## 🔧 Development Tools & Debugging

### Data Pipeline Debugging

**Debug Training Data Generation:**
```bash
# Debug specific symbol processing
python scripts/debug_training_data.py --symbol AAPL --date 2024-01-01 --timeframe 5m

# Validate ArrayRecord files
python scripts/validate_arrayrecord.py --file /data/training_data/dataset_123/AAPL_20240101/5m/data.arrayrecord

# Check data pipeline health
python scripts/run_dev.py run --script scripts/debug_data_pipeline.py --full-check
```

**Feature Engineering Testing:**
```bash
# Test individual indicators
python -c "
from src.signals.indicator import TechnicalIndicators
ti = TechnicalIndicators()
result = ti.calculate_rsi([1, 2, 3, 4, 5], period=14)
print(f'RSI calculation: {result}')
"

# Validate feature consistency
python scripts/validate_features.py --dataset-id 1 --check-all
```

### Performance Profiling

**Training Performance Analysis:**
```bash
# Profile training script
python -m cProfile -o training_profile.stats scripts/train_model.py
python -c "
import pstats
p = pstats.Stats('training_profile.stats')
p.sort_stats('cumulative').print_stats(10)
"

# GPU utilization monitoring
nvidia-smi -l 1  # Monitor GPU usage during training
```

---

**🎯 This data and ML platform guide provides comprehensive coverage of data pipelines, feature engineering, model training, and deployment workflows for the ATS platform.**