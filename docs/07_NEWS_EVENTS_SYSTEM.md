# 📰 ATS News & Events System Guide

**Comprehensive news collection, event processing, sentiment analysis, and signal generation for the ATS platform.**

---

## 🔄 News & Events Architecture

### Complete Data Flow Pipeline

```
1. Real-time Data Sources
   ↓ Polygon, Tiingo, EODHD, FMP APIs
2. Multi-vendor News Collection
   ↓ Unified ingestion pipeline  
3. Event Classification & Processing
   ↓ Earnings, dividends, splits, economic events
4. LLM-based Sentiment Analysis
   ↓ Multi-modal news processing
5. Signal Generation & Broadcasting
   ↓ Trading signals and alerts
6. Historical Analysis & Research
```

### Data Sources & Collection

**News API Sources:**
```bash
# Verified working API keys for news collection
POLYGON_API_KEY="wfrcZNX3ZJJt55Or_CmBXda8G8e8tABD"      # Primary news, earnings, economic events
TIINGO_API_KEY="5f40b4f36e171405746304ec0e5a6f3aa9ca77e5"    # News sentiment, fundamentals
EODHD_API_KEY="68aa0c7d2fe831.67386369"                   # Historical news, corporate actions
FMP_API_KEY="Qf5MGG5HrOnEaWTumhVJzx3Onb3kw7Rr"            # Earnings, analyst ratings
ALPHA_VANTAGE_API_KEY="9GI0NZ3V4VNFX271"                  # Economic indicators
```

**News Collection Services:**
```bash
# Real-time news ingestion (INTG environment)
python scripts/run_intg.py start --service news-realtime

# Historical news backfill
python scripts/run_dev.py run --script scripts/polygon_news_backfill.py --start-date 2024-01-01 --end-date 2024-12-31

# Multi-vendor comprehensive collection
python scripts/run_dev.py run --script scripts/multi_vendor_news_backfill.py --symbols AAPL,TSLA,MSFT
```

---

## 📊 News Data Schema & Storage

### Database Tables

**Core News Tables:**
```sql
-- Real-time news storage
CREATE TABLE intg_news_realtime (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(10),
    title TEXT,
    content TEXT,
    published_at TIMESTAMP,
    source VARCHAR(50),
    sentiment_score DECIMAL(3,2),
    relevance_score DECIMAL(3,2),
    created_at TIMESTAMP DEFAULT NOW()
);

-- Historical news archive
CREATE TABLE intg_news_historical (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(10),
    title TEXT,
    content TEXT,
    published_at TIMESTAMP,
    source VARCHAR(50),
    metadata JSONB,
    created_at TIMESTAMP DEFAULT NOW()
);

-- News sentiment analysis
CREATE TABLE intg_news_llm_analysis (
    id SERIAL PRIMARY KEY,
    news_id INTEGER REFERENCES intg_news_realtime(id),
    sentiment_label VARCHAR(20),  -- positive, negative, neutral
    confidence_score DECIMAL(3,2),
    key_topics TEXT[],
    market_impact VARCHAR(20),    -- high, medium, low
    analysis_timestamp TIMESTAMP DEFAULT NOW()
);
```

**Event Processing Tables:**
```sql
-- Economic events
CREATE TABLE intg_economic_events (
    id SERIAL PRIMARY KEY,
    event_date DATE,
    event_type VARCHAR(100),
    country VARCHAR(10),
    actual_value DECIMAL,
    forecast_value DECIMAL,
    previous_value DECIMAL,
    impact_level VARCHAR(20),     -- HIGH, MEDIUM, LOW
    currency VARCHAR(10),
    created_at TIMESTAMP DEFAULT NOW()
);

-- Corporate events (earnings, splits, dividends)
CREATE TABLE intg_corporate_events (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(10),
    event_type VARCHAR(50),       -- earnings, split, dividend
    event_date DATE,
    details JSONB,
    market_reaction JSONB,
    created_at TIMESTAMP DEFAULT NOW()
);
```

### News Collection Status

**Monitor Collection Health:**
```bash
# Check recent news collection
python scripts/run_intg.py query --query "
SELECT source, COUNT(*) as articles, MAX(published_at) as latest_article
FROM intg_news_realtime 
WHERE created_at >= CURRENT_DATE - INTERVAL '24 hours'
GROUP BY source
ORDER BY articles DESC
"

# Verify data quality
python scripts/run_intg.py query --query "
SELECT 
    COUNT(*) as total_articles,
    COUNT(CASE WHEN sentiment_score IS NOT NULL THEN 1 END) as with_sentiment,
    AVG(relevance_score) as avg_relevance
FROM intg_news_realtime 
WHERE created_at >= CURRENT_DATE - INTERVAL '7 days'
"
```

---

## 🤖 LLM-Based News Analysis

### Sentiment Analysis Pipeline

**Multi-Modal LLM Processing:**
```bash
# Start enhanced news processing service
python scripts/run_intg.py start --service enhanced-news-llm

# Process recent news with LLM analysis
python scripts/run_dev.py run --script scripts/enhanced_news_llm_processor.py --symbols AAPL,TSLA --days 7

# Batch process historical news
python scripts/run_dev.py run --script scripts/historic_news_signal_extractor.py --start-date 2024-01-01 --end-date 2024-12-31
```

**LLM Analysis Features:**
- **Sentiment Classification**: Positive, negative, neutral with confidence scores
- **Key Topic Extraction**: Automated identification of market-relevant topics  
- **Market Impact Assessment**: High/medium/low impact scoring
- **Entity Recognition**: Company mentions, product releases, executive changes
- **Event Classification**: Earnings, mergers, regulatory changes, product launches

### News Signal Generation

**Trading Signal Extraction:**
```python
# Example LLM-based signal generation
from src.infrastructure.llm.event_analysis import NewsSignalExtractor

extractor = NewsSignalExtractor()
signals = extractor.analyze_news(
    symbol="AAPL",
    news_articles=recent_news,
    timeframe="1h"
)

# Signals include:
# - Momentum signals from breaking news
# - Volatility predictions from event analysis
# - Sector rotation signals from industry news
# - Risk signals from regulatory/legal news
```

**Signal Broadcasting System:**
```bash
# Start signal broadcasting service
python scripts/run_intg.py start --service signal-broadcaster

# Monitor signal generation
python scripts/run_intg.py query --query "
SELECT signal_type, COUNT(*) as signal_count, AVG(confidence) as avg_confidence
FROM intg_trading_signals 
WHERE created_at >= CURRENT_DATE - INTERVAL '24 hours'
GROUP BY signal_type
ORDER BY signal_count DESC
"
```

---

## 📈 Economic Events Integration

### FRED Economic Data

**Economic Indicators Collection:**
```bash
# Federal Reserve Economic Data (FRED) integration
python scripts/run_dev.py run --script scripts/populate_economic_events.py --indicators GDP,CPI,UNEMPLOYMENT,FOMC

# Monitor economic events impact
python scripts/run_intg.py query --query "
SELECT event_type, event_date, actual_value, forecast_value,
       CASE 
         WHEN ABS(actual_value - forecast_value) > ABS(forecast_value * 0.1) THEN 'SURPRISE'
         ELSE 'EXPECTED'
       END as surprise_factor
FROM intg_economic_events 
WHERE event_date >= CURRENT_DATE - INTERVAL '30 days'
AND impact_level = 'HIGH'
ORDER BY event_date DESC
"
```

**Economic Event Correlation:**
```bash
# Analyze market reactions to economic events
python scripts/run_dev.py run --script scripts/analyze_economic_correlation.py --events CPI,FOMC,GDP --symbols SPY,QQQ,TLT

# Generate economic-based trading signals
python scripts/run_dev.py run --script scripts/economic_signals_generator.py --lookback-days 90
```

### Event-Based Universe Management

**Universe State Integration:**
```bash
# Update universe membership based on events
python scripts/run_dev.py query --query "
UPDATE dev_universe_membership 
SET in_universe = false, exit_reason = 'earnings_miss'
WHERE symbol IN (
    SELECT symbol FROM intg_corporate_events 
    WHERE event_type = 'earnings' 
    AND details->>'eps_surprise' < '-0.1'
    AND event_date >= CURRENT_DATE - INTERVAL '5 days'
)
"

# Monitor universe changes due to events
python scripts/run_dev.py query --query "
SELECT exit_reason, COUNT(*) as exits, entry_reason, COUNT(*) as entries
FROM dev_universe_membership 
WHERE (exit_date >= CURRENT_DATE - INTERVAL '30 days' OR entry_date >= CURRENT_DATE - INTERVAL '30 days')
GROUP BY exit_reason, entry_reason
"
```

---

## 🚀 Real-time News Processing

### Production News Collection

**INTG Environment News Services:**
```bash
# Start comprehensive news collection (production-ready)
python scripts/run_intg.py start --service news-metrics

# Monitor collection status
curl -s http://localhost:4080/metrics | grep "news_"

# Check collection health via Grafana
open http://localhost:4002/d/news-collection-dashboard
```

**News Collection Cron Jobs:**
```cron
# Real-time news collection (every 15 minutes during market hours)
*/15 9-16 * * 1-5  ENVIRONMENT=intg POLYGON_API_KEY="wfrcZNX3ZJJt55Or_CmBXda8G8e8tABD" /home/jianjun/ats-genai-data/scripts/cron/daily_news_collection.sh

# Daily comprehensive news backfill
0 18 * * *  ENVIRONMENT=intg POLYGON_API_KEY="wfrcZNX3ZJJt55Or_CmBXda8G8e8tABD" /home/jianjun/ats-genai-data/scripts/comprehensive_news_backfill.py

# News health monitoring
0 */6 * * *  ENVIRONMENT=intg /home/jianjun/ats-genai-data/scripts/cron/news_health_monitor.sh
```

### News Processing Performance

**Performance Monitoring:**
```bash
# Monitor news processing throughput
python scripts/run_intg.py query --query "
SELECT DATE(created_at) as date, 
       COUNT(*) as articles_processed,
       AVG(EXTRACT(EPOCH FROM (analysis_timestamp - created_at))) as avg_processing_time
FROM intg_news_llm_analysis 
WHERE created_at >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY DATE(created_at)
ORDER BY date DESC
"

# Check processing bottlenecks
python scripts/run_dev.py query --query "
SELECT source, 
       COUNT(*) as total_articles,
       COUNT(CASE WHEN sentiment_score IS NOT NULL THEN 1 END) as processed_articles,
       ROUND(100.0 * COUNT(CASE WHEN sentiment_score IS NOT NULL THEN 1 END) / COUNT(*), 2) as processing_rate
FROM intg_news_realtime 
WHERE created_at >= CURRENT_DATE - INTERVAL '24 hours'
GROUP BY source
ORDER BY total_articles DESC
"
```

---

## 📊 News Analytics & Insights

### Sentiment Trend Analysis

**Symbol-Specific Sentiment Trends:**
```bash
# Track sentiment evolution for specific symbols
python scripts/run_intg.py query --query "
SELECT DATE(published_at) as date,
       symbol,
       AVG(sentiment_score) as avg_sentiment,
       COUNT(*) as article_count,
       STDDEV(sentiment_score) as sentiment_volatility
FROM intg_news_realtime 
WHERE symbol IN ('AAPL', 'TSLA', 'MSFT')
AND published_at >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY DATE(published_at), symbol
ORDER BY date DESC, symbol
"

# Identify sentiment momentum shifts
python scripts/run_dev.py run --script scripts/detect_sentiment_shifts.py --symbols AAPL,TSLA --lookback-days 14
```

**Market-Wide Sentiment Analysis:**
```bash
# Overall market sentiment from news
python scripts/run_intg.py query --query "
SELECT DATE(published_at) as date,
       AVG(sentiment_score) as market_sentiment,
       COUNT(*) as total_articles,
       COUNT(CASE WHEN sentiment_score > 0.6 THEN 1 END) as positive_news,
       COUNT(CASE WHEN sentiment_score < -0.6 THEN 1 END) as negative_news
FROM intg_news_realtime 
WHERE published_at >= CURRENT_DATE - INTERVAL '30 days'
AND relevance_score > 0.5
GROUP BY DATE(published_at)
ORDER BY date DESC
"
```

### Topic and Theme Analysis

**Key Topic Extraction:**
```bash
# Most discussed topics by time period
python scripts/run_intg.py query --query "
SELECT unnest(key_topics) as topic,
       COUNT(*) as mention_count,
       AVG(sentiment_score) as avg_sentiment
FROM intg_news_llm_analysis nla
JOIN intg_news_realtime nr ON nla.news_id = nr.id
WHERE nla.analysis_timestamp >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY topic
HAVING COUNT(*) > 5
ORDER BY mention_count DESC
LIMIT 20
"

# Theme-based signal generation
python scripts/run_dev.py run --script scripts/theme_based_signals.py --themes "AI,crypto,EV,green_energy" --lookback-days 30
```

---

## 🔍 Historical News Research

### News Archive Analysis

**Historical Pattern Recognition:**
```bash
# Analyze historical news impact on price movements
python scripts/run_dev.py run --script scripts/news_price_correlation.py --symbols AAPL,TSLA --years 2020-2024

# Research earnings announcement patterns
python scripts/run_dev.py run --script scripts/earnings_news_analysis.py --universe large_cap --quarters 8
```

**Event-Driven Research:**
```bash
# Study market reactions to specific event types
python scripts/run_intg.py query --query "
WITH news_events AS (
  SELECT symbol, published_at, sentiment_score, key_topics
  FROM intg_news_realtime nr
  JOIN intg_news_llm_analysis nla ON nr.id = nla.news_id
  WHERE 'earnings' = ANY(key_topics)
  AND published_at >= '2024-01-01'
),
price_reactions AS (
  SELECT symbol, date, 
         (close - open) / open as intraday_return,
         LAG(close) OVER (PARTITION BY symbol ORDER BY date) as prev_close
  FROM intg_daily_prices
  WHERE date >= '2024-01-01'
)
SELECT ne.symbol, 
       AVG(ne.sentiment_score) as avg_sentiment,
       AVG(pr.intraday_return) as avg_reaction
FROM news_events ne
JOIN price_reactions pr ON ne.symbol = pr.symbol 
AND DATE(ne.published_at) = pr.date
GROUP BY ne.symbol
HAVING COUNT(*) > 5
ORDER BY avg_reaction DESC
"
```

### Research Signal Backtesting

**News-Based Strategy Backtesting:**
```bash
# Backtest sentiment-based trading strategies
python scripts/run_dev.py run --script scripts/backtest_news_strategies.py --strategy momentum_sentiment --universe large_cap --years 2020-2024

# Test event-driven strategies
python scripts/run_dev.py run --script scripts/backtest_event_strategies.py --events earnings,FDA_approval,merger --lookback-years 3
```

---

## 🚨 News-Based Alerting

### Real-time Alert System

**Critical News Alerts:**
```bash
# Configure high-impact news alerts
python scripts/setup_news_alerts.py --impact-threshold high --sentiment-threshold 0.8

# Monitor for breaking news
python scripts/run_intg.py query --query "
SELECT symbol, title, published_at, sentiment_score, market_impact
FROM intg_news_realtime nr
JOIN intg_news_llm_analysis nla ON nr.id = nla.news_id
WHERE published_at >= NOW() - INTERVAL '1 hour'
AND (market_impact = 'high' OR ABS(sentiment_score) > 0.8)
ORDER BY published_at DESC
"
```

**Custom News Triggers:**
```bash
# Set up custom news monitoring
cat > config/news_alerts.yml << 'EOF'
alerts:
  - name: "FDA_Approval_News"
    keywords: ["FDA", "approval", "drug", "clinical trial"]
    symbols: ["JNJ", "PFE", "MRNA", "BNTX"]
    sentiment_threshold: 0.6
    actions: ["slack_notification", "trading_signal"]
    
  - name: "Earnings_Surprise"
    event_types: ["earnings"]
    surprise_threshold: 0.1
    actions: ["email_alert", "portfolio_review"]
EOF

python scripts/start_news_monitoring.py --config config/news_alerts.yml
```

---

## 📈 News-Driven Trading Integration

### Signal Generation from News

**News Signal APIs:**
```bash
# Get recent trading signals from news
curl -s http://localhost:4000/api/news_signals?hours=24 | jq

# Get sentiment-based momentum signals
curl -s http://localhost:4000/api/sentiment_signals?symbol=AAPL&timeframe=1h | jq

# Get event-driven signals
curl -s http://localhost:4000/api/event_signals?event_types=earnings,merger&days=7 | jq
```

**Integration with Trading System:**
```python
# Example news signal integration
from src.services.news_services.signal_generator import NewsSignalGenerator

generator = NewsSignalGenerator()

# Generate signals from recent news
signals = generator.generate_signals(
    symbols=['AAPL', 'TSLA', 'MSFT'],
    lookback_hours=6,
    signal_types=['momentum', 'volatility', 'mean_reversion']
)

# Integrate with portfolio management
for signal in signals:
    if signal.confidence > 0.7:
        # Send to trading system
        trading_system.process_signal(signal)
```

### Portfolio Impact Analysis

**News Impact on Holdings:**
```bash
# Analyze news impact on current portfolio
python scripts/run_dev.py run --script scripts/portfolio_news_impact.py --portfolio large_cap_growth --days 30

# Monitor news-driven risk factors
python scripts/run_intg.py query --query "
SELECT p.symbol, p.weight,
       AVG(nr.sentiment_score) as avg_sentiment,
       COUNT(CASE WHEN nla.market_impact = 'high' THEN 1 END) as high_impact_events
FROM portfolio_holdings p
JOIN intg_news_realtime nr ON p.symbol = nr.symbol
JOIN intg_news_llm_analysis nla ON nr.id = nla.news_id
WHERE nr.published_at >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY p.symbol, p.weight
ORDER BY high_impact_events DESC, ABS(avg_sentiment) DESC
"
```

---

**🎯 This news and events system provides comprehensive coverage of real-time news collection, LLM-based analysis, economic event integration, and trading signal generation for the ATS platform.**