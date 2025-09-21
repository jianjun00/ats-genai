# Feature Explorer - Product Requirements Document

## Overview

Transform training data generation into feature extraction system. Track which features are available for which symbols across date ranges. Store feature groups in monthly ArrayRecord files.

## Users

**Data Scientist**: Discover features, validate coverage, select for experiments
**ML Engineer**: Manage production features, monitor quality, deploy models  
**Quant Analyst**: Research signals, backtest features, validate computations

## User Journeys

**Feature Discovery**: Search "AAPL" → View available feature groups → Check date coverage → Preview values
**Coverage Analysis**: Select features + instruments + date range → Generate coverage heatmap → Export report  
**Value Exploration**: Select feature → Filter instruments/dates → View charts + tables → Export data
**Tag Management**: Select feature groups → Apply tags (experimental/validated/prod) → Notify teams
**Training Generation**: Select prod-tagged features → Define universe + parameters → Generate dataset
**Quality Monitoring**: Monitor feature quality → Detect alerts → Investigate issues → Trigger fixes

## Feature Groups

**ohlcv_basic**: timestamp, symbol, open, high, low, close, volume, vwap
**technical_momentum**: sma_20, ema_12, rsi_14, macd, macd_signal, momentum_1d, momentum_5d  
**technical_volatility**: bb_upper, bb_lower, bb_width, atr_14, realized_vol_20d, garch_vol
**fundamental_quarterly**: pe_ratio, pb_ratio, roe, debt_equity, revenue_growth, eps_growth

## Implementation

**Phase 1**: Database schema, feature extraction runner, ArrayRecord storage
**Phase 2**: Feature Explorer dashboard, coverage analysis, value exploration  
**Phase 3**: Tag management, quality monitoring, training data integration
**Phase 4**: Performance optimization, advanced analytics