# ATS Event System Redesign: PRD & DRD (Python-Based)

**Version**: 2.0  
**Date**: January 2025  
**Authors**: ATS Engineering Team  
**Status**: Updated for Python/Hourly Event Processing  

---

# Product Requirements Document (PRD)

## 1. Executive Summary

### 1.1 Vision Statement
Redesign the ATS event system to create a unified, Python-native event-driven architecture that processes financial events with hourly frequency while providing correlation detection and seamless integration with training datasets using Protocol Buffers for serialization.

### 1.2 Problem Statement
The current ATS event system lacks standardized event representation and efficient serialization for both database persistence and training dataset storage. We need a Python-based architecture with Protocol Buffer schemas that enables consistent event handling across the entire ML pipeline.

### 1.3 Success Criteria
- **Performance**: Process 1K-10K events/hour with <30 second end-to-end processing
- **Integration**: Seamless event flow from ingestion → database → training datasets
- **Standardization**: Protocol Buffer schemas for all event types with versioning
- **Intelligence**: 40% reduction in false signals through improved event correlation
- **Reliability**: 99.9% system availability with simple recovery mechanisms

## 2. Current State Analysis

### 2.1 Existing Capabilities ✅
- **Professional Database Schema**: Comprehensive financial events tables with proper constraints
- **Multi-Vendor Integration**: News, earnings, economic data from Polygon, Tiingo, Alpha Vantage
- **Real-Time Processing**: Sub-30 second news ingestion with deduplication
- **ML Integration**: Event features for backtesting and signal generation
- **LLM Enhancement**: Advanced news analysis and sentiment extraction

### 2.2 Critical Gaps ❌
- **No Event Streaming Platform**: Missing Kafka/Pulsar for reliable event distribution
- **Limited Event Sourcing**: No append-only event store for complete audit trails
- **Basic Correlation**: No real-time event correlation or causality modeling
- **Scalability Constraints**: Queue-based processing limits high-frequency event handling
- **Missing Standards**: No schema evolution strategy or event versioning

## 3. Product Requirements

### 3.1 Functional Requirements

#### FR1: Universal Event Taxonomy
- **Priority**: P0 (Critical)
- **Description**: Standardized event classification system covering all financial event types
- **Acceptance Criteria**:
  - Support for 6 core event types: News, Earnings, Economic, Corporate Actions, Technical Signals, Market Structure
  - Extensible taxonomy allowing new event types without schema changes
  - Consistent event IDs across all systems and vendors

#### FR2: Python-Native Event Processing
- **Priority**: P0 (Critical)  
- **Description**: Python-based event processing pipeline with Redis queue and Celery workers
- **Acceptance Criteria**:
  - Process 1K-10K events/hour sustained throughput  
  - <30 second end-to-end processing latency (99th percentile)
  - At-least-once delivery semantics with idempotency
  - Simple recovery mechanisms using existing Docker infrastructure

#### FR3: Event Correlation Engine
- **Priority**: P1 (High)
- **Description**: Real-time detection of event relationships and causal patterns
- **Acceptance Criteria**:
  - Detect temporal correlations within sliding time windows
  - Identify causal chains across different event types
  - Support cross-asset event correlation analysis
  - Generate correlation confidence scores (0.0-1.0 scale)

#### FR4: Advanced Event Processing
- **Priority**: P1 (High)
- **Description**: Complex event processing (CEP) for pattern recognition and signal generation
- **Acceptance Criteria**:
  - Support temporal patterns (sequences, intervals, trends)
  - Real-time anomaly detection and alerting
  - Configurable event filtering and routing rules
  - Integration with ML models for predictive analytics

#### FR5: Event Sourcing & Audit Trails
- **Priority**: P1 (High)
- **Description**: Complete event history with immutable audit trails for compliance
- **Acceptance Criteria**:
  - Append-only event store with cryptographic integrity
  - Point-in-time event replay capabilities
  - Complete lineage tracking from source to derived events
  - Regulatory-compliant data retention and archival

### 3.2 Non-Functional Requirements

#### NFR1: Performance
- **Ingestion Latency**: <5 seconds (99th percentile) 
- **Processing Latency**: <30 seconds end-to-end for critical events
- **Throughput**: 1K-10K events/hour sustained
- **Query Response**: <100ms for cached event lookups, <500ms for complex queries

#### NFR2: Scalability  
- **Horizontal Scaling**: Scale to 5-10 Celery workers as needed
- **Storage**: Support terabyte-scale event storage with PostgreSQL partitioning
- **Concurrent Users**: Support 50-100 simultaneous API consumers
- **Data Volume**: Handle 1-10GB daily event ingestion

#### NFR3: Reliability
- **Availability**: 99.9% uptime (8.76 hours downtime/year)
- **Durability**: 99.99% event durability with PostgreSQL backups
- **Recovery**: <15 minutes recovery from failures using run_dev restart
- **Data Consistency**: Strong consistency for all events using PostgreSQL transactions

#### NFR4: Security & Compliance
- **Encryption**: End-to-end encryption for all event data
- **Access Control**: Role-based permissions with audit logging
- **Compliance**: SOX, MiFID II, and Dodd-Frank compliance requirements
- **Data Privacy**: GDPR-compliant data handling and retention

## 4. User Stories & Use Cases

### 4.1 Primary Personas

#### Quantitative Researcher
- **Goal**: Access real-time and historical events for strategy development
- **User Stories**:
  - "As a quant researcher, I want to query correlated events across timeframes to identify alpha opportunities"
  - "As a quant researcher, I want to replay historical events to backtest trading strategies"
  - "As a quant researcher, I want to detect event patterns that predict market movements"

#### Algorithmic Trader  
- **Goal**: React to market events with minimal latency for execution
- **User Stories**:
  - "As an algo trader, I want to receive real-time event notifications to trigger trading decisions"
  - "As an algo trader, I want event prioritization to focus on high-impact market movers"
  - "As an algo trader, I want event confidence scores to filter noise from signals"

#### Risk Manager
- **Goal**: Monitor and respond to risk events across portfolios
- **User Stories**:
  - "As a risk manager, I want real-time alerts for events affecting portfolio positions"
  - "As a risk manager, I want to correlate events with portfolio P&L movements"
  - "As a risk manager, I want to simulate event impacts on portfolio risk metrics"

#### Compliance Officer
- **Goal**: Ensure regulatory compliance and audit capabilities
- **User Stories**:
  - "As a compliance officer, I want immutable audit trails for all trading-related events"
  - "As a compliance officer, I want to generate regulatory reports from event data"
  - "As a compliance officer, I want to monitor for potential market manipulation patterns"

### 4.2 Critical Use Cases

#### UC1: Real-Time News Impact Analysis
1. **Trigger**: News event published by vendor (Polygon, Bloomberg)
2. **Process**: 
   - Event ingested via streaming API
   - NLP processing for sentiment and entity extraction
   - Asset impact analysis and correlation
   - Signal generation and distribution
3. **Outcome**: Trading signals delivered within 50ms of news publication
4. **Success Metrics**: <50ms end-to-end latency, >95% signal accuracy

#### UC2: Earnings Surprise Detection
1. **Trigger**: Earnings announcement published
2. **Process**:
   - Compare actual vs. consensus estimates
   - Calculate surprise magnitude and significance
   - Analyze historical price reactions to similar surprises
   - Generate probability-weighted expected moves
3. **Outcome**: Pre-market position adjustments based on earnings impact
4. **Success Metrics**: >80% accuracy in directional predictions

#### UC3: Multi-Asset Event Correlation
1. **Trigger**: Significant event affecting multiple assets
2. **Process**:
   - Detect cross-asset correlation patterns
   - Identify lead-lag relationships
   - Calculate contagion probabilities
   - Generate hedging recommendations
3. **Outcome**: Portfolio risk adjusted based on event correlations
4. **Success Metrics**: 30% reduction in portfolio volatility during event periods

#### UC4: Regulatory Compliance Monitoring
1. **Trigger**: Continuous monitoring of all events and trades
2. **Process**:
   - Pattern detection for potential violations
   - Automated report generation
   - Alert generation for suspicious activities
   - Audit trail maintenance
3. **Outcome**: Proactive compliance monitoring with automated reporting
4. **Success Metrics**: 99.9% detection accuracy, <1 hour violation reporting

## 5. Business Impact & Metrics

### 5.1 Key Performance Indicators (KPIs)

#### Revenue Impact
- **Alpha Generation**: 25% improvement in information ratio
- **Latency Advantage**: 50% faster market reaction time
- **Signal Quality**: 40% reduction in false positive signals
- **Risk-Adjusted Returns**: 15% improvement in Sharpe ratio

#### Operational Efficiency  
- **Processing Costs**: 60% reduction through efficient streaming architecture
- **Compliance Costs**: 90% reduction in manual regulatory reporting
- **System Maintenance**: 50% reduction in event processing incidents
- **Development Velocity**: 3x faster feature development with standardized events

#### Risk Reduction
- **Event Miss Rate**: <0.1% critical events missed or delayed
- **System Downtime**: <26 minutes per year (99.95% availability)
- **Data Quality**: >99.9% event accuracy and completeness
- **Compliance Violations**: Zero violations due to system failures

### 5.2 ROI Calculation
- **Implementation Cost**: $2.5M (development + infrastructure)
- **Annual Benefits**: $8M (alpha generation + cost savings)
- **Payback Period**: 4 months
- **3-Year NPV**: $21.5M at 10% discount rate

---

# Design Requirements Document (DRD)

## 6. Technical Architecture

### 6.1 Python-Native High-Level Architecture

```
┌─────────────────┐    ┌──────────────┐    ┌─────────────────┐
│  Event Sources  │────│ Redis Queue  │────│ Celery Workers  │
│  (Polygon, etc.)│    │   (LPUSH)    │    │ (Python + Proto)│
└─────────────────┘    └──────────────┘    └─────────────────┘
                              │                       │
                       ┌──────────────┐    ┌─────────────────┐
                       │ PostgreSQL   │    │   FastAPI       │
                       │ (JSONB Events│    │ (GraphQL + REST)│
                       │  + Proto)    │    │                 │
                       └──────────────┘    └─────────────────┘
                              │                       │
                       ┌──────────────┐    ┌─────────────────┐
                       │Training Data │    │   Frontend      │
                       │(.riegeli     │    │   (React/Next)  │
                       │ Proto Arrays)│    │                 │
                       └──────────────┘    └─────────────────┘
```

### 6.2 Python-Based Technology Stack

#### Event Queue Layer
- **Redis**: Simple, reliable message queue
  - **Justification**: Python-native, handles 10K events/hour easily, existing Docker setup
  - **Configuration**: Single Redis instance with persistence, 1-day retention
  - **Usage**: LPUSH for producers, BRPOP for consumers

#### Event Processing
- **Celery**: Distributed task queue for Python
  - **Justification**: Perfect for Python, handles async processing, integrates with existing codebase
  - **Use Cases**: Event processing, correlation detection, database storage
  - **Scaling**: 5-10 workers based on load, can scale horizontally

#### Protocol Buffers Integration
- **protobuf**: Event serialization for DB and training datasets
  - **Database**: Store proto as JSONB in PostgreSQL using `MessageToDict()`
  - **Training Data**: Serialize proto arrays to .riegeli files  
  - **Python**: Generated classes provide type safety and validation

#### Event Storage
- **PostgreSQL**: Hot storage for recent events (1-3 months)
  - **Schema**: Time-partitioned tables with JSONB for flexibility
  - **Indexing**: GiST indexes on event metadata, temporal indexes
  - **Replication**: Master-replica with automatic failover

- **ClickHouse**: Analytical queries and historical storage
  - **Justification**: Columnar storage, exceptional query performance
  - **Use Cases**: Event analytics, correlation analysis, reporting
  - **Retention**: 2+ years with automatic compression

- **Amazon S3**: Cold storage and archival (3+ years)
  - **Format**: Parquet files with Snappy compression
  - **Organization**: Partitioned by year/month/day/hour
  - **Access**: Athena/Presto for ad-hoc analytical queries

#### API & Query Layer
- **GraphQL**: Unified API for event queries and subscriptions
  - **Real-time**: WebSocket subscriptions for live event streams
  - **Batching**: DataLoader pattern for efficient bulk queries
  - **Caching**: Redis for frequently accessed event data

### 6.3 Protocol Buffer Event Schema Design

#### 6.3.1 Core Event Protocol Buffer Schema

**Key Requirements**:
- **Database Persistence**: Events stored as JSONB in PostgreSQL 
- **Training Dataset Integration**: Events serialized as proto arrays in .riegeli files
- **Python Native**: Generated Python classes for type safety
- **Versioning**: Forward/backward compatibility with schema evolution

```protobuf
// events.proto - Core event schema
syntax = "proto3";
package ats.events.v1;

import "google/protobuf/timestamp.proto";
import "google/protobuf/struct.proto";

// Core event message - used for DB storage and training datasets
message Event {
  // Identity & Versioning
  string event_id = 1;           // UUID v4
  EventType event_type = 2;      // Enum of supported event types  
  string event_version = 3;      // Semantic versioning (e.g., "1.2.0")
  
  // Temporal Properties
  google.protobuf.Timestamp timestamp = 4;        // Event occurrence time
  string time_zone = 5;                           // IANA timezone identifier  
  google.protobuf.Timestamp valid_from = 6;       // Optional validity start
  google.protobuf.Timestamp valid_to = 7;         // Optional validity end
  
  // Source & Attribution
  string source = 8;                              // Vendor/system (e.g., "polygon", "tiingo")
  string source_id = 9;                           // Original ID from source system
  google.protobuf.Timestamp ingestion_time = 10;  // When event entered ATS system
  
  // Event Relationships
  string causation_id = 11;      // ID of event that caused this event
  string correlation_id = 12;    // Business process or workflow ID
  string parent_event_id = 13;   // Hierarchical parent relationship
  string root_event_id = 14;     // Root event in event chain
  
  // Subject (What the event is about)
  EventSubject subject = 15;
  
  // Event-specific data (polymorphic based on event_type)
  oneof event_data {
    NewsEventData news_data = 16;
    EarningsEventData earnings_data = 17;
    TechnicalSignalEventData technical_data = 18;
    CorporateActionEventData corporate_action_data = 19;
    EconomicEventData economic_data = 20;
  }
  
  // System metadata
  EventMetadata metadata = 21;
  
  // Quality indicators
  double confidence = 22;        // Confidence score (0.0-1.0)
  string reliability = 23;       // Source reliability rating
}

enum EventType {
  EVENT_TYPE_UNSPECIFIED = 0;
  // External Events
  EVENT_TYPE_NEWS = 1;
  EVENT_TYPE_EARNINGS = 2;
  EVENT_TYPE_CORPORATE_ACTION = 3;
  EVENT_TYPE_ECONOMIC_INDICATOR = 4;
  EVENT_TYPE_ANALYST_RECOMMENDATION = 5;
  EVENT_TYPE_REGULATORY_FILING = 6;
  // Internal Events (Generated by ATS)
  EVENT_TYPE_PRICE_GAP = 7;
  EVENT_TYPE_SUPPORT_RESISTANCE_BREAK = 8; 
  EVENT_TYPE_VOLUME_ANOMALY = 9;
  EVENT_TYPE_TECHNICAL_SIGNAL = 10;
  EVENT_TYPE_RISK_ALERT = 11;
}

message EventSubject {
  string instrument_id = 1;      // Internal instrument identifier
  string symbol = 2;             // Ticker symbol (e.g., "AAPL")
  string isin = 3;               // International identifier
  string cusip = 4;              // US identifier  
  string exchange = 5;           // Exchange code (e.g., "NASDAQ")
  string asset_class = 6;        // "equity", "bond", "derivative", etc.
  string sector = 7;             // GICS sector classification
  string industry = 8;           // GICS industry classification
  string country = 9;            // ISO country code
  string currency = 10;          // ISO currency code
}

message EventMetadata {
  Priority priority = 1;
  Classification classification = 2;
  repeated string tags = 3;      // Searchable tags
  string processed_by = 4;       // System/service that processed event
  int64 processing_time_ms = 5;  // Processing duration in milliseconds
  int32 retry_count = 6;         // Number of processing retries
  string checksum = 7;           // Content integrity hash
}

enum Priority {
  PRIORITY_UNSPECIFIED = 0;
  PRIORITY_LOW = 1;
  PRIORITY_MEDIUM = 2;
  PRIORITY_HIGH = 3;
  PRIORITY_CRITICAL = 4;
}

enum Classification {
  CLASSIFICATION_UNSPECIFIED = 0;
  CLASSIFICATION_PUBLIC = 1;
  CLASSIFICATION_INTERNAL = 2;
  CLASSIFICATION_CONFIDENTIAL = 3;
  CLASSIFICATION_RESTRICTED = 4;
}
```

#### 6.3.2 Event-Specific Data Structures

**News Event Data:**
```protobuf
message NewsEventData {
  string headline = 1;
  string summary = 2;
  string full_text = 3;
  string url = 4;
  string author = 5;
  string publisher = 6;
  string language = 7;
  
  // Sentiment analysis
  SentimentAnalysis sentiment = 8;
  
  // Named entities
  repeated Entity entities = 9;
  repeated string categories = 10;
  double importance = 11;        // 0.0 to 1.0 importance score
  
  // Market impact assessment
  MarketImpact market_impact = 12;
}

message SentimentAnalysis {
  double overall = 1;            // -1.0 to 1.0
  double confidence = 2;         // 0.0 to 1.0
  repeated AspectSentiment aspects = 3;
}

message AspectSentiment {
  string aspect = 1;
  double sentiment = 2;
  double confidence = 3;
}

message Entity {
  EntityType type = 1;
  string name = 2;
  double relevance = 3;          // 0.0 to 1.0
  string identifier = 4;         // Internal ID if known
}

enum EntityType {
  ENTITY_TYPE_UNSPECIFIED = 0;
  ENTITY_TYPE_PERSON = 1;
  ENTITY_TYPE_ORGANIZATION = 2;
  ENTITY_TYPE_LOCATION = 3;
  ENTITY_TYPE_INSTRUMENT = 4;
}

message MarketImpact {
  ImpactDirection expected = 1;
  ImpactMagnitude magnitude = 2;
  TimeHorizon time_horizon = 3;
}

enum ImpactDirection {
  IMPACT_DIRECTION_UNSPECIFIED = 0;
  IMPACT_DIRECTION_POSITIVE = 1;
  IMPACT_DIRECTION_NEGATIVE = 2;
  IMPACT_DIRECTION_NEUTRAL = 3;
}

enum ImpactMagnitude {
  IMPACT_MAGNITUDE_UNSPECIFIED = 0;
  IMPACT_MAGNITUDE_LOW = 1;
  IMPACT_MAGNITUDE_MEDIUM = 2;
  IMPACT_MAGNITUDE_HIGH = 3;
}

enum TimeHorizon {
  TIME_HORIZON_UNSPECIFIED = 0;
  TIME_HORIZON_IMMEDIATE = 1;
  TIME_HORIZON_SHORT_TERM = 2;
  TIME_HORIZON_LONG_TERM = 3;
}
```

**Earnings Event Data:**
```protobuf
message EarningsEventData {
  ReportType report_type = 1;
  EarningsPeriod period = 2;
  EarningsAnnouncement announcement = 3;
  EarningsEstimates estimates = 4;
  EarningsGuidance guidance = 5;
  ConferenceCallInfo call_info = 6;
}

enum ReportType {
  REPORT_TYPE_UNSPECIFIED = 0;
  REPORT_TYPE_PRELIMINARY = 1;
  REPORT_TYPE_OFFICIAL = 2;
  REPORT_TYPE_RESTATEMENT = 3;
}

message EarningsPeriod {
  PeriodType type = 1;
  int32 year = 2;
  int32 quarter = 3;           // Only for quarterly
  int32 fiscal_year = 4;
  int32 fiscal_quarter = 5;    // Only for quarterly
}

enum PeriodType {
  PERIOD_TYPE_UNSPECIFIED = 0;
  PERIOD_TYPE_QUARTERLY = 1;
  PERIOD_TYPE_ANNUAL = 2;
}

message EarningsAnnouncement {
  google.protobuf.Timestamp date = 1;
  string time = 2;             // Time if known
  bool is_premarket = 3;
  bool is_after_hours = 4;
}

message EarningsEstimates {
  EpsEstimate eps = 1;
  RevenueEstimate revenue = 2;
}

message EpsEstimate {
  double actual = 1;
  double consensus = 2;
  double surprise = 3;         // actual - consensus
  double surprise_percent = 4;
}

message RevenueEstimate {
  double actual = 1;
  double consensus = 2;
  double surprise = 3;
  double surprise_percent = 4;
}
```

**Technical Signal Event Data:**
```protobuf
message TechnicalSignalEventData {
  SignalType signal_type = 1;
  string indicator = 2;        // RSI, MACD, Moving Average, etc.
  string timeframe = 3;        // 1m, 5m, 15m, 1h, 1d, etc.
  
  Signal signal = 4;
  PriceContext price_context = 5;
  TechnicalLevels technical_levels = 6;
}

enum SignalType {
  SIGNAL_TYPE_UNSPECIFIED = 0;
  SIGNAL_TYPE_BREAKOUT = 1;
  SIGNAL_TYPE_BREAKDOWN = 2;
  SIGNAL_TYPE_REVERSAL = 3;
  SIGNAL_TYPE_CONTINUATION = 4;
  SIGNAL_TYPE_DIVERGENCE = 5;
}

message Signal {
  SignalDirection direction = 1;
  double strength = 2;         // 0.0 to 1.0
  double confidence = 3;       // 0.0 to 1.0
}

enum SignalDirection {
  SIGNAL_DIRECTION_UNSPECIFIED = 0;
  SIGNAL_DIRECTION_BULLISH = 1;
  SIGNAL_DIRECTION_BEARISH = 2;
  SIGNAL_DIRECTION_NEUTRAL = 3;
}

enum EventType {
  // External Events
  NEWS = "news",
  EARNINGS = "earnings", 
  CORPORATE_ACTION = "corporate_action",
  ECONOMIC_INDICATOR = "economic_indicator",
  ANALYST_RECOMMENDATION = "analyst_recommendation",
  REGULATORY_FILING = "regulatory_filing",
  
  // Internal Events (Generated)
  PRICE_GAP = "price_gap",
  SUPPORT_RESISTANCE_BREAK = "support_resistance_break",
  SUPPORT_RESISTANCE_HOLD = "support_resistance_hold", 
  VOLUME_ANOMALY = "volume_anomaly",
  TECHNICAL_SIGNAL = "technical_signal",
  RISK_ALERT = "risk_alert",
  
  // System Events
  SYSTEM_ALERT = "system_alert",
  DATA_QUALITY_ISSUE = "data_quality_issue",
  PROCESSING_ERROR = "processing_error"
}

interface EventSubject {
  instrumentId?: string;    // Internal instrument identifier
  symbol?: string;         // Ticker symbol
  isin?: string;           // International identifier
  cusip?: string;          // US identifier
  exchange?: string;       // Exchange code
  assetClass?: string;     // equity, bond, derivative, etc.
  sector?: string;         // GICS sector classification
  industry?: string;       // GICS industry classification
  country?: string;        // ISO country code
  currency?: string;       // ISO currency code
}

interface EventMetadata {
  priority: "critical" | "high" | "medium" | "low";
  classification: "public" | "internal" | "confidential" | "restricted";
  tags: string[];          // Searchable tags
  processedBy?: string;    // System/service that processed event
  processingTime?: number; // Processing duration in milliseconds
  retryCount?: number;     // Number of processing retries
  checksum?: string;       // Content integrity hash
}
```

#### 6.3.2 Event-Specific Schemas

**News Event:**
```typescript
interface NewsEventData {
  headline: string;
  summary?: string;
  fullText?: string;
  url?: string;
  author?: string;
  publisher: string;
  language: string;
  sentiment?: {
    overall: number;       // -1.0 to 1.0
    confidence: number;    // 0.0 to 1.0
    aspects?: Array<{
      aspect: string;
      sentiment: number;
      confidence: number;
    }>;
  };
  entities?: Array<{
    type: "person" | "organization" | "location" | "instrument";
    name: string;
    relevance: number;     // 0.0 to 1.0
    identifier?: string;   // Internal ID if known
  }>;
  categories: string[];    // News categories/topics
  importance?: number;     // 0.0 to 1.0 importance score
  marketImpact?: {
    expected: "positive" | "negative" | "neutral";
    magnitude: "high" | "medium" | "low";
    timeHorizon: "immediate" | "short_term" | "long_term";
  };
}
```

**Earnings Event:**
```typescript
interface EarningsEventData {
  reportType: "preliminary" | "official" | "restatement";
  period: {
    type: "quarterly" | "annual";
    year: number;
    quarter?: number;
    fiscalYear: number;
    fiscalQuarter?: number;
  };
  announcement: {
    date: string;          // Announcement date
    time?: string;         // Announcement time if known
    isPremarket?: boolean; // Before market open
    isAfterHours?: boolean; // After market close
  };
  estimates?: {
    eps?: {
      actual?: number;
      consensus?: number;
      surprise?: number;     // actual - consensus
      surprisePercent?: number;
    };
    revenue?: {
      actual?: number;
      consensus?: number; 
      surprise?: number;
      surprisePercent?: number;
    };
  };
  guidance?: {
    epsLow?: number;
    epsHigh?: number;
    revenueLow?: number;
    revenueHigh?: number;
    period?: string;
  };
  callInfo?: {
    date?: string;
    time?: string;
    dialIn?: string;
    webcast?: string;
  };
}
```

**Technical Signal Event:**
```typescript
interface TechnicalSignalEventData {
  signalType: "breakout" | "breakdown" | "reversal" | "continuation" | "divergence";
  indicator: string;       // RSI, MACD, Moving Average, etc.
  timeframe: string;       // 1m, 5m, 15m, 1h, 1d, etc.
  signal: {
    direction: "bullish" | "bearish" | "neutral";
    strength: number;      // 0.0 to 1.0
    confidence: number;    // 0.0 to 1.0
  };
  priceContext: {
    currentPrice: number;
    signalPrice: number;   // Price when signal was generated
    supportLevel?: number;
    resistanceLevel?: number;
    volume?: number;       // Volume at signal
    averageVolume?: number; // Recent average volume
  };
  technicalLevels?: {
    support: number[];
    resistance: number[];
    pivotPoint?: number;
    fibonacci?: Array<{
      level: number;
      percentage: number;
    }>;
  };
}
```

### 6.4 Event Processing Pipeline

#### 6.4.1 Ingestion Layer

**Python Event Producers with Protocol Buffers:**

```python
# src/events/producer.py
import redis
import uuid
from datetime import datetime
from google.protobuf.json_format import MessageToDict
from google.protobuf.timestamp_pb2 import Timestamp
from events.proto.events_pb2 import Event, EventType, Priority, Classification

class EventProducer:
    def __init__(self, redis_client: redis.Redis):
        self.redis = redis_client
        
    def publish_event(self, event: Event) -> str:
        """Publish event to Redis queue with Protocol Buffer serialization"""
        
        # 1. Set system metadata
        event.event_id = str(uuid.uuid4()) if not event.event_id else event.event_id
        event.ingestion_time.CopyFrom(Timestamp(seconds=int(datetime.utcnow().timestamp())))
        
        # 2. Validate event (basic validation)
        self._validate_event(event)
        
        # 3. Serialize to bytes and publish to Redis
        serialized_event = event.SerializeToString()
        queue_name = f"events:{event.event_type.name.lower()}"
        
        # 4. Push to Redis queue (atomic operation)
        self.redis.lpush(queue_name, serialized_event)
        
        return event.event_id
        
    def _validate_event(self, event: Event):
        """Basic event validation"""
        if not event.event_id:
            raise ValueError("Event ID is required")
        if not event.subject.symbol and not event.subject.instrument_id:
            raise ValueError("Event subject must have symbol or instrument_id")
        if event.event_type == EventType.EVENT_TYPE_UNSPECIFIED:
            raise ValueError("Event type must be specified")

# Usage example:
def create_news_event(headline: str, symbol: str, sentiment: float = 0.0):
    """Factory function to create news events"""
    from events.proto.events_pb2 import NewsEventData, SentimentAnalysis
    
    event = Event()
    event.event_type = EventType.EVENT_TYPE_NEWS
    event.event_version = "1.0.0"
    event.timestamp.CopyFrom(Timestamp(seconds=int(datetime.utcnow().timestamp())))
    event.source = "polygon"
    
    # Subject
    event.subject.symbol = symbol
    event.subject.exchange = "NASDAQ"  # Could be determined from symbol lookup
    
    # News-specific data
    event.news_data.headline = headline
    event.news_data.sentiment.overall = sentiment
    event.news_data.sentiment.confidence = 0.8
    
    # Metadata  
    event.metadata.priority = Priority.PRIORITY_HIGH if abs(sentiment) > 0.5 else Priority.PRIORITY_MEDIUM
    event.metadata.classification = Classification.CLASSIFICATION_PUBLIC
    event.metadata.tags.extend(["automated", "sentiment-analyzed"])
    
    return event
```

**Event Validation:**
```python
class EventValidator:
    def __init__(self, schema_registry: SchemaRegistry):
        self.schema_registry = schema_registry
        
    async def validate(self, event: BaseEvent) -> ValidationResult:
        errors = []
        
        # Schema validation
        schema = await self.schema_registry.get_schema(
            event.eventType, 
            event.eventVersion
        )
        schema_errors = self._validate_against_schema(event, schema)
        errors.extend(schema_errors)
        
        # Business rules validation
        business_errors = await self._validate_business_rules(event)
        errors.extend(business_errors)
        
        # Data quality checks
        quality_errors = self._validate_data_quality(event)
        errors.extend(quality_errors)
        
        return ValidationResult(
            is_valid=len(errors) == 0,
            errors=errors,
            warnings=self._get_warnings(event)
        )
```

#### 6.4.2 Celery Event Processing Layer

**Celery Workers with Protocol Buffers:**

```python
# src/events/consumers.py
import redis
from celery import Celery
from sqlalchemy.orm import Session
from google.protobuf.json_format import MessageToDict
from events.proto.events_pb2 import Event
from events.database import EventStorage
from events.correlation import CorrelationEngine

# Celery app configuration
celery_app = Celery('event_processor')
celery_app.conf.update(
    broker_url='redis://localhost:6379/0',
    result_backend='redis://localhost:6379/0',
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    timezone='UTC'
)

@celery_app.task(bind=True, max_retries=3)
def process_event_from_queue(self, queue_name: str):
    """Process single event from Redis queue"""
    redis_client = redis.Redis(host='localhost', port=6379, db=0)
    event_storage = EventStorage()
    correlation_engine = CorrelationEngine()
    
    try:
        # 1. Pop event from queue (blocking with timeout)
        result = redis_client.brpop([queue_name], timeout=30)
        if not result:
            return f"No events in queue {queue_name}"
            
        queue, serialized_event = result
        
        # 2. Deserialize Protocol Buffer
        event = Event()
        event.ParseFromString(serialized_event)
        
        # 3. Store event in PostgreSQL (as JSONB)
        event_dict = MessageToDict(event, preserving_proto_field_name=True)
        event_storage.store_event(event_dict)
        
        # 4. Run correlation analysis
        correlations = correlation_engine.find_correlations(event)
        if correlations:
            for correlation in correlations:
                correlation_storage.store_correlation(correlation)
                
        # 5. Generate training dataset record if needed
        if should_include_in_training(event):
            training_dataset_writer.append_event(event)
            
        return f"Successfully processed event {event.event_id}"
        
    except Exception as exc:
        # Retry with exponential backoff
        countdown = 2 ** self.request.retries
        raise self.retry(exc=exc, countdown=countdown)

@celery_app.task
def batch_process_events(queue_names: list, batch_size: int = 100):
    """Process events in batches for efficiency"""
    for queue_name in queue_names:
        for _ in range(batch_size):
            process_event_from_queue.delay(queue_name)

# Periodic tasks
@celery_app.task
def hourly_event_processing():
    """Scheduled task to process events every hour"""
    queues = ['events:news', 'events:earnings', 'events:technical']
    batch_process_events.delay(queues, batch_size=50)
```

**Simple Correlation Engine:**

```python
# src/events/correlation.py
from typing import List, Optional
from datetime import datetime, timedelta
from events.proto.events_pb2 import Event, EventType

class CorrelationEngine:
    def __init__(self, db_session):
        self.db = db_session
        
    def find_correlations(self, event: Event) -> List[dict]:
        """Find correlations with recent events for the same symbol"""
        correlations = []
        
        # Look for events in the last hour for the same symbol
        if not event.subject.symbol:
            return correlations
            
        time_window = timedelta(hours=1)
        cutoff_time = datetime.utcnow() - time_window
        
        # Query recent events for same symbol
        recent_events = self.db.query_events(
            symbol=event.subject.symbol,
            after_timestamp=cutoff_time,
            limit=50
        )
        
        for recent_event in recent_events:
            if recent_event['event_id'] == event.event_id:
                continue
                
            # Simple correlation rules
            correlation_score = self._calculate_correlation_score(event, recent_event)
            if correlation_score > 0.5:
                correlations.append({
                    'primary_event_id': event.event_id,
                    'related_event_id': recent_event['event_id'],
                    'correlation_type': 'temporal',
                    'correlation_score': correlation_score,
                    'time_lag_seconds': int((datetime.utcnow() - recent_event['timestamp']).total_seconds())
                })
                
        return correlations
        
    def _calculate_correlation_score(self, event1: Event, event2: dict) -> float:
        """Calculate simple correlation score between events"""
        score = 0.0
        
        # Same symbol = +0.3
        if event1.subject.symbol == event2.get('subject', {}).get('symbol'):
            score += 0.3
            
        # News followed by technical signal = +0.4
        if (event1.event_type == EventType.EVENT_TYPE_NEWS and 
            event2.get('event_type') == 'technical_signal'):
            score += 0.4
            
        # Similar sentiment direction = +0.3
        if self._similar_sentiment(event1, event2):
            score += 0.3
            
        return min(score, 1.0)  # Cap at 1.0
```

**Database Storage with Protocol Buffers:**

```python
# src/events/database.py
import json
from sqlalchemy import Column, String, DateTime, Text, Index
from sqlalchemy.dialects.postgresql import JSONB, UUID
from google.protobuf.json_format import MessageToDict

class EventStorage:
    def store_event(self, event_dict: dict):
        """Store Protocol Buffer event as JSONB in PostgreSQL"""
        
        # Create event record
        event_record = {
            'event_id': event_dict['event_id'],
            'event_type': event_dict['event_type'],
            'symbol': event_dict.get('subject', {}).get('symbol'),
            'timestamp': event_dict['timestamp'],
            'source': event_dict['source'],
            'event_data': event_dict,  # Full proto as JSONB
            'search_vector': self._create_search_vector(event_dict)
        }
        
        # Insert into PostgreSQL
        self.db.execute("""
            INSERT INTO events (event_id, event_type, symbol, timestamp, source, event_data, search_vector)
            VALUES (:event_id, :event_type, :symbol, :timestamp, :source, :event_data, to_tsvector(:search_vector))
            ON CONFLICT (event_id) DO NOTHING
        """, event_record)
        
    def query_events(self, symbol=None, event_type=None, limit=100):
        """Query events from PostgreSQL"""
        query = "SELECT * FROM events WHERE 1=1"
        params = {}
        
        if symbol:
            query += " AND symbol = :symbol"
            params['symbol'] = symbol
            
        if event_type:
            query += " AND event_type = :event_type" 
            params['event_type'] = event_type
            
        query += " ORDER BY timestamp DESC LIMIT :limit"
        params['limit'] = limit
        
        return self.db.execute(query, params).fetchall()
```
```java
public class EventCorrelationProcessor extends RichCoProcessFunction<BaseEvent, BaseEvent, CorrelatedEvent> {
    
    private transient ValueState<Map<String, BaseEvent>> eventBuffer;
    private transient ValueState<Map<String, Timer>> activeTimers;
    
    @Override
    public void processElement1(BaseEvent event, Context ctx, Collector<CorrelatedEvent> out) throws Exception {
        // Pattern: News event followed by significant price movement within 5 minutes
        if (event.getEventType() == EventType.NEWS) {
            // Buffer the news event
            Map<String, BaseEvent> buffer = eventBuffer.value();
            if (buffer == null) buffer = new HashMap<>();
            buffer.put(event.getEventId(), event);
            eventBuffer.update(buffer);
            
            // Set cleanup timer for 5 minutes
            long timerTime = ctx.timerService().currentProcessingTime() + TimeUnit.MINUTES.toMillis(5);
            ctx.timerService().registerProcessingTimeTimer(timerTime);
            
            Map<String, Timer> timers = activeTimers.value();
            if (timers == null) timers = new HashMap<>();
            timers.put(event.getEventId(), new Timer(timerTime, event.getEventId()));
            activeTimers.update(timers);
        }
    }
    
    @Override
    public void processElement2(BaseEvent event, Context ctx, Collector<CorrelatedEvent> out) throws Exception {
        // Look for price gap events that might correlate with buffered news
        if (event.getEventType() == EventType.PRICE_GAP) {
            Map<String, BaseEvent> buffer = eventBuffer.value();
            if (buffer != null) {
                for (BaseEvent newsEvent : buffer.values()) {
                    if (isCorrelated(newsEvent, event)) {
                        CorrelatedEvent correlation = new CorrelatedEvent(
                            newsEvent, 
                            event, 
                            CorrelationType.NEWS_PRICE_IMPACT,
                            calculateCorrelationStrength(newsEvent, event)
                        );
                        out.collect(correlation);
                    }
                }
            }
        }
    }
    
    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<CorrelatedEvent> out) throws Exception {
        // Clean up expired events
        Map<String, Timer> timers = activeTimers.value();
        if (timers != null) {
            Timer expiredTimer = timers.values().stream()
                .filter(timer -> timer.getTimestamp() == timestamp)
                .findFirst()
                .orElse(null);
                
            if (expiredTimer != null) {
                Map<String, BaseEvent> buffer = eventBuffer.value();
                if (buffer != null) {
                    buffer.remove(expiredTimer.getEventId());
                    eventBuffer.update(buffer);
                }
                timers.remove(expiredTimer.getEventId());
                activeTimers.update(timers);
            }
        }
    }
}
```

**Event Aggregation and Materialized Views:**
```sql
-- ClickHouse materialized view for real-time event counts by type and symbol
CREATE MATERIALIZED VIEW event_counts_by_symbol_mv
TO event_counts_by_symbol
AS
SELECT
    subject.symbol as symbol,
    eventType,
    toDate(timestamp) as date,
    toHour(timestamp) as hour,
    count() as event_count,
    avg(confidence) as avg_confidence,
    countIf(metadata.priority = 'critical') as critical_events,
    countIf(metadata.priority = 'high') as high_events
FROM events_stream
WHERE subject.symbol IS NOT NULL
GROUP BY 
    symbol, 
    eventType, 
    toDate(timestamp), 
    toHour(timestamp);

-- Real-time correlation tracking
CREATE MATERIALIZED VIEW event_correlations_mv
TO event_correlations
AS
SELECT
    e1.eventId as primary_event_id,
    e2.eventId as related_event_id,
    e1.eventType as primary_event_type,
    e2.eventType as related_event_type,
    e1.subject.symbol as symbol,
    dateDiff('second', e1.timestamp, e2.timestamp) as time_lag_seconds,
    'temporal' as correlation_type,
    CASE 
        WHEN abs(time_lag_seconds) <= 60 THEN 0.9
        WHEN abs(time_lag_seconds) <= 300 THEN 0.7
        WHEN abs(time_lag_seconds) <= 900 THEN 0.5
        ELSE 0.3
    END as correlation_strength
FROM events_stream e1
JOIN events_stream e2 ON 
    e1.subject.symbol = e2.subject.symbol
    AND e1.eventId != e2.eventId
    AND abs(dateDiff('second', e1.timestamp, e2.timestamp)) <= 900
WHERE e1.eventType IN ('news', 'earnings') 
    AND e2.eventType IN ('price_gap', 'volume_anomaly');
```

#### 6.4.3 Event Storage Architecture

**Multi-Tiered Storage Strategy:**

```python
class EventStorageManager:
    def __init__(self):
        self.hot_storage = PostgreSQLEventStore()      # Last 3 months
        self.warm_storage = ClickHouseEventStore()     # Last 2 years  
        self.cold_storage = S3EventStore()             # 2+ years
        self.cache = RedisEventCache()                 # Most accessed events
        
    async def store_event(self, event: BaseEvent) -> StorageResult:
        # Always store in hot storage first
        hot_result = await self.hot_storage.store(event)
        
        # Async replication to warm storage
        asyncio.create_task(self.warm_storage.store(event))
        
        # Cache high-priority events
        if event.metadata.priority in ['critical', 'high']:
            await self.cache.set(event.eventId, event, ttl=3600)
            
        return hot_result
        
    async def get_event(self, event_id: str) -> Optional[BaseEvent]:
        # Try cache first
        event = await self.cache.get(event_id)
        if event:
            return event
            
        # Try hot storage
        event = await self.hot_storage.get(event_id)
        if event:
            await self.cache.set(event_id, event, ttl=1800)
            return event
            
        # Try warm storage
        event = await self.warm_storage.get(event_id)
        if event:
            await self.cache.set(event_id, event, ttl=900)
            return event
            
        # Finally try cold storage
        return await self.cold_storage.get(event_id)
        
    async def query_events(self, query: EventQuery) -> EventQueryResult:
        # Route query to appropriate storage based on time range
        if query.time_range.is_recent(days=90):
            return await self.hot_storage.query(query)
        elif query.time_range.is_recent(days=730):
            return await self.warm_storage.query(query)
        else:
            return await self.cold_storage.query(query)
```

**PostgreSQL Hot Storage Schema:**
```sql
-- Time-partitioned events table for hot storage
CREATE TABLE events (
    event_id UUID PRIMARY KEY,
    event_type VARCHAR(50) NOT NULL,
    event_version VARCHAR(10) NOT NULL DEFAULT '1.0.0',
    timestamp TIMESTAMPTZ NOT NULL,
    ingestion_time TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    
    -- Source information
    source VARCHAR(100) NOT NULL,
    source_id VARCHAR(255),
    
    -- Event relationships
    causation_id UUID REFERENCES events(event_id),
    correlation_id UUID,
    parent_event_id UUID REFERENCES events(event_id),
    root_event_id UUID,
    
    -- Subject (what the event is about)
    subject JSONB NOT NULL,
    
    -- Event data and metadata
    data JSONB NOT NULL,
    metadata JSONB NOT NULL,
    
    -- Quality indicators
    confidence DECIMAL(3,2) CHECK (confidence >= 0 AND confidence <= 1),
    reliability VARCHAR(20),
    data_quality JSONB,
    
    -- Indexing and search
    search_vector TSVECTOR,
    tags TEXT[],
    
    CONSTRAINT valid_timestamp CHECK (timestamp <= NOW() + INTERVAL '1 hour'),
    CONSTRAINT valid_ingestion_time CHECK (ingestion_time >= timestamp)
) PARTITION BY RANGE (timestamp);

-- Create monthly partitions automatically
CREATE OR REPLACE FUNCTION create_event_partition(partition_date DATE)
RETURNS VOID AS $$
DECLARE
    partition_name TEXT;
    start_date DATE;
    end_date DATE;
BEGIN
    partition_name := 'events_' || to_char(partition_date, 'YYYY_MM');
    start_date := date_trunc('month', partition_date)::DATE;
    end_date := (date_trunc('month', partition_date) + INTERVAL '1 month')::DATE;
    
    EXECUTE format('CREATE TABLE IF NOT EXISTS %I PARTITION OF events
                   FOR VALUES FROM (%L) TO (%L)',
                   partition_name, start_date, end_date);
                   
    -- Add indexes to new partition
    EXECUTE format('CREATE INDEX IF NOT EXISTS %I ON %I (event_type, timestamp)',
                   partition_name || '_type_time_idx', partition_name);
    EXECUTE format('CREATE INDEX IF NOT EXISTS %I ON %I USING GIN (subject)',
                   partition_name || '_subject_idx', partition_name);
    EXECUTE format('CREATE INDEX IF NOT EXISTS %I ON %I USING GIN (search_vector)',
                   partition_name || '_search_idx', partition_name);
END;
$$ LANGUAGE plpgsql;

-- Automatically create partitions
SELECT create_event_partition(CURRENT_DATE);
SELECT create_event_partition(CURRENT_DATE + INTERVAL '1 month');
SELECT create_event_partition(CURRENT_DATE + INTERVAL '2 months');

-- Indexes for optimal query performance
CREATE INDEX CONCURRENTLY IF NOT EXISTS events_timestamp_idx 
ON events (timestamp) WHERE timestamp >= CURRENT_DATE - INTERVAL '90 days';

CREATE INDEX CONCURRENTLY IF NOT EXISTS events_symbol_type_time_idx 
ON events ((subject->>'symbol'), event_type, timestamp)
WHERE subject->>'symbol' IS NOT NULL;

CREATE INDEX CONCURRENTLY IF NOT EXISTS events_correlation_idx
ON events (correlation_id) WHERE correlation_id IS NOT NULL;

CREATE INDEX CONCURRENTLY IF NOT EXISTS events_priority_idx
ON events ((metadata->>'priority'), timestamp)
WHERE metadata->>'priority' IN ('critical', 'high');

-- Full-text search index
CREATE INDEX CONCURRENTLY IF NOT EXISTS events_fulltext_idx
ON events USING GIN (search_vector);

-- Function to update search vector
CREATE OR REPLACE FUNCTION update_event_search_vector()
RETURNS TRIGGER AS $$
BEGIN
    NEW.search_vector := setweight(to_tsvector('english', COALESCE(NEW.data->>'headline', '')), 'A') ||
                        setweight(to_tsvector('english', COALESCE(NEW.data->>'summary', '')), 'B') ||
                        setweight(to_tsvector('english', array_to_string(NEW.tags, ' ')), 'C');
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER update_search_vector_trigger
    BEFORE INSERT OR UPDATE ON events
    FOR EACH ROW EXECUTE FUNCTION update_event_search_vector();
```

### 6.5 API Design

#### 6.5.1 GraphQL Schema

```graphql
# Core Event Types
type Event {
  eventId: ID!
  eventType: EventType!
  eventVersion: String!
  timestamp: DateTime!
  timeZone: String!
  validFrom: DateTime
  validTo: DateTime
  
  # Source & Attribution
  source: EventSource!
  sourceId: String
  ingestionTime: DateTime!
  
  # Relationships
  causationId: ID
  correlationId: ID
  parentEventId: ID
  rootEventId: ID
  parentEvent: Event
  childEvents: [Event!]!
  correlatedEvents(limit: Int = 10): [CorrelatedEvent!]!
  
  # Subject & Content
  subject: EventSubject!
  data: JSON!
  metadata: EventMetadata!
  
  # Quality Indicators
  confidence: Float
  reliability: String
  dataQuality: DataQuality
}

enum EventType {
  NEWS
  EARNINGS
  CORPORATE_ACTION
  ECONOMIC_INDICATOR
  ANALYST_RECOMMENDATION
  REGULATORY_FILING
  PRICE_GAP
  SUPPORT_RESISTANCE_BREAK
  SUPPORT_RESISTANCE_HOLD
  VOLUME_ANOMALY
  TECHNICAL_SIGNAL
  RISK_ALERT
  SYSTEM_ALERT
  DATA_QUALITY_ISSUE
  PROCESSING_ERROR
}

type EventSubject {
  instrumentId: ID
  symbol: String
  isin: String
  cusip: String
  exchange: String
  assetClass: String
  sector: String
  industry: String
  country: String
  currency: String
}

type EventMetadata {
  priority: Priority!
  classification: Classification!
  tags: [String!]!
  processedBy: String
  processingTime: Int
  retryCount: Int
  checksum: String
}

type CorrelatedEvent {
  primaryEvent: Event!
  relatedEvent: Event!
  correlationType: CorrelationType!
  correlationStrength: Float!
  timeLagSeconds: Int
  causalDirection: CausalDirection
}

enum CorrelationType {
  TEMPORAL
  SEMANTIC
  CAUSAL
  CROSS_ASSET
}

enum CausalDirection {
  FORWARD
  REVERSE
  BIDIRECTIONAL
}

# Query Interface
type Query {
  # Single event retrieval
  event(id: ID!): Event
  
  # Event queries with filtering and pagination
  events(
    filter: EventFilter
    orderBy: EventOrderBy = TIMESTAMP_DESC
    first: Int
    after: String
  ): EventConnection!
  
  # Event correlations
  correlatedEvents(
    eventId: ID!
    correlationTypes: [CorrelationType!]
    minStrength: Float = 0.5
    maxTimeLag: Int = 3600
  ): [CorrelatedEvent!]!
  
  # Event aggregations
  eventCounts(
    filter: EventFilter!
    groupBy: [EventGroupBy!]!
    timeRange: TimeRange!
  ): [EventCount!]!
  
  # Event patterns and analytics
  eventPatterns(
    symbols: [String!]!
    timeRange: TimeRange!
    patternTypes: [PatternType!]!
  ): [EventPattern!]!
}

# Subscription Interface for Real-time Events
type Subscription {
  # Real-time event stream
  events(
    filter: EventFilter
    priority: [Priority!] = [CRITICAL, HIGH]
  ): Event!
  
  # Event correlation notifications
  correlatedEvents(
    symbols: [String!]!
    minStrength: Float = 0.7
  ): CorrelatedEvent!
  
  # System health and performance
  systemMetrics: SystemMetrics!
}

input EventFilter {
  eventTypes: [EventType!]
  symbols: [String!]
  sources: [EventSource!]
  timeRange: TimeRange
  priority: [Priority!]
  classification: [Classification!]
  tags: [String!]
  search: String
  confidence: FloatRange
  hasCorrelations: Boolean
}

input TimeRange {
  from: DateTime!
  to: DateTime!
}

type EventConnection {
  edges: [EventEdge!]!
  pageInfo: PageInfo!
  totalCount: Int!
}

type EventEdge {
  node: Event!
  cursor: String!
}
```

#### 6.5.2 REST API Endpoints

**Core Event Operations:**
```typescript
// Event retrieval
GET /api/v1/events/{eventId}
GET /api/v1/events?filter[eventType]=news&filter[symbol]=AAPL&limit=100

// Event search
POST /api/v1/events/search
{
  "query": "earnings surprise apple",
  "filter": {
    "eventTypes": ["earnings", "news"],
    "timeRange": {
      "from": "2024-01-01T00:00:00Z",
      "to": "2024-12-31T23:59:59Z"
    }
  },
  "sort": [
    {"field": "confidence", "order": "desc"},
    {"field": "timestamp", "order": "desc"}
  ]
}

// Event correlations
GET /api/v1/events/{eventId}/correlations?minStrength=0.7&maxTimeLag=300

// Event ingestion (internal API)
POST /api/v1/events
{
  "eventType": "news",
  "eventVersion": "1.0.0",
  "source": "polygon",
  "subject": {
    "symbol": "AAPL",
    "instrumentId": "12345"
  },
  "data": {
    "headline": "Apple Reports Record Q4 Earnings",
    "sentiment": {
      "overall": 0.8,
      "confidence": 0.9
    }
  },
  "metadata": {
    "priority": "high",
    "classification": "public"
  }
}

// Batch event ingestion
POST /api/v1/events/batch
{
  "events": [
    // Array of event objects
  ]
}

// Event analytics
GET /api/v1/analytics/events/counts?groupBy=eventType,symbol&timeRange=1d
GET /api/v1/analytics/events/correlations?symbols=AAPL,MSFT&timeRange=7d
GET /api/v1/analytics/events/patterns?symbol=AAPL&patternType=earnings_reaction
```

**WebSocket API for Real-time Events:**
```typescript
// Subscribe to real-time events
WebSocket: /api/v1/ws/events

// Subscription message
{
  "type": "subscribe",
  "channel": "events",
  "filter": {
    "eventTypes": ["news", "earnings"],
    "symbols": ["AAPL", "MSFT"],
    "priority": ["critical", "high"]
  }
}

// Event notification
{
  "type": "event",
  "channel": "events", 
  "data": {
    "eventId": "uuid",
    "eventType": "news",
    "timestamp": "2024-12-07T15:30:00Z",
    "subject": {
      "symbol": "AAPL"
    },
    "data": {
      "headline": "Apple Announces New Product",
      "sentiment": {"overall": 0.7}
    },
    "metadata": {
      "priority": "high"
    }
  }
}
```

### 6.6 Performance Optimization

#### 6.6.1 Caching Strategy

```python
class EventCacheManager:
    def __init__(self):
        self.redis_client = redis.Redis(
            host='redis-cluster',
            port=6379,
            db=0,
            decode_responses=True
        )
        self.cache_ttl = {
            'critical': 3600,    # 1 hour
            'high': 1800,        # 30 minutes  
            'medium': 900,       # 15 minutes
            'low': 300           # 5 minutes
        }
        
    async def cache_event(self, event: BaseEvent) -> None:
        """Cache event with TTL based on priority"""
        cache_key = f"event:{event.eventId}"
        ttl = self.cache_ttl.get(event.metadata.priority, 300)
        
        serialized_event = self._serialize_event(event)
        await self.redis_client.setex(cache_key, ttl, serialized_event)
        
        # Cache by symbol for quick lookups
        if event.subject.symbol:
            symbol_key = f"events:symbol:{event.subject.symbol}:latest"
            await self.redis_client.lpush(symbol_key, event.eventId)
            await self.redis_client.ltrim(symbol_key, 0, 99)  # Keep last 100
            await self.redis_client.expire(symbol_key, ttl)
            
    async def get_cached_events(self, symbol: str, limit: int = 10) -> List[BaseEvent]:
        """Get latest cached events for symbol"""
        symbol_key = f"events:symbol:{symbol}:latest"
        event_ids = await self.redis_client.lrange(symbol_key, 0, limit - 1)
        
        events = []
        for event_id in event_ids:
            cache_key = f"event:{event_id}"
            cached_event = await self.redis_client.get(cache_key)
            if cached_event:
                events.append(self._deserialize_event(cached_event))
                
        return events
```

#### 6.6.2 Query Optimization

**Index Strategy:**
```sql
-- Composite indexes for common query patterns
CREATE INDEX CONCURRENTLY events_symbol_type_time_idx 
ON events ((subject->>'symbol'), event_type, timestamp DESC)
WHERE subject->>'symbol' IS NOT NULL;

CREATE INDEX CONCURRENTLY events_correlation_time_idx
ON events (correlation_id, timestamp DESC)
WHERE correlation_id IS NOT NULL;

CREATE INDEX CONCURRENTLY events_priority_time_idx
ON events ((metadata->>'priority'), timestamp DESC)
WHERE metadata->>'priority' IN ('critical', 'high');

-- Partial indexes for active data
CREATE INDEX CONCURRENTLY events_recent_idx
ON events (timestamp DESC, event_type)
WHERE timestamp >= CURRENT_DATE - INTERVAL '30 days';

-- GIN indexes for JSON queries
CREATE INDEX CONCURRENTLY events_subject_gin_idx
ON events USING GIN (subject);

CREATE INDEX CONCURRENTLY events_data_gin_idx  
ON events USING GIN (data);

CREATE INDEX CONCURRENTLY events_tags_gin_idx
ON events USING GIN (tags);
```

**Query Plans:**
```sql
-- Optimized query for recent events by symbol
EXPLAIN (ANALYZE, BUFFERS)
SELECT event_id, event_type, timestamp, data->'headline' as headline
FROM events 
WHERE subject->>'symbol' = 'AAPL'
  AND event_type IN ('news', 'earnings')
  AND timestamp >= CURRENT_DATE - INTERVAL '7 days'
ORDER BY timestamp DESC
LIMIT 50;

-- Should use: events_symbol_type_time_idx
-- Index Scan using events_symbol_type_time_idx
-- Buffers: shared hit=XX read=YY
```

### 6.7 Monitoring & Observability

#### 6.7.1 Metrics Collection

```python
from prometheus_client import Counter, Histogram, Gauge
import structlog

# Prometheus metrics
event_ingestion_total = Counter(
    'event_ingestion_total',
    'Total number of events ingested',
    ['event_type', 'source', 'status']
)

event_processing_duration = Histogram(
    'event_processing_duration_seconds',
    'Time spent processing events',
    ['event_type', 'processor']
)

event_queue_size = Gauge(
    'event_queue_size',
    'Number of events in processing queue',
    ['queue_name']
)

correlation_detection_total = Counter(
    'correlation_detection_total',
    'Total number of correlations detected',
    ['correlation_type', 'strength_bucket']
)

# Structured logging
logger = structlog.get_logger()

class EventMetricsCollector:
    def __init__(self):
        self.metrics = {
            'events_per_second': 0,
            'average_latency_ms': 0,
            'error_rate': 0,
            'queue_depth': 0
        }
        
    async def record_event_ingestion(self, event: BaseEvent, status: str, processing_time: float):
        # Prometheus metrics
        event_ingestion_total.labels(
            event_type=event.eventType,
            source=event.source,
            status=status
        ).inc()
        
        event_processing_duration.labels(
            event_type=event.eventType,
            processor='ingestion'
        ).observe(processing_time)
        
        # Structured logging
        await logger.ainfo(
            "Event ingested",
            event_id=event.eventId,
            event_type=event.eventType,
            source=event.source,
            symbol=event.subject.symbol,
            processing_time_ms=processing_time * 1000,
            priority=event.metadata.priority,
            status=status
        )
        
    async def record_correlation_detection(self, correlation: CorrelatedEvent):
        strength_bucket = self._get_strength_bucket(correlation.correlationStrength)
        
        correlation_detection_total.labels(
            correlation_type=correlation.correlationType,
            strength_bucket=strength_bucket
        ).inc()
        
        await logger.ainfo(
            "Correlation detected",
            primary_event_id=correlation.primaryEvent.eventId,
            related_event_id=correlation.relatedEvent.eventId,
            correlation_type=correlation.correlationType,
            strength=correlation.correlationStrength,
            time_lag_seconds=correlation.timeLagSeconds
        )
        
    def _get_strength_bucket(self, strength: float) -> str:
        if strength >= 0.9:
            return "very_high"
        elif strength >= 0.7:
            return "high"
        elif strength >= 0.5:
            return "medium"
        else:
            return "low"
```

#### 6.7.2 Health Checks

```python
class EventSystemHealthCheck:
    def __init__(self, kafka_client, postgres_client, redis_client):
        self.kafka = kafka_client
        self.postgres = postgres_client
        self.redis = redis_client
        
    async def check_system_health(self) -> HealthStatus:
        checks = {
            'kafka': await self._check_kafka(),
            'postgres': await self._check_postgres(),
            'redis': await self._check_redis(),
            'event_processing': await self._check_event_processing(),
            'correlation_engine': await self._check_correlation_engine()
        }
        
        overall_status = 'healthy' if all(
            check['status'] == 'healthy' for check in checks.values()
        ) else 'unhealthy'
        
        return HealthStatus(
            status=overall_status,
            checks=checks,
            timestamp=datetime.utcnow().isoformat()
        )
        
    async def _check_kafka(self) -> Dict[str, Any]:
        try:
            # Check if we can list topics
            topics = await self.kafka.list_topics(timeout=5.0)
            
            # Check if event topics exist
            required_topics = ['events-news', 'events-earnings', 'events-technical']
            missing_topics = set(required_topics) - set(topics)
            
            if missing_topics:
                return {
                    'status': 'unhealthy',
                    'message': f'Missing topics: {missing_topics}',
                    'details': {'topics': list(topics)}
                }
                
            return {
                'status': 'healthy',
                'message': 'Kafka cluster is responsive',
                'details': {'topic_count': len(topics)}
            }
            
        except Exception as e:
            return {
                'status': 'unhealthy',
                'message': f'Kafka connection failed: {str(e)}',
                'details': {'error': str(e)}
            }
```

### 6.8 Security & Compliance

#### 6.8.1 Access Control

```python
class EventAccessControl:
    def __init__(self, auth_service: AuthService):
        self.auth = auth_service
        
    async def authorize_event_access(
        self, 
        user: User, 
        event: BaseEvent, 
        operation: str
    ) -> bool:
        """Authorize user access to event based on classification and role"""
        
        # Check basic permissions
        if not await self.auth.has_permission(user, f"events:{operation}"):
            return False
            
        # Check classification-based access
        event_classification = event.metadata.classification
        user_clearance = await self.auth.get_user_clearance(user)
        
        clearance_levels = {
            'public': 0,
            'internal': 1, 
            'confidential': 2,
            'restricted': 3
        }
        
        required_level = clearance_levels.get(event_classification, 0)
        user_level = clearance_levels.get(user_clearance, 0)
        
        if user_level < required_level:
            await self._log_access_denied(user, event, "insufficient_clearance")
            return False
            
        # Check symbol-based access controls
        if event.subject.symbol:
            if not await self.auth.has_symbol_access(user, event.subject.symbol):
                await self._log_access_denied(user, event, "symbol_access_denied")
                return False
                
        # Log successful access
        await self._log_access_granted(user, event, operation)
        return True
        
    async def _log_access_denied(self, user: User, event: BaseEvent, reason: str):
        await logger.awarn(
            "Event access denied",
            user_id=user.id,
            user_role=user.role,
            event_id=event.eventId,
            event_classification=event.metadata.classification,
            symbol=event.subject.symbol,
            reason=reason
        )
        
    async def _log_access_granted(self, user: User, event: BaseEvent, operation: str):
        await logger.ainfo(
            "Event access granted",
            user_id=user.id,
            user_role=user.role,
            event_id=event.eventId,
            operation=operation,
            symbol=event.subject.symbol
        )
```

#### 6.8.2 Data Encryption & Privacy

```python
class EventEncryptionService:
    def __init__(self, encryption_key: bytes):
        self.cipher = Fernet(encryption_key)
        
    async def encrypt_sensitive_data(self, event: BaseEvent) -> BaseEvent:
        """Encrypt sensitive fields in event data"""
        
        # Fields that should be encrypted
        sensitive_fields = [
            'fullText',         # Full news article text
            'personalData',     # Any PII
            'internalNotes',    # Internal analysis
            'confidentialData'  # Marked confidential
        ]
        
        encrypted_event = copy.deepcopy(event)
        
        for field in sensitive_fields:
            if field in encrypted_event.data:
                original_value = encrypted_event.data[field]
                if isinstance(original_value, str):
                    encrypted_value = self.cipher.encrypt(original_value.encode()).decode()
                    encrypted_event.data[field] = encrypted_value
                    encrypted_event.metadata.encryptedFields = encrypted_event.metadata.get('encryptedFields', [])
                    encrypted_event.metadata.encryptedFields.append(field)
                    
        return encrypted_event
        
    async def decrypt_event_data(self, event: BaseEvent) -> BaseEvent:
        """Decrypt sensitive fields in event data"""
        
        encrypted_fields = event.metadata.get('encryptedFields', [])
        if not encrypted_fields:
            return event
            
        decrypted_event = copy.deepcopy(event)
        
        for field in encrypted_fields:
            if field in decrypted_event.data:
                encrypted_value = decrypted_event.data[field]
                try:
                    decrypted_value = self.cipher.decrypt(encrypted_value.encode()).decode()
                    decrypted_event.data[field] = decrypted_value
                except Exception as e:
                    logger.error(f"Failed to decrypt field {field}: {e}")
                    
        return decrypted_event
```

## 7. Implementation Plan

### 7.1 Python-Based Development Phases

#### Phase 1: Protocol Buffer Foundation (Weeks 1-4)
**Deliverables:**
- [ ] Protocol Buffer schema design and Python code generation
- [ ] Redis queue setup and configuration using existing Docker
- [ ] Basic event producers for existing data sources (Polygon, Tiingo)
- [ ] PostgreSQL event table with JSONB support and time-based partitioning
- [ ] Simple Celery workers for core event types
- [ ] Integration with existing `run_dev.py` workflow

**Success Criteria:**
- 1K events/hour throughput 
- <30 second end-to-end processing
- All existing news/earnings data can be converted to proto format

#### Phase 2: Event Processing & Correlation (Weeks 5-8)  
**Deliverables:**
- [ ] Celery worker deployment and scaling
- [ ] Simple correlation detection algorithms 
- [ ] FastAPI + GraphQL endpoint for event queries
- [ ] Basic event search and filtering capabilities
- [ ] Integration with training dataset generation (.riegeli proto arrays)
- [ ] Simple monitoring using existing infrastructure

**Success Criteria:**
- Basic event correlation working
- <30 second processing latency maintained
- Events flowing to training datasets correctly

#### Phase 3: Enhancement & Integration (Weeks 9-12)
**Deliverables:**
- [ ] Enhanced correlation algorithms and pattern detection
- [ ] Complete API coverage for all event types
- [ ] Event analytics and reporting capabilities
- [ ] Performance optimization and horizontal scaling
- [ ] Integration testing with existing ML pipeline
- [ ] Documentation and deployment procedures

**Success Criteria:**
- 40% reduction in false signals through better correlation
- 10K events/hour sustained throughput
- Complete integration with training data pipeline

### 7.2 Python-Based Resource Requirements

#### Team Structure (Simplified)
- **Senior Backend Engineer (1)**: Python development, Protocol Buffers, event processing
- **Backend Engineer (1)**: API development, database optimization, integration
- **DevOps/SRE (0.5)**: Docker, Redis, deployment automation (leverages existing)

**Total Team: 2.5 people vs 8 people in original plan**

#### Infrastructure Requirements (Minimal)
- **Redis**: Single instance with persistence (existing Docker setup)
- **PostgreSQL**: Existing database with additional event tables
- **Celery Workers**: 3-5 Python processes (can run on existing servers)
- **FastAPI Service**: Single Python service (existing pattern)
- **Monitoring**: Existing Prometheus/Grafana stack

**Key Point**: Leverages existing Docker infrastructure managed by `run_dev.py`

#### Budget Estimation (Dramatically Reduced)
- **Development Team**: $350K for 3 months (2.5 people × $140K annual × 0.25 year)
- **Infrastructure Costs**: $200/month additional (Redis persistence, minor scaling)
- **Third-party Software**: $0 (using open source: Redis, Celery, PostgreSQL, FastAPI)
- **Total Implementation Cost**: ~$355K vs $2.6M (86% cost reduction)

#### ROI Comparison
- **Original Plan**: $2.6M implementation, 4-month payback
- **Python Plan**: $355K implementation, 1-2 month payback  
- **Risk Reduction**: Much lower complexity, faster delivery, easier maintenance

### 7.3 Risk Mitigation Plan

#### Technical Risks
| Risk | Probability | Impact | Mitigation |
|------|-------------|--------|------------|
| Performance bottlenecks | Medium | High | Load testing, performance monitoring, horizontal scaling |
| Data loss during migration | Low | Critical | Comprehensive backup strategy, parallel processing |
| Schema evolution conflicts | Medium | Medium | Schema registry, versioning strategy, migration tools |
| Kafka cluster failures | Low | High | Multi-AZ deployment, automated failover, monitoring |

#### Business Risks  
| Risk | Probability | Impact | Mitigation |
|------|-------------|--------|------------|
| Delayed delivery | Medium | High | Agile methodology, regular sprint reviews, contingency planning |
| Budget overruns | Low | Medium | Regular budget reviews, scope management, phased delivery |
| Stakeholder alignment | Medium | Medium | Regular demos, stakeholder involvement, clear communication |
| Regulatory compliance gaps | Low | Critical | Legal review, compliance consultation, audit preparation |

### 7.4 Testing Strategy

#### Unit Testing
- **Coverage Target**: >90% code coverage
- **Framework**: pytest for Python, Jest for TypeScript
- **Mock Strategy**: Mock external dependencies, use test doubles
- **CI Integration**: Automated testing on every PR

#### Integration Testing
- **Event Flow Testing**: End-to-end event processing validation
- **API Testing**: GraphQL/REST API functionality and performance
- **Database Testing**: Schema validation, query performance
- **Message Queue Testing**: Kafka producer/consumer reliability

#### Performance Testing
- **Load Testing**: Gradual load increase up to target throughput
- **Stress Testing**: System behavior under extreme load
- **Latency Testing**: End-to-end latency measurement and optimization
- **Endurance Testing**: Long-running stability validation

#### User Acceptance Testing
- **Stakeholder Validation**: Feature validation with business users
- **Workflow Testing**: Complete user journey validation
- **Performance Acceptance**: Latency and throughput validation
- **Security Testing**: Access control and data protection validation

## 8. Success Metrics & KPIs

### 8.1 Technical Performance Metrics

| Metric | Current | Target | Measurement Method |
|--------|---------|--------|-------------------|
| Event Ingestion Latency | 500ms | <5 seconds (p99) | Celery task timing |
| Processing Throughput | Manual | 1K-10K/hour | Events processed counter |
| End-to-End Latency | Manual | <30 seconds (p99) | Redis queue + DB timing |
| System Availability | 95% | 99.9% | Docker container uptime |
| Query Response Time | 1000ms | <500ms (p95) | FastAPI response metrics |
| Storage Efficiency | N/A | Proto compression | PostgreSQL storage metrics |

### 8.2 Business Impact Metrics

| Metric | Current | Target | Measurement Method |
|--------|---------|--------|-------------------|
| Alpha Generation | Baseline | +25% improvement | Portfolio returns analysis |
| Signal Accuracy | 60% | >85% | Backtesting validation |
| False Positive Rate | 40% | <15% | Signal analysis |
| Market Reaction Time | 30 seconds | <5 seconds | Event-to-action latency |
| Compliance Automation | 10% | 90% | Manual vs automated processes |
| Operational Cost Reduction | Baseline | 60% reduction | Infrastructure cost analysis |

### 8.3 User Experience Metrics

| Metric | Current | Target | Measurement Method |
|--------|---------|--------|-------------------|
| Query Success Rate | 95% | >99.5% | API success metrics |
| User Satisfaction | N/A | >4.5/5 | User surveys |
| Feature Adoption Rate | N/A | >80% | Usage analytics |
| Time to Insights | 5 minutes | <30 seconds | User workflow analysis |
| Support Ticket Volume | Baseline | 70% reduction | Ticket system metrics |

## 9. Conclusion

This revised PRD and DRD outlines a practical, Python-native transformation of the ATS event system that balances functionality with implementation simplicity. The proposed system leverages Protocol Buffers for standardization while using familiar Python technologies to deliver significant value at a fraction of the cost and complexity.

**Key Benefits of Python-Based Approach:**
- **Performance**: <30 second event processing with 1K-10K events/hour throughput (appropriate for hourly data)
- **Integration**: Seamless Protocol Buffer serialization for database + training dataset storage  
- **Simplicity**: Python-native stack (Redis, Celery, FastAPI) integrated with existing infrastructure
- **Cost-Effective**: 86% cost reduction ($355K vs $2.6M) with 1-2 month payback
- **Low Risk**: Builds on existing Docker patterns and team Python expertise

**Success Factors:**
1. **Appropriate Scale**: Right-sized for hourly event frequency vs high-frequency trading
2. **Protocol Buffer Foundation**: Standardized serialization across DB and training data
3. **Existing Infrastructure**: Leverages current Docker, PostgreSQL, and `run_dev.py` workflows
4. **Team Skills**: Matches existing Python expertise and reduces learning curve
5. **Incremental Value**: Quick wins in 3-month timeline with immediate ROI

**Implementation Advantages:**
- **Faster Delivery**: 3 months vs 12 months
- **Lower Complexity**: Simple message queue vs complex streaming platform
- **Better Integration**: Native integration with existing training data pipeline
- **Easier Maintenance**: Familiar Python stack vs specialized streaming infrastructure

**Next Steps:**
1. **Stakeholder Approval**: Review Python-based architecture approach
2. **Protocol Buffer Design**: Finalize event schemas and generate Python code
3. **Infrastructure Setup**: Configure Redis and Celery within existing Docker environment  
4. **Phase 1 Kickoff**: Begin with basic event ingestion and storage

This Python-native event system redesign provides the foundation for improved event correlation and training data integration while maintaining the simplicity and reliability that matches ATS's current operational model and hourly data processing requirements.

---

**Document Control:**
- **Version**: 1.0
- **Last Updated**: December 2024
- **Review Cycle**: Quarterly
- **Approval Required**: CTO, Head of Engineering, Head of Product
- **Distribution**: Engineering Team, Product Team, Executive Stakeholders