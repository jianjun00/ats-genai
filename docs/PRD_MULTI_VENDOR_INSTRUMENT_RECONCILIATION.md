# PRD: Multi-Vendor Instrument Data Reconciliation System

## Executive Summary

This PRD defines the comprehensive instrument population system that uses EODHD as the primary data source while reconciling data from Polygon and Tiingo to ensure data quality, completeness, and accuracy. The system creates standardized instrument records with comprehensive cross-references for exchanges, CUSIP, ISIN, and tracks complete listing/delisting lifecycle.

## Background & Business Need

### Current Challenges
- **Data Source Fragmentation**: Different vendors provide complementary but sometimes inconsistent data
- **Reference Data Quality**: CUSIP, ISIN, and exchange mappings vary across providers
- **Listing History Gaps**: Incomplete tracking of exchange migrations and delisting events
- **Manual Reconciliation**: Time-consuming manual processes to reconcile vendor discrepancies

### Business Impact
- **Regulatory Compliance**: Accurate instrument identification required for reporting
- **Risk Management**: Complete exchange migration history essential for delisting risk analysis
- **Trading Operations**: Reliable reference data prevents trading errors and settlements issues
- **Data Quality**: Consistent instrument universe across all trading and analytics systems

## Solution Overview

### Primary Data Source Strategy
**EODHD as Primary**: EODHD provides the most comprehensive historical coverage and exchange migration tracking.

**Multi-Vendor Reconciliation**: Polygon and Tiingo data used to validate and enrich EODHD data.

**Authoritative Record**: Single source of truth with clear data lineage and reconciliation notes.

## Detailed Requirements

### 1. Data Source Hierarchy & Reconciliation Rules

#### Primary Data Source: EODHD
```
Priority: PRIMARY
Coverage: US instruments, comprehensive historical data
Key Strengths:
- Complete exchange migration history
- Delisting status tracking
- Symbol change history
- ISIN/CUSIP reference data
```

#### Secondary Sources for Validation & Enrichment

**Polygon API**:
```
Priority: VALIDATION
Coverage: US equities, ETFs, options
Usage:
- Validate exchange mappings
- Cross-check active/inactive status
- Verify listing dates
- Provide real-time status updates
```

**Tiingo API** (Optional):
```
Priority: ENRICHMENT
Coverage: Global equities
Usage:  
- Additional validation layer
- International instrument coverage
- Alternative data quality check
```

#### Reconciliation Decision Matrix

| Data Field | Primary Source | Validation Logic | Conflict Resolution |
|------------|----------------|------------------|-------------------|
| **Symbol** | EODHD | Exact match required across all sources | Manual review if mismatch |
| **Name** | EODHD | Fuzzy match tolerance (90%+) | Use primary source name |
| **Exchange** | EODHD | Normalize and cross-validate | Flag discrepancies, use EODHD |
| **ISIN** | EODHD | Format validation (12 chars, US prefix) | Reject if invalid format |
| **CUSIP** | EODHD | Format validation (9 chars, alphanumeric) | Reject if invalid format |
| **Listing Date** | EODHD | Cross-validate ±30 days tolerance | Use earliest valid date |
| **Active Status** | Polygon | EODHD delisted flag vs Polygon active | EODHD delisted takes precedence |
| **Delisting Date** | EODHD | Must be after listing date | Reject if date validation fails |

### 2. Exchange Reference System

#### Exchange Normalization Mapping
```python
EXCHANGE_MAPPING = {
    # Polygon to Standard
    'XNAS': 'NASDAQ',
    'XNYS': 'NYSE',
    'ARCX': 'NYSE ARCA',
    'BATS': 'BATS',
    
    # Tiingo to Standard  
    'NASDAQ': 'NASDAQ',
    'NYSE': 'NYSE',
    'NYSE ARCA': 'NYSE ARCA',
    
    # EODHD variants
    'NASDAQ': 'NASDAQ',
    'NYSE': 'NYSE',
    'OTCMKTS': 'OTC',
    'PINK': 'OTC',
    'OTCGREY': 'OTC'
}
```

#### Exchange Migration Tracking
```sql
-- Example exchange migration for SHPW
INSERT INTO instrument_xrefs (instrument_id, vendor_id, external_symbol, start_date, end_date)
VALUES 
  (12345, 1, 'NASDAQ', '2021-06-15', '2023-08-14'),  -- Original listing
  (12345, 1, 'OTC', '2023-08-15', NULL);             -- Current OTC trading
```

### 3. Instrument Cross-References (instrument_xrefs)

#### Vendor Types & Usage

| Vendor Type | Purpose | External Symbol Format | Lifecycle |
|-------------|---------|------------------------|-----------|
| **exchange** | Track exchange history | NYSE, NASDAQ, OTC | Temporal (start/end dates) |
| **cusip** | US regulatory identifier | 9-char alphanumeric | Permanent (no end date) |
| **isin** | International identifier | 12-char (US prefix) | Permanent (no end date) |
| **bloomberg** | Professional identifier | ticker format | Permanent |
| **reuters** | News/data identifier | RIC format | Permanent |

#### Reference Data Validation Rules

**CUSIP Validation**:
```python
def validate_cusip(cusip: str) -> bool:
    return (
        len(cusip) == 9 and
        cusip.isalnum() and
        cusip.isupper()
    )
```

**ISIN Validation**:
```python
def validate_isin(isin: str) -> bool:
    return (
        len(isin) == 12 and
        isin.startswith('US') and  # US instruments only
        isin[2:].isalnum()
    )
```

**Exchange Code Validation**:
```python
VALID_EXCHANGES = {'NYSE', 'NASDAQ', 'NYSE ARCA', 'BATS', 'OTC'}

def validate_exchange(exchange: str) -> bool:
    return exchange in VALID_EXCHANGES
```

### 4. Listing/Delisting Lifecycle Management

#### Lifecycle States
```python
LIFECYCLE_STATES = {
    'pre_ipo': 'Announced but not yet trading',
    'ipo': 'Recently listed (< 1 year)',
    'established': 'Actively trading (1-10 years)',
    'mature': 'Long-term listing (10+ years)',
    'warning': 'Delisting warning issued',
    'suspended': 'Trading suspended',
    'delisted': 'No longer trading on exchange',
    'otc': 'Trading over-the-counter only'
}
```

#### Date Tracking Requirements

**Listing Date**:
- Format: YYYY-MM-DD
- Source Priority: EODHD primary, Polygon validation
- Validation: Must be ≤ current date
- Required: Yes for all active instruments

**Delisting Date**:
- Format: YYYY-MM-DD  
- Source Priority: EODHD exclusively
- Validation: Must be > listing_date
- Required: Yes for delisted instruments

**Exchange Migration Dates**:
- Track each exchange change with precise dates
- No gaps allowed between exchange periods
- End date of previous = start date of next exchange

### 5. Real-World Test Scenarios

Based on sampled real market data, the system must handle:

#### Scenario 1: Stable Major Instruments
```
Example: AAPL (Apple Inc)
- Single exchange throughout life (NASDAQ since 1980-12-12)
- Complete reference data (CUSIP: 037833100, ISIN: US0378331005)
- No exchange migrations
- Expected: Single exchange xref, permanent CUSIP/ISIN xrefs
```

#### Scenario 2: NYSE Blue Chip Instruments  
```
Example: JNJ (Johnson & Johnson)
- Single exchange (NYSE since 1944-09-24) 
- Mature lifecycle (80+ years)
- Institutional grade reference data
- Expected: Long-term stability validation
```

#### Scenario 3: Delisted Instruments
```
Example: BIOCQ (Biocept Inc.)
- NASDAQ listing ended (delisted in 2024)
- Bankruptcy proceedings
- Symbol suffix 'Q' indicates bankruptcy
- Expected: End-dated exchange xref, historical-only status
```

#### Scenario 4: Symbol Change History
```
Example: AAMI (Acadian Asset Management)
- Corporate action resulted in symbol change
- Reference data continuity maintained
- Historical symbol mapping required
- Expected: Symbol change tracking in xrefs
```

#### Scenario 5: Exchange Downgrade
```
Example: SHPW (Shapeways Holdings)
- Original NASDAQ listing (2021-06-15)
- Downgraded to OTC (2023-08-15)
- Risk analysis implications
- Expected: Multi-period exchange xrefs showing migration
```

### 6. Data Quality & Validation Framework

#### Quality Scoring Algorithm
```python
def calculate_quality_score(instrument_data: Dict) -> int:
    score = 0
    
    # Required fields (20 points each)
    required_fields = ['symbol', 'name', 'exchange']
    for field in required_fields:
        if instrument_data.get(field):
            score += 20
    
    # Reference data (10 points each)
    if validate_cusip(instrument_data.get('cusip', '')):
        score += 10
    if validate_isin(instrument_data.get('isin', '')):
        score += 10
    
    # Date consistency (10 points each)
    if instrument_data.get('listing_date'):
        score += 10
    
    # Cross-vendor consistency (10 points each)
    if instrument_data.get('vendor_agreement', {}).get('exchange_match'):
        score += 10
    if instrument_data.get('vendor_agreement', {}).get('symbol_match'):
        score += 10
    
    return min(score, 100)  # Cap at 100%
```

#### Quality Thresholds
- **Tier 1 (Score ≥ 90)**: Production ready, high confidence
- **Tier 2 (Score ≥ 70)**: Acceptable with monitoring
- **Tier 3 (Score ≥ 50)**: Requires manual review
- **Tier 4 (Score < 50)**: Reject or flag for investigation

### 7. Implementation Architecture

#### Database Schema Extensions
```sql
-- Enhanced dev_instruments table
ALTER TABLE dev_instruments ADD COLUMN eodhd_raw JSONB;
ALTER TABLE dev_instruments ADD COLUMN polygon_raw JSONB;  
ALTER TABLE dev_instruments ADD COLUMN tiingo_raw JSONB;
ALTER TABLE dev_instruments ADD COLUMN reconciliation_notes TEXT[];
ALTER TABLE dev_instruments ADD COLUMN quality_score INTEGER;
ALTER TABLE dev_instruments ADD COLUMN data_sources TEXT[];
ALTER TABLE dev_instruments ADD COLUMN last_validated_at TIMESTAMP;

-- Vendor tracking  
INSERT INTO vendors (vendor_name, vendor_description) VALUES 
('eodhd', 'EODHD Financial APIs - Primary data source'),
('polygon', 'Polygon.io - Market data validation'),
('tiingo', 'Tiingo APIs - Additional validation'),
('cusip', 'CUSIP Global Services - US identifiers'),
('isin', 'International Securities Identification Numbers');
```

#### Processing Pipeline
```
1. EODHD Data Extraction
   ├── Fetch all US instruments from EODHD
   ├── Apply quality filters (exclude funds, low-quality OTC)
   ├── Extract exchange history and symbol changes
   
2. Multi-Vendor Validation
   ├── Cross-reference with Polygon API (active instruments)
   ├── Validate exchange mappings and dates
   ├── Flag discrepancies for manual review
   
3. Reference Data Processing
   ├── Validate CUSIP format and check digits
   ├── Validate ISIN format and country codes
   ├── Create permanent reference xrefs
   
4. Exchange History Reconstruction  
   ├── Build temporal exchange timeline
   ├── Detect migrations and downgrades
   ├── Create dated exchange xrefs
   
5. Quality Assessment & Scoring
   ├── Calculate comprehensive quality scores
   ├── Generate reconciliation reports
   ├── Flag instruments requiring manual review
```

### 8. Operational Procedures

#### Daily Operations
- **Incremental Updates**: Process new/changed instruments from EODHD
- **Status Monitoring**: Check for newly delisted or migrated instruments
- **Quality Alerts**: Monitor for data quality degradation or vendor discrepancies

#### Weekly Operations  
- **Full Reconciliation**: Complete cross-vendor validation cycle
- **Reference Data Refresh**: Update CUSIP/ISIN mappings
- **Exception Review**: Manual review of flagged instruments

#### Monthly Operations
- **Historical Validation**: Verify exchange migration dates against corporate actions
- **Vendor Coverage Analysis**: Assess completeness across data sources
- **Quality Trend Analysis**: Track data quality metrics over time

### 9. Success Metrics

#### Data Quality KPIs
- **Completeness**: >95% of instruments have required reference data
- **Accuracy**: <1% discrepancy rate between vendors for key fields
- **Timeliness**: Delisting events captured within 24 hours
- **Consistency**: >98% exchange mapping agreement across sources

#### Business Impact KPIs
- **Risk Mitigation**: 100% coverage of exchange downgrade events
- **Regulatory Compliance**: Zero missed CUSIP/ISIN requirements
- **Operational Efficiency**: 90% reduction in manual reconciliation time
- **Trading Safety**: Zero trading errors due to stale instrument data

## Risk Assessment & Mitigation

### Technical Risks
- **API Rate Limits**: Stagger requests across vendors, implement caching
- **Data Schema Changes**: Version control and backwards compatibility testing
- **Vendor Outages**: Fallback to cached data with staleness warnings

### Data Quality Risks
- **Conflicting Information**: Clear precedence rules and manual escalation
- **Historical Data Gaps**: Accept limitations, flag incomplete records
- **Reference Data Errors**: Multi-source validation before committing updates

### Business Risks  
- **Regulatory Non-compliance**: Comprehensive audit trails and validation logs
- **Trading Disruptions**: Fail-safe modes with conservative fallbacks
- **Operational Dependencies**: Cross-training and documented procedures

## Implementation Timeline

### Phase 1: Foundation (Weeks 1-2)
- Database schema updates
- Basic EODHD integration
- Core reconciliation framework

### Phase 2: Multi-Vendor Integration (Weeks 3-4)  
- Polygon API integration
- Cross-vendor validation logic
- Quality scoring implementation

### Phase 3: Reference Data Enhancement (Weeks 5-6)
- CUSIP/ISIN validation and xref creation
- Exchange migration tracking
- Historical data reconciliation

### Phase 4: Production Deployment (Weeks 7-8)
- Full testing with real data scenarios
- Performance optimization
- Monitoring and alerting setup

This comprehensive PRD ensures robust, accurate, and maintainable multi-vendor instrument data reconciliation that meets regulatory requirements while providing operational excellence for trading and risk management systems.