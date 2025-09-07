# Analytics Service Split Plan

## Current: 2,374 lines in single file
## Target: Multiple focused modules <500 lines each

## Split Strategy:

### 1. Core Service (analytics_service_core.py) - ~400 lines
- Main UnifiedAnalyticsService class initialization  
- Configuration and basic setup
- Service coordination

### 2. Type-Aware Analysis (type_aware_analyzer.py) - ~350 lines
- get_intelligent_filters()
- Type system integration
- Schema-based analysis

### 3. Dashboard Generator (dashboard_generator.py) - ~400 lines
- EDA dashboard HTML generation
- Chart and visualization setup
- Dashboard template integration

### 4. Data Analysis Engine (data_analysis_engine.py) - ~450 lines
- Data filtering and aggregation
- Statistical analysis
- Query execution

### 5. Ray Integration (ray_analytics.py) - ~300 lines
- Ray distributed computing features
- Parallel processing
- Scalable analytics

### 6. Request Handler (analytics_request_handler.py) - ~400 lines
- HTTP request handling
- API endpoints
- Response formatting

### 7. Server Manager (analytics_server.py) - ~100 lines
- Server startup and configuration
- Main entry point
- Service orchestration

Total: ~2,400 lines across 7 focused modules
Each module: <500 lines ✅
