# Ultra Directory Refactoring Plan

## New Consolidated Structure (Max 7 items per level)

```
src/
├── api/          # Consolidated REST endpoints
│   ├── analytics/
│   ├── datasets/ 
│   ├── markets/
│   ├── models/
│   ├── trading/
│   └── webhooks/
├── core/         # Core business domain logic
│   ├── analytics/
│   ├── markets/
│   ├── models/
│   ├── trading/
│   ├── types/
│   └── validation/
├── data/         # Consolidated data access layer
│   ├── access/   # All DAOs consolidated  
│   ├── migrate/  # Migration utilities
│   ├── models/   # Database models
│   ├── query/    # Query builders
│   ├── storage/  # File/cache storage
│   └── stream/   # Real-time data streams
├── infra/        # Infrastructure layer
│   ├── auth/
│   ├── config/
│   ├── logging/
│   ├── monitor/
│   ├── network/
│   └── security/
├── lib/          # Reusable utilities & libraries
│   ├── calc/     # Calculations & math
│   ├── format/   # Data formatting
│   ├── parse/    # Data parsing
│   ├── time/     # Time utilities  
│   ├── validate/ # Validation helpers
│   └── viz/      # Visualization helpers
├── ml/           # Machine learning domain
│   ├── data/     # Training data generation
│   ├── eval/     # Model evaluation
│   ├── models/   # ML model implementations
│   ├── train/    # Training pipelines
│   └── utils/    # ML-specific utilities
└── web/          # Web interface layer
    ├── assets/
    ├── pages/
    └── templates/
```

## File Size Refactoring Strategy

### Giant Files to Split:
1. **analytics_service.py (3,817 lines)**
   - Split into: analytics_core.py, dashboard_builder.py, data_fetcher.py, viz_generator.py
   - Extract 1,456-line function into template engine

2. **indicator.py (2,043 lines)**  
   - Split into: base_indicator.py, technical_indicators.py, volume_indicators.py, price_indicators.py

3. **hybrid_minute_data_manager.py (1,702 lines)**
   - Split into: data_manager.py, storage_adapter.py, cache_manager.py, file_handler.py

### Function Refactoring (>200 lines):
- Extract template generation into separate template engine
- Break large calculation functions into smaller composable functions
- Use strategy pattern for complex conditional logic

## Migration Strategy

### Phase 1: Core Utilities (lib/)
- Extract common utilities first
- Create shared libraries that multiple modules use
- Establish clean interfaces

### Phase 2: Infrastructure (infra/)
- Consolidate config, logging, monitoring
- Remove duplication between src/ and core/

### Phase 3: Data Layer (data/)
- Merge src/dao/ and src/core/dao/
- Consolidate all database access
- Standardize query patterns

### Phase 4: Business Logic (core/)
- Move domain-specific logic
- Remove duplication between domains/ and root modules

### Phase 5: API & Web (api/, web/)  
- Consolidate REST endpoints
- Move frontend assets to web/

### Phase 6: Tests Alignment
- Mirror new src/ structure in tests/
- Consolidate test utilities