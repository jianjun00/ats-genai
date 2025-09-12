# Src Directory Consolidation Plan

## Current Problem: 52 items in src/ (should be ≤7)

## Target Structure (7 items):
```
src/
├── core/           # Platform infrastructure (already organized)
├── domains/        # Business domains (market_data, ml, trading, instruments)
├── services/       # Application services and APIs
├── infrastructure/ # External systems, monitoring, storage
├── interfaces/     # APIs, CLI, web UI
├── shared/         # Common utilities, types, validation
└── lib/           # Reusable utility libraries
```

## Consolidation Mapping:

### → domains/
**Consolidate business logic by domain**
- `market_data/` → `domains/market_data/`
- `ml/` → `domains/ml/`
- `signals/` → `domains/trading/signals/`
- `universe/` → `domains/trading/universe/`
- `portfolio/` → `domains/trading/portfolio/`
- `secmaster/` → `domains/instruments/secmaster/`
- `models/` → `domains/ml/models/`
- `modeling/` → `domains/ml/modeling/`
- `state/` → `domains/trading/state/`
- `sentiment/` → `domains/analytics/sentiment/`
- `schema/` → `domains/ml/schema/`

### → services/
**Application and business services**
- `services/` (keep existing)
- `api/` → `services/api/`
- `analytics/` → `services/analytics/`
- `frontfill/` → `services/data_management/`
- `pipeline/` → `services/pipeline/`

### → infrastructure/
**External systems and platform concerns**
- `infrastructure/` (keep existing, consolidate duplicates)
- `db/` → `infrastructure/database/` (merge with existing)
- `vendor/` → `infrastructure/vendor/`
- `monitoring/` → `infrastructure/monitoring/` (merge with existing)
- `storage/` → `infrastructure/storage/` (merge with existing)
- `llm/` → `infrastructure/llm/`

### → interfaces/
**User-facing interfaces**
- `interfaces/` (keep existing)
- `frontend/` → `interfaces/web_ui/`
- `main.py` → `interfaces/cli/main.py`
- `simple_main.py` → `interfaces/cli/simple_main.py`

### → shared/
**Cross-cutting concerns**
- `shared/` (keep existing, consolidate duplicates)
- `utils/` → `shared/utils/` (merge with existing)
- `validation/` → `shared/validation/`
- `config/` → `shared/config/` (if any remaining)
- `auth/` → `shared/auth/` (if any remaining)
- `calendars/` → `shared/calendars/` (if any remaining)

### → Remove/Clean
**Files to remove or consolidate**
- Duplicate directories with same content
- Empty directories
- `__pycache__/` directories
- Backup files (*.bak)
- Temporary files
- `update_imports.py` (move to root scripts/)

## Implementation Steps:

1. **Phase 1**: Remove duplicates and empty directories
2. **Phase 2**: Move domains (market_data, ml, trading components)
3. **Phase 3**: Consolidate infrastructure components
4. **Phase 4**: Organize services and interfaces
5. **Phase 5**: Clean up shared utilities
6. **Phase 6**: Update imports and validate

## Expected Results:
- `src/`: 52 → 7 items ✅
- Clear separation of concerns ✅
- Eliminate duplicate directories ✅
- Maintain backward compatibility ✅
- Follow domain-driven design ✅
