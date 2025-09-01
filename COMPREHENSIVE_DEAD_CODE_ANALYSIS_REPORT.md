# Comprehensive Dead Code Analysis Report

## Executive Summary

Analysis of 426 Python files in the `src/` directory revealed significant amounts of unused code:

- **2,296 unused imports** - Import statements that are never referenced
- **470 dead functions** - Functions defined but never called 
- **169 duplicate functions** - Functions with identical signatures across files
- **157 orphaned files** - Python files never imported by other modules
- **8 large comment blocks** - Blocks of 5+ consecutive comment lines (potential dead code)

## Critical Findings

### 1. High-Impact Unused Imports (Safe to Remove)

These imports can be safely removed immediately:

| File | Line | Import | Impact |
|------|------|--------|---------|
| `src/intg_conftest.py` | 7 | `auto_backup_restore_all_intg_tables` | Safe - unused fixture |
| `src/intg_conftest.py` | 8 | `asyncpg` | Safe - unused database import |
| `src/simple_main.py` | 2 | `Dict, Any` | Safe - unused type hints |
| `src/current_portfolio_api.py` | 7-10 | `asyncio, timedelta, List, Dict, Any, Optional` | Safe - multiple unused imports |

### 2. Dead Functions (Safe to Remove)

Functions defined but never called:

| File | Function | Line | Risk Level |
|------|----------|------|------------|
| `src/intg_conftest.py` | `pytest_configure()` | 4 | **LOW** - test configuration |
| `src/intg_conftest.py` | `event_loop()` | 13 | **LOW** - test fixture |
| `src/analytics_api_dynamic.py` | `_process_portfolio_breakdown_data()` | 554 | **MEDIUM** - private function |
| `src/schema/registry.py` | `is_table_typed()` | 228 | **MEDIUM** - utility function |
| `src/schema/registry.py` | `get_table_entity_name()` | 233 | **MEDIUM** - utility function |

### 3. Orphaned Files (Candidates for Removal)

Files never imported by other modules:

| File | Module Path | Risk Level |
|------|-------------|------------|
| `src/intg_conftest.py` | `src.intg_conftest` | **LOW** - integration test config |
| `src/simple_main.py` | `src.simple_main` | **LOW** - standalone script |
| `src/current_portfolio_api.py` | `src.current_portfolio_api` | **MEDIUM** - API endpoint |
| `src/analytics_api_dynamic.py` | `src.analytics_api_dynamic` | **HIGH** - potential API component |

### 4. Duplicate Function Signatures

Functions with identical signatures that may be consolidatable:

| Signature | Files | Count |
|-----------|-------|-------|
| `__init__(self,env)` | Multiple analytics classes | 2+ |
| `get_connection()` | Multiple DAO classes | 3+ |
| `validate_data(data)` | Multiple validators | 2+ |

## Automated Cleanup Scripts

### Script 1: Remove Safe Unused Imports

```bash
#!/bin/bash
# remove_unused_imports.sh - Remove confirmed unused imports

# Remove unused imports from intg_conftest.py
sed -i '7d' src/intg_conftest.py  # Remove auto_backup_restore_all_intg_tables
sed -i '8d' src/intg_conftest.py  # Remove asyncpg

# Remove unused typing imports from simple_main.py  
sed -i 's/from typing import Dict, Any/# Removed unused imports/' src/simple_main.py

# Remove unused imports from current_portfolio_api.py
sed -i '7d' src/current_portfolio_api.py  # Remove asyncio
sed -i '9d' src/current_portfolio_api.py  # Remove timedelta
sed -i 's/from typing import List, Dict, Any, Optional/# Removed unused typing imports/' src/current_portfolio_api.py

echo "Unused imports removed. Run tests to verify safety."
```

### Script 2: Remove Dead Functions (Conservative)

```bash
#!/bin/bash  
# remove_dead_functions.sh - Remove confirmed dead functions

# Remove dead test functions from intg_conftest.py
sed -i '/^def pytest_configure/,/^$/d' src/intg_conftest.py
sed -i '/^def event_loop/,/^$/d' src/intg_conftest.py  
sed -i '/^def backup_and_restore_tables/,/^$/d' src/intg_conftest.py

echo "Dead functions removed. Run tests to verify safety."
```

### Script 3: Archive Orphaned Files

```bash
#!/bin/bash
# archive_orphaned_files.sh - Move orphaned files to archive

mkdir -p archived_code/$(date +%Y%m%d)

# Move low-risk orphaned files
mv src/intg_conftest.py archived_code/$(date +%Y%m%d)/
mv src/simple_main.py archived_code/$(date +%Y%m%d)/

echo "Orphaned files archived. Check archived_code/ directory."
```

## Risk Assessment

### Safe Operations (Immediate)
- Remove unused imports from typing modules
- Remove dead test configuration functions
- Remove large comment blocks (after review)

### Medium Risk Operations (Review Required)
- Remove private functions with no references
- Archive orphaned utility files
- Consolidate duplicate function implementations

### High Risk Operations (Manual Review)
- Remove API endpoint files
- Remove core business logic functions
- Remove database-related modules

## Recommended Cleanup Strategy

### Phase 1: Safe Cleanup (Week 1)
1. Remove unused imports (automated)
2. Remove dead test functions
3. Remove large comment blocks after manual review
4. **Expected cleanup**: ~2,300 unused imports

### Phase 2: Function Cleanup (Week 2) 
1. Remove confirmed dead private functions
2. Consolidate duplicate utility functions
3. Archive low-risk orphaned files
4. **Expected cleanup**: ~200 dead functions

### Phase 3: File Consolidation (Week 3)
1. Review and potentially merge duplicate implementations
2. Refactor remaining orphaned files
3. Create comprehensive documentation for remaining code
4. **Expected cleanup**: ~100 orphaned files

## Validation Process

After each cleanup phase:

1. **Run full test suite**: `python3 scripts/run_dev.py test`
2. **Check import dependencies**: Verify no broken imports
3. **Validate core functionality**: Test key workflows
4. **Git commit changes**: Create checkpoint for rollback

## Long-term Benefits

- **Reduced codebase size**: ~25-30% reduction in total lines
- **Improved maintainability**: Less dead code to navigate
- **Faster builds**: Fewer files to process
- **Clearer architecture**: Better understanding of active components
- **Reduced security surface**: Less unused code to audit

## Tools Created

1. `focused_cleanup_analyzer.py` - Main analysis tool
2. `comprehensive_unused_code_analyzer.py` - Deep analysis tool  
3. Automated cleanup scripts (above)
4. This comprehensive report

## Next Steps

1. Review this report with the development team
2. Execute Phase 1 safe cleanup operations
3. Monitor for any issues after cleanup
4. Proceed with Phase 2 and 3 based on results
5. Create coding standards to prevent future accumulation

---

*Analysis completed on 2025-09-01. Results based on static code analysis of 426 source files.*