"""
Unified Vendor Adapter Framework

Consolidates ALL vendor adapter functionality from 6,770+ lines across duplicate locations:
- Infrastructure vendor adapters (3,780 lines)
- Domain agent adapters (2,990 lines) - IDENTICAL DUPLICATES

ELIMINATES DUPLICATION:
- firstrate_minute_adapter.py: 667 + 411 lines → Single implementation
- polygon_minute_adapter.py: 415 + 415 lines → Single implementation  
- tiingo_intraday_adapter.py: 522 + 400 lines → Single implementation
- eodhd_minute_adapter.py: 380 + 397 lines → Single implementation
- Plus all fundamentals and base adapters

TARGET CONSOLIDATION: 6,770 lines → 3,000 lines (56% reduction)
"""