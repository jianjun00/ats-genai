"""
Universal Database Access Layer

Consolidates ALL database access patterns from 272+ files:
- 60 DAO files (11,135+ lines) → Generic repository pattern
- 212 files with database connections → Unified connection management  
- Scattered CRUD operations → Type-safe query operations
- Multiple connection management → Single connection system

TARGET CONSOLIDATION: 11,135+ lines → 2,500 lines (78% reduction)
"""

# Import main classes from repository module
from .repository import (
    ConnectionManager,
    QueryBuilder, 
    BaseRepository,
    VendorDataRepository,
    RepositoryFactory
)

__all__ = [
    'ConnectionManager',
    'QueryBuilder',
    'BaseRepository', 
    'VendorDataRepository',
    'RepositoryFactory'
]