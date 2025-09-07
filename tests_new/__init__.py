"""
Organized Test Structure - Aligned with src/ Directory.

This new test structure follows the 7-item directory rule and mirrors
the src/ organization for better maintainability:

Structure Mapping:
- core/          -> Tests for src/core/ (platform, security, dao, etc.)
- domains/       -> Tests for src/domains/ (market_data, ml, etc.)
- signals/       -> Tests for src/signals/ (indicators, technical analysis)
- services/      -> Tests for src/services/ (analytics, APIs, etc.)
- integration/   -> End-to-end integration tests
- unit/          -> Pure unit tests (fast, isolated)

Migration Strategy:
- Phase 1: Create new structure (completed)
- Phase 2: Move critical tests first (dao, core, signals)
- Phase 3: Gradually migrate remaining tests
- Phase 4: Update CI/CD to use new structure
- Phase 5: Remove old test directory

Benefits:
- Easier test discovery and navigation
- Clear separation of unit vs integration tests
- Mirrors source code organization
- Follows 7-item directory constraint
"""

__version__ = "1.0.0"
