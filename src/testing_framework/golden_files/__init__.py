"""
Golden File Testing Framework

Provides generic infrastructure for deterministic business object testing:
- BaseGoldenFileManager[T]: Generic golden file management
- BaseGoldenTestCase: Common testing patterns
- Business object serialization utilities

Usage:
    from testing_framework.golden_files import BaseGoldenFileManager, BaseGoldenTestCase
    from testing_framework.golden_files.universe_state_golden_manager import UniverseStateGoldenFileManager
"""

from .base_golden_test_framework import BaseGoldenFileManager, BaseGoldenTestCase, TestDataHelper
from .universe_state_golden_manager import (
    UniverseStateGoldenFileManager, 
    UniverseStateGoldenTestCase,
    UniverseStateCollectionGoldenManager
)

__all__ = [
    'BaseGoldenFileManager',
    'BaseGoldenTestCase', 
    'TestDataHelper',
    'UniverseStateGoldenFileManager',
    'UniverseStateGoldenTestCase',
    'UniverseStateCollectionGoldenManager'
]