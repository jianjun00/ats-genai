#!/usr/bin/env python3
"""
Priority analysis for fixing broken imports
Following fail-fast principles - identify critical imports first
"""

from collections import defaultdict
import re

def analyze_import_priorities():
    """Analyze and categorize broken imports by priority"""
    
    # Critical business logic patterns (HIGHEST PRIORITY)
    critical_patterns = [
        r'domains\.ml\.services\.training_data',
        r'domains\.trading\.services',
        r'domains\.instruments\.services', 
        r'infrastructure\.database',
        r'storage\..*manager',
        r'universe_state',
        r'training_callback',
        r'UniverseManager',
        r'DatabaseManager'
    ]
    
    # Infrastructure patterns (HIGH PRIORITY)
    infrastructure_patterns = [
        r'infrastructure\.database\.connection_manager',
        r'infrastructure\.database\.migration_manager',
        r'infrastructure\.caching',
        r'infrastructure\.monitoring',
        r'infrastructure\.storage'
    ]
    
    # Legacy service patterns (MEDIUM PRIORITY)
    legacy_patterns = [
        r'services_legacy',
        r'web_services',
        r'analytics_modules',
        r'agents\.',
        r'frontfill\.',
        r'validation\.'
    ]
    
    # External library patterns (LOW PRIORITY - likely need installation)
    external_patterns = [
        r'matplotlib',
        r'plotly',
        r'streamlit',
        r'tensorflow',
        r'aiofiles',
        r'flask\.'
    ]
    
    # Migration/scaffolding patterns (LOWEST PRIORITY)
    migration_patterns = [
        r'migration',
        r'migrator',
        r'scaffold'
    ]
    
    return {
        'CRITICAL_BUSINESS_LOGIC': critical_patterns,
        'HIGH_INFRASTRUCTURE': infrastructure_patterns, 
        'MEDIUM_LEGACY': legacy_patterns,
        'LOW_EXTERNAL': external_patterns,
        'LOWEST_MIGRATION': migration_patterns
    }

def categorize_broken_import(import_name, patterns_dict):
    """Categorize a broken import by priority"""
    for priority, patterns in patterns_dict.items():
        for pattern in patterns:
            if re.search(pattern, import_name):
                return priority
    return 'UNCATEGORIZED'

def create_fix_plan():
    """Create systematic plan to fix broken imports"""
    print("🎯 BROKEN IMPORT FIX PLAN")
    print("=" * 80)
    
    patterns = analyze_import_priorities()
    
    print("\n📋 PRIORITY ORDER:")
    print("1. CRITICAL_BUSINESS_LOGIC - Core ML training, trading, universe state")
    print("2. HIGH_INFRASTRUCTURE - Database connections, storage, caching") 
    print("3. MEDIUM_LEGACY - Legacy services (can be deprecated)")
    print("4. LOW_EXTERNAL - External libraries (pip install)")
    print("5. LOWEST_MIGRATION - Migration scaffolding (can be removed)")
    
    print("\n🔧 SYSTEMATIC APPROACH:")
    print("1. Fix critical business logic imports first")
    print("2. Focus on current feature work (training data, universe state)")
    print("3. Use fail-fast approach - don't mask import errors")
    print("4. Remove broken legacy code rather than fixing")
    print("5. Install missing external dependencies only if actively used")
    
    print("\n⚠️  FAIL-FAST PRINCIPLES:")
    print("- Fix root causes, not symptoms") 
    print("- Remove broken code rather than adding workarounds")
    print("- Let import errors surface immediately")
    print("- Don't install unused dependencies")
    
    return patterns

if __name__ == "__main__":
    create_fix_plan()