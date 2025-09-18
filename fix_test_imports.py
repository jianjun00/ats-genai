#!/usr/bin/env python3
"""
Script to fix remaining test import paths after directory restructuring.
"""

import os
import sys
from pathlib import Path

# Enhanced mapping for test files that weren't caught in the first pass
TEST_IMPORT_MAPPINGS = {
    # Agents moved to domains/data_quality/agents
    'from src.agents.': 'from src.domains.data_quality.agents.',
    'import src.agents.': 'import src.domains.data_quality.agents.',
    
    # Events moved to domains/analytics/events  
    'from src.events.': 'from src.domains.analytics.events.',
    'import src.events.': 'import src.domains.analytics.events.',
    
    # Signals moved to domains/trading/signals
    'from src.signals.': 'from src.domains.trading.signals.',
    'import src.signals.': 'import src.domains.trading.signals.',
    
    # Shared moved to core/shared
    'from src.shared.': 'from src.core.shared.shared.',
    'import src.shared.': 'import src.core.shared.shared.',
    
    # Interfaces moved to infrastructure/interfaces
    'from src.interfaces.': 'from src.infrastructure.interfaces.',
    'import src.interfaces.': 'import src.infrastructure.interfaces.',
    
    # ML moved to domains/ml/legacy or domains/ml/services
    'from src.ml.': 'from src.domains.ml.legacy.',
    'import src.ml.': 'import src.domains.ml.legacy.',
    
    # Monitoring moved to infrastructure/monitoring/legacy
    'from src.monitoring.': 'from src.infrastructure.monitoring.legacy.',
    'import src.monitoring.': 'import src.infrastructure.monitoring.legacy.',
    
    # Observability moved to infrastructure/monitoring/observability
    'from src.observability.': 'from src.infrastructure.monitoring.observability.',
    'import src.observability.': 'import src.infrastructure.monitoring.observability.',
    
    # Jobs moved to infrastructure/jobs
    'from src.jobs.': 'from src.infrastructure.jobs.',
    'import src.jobs.': 'import src.infrastructure.jobs.',
    
    # MCP tools moved to infrastructure/tools/mcp
    'from src.mcp_tools.': 'from src.infrastructure.tools.mcp.',
    'import src.mcp_tools.': 'import src.infrastructure.tools.mcp.',
    
    # Services - more specific mappings
    'from src.services.analytics_service': 'from src.domains.analytics.services.analytics_service',
    'import src.services.analytics_service': 'import src.domains.analytics.services.analytics_service',
    'from src.services.data_quality.': 'from src.domains.data_quality.services.',
    'import src.services.data_quality.': 'import src.domains.data_quality.services.',
    'from src.services.ml_services.': 'from src.domains.ml.services.ml_services.',
    'import src.services.ml_services.': 'import src.domains.ml.services.ml_services.',
    'from src.services.financial_events.': 'from src.domains.analytics.services.financial_events.',
    'import src.services.financial_events.': 'import src.domains.analytics.services.financial_events.',
    'from src.services.data_services.': 'from src.infrastructure.data.data_services.',
    'import src.services.data_services.': 'import src.infrastructure.data.data_services.',
    'from src.services.web_services.': 'from src.infrastructure.web.web_services.',
    'import src.services.web_services.': 'import src.infrastructure.web.web_services.',
    'from src.services.core.': 'from src.domains.trading.services.core.',
    'import src.services.core.': 'import src.domains.trading.services.core.',
    'from src.services.': 'from src.infrastructure.services_legacy.',
    'import src.services.': 'import src.infrastructure.services_legacy.',
}

def update_file_imports(file_path: Path) -> bool:
    """Update imports in a single test file."""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        original_content = content
        
        # Apply test import mappings
        for old_import, new_import in TEST_IMPORT_MAPPINGS.items():
            content = content.replace(old_import, new_import)
        
        # Write back if changed
        if content != original_content:
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(content)
            print(f"✅ Fixed: {file_path}")
            return True
        
        return False
    
    except Exception as e:
        print(f"❌ Error updating {file_path}: {e}")
        return False

def main():
    """Main function to fix test imports."""
    
    # List of test files that need fixing (from grep output)
    test_files_to_fix = [
        "tests/integration/test_backtest_generates_portfolio_files.py",
        "tests/integration/test_schema_aware_training_generation.py", 
        "tests/integration/test_model_registry_integration.py",
        "tests/integration/test_sequence_selection_end_to_end.py",
        "tests/integration/test_five_two_indicators.py",
        "tests/integration/test_five_one_indicators.py",
        "tests/integration/test_five_one_indicators_simple.py",
        "tests/integration/test_dataset_service_integration.py",
        "tests/shared/clients/test_dataset_client.py",
        "tests/infrastructure/storage/test_multi_scale_sequence.py",
        "tests/services/test_analytics_service_comprehensive.py",
        "tests/services/test_dataset_service.py",
        "tests/agents/test_system_monitor_fail_fast.py",
        "tests/core/config/test_environment_aware_api_integration.py",
        "tests/unit/test_sequence_selection_critical_fixes.py",
        "tests/events/test_event_integration.py",
        "tests/monitoring/test_data_validation_reporter.py",
    ]
    
    updated_count = 0
    total_count = len(test_files_to_fix)
    
    for test_file in test_files_to_fix:
        file_path = Path(test_file)
        if file_path.exists():
            if update_file_imports(file_path):
                updated_count += 1
        else:
            print(f"⚠️ File not found: {file_path}")
    
    print(f"\n🎉 Fixed {updated_count} test files out of {total_count} total files")
    return 0

if __name__ == "__main__":
    sys.exit(main())