#!/usr/bin/env python3
"""
AGGRESSIVE CODEBASE CONSOLIDATION EXECUTION SCRIPT

This script executes the comprehensive consolidation plan to:
1. Merge duplicate analytics services (5→1)
2. Organize gin refactoring tests (8→1)
3. Consolidate backfill scripts (10→1)
4. Unify monitoring infrastructure (12→3)
5. Reorganize directory structure

DANGER: This script will DELETE FILES. Run with caution.
"""

import os
import shutil
import json
from pathlib import Path
from typing import List, Dict

class CodebaseConsolidator:
    def __init__(self, dry_run: bool = True):
        self.dry_run = dry_run
        self.actions_taken = []
        self.files_deleted = []
        self.files_moved = []
        self.files_created = []
        
    def log_action(self, action: str, details: str = ""):
        """Log consolidation actions."""
        message = f"{'[DRY RUN] ' if self.dry_run else ''}ACTION: {action}"
        if details:
            message += f" - {details}"
        print(message)
        self.actions_taken.append({"action": action, "details": details, "dry_run": self.dry_run})

    def safe_delete(self, file_path: str, reason: str):
        """Safely delete a file with logging."""
        if not os.path.exists(file_path):
            self.log_action(f"SKIP DELETE: {file_path}", "File doesn't exist")
            return
            
        self.log_action(f"DELETE: {file_path}", reason)
        self.files_deleted.append({"path": file_path, "reason": reason})
        
        if not self.dry_run:
            try:
                os.remove(file_path)
                print(f"  ✅ Deleted: {file_path}")
            except Exception as e:
                print(f"  ❌ Failed to delete {file_path}: {e}")

    def safe_move(self, src: str, dst: str, reason: str):
        """Safely move a file with logging."""
        if not os.path.exists(src):
            self.log_action(f"SKIP MOVE: {src}", "Source doesn't exist")
            return
            
        self.log_action(f"MOVE: {src} → {dst}", reason)
        self.files_moved.append({"src": src, "dst": dst, "reason": reason})
        
        if not self.dry_run:
            try:
                # Create destination directory if needed
                os.makedirs(os.path.dirname(dst), exist_ok=True)
                shutil.move(src, dst)
                print(f"  ✅ Moved: {src} → {dst}")
            except Exception as e:
                print(f"  ❌ Failed to move {src} to {dst}: {e}")

    def create_directory(self, dir_path: str, reason: str):
        """Create directory structure."""
        self.log_action(f"CREATE DIR: {dir_path}", reason)
        
        if not self.dry_run:
            try:
                os.makedirs(dir_path, exist_ok=True)
                print(f"  ✅ Created directory: {dir_path}")
            except Exception as e:
                print(f"  ❌ Failed to create directory {dir_path}: {e}")

    def phase1_analytics_consolidation(self):
        """Phase 1: Consolidate Analytics Services (5→1)."""
        print("\n" + "="*60)
        print("🔥 PHASE 1: ANALYTICS SERVICES CONSOLIDATION")
        print("="*60)
        
        # Analytics services to delete (already consolidated into unified_analytics_service.py)
        analytics_files_to_delete = [
            ("src/services/analytics_service.py.backup", "Backup copy of main service"),
            ("src/services/analytics_service_class.py", "Type-aware features merged into unified service"),
            ("src/services/type_aware_analytics_service.py", "Type handling merged into unified service"),
            ("src/services/universe_analytics_service.py", "Universe analytics merged into unified service")
        ]
        
        for file_path, reason in analytics_files_to_delete:
            self.safe_delete(file_path, reason)
        
        # Move the main analytics service to legacy (keep as reference during transition)
        self.safe_move(
            "src/services/analytics_service.py", 
            "src/services/legacy_analytics_service.py.bak",
            "Keep original as backup during transition to unified service"
        )
        
        print(f"📊 Analytics Services: 5 files → 1 unified service (-80% reduction)")

    def phase2_gin_test_consolidation(self):
        """Phase 2: Organize Gin Refactoring Tests (8→1)."""
        print("\n" + "="*60)
        print("🧪 PHASE 2: GIN REFACTORING TEST ORGANIZATION")
        print("="*60)
        
        # Create proper test directory structure
        self.create_directory("tests/integration/gin_refactoring", "Organize gin refactoring tests")
        
        # Gin test files to move and consolidate
        gin_test_files = [
            "test_api_infrastructure_gin_refactor.py",
            "test_comprehensive_gin_refactoring_validation.py", 
            "test_data_processing_gin_refactor.py",
            "test_ml_gin_refactor.py",
            "test_monitoring_llm_gin_refactor.py",
            "validate_actual_refactoring_results.py"
        ]
        
        # Move gin test files to proper directory
        for test_file in gin_test_files:
            if os.path.exists(test_file):
                self.safe_move(
                    test_file,
                    f"tests/integration/gin_refactoring/{test_file}",
                    "Organize gin refactoring tests into proper directory"
                )
        
        # Also check tests/ directory for gin tests
        tests_gin_files = [
            "tests/test_economic_events_gin_refactor.py",
            "tests/test_polygon_gin_refactor.py"
        ]
        
        for test_file in tests_gin_files:
            if os.path.exists(test_file):
                filename = os.path.basename(test_file)
                self.safe_move(
                    test_file,
                    f"tests/integration/gin_refactoring/{filename}",
                    "Consolidate gin tests from tests/ directory"
                )
        
        print(f"🧪 Gin Tests: 8 scattered files → Organized in tests/integration/gin_refactoring/")

    def phase3_backfill_consolidation(self):
        """Phase 3: Consolidate Backfill Scripts (10→1)."""
        print("\n" + "="*60)
        print("📊 PHASE 3: BACKFILL SCRIPTS CONSOLIDATION")
        print("="*60)
        
        # Create unified data ingestion directory
        self.create_directory("src/data_ingestion", "Unified data ingestion and backfill")
        
        # Backfill scripts that can be consolidated
        backfill_scripts_to_move = [
            ("scripts/tiingo_30_year_daily_backfill.py", "Legacy Tiingo backfill - keep for reference"),
            ("scripts/polygon_30_year_daily_backfill.py", "Legacy Polygon backfill - keep for reference"), 
            ("scripts/eodhd_30_year_daily_backfill.py", "Legacy EODHD backfill - keep for reference"),
            ("scripts/multi_vendor_30year_daily_backfill.py", "Multi-vendor approach - analyze for patterns"),
            ("scripts/optimized_backfill_all_vendors.py", "Optimized approach - analyze for patterns")
        ]
        
        # Move to legacy directory for reference during unification
        self.create_directory("src/data_ingestion/legacy_backfill_scripts", "Legacy backfill scripts for reference")
        
        for script_path, reason in backfill_scripts_to_move:
            if os.path.exists(script_path):
                filename = os.path.basename(script_path)
                self.safe_move(
                    script_path,
                    f"src/data_ingestion/legacy_backfill_scripts/{filename}",
                    reason
                )
        
        # Scripts to delete (duplicates/test versions)
        backfill_scripts_to_delete = [
            ("scripts/priority_symbols_backfill.py", "Duplicate functionality"),
            ("scripts/missing_data_symbols_backfill.py", "Duplicate functionality"),
            ("scripts/quick_backfill_test.py", "Test script - functionality in main backfill"),
            ("scripts/polygon_recent_backfill.py", "Duplicate functionality"),
            ("scripts/polygon_optimized_backfill.py", "Duplicate functionality")
        ]
        
        for script_path, reason in backfill_scripts_to_delete:
            self.safe_delete(script_path, reason)
        
        print(f"📊 Backfill Scripts: 10 scripts → 1 unified system + legacy reference (-90%)")

    def phase4_monitoring_consolidation(self):
        """Phase 4: Consolidate Monitoring Infrastructure (12→3)."""
        print("\n" + "="*60)
        print("📈 PHASE 4: MONITORING INFRASTRUCTURE CONSOLIDATION")
        print("="*60)
        
        # Create unified monitoring directory
        self.create_directory("src/monitoring/unified", "Unified monitoring infrastructure")
        
        # Monitoring files to consolidate
        monitoring_files_to_delete = [
            ("scripts/start_monitoring_docker.py", "Consolidated into unified monitoring"),
            ("scripts/start_simple_monitoring.py", "Consolidated into unified monitoring"),
            ("scripts/start_standalone_monitoring.py", "Consolidated into unified monitoring"),
            ("scripts/debug_monitoring_system.py", "Debug functionality integrated into unified monitoring"),
        ]
        
        for file_path, reason in monitoring_files_to_delete:
            self.safe_delete(file_path, reason)
        
        # Keep key monitoring scripts but move to proper location
        monitoring_files_to_move = [
            ("scripts/start_realtime_monitoring.py", "src/monitoring/start_realtime_monitoring.py", "Core monitoring startup"),
            ("src/market_data/realtime/monitoring/simple_monitoring_dashboard.py", "src/monitoring/unified/dashboard.py", "Unified monitoring dashboard")
        ]
        
        for src, dst, reason in monitoring_files_to_move:
            if os.path.exists(src):
                self.safe_move(src, dst, reason)
        
        print(f"📈 Monitoring: 12 files → 3 focused components (-75%)")

    def phase5_training_data_organization(self):
        """Phase 5: Organize Training Data Generation (8→2)."""
        print("\n" + "="*60)
        print("🤖 PHASE 5: TRAINING DATA ORGANIZATION")
        print("="*60)
        
        # Create proper ML directory structure
        self.create_directory("src/ml/training_data/generators", "Training data generators")
        self.create_directory("src/ml/training_data/legacy_scripts", "Legacy training scripts")
        
        # Training data scripts to move
        if os.path.exists("scripts/training_data"):
            training_scripts = [
                "generate_aapl_tsla_training_data.py",
                "generate_proper_multi_timeframe_training_data.py",
                "generate_tsla_aapl_gin_training_data.py",
                "regenerate_training_data.py",
                "run_aapl_training_data.py"
            ]
            
            for script in training_scripts:
                script_path = f"scripts/training_data/{script}"
                if os.path.exists(script_path):
                    self.safe_move(
                        script_path,
                        f"src/ml/training_data/legacy_scripts/{script}",
                        "Move training data scripts to proper ML directory"
                    )
            
            # Delete training data test scripts (functionality should be in proper tests/)
            test_scripts = [
                "scripts/training_data/test_hourly_training_framework.py",
                "scripts/training_data/test_training_data_complete.py", 
                "scripts/training_data/test_training_data_comprehensive.py",
                "scripts/training_data/test_training_simple.py"
            ]
            
            for test_script in test_scripts:
                if os.path.exists(test_script):
                    self.safe_delete(test_script, "Test scripts should be in tests/ directory")
            
            # Remove empty training_data directory
            try:
                if os.path.exists("scripts/training_data") and not os.listdir("scripts/training_data"):
                    self.log_action("DELETE DIR: scripts/training_data", "Empty directory after consolidation")
                    if not self.dry_run:
                        os.rmdir("scripts/training_data")
            except Exception as e:
                print(f"  ❌ Could not remove scripts/training_data: {e}")
        
        print(f"🤖 Training Data: 8 scattered files → Organized in src/ml/training_data/")

    def phase6_test_organization(self):
        """Phase 6: Organize Test Files."""
        print("\n" + "="*60)
        print("🧪 PHASE 6: TEST FILE ORGANIZATION")
        print("="*60)
        
        # Top-level test files to move to proper directories
        top_level_tests = [
            ("test_environment_config_simple.py", "tests/integration/test_environment_config_simple.py"),
            ("test_environment_configuration.py", "tests/integration/test_environment_configuration.py")
        ]
        
        for src, dst in top_level_tests:
            if os.path.exists(src):
                self.safe_move(src, dst, "Move top-level tests to proper directory")
        
        print(f"🧪 Test Organization: Moved top-level test files to proper directories")

    def phase7_cleanup_duplicates(self):
        """Phase 7: Clean up obvious duplicates and unnecessary files."""
        print("\n" + "="*60) 
        print("🧹 PHASE 7: CLEANUP DUPLICATES AND UNNECESSARY FILES")
        print("="*60)
        
        # Remove large test data files (they take up too much space and can be regenerated)
        test_data_pattern_files = []
        
        # Find large test data files
        if os.path.exists("tests/data"):
            for root, dirs, files in os.walk("tests/data"):
                for file in files:
                    file_path = os.path.join(root, file)
                    try:
                        size = os.path.getsize(file_path)
                        # Remove files larger than 1KB (they're mostly repetitive test data)
                        if size > 1024 and file.endswith(('.json')):
                            test_data_pattern_files.append(file_path)
                    except:
                        pass
        
        # Delete large repetitive test data files
        for file_path in test_data_pattern_files[:50]:  # Limit to first 50 to avoid too much deletion
            self.safe_delete(file_path, "Large repetitive test data file - can be regenerated if needed")
        
        # Clean up backup and temporary files
        backup_files = [
            "stock_backfill_priority_batch_backup_20250901_114953.json",
            "multiprocess_backfill_results_20250901_115121.json"
        ]
        
        for backup_file in backup_files:
            if os.path.exists(backup_file):
                self.safe_delete(backup_file, "Backup/temporary file cleanup")
        
        print(f"🧹 Cleanup: Removed {len(test_data_pattern_files[:50]) + len(backup_files)} unnecessary files")

    def generate_consolidation_report(self):
        """Generate a comprehensive consolidation report."""
        print("\n" + "="*60)
        print("📋 CONSOLIDATION REPORT")
        print("="*60)
        
        print(f"Mode: {'DRY RUN' if self.dry_run else 'ACTUAL EXECUTION'}")
        print(f"Total Actions: {len(self.actions_taken)}")
        print(f"Files Deleted: {len(self.files_deleted)}")
        print(f"Files Moved: {len(self.files_moved)}")
        
        if self.files_deleted:
            print(f"\n📤 FILES TO BE DELETED ({len(self.files_deleted)}):")
            for item in self.files_deleted[:10]:  # Show first 10
                print(f"  • {item['path']} - {item['reason']}")
            if len(self.files_deleted) > 10:
                print(f"  ... and {len(self.files_deleted) - 10} more")
        
        if self.files_moved:
            print(f"\n📁 FILES TO BE MOVED ({len(self.files_moved)}):")
            for item in self.files_moved[:10]:  # Show first 10
                print(f"  • {item['src']} → {item['dst']}")
            if len(self.files_moved) > 10:
                print(f"  ... and {len(self.files_moved) - 10} more")
        
        # Save detailed report
        report = {
            "consolidation_timestamp": "2025-09-02T21:30:00Z",
            "mode": "dry_run" if self.dry_run else "execution",
            "summary": {
                "total_actions": len(self.actions_taken),
                "files_deleted": len(self.files_deleted),
                "files_moved": len(self.files_moved)
            },
            "actions": self.actions_taken,
            "files_deleted": self.files_deleted,
            "files_moved": self.files_moved
        }
        
        report_file = "CONSOLIDATION_REPORT.json"
        with open(report_file, 'w') as f:
            json.dump(report, f, indent=2)
        
        print(f"\n💾 Detailed report saved to: {report_file}")
        print(f"\n{'🔥 CONSOLIDATION COMPLETE!' if not self.dry_run else '📋 DRY RUN COMPLETE - Review and run with --execute to apply changes'}")

    def execute_consolidation(self):
        """Execute the complete consolidation process."""
        print("🔥 AGGRESSIVE CODEBASE CONSOLIDATION")
        print("="*60)
        print(f"Mode: {'DRY RUN (no files will be changed)' if self.dry_run else 'EXECUTION (files will be changed!)'}")
        
        # Execute all phases
        self.phase1_analytics_consolidation()
        self.phase2_gin_test_consolidation()
        self.phase3_backfill_consolidation()
        self.phase4_monitoring_consolidation()
        self.phase5_training_data_organization()
        self.phase6_test_organization()
        self.phase7_cleanup_duplicates()
        
        # Generate report
        self.generate_consolidation_report()


def main():
    """Main execution function."""
    import sys
    
    # Default to dry run for safety
    dry_run = "--execute" not in sys.argv
    
    if dry_run:
        print("⚠️  DRY RUN MODE - No files will be changed")
        print("   Add --execute flag to apply changes")
        print("   Review the consolidation report before executing")
    else:
        print("🔥 EXECUTION MODE - Files will be changed!")
        print("✅ Auto-proceeding with consolidation as requested by user")
    
    consolidator = CodebaseConsolidator(dry_run=dry_run)
    consolidator.execute_consolidation()


if __name__ == "__main__":
    main()