#!/bin/bash
# ATS GenAI Admin - Safe Codebase Cleanup Script
# This script performs only LOW-RISK cleanup operations

set -e  # Exit on any error

echo "🧹 ATS GenAI Admin - Safe Codebase Cleanup"
echo "=========================================="

# Create backup directory
BACKUP_DIR="cleanup_backup_$(date +%Y%m%d_%H%M%S)"
mkdir -p "$BACKUP_DIR"

echo "📋 Creating backup in: $BACKUP_DIR"

# Function to backup before removing
backup_and_remove() {
    local file="$1"
    if [[ -f "$file" ]]; then
        cp "$file" "$BACKUP_DIR/$(basename "$file")"
        echo "  ✅ Backed up: $file"
        rm "$file"
        echo "  🗑️  Removed: $file"
    fi
}

echo ""
echo "🔍 Phase 1: Remove Duplicate Files"
echo "-----------------------------------"

# Remove duplicate minute_price_service.py (keeping the one in /services/minute/)
if [[ -f "src/services/intraday/minute_price_service.py" ]]; then
    backup_and_remove "src/services/intraday/minute_price_service.py"
    echo "  ✅ Removed duplicate minute_price_service.py"
else
    echo "  ℹ️  No duplicate minute_price_service.py found"
fi

echo ""
echo "🧹 Phase 2: Remove Temporary Files" 
echo "-----------------------------------"

# Remove checkpoint files
echo "Removing checkpoint files..."
find . -name "*_checkpoint.json" -type f | while read -r file; do
    backup_and_remove "$file"
done

# Remove results files  
echo "Removing results files..."
find . -name "*_results_*.json" -type f | while read -r file; do
    backup_and_remove "$file"
done

# Remove specific temporary files
temp_files=(
    "docker_parallel_launch_info.json"
    "simple_production_launch_info.json"
    "test_backfill_progress.json"
    "test_analysis.json"
    "unified_analytics_test_report.json"
)

for file in "${temp_files[@]}"; do
    if [[ -f "$file" ]]; then
        backup_and_remove "$file"
    fi
done

echo ""
echo "📁 Phase 3: Create Proper Directory Structure"
echo "---------------------------------------------"

# Create test directory structure if it doesn't exist
mkdir -p tests/{integration,validation,analysis,utilities}
echo "  ✅ Created test directory structure"

# Create scripts directory structure  
mkdir -p scripts/{analysis,monitoring,utilities}
echo "  ✅ Created scripts directory structure"

echo ""
echo "📋 Phase 4: Generate Cleanup Report"
echo "-----------------------------------"

# Count remaining issues for reporting
total_py_files=$(find . -name "*.py" -not -path "./.git/*" -not -path "./.*" | wc -l)
root_test_files=$(find . -maxdepth 1 -name "test_*.py" | wc -l)
root_check_files=$(find . -maxdepth 1 -name "check_*.py" | wc -l)
root_validate_files=$(find . -maxdepth 1 -name "validate_*.py" | wc -l)

echo "📊 Cleanup Summary:"
echo "  • Total Python files: $total_py_files"
echo "  • Root-level test files remaining: $root_test_files"
echo "  • Root-level check files remaining: $root_check_files" 
echo "  • Root-level validate files remaining: $root_validate_files"
echo "  • Backup created: $BACKUP_DIR"

echo ""
echo "⚠️  Manual Steps Still Required:"
echo "1. Move root-level test_*.py files to tests/integration/"
echo "2. Review and move check_*.py, validate_*.py files" 
echo "3. Replace debug print statements with proper logging"
echo "4. Remove commented code blocks (86 blocks identified)"
echo "5. Address TODO/FIXME comments (122 found)"

echo ""
echo "📖 Next Steps:"
echo "1. Review COMPREHENSIVE_CLEANUP_REPORT.md for detailed recommendations"
echo "2. Check detailed_cleanup_recommendations.json for file-specific actions"
echo "3. Execute manual cleanup phases as outlined in the report"

echo ""
echo "✅ Safe cleanup completed successfully!"
echo "🔍 Review the backup directory if you need to restore any files"

# Create a summary file
cat > cleanup_summary.txt << EOF
ATS GenAI Admin - Cleanup Summary
Generated: $(date)

Safe Operations Completed:
✅ Removed duplicate files
✅ Removed temporary/checkpoint files  
✅ Created proper directory structure
✅ Generated cleanup reports

Files backed up to: $BACKUP_DIR

Manual Steps Required:
- Move test files to proper directories
- Replace debug prints with logging
- Remove commented code blocks
- Address TODO/FIXME comments

See COMPREHENSIVE_CLEANUP_REPORT.md for full details.
EOF

echo "📝 Summary saved to: cleanup_summary.txt"