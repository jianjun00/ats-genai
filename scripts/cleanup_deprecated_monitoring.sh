#!/bin/bash
# Clean up deprecated monitoring and fragmented backfill files
# Run this script after unified data quality service is deployed and validated

set -e

echo "🧹 ATS Deprecated Code Cleanup - Unified Data Quality Consolidation"
echo "=================================================================="

PROJECT_ROOT="/home/jianjun/ats-genai-pm"
cd "$PROJECT_ROOT"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Counters
REMOVED_COUNT=0
BACKED_UP_COUNT=0

echo -e "\n${BLUE}📋 Phase 1: Backup deprecated files before removal${NC}"

# Create backup directory
BACKUP_DIR="deprecated_monitoring_backup_$(date +%Y%m%d_%H%M%S)"
mkdir -p "$BACKUP_DIR"

# Function to backup and remove file
backup_and_remove() {
    local file="$1"
    local reason="$2"
    
    if [ -f "$file" ]; then
        echo -e "${YELLOW}📦 Backing up: $file${NC}"
        cp "$file" "$BACKUP_DIR/"
        echo -e "${RED}🗑️  Removing: $file ($reason)${NC}"
        rm "$file"
        ((REMOVED_COUNT++))
        ((BACKED_UP_COUNT++))
    else
        echo -e "${BLUE}ℹ️  Not found: $file (already removed or never existed)${NC}"
    fi
}

# Phase 1: Remove standalone monitoring scripts
echo -e "\n${YELLOW}🎯 Removing standalone monitoring scripts...${NC}"
backup_and_remove "run_coverage_monitoring.py" "Replaced by unified service"
backup_and_remove "coverage_dashboard.py" "Replaced by unified dashboard"
backup_and_remove "coverage_dashboard_fixed.py" "Replaced by unified service"
backup_and_remove "monitoring_system_design.md" "Replaced by unified PRD/DRD"

# Phase 2: Remove fragmented backfill scripts  
echo -e "\n${YELLOW}🔄 Removing fragmented backfill scripts...${NC}"
backup_and_remove "backfill_all_firstrate_30days.py" "Replaced by unified backfill orchestration"
backup_and_remove "batch_backfill.py" "Replaced by agent-driven backfill"
backup_and_remove "efficient_firstrate_backfill.py" "Replaced by MCP backfill tools"
backup_and_remove "quick_priority_backfill.py" "Replaced by agent priority scoring"  
backup_and_remove "ray_firstrate_backfill.py" "Replaced by unified orchestration"

# Phase 3: Remove standalone validation scripts
echo -e "\n${YELLOW}🔍 Removing standalone validation scripts...${NC}"
backup_and_remove "firstrate_validation_90days.py" "Replaced by unified validation"
backup_and_remove "download_recent_firstrate.py" "Replaced by agent-driven validation"

# Phase 4: Remove deprecated operational scripts
echo -e "\n${YELLOW}⚙️ Removing deprecated operational scripts...${NC}"
backup_and_remove "scripts/run_daily_coverage_monitoring.sh" "Replaced by unified monitoring"
backup_and_remove "scripts/setup_coverage_monitoring_cron.sh" "Replaced by unified agent"

# Phase 5: Remove deprecated documentation
echo -e "\n${YELLOW}📚 Removing deprecated documentation...${NC}"
backup_and_remove "docs/OPERATIONS_COVERAGE_MONITORING.md" "Replaced by unified docs"

# Phase 6: Remove duplicate schema files
echo -e "\n${YELLOW}🗄️ Removing duplicate schema files...${NC}"
backup_and_remove "src/db/migrations/coverage_monitoring_schema.sql" "Replaced by unified schema"
backup_and_remove "src/infrastructure/database/migrations/coverage_monitoring_schema.sql" "Replaced by unified schema"

# Phase 7: Clean up standalone validation scripts in scripts/
echo -e "\n${YELLOW}🧪 Removing standalone validation scripts in scripts/...${NC}"
backup_and_remove "scripts/firstrate_comprehensive_validation.py" "Replaced by unified issue detection"
backup_and_remove "scripts/firstrate_efficient_validation.py" "Replaced by unified quality scanning"
backup_and_remove "scripts/firstrate_quick_validation.py" "Replaced by unified validation"

echo -e "\n${BLUE}📊 Phase 2: Cleanup summary and verification${NC}"

echo -e "${GREEN}✅ Cleanup completed successfully!${NC}"
echo -e "📦 Files backed up: ${GREEN}$BACKED_UP_COUNT${NC}"
echo -e "🗑️  Files removed: ${GREEN}$REMOVED_COUNT${NC}"
echo -e "💾 Backup location: ${BLUE}$BACKUP_DIR${NC}"

echo -e "\n${BLUE}📋 Phase 3: Verification commands${NC}"
echo "Run these commands to verify unified service is working:"
echo ""
echo "# Verify unified service container"
echo "python -c \"from src.domains.data_quality.services.config.unified_data_quality_service_container import UnifiedDataQualityServiceContainer; print('✅ Unified service imports successfully')\""
echo ""
echo "# Verify unified database schema"
echo "psql -d dev_db -c \"SELECT COUNT(*) FROM dev_data_quality_issues;\""
echo ""
echo "# Verify no broken imports"
echo "python -m pytest tests/ --collect-only | grep ERROR || echo '✅ No import errors'"

echo -e "\n${BLUE}🚨 Phase 4: Rollback instructions (if needed)${NC}"
echo "If issues are discovered, restore files from backup:"
echo "cp $BACKUP_DIR/* ./"
echo ""
echo "Or restore specific files:"
echo "cp $BACKUP_DIR/run_coverage_monitoring.py ./"
echo "cp $BACKUP_DIR/coverage_dashboard_fixed.py ./"

echo -e "\n${YELLOW}⚠️  IMPORTANT NOTES:${NC}"
echo "• Database cleanup will be handled by separate migration script"
echo "• Backup directory preserved for 30 days minimum"
echo "• Unified data quality service should be fully operational before running this"
echo "• Test all functionality with unified service before finalizing cleanup"

echo -e "\n${GREEN}🎉 Deprecated code cleanup completed!${NC}"
echo "Fragmented monitoring systems consolidated into unified data quality framework."

# Create cleanup completion marker
echo "$(date): Deprecated monitoring code cleanup completed" > .cleanup_completed
echo "Removed files: $REMOVED_COUNT" >> .cleanup_completed
echo "Backup location: $BACKUP_DIR" >> .cleanup_completed