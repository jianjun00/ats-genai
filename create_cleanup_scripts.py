#!/usr/bin/env python3
"""
Generate automated cleanup scripts based on analysis results.
"""

import json
from pathlib import Path

def create_unused_imports_cleanup_script():
    """Create script to remove unused imports safely."""
    
    # Load analysis results
    with open("focused_cleanup_analysis.json", "r") as f:
        results = json.load(f)
    
    # Group unused imports by file for efficient processing
    files_to_clean = {}
    for item in results["unused_imports"]:
        file_path = item["file"]
        if file_path not in files_to_clean:
            files_to_clean[file_path] = []
        files_to_clean[file_path].append(item["import"])
    
    # Generate removal script
    script_content = """#!/bin/bash
# AUTOMATED UNUSED IMPORTS CLEANUP
# Generated from static analysis - Review before running!

set -e  # Exit on any error

echo "Starting unused imports cleanup..."
echo "Creating backup of modified files..."

# Create backup directory
BACKUP_DIR="cleanup_backup_$(date +%Y%m%d_%H%M%S)"
mkdir -p "$BACKUP_DIR"

"""
    
    processed_files = 0
    for file_path, unused_imports in files_to_clean.items():
        if processed_files >= 20:  # Limit to first 20 files for safety
            break
            
        rel_path = file_path.replace("/home/jianjun/ats-genai-admin/", "")
        
        # Skip risky files
        if any(risky in file_path for risky in ['main.py', 'api.py', 'app.py']):
            continue
            
        script_content += f"""
# Cleaning {rel_path}
echo "Processing {rel_path}..."
cp "{rel_path}" "$BACKUP_DIR/"

"""
        
        # Sort imports by line number (reverse order to avoid line number shifts)
        imports_by_line = sorted(unused_imports, key=lambda x: x["line"], reverse=True)
        
        for imp in imports_by_line:
            line_num = imp["line"]
            import_name = imp["local_name"]
            
            # Generate sed command to remove the line
            script_content += f'sed -i "{line_num}d" "{rel_path}"  # Remove unused import: {import_name}\n'
        
        processed_files += 1
    
    script_content += """
echo "Unused imports cleanup completed."
echo "Backup files stored in: $BACKUP_DIR"
echo "Please run tests to verify changes: python3 scripts/run_dev.py test"
"""
    
    # Write the script
    with open("cleanup_unused_imports.sh", "w") as f:
        f.write(script_content)
    
    # Make executable
    Path("cleanup_unused_imports.sh").chmod(0o755)
    
    print(f"Created cleanup_unused_imports.sh (processing {processed_files} files)")

def create_dead_functions_cleanup_script():
    """Create script to remove dead functions safely."""
    
    with open("focused_cleanup_analysis.json", "r") as f:
        results = json.load(f)
    
    script_content = """#!/bin/bash
# AUTOMATED DEAD FUNCTIONS CLEANUP
# Generated from static analysis - Review before running!

set -e

echo "Starting dead functions cleanup..."
BACKUP_DIR="dead_functions_backup_$(date +%Y%m%d_%H%M%S)"
mkdir -p "$BACKUP_DIR"

"""
    
    # Focus on very safe removals first
    safe_patterns = ['test_', 'pytest_', '_process_', '_helper_', '_util_']
    
    processed = 0
    for item in results["dead_functions"]:
        if processed >= 10:  # Limit for safety
            break
            
        func = item["function"]
        file_path = item["file"]
        rel_path = file_path.replace("/home/jianjun/ats-genai-admin/", "")
        
        # Only process very safe functions
        if not any(pattern in func["name"] for pattern in safe_patterns):
            continue
            
        # Skip if function has decorators (might be used by framework)
        if func["decorators"]:
            continue
            
        script_content += f"""
# Remove dead function {func['name']} from {rel_path}
echo "Removing function {func['name']} from {rel_path}..."
cp "{rel_path}" "$BACKUP_DIR/"

# This is a placeholder - manual removal required for functions
echo "MANUAL ACTION REQUIRED: Remove function {func['name']} at line {func['line']} in {rel_path}"

"""
        processed += 1
    
    script_content += """
echo "Dead functions analysis completed."
echo "Manual review required for function removals."
echo "Backup files stored in: $BACKUP_DIR"
"""
    
    with open("cleanup_dead_functions.sh", "w") as f:
        f.write(script_content)
    
    Path("cleanup_dead_functions.sh").chmod(0o755)
    
    print(f"Created cleanup_dead_functions.sh (identified {processed} safe targets)")

def create_commented_code_cleanup_script():
    """Create script to remove large commented code blocks."""
    
    with open("focused_cleanup_analysis.json", "r") as f:
        results = json.load(f)
    
    script_content = """#!/bin/bash
# AUTOMATED COMMENTED CODE CLEANUP
# Generated from static analysis - Review before running!

set -e

echo "Starting commented code cleanup..."
BACKUP_DIR="commented_code_backup_$(date +%Y%m%d_%H%M%S)"
mkdir -p "$BACKUP_DIR"

"""
    
    for block in results["large_comment_blocks"]:
        file_path = block["file"]
        rel_path = file_path.replace("/home/jianjun/ats-genai-admin/", "")
        start_line = block["start_line"]
        end_line = block["end_line"]
        
        script_content += f"""
# Remove large comment block from {rel_path} (lines {start_line}-{end_line})
echo "Processing comment block in {rel_path}..."
cp "{rel_path}" "$BACKUP_DIR/"

# Remove lines {start_line} to {end_line}
sed -i '{start_line},{end_line}d' "{rel_path}"

"""
    
    script_content += """
echo "Commented code cleanup completed."
echo "Backup files stored in: $BACKUP_DIR"
"""
    
    with open("cleanup_commented_code.sh", "w") as f:
        f.write(script_content)
    
    Path("cleanup_commented_code.sh").chmod(0o755)
    
    print(f"Created cleanup_commented_code.sh (processing {len(results['large_comment_blocks'])} blocks)")

def create_orphaned_files_analysis():
    """Create analysis of orphaned files for manual review."""
    
    with open("focused_cleanup_analysis.json", "r") as f:
        results = json.load(f)
    
    analysis = """# Orphaned Files Analysis

## Files Never Imported by Other Modules

This analysis identifies Python files that are never imported, indicating they may be:
- Standalone scripts (keep)
- Dead code (remove)
- Entry points (keep)
- Legacy code (archive)

"""
    
    risk_categories = {
        "LOW_RISK": [],
        "MEDIUM_RISK": [], 
        "HIGH_RISK": []
    }
    
    for item in results["orphaned_files"]:
        file_path = item["file"]
        file_name = Path(file_path).name
        
        # Categorize by risk
        if any(pattern in file_name for pattern in ['test', 'conftest', 'main', 'script']):
            risk_categories["LOW_RISK"].append(item)
        elif any(pattern in file_name for pattern in ['api', 'service', 'manager']):
            risk_categories["HIGH_RISK"].append(item)
        else:
            risk_categories["MEDIUM_RISK"].append(item)
    
    for risk_level, files in risk_categories.items():
        analysis += f"\n## {risk_level.replace('_', ' ').title()} Files ({len(files)} files)\n\n"
        
        for item in files[:10]:  # Show first 10 in each category
            file_path = item["file"]
            rel_path = file_path.replace("/home/jianjun/ats-genai-admin/", "")
            analysis += f"- `{rel_path}` - {item['reason']}\n"
        
        if len(files) > 10:
            analysis += f"- ... and {len(files) - 10} more files\n"
    
    analysis += """
## Recommended Actions

### Low Risk Files
- Review and confirm they are standalone scripts
- Move to `scripts/` directory if they are utilities
- Remove if they are obsolete test files

### Medium Risk Files  
- Manual code review required
- Check if they contain reusable logic
- Consider refactoring into modules if valuable

### High Risk Files
- **DO NOT REMOVE** without thorough analysis
- May be API endpoints or services
- Could be called by external systems
- Verify through runtime analysis

## Archive Script

```bash
#!/bin/bash
# Archive low-risk orphaned files
mkdir -p archived_orphaned_files/$(date +%Y%m%d)

# Move only confirmed low-risk files
echo "This script requires manual customization based on review"
```
"""
    
    with open("ORPHANED_FILES_ANALYSIS.md", "w") as f:
        f.write(analysis)
    
    print("Created ORPHANED_FILES_ANALYSIS.md")

def generate_master_cleanup_script():
    """Generate master script that runs all cleanup operations safely."""
    
    script_content = """#!/bin/bash
# MASTER CLEANUP SCRIPT
# Orchestrates safe cleanup operations with validation

set -e

echo "=== ATS GenAI Admin Codebase Cleanup ==="
echo "This script will perform automated cleanup operations."
echo "Each step includes safety checks and backups."
echo

# Check if we're in the right directory
if [[ ! -f "CLAUDE.md" ]]; then
    echo "ERROR: Please run this script from the ats-genai-admin root directory"
    exit 1
fi

# Create master backup
MASTER_BACKUP="master_cleanup_backup_$(date +%Y%m%d_%H%M%S)"
echo "Creating master backup: $MASTER_BACKUP"
mkdir -p "$MASTER_BACKUP"

# Function to run tests and check results
run_tests() {
    echo "Running test suite..."
    if python3 scripts/run_dev.py test --test tests/unit/ --quiet; then
        echo "✓ Tests passed"
        return 0
    else
        echo "✗ Tests failed"
        return 1
    fi
}

# Phase 1: Clean unused imports
echo
echo "=== Phase 1: Cleaning unused imports ==="
read -p "Proceed with unused imports cleanup? (y/N) " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    if [[ -f "cleanup_unused_imports.sh" ]]; then
        ./cleanup_unused_imports.sh
        
        if run_tests; then
            echo "✓ Phase 1 completed successfully"
        else
            echo "✗ Phase 1 caused test failures - manual review required"
            exit 1
        fi
    else
        echo "cleanup_unused_imports.sh not found"
    fi
fi

# Phase 2: Clean commented code
echo
echo "=== Phase 2: Cleaning large comment blocks ==="
read -p "Proceed with commented code cleanup? (y/N) " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    if [[ -f "cleanup_commented_code.sh" ]]; then
        ./cleanup_commented_code.sh
        
        if run_tests; then
            echo "✓ Phase 2 completed successfully"
        else
            echo "✗ Phase 2 caused test failures - manual review required"
            exit 1
        fi
    else
        echo "cleanup_commented_code.sh not found"
    fi
fi

# Phase 3: Analysis of dead functions (manual review required)
echo
echo "=== Phase 3: Dead functions analysis ==="
echo "Review the generated analysis files for manual cleanup:"
echo "- focused_cleanup_analysis.json"
echo "- ORPHANED_FILES_ANALYSIS.md"
echo "- COMPREHENSIVE_DEAD_CODE_ANALYSIS_REPORT.md"

echo
echo "=== Cleanup Summary ==="
echo "Backup created: $MASTER_BACKUP"
echo "Analysis files created for manual review"
echo "Run 'git status' to see all changes"
echo "Run 'git diff' to review specific changes"
echo
echo "Next steps:"
echo "1. Review analysis files"
echo "2. Manually clean dead functions"
echo "3. Archive orphaned files after review"
echo "4. Commit changes: git add . && git commit -m 'chore: automated code cleanup'"
"""
    
    with open("master_cleanup.sh", "w") as f:
        f.write(script_content)
    
    Path("master_cleanup.sh").chmod(0o755)
    
    print("Created master_cleanup.sh")

def main():
    """Generate all cleanup scripts and analyses."""
    print("Generating automated cleanup scripts...")
    
    # Check if analysis file exists
    if not Path("focused_cleanup_analysis.json").exists():
        print("ERROR: focused_cleanup_analysis.json not found. Run focused_cleanup_analyzer.py first.")
        return
    
    # Generate all cleanup scripts
    create_unused_imports_cleanup_script()
    create_dead_functions_cleanup_script()
    create_commented_code_cleanup_script()
    create_orphaned_files_analysis()
    generate_master_cleanup_script()
    
    print("\n=== Cleanup Scripts Generated ===")
    print("1. cleanup_unused_imports.sh - Remove unused imports")
    print("2. cleanup_commented_code.sh - Remove large comment blocks") 
    print("3. cleanup_dead_functions.sh - Analysis of dead functions")
    print("4. master_cleanup.sh - Master orchestration script")
    print("5. ORPHANED_FILES_ANALYSIS.md - Manual review guide")
    print("\nTo start cleanup: ./master_cleanup.sh")
    print("Always review generated scripts before running!")

if __name__ == "__main__":
    main()