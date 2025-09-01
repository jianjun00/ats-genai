#!/usr/bin/env python3
"""
Quick Duplicate Code Finder - Optimized for speed
"""

import hashlib
from pathlib import Path
from collections import defaultdict

def quick_duplicate_analysis():
    """Fast analysis of duplicate patterns in src/ directory."""
    
    src_dir = Path("/home/jianjun/ats-genai-admin/src")
    python_files = list(src_dir.rglob("*.py"))
    
    print(f"Quick analysis of {len(python_files)} files...")
    
    # Find duplicate file names
    file_names = defaultdict(list)
    
    # Find duplicate import lines
    import_lines = defaultdict(list)
    
    # Find duplicate function signatures (simple regex)
    function_sigs = defaultdict(list)
    
    for file_path in python_files:
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
                lines = content.split('\n')
                
            file_name = file_path.name
            file_names[file_name].append(str(file_path))
            
            for line_num, line in enumerate(lines, 1):
                stripped = line.strip()
                
                # Track import statements
                if stripped.startswith(('import ', 'from ')):
                    import_lines[stripped].append((str(file_path), line_num))
                
                # Track function definitions (simple pattern)
                if stripped.startswith('def ') and '(' in stripped:
                    sig = stripped.split(':')[0].strip()
                    function_sigs[sig].append((str(file_path), line_num))
                    
        except Exception as e:
            print(f"Error reading {file_path}: {e}")
    
    # Generate results
    results = {
        'duplicate_file_names': {name: paths for name, paths in file_names.items() if len(paths) > 1},
        'common_imports': {imp: locs for imp, locs in import_lines.items() if len(locs) > 5},
        'duplicate_function_sigs': {sig: locs for sig, locs in function_sigs.items() if len(locs) > 1},
    }
    
    print(f"\n=== QUICK DUPLICATE ANALYSIS RESULTS ===")
    print(f"Duplicate file names: {len(results['duplicate_file_names'])}")
    print(f"Common imports (5+ files): {len(results['common_imports'])}")
    print(f"Duplicate function signatures: {len(results['duplicate_function_sigs'])}")
    
    return results

def print_top_findings(results):
    """Print the most significant findings."""
    
    print(f"\n=== TOP DUPLICATE FILE NAMES ===")
    for name, paths in list(results['duplicate_file_names'].items())[:5]:
        print(f"\n{name} ({len(paths)} copies):")
        for path in paths[:3]:  # Show first 3
            rel_path = path.replace("/home/jianjun/ats-genai-admin/", "")
            print(f"  - {rel_path}")
    
    print(f"\n=== MOST COMMON IMPORTS ===")  
    common_imports_sorted = sorted(results['common_imports'].items(), 
                                 key=lambda x: len(x[1]), reverse=True)
    for imp, locations in common_imports_sorted[:10]:
        print(f"{imp} (in {len(locations)} files)")
    
    print(f"\n=== DUPLICATE FUNCTION SIGNATURES ===")
    dup_funcs_sorted = sorted(results['duplicate_function_sigs'].items(),
                             key=lambda x: len(x[1]), reverse=True)
    for sig, locations in dup_funcs_sorted[:10]:
        if len(locations) > 2:  # Only show significant duplicates
            print(f"\n{sig} ({len(locations)} copies):")
            for path, line in locations[:3]:
                rel_path = path.replace("/home/jianjun/ats-genai-admin/", "")
                file_name = Path(path).name
                print(f"  - {file_name}:{line}")

def generate_simple_cleanup_recommendations(results):
    """Generate actionable cleanup recommendations."""
    
    recommendations = """# Quick Cleanup Recommendations

## Immediate Actions

### 1. Consolidate Duplicate File Names

"""
    
    for name, paths in results['duplicate_file_names'].items():
        if len(paths) > 1:
            recommendations += f"""
**{name}** - {len(paths)} copies found:
"""
            for path in paths:
                rel_path = path.replace("/home/jianjun/ats-genai-admin/", "")
                recommendations += f"- {rel_path}\n"
            recommendations += "\n**Action:** Review and consolidate or rename for clarity.\n"
    
    recommendations += """
### 2. Create Common Imports Module

Most frequently imported modules that could be centralized:

"""
    
    common_imports_sorted = sorted(results['common_imports'].items(), 
                                 key=lambda x: len(x[1]), reverse=True)
    for imp, locations in common_imports_sorted[:10]:
        recommendations += f"- `{imp}` (used in {len(locations)} files)\n"
    
    recommendations += """

**Action:** Create `src/common/imports.py` with these common imports.

### 3. Review Duplicate Functions

"""
    
    dup_funcs_sorted = sorted(results['duplicate_function_sigs'].items(),
                             key=lambda x: len(x[1]), reverse=True)
    for sig, locations in dup_funcs_sorted[:5]:
        if len(locations) > 2:
            recommendations += f"""
**{sig}** - {len(locations)} identical signatures:
"""
            for path, line in locations:
                file_name = Path(path).name
                recommendations += f"- {file_name}:{line}\n"
            recommendations += "\n**Action:** Review for consolidation into utility module.\n"
    
    recommendations += """

## Quick Win Scripts

```bash
# Find all __init__.py files that might be empty
find src/ -name "__init__.py" -size 0

# Find files with very similar names
find src/ -name "*.py" | sort | uniq -d

# Count import statement frequencies  
grep -h "^import\|^from" src/**/*.py | sort | uniq -c | sort -nr | head -20
```

## Priority Order

1. **High Impact, Low Risk**: Remove unused imports
2. **Medium Impact, Low Risk**: Consolidate identical utility functions
3. **High Impact, Medium Risk**: Merge duplicate files after review
4. **Low Impact, High Value**: Create common imports module
"""
    
    return recommendations

def main():
    results = quick_duplicate_analysis()
    print_top_findings(results)
    
    # Save results
    import json
    with open("quick_duplicate_analysis.json", "w") as f:
        json.dump(results, f, indent=2, default=str)
    
    # Generate recommendations
    recommendations = generate_simple_cleanup_recommendations(results)
    with open("QUICK_CLEANUP_RECOMMENDATIONS.md", "w") as f:
        f.write(recommendations)
    
    print(f"\nFiles created:")
    print(f"  - quick_duplicate_analysis.json")
    print(f"  - QUICK_CLEANUP_RECOMMENDATIONS.md")

if __name__ == "__main__":
    main()