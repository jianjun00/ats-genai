#!/usr/bin/env python3
"""
Advanced Duplicate Code Detection
Finds identical and similar code blocks across the codebase.
"""

import ast
import hashlib
import difflib
from pathlib import Path
from collections import defaultdict
from typing import Dict, List, Set, Tuple

class DuplicateCodeDetector:
    def __init__(self, root_dir: str):
        self.root_dir = Path(root_dir)
        self.src_files = [f for f in self.root_dir.rglob("*.py") if "/src/" in str(f)]
        
        # Storage for analysis
        self.function_hashes = defaultdict(list)  # hash -> [(file, func_name, line, code)]
        self.code_blocks = defaultdict(list)      # hash -> [(file, line_start, line_end, code)]
        self.import_patterns = defaultdict(list)  # pattern -> [files]
        
    def normalize_code(self, code: str) -> str:
        """Normalize code for comparison by removing whitespace and comments."""
        # Remove comments and normalize whitespace
        lines = []
        for line in code.split('\n'):
            line = line.strip()
            if line and not line.startswith('#'):
                # Replace multiple spaces with single space
                line = ' '.join(line.split())
                lines.append(line)
        return '\n'.join(lines)
    
    def get_function_signature(self, node: ast.FunctionDef) -> str:
        """Get normalized function signature."""
        args = []
        for arg in node.args.args:
            args.append(arg.arg)
        for arg in node.args.kwonlyargs:
            args.append(arg.arg)
            
        return f"{node.name}({','.join(args)})"
    
    def hash_code_block(self, code: str) -> str:
        """Create hash of normalized code block."""
        normalized = self.normalize_code(code)
        return hashlib.md5(normalized.encode()).hexdigest()
    
    def analyze_file_for_duplicates(self, file_path: Path):
        """Analyze a single file for duplicate patterns."""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
                
            tree = ast.parse(content, filename=str(file_path))
            lines = content.split('\n')
            
            # Analyze functions
            for node in ast.walk(tree):
                if isinstance(node, ast.FunctionDef):
                    # Get function code
                    start_line = node.lineno - 1
                    end_line = node.end_lineno if hasattr(node, 'end_lineno') else start_line + 10
                    func_code = '\n'.join(lines[start_line:end_line])
                    
                    # Hash function body (excluding signature)
                    body_lines = []
                    in_body = False
                    for line in func_code.split('\n'):
                        if ':' in line and 'def ' in line:
                            in_body = True
                            continue
                        if in_body:
                            body_lines.append(line)
                    
                    if body_lines:
                        body_code = '\n'.join(body_lines)
                        body_hash = self.hash_code_block(body_code)
                        
                        self.function_hashes[body_hash].append({
                            'file': str(file_path),
                            'function': node.name,
                            'line': node.lineno,
                            'signature': self.get_function_signature(node),
                            'code': func_code[:200] + '...' if len(func_code) > 200 else func_code
                        })
            
            # Analyze code blocks (groups of 5+ lines)
            for i in range(0, len(lines) - 4):  # Minimum 5 lines
                block = '\n'.join(lines[i:i+5])
                if block.strip() and not block.strip().startswith('#'):
                    block_hash = self.hash_code_block(block)
                    self.code_blocks[block_hash].append({
                        'file': str(file_path),
                        'line_start': i + 1,
                        'line_end': i + 5,
                        'code': block
                    })
            
            # Analyze import patterns
            for node in ast.walk(tree):
                if isinstance(node, (ast.Import, ast.ImportFrom)):
                    if isinstance(node, ast.Import):
                        pattern = f"import {','.join(alias.name for alias in node.names)}"
                    else:
                        pattern = f"from {node.module} import {','.join(alias.name for alias in node.names)}"
                    
                    self.import_patterns[pattern].append(str(file_path))
                    
        except Exception as e:
            print(f"Error analyzing {file_path}: {e}")
    
    def find_similar_functions(self, threshold: float = 0.8) -> List[Dict]:
        """Find functions with similar code using text similarity."""
        similar_groups = []
        
        all_functions = []
        for occurrences in self.function_hashes.values():
            all_functions.extend(occurrences)
        
        # Compare all function pairs
        for i, func1 in enumerate(all_functions):
            for func2 in all_functions[i+1:]:
                # Skip if same file and same function
                if func1['file'] == func2['file'] and func1['function'] == func2['function']:
                    continue
                
                # Calculate similarity
                similarity = difflib.SequenceMatcher(
                    None, 
                    self.normalize_code(func1['code']), 
                    self.normalize_code(func2['code'])
                ).ratio()
                
                if similarity >= threshold:
                    similar_groups.append({
                        'similarity': similarity,
                        'functions': [func1, func2]
                    })
        
        return sorted(similar_groups, key=lambda x: x['similarity'], reverse=True)
    
    def generate_duplicate_report(self) -> Dict:
        """Generate comprehensive duplicate code report."""
        
        # Find exact duplicate functions
        exact_duplicate_functions = []
        for code_hash, occurrences in self.function_hashes.items():
            if len(occurrences) > 1:
                exact_duplicate_functions.append({
                    'hash': code_hash,
                    'count': len(occurrences),
                    'occurrences': occurrences
                })
        
        # Find exact duplicate code blocks
        duplicate_code_blocks = []
        for block_hash, occurrences in self.code_blocks.items():
            if len(occurrences) > 1:
                duplicate_code_blocks.append({
                    'hash': block_hash,
                    'count': len(occurrences),
                    'occurrences': occurrences[:5]  # Show first 5 occurrences
                })
        
        # Find common import patterns
        common_imports = []
        for pattern, files in self.import_patterns.items():
            if len(files) > 5:  # Appears in 5+ files
                common_imports.append({
                    'pattern': pattern,
                    'count': len(files),
                    'files': files[:10]  # Show first 10 files
                })
        
        # Find similar functions
        similar_functions = self.find_similar_functions()
        
        return {
            'exact_duplicate_functions': sorted(exact_duplicate_functions, key=lambda x: x['count'], reverse=True),
            'duplicate_code_blocks': sorted(duplicate_code_blocks, key=lambda x: x['count'], reverse=True)[:20],
            'common_import_patterns': sorted(common_imports, key=lambda x: x['count'], reverse=True)[:20],
            'similar_functions': similar_functions[:20],
            'statistics': {
                'total_files_analyzed': len(self.src_files),
                'exact_duplicate_functions': len(exact_duplicate_functions),
                'duplicate_code_blocks': len(duplicate_code_blocks),
                'similar_functions_found': len(similar_functions)
            }
        }
    
    def run_analysis(self):
        """Run the complete duplicate code analysis."""
        print(f"Analyzing {len(self.src_files)} source files for duplicates...")
        
        for i, file_path in enumerate(self.src_files):
            if i % 50 == 0 and i > 0:
                print(f"Processed {i}/{len(self.src_files)} files...")
            
            self.analyze_file_for_duplicates(file_path)
        
        print("Generating duplicate code report...")
        return self.generate_duplicate_report()

def create_duplicate_cleanup_recommendations(report: Dict) -> str:
    """Create actionable recommendations for duplicate code cleanup."""
    
    recommendations = """# Duplicate Code Cleanup Recommendations

## Executive Summary

"""
    
    stats = report['statistics']
    recommendations += f"""
- **{stats['exact_duplicate_functions']}** sets of identical functions found
- **{stats['duplicate_code_blocks']}** duplicate code blocks identified  
- **{stats['similar_functions_found']}** similar functions detected
- **{len(report['common_import_patterns'])}** common import patterns

## Top Duplicate Functions (Exact Matches)

"""
    
    for i, dup in enumerate(report['exact_duplicate_functions'][:10]):
        recommendations += f"""
### {i+1}. Duplicate Function Set ({dup['count']} occurrences)

**Files:**
"""
        for occ in dup['occurrences']:
            file_name = Path(occ['file']).name
            recommendations += f"- `{file_name}:{occ['line']}` - `{occ['function']}()`\n"
        
        recommendations += f"""
**Code Preview:**
```python
{dup['occurrences'][0]['code']}
```

**Recommendation:** Consolidate into single utility function in appropriate module.

"""
    
    recommendations += """
## Top Similar Functions (>80% similarity)

"""
    
    for i, sim in enumerate(report['similar_functions'][:5]):
        func1, func2 = sim['functions']
        file1 = Path(func1['file']).name
        file2 = Path(func2['file']).name
        
        recommendations += f"""
### {i+1}. Similar Functions ({sim['similarity']:.2%} similarity)

- `{file1}:{func1['line']}` - `{func1['function']}()`
- `{file2}:{func2['line']}` - `{func2['function']}()`

**Recommendation:** Review for consolidation or shared base class.

"""
    
    recommendations += """
## Common Import Patterns

The following imports appear in many files and could be candidates for a common imports module:

"""
    
    for pattern in report['common_import_patterns'][:10]:
        recommendations += f"- `{pattern['pattern']}` (appears in {pattern['count']} files)\n"
    
    recommendations += """

## Automated Cleanup Scripts

### Script 1: Extract Common Utility Functions

```python
# consolidate_duplicate_functions.py
# Extract duplicate functions to common utility modules

def extract_duplicate_function(source_files, target_module, function_name):
    # Implementation to move duplicate functions to shared module
    pass
```

### Script 2: Create Common Imports Module

```python  
# create_common_imports.py
# Create shared imports module for common patterns

COMMON_IMPORTS = '''
# Common imports used across the codebase
from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta
from pathlib import Path
import asyncio
import logging
'''

def create_common_imports_module():
    with open('src/common/imports.py', 'w') as f:
        f.write(COMMON_IMPORTS)
```

## Refactoring Strategy

### Phase 1: Extract Utility Functions
1. Identify top 10 duplicate functions
2. Create appropriate utility modules
3. Replace duplicates with imports
4. Run tests to verify

### Phase 2: Consolidate Similar Functions  
1. Review similar functions manually
2. Create shared base classes where appropriate
3. Refactor to use inheritance/composition
4. Update tests and documentation

### Phase 3: Common Imports
1. Create common imports module
2. Update files to use common imports
3. Remove redundant import statements
4. Validate with import analysis

## Risk Assessment

- **Low Risk**: Identical utility functions (safe to consolidate)
- **Medium Risk**: Similar functions with small differences
- **High Risk**: Functions with identical names but different behavior

"""
    
    return recommendations

def main():
    detector = DuplicateCodeDetector("/home/jianjun/ats-genai-admin")
    report = detector.run_analysis()
    
    # Save detailed report
    import json
    with open("duplicate_code_analysis.json", "w") as f:
        json.dump(report, f, indent=2, default=str)
    
    # Generate cleanup recommendations  
    recommendations = create_duplicate_cleanup_recommendations(report)
    with open("DUPLICATE_CODE_CLEANUP_RECOMMENDATIONS.md", "w") as f:
        f.write(recommendations)
    
    # Print summary
    stats = report['statistics']
    print(f"\n=== DUPLICATE CODE ANALYSIS RESULTS ===")
    print(f"Files analyzed: {stats['total_files_analyzed']}")
    print(f"Exact duplicate functions: {stats['exact_duplicate_functions']}")
    print(f"Duplicate code blocks: {stats['duplicate_code_blocks']}")
    print(f"Similar functions: {stats['similar_functions_found']}")
    
    if report['exact_duplicate_functions']:
        print(f"\nTop duplicate function:")
        top_dup = report['exact_duplicate_functions'][0]
        print(f"  {top_dup['count']} identical copies found")
        func_name = top_dup['occurrences'][0]['function']
        print(f"  Function: {func_name}()")
        
    print(f"\nReports saved:")
    print(f"  - duplicate_code_analysis.json (detailed data)")
    print(f"  - DUPLICATE_CODE_CLEANUP_RECOMMENDATIONS.md (action plan)")

if __name__ == "__main__":
    main()