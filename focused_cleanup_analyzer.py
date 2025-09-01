#!/usr/bin/env python3
"""
Focused Code Cleanup Analyzer for ATS GenAI Admin src/ directory
Identifies specific unused code patterns for safe removal.
"""

import ast
import os
import json
from pathlib import Path
from collections import defaultdict, Counter
from typing import Dict, List, Set, Any

class FocusedCleanupAnalyzer:
    def __init__(self, root_dir: str):
        self.root_dir = Path(root_dir)
        self.src_dir = self.root_dir / "src"
        
        # Only analyze source code files, not tests initially
        self.python_files = [
            f for f in self.src_dir.rglob("*.py") 
            if not any(part.startswith('test') for part in f.parts)
        ]
        
        print(f"Analyzing {len(self.python_files)} source Python files...")
        
        self.imports_by_file = {}
        self.functions_by_file = {}
        self.classes_by_file = {}
        self.imports_usage = defaultdict(set)  # import_name -> {files_that_use_it}
        self.function_calls = defaultdict(set)  # func_name -> {files_that_call_it}
        
    def analyze_file(self, file_path: Path) -> Dict[str, Any]:
        """Analyze a single Python file."""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            tree = ast.parse(content, filename=str(file_path))
            
            analysis = {
                "imports": [],
                "functions": [],
                "classes": [],
                "function_calls": [],
                "attribute_access": [],
                "issues": []
            }
            
            for node in ast.walk(tree):
                if isinstance(node, (ast.Import, ast.ImportFrom)):
                    self._analyze_import(node, analysis)
                elif isinstance(node, ast.FunctionDef):
                    self._analyze_function_def(node, analysis)
                elif isinstance(node, ast.ClassDef):
                    self._analyze_class_def(node, analysis)
                elif isinstance(node, ast.Call):
                    self._analyze_function_call(node, analysis)
                elif isinstance(node, ast.Attribute):
                    self._analyze_attribute(node, analysis)
                elif isinstance(node, ast.Name):
                    self._analyze_name_usage(node, analysis)
            
            # Find large blocks of commented code
            self._find_commented_code(content, analysis)
            
            return analysis
            
        except Exception as e:
            return {"error": str(e)}
    
    def _analyze_import(self, node, analysis):
        """Analyze import statements."""
        if isinstance(node, ast.Import):
            for alias in node.names:
                import_name = alias.asname or alias.name
                analysis["imports"].append({
                    "type": "import",
                    "name": alias.name,
                    "alias": alias.asname,
                    "local_name": import_name,
                    "line": node.lineno
                })
        elif isinstance(node, ast.ImportFrom):
            module = node.module or ""
            for alias in node.names:
                import_name = alias.asname or alias.name
                analysis["imports"].append({
                    "type": "from_import", 
                    "module": module,
                    "name": alias.name,
                    "alias": alias.asname,
                    "local_name": import_name,
                    "line": node.lineno
                })
    
    def _analyze_function_def(self, node, analysis):
        """Analyze function definitions."""
        analysis["functions"].append({
            "name": node.name,
            "line": node.lineno,
            "args": [arg.arg for arg in node.args.args],
            "is_private": node.name.startswith('_'),
            "is_test": node.name.startswith('test_'),
            "is_special": node.name.startswith('__') and node.name.endswith('__'),
            "decorators": [self._get_decorator_name(d) for d in node.decorator_list]
        })
    
    def _analyze_class_def(self, node, analysis):
        """Analyze class definitions."""
        analysis["classes"].append({
            "name": node.name,
            "line": node.lineno,
            "bases": [self._get_name_from_node(b) for b in node.bases],
            "is_private": node.name.startswith('_'),
            "decorators": [self._get_decorator_name(d) for d in node.decorator_list]
        })
    
    def _analyze_function_call(self, node, analysis):
        """Analyze function calls."""
        func_name = self._get_call_name(node)
        if func_name:
            analysis["function_calls"].append({
                "name": func_name,
                "line": node.lineno
            })
    
    def _analyze_attribute(self, node, analysis):
        """Analyze attribute access."""
        if isinstance(node.value, ast.Name):
            analysis["attribute_access"].append({
                "object": node.value.id,
                "attribute": node.attr,
                "line": node.lineno
            })
    
    def _analyze_name_usage(self, node, analysis):
        """Track name usage for imported items."""
        if isinstance(node.ctx, ast.Load):
            # This is a name being used (not defined)
            pass  # We'll use this in cross-reference analysis
    
    def _get_decorator_name(self, decorator):
        """Get decorator name from AST node."""
        if isinstance(decorator, ast.Name):
            return decorator.id
        elif isinstance(decorator, ast.Attribute):
            return f"{self._get_name_from_node(decorator.value)}.{decorator.attr}"
        return str(decorator)
    
    def _get_name_from_node(self, node):
        """Get name from AST node."""
        if isinstance(node, ast.Name):
            return node.id
        elif isinstance(node, ast.Attribute):
            return f"{self._get_name_from_node(node.value)}.{node.attr}"
        return str(node)
    
    def _get_call_name(self, node):
        """Get function name from call node."""
        if isinstance(node.func, ast.Name):
            return node.func.id
        elif isinstance(node.func, ast.Attribute):
            return node.func.attr
        return None
    
    def _find_commented_code(self, content, analysis):
        """Find large blocks of commented code."""
        lines = content.split('\n')
        comment_blocks = []
        current_block = []
        
        for i, line in enumerate(lines, 1):
            stripped = line.strip()
            if stripped.startswith('#') and len(stripped) > 5:
                current_block.append((i, line))
            else:
                if len(current_block) >= 5:  # 5+ consecutive comment lines
                    comment_blocks.append({
                        "start_line": current_block[0][0],
                        "end_line": current_block[-1][0],
                        "line_count": len(current_block),
                        "preview": current_block[0][1].strip()[:100]
                    })
                current_block = []
        
        if len(current_block) >= 5:
            comment_blocks.append({
                "start_line": current_block[0][0],
                "end_line": current_block[-1][0],
                "line_count": len(current_block),
                "preview": current_block[0][1].strip()[:100]
            })
        
        analysis["large_comment_blocks"] = comment_blocks
    
    def cross_reference_analysis(self):
        """Perform cross-reference analysis to find unused imports and functions."""
        print("Performing cross-reference analysis...")
        
        # Collect all names used across all files
        all_used_names = set()
        all_function_calls = set()
        
        for file_path, analysis in self.file_analyses.items():
            if "error" in analysis:
                continue
                
            # Collect function calls
            for call in analysis.get("function_calls", []):
                all_function_calls.add(call["name"])
                all_used_names.add(call["name"])
            
            # Collect attribute access
            for attr in analysis.get("attribute_access", []):
                all_used_names.add(attr["object"])
                all_used_names.add(attr["attribute"])
        
        # Find unused imports
        unused_imports = []
        for file_path, analysis in self.file_analyses.items():
            if "error" in analysis:
                continue
                
            for imp in analysis.get("imports", []):
                local_name = imp["local_name"]
                
                # Check if this import is used in this file
                file_used_names = set()
                for call in analysis.get("function_calls", []):
                    file_used_names.add(call["name"])
                for attr in analysis.get("attribute_access", []):
                    file_used_names.add(attr["object"])
                
                if local_name not in file_used_names:
                    # Additional checks for module usage patterns
                    module_parts = local_name.split('.')
                    if not any(part in file_used_names for part in module_parts):
                        unused_imports.append({
                            "file": file_path,
                            "import": imp,
                            "reason": f"'{local_name}' imported but never used"
                        })
        
        # Find potentially dead functions
        dead_functions = []
        all_defined_functions = {}
        
        for file_path, analysis in self.file_analyses.items():
            if "error" in analysis:
                continue
                
            for func in analysis.get("functions", []):
                if func["name"] not in all_defined_functions:
                    all_defined_functions[func["name"]] = []
                all_defined_functions[func["name"]].append((file_path, func))
        
        for func_name, definitions in all_defined_functions.items():
            if (func_name not in all_function_calls and 
                not func_name.startswith('__') and
                not func_name.startswith('test_') and
                func_name not in ['main', 'run', 'setup', 'teardown']):
                
                for file_path, func_def in definitions:
                    dead_functions.append({
                        "file": file_path,
                        "function": func_def,
                        "reason": f"Function '{func_name}' defined but never called"
                    })
        
        return unused_imports, dead_functions
    
    def generate_duplicate_analysis(self):
        """Find duplicate function implementations."""
        print("Analyzing for duplicate functions...")
        
        function_signatures = defaultdict(list)
        
        for file_path, analysis in self.file_analyses.items():
            if "error" in analysis:
                continue
                
            for func in analysis.get("functions", []):
                # Create signature based on name and arguments
                args_str = ",".join(func.get("args", []))
                signature = f"{func['name']}({args_str})"
                
                function_signatures[signature].append({
                    "file": file_path,
                    "function": func
                })
        
        duplicates = []
        for signature, occurrences in function_signatures.items():
            if len(occurrences) > 1:
                duplicates.append({
                    "signature": signature,
                    "occurrences": occurrences,
                    "count": len(occurrences)
                })
        
        return duplicates
    
    def find_orphaned_files(self):
        """Find Python files that are never imported."""
        print("Finding orphaned files...")
        
        # Get all imported modules
        imported_modules = set()
        
        for file_path, analysis in self.file_analyses.items():
            if "error" in analysis:
                continue
                
            for imp in analysis.get("imports", []):
                if imp["type"] == "from_import":
                    imported_modules.add(imp["module"])
                else:
                    imported_modules.add(imp["name"])
        
        # Check which files are never imported
        orphaned_files = []
        
        for py_file in self.python_files:
            relative_path = py_file.relative_to(self.root_dir)
            module_path = str(relative_path).replace('/', '.').replace('\\', '.').replace('.py', '')
            
            # Check various forms of module reference
            is_imported = False
            for imported in imported_modules:
                if (module_path.endswith(imported) or
                    imported.endswith(py_file.stem) or
                    py_file.stem in imported.split('.')):
                    is_imported = True
                    break
            
            if (not is_imported and
                py_file.stem not in ['__init__', 'main', 'conftest'] and
                not py_file.stem.startswith('test_')):
                
                orphaned_files.append({
                    "file": str(py_file),
                    "module_path": module_path,
                    "reason": "File never imported by other modules"
                })
        
        return orphaned_files
    
    def run_analysis(self):
        """Run the complete focused analysis."""
        print(f"Starting analysis of {len(self.python_files)} files...")
        
        # Analyze each file
        self.file_analyses = {}
        for i, py_file in enumerate(self.python_files):
            if i % 50 == 0 and i > 0:
                print(f"Processed {i}/{len(self.python_files)} files...")
            
            analysis = self.analyze_file(py_file)
            self.file_analyses[str(py_file)] = analysis
        
        # Cross-reference analysis
        unused_imports, dead_functions = self.cross_reference_analysis()
        duplicates = self.generate_duplicate_analysis()
        orphaned = self.find_orphaned_files()
        
        # Collect comment blocks
        large_comment_blocks = []
        for file_path, analysis in self.file_analyses.items():
            if "error" not in analysis:
                for block in analysis.get("large_comment_blocks", []):
                    block["file"] = file_path
                    large_comment_blocks.append(block)
        
        results = {
            "unused_imports": unused_imports,
            "dead_functions": dead_functions,
            "duplicate_functions": duplicates,
            "orphaned_files": orphaned,
            "large_comment_blocks": large_comment_blocks,
            "statistics": {
                "files_analyzed": len([a for a in self.file_analyses.values() if "error" not in a]),
                "files_with_errors": len([a for a in self.file_analyses.values() if "error" in a]),
                "unused_imports_count": len(unused_imports),
                "dead_functions_count": len(dead_functions),
                "duplicate_functions_count": len(duplicates),
                "orphaned_files_count": len(orphaned),
                "large_comment_blocks_count": len(large_comment_blocks)
            }
        }
        
        return results

def main():
    analyzer = FocusedCleanupAnalyzer("/home/jianjun/ats-genai-admin")
    results = analyzer.run_analysis()
    
    # Save detailed results
    with open("focused_cleanup_analysis.json", "w") as f:
        json.dump(results, f, indent=2, default=str)
    
    # Print summary
    stats = results["statistics"]
    print(f"\n=== FOCUSED CLEANUP ANALYSIS RESULTS ===")
    print(f"Files analyzed: {stats['files_analyzed']}")
    print(f"Files with errors: {stats['files_with_errors']}")
    print(f"Unused imports: {stats['unused_imports_count']}")
    print(f"Dead functions: {stats['dead_functions_count']}")
    print(f"Duplicate functions: {stats['duplicate_functions_count']}")
    print(f"Orphaned files: {stats['orphaned_files_count']}")
    print(f"Large comment blocks: {stats['large_comment_blocks_count']}")
    
    # Show top cleanup opportunities
    print(f"\n=== TOP CLEANUP OPPORTUNITIES ===")
    
    if results["unused_imports"]:
        print(f"\nTop unused imports (showing first 10):")
        for i, item in enumerate(results["unused_imports"][:10]):
            file_name = Path(item["file"]).name
            print(f"  {i+1}. {file_name}:{item['import']['line']} - {item['import']['local_name']}")
    
    if results["dead_functions"]:
        print(f"\nTop dead functions (showing first 10):")
        for i, item in enumerate(results["dead_functions"][:10]):
            file_name = Path(item["file"]).name
            func = item["function"]
            print(f"  {i+1}. {file_name}:{func['line']} - {func['name']}()")
    
    if results["orphaned_files"]:
        print(f"\nOrphaned files (showing first 10):")
        for i, item in enumerate(results["orphaned_files"][:10]):
            file_name = Path(item["file"]).name
            print(f"  {i+1}. {file_name}")
    
    print(f"\nDetailed results saved to focused_cleanup_analysis.json")

if __name__ == "__main__":
    main()