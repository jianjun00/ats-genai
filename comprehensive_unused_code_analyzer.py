#!/usr/bin/env python3
"""
Comprehensive Unused Code Analyzer for ATS GenAI Admin
Scans for unused imports, dead code, duplicate functions, and orphaned files.
"""

import ast
import os
import re
import json
import hashlib
from collections import defaultdict, Counter
from pathlib import Path
from typing import Dict, List, Set, Tuple, Any

class UnusedCodeAnalyzer:
    def __init__(self, root_dir: str):
        self.root_dir = Path(root_dir)
        # Focus on source code, exclude .venv, node_modules, etc.
        self.python_files = []
        for py_file in self.root_dir.rglob("*.py"):
            path_str = str(py_file)
            if not any(exclude in path_str for exclude in ['.venv', 'node_modules', '__pycache__', '.git']):
                self.python_files.append(py_file)
        self.analysis_results = {
            "unused_imports": [],
            "dead_functions": [],
            "dead_classes": [],
            "duplicate_functions": [],
            "orphaned_files": [],
            "large_commented_blocks": [],
            "unused_variables": [],
            "files_with_issues": defaultdict(list),
            "statistics": {},
        }
        
        # Track all definitions and their usage
        self.all_definitions = defaultdict(list)  # name -> [(file, line, type)]
        self.all_usages = defaultdict(list)       # name -> [(file, line)]
        self.imports_per_file = defaultdict(list) # file -> [import_info]
        self.function_signatures = defaultdict(list)  # signature_hash -> [(file, name, line)]
        
    def analyze_file(self, file_path: Path) -> Dict[str, Any]:
        """Analyze a single Python file for various code issues."""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
                
            tree = ast.parse(content, filename=str(file_path))
            
            file_analysis = {
                "imports": [],
                "functions": [],
                "classes": [],
                "variables": [],
                "usages": [],
                "commented_lines": 0,
                "total_lines": len(content.splitlines()),
            }
            
            # Analyze AST
            self._analyze_ast_node(tree, file_path, file_analysis)
            
            # Find large commented blocks
            file_analysis["large_commented_blocks"] = self._find_large_commented_blocks(content)
            
            return file_analysis
            
        except Exception as e:
            print(f"Error analyzing {file_path}: {e}")
            return {"error": str(e)}
    
    def _analyze_ast_node(self, node: ast.AST, file_path: Path, file_analysis: Dict, scope_vars: Set = None):
        """Recursively analyze AST nodes."""
        if scope_vars is None:
            scope_vars = set()
            
        for child in ast.walk(node):
            # Track imports
            if isinstance(child, (ast.Import, ast.ImportFrom)):
                self._track_import(child, file_path, file_analysis)
                
            # Track function definitions
            elif isinstance(child, ast.FunctionDef):
                self._track_function(child, file_path, file_analysis)
                
            # Track class definitions
            elif isinstance(child, ast.ClassDef):
                self._track_class(child, file_path, file_analysis)
                
            # Track variable assignments
            elif isinstance(child, ast.Assign):
                self._track_assignment(child, file_path, file_analysis, scope_vars)
                
            # Track name usage
            elif isinstance(child, ast.Name):
                self._track_name_usage(child, file_path, file_analysis)
    
    def _track_import(self, node: ast.AST, file_path: Path, file_analysis: Dict):
        """Track import statements."""
        if isinstance(node, ast.Import):
            for alias in node.names:
                import_info = {
                    "name": alias.name,
                    "alias": alias.asname,
                    "line": node.lineno,
                    "type": "import",
                }
                file_analysis["imports"].append(import_info)
                self.imports_per_file[str(file_path)].append(import_info)
                
        elif isinstance(node, ast.ImportFrom):
            module = node.module or ""
            for alias in node.names:
                import_info = {
                    "module": module,
                    "name": alias.name,
                    "alias": alias.asname,
                    "line": node.lineno,
                    "type": "from_import",
                }
                file_analysis["imports"].append(import_info)
                self.imports_per_file[str(file_path)].append(import_info)
    
    def _track_function(self, node: ast.FunctionDef, file_path: Path, file_analysis: Dict):
        """Track function definitions."""
        func_info = {
            "name": node.name,
            "line": node.lineno,
            "args": [arg.arg for arg in node.args.args],
            "decorators": [self._get_decorator_name(d) for d in node.decorator_list],
        }
        file_analysis["functions"].append(func_info)
        
        # Track in global definitions
        self.all_definitions[node.name].append((str(file_path), node.lineno, "function"))
        
        # Create function signature hash for duplicate detection
        signature = f"{node.name}({','.join(func_info['args'])})"
        sig_hash = hashlib.md5(signature.encode()).hexdigest()
        self.function_signatures[sig_hash].append((str(file_path), node.name, node.lineno))
    
    def _track_class(self, node: ast.ClassDef, file_path: Path, file_analysis: Dict):
        """Track class definitions."""
        class_info = {
            "name": node.name,
            "line": node.lineno,
            "bases": [self._get_name_from_node(base) for base in node.bases],
            "decorators": [self._get_decorator_name(d) for d in node.decorator_list],
        }
        file_analysis["classes"].append(class_info)
        
        # Track in global definitions
        self.all_definitions[node.name].append((str(file_path), node.lineno, "class"))
    
    def _track_assignment(self, node: ast.Assign, file_path: Path, file_analysis: Dict, scope_vars: Set):
        """Track variable assignments."""
        for target in node.targets:
            if isinstance(target, ast.Name):
                var_info = {
                    "name": target.id,
                    "line": node.lineno,
                }
                file_analysis["variables"].append(var_info)
                scope_vars.add(target.id)
                self.all_definitions[target.id].append((str(file_path), node.lineno, "variable"))
    
    def _track_name_usage(self, node: ast.Name, file_path: Path, file_analysis: Dict):
        """Track name usage (function calls, variable references)."""
        if isinstance(node.ctx, (ast.Load, ast.Del)):
            usage_info = {
                "name": node.id,
                "line": node.lineno,
            }
            file_analysis["usages"].append(usage_info)
            self.all_usages[node.id].append((str(file_path), node.lineno))
    
    def _get_decorator_name(self, decorator: ast.AST) -> str:
        """Get decorator name from AST node."""
        if isinstance(decorator, ast.Name):
            return decorator.id
        elif isinstance(decorator, ast.Attribute):
            return f"{self._get_name_from_node(decorator.value)}.{decorator.attr}"
        return str(decorator)
    
    def _get_name_from_node(self, node: ast.AST) -> str:
        """Get name from AST node."""
        if isinstance(node, ast.Name):
            return node.id
        elif isinstance(node, ast.Attribute):
            return f"{self._get_name_from_node(node.value)}.{node.attr}"
        return str(node)
    
    def _find_large_commented_blocks(self, content: str) -> List[Dict]:
        """Find large blocks of commented code."""
        lines = content.splitlines()
        commented_blocks = []
        current_block = []
        
        for i, line in enumerate(lines, 1):
            stripped = line.strip()
            if stripped.startswith('#') and len(stripped) > 10:  # Substantial comment
                current_block.append((i, line))
            else:
                if len(current_block) >= 5:  # Block of 5+ commented lines
                    commented_blocks.append({
                        "start_line": current_block[0][0],
                        "end_line": current_block[-1][0],
                        "line_count": len(current_block),
                        "content_preview": current_block[0][1][:50] + "..."
                    })
                current_block = []
        
        # Check final block
        if len(current_block) >= 5:
            commented_blocks.append({
                "start_line": current_block[0][0],
                "end_line": current_block[-1][0],
                "line_count": len(current_block),
                "content_preview": current_block[0][1][:50] + "..."
            })
            
        return commented_blocks
    
    def find_unused_imports(self) -> List[Dict]:
        """Find imports that are never used."""
        unused_imports = []
        
        for file_path, imports in self.imports_per_file.items():
            file_usages = set()
            if file_path in self.file_analyses:
                file_usages = {usage["name"] for usage in self.file_analyses[file_path].get("usages", [])}
            
            for import_info in imports:
                import_name = import_info.get("alias") or import_info["name"]
                
                # Handle from imports differently
                if import_info["type"] == "from_import":
                    # Check if the imported name is used
                    if import_name not in file_usages and import_name != "*":
                        unused_imports.append({
                            "file": file_path,
                            "import": import_info,
                            "reason": "imported but never used"
                        })
                else:
                    # Regular import - check if module name is used
                    module_name = import_name.split('.')[0]
                    if module_name not in file_usages:
                        unused_imports.append({
                            "file": file_path,
                            "import": import_info,
                            "reason": "imported but never used"
                        })
        
        return unused_imports
    
    def find_dead_functions(self) -> List[Dict]:
        """Find functions that are defined but never called."""
        dead_functions = []
        
        for func_name, definitions in self.all_definitions.items():
            func_definitions = [d for d in definitions if d[2] == "function"]
            if not func_definitions:
                continue
                
            usages = self.all_usages.get(func_name, [])
            
            for file_path, line, _ in func_definitions:
                # Check if function is used anywhere outside its definition file
                external_usages = [u for u in usages if u[0] != file_path]
                is_used = len(external_usages) > 0 or len(usages) > 1
                
                # Skip special methods and common patterns
                if (not is_used and 
                    not func_name.startswith('__') and 
                    not func_name.startswith('test_') and
                    func_name not in ['main', 'run', 'setup', 'teardown']):
                    
                    dead_functions.append({
                        "name": func_name,
                        "file": file_path,
                        "line": line,
                        "reason": "defined but never called"
                    })
        
        return dead_functions
    
    def find_dead_classes(self) -> List[Dict]:
        """Find classes that are defined but never instantiated."""
        dead_classes = []
        
        for class_name, definitions in self.all_definitions.items():
            class_definitions = [d for d in definitions if d[2] == "class"]
            if not class_definitions:
                continue
                
            usages = self.all_usages.get(class_name, [])
            
            for file_path, line, _ in class_definitions:
                # Check if class is used anywhere (not just defined)
                external_usages = [u for u in usages if u[0] != file_path]
                
                if not external_usages and not class_name.endswith('Test'):
                    dead_classes.append({
                        "name": class_name,
                        "file": file_path,
                        "line": line,
                        "reason": "defined but never instantiated outside of definition file"
                    })
        
        return dead_classes
    
    def find_duplicate_functions(self) -> List[Dict]:
        """Find functions with identical signatures."""
        duplicates = []
        
        for sig_hash, occurrences in self.function_signatures.items():
            if len(occurrences) > 1:
                duplicates.append({
                    "signature_hash": sig_hash,
                    "occurrences": occurrences,
                    "count": len(occurrences)
                })
        
        return duplicates
    
    def find_orphaned_files(self) -> List[Dict]:
        """Find Python files that are never imported."""
        orphaned = []
        
        # Get all module names from file paths
        all_modules = set()
        for py_file in self.python_files:
            relative_path = py_file.relative_to(self.root_dir)
            module_path = str(relative_path).replace('/', '.').replace('\\', '.').replace('.py', '')
            all_modules.add(module_path)
            
            # Also add just the filename
            all_modules.add(py_file.stem)
        
        # Check which modules are imported
        imported_modules = set()
        for imports in self.imports_per_file.values():
            for imp in imports:
                if imp["type"] == "from_import":
                    imported_modules.add(imp["module"])
                else:
                    imported_modules.add(imp["name"])
        
        # Find modules that are never imported
        for module in all_modules:
            if (module not in imported_modules and 
                not module.endswith('__init__') and
                not module.startswith('test_') and
                module not in ['main', 'conftest', 'setup']):
                
                # Find corresponding file
                for py_file in self.python_files:
                    if py_file.stem == module.split('.')[-1]:
                        orphaned.append({
                            "file": str(py_file),
                            "module": module,
                            "reason": "file never imported"
                        })
                        break
        
        return orphaned
    
    def _is_function_call_usage(self, usage: Tuple, file_path: str) -> bool:
        """Check if usage is a function call (simplified heuristic)."""
        # This is a simplified check - in practice, we'd need more sophisticated AST analysis
        return True
    
    def run_analysis(self):
        """Run the complete analysis."""
        print(f"Analyzing {len(self.python_files)} Python files...")
        
        # Analyze each file
        self.file_analyses = {}
        for i, py_file in enumerate(self.python_files):
            if i % 100 == 0:
                print(f"Processed {i}/{len(self.python_files)} files...")
            
            analysis = self.analyze_file(py_file)
            self.file_analyses[str(py_file)] = analysis
        
        print("Running cross-file analysis...")
        
        # Cross-file analysis
        self.analysis_results["unused_imports"] = self.find_unused_imports()
        self.analysis_results["dead_functions"] = self.find_dead_functions()
        self.analysis_results["dead_classes"] = self.find_dead_classes()
        self.analysis_results["duplicate_functions"] = self.find_duplicate_functions()
        self.analysis_results["orphaned_files"] = self.find_orphaned_files()
        
        # Collect large commented blocks
        for file_path, analysis in self.file_analyses.items():
            if analysis.get("large_commented_blocks"):
                for block in analysis["large_commented_blocks"]:
                    block["file"] = file_path
                    self.analysis_results["large_commented_blocks"].append(block)
        
        # Generate statistics
        self.analysis_results["statistics"] = {
            "total_files": len(self.python_files),
            "files_analyzed": len(self.file_analyses),
            "unused_imports_count": len(self.analysis_results["unused_imports"]),
            "dead_functions_count": len(self.analysis_results["dead_functions"]),
            "dead_classes_count": len(self.analysis_results["dead_classes"]),
            "duplicate_functions_count": len(self.analysis_results["duplicate_functions"]),
            "orphaned_files_count": len(self.analysis_results["orphaned_files"]),
            "large_commented_blocks_count": len(self.analysis_results["large_commented_blocks"]),
        }
        
        print("Analysis complete!")
        return self.analysis_results
    
    def save_results(self, output_file: str):
        """Save analysis results to JSON file."""
        with open(output_file, 'w') as f:
            json.dump(self.analysis_results, f, indent=2, default=str)
        print(f"Results saved to {output_file}")

if __name__ == "__main__":
    analyzer = UnusedCodeAnalyzer("/home/jianjun/ats-genai-admin")
    results = analyzer.run_analysis()
    analyzer.save_results("comprehensive_unused_code_analysis.json")
    
    # Print summary
    stats = results["statistics"]
    print(f"\n=== ANALYSIS SUMMARY ===")
    print(f"Files analyzed: {stats['files_analyzed']}")
    print(f"Unused imports: {stats['unused_imports_count']}")
    print(f"Dead functions: {stats['dead_functions_count']}")
    print(f"Dead classes: {stats['dead_classes_count']}")
    print(f"Duplicate functions: {stats['duplicate_functions_count']}")
    print(f"Orphaned files: {stats['orphaned_files_count']}")
    print(f"Large commented blocks: {stats['large_commented_blocks_count']}")