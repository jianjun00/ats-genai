#!/usr/bin/env python3
"""
Comprehensive import audit script to find broken module references
Following fail-fast principles - no exception masking
"""

import os
import sys
import ast
import importlib.util
from pathlib import Path
from typing import Dict, List, Tuple, Set

def find_python_files(root_dir: str) -> List[Path]:
    """Find all Python files in the codebase"""
    root_path = Path(root_dir)
    return list(root_path.rglob("*.py"))

def extract_imports_from_file(file_path: Path) -> List[Tuple[str, int, str]]:
    """
    Extract all import statements from a Python file
    Returns: List of (import_name, line_number, import_type)
    """
    imports = []
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    tree = ast.parse(content, filename=str(file_path))
    
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                imports.append((alias.name, node.lineno, 'import'))
        elif isinstance(node, ast.ImportFrom):
            module = node.module or ''
            for alias in node.names:
                full_import = f"{module}.{alias.name}" if module else alias.name
                imports.append((full_import, node.lineno, 'from'))
    return imports

def check_module_exists(module_name: str, src_root: Path) -> Tuple[bool, str]:
    """
    Check if a module exists either as a standard library module or in src/
    Returns: (exists, reason)
    """
    # Check if it's a standard library module
    spec = importlib.util.find_spec(module_name.split('.')[0])
    if spec is not None and spec.origin:
        # Standard library or installed package
        return True, "standard_library"
    module_parts = module_name.split('.')
    
    # Try different path combinations in src/
    possible_paths = [
        src_root / '/'.join(module_parts) / '__init__.py',
        src_root / '/'.join(module_parts[:-1]) / f"{module_parts[-1]}.py",
        src_root / f"{module_parts[0]}.py"
    ]
    
    if len(module_parts) > 1:
        # Try with src/ prefix
        possible_paths.extend([
            src_root / module_parts[0] / '/'.join(module_parts[1:]) / '__init__.py',
            src_root / module_parts[0] / '/'.join(module_parts[1:-1]) / f"{module_parts[-1]}.py"
        ])
    
    for path in possible_paths:
        if path.exists():
            return True, f"found_at_{path}"
    
    return False, "not_found"

def audit_imports() -> Dict[str, List[Tuple[str, int, str, str]]]:
    """
    Comprehensive audit of all imports in the codebase
    Returns: Dict mapping file paths to lists of broken imports
    """
    src_root = Path("src")
    if not src_root.exists():
        raise FileNotFoundError("src/ directory not found")
    
    python_files = find_python_files("src")
    broken_imports = {}
    total_imports = 0
    broken_count = 0
    
    print(f"Auditing imports in {len(python_files)} Python files...")
    
    for file_path in python_files:
        imports = extract_imports_from_file(file_path)
        file_broken_imports = []
        
        for import_name, line_no, import_type in imports:
            total_imports += 1
            
            # Skip relative imports for now
            if import_name.startswith('.'):
                continue
                
            # Skip common third-party packages
            if any(import_name.startswith(pkg) for pkg in [
                'pandas', 'numpy', 'asyncio', 'logging', 'datetime', 
                'typing', 'dataclasses', 'enum', 'json', 'os', 'sys',
                'pathlib', 'collections', 'functools', 'itertools',
                'pytest', 'asyncpg', 'fastapi', 'pydantic', 'uvicorn',
                'sqlalchemy', 'alembic', 'requests', 'httpx'
            ]):
                continue
            
            exists, reason = check_module_exists(import_name, src_root)
            if not exists:
                file_broken_imports.append((import_name, line_no, import_type, reason))
                broken_count += 1
        
        if file_broken_imports:
            broken_imports[str(file_path)] = file_broken_imports
    
    print(f"\nAudit Summary:")
    print(f"Total imports checked: {total_imports}")
    print(f"Broken imports found: {broken_count}")
    print(f"Files with broken imports: {len(broken_imports)}")
    
    return broken_imports

def main():
    """Run the comprehensive import audit"""
    broken_imports = audit_imports()
    
    if not broken_imports:
        print("✅ No broken imports found!")
        return
    
    print("\n🚨 BROKEN IMPORTS DETECTED:")
    print("=" * 80)
    
    for file_path, imports in broken_imports.items():
        print(f"\n📁 {file_path}")
        for import_name, line_no, import_type, reason in imports:
            print(f"  ❌ Line {line_no}: {import_type} {import_name} ({reason})")
    
    print(f"\n⚠️  Found {sum(len(imports) for imports in broken_imports.values())} broken imports across {len(broken_imports)} files")
    print("These need to be fixed to prevent runtime import errors.")
    
if __name__ == "__main__":
    main()