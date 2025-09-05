#!/usr/bin/env python3

import os
import sys

print("=== ArrayRecord Package File Structure ===")

try:
    import array_record
    package_path = array_record.__path__[0]
    print(f"Package path: {package_path}")
    
    # List all files in the package
    for root, dirs, files in os.walk(package_path):
        level = root.replace(package_path, '').count(os.sep)
        indent = ' ' * 2 * level
        print(f"{indent}{os.path.basename(root)}/")
        subindent = ' ' * 2 * (level + 1)
        for file in files:
            print(f"{subindent}{file}")
            
    # Try to read __init__.py files to understand the structure
    init_file = os.path.join(package_path, '__init__.py')
    if os.path.exists(init_file):
        print(f"\n=== Main __init__.py content ===")
        with open(init_file, 'r') as f:
            content = f.read()
            print(content[:1000] + "..." if len(content) > 1000 else content)
            
    # Check python submodule __init__.py
    python_init = os.path.join(package_path, 'python', '__init__.py')
    if os.path.exists(python_init):
        print(f"\n=== Python submodule __init__.py content ===")
        with open(python_init, 'r') as f:
            content = f.read()
            print(content[:1000] + "..." if len(content) > 1000 else content)
            
    # Check for any .py files that might contain ArrayRecordWriter
    print(f"\n=== Searching for ArrayRecordWriter in Python files ===")
    for root, dirs, files in os.walk(package_path):
        for file in files:
            if file.endswith('.py'):
                file_path = os.path.join(root, file)
                try:
                    with open(file_path, 'r') as f:
                        content = f.read()
                        if 'ArrayRecordWriter' in content or 'RecordWriter' in content:
                            print(f"Found references in {file_path}")
                            lines = content.split('\n')
                            for i, line in enumerate(lines):
                                if 'ArrayRecordWriter' in line or 'RecordWriter' in line:
                                    print(f"  Line {i+1}: {line.strip()}")
                except:
                    pass
                    
except Exception as e:
    print(f"Error: {e}")

print("\n=== Binary/C Extension Check ===")
try:
    import array_record
    package_path = array_record.__path__[0]
    
    # Look for .so files (compiled extensions)
    for root, dirs, files in os.walk(package_path):
        for file in files:
            if file.endswith('.so') or file.endswith('.pyd'):
                print(f"Found binary extension: {os.path.join(root, file)}")
                
                # Try to load the extension and see what it provides
                try:
                    import importlib.util
                    file_path = os.path.join(root, file)
                    module_name = file.replace('.so', '').replace('.pyd', '')
                    
                    spec = importlib.util.spec_from_file_location(module_name, file_path)
                    if spec and spec.loader:
                        module = importlib.util.module_from_spec(spec)
                        spec.loader.exec_module(module)
                        attrs = [attr for attr in dir(module) if not attr.startswith('_')]
                        print(f"  Module {module_name} attributes: {attrs}")
                        
                        # Look for writer classes
                        writer_attrs = [attr for attr in attrs if 'writer' in attr.lower() or 'Writer' in attr]
                        if writer_attrs:
                            print(f"  Writer classes: {writer_attrs}")
                            
                except Exception as e:
                    print(f"  Error loading {file}: {e}")
                    
except Exception as e:
    print(f"Error: {e}")

print("\n=== Direct Module Import Attempts ===")

# Try various import patterns that might work
import_attempts = [
    'array_record.python.array_record_module',
    'array_record.python.array_record_writer', 
    'array_record.ArrayRecordModule',
    'array_record.array_record_module'
]

for import_path in import_attempts:
    try:
        module = __import__(import_path, fromlist=[''])
        attrs = [attr for attr in dir(module) if not attr.startswith('_')]
        print(f"✅ {import_path} imported successfully")
        print(f"   Attributes: {attrs}")
        
        # Look for writer classes
        writer_classes = [attr for attr in attrs if 'writer' in attr.lower() or 'Writer' in attr]
        if writer_classes:
            print(f"   Writer classes: {writer_classes}")
            for cls_name in writer_classes:
                cls = getattr(module, cls_name)
                print(f"     {cls_name}: {cls}")
                print(f"     Methods: {[m for m in dir(cls) if not m.startswith('_')]}")
    except ImportError as e:
        print(f"❌ {import_path}: {e}")
    except Exception as e:
        print(f"❌ {import_path} error: {e}")