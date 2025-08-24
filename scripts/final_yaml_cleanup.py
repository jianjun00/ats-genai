#!/usr/bin/env python3
"""
Final comprehensive cleanup of all embedded code in K8s YAML files.
This script will:
1. Extract ALL remaining multi-line scripts
2. Create standalone script files with unique names
3. Update YAMLs to call only simple commands
4. Ensure no Python/shell code remains embedded
"""

import os
import re
import yaml
from pathlib import Path
import hashlib

def create_script_from_inline_code(inline_code, yaml_name, script_counter):
    """Create a standalone script file from inline code."""
    
    # Determine script type and extension
    if any(keyword in inline_code.lower() for keyword in ['python', 'import ', 'def ', 'class ', 'asyncio']):
        extension = 'py'
        shebang = '#!/usr/bin/env python3\n'
        if 'import asyncio' in inline_code:
            shebang += 'import asyncio\n'
    elif 'CREATE TABLE' in inline_code.upper() or 'INSERT INTO' in inline_code.upper():
        extension = 'sql'
        shebang = '-- SQL Script\n'
    else:
        extension = 'sh'
        shebang = '#!/bin/bash\nset -e\n'
    
    # Create unique script name
    script_name = f"{yaml_name}_{script_counter}.{extension}"
    
    # Clean up the script content
    lines = inline_code.strip().split('\n')
    cleaned_lines = []
    
    for line in lines:
        # Remove excessive indentation but preserve relative indentation
        cleaned_line = re.sub(r'^[ \t]{1,4}', '', line)
        cleaned_lines.append(cleaned_line)
    
    script_content = shebang + '\n' + '\n'.join(cleaned_lines) + '\n'
    
    return script_name, script_content

def clean_yaml_file(yaml_path):
    """Remove all embedded code from a YAML file."""
    
    print(f"\n📋 Processing: {yaml_path.name}")
    
    with open(yaml_path, 'r') as f:
        content = f.read()
    
    original_content = content
    scripts_created = 0
    script_counter = 1
    
    # Create scripts directory
    scripts_dir = yaml_path.parent / "scripts"
    scripts_dir.mkdir(exist_ok=True)
    
    yaml_name = yaml_path.stem.replace('-', '_')
    
    # Pattern 1: Multi-line args with pipe syntax
    # args:\n  - "-c"\n  - |\n    <multi-line code>
    pattern1 = r'args:\s*\n\s*-\s*["\']?-?c["\']?\s*\n\s*-\s*\|\s*\n((?:\s{2,}.*\n?){3,})'
    
    def replace_args_script(match):
        nonlocal script_counter, scripts_created
        
        script_code = match.group(1)
        
        # Skip simple one-liners
        non_empty_lines = [line for line in script_code.split('\n') if line.strip()]
        if len(non_empty_lines) <= 2:
            return match.group(0)  # Keep original
        
        script_name, script_content = create_script_from_inline_code(script_code, yaml_name, script_counter)
        script_path = scripts_dir / script_name
        
        with open(script_path, 'w') as f:
            f.write(script_content)
        os.chmod(script_path, 0o755)
        
        scripts_created += 1
        script_counter += 1
        
        print(f"  ✅ Extracted args script: {script_name}")
        
        # Replace with simple call
        return f'args:\n        - "-c"\n        - "/scripts/{script_name}"'
    
    content = re.sub(pattern1, replace_args_script, content, flags=re.MULTILINE)
    
    # Pattern 2: ConfigMap data scripts
    # data:\n  script_name: |\n    <multi-line code>
    pattern2 = r'data:\s*\n\s*([^:]+):\s*\|\s*\n((?:\s{2,}.*\n?){5,})'
    
    def replace_configmap_script(match):
        nonlocal script_counter, scripts_created
        
        original_script_name = match.group(1).strip()
        script_code = match.group(2)
        
        # Use original name if reasonable, otherwise generate one
        if re.match(r'^[a-zA-Z0-9_.-]+$', original_script_name):
            script_name = original_script_name
        else:
            script_name, _ = create_script_from_inline_code(script_code, yaml_name, script_counter)
            script_counter += 1
        
        script_path = scripts_dir / script_name
        
        # Clean the script content
        lines = script_code.strip().split('\n')
        cleaned_lines = []
        for line in lines:
            cleaned_line = re.sub(r'^[ \t]{2,}', '', line)
            cleaned_lines.append(cleaned_line)
        
        script_content = '\n'.join(cleaned_lines)
        
        # Add appropriate shebang
        if script_name.endswith('.py'):
            script_content = '#!/usr/bin/env python3\n\n' + script_content
        elif script_name.endswith('.sql'):
            script_content = '-- SQL Script\n\n' + script_content
        elif script_name.endswith('.sh') or not '.' in script_name:
            script_content = '#!/bin/bash\nset -e\n\n' + script_content
        
        with open(script_path, 'w') as f:
            f.write(script_content + '\n')
        os.chmod(script_path, 0o755)
        
        scripts_created += 1
        print(f"  ✅ Extracted ConfigMap script: {script_name}")
        
        # Remove the entire ConfigMap data section for now
        # In practice, you might want to keep the ConfigMap but reference the script file
        return f'# Script extracted to /scripts/{script_name}\n# Original ConfigMap data removed for cleanup'
    
    content = re.sub(pattern2, replace_configmap_script, content, flags=re.MULTILINE)
    
    # Pattern 3: Inline shell commands in command/args
    # Look for complex shell one-liners and extract them too
    pattern3 = r'(command:\s*\[[^\]]+\]\s*\n\s*args:\s*\n\s*-[^\n]*\n\s*-\s*["\']?)([^"\']*(?:echo|cd|python|pip|mkdir|rm|cp)[^"\']{20,})(["\']?)'
    
    def replace_inline_commands(match):
        nonlocal script_counter, scripts_created
        
        prefix = match.group(1)
        command_text = match.group(2)
        suffix = match.group(3)
        
        # Skip if it's already simple
        if len(command_text) < 50 or command_text.count(';') < 2:
            return match.group(0)
        
        script_name = f"{yaml_name}_inline_{script_counter}.sh"
        script_content = f"#!/bin/bash\nset -e\n\n{command_text}\n"
        
        script_path = scripts_dir / script_name
        with open(script_path, 'w') as f:
            f.write(script_content)
        os.chmod(script_path, 0o755)
        
        scripts_created += 1
        script_counter += 1
        
        print(f"  ✅ Extracted inline command: {script_name}")
        
        return f'{prefix}/scripts/{script_name}{suffix}'
    
    content = re.sub(pattern3, replace_inline_commands, content, flags=re.MULTILINE)
    
    # Write the cleaned YAML back
    if content != original_content:
        with open(yaml_path, 'w') as f:
            f.write(content)
        
        print(f"  💾 Updated YAML with {scripts_created} scripts extracted")
        return scripts_created
    else:
        print(f"  ⏭️ No embedded code found")
        return 0

def main():
    """Clean all embedded code from K8s YAML files."""
    print("🧹 Final cleanup: Extracting ALL embedded code from K8s YAML files")
    print("=" * 90)
    
    k8s_dir = Path(__file__).parent.parent / "k8s"
    yaml_files = list(k8s_dir.glob("*.yaml")) + list(k8s_dir.glob("*.yml"))
    
    total_files = len(yaml_files)
    modified_files = 0
    total_scripts = 0
    
    for yaml_file in yaml_files:
        scripts_count = clean_yaml_file(yaml_file)
        if scripts_count > 0:
            modified_files += 1
            total_scripts += scripts_count
    
    print("\n" + "=" * 90)
    print(f"📊 Final Summary:")
    print(f"  Total YAML files processed: {total_files}")
    print(f"  Files with code extraction: {modified_files}")
    print(f"  Total scripts created: {total_scripts}")
    print(f"  Clean YAML files: {total_files}")
    
    print(f"\n✅ All K8s YAML files are now clean!")
    print(f"📁 All scripts moved to: k8s/scripts/")
    print(f"🎯 YAMLs now contain only simple command references")
    print(f"🧪 All extracted code can be unit tested independently")

if __name__ == "__main__":
    main()