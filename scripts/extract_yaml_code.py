#!/usr/bin/env python3
"""
Extract all embedded code from K8s YAML files into standalone Python/shell scripts.
Clean up YAMLs to only contain simple command calls.
"""

import os
import re
import yaml
from pathlib import Path
import tempfile

def extract_embedded_code_from_yaml(yaml_path):
    """Extract embedded Python/shell code from a YAML file."""
    
    print(f"\n📋 Processing: {yaml_path.name}")
    
    with open(yaml_path, 'r') as f:
        content = f.read()
    
    # Look for multi-line scripts in args sections
    script_pattern = r'args:\s*\n\s*-\s*"-?c"\s*\n\s*-\s*\|\s*\n((?:\s{2,}.*\n?)*)'
    matches = re.finditer(script_pattern, content, re.MULTILINE)
    
    scripts_found = 0
    new_content = content
    
    for match in matches:
        script_content = match.group(1)
        
        # Skip if it's already a simple command (no multi-line logic)
        if script_content.strip().count('\n') < 2:
            continue
            
        scripts_found += 1
        
        # Extract the script name from the YAML filename
        base_name = yaml_path.stem.replace('-', '_')
        script_name = f"{base_name}_script_{scripts_found}.py"
        script_path = yaml_path.parent / "scripts" / script_name
        
        # Create scripts directory if it doesn't exist
        script_path.parent.mkdir(exist_ok=True)
        
        # Clean up the script content
        cleaned_script = []
        for line in script_content.split('\n'):
            # Remove YAML indentation
            clean_line = re.sub(r'^[ ]{2,}', '', line)
            if clean_line.strip():
                cleaned_script.append(clean_line)
        
        # Create the standalone script
        script_content_clean = '\n'.join(cleaned_script)
        
        # Add shebang if it's Python code
        if 'python' in script_content_clean or 'import' in script_content_clean:
            script_content_clean = '#!/usr/bin/env python3\n\n' + script_content_clean
        else:
            script_content_clean = '#!/bin/bash\nset -e\n\n' + script_content_clean
        
        # Write the script file
        with open(script_path, 'w') as f:
            f.write(script_content_clean)
        
        os.chmod(script_path, 0o755)  # Make executable
        
        # Replace the embedded script with a simple call
        simple_call = f'python /scripts/{script_name}' if 'python' in script_content_clean else f'/scripts/{script_name}'
        
        replacement = f'args:\n        - "-c"\n        - |\n          {simple_call}'
        new_content = new_content.replace(match.group(0), replacement)
        
        print(f"  ✅ Extracted script to: scripts/{script_name}")
    
    # Also look for ConfigMap embedded scripts
    configmap_pattern = r'data:\s*\n\s*([^:]+):\s*\|\s*\n((?:\s{2,}.*\n?)*)'
    configmap_matches = re.finditer(configmap_pattern, content, re.MULTILINE)
    
    for match in configmap_matches:
        script_name = match.group(1).strip()
        script_content = match.group(2)
        
        if len(script_content.strip()) < 100:  # Skip simple scripts
            continue
            
        scripts_found += 1
        script_path = yaml_path.parent / "scripts" / script_name
        script_path.parent.mkdir(exist_ok=True)
        
        # Clean up the script content
        cleaned_script = []
        for line in script_content.split('\n'):
            clean_line = re.sub(r'^[ ]{2,}', '', line)
            if clean_line.strip():
                cleaned_script.append(clean_line)
        
        script_content_clean = '\n'.join(cleaned_script)
        
        with open(script_path, 'w') as f:
            f.write(script_content_clean)
        
        os.chmod(script_path, 0o755)
        
        print(f"  ✅ Extracted ConfigMap script to: scripts/{script_name}")
    
    # Write the cleaned YAML back if we made changes
    if scripts_found > 0:
        with open(yaml_path, 'w') as f:
            f.write(new_content)
        print(f"  💾 Updated YAML file with {scripts_found} scripts extracted")
        return True
    else:
        print(f"  ⏭️ No complex embedded code found")
        return False

def main():
    """Extract embedded code from all K8s YAML files."""
    print("🧹 Extracting embedded code from K8s YAML files")
    print("=" * 80)
    
    k8s_dir = Path(__file__).parent.parent / "k8s"
    yaml_files = list(k8s_dir.glob("*.yaml")) + list(k8s_dir.glob("*.yml"))
    
    total_files = len(yaml_files)
    modified_files = 0
    
    for yaml_file in yaml_files:
        if extract_embedded_code_from_yaml(yaml_file):
            modified_files += 1
    
    print("\n" + "=" * 80)
    print(f"📊 Summary:")
    print(f"  Total files processed: {total_files}")
    print(f"  Files with extracted code: {modified_files}")
    print(f"  Files unchanged: {total_files - modified_files}")
    
    if modified_files > 0:
        print(f"\n✅ Successfully extracted embedded code from {modified_files} YAML files!")
        print(f"📁 Scripts created in: k8s/scripts/")
        print("🧹 YAML files now contain only simple command calls")
    else:
        print("\n✅ All YAML files already clean!")

if __name__ == "__main__":
    main()