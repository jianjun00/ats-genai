#!/usr/bin/env python3
"""
Generate Protocol Buffer Python bindings for training schema definitions.
"""

import subprocess
import sys
import os
from pathlib import Path

def main():
    """Generate protobuf Python files."""
    
    # Check if protobuf compiler is available
    try:
        result = subprocess.run(['protoc', '--version'], capture_output=True, text=True)
        print(f"✅ Found protoc: {result.stdout.strip()}")
    except FileNotFoundError:
        print("❌ protoc not found. Installing...")
        # Try to install protobuf compiler
        subprocess.run(['apt', 'update'], check=True)
        subprocess.run(['apt', 'install', '-y', 'protobuf-compiler'], check=True)
    
    # Create output directory
    proto_dir = Path('/workspace/proto')
    output_dir = Path('/workspace/src/schema/proto')
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Generate Python code from proto file
    proto_file = proto_dir / 'training_feature_schema.proto'
    
    if not proto_file.exists():
        print(f"❌ Proto file not found: {proto_file}")
        return 1
    
    try:
        cmd = [
            'protoc',
            f'--python_out={output_dir}',
            f'--proto_path={proto_dir}',
            str(proto_file)
        ]
        
        print(f"🔧 Running: {' '.join(cmd)}")
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        
        print("✅ Successfully generated Python protobuf bindings")
        
        # Create __init__.py file
        init_file = output_dir / '__init__.py'
        init_file.write_text('# Generated protobuf schemas\n')
        
        # List generated files
        generated_files = list(output_dir.glob('*.py'))
        print(f"📁 Generated files:")
        for file in generated_files:
            print(f"   - {file}")
            
        return 0
        
    except subprocess.CalledProcessError as e:
        print(f"❌ Error generating protobuf code: {e}")
        print(f"stdout: {e.stdout}")
        print(f"stderr: {e.stderr}")
        return 1

if __name__ == '__main__':
    sys.exit(main())