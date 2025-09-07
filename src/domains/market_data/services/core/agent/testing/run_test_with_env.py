#!/usr/bin/env python3
"""
Helper script to run the database connection test with environment variables loaded from .env.dev
"""

import os
import sys
import subprocess
from pathlib import Path

def load_env_file(env_file_path):
    """Load environment variables from a .env file"""
    if not os.path.exists(env_file_path):
        print(f"Error: Environment file {env_file_path} not found")
        return False
    
    print(f"Loading environment variables from {env_file_path}")
    with open(env_file_path, 'r') as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('#'):
                continue
                
            key, value = line.split('=', 1)
            os.environ[key] = value
            print(f"Set {key}={value if 'PASSWORD' not in key else '********'}")
    
    return True

def main():
    """Main function to run the test with environment variables"""
    # Determine the path to the .env.dev file
    script_dir = Path(__file__).parent
    env_file = script_dir / ".env.dev"
    
    # Load environment variables
    if not load_env_file(env_file):
        return 1
    
    # Run the test script
    test_script = script_dir / "test_db_connection.py"
    print(f"\nRunning test script: {test_script}\n")
    
    # Print current environment variables for debugging
    print("Current environment variables:")
    for key in ['DB_HOST', 'DB_PORT', 'DB_NAME', 'DB_USER', 'DATABASE_URL', 'ENVIRONMENT']:
        if key in os.environ:
            value = os.environ[key]
            if 'PASSWORD' in key:
                value = '********'
            print(f"{key}={value}")
    
    print("\n--- Test Output ---\n")
    
    # Execute the test script with the environment variables
    result = subprocess.run([sys.executable, str(test_script)], env=os.environ)
    return result.returncode

if __name__ == "__main__":
    sys.exit(main())
