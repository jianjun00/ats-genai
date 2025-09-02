#!/usr/bin/env python3
"""
Install Playwright in Docker environment
"""

import subprocess
import sys

def main():
    print("🎭 Installing Playwright in Docker environment...")
    
    # Install packages
    subprocess.run([sys.executable, "-m", "pip", "install", "playwright", "pytest-playwright"], check=True)
    print("✅ Playwright packages installed")
    
    # Install browser using Python module
    subprocess.run([sys.executable, "-m", "playwright", "install", "chromium"], check=True)
    print("✅ Chromium browser installed")
    
    print("🎉 Playwright setup complete!")

if __name__ == "__main__":
    main()