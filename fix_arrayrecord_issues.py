#!/usr/bin/env python3
"""
Fix the specific issues identified by comprehensive testing.
"""
import subprocess

def fix_file_discovery():
    """Fix file discovery - test the actual find command that's failing."""
    print("🔧 Testing file discovery...")

    # Test the exact find command that failed
    result = subprocess.run([
        "docker", "exec", "ats-dev-analytics", "find", "/data/training",
        "-type", "f", "\\(", "-name", "*.arrayrecord", "-o", "-name", "*.riegeli", "\\)",
        "-exec", "basename", "{}", "\\;"
    ], capture_output=True, text=True, timeout=10)

    print(f"Find command result: {result.returncode}")
    print(f"Output: {result.stdout}")
    print(f"Error: {result.stderr}")

    # Try simpler find command
    result2 = subprocess.run([
        "docker", "exec", "ats-dev-analytics", "find", "/data/training",
        "-name", "*.arrayrecord"
    ], capture_output=True, text=True, timeout=10)

    print(f"\nSimpler find result: {result2.returncode}")
    print(f"Output: {result2.stdout}")

    return result2.returncode == 0

def test_arrayrecord_reader():
    """Test correct ArrayRecord reading syntax."""
    print("\n🔧 Testing ArrayRecord reading...")

    # Find a file to test with
    result = subprocess.run([
        "docker", "exec", "ats-dev-analytics", "find", "/data/training",
        "-name", "*.arrayrecord", "-type", "f"
    ], capture_output=True, text=True, timeout=10)

    files = result.stdout.strip().split('\n') if result.stdout.strip() else []
    files = [f for f in files if f]

    if not files:
        print("❌ No ArrayRecord files found for testing")
        return False

    test_file = files[0]
    print(f"Testing with file: {test_file}")

    # Test correct syntax (without context manager)
    read_script = f'''
import json
from array_record.python.array_record_module import ArrayRecordReader

try:
    reader = ArrayRecordReader("{test_file}")
    records = list(reader)
    print(f"SUCCESS: Read {{len(records)}} records")
    if records:
        first_record = json.loads(records[0].decode())
        print(f"SAMPLE_KEYS: {{list(first_record.keys())}}")
        print(f"SAMPLE_DATA: open={{first_record.get('open')}}, high={{first_record.get('high')}}")
except Exception as e:
    print(f"ERROR: {{e}}")
    import traceback
    traceback.print_exc()
'''

    result = subprocess.run([
        "docker", "exec", "ats-dev-analytics", "python3", "-c", read_script
    ], capture_output=True, text=True, timeout=15)

    print(f"Reading test result: {result.returncode}")
    print(f"Output: {result.stdout}")
    if result.stderr:
        print(f"Error: {result.stderr}")

    return "SUCCESS" in result.stdout

def main():
    print("🚀 Fixing ArrayRecord Issues Systematically")
    print("=" * 50)

    # Fix 1: File discovery
    discovery_ok = fix_file_discovery()

    # Fix 2: ArrayRecord reading
    reading_ok = test_arrayrecord_reader()

    print(f"\n📊 Fix Results:")
    print(f"File Discovery: {'✅ OK' if discovery_ok else '❌ FAILED'}")
    print(f"ArrayRecord Reading: {'✅ OK' if reading_ok else '❌ FAILED'}")

    if discovery_ok and reading_ok:
        print("\n🎉 Both issues resolved! Now updating analytics service...")
        return True
    else:
        print("\n⚠️  Issues remain - need to investigate further")
        return False

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)