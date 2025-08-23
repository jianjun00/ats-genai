#!/usr/bin/env python3
"""
JIRA Connection Test Script

This script tests your JIRA credentials and connection.
Run this after setting up your .env file to verify everything works.

Usage:
    python scripts/test_jira_connection.py
"""

import os
import sys
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def test_imports():
    """Test if required packages are installed."""
    try:
        from jira import JIRA
        print("✅ JIRA package imported successfully")
        return True
    except ImportError:
        print("❌ Missing JIRA package. Install with:")
        print("   pip install jira python-dotenv")
        return False

def test_env_variables():
    """Test if environment variables are set."""
    required_vars = ['JIRA_SERVER', 'JIRA_EMAIL', 'JIRA_API_TOKEN', 'JIRA_PROJECT']
    missing_vars = []
    
    print("\n🔍 Checking environment variables...")
    
    for var in required_vars:
        value = os.getenv(var)
        if not value:
            missing_vars.append(var)
            print(f"❌ Missing: {var}")
        else:
            # Mask sensitive data
            if 'TOKEN' in var:
                masked = value[:10] + "..." + value[-4:] if len(value) > 14 else "***"
                print(f"✅ Found: {var} = {masked}")
            else:
                print(f"✅ Found: {var} = {value}")
    
    if missing_vars:
        print(f"\n❌ Missing environment variables: {', '.join(missing_vars)}")
        print("Please check your .env file or environment variables.")
        return False
    
    return True

def test_jira_connection():
    """Test connection to JIRA."""
    try:
        from jira import JIRA
        
        server = os.getenv('JIRA_SERVER')
        email = os.getenv('JIRA_EMAIL')
        token = os.getenv('JIRA_API_TOKEN')
        
        print(f"\n🔌 Connecting to JIRA server: {server}")
        
        jira = JIRA(
            server=server,
            basic_auth=(email, token)
        )
        
        # Test connection by getting current user
        current_user = jira.current_user()
        print(f"✅ Connected successfully as: {current_user}")
        
        return jira
        
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        print("\nCommon solutions:")
        print("1. Check JIRA_SERVER URL (should include https://)")
        print("2. Verify JIRA_EMAIL matches your login email")
        print("3. Regenerate JIRA_API_TOKEN from Atlassian settings")
        print("4. Ensure you have access to the JIRA instance")
        return None

def test_project_access(jira):
    """Test access to the specified JIRA project."""
    try:
        project_key = os.getenv('JIRA_PROJECT')
        print(f"\n📋 Testing access to project: {project_key}")
        
        project = jira.project(project_key)
        print(f"✅ Project access confirmed: {project.name}")
        print(f"   Project description: {project.description or 'No description'}")
        print(f"   Project lead: {project.lead}")
        
        return True
        
    except Exception as e:
        print(f"❌ Project access failed: {e}")
        print("\nCommon solutions:")
        print("1. Check JIRA_PROJECT key is correct")
        print("2. Ask admin to grant access to the project")
        print("3. Verify project exists and is not archived")
        return False

def test_create_permission(jira):
    """Test if user can create issues."""
    try:
        project_key = os.getenv('JIRA_PROJECT')
        print(f"\n🎫 Testing issue creation permissions...")
        
        # Get issue types for the project
        project = jira.project(project_key)
        issue_types = jira.createmeta(projectKeys=project_key)
        
        if issue_types and len(issue_types['projects']) > 0:
            available_types = []
            for issue_type in issue_types['projects'][0].get('issuetypes', []):
                available_types.append(issue_type['name'])
            
            print(f"✅ Can create issues. Available types: {', '.join(available_types)}")
            return True
        else:
            print("❌ Cannot create issues or no issue types available")
            return False
            
    except Exception as e:
        print(f"❌ Permission check failed: {e}")
        print("You may not have permission to create issues in this project")
        return False

def main():
    """Main test function."""
    print("🧪 JIRA Connection Test")
    print("=" * 50)
    
    # Test 1: Package imports
    if not test_imports():
        sys.exit(1)
    
    # Test 2: Environment variables
    if not test_env_variables():
        print("\n💡 To fix:")
        print("1. Copy .env.example to .env")
        print("2. Edit .env with your JIRA credentials")
        print("3. See docs/setup/JIRA_CREDENTIALS_SETUP.md for detailed instructions")
        sys.exit(1)
    
    # Test 3: JIRA connection
    jira = test_jira_connection()
    if not jira:
        sys.exit(1)
    
    # Test 4: Project access
    project_access = test_project_access(jira)
    
    # Test 5: Create permissions
    create_permission = test_create_permission(jira)
    
    # Summary
    print("\n" + "=" * 50)
    print("📊 TEST RESULTS SUMMARY")
    print("=" * 50)
    
    if project_access and create_permission:
        print("🎉 ALL TESTS PASSED!")
        print("\nYou're ready to create JIRA issues:")
        print("  python scripts/create_jira_issue.py bug 'Test issue creation'")
        print("  python scripts/create_jira_issue.py feature 'Test feature request'")
        print("  python scripts/create_jira_issue.py task 'Test task creation' --interactive")
    else:
        print("⚠️  SOME TESTS FAILED")
        if not project_access:
            print("- Project access issues detected")
        if not create_permission:
            print("- Issue creation permission issues detected")
        print("\nContact your JIRA administrator for help with permissions.")

if __name__ == "__main__":
    main()