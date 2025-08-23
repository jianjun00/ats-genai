#!/usr/bin/env python3
"""
JIRA Authentication Diagnostics

Helps identify why JIRA authentication is failing.
"""

import os
import requests
import base64
from dotenv import load_dotenv

load_dotenv()

def check_environment():
    """Check environment variables"""
    print("🔍 Environment Variables:")
    print("-" * 40)
    
    server = os.getenv('JIRA_SERVER')
    email = os.getenv('JIRA_EMAIL')
    token = os.getenv('JIRA_API_TOKEN')
    
    if not server:
        print("❌ JIRA_SERVER not set")
        return False
    if not email:
        print("❌ JIRA_EMAIL not set")
        return False
    if not token:
        print("❌ JIRA_API_TOKEN not set")
        return False
    
    print(f"✅ JIRA_SERVER: {server}")
    print(f"✅ JIRA_EMAIL: {email}")
    print(f"✅ JIRA_API_TOKEN: {token[:15]}...{token[-5:]}")
    print(f"✅ Token Length: {len(token)} characters")
    
    return True

def check_server_access():
    """Check if JIRA server is accessible"""
    print("\n🌐 Server Accessibility:")
    print("-" * 40)
    
    server = os.getenv('JIRA_SERVER')
    
    try:
        response = requests.get(server, timeout=10)
        print(f"✅ Server accessible: HTTP {response.status_code}")
        
        # Check server info
        response = requests.get(f"{server}/rest/api/2/serverInfo", timeout=10)
        if response.status_code == 200:
            info = response.json()
            print(f"✅ JIRA Version: {info['version']}")
            print(f"✅ Deployment Type: {info['deploymentType']}")
            print(f"✅ Server Title: {info['serverTitle']}")
            return True
        else:
            print(f"⚠️  Server info unavailable: HTTP {response.status_code}")
            return False
    except Exception as e:
        print(f"❌ Server not accessible: {e}")
        return False

def check_authentication():
    """Test different authentication methods"""
    print("\n🔐 Authentication Tests:")
    print("-" * 40)
    
    server = os.getenv('JIRA_SERVER')
    email = os.getenv('JIRA_EMAIL')
    token = os.getenv('JIRA_API_TOKEN')
    
    # Test 1: Basic auth with requests library
    print("Test 1: Requests basic auth")
    try:
        response = requests.get(
            f"{server}/rest/api/2/myself",
            auth=(email, token),
            headers={'Accept': 'application/json'},
            timeout=10
        )
        if response.status_code == 200:
            user_info = response.json()
            print(f"✅ Authentication successful! User: {user_info.get('displayName', email)}")
            return True
        else:
            print(f"❌ Auth failed: HTTP {response.status_code}")
            print(f"   Response: {response.text[:200]}")
    except Exception as e:
        print(f"❌ Auth error: {e}")
    
    # Test 2: Manual base64 encoding
    print("\nTest 2: Manual base64 encoding")
    try:
        credentials = f"{email}:{token}"
        encoded = base64.b64encode(credentials.encode()).decode()
        
        response = requests.get(
            f"{server}/rest/api/2/myself",
            headers={
                'Authorization': f'Basic {encoded}',
                'Accept': 'application/json'
            },
            timeout=10
        )
        if response.status_code == 200:
            user_info = response.json()
            print(f"✅ Manual encoding successful! User: {user_info.get('displayName', email)}")
            return True
        else:
            print(f"❌ Manual encoding failed: HTTP {response.status_code}")
            print(f"   Response: {response.text[:200]}")
    except Exception as e:
        print(f"❌ Manual encoding error: {e}")
    
    # Test 3: Check for specific error details
    print("\nTest 3: Detailed error analysis")
    try:
        response = requests.get(
            f"{server}/rest/api/2/myself",
            auth=(email, token),
            headers={'Accept': 'application/json'},
            timeout=10
        )
        
        if 'X-Seraph-Loginreason' in response.headers:
            reason = response.headers['X-Seraph-Loginreason']
            print(f"🔍 Login failure reason: {reason}")
            
            if reason == 'AUTHENTICATED_FAILED':
                print("   This usually means:")
                print("   - API token is invalid/expired")
                print("   - Email address doesn't match JIRA account")
                print("   - Account is disabled or suspended")
        
        if 'Www-Authenticate' in response.headers:
            auth_header = response.headers['Www-Authenticate']
            print(f"🔍 Authentication method expected: {auth_header}")
    except Exception as e:
        print(f"❌ Error analysis failed: {e}")
    
    return False

def suggest_solutions():
    """Provide troubleshooting suggestions"""
    print("\n🔧 Troubleshooting Steps:")
    print("-" * 40)
    
    print("1. **Verify API Token:**")
    print("   - Go to: https://id.atlassian.com/manage-profile/security/api-tokens")
    print("   - Check if your token still exists and is active")
    print("   - If expired/revoked, create a new token")
    
    print("\n2. **Verify Email Address:**")
    print("   - Ensure JIRA_EMAIL matches your actual login email")
    print("   - Try logging into JIRA web interface with the same email")
    
    print("\n3. **Check Account Status:**")
    print("   - Log into JIRA web interface to ensure account is active")
    print("   - Verify you have permissions to access JIRA")
    
    print("\n4. **Test in Browser:**")
    print("   - Visit: https://jianjun00.atlassian.net")
    print("   - Ensure you can log in successfully")
    
    print("\n5. **API Token Regeneration:**")
    print("   - Delete existing token")
    print("   - Generate new token with label 'ATS Development CLI'")
    print("   - Update .env file with new token")

def main():
    """Main diagnostic function"""
    print("🧪 JIRA Authentication Diagnostics")
    print("=" * 50)
    
    # Step 1: Check environment
    if not check_environment():
        print("\n❌ Environment setup incomplete!")
        return False
    
    # Step 2: Check server
    if not check_server_access():
        print("\n❌ Server access issues!")
        return False
    
    # Step 3: Check authentication
    if not check_authentication():
        print("\n❌ Authentication failed!")
        suggest_solutions()
        return False
    
    print("\n🎉 All diagnostics passed! JIRA integration should work.")
    return True

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)