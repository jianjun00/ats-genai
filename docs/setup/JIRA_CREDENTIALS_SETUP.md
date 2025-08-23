# JIRA Credentials Setup Guide

## 🔐 Step-by-Step JIRA Credentials Setup

### Prerequisites
- Access to your company's JIRA instance
- Your work email address
- Admin access or permission to create API tokens

## Step 1: Find Your JIRA Server URL

Your JIRA server URL is typically in one of these formats:

```
https://your-company.atlassian.net
https://company-name.atlassian.net  
https://jira.your-company.com
```

**How to find it:**
1. **Ask your IT team/admin** - They can provide the exact URL
2. **Check browser history** - If you've accessed JIRA before
3. **Look at JIRA email notifications** - Links in emails show the URL
4. **Check existing documentation** - May be listed in company docs

**Example URLs:**
- `https://akolotechnologies.atlassian.net`
- `https://mycompany.atlassian.net`

## Step 2: Get Your JIRA Email

This is simply the email address you use to log into JIRA.

```
Usually your work email: firstname.lastname@company.com
```

## Step 3: Generate JIRA API Token

### Method A: Atlassian Account Settings (Recommended)

1. **Go to Atlassian API Tokens page:**
   ```
   https://id.atlassian.com/manage-profile/security/api-tokens
   ```

2. **Login** with your JIRA email and password

3. **Create API Token:**
   - Click "Create API token" button
   - **Label:** Enter a descriptive name like:
     - `ATS Development CLI`
     - `Local Development Tools`
     - `JIRA CLI Access`
   - Click "Create"

4. **COPY THE TOKEN IMMEDIATELY:**
   ```
   The token looks like: ATATT3xFfGF0T4JL8m1NqJxVs8QzM7bF9pR2...
   ```
   ⚠️ **Important:** You won't be able to see this token again!

5. **Save the token securely** - you'll need it for the next step

### Method B: Through JIRA Web Interface

1. **Login to JIRA:**
   ```
   https://your-company.atlassian.net
   ```

2. **Access Profile Settings:**
   - Click your profile picture (top-right corner)
   - Select "Account settings" or "Manage account"
   - Navigate to "Security" tab
   - Find "API tokens" or "Create and manage API tokens"

3. **Create API Token:**
   - Click "Create API token"
   - Enter descriptive name: `Development CLI Tools`
   - Click "Create"
   - **Copy the token immediately**

## Step 4: Configure Environment Variables

### Option A: Using .env File (Recommended for Development)

1. **Copy the example file:**
   ```bash
   cp .env.example .env
   ```

2. **Edit the .env file:**
   ```bash
   # Open in your preferred editor
   nano .env
   # or
   code .env
   ```

3. **Fill in your actual values:**
   ```bash
   # JIRA Configuration
   JIRA_SERVER=https://your-company.atlassian.net
   JIRA_EMAIL=your.email@company.com
   JIRA_API_TOKEN=ATATT3xFfGF0T4JL8m1NqJxVs8QzM7bF9pR2...
   JIRA_PROJECT=PGPT
   ```

4. **Save and secure the file:**
   ```bash
   # Make sure .env is in .gitignore (it already is)
   chmod 600 .env  # Restrict permissions
   ```

### Option B: Export Environment Variables

```bash
# Add these to your shell profile (~/.bashrc, ~/.zshrc, etc.)
export JIRA_SERVER="https://your-company.atlassian.net"
export JIRA_EMAIL="your.email@company.com"  
export JIRA_API_TOKEN="ATATT3xFfGF0T4JL8m1NqJxVs8QzM7bF9pR2..."
export JIRA_PROJECT="PGPT"

# Reload your shell
source ~/.bashrc  # or ~/.zshrc
```

## Step 5: Test Your Configuration

### Install Required Dependencies
```bash
pip install jira python-dotenv
```

### Test JIRA Connection
```bash
# Test the connection
python -c "
from jira import JIRA
from dotenv import load_dotenv
import os

load_dotenv()
jira = JIRA(
    server=os.getenv('JIRA_SERVER'),
    basic_auth=(os.getenv('JIRA_EMAIL'), os.getenv('JIRA_API_TOKEN'))
)
print(f'✅ Connected successfully as: {jira.current_user()}')
"
```

### Create Test JIRA Issue
```bash
# Create a test issue
python scripts/create_jira_issue.py task "Test JIRA API connection"
```

**Expected Output:**
```
✅ Created JIRA issue: PGPT-1234
📋 URL: https://your-company.atlassian.net/browse/PGPT-1234
📝 Title: Test JIRA API connection
🔄 Next Steps:
1. Edit the issue description with specific details
2. Create development branch:
   git checkout -b PGPT-1234/task-test-jira-api-connection
```

## 🚨 Common Issues and Solutions

### Issue 1: "Unauthorized" or "Authentication failed"
```bash
# Causes:
# - Wrong email address
# - Wrong API token  
# - Token expired or revoked

# Solutions:
1. Double-check email matches JIRA login email
2. Regenerate API token from Atlassian settings
3. Ensure no extra spaces in credentials
```

### Issue 2: "Project does not exist" 
```bash
# Cause: JIRA project key is wrong

# Solution:
# 1. Go to your JIRA instance
# 2. Find your project
# 3. Check the project key (usually visible in URL or project settings)
# 4. Update JIRA_PROJECT in .env file
```

### Issue 3: "Insufficient permissions"
```bash
# Cause: Your JIRA account doesn't have permission to create issues

# Solutions:
# 1. Ask JIRA admin to grant "Create Issues" permission
# 2. Ask to be added to appropriate JIRA project role
# 3. Request developer access to JIRA project
```

### Issue 4: "Connection timeout" or "Cannot reach server"
```bash
# Causes:
# - Wrong server URL
# - Network/firewall issues
# - VPN required

# Solutions:
1. Verify JIRA server URL by opening in browser
2. Check if VPN is required for JIRA access
3. Ask IT team about network requirements
```

## 🔒 Security Best Practices

### Protect Your API Token
```bash
# DO:
✅ Store in .env file (already in .gitignore)
✅ Use environment variables
✅ Set restrictive file permissions: chmod 600 .env
✅ Regenerate token if compromised

# DON'T:
❌ Commit API tokens to git
❌ Share tokens in chat/email
❌ Store in plain text files
❌ Use personal tokens for shared/production systems
```

### Token Management
```bash
# Regularly rotate API tokens (every 6 months)
# Create separate tokens for different purposes
# Revoke unused tokens immediately
# Monitor token usage in Atlassian security settings
```

## 📞 Getting Help

### If You Can't Access JIRA:
1. **Contact IT Support** - They manage JIRA access
2. **Ask Team Lead** - They may have admin access
3. **Check Company Documentation** - JIRA info may be documented

### If You Don't Have JIRA Admin Access:
1. **Request permissions** from JIRA admin
2. **Ask for project access** if you can't see PGPT project
3. **Request API token permissions** if disabled

### Contact Information:
```bash
# Typical contacts for JIRA help:
# - IT Support team
# - Project Manager  
# - JIRA Administrator
# - Senior Developer/Team Lead
```

## 🧪 Verification Checklist

After setup, verify you can:

- [ ] Access JIRA web interface with your credentials
- [ ] See the PGPT project in JIRA
- [ ] Connect via Python script without errors
- [ ] Create a test JIRA issue successfully
- [ ] View the created issue in JIRA web interface
- [ ] Edit and update the test issue

If all items are checked, you're ready to use JIRA integration! 🎉

---

## Quick Reference Commands

```bash
# Setup
cp .env.example .env
# Edit .env with your credentials
pip install jira python-dotenv

# Test connection  
python scripts/create_jira_issue.py task "Test connection"

# Create issues
python scripts/create_jira_issue.py bug "Fix something"
python scripts/create_jira_issue.py feature "Add new functionality"  
python scripts/create_jira_issue.py task "Update documentation" --interactive
```