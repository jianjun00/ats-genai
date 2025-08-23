# Manual JIRA Project Creation Guide

## 🎯 Create JIRA Project for ATS Platform

Since the JIRA API doesn't allow programmatic project creation with your current permissions, you need to create the project manually through the web interface.

## Step 1: Access JIRA Web Interface

1. **Open your JIRA instance:**
   ```
   https://jianjun00.atlassian.net
   ```

2. **Login** with your credentials:
   - Email: `janjun00@gmail.com`
   - Password: Your JIRA password (not the API token)

## Step 2: Create New Project

1. **Find "Create Project" option:**
   - Look for a "+" button or "Create Project" button
   - Usually in the top navigation bar or sidebar

2. **Select Project Template:**
   - Choose **"Scrum"** or **"Kanban"** template
   - Both work well for software development

3. **Configure Project Details:**
   - **Project Name:** `ATS Platform`
   - **Project Key:** `PGPT` (CRITICAL: Must be exactly "PGPT")
   - **Project Type:** Software Development
   - **Access:** Team-managed project (recommended)

## Step 3: Verify Project Settings

After creation, verify these settings:

- **Project Key:** Must be `PGPT` (matches your GitHub Actions workflow)
- **Issue Types:** Should include Task, Bug, Story, Epic
- **Permissions:** You should be able to create issues

## Step 4: Test Integration

Once the project is created, run this test:

```bash
# Test connection and project access
python scripts/test_jira_connection.py
```

**Expected output:**
```
✅ Connected successfully as: janjun00@gmail.com
✅ Project access confirmed: ATS Platform
✅ Can create issues. Available types: Task, Bug, Story, Epic
🎉 ALL TESTS PASSED!
```

## Step 5: Create Test Issue

```bash
# Create your first JIRA issue
python scripts/create_jira_issue.py task "Test JIRA project setup"
```

**Expected output:**
```
✅ Created JIRA issue: PGPT-1
📋 URL: https://jianjun00.atlassian.net/browse/PGPT-1
📝 Title: Test JIRA project setup
```

## 🚨 Important Notes

- **Project Key MUST be "PGPT"** - This matches your existing GitHub Actions workflow
- **Don't change the project key later** - It will break GitHub integration
- **Grant yourself admin permissions** on the project if possible

## If You Can't Create Projects

If you don't see the "Create Project" option:

1. **Check permissions** - You may need admin access
2. **Contact support** - Some JIRA instances restrict project creation
3. **Ask existing admin** to create the project for you with these exact specs:
   - Name: `ATS Platform`
   - Key: `PGPT`
   - Type: Software Development (Scrum or Kanban)

## Next Steps After Creation

Once the project exists:

1. ✅ Run connection test: `python scripts/test_jira_connection.py`
2. ✅ Create test issue: `python scripts/create_jira_issue.py task "Setup complete"`
3. ✅ Verify GitHub Actions integration works
4. ✅ Start using JIRA workflow for all development

---

**After creating the project manually, the complete JIRA integration will be functional and ready for development workflow.**