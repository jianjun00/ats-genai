#!/usr/bin/env python3
"""
Jira Setup Script for Portfolio GPT MVP
Creates initial epics, stories, and tasks for project tracking
"""

import os
import requests
import json
from typing import Dict, List

class JiraSetup:
    def __init__(self):
        self.base_url = os.getenv('JIRA_BASE_URL', 'https://yourcompany.atlassian.net')
        self.email = os.getenv('JIRA_USER_EMAIL')
        self.token = os.getenv('JIRA_API_TOKEN')
        self.project_key = 'PGPT'
        
        if not all([self.email, self.token]):
            print("ERROR: JIRA_USER_EMAIL and JIRA_API_TOKEN environment variables required")
            exit(1)
    
    def create_issue(self, issue_data: Dict) -> Dict:
        """Create a Jira issue"""
        url = f"{self.base_url}/rest/api/3/issue"
        
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json"
        }
        
        response = requests.post(
            url,
            headers=headers,
            auth=(self.email, self.token),
            data=json.dumps(issue_data)
        )
        
        if response.status_code == 201:
            result = response.json()
            print(f"✅ Created: {result['key']} - {issue_data['fields']['summary']}")
            return result
        else:
            print(f"❌ Failed to create issue: {response.status_code}")
            print(f"Response: {response.text}")
            return {}
    
    def setup_epics(self) -> Dict[str, str]:
        """Create the main epics for Portfolio GPT MVP"""
        epics = [
            {
                "summary": "Data Pipeline Infrastructure",
                "description": "Build robust data pipeline for 3000+ stocks with 5-year historical data backfill. Includes instrument population, daily price ingestion, and data reconciliation.",
                "priority": "High"
            },
            {
                "summary": "Core Recommendation Engine", 
                "description": "Develop ML-based recommendation engine with multi-modal transformer for hourly price forecasts and stock recommendations.",
                "priority": "High"
            },
            {
                "summary": "Authentication & Subscription System",
                "description": "Implement API key management, usage tracking, and tier-based access control (free vs premium).",
                "priority": "Medium"
            },
            {
                "summary": "Dashboard & API Integration",
                "description": "Build user-facing dashboard and REST API for accessing recommendations and portfolio management.",
                "priority": "Medium"
            }
        ]
        
        epic_keys = {}
        for i, epic in enumerate(epics, 1):
            issue_data = {
                "fields": {
                    "project": {"key": self.project_key},
                    "summary": epic["summary"],
                    "description": epic["description"],
                    "issuetype": {"name": "Epic"},
                    "priority": {"name": epic["priority"]}
                }
            }
            
            result = self.create_issue(issue_data)
            if result:
                epic_keys[epic["summary"]] = result["key"]
        
        return epic_keys
    
    def setup_current_stories(self, epic_keys: Dict[str, str]) -> List[str]:
        """Create stories for current development work"""
        stories = [
            {
                "summary": "Fix database migration system table prefixing",
                "description": "Migration 028 (auth tables) has incorrect {env}_ placeholder format. Need to update to use standard table names for automatic prefixing by migration manager.",
                "epic": "Data Pipeline Infrastructure",
                "priority": "High",
                "story_points": 2
            },
            {
                "summary": "Complete dev_db database migrations",
                "description": "Run remaining migrations (028, 029) to bring dev_db to latest schema version. Currently at version 27/29.",
                "epic": "Data Pipeline Infrastructure", 
                "priority": "High",
                "story_points": 1
            },
            {
                "summary": "Populate 3000 instruments in ats-dev environment",
                "description": "Use existing populate_instrument_polygon.py to scale from 50 to 3000 liquid stocks in dev_db via port-forward approach.",
                "epic": "Data Pipeline Infrastructure",
                "priority": "High", 
                "story_points": 5
            },
            {
                "summary": "Implement 5-year daily price backfill",
                "description": "Backfill historical daily prices for 3000 stocks covering 2020-2025 period using existing data pipeline infrastructure.",
                "epic": "Data Pipeline Infrastructure",
                "priority": "High",
                "story_points": 8
            },
            {
                "summary": "Data reconciliation and quality validation",
                "description": "Run data reconciliation between Polygon and Tiingo sources, validate data quality, and generate completeness reports.",
                "epic": "Data Pipeline Infrastructure", 
                "priority": "Medium",
                "story_points": 3
            }
        ]
        
        story_keys = []
        for story in stories:
            epic_key = epic_keys.get(story["epic"])
            
            issue_data = {
                "fields": {
                    "project": {"key": self.project_key},
                    "summary": story["summary"],
                    "description": story["description"],
                    "issuetype": {"name": "Story"},
                    "priority": {"name": story["priority"]},
                    "customfield_10014": story["story_points"],  # Story Points field ID
                }
            }
            
            # Link to epic if epic key exists
            if epic_key:
                issue_data["fields"]["customfield_10008"] = epic_key  # Epic Link field ID
            
            result = self.create_issue(issue_data)
            if result:
                story_keys.append(result["key"])
        
        return story_keys
    
    def run_setup(self):
        """Execute full Jira setup"""
        print("🚀 Setting up Jira project structure for Portfolio GPT MVP...")
        print(f"📊 Project: {self.project_key}")
        print(f"🔗 Jira URL: {self.base_url}")
        print()
        
        print("📋 Creating Epics...")
        epic_keys = self.setup_epics()
        print()
        
        print("📝 Creating Current Development Stories...")
        story_keys = self.setup_current_stories(epic_keys)
        print()
        
        print("✅ Setup complete!")
        print(f"📊 Created {len(epic_keys)} epics and {len(story_keys)} stories")
        print(f"🔗 View project: {self.base_url}/jira/software/projects/{self.project_key}")

if __name__ == "__main__":
    setup = JiraSetup()
    setup.run_setup()