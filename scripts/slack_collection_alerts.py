#!/usr/bin/env python3
"""
ATS Collection Slack Alerts

Sends collection progress and alerts to Slack channel.
Monitors all collection jobs and sends status updates.

Usage:
    python slack_collection_alerts.py --webhook-url YOUR_WEBHOOK
    python slack_collection_alerts.py --config slack_config.json
"""

import sys
sys.path.append('/workspace/src')

import os
import json
import requests
import argparse
import subprocess
from datetime import datetime
from typing import Dict, List

class SlackCollectionAlerts:
    """Send ATS collection alerts to Slack."""
    
    def __init__(self, webhook_url: str):
        self.webhook_url = webhook_url
        
        self.collections = {
            'price_backfills': [
                {'name': 'Polygon 30Y', 'log': '/tmp/polygon_30year_daily_backfill.log'},
                {'name': 'Tiingo 30Y', 'log': '/tmp/tiingo_30year_backfill.log'},
                {'name': 'EODHD 30Y', 'log': '/tmp/eodhd_30year_backfill.log'},
            ],
            'events': [
                {'name': 'Polygon Earnings', 'log': '/tmp/polygon_earnings_fixed.log'},
                {'name': 'EODHD Events', 'log': '/tmp/eodhd_events.log'},
                {'name': 'Tiingo Events', 'log': '/tmp/tiingo_events.log'},
            ]
        }

    def get_collection_status(self, log_path: str) -> Dict:
        """Get status from log file."""
        status = {
            'running': False,
            'progress': 'Unknown',
            'current': 'Unknown',
            'errors': 0,
            'last_activity_mins': 999
        }
        
        try:
            if not os.path.exists(log_path):
                return status
            
            # Check if active (modified within 5 minutes)
            log_mtime = os.path.getmtime(log_path)
            last_activity = datetime.fromtimestamp(log_mtime)
            mins_ago = (datetime.now() - last_activity).total_seconds() / 60
            status['last_activity_mins'] = int(mins_ago)
            status['running'] = mins_ago < 5
            
            # Get latest info from log
            result = subprocess.run(['tail', '-20', log_path], 
                                  capture_output=True, text=True, timeout=5)
            
            for line in reversed(result.stdout.split('\n')):
                if not line.strip():
                    continue
                
                # Progress info
                if 'Progress:' in line and '%' in line:
                    start = line.find('(') + 1
                    end = line.find('%)')
                    if start > 0 and end > start:
                        status['progress'] = line[start:end+1]
                
                # Current activity
                if any(word in line for word in ['Processing', 'Collecting', 'events for']):
                    parts = line.split()
                    for part in parts:
                        if part.endswith('...') or part.endswith(':'):
                            symbol = part.replace('...', '').replace(':', '')
                            if 2 <= len(symbol) <= 6 and symbol.replace('-', '').replace('_', '').isalnum():
                                status['current'] = symbol
                                break
                
                # Error count
                if 'ERROR' in line or '❌' in line:
                    status['errors'] += 1
                    
        except Exception as e:
            status['error'] = str(e)
        
        return status

    def get_database_summary(self) -> str:
        """Get database record counts."""
        try:
            # Try to get event counts
            result = subprocess.run([
                'psql', '-h', 'localhost', '-p', '5433', '-U', 'postgres', '-d', 'dev_db', '-t', '-c',
                "SELECT vendor || ': ' || COUNT(*) || ' events' FROM dev_financial_events GROUP BY vendor ORDER BY COUNT(*) DESC"
            ], env={'PGPASSWORD': 'dev_password'}, capture_output=True, text=True, timeout=10)
            
            if result.returncode == 0:
                lines = [line.strip() for line in result.stdout.split('\n') if line.strip()]
                return '\n'.join(f"• {line}" for line in lines[:5])  # Top 5
            else:
                return "• Database connection failed"
                
        except Exception as e:
            return f"• Database error: {str(e)[:50]}"

    def create_status_message(self) -> Dict:
        """Create comprehensive status message for Slack."""
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        # Collection status
        active_jobs = []
        inactive_jobs = []
        error_jobs = []
        
        for category, jobs in self.collections.items():
            for job in jobs:
                status = self.get_collection_status(job['log'])
                
                job_info = {
                    'name': job['name'],
                    'status': status,
                    'category': category
                }
                
                if status['errors'] > 5:
                    error_jobs.append(job_info)
                elif status['running']:
                    active_jobs.append(job_info)
                else:
                    inactive_jobs.append(job_info)
        
        # Build Slack message
        color = "good" if len(active_jobs) > 0 else "warning" if len(error_jobs) == 0 else "danger"
        
        fields = []
        
        # Active jobs
        if active_jobs:
            active_text = ""
            for job in active_jobs:
                status = job['status']
                progress_info = f" ({status['progress']})" if status['progress'] != 'Unknown' else ""
                current_info = f" - {status['current']}" if status['current'] != 'Unknown' else ""
                active_text += f"🟢 {job['name']}{progress_info}{current_info}\n"
            
            fields.append({
                "title": f"🚀 Active Jobs ({len(active_jobs)})",
                "value": active_text.strip(),
                "short": False
            })
        
        # Inactive jobs
        if inactive_jobs:
            inactive_text = ""
            for job in inactive_jobs:
                mins = job['status']['last_activity_mins']
                time_info = f" ({mins}min ago)" if mins < 999 else " (no activity)"
                inactive_text += f"🔴 {job['name']}{time_info}\n"
            
            fields.append({
                "title": f"⏸️ Inactive Jobs ({len(inactive_jobs)})",
                "value": inactive_text.strip(),
                "short": True
            })
        
        # Error jobs
        if error_jobs:
            error_text = ""
            for job in error_jobs:
                error_count = job['status']['errors']
                error_text += f"⚠️ {job['name']} ({error_count} errors)\n"
            
            fields.append({
                "title": f"❌ Jobs with Errors ({len(error_jobs)})",
                "value": error_text.strip(),
                "short": True
            })
        
        # Database summary
        db_summary = self.get_database_summary()
        fields.append({
            "title": "📊 Database Records",
            "value": db_summary,
            "short": True
        })
        
        # Overall status
        total_jobs = len(active_jobs) + len(inactive_jobs) + len(error_jobs)
        status_emoji = "🟢" if len(active_jobs) > 0 else "🟡" if len(error_jobs) == 0 else "🔴"
        
        message = {
            "text": f"{status_emoji} ATS Collection Status Update",
            "attachments": [
                {
                    "color": color,
                    "title": f"📈 ATS Data Collection Dashboard - {timestamp}",
                    "text": f"Active: {len(active_jobs)} | Inactive: {len(inactive_jobs)} | Errors: {len(error_jobs)} | Total: {total_jobs}",
                    "fields": fields,
                    "footer": "ATS Collection Monitor",
                    "ts": int(datetime.now().timestamp())
                }
            ]
        }
        
        return message

    def send_to_slack(self, message: Dict) -> bool:
        """Send message to Slack."""
        try:
            response = requests.post(
                self.webhook_url,
                json=message,
                headers={'Content-Type': 'application/json'},
                timeout=10
            )
            
            if response.status_code == 200:
                print(f"✅ Slack message sent successfully")
                return True
            else:
                print(f"❌ Slack error: {response.status_code} - {response.text}")
                return False
                
        except Exception as e:
            print(f"❌ Failed to send to Slack: {e}")
            return False

    def send_alert(self, alert_type: str = "status") -> bool:
        """Send alert to Slack."""
        if alert_type == "status":
            message = self.create_status_message()
        else:
            message = {"text": f"Unknown alert type: {alert_type}"}
        
        return self.send_to_slack(message)

def main():
    parser = argparse.ArgumentParser(description="ATS Collection Slack Alerts")
    parser.add_argument('--webhook-url', type=str, help='Slack webhook URL')
    parser.add_argument('--config', type=str, help='Config file with webhook URL')
    parser.add_argument('--alert-type', type=str, default='status', help='Type of alert to send')
    
    args = parser.parse_args()
    
    # Get webhook URL
    webhook_url = None
    
    if args.webhook_url:
        webhook_url = args.webhook_url
    elif args.config and os.path.exists(args.config):
        with open(args.config) as f:
            config = json.load(f)
            webhook_url = config.get('webhook_url')
    else:
        webhook_url = os.environ.get('SLACK_WEBHOOK_URL')
    
    if not webhook_url:
        print("❌ No Slack webhook URL provided. Use --webhook-url, --config, or SLACK_WEBHOOK_URL env var")
        sys.exit(1)
    
    # Send alert
    alerter = SlackCollectionAlerts(webhook_url)
    success = alerter.send_alert(args.alert_type)
    
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()